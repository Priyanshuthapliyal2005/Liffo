// Package modellife manages inference model lifecycle states with an Inngest-like
// deferred job queue: if a replace/upload arrives while requests are in flight,
// the manager drains first, then evicts, then loads the queued model.
//
// States: loading | ready | draining | evicting | failed | stopped
package modellife

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// State is a model instance lifecycle state.
type State string

const (
	StateLoading  State = "loading"
	StateReady    State = "ready"
	StateDraining State = "draining"
	StateEvicting State = "evicting"
	StateFailed   State = "failed"
	StateStopped  State = "stopped"
)

// JobKind is a deferred lifecycle job (queued like Inngest steps).
type JobKind string

const (
	JobLoad  JobKind = "load"
	JobEvict JobKind = "evict"
)

var (
	ErrNotReady     = errors.New("modellife: model not ready")
	ErrDraining     = errors.New("modellife: model draining; not admitting")
	ErrEvicting     = errors.New("modellife: model evicting")
	ErrStopped      = errors.New("modellife: model stopped")
	ErrFailed       = errors.New("modellife: model failed")
	ErrLoading      = errors.New("modellife: model loading")
	ErrLoadTimeout  = errors.New("modellife: load timeout")
	ErrEvictTimeout = errors.New("modellife: evict timeout")
	ErrDrainTimeout = errors.New("modellife: drain timeout")
	ErrInvalidModel = errors.New("modellife: empty model name")
	ErrQueueFull    = errors.New("modellife: job queue full")
)

// Loader performs actual load/unload (GGUF pull, remote fetch, etc.). Tests
// inject controlled test doubles.
type Loader interface {
	Load(ctx context.Context, model string) error
	Unload(ctx context.Context, model string) error
}

// Config tunes timeouts and admission.
type Config struct {
	LoadTimeout  time.Duration // default 30s
	EvictTimeout time.Duration // default 15s
	DrainTimeout time.Duration // 0 = wait until inflight==0
	MaxQueue     int           // default 32
}

func (c Config) normalized() Config {
	if c.LoadTimeout <= 0 {
		c.LoadTimeout = 30 * time.Second
	}
	if c.EvictTimeout <= 0 {
		c.EvictTimeout = 15 * time.Second
	}
	if c.MaxQueue <= 0 {
		c.MaxQueue = 32
	}
	return c
}

// Job is a queued lifecycle operation.
type Job struct {
	ID         string    `json:"id"`
	Kind       JobKind   `json:"kind"`
	Model      string    `json:"model,omitempty"`
	EnqueuedAt time.Time `json:"enqueued_at"`
	done       chan struct{}
	err        error
}

// Event is a privacy-safe audit row (no prompt text).
type Event struct {
	TS         time.Time `json:"ts"`
	Seq        int       `json:"seq"`
	Kind       string    `json:"kind"`
	From       State     `json:"from,omitempty"`
	To         State     `json:"to,omitempty"`
	Model      string    `json:"model,omitempty"`
	Inflight   int       `json:"inflight"`
	QueueDepth int       `json:"queue_depth"`
	JobID      string    `json:"job_id,omitempty"`
	JobKind    string    `json:"job_kind,omitempty"`
	Detail     string    `json:"detail,omitempty"`
	Error      string    `json:"error,omitempty"`
}

// Snapshot is a point-in-time view.
type Snapshot struct {
	State      State  `json:"state"`
	Model      string `json:"model"`
	Inflight   int    `json:"inflight"`
	QueueDepth int    `json:"queue_depth"`
	LastError  string `json:"last_error,omitempty"`
	LoadGen    uint64 `json:"load_gen"`
}

// Lease is one admitted in-flight request on a ready model.
type Lease struct {
	id       uint64
	manager  *Manager
	model    string
	gen      uint64
	released atomic.Bool
}

// Model returns the model name captured at Acquire.
func (l *Lease) Model() string {
	if l == nil {
		return ""
	}
	return l.model
}

// Manager owns one model slot: lifecycle FSM + deferred job queue.
type Manager struct {
	cfg    Config
	loader Loader

	mu       sync.Mutex
	state    State
	model    string
	inflight int
	lastErr  string
	loadGen  uint64
	queue    []*Job
	workerOn bool
	closed   bool
	seq      int
	events   []Event
	idSeq    uint64
	drainCh  chan struct{} // closed when inflight hits 0 while draining
	wake     chan struct{}
	wg       sync.WaitGroup
}

// New creates a manager in StateStopped.
func New(cfg Config, loader Loader) (*Manager, error) {
	if loader == nil {
		return nil, fmt.Errorf("modellife: nil loader")
	}
	m := &Manager{
		cfg:    cfg.normalized(),
		loader: loader,
		state:  StateStopped,
		wake:   make(chan struct{}, 1),
		events: make([]Event, 0, 256),
	}
	m.recordLocked(Event{Kind: "init", To: StateStopped, Detail: "manager created"})
	return m, nil
}

// Snapshot returns current state.
func (m *Manager) Snapshot() Snapshot {
	m.mu.Lock()
	defer m.mu.Unlock()
	return Snapshot{
		State: m.state, Model: m.model, Inflight: m.inflight,
		QueueDepth: len(m.queue), LastError: m.lastErr, LoadGen: m.loadGen,
	}
}

// Events returns a copy of the audit log.
func (m *Manager) Events() []Event {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]Event, len(m.events))
	copy(out, m.events)
	return out
}

// Shutdown stops admission, evicts the resident model, cancels queued jobs, and
// waits for the worker. It returns unload/timeout failures to the caller.
func (m *Manager) Shutdown(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return nil
	}
	hasModel := m.model != "" || m.state == StateReady || m.state == StateDraining || m.state == StateFailed
	m.mu.Unlock()

	var unloadErr error
	if hasModel {
		unloadErr = m.EnqueueEvict(ctx)
	}

	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return unloadErr
	}
	m.closed = true
	m.mu.Unlock()
	m.signal()
	done := make(chan struct{})
	go func() {
		m.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		return unloadErr
	case <-ctx.Done():
		if unloadErr != nil {
			return errors.Join(unloadErr, ctx.Err())
		}
		return ctx.Err()
	}
}

// Close preserves the existing API while enforcing bounded process cleanup.
func (m *Manager) Close() {
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	_ = m.Shutdown(ctx)
}

// Acquire admits one concurrent request. Only StateReady accepts new work.
func (m *Manager) Acquire(ctx context.Context) (*Lease, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return nil, ErrStopped
	}
	switch m.state {
	case StateReady:
		// admit
	case StateDraining:
		m.recordLocked(Event{Kind: "admit_reject", Detail: "draining", Inflight: m.inflight, QueueDepth: len(m.queue), Model: m.model})
		return nil, ErrDraining
	case StateEvicting:
		m.recordLocked(Event{Kind: "admit_reject", Detail: "evicting", Inflight: m.inflight})
		return nil, ErrEvicting
	case StateLoading:
		m.recordLocked(Event{Kind: "admit_reject", Detail: "loading"})
		return nil, ErrLoading
	case StateFailed:
		return nil, ErrFailed
	case StateStopped:
		return nil, ErrStopped
	default:
		return nil, ErrNotReady
	}
	m.inflight++
	m.idSeq++
	id := m.idSeq
	gen := m.loadGen
	model := m.model
	m.recordLocked(Event{
		Kind: "admit", Model: model, Inflight: m.inflight, QueueDepth: len(m.queue),
		Detail: fmt.Sprintf("lease=%d gen=%d", id, gen),
	})
	return &Lease{id: id, manager: m, model: model, gen: gen}, nil
}

// Release finishes a request. When draining and inflight hits 0, signals drain complete.
func (l *Lease) Release() {
	if l == nil || l.manager == nil {
		return
	}
	if !l.released.CompareAndSwap(false, true) {
		return
	}
	m := l.manager
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.inflight > 0 {
		m.inflight--
	}
	m.recordLocked(Event{
		Kind: "complete", Model: l.model, Inflight: m.inflight, QueueDepth: len(m.queue),
		Detail: fmt.Sprintf("lease=%d", l.id),
	})
	if m.state == StateDraining && m.inflight == 0 {
		m.signalDrainLocked()
	}
	m.signalLocked()
}

// EnqueueLoad queues a model load (Inngest-style deferred step). If the current
// model has in-flight requests, the manager drains → evicts → then loads.
// Blocks until this load job finishes or ctx is canceled.
func (m *Manager) EnqueueLoad(ctx context.Context, model string) error {
	if model == "" {
		return ErrInvalidModel
	}
	job, err := m.enqueue(JobLoad, model)
	if err != nil {
		return err
	}
	return m.waitJob(ctx, job)
}

// EnqueueEvict queues drain → evict → stopped.
func (m *Manager) EnqueueEvict(ctx context.Context) error {
	job, err := m.enqueue(JobEvict, "")
	if err != nil {
		return err
	}
	return m.waitJob(ctx, job)
}

func (m *Manager) enqueue(kind JobKind, model string) (*Job, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return nil, ErrStopped
	}
	if len(m.queue) >= m.cfg.MaxQueue {
		return nil, ErrQueueFull
	}
	m.idSeq++
	job := &Job{
		ID:         fmt.Sprintf("job-%d", m.idSeq),
		Kind:       kind,
		Model:      model,
		EnqueuedAt: time.Now().UTC(),
		done:       make(chan struct{}),
	}
	m.queue = append(m.queue, job)
	m.recordLocked(Event{
		Kind: "queue", JobID: job.ID, JobKind: string(kind), Model: model,
		Inflight: m.inflight, QueueDepth: len(m.queue),
		Detail: fmt.Sprintf("state=%s", m.state),
	})
	// Stop new admits immediately when replacing/evicting a busy ready model.
	if m.state == StateReady && m.inflight > 0 {
		m.beginDrainLocked("queued_" + string(kind))
	}
	m.ensureWorkerLocked()
	m.signalLocked()
	return job, nil
}

func (m *Manager) waitJob(ctx context.Context, job *Job) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-job.done:
		return job.err
	}
}

func (m *Manager) ensureWorkerLocked() {
	if m.workerOn || m.closed {
		return
	}
	m.workerOn = true
	m.wg.Add(1)
	go m.worker()
}

func (m *Manager) signal() {
	m.mu.Lock()
	m.signalLocked()
	m.mu.Unlock()
}

func (m *Manager) signalLocked() {
	select {
	case m.wake <- struct{}{}:
	default:
	}
}

func (m *Manager) worker() {
	defer func() {
		m.mu.Lock()
		m.workerOn = false
		m.mu.Unlock()
		m.wg.Done()
	}()

	for {
		m.mu.Lock()
		for len(m.queue) == 0 {
			if m.closed {
				m.mu.Unlock()
				return
			}
			m.mu.Unlock()
			<-m.wake
			m.mu.Lock()
		}
		job := m.queue[0]
		state := m.state
		model := m.model
		m.mu.Unlock()

		// Phase 1: drain → evict before load/evict when a model is (or was) resident.
		needsClear := model != "" && (state == StateReady || state == StateDraining || state == StateFailed)
		if job.Kind == JobLoad || job.Kind == JobEvict {
			if needsClear || state == StateDraining {
				if err := m.drainThenEvict(job); err != nil {
					m.finishHead(job, err)
					continue
				}
			}
		}

		// Phase 2: apply job
		var jobErr error
		switch job.Kind {
		case JobLoad:
			jobErr = m.doLoad(job.Model)
		case JobEvict:
			m.mu.Lock()
			if m.state != StateStopped {
				m.transitionLocked(StateStopped, "evict_job_finalize")
			}
			m.mu.Unlock()
		default:
			jobErr = fmt.Errorf("modellife: unknown job %s", job.Kind)
		}
		m.finishHead(job, jobErr)
	}
}

// drainThenEvict implements: running requests finish → draining complete → evicting → stopped.
func (m *Manager) drainThenEvict(job *Job) error {
	m.mu.Lock()
	st := m.state
	hasModel := m.model != ""
	m.mu.Unlock()

	if !hasModel && st != StateDraining {
		return nil
	}

	// Enter draining if ready with work (or ready about to be replaced).
	m.mu.Lock()
	if m.state == StateReady {
		m.beginDrainLocked("before_" + string(job.Kind))
	}
	// Failed/stopped with leftover name: skip drain, go straight to unload if needed
	m.mu.Unlock()

	if err := m.waitDrain(); err != nil {
		return err
	}

	// Evict if anything still resident or state still draining/ready
	m.mu.Lock()
	st = m.state
	hasModel = m.model != ""
	m.mu.Unlock()
	if hasModel || st == StateDraining || st == StateReady {
		return m.doEvict()
	}
	return nil
}

func (m *Manager) finishHead(job *Job, err error) {
	m.mu.Lock()
	if len(m.queue) > 0 && m.queue[0].ID == job.ID {
		m.queue = m.queue[1:]
	}
	job.err = err
	detail := "ok"
	if err != nil {
		detail = err.Error()
	}
	m.recordLocked(Event{
		Kind: "job_done", JobID: job.ID, JobKind: string(job.Kind), Model: job.Model,
		Inflight: m.inflight, QueueDepth: len(m.queue), Detail: detail, Error: errString(err),
	})
	// dequeue audit
	m.recordLocked(Event{
		Kind: "dequeue", JobID: job.ID, JobKind: string(job.Kind), Model: job.Model,
		Inflight: m.inflight, QueueDepth: len(m.queue),
	})
	close(job.done)
	m.mu.Unlock()
}

func (m *Manager) beginDrainLocked(reason string) {
	if m.state == StateDraining {
		return
	}
	if m.state != StateReady {
		return
	}
	from := m.state
	m.state = StateDraining
	m.drainCh = make(chan struct{})
	m.recordLocked(Event{
		Kind: "transition", From: from, To: StateDraining, Detail: reason,
		Inflight: m.inflight, QueueDepth: len(m.queue), Model: m.model,
	})
	if m.inflight == 0 {
		m.signalDrainLocked()
	}
}

func (m *Manager) signalDrainLocked() {
	m.recordLocked(Event{Kind: "drain_complete", From: StateDraining, Inflight: m.inflight, QueueDepth: len(m.queue), Model: m.model})
	if m.drainCh != nil {
		ch := m.drainCh
		m.drainCh = nil
		close(ch)
	}
}

func (m *Manager) waitDrain() error {
	m.mu.Lock()
	if m.inflight == 0 {
		if m.state == StateDraining && m.drainCh != nil {
			m.signalDrainLocked()
		}
		m.mu.Unlock()
		return nil
	}
	if m.state == StateReady {
		m.beginDrainLocked("wait_drain")
	}
	ch := m.drainCh
	timeout := m.cfg.DrainTimeout
	m.mu.Unlock()

	if ch == nil {
		// No channel: poll until inflight 0
		deadline := time.Now().Add(timeout)
		for {
			m.mu.Lock()
			n := m.inflight
			m.mu.Unlock()
			if n == 0 {
				return nil
			}
			if timeout > 0 && time.Now().After(deadline) {
				m.mu.Lock()
				m.recordLocked(Event{Kind: "drain_timeout", Error: ErrDrainTimeout.Error(), Inflight: m.inflight})
				m.mu.Unlock()
				return ErrDrainTimeout
			}
			time.Sleep(5 * time.Millisecond)
		}
	}
	if timeout <= 0 {
		<-ch
		return nil
	}
	select {
	case <-ch:
		return nil
	case <-time.After(timeout):
		m.mu.Lock()
		m.recordLocked(Event{Kind: "drain_timeout", Error: ErrDrainTimeout.Error(), Inflight: m.inflight})
		m.mu.Unlock()
		return ErrDrainTimeout
	}
}

func (m *Manager) doEvict() error {
	m.mu.Lock()
	if m.state == StateStopped && m.model == "" {
		m.mu.Unlock()
		return nil
	}
	model := m.model
	from := m.state
	m.state = StateEvicting
	m.recordLocked(Event{
		Kind: "transition", From: from, To: StateEvicting, Model: model,
		Inflight: m.inflight, QueueDepth: len(m.queue),
	})
	m.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), m.cfg.EvictTimeout)
	defer cancel()
	err := m.loader.Unload(ctx, model)
	if err != nil {
		if ctx.Err() != nil {
			err = fmt.Errorf("%w: %v", ErrEvictTimeout, err)
		}
		m.mu.Lock()
		m.lastErr = err.Error()
		m.transitionLocked(StateFailed, "evict_failed")
		m.recordLocked(Event{Kind: "evict_fail", Model: model, Error: err.Error(), Inflight: m.inflight})
		m.mu.Unlock()
		return err
	}
	m.mu.Lock()
	m.model = ""
	m.transitionLocked(StateStopped, "evict_ok")
	m.recordLocked(Event{Kind: "evict_ok", Model: model, Inflight: m.inflight, QueueDepth: len(m.queue)})
	m.mu.Unlock()
	return nil
}

func (m *Manager) doLoad(model string) error {
	m.mu.Lock()
	// Safety: never load over a resident non-terminal model.
	if m.model != "" && m.state != StateStopped && m.state != StateFailed {
		m.mu.Unlock()
		if err := m.doEvict(); err != nil {
			return err
		}
		m.mu.Lock()
	}
	from := m.state
	m.state = StateLoading
	m.model = model
	m.lastErr = ""
	m.recordLocked(Event{Kind: "transition", From: from, To: StateLoading, Model: model, QueueDepth: len(m.queue)})
	m.recordLocked(Event{Kind: "load_start", Model: model, QueueDepth: len(m.queue)})
	m.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), m.cfg.LoadTimeout)
	defer cancel()
	err := m.loader.Load(ctx, model)
	if err != nil {
		if ctx.Err() != nil {
			err = fmt.Errorf("%w: %v", ErrLoadTimeout, err)
		}
		m.mu.Lock()
		m.lastErr = err.Error()
		m.transitionLocked(StateFailed, "load_failed")
		m.recordLocked(Event{Kind: "load_fail", Model: model, Error: err.Error()})
		m.mu.Unlock()
		return err
	}
	m.mu.Lock()
	m.loadGen++
	m.transitionLocked(StateReady, "load_ok")
	m.recordLocked(Event{Kind: "load_ok", Model: model, To: StateReady, QueueDepth: len(m.queue)})
	m.mu.Unlock()
	return nil
}

func (m *Manager) transitionLocked(to State, reason string) {
	from := m.state
	m.state = to
	m.recordLocked(Event{
		Kind: "transition", From: from, To: to, Model: m.model,
		Inflight: m.inflight, QueueDepth: len(m.queue), Detail: reason,
	})
}

func (m *Manager) recordLocked(e Event) {
	m.seq++
	e.Seq = m.seq
	if e.TS.IsZero() {
		e.TS = time.Now().UTC()
	}
	m.events = append(m.events, e)
	if len(m.events) > 2000 {
		m.events = m.events[len(m.events)-2000:]
	}
}

func errString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

// States returns the canonical lifecycle states.
func States() []State {
	return []State{StateLoading, StateReady, StateDraining, StateEvicting, StateFailed, StateStopped}
}

package modellife

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
)

// RuntimeState supplies live placement factors without coupling modellife to
// the router package.
type RuntimeState struct {
	Healthy   bool
	Inflight  int64
	LatencyMs float64
}

// WorkerController is one lifecycle-capable or route-only worker.
type WorkerController struct {
	ID       int
	Label    string
	Models   map[string]bool
	Manager  *Manager
	Resident string
	Cost     float64
	Latency  float64
	Runtime  func() RuntimeState
}

// PlacementResult describes a verified load decision.
type PlacementResult struct {
	Model    string `json:"model"`
	WorkerID int    `json:"worker_id"`
	Worker   string `json:"worker"`
	State    State  `json:"state"`
	LoadGen  uint64 `json:"load_gen"`
}

// ErrUnknownWorker identifies an unrecognized logical placement target.
var ErrUnknownWorker = errors.New("modellife: unknown worker")

// WorkerStatus is the public, path-free lifecycle view.
type WorkerStatus struct {
	WorkerID   int      `json:"worker_id"`
	Worker     string   `json:"worker"`
	Managed    bool     `json:"managed"`
	Models     []string `json:"models"`
	Resident   string   `json:"resident,omitempty"`
	State      State    `json:"state"`
	Inflight   int      `json:"inflight"`
	QueueDepth int      `json:"queue_depth"`
	LastError  string   `json:"last_error,omitempty"`
	LoadGen    uint64   `json:"load_gen"`
}

// Coordinator owns one lifecycle FSM per controllable worker.
type Coordinator struct {
	mu      sync.RWMutex
	workers map[int]*WorkerController
	labels  map[string]int
}

func NewCoordinator(workers []*WorkerController) (*Coordinator, error) {
	c := &Coordinator{workers: map[int]*WorkerController{}, labels: map[string]int{}}
	for _, w := range workers {
		if w == nil || w.ID < 0 || w.Label == "" {
			return nil, fmt.Errorf("modellife: invalid worker controller")
		}
		if _, ok := c.workers[w.ID]; ok {
			return nil, fmt.Errorf("modellife: duplicate worker id %d", w.ID)
		}
		if _, ok := c.labels[w.Label]; ok {
			return nil, fmt.Errorf("modellife: duplicate worker label %q", w.Label)
		}
		c.workers[w.ID] = w
		c.labels[w.Label] = w.ID
	}
	return c, nil
}

func (c *Coordinator) HasManaged() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	for _, w := range c.workers {
		if w.Manager != nil {
			return true
		}
	}
	return false
}

func (c *Coordinator) SetRuntime(workerID int, fn func() RuntimeState) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if w := c.workers[workerID]; w != nil {
		w.Runtime = fn
	}
}

// ReadyWorkers returns workers that are presently able to serve model.
func (c *Coordinator) ReadyWorkers(model string) map[int]bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	out := map[int]bool{}
	for id, w := range c.workers {
		if !w.Models[model] {
			continue
		}
		if w.Manager == nil {
			if w.Resident == model {
				out[id] = true
			}
			continue
		}
		s := w.Manager.Snapshot()
		if s.State == StateReady && s.Model == model {
			out[id] = true
		}
	}
	return out
}

// Load selects an eligible managed worker and performs a real load.
func (c *Coordinator) Load(ctx context.Context, model string) (PlacementResult, error) {
	if model == "" {
		return PlacementResult{}, ErrInvalidModel
	}
	c.mu.RLock()
	var best *WorkerController
	bestScore := -1e18
	for _, w := range c.workers {
		if w.Manager == nil || !w.Models[model] {
			continue
		}
		rt := RuntimeState{Healthy: true, LatencyMs: w.Latency}
		if w.Runtime != nil {
			rt = w.Runtime()
		}
		snap := w.Manager.Snapshot()
		score := 0.0
		if rt.Healthy {
			score += 100
		}
		if snap.State == StateReady && snap.Model == model {
			score += 1000
		}
		score -= float64(rt.Inflight) * 10
		score -= rt.LatencyMs / 10
		score -= w.Cost * 10
		if best == nil || score > bestScore || (score == bestScore && w.ID < best.ID) {
			best = w
			bestScore = score
		}
	}
	c.mu.RUnlock()
	if best == nil {
		return PlacementResult{}, fmt.Errorf("%w: %q", ErrUnknownModel, model)
	}
	return loadOnWorker(ctx, best, model)
}

// LoadOnWorker performs an explicit placement request using only a logical
// model ID and worker label. Filesystem paths never cross this boundary.
func (c *Coordinator) LoadOnWorker(ctx context.Context, label, model string) (PlacementResult, error) {
	worker, err := c.loadTarget(label, model)
	if err != nil {
		return PlacementResult{}, err
	}
	return loadOnWorker(ctx, worker, model)
}

// ValidateLoadTarget checks an explicit placement without changing runtime
// state. It lets asynchronous HTTP requests reject invalid worker/model pairs
// before returning 202 Accepted.
func (c *Coordinator) ValidateLoadTarget(label, model string) error {
	_, err := c.loadTarget(label, model)
	return err
}

func (c *Coordinator) loadTarget(label, model string) (*WorkerController, error) {
	if model == "" {
		return nil, ErrInvalidModel
	}
	c.mu.RLock()
	id, ok := c.labels[label]
	worker := c.workers[id]
	c.mu.RUnlock()
	if !ok || worker == nil {
		return nil, fmt.Errorf("%w: %q", ErrUnknownWorker, label)
	}
	if worker.Manager == nil {
		return nil, fmt.Errorf("modellife: worker %q is route-only", label)
	}
	if !worker.Models[model] {
		return nil, fmt.Errorf("%w: %q", ErrUnknownModel, model)
	}
	return worker, nil
}

func loadOnWorker(ctx context.Context, worker *WorkerController, model string) (PlacementResult, error) {
	if worker == nil || worker.Manager == nil {
		return PlacementResult{}, errors.New("modellife: managed worker required")
	}
	if err := worker.Manager.EnqueueLoad(ctx, model); err != nil {
		return PlacementResult{}, err
	}
	s := worker.Manager.Snapshot()
	if s.State != StateReady || s.Model != model {
		return PlacementResult{}, fmt.Errorf("modellife: load verification failed: worker=%s state=%s model=%s", worker.Label, s.State, s.Model)
	}
	return PlacementResult{Model: model, WorkerID: worker.ID, Worker: worker.Label, State: s.State, LoadGen: s.LoadGen}, nil
}

// BootstrapAll establishes the configured initial model on every managed
// worker that advertises it. This gives the mesh redundancy before later
// single-worker placement operations.
func (c *Coordinator) BootstrapAll(ctx context.Context, model string) error {
	c.mu.RLock()
	var managers []*WorkerController
	for _, w := range c.workers {
		if w.Manager != nil && w.Models[model] {
			managers = append(managers, w)
		}
	}
	c.mu.RUnlock()
	var errs []error
	for _, w := range managers {
		if err := w.Manager.EnqueueLoad(ctx, model); err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", w.Label, err))
		}
	}
	return errors.Join(errs...)
}

func (c *Coordinator) Evict(ctx context.Context, label string) error {
	c.mu.RLock()
	id, ok := c.labels[label]
	w := c.workers[id]
	c.mu.RUnlock()
	if !ok || w == nil {
		return fmt.Errorf("modellife: unknown worker %q", label)
	}
	if w.Manager == nil {
		return fmt.Errorf("modellife: worker %q is route-only", label)
	}
	return w.Manager.EnqueueEvict(ctx)
}

type WorkerLease struct {
	Lease  *Lease
	Worker int
	Model  string
}

func (l *WorkerLease) Release() {
	if l != nil && l.Lease != nil {
		l.Lease.Release()
		l.Lease = nil
	}
}

func (c *Coordinator) Acquire(ctx context.Context, workerID int, model string) (*WorkerLease, error) {
	c.mu.RLock()
	w := c.workers[workerID]
	c.mu.RUnlock()
	if w == nil || !w.Models[model] {
		return nil, fmt.Errorf("%w: %q", ErrUnknownModel, model)
	}
	if w.Manager == nil {
		if w.Resident != model {
			return nil, ErrNotReady
		}
		return &WorkerLease{Worker: workerID, Model: model}, nil
	}
	lease, err := w.Manager.Acquire(ctx)
	if err != nil {
		return nil, err
	}
	if lease.Model() != model {
		lease.Release()
		return nil, ErrNotReady
	}
	return &WorkerLease{Lease: lease, Worker: workerID, Model: model}, nil
}

func (c *Coordinator) Status() []WorkerStatus {
	c.mu.RLock()
	defer c.mu.RUnlock()
	out := make([]WorkerStatus, 0, len(c.workers))
	for _, w := range c.workers {
		models := make([]string, 0, len(w.Models))
		for m := range w.Models {
			models = append(models, m)
		}
		sort.Strings(models)
		st := WorkerStatus{WorkerID: w.ID, Worker: w.Label, Managed: w.Manager != nil, Models: models, Resident: w.Resident, State: StateReady}
		if w.Manager != nil {
			s := w.Manager.Snapshot()
			st.Resident, st.State, st.Inflight, st.QueueDepth, st.LastError, st.LoadGen = s.Model, s.State, s.Inflight, s.QueueDepth, s.LastError, s.LoadGen
		}
		out = append(out, st)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].WorkerID < out[j].WorkerID })
	return out
}

func (c *Coordinator) WorkerStatus(workerID int) (WorkerStatus, bool) {
	for _, status := range c.Status() {
		if status.WorkerID == workerID {
			return status, true
		}
	}
	return WorkerStatus{}, false
}

func (c *Coordinator) Manager(workerID int) *Manager {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if w := c.workers[workerID]; w != nil {
		return w.Manager
	}
	return nil
}

func (c *Coordinator) Events() map[string][]Event {
	c.mu.RLock()
	defer c.mu.RUnlock()
	out := map[string][]Event{}
	for _, w := range c.workers {
		if w.Manager != nil {
			out[w.Label] = w.Manager.Events()
		}
	}
	return out
}

func (c *Coordinator) Shutdown(ctx context.Context) error {
	c.mu.RLock()
	workers := make([]*WorkerController, 0, len(c.workers))
	for _, w := range c.workers {
		if w.Manager != nil {
			workers = append(workers, w)
		}
	}
	c.mu.RUnlock()
	var errs []error
	for _, w := range workers {
		if err := w.Manager.Shutdown(ctx); err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", w.Label, err))
		}
	}
	return errors.Join(errs...)
}

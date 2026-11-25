package adapt

import (
	"context"
	"errors"
	"sync/atomic"
	"time"

	"github.com/Priyanshuthapliyal2005/Liffo/internal/modellife"
)

// SharedPathID identifies the shared admission mechanism used by both entry points.
const SharedPathID = "gateway.BeginRequest"

// SessionError is a structured admission or lifecycle rejection from BeginRequest.
// Callers map it to Result (Handle) or HTTP status (HTTPServer) without re-implementing codes.
type SessionError struct {
	// Kind is "lifecycle" or "admit".
	Kind string
	// Code is stable machine code (admit_timeout, model_draining, ...).
	Code string
	// Class is coarse class (reject, cancel, lifecycle).
	Class string
	// Err is the underlying error (may be ctx.Err() or modellife sentinel).
	Err error
	// Life is set for lifecycle rejections.
	Life modellife.Snapshot
	// QueueWaitMs is set for admit rejections after waiting.
	QueueWaitMs float64
}

func (e *SessionError) Error() string {
	if e == nil || e.Err == nil {
		return "session error"
	}
	return e.Err.Error()
}

func (e *SessionError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}

// RequestSession is the shared admit + lifecycle lease for one request.
// Both in-process Handle and the HTTP endpoint hold one of these.
// Caller MUST Release (typically via defer).
type RequestSession struct {
	g *Gateway

	// PathID always SharedPathID when created by BeginRequest (tests assert this).
	PathID string
	// Lease is non-nil when modellife is wired and acquire succeeded.
	Lease *modellife.Lease
	// CoordLease is held for a specific model-aware worker.
	CoordLease *modellife.WorkerLease
	// QueueWaitMs time spent waiting for an admission slot.
	QueueWaitMs float64
	// InflightSeen gateway inflight count after taking the slot (includes this request).
	InflightSeen int
	// Model is the active model name at admit time.
	Model string

	admitted bool
	released atomic.Bool
}

// Release drops lifecycle lease (if any) and the admission slot.
// Safe to call multiple times.
func (s *RequestSession) Release() {
	if s == nil {
		return
	}
	if !s.released.CompareAndSwap(false, true) {
		return
	}
	if s.Lease != nil {
		s.Lease.Release()
		s.Lease = nil
	}
	if s.CoordLease != nil {
		s.CoordLease.Release()
		s.CoordLease = nil
	}
	if s.admitted {
		atomic.AddInt32(&s.g.inflight, -1)
		s.g.mu.Lock()
		s.g.cond.Broadcast()
		s.g.mu.Unlock()
		s.admitted = false
	}
}

// LifecycleHold reports whether a modellife lease is held.
func (s *RequestSession) LifecycleHold() bool {
	return s != nil && (s.Lease != nil || s.CoordLease != nil)
}

// BeginRequest admits an in-process request and acquires its lifecycle lease, if configured.
func (g *Gateway) BeginRequest(ctx context.Context) (*RequestSession, *SessionError) {
	return g.beginRequest(ctx, -1, "")
}

// BeginRequestFor holds lifecycle state for the selected worker/model.
func (g *Gateway) BeginRequestFor(ctx context.Context, workerID int, model string) (*RequestSession, *SessionError) {
	return g.beginRequest(ctx, workerID, model)
}

// BeginAdmission takes an admission slot. The HTTP path then reserves a worker
// and calls AttachLifecycle. This keeps queued requests out of routing load counts.
func (g *Gateway) BeginAdmission(ctx context.Context) (*RequestSession, *SessionError) {
	if ctx == nil {
		ctx = context.Background()
	}
	g.sharedPathHits.Add(1)
	wait0 := time.Now()
	if serr := g.awaitAdmission(ctx, wait0); serr != nil {
		g.rejected.Add(1)
		return nil, serr
	}
	return &RequestSession{
		g:            g,
		PathID:       SharedPathID,
		QueueWaitMs:  float64(time.Since(wait0).Seconds() * 1000),
		InflightSeen: int(atomic.LoadInt32(&g.inflight)),
		Model:        g.cfg.Model,
		admitted:     true,
	}, nil
}

// AttachLifecycle acquires the model-lifecycle lease for an already-admitted
// session. On failure the caller must still Release the session.
func (s *RequestSession) AttachLifecycle(ctx context.Context, workerID int, model string) *SessionError {
	if s == nil {
		return nil
	}
	g := s.g
	if g.coord != nil && workerID >= 0 {
		lease, err := g.coord.Acquire(ctx, workerID, model)
		if err != nil {
			return g.sessionLifeErr(err)
		}
		s.CoordLease = lease
	}
	if g.life != nil {
		lease, err := g.life.Acquire(ctx)
		if err != nil {
			if s.CoordLease != nil {
				s.CoordLease.Release()
				s.CoordLease = nil
			}
			return g.sessionLifeErr(err)
		}
		s.Lease = lease
	}
	if s.CoordLease != nil && s.CoordLease.Model != "" {
		s.Model = s.CoordLease.Model
	}
	if s.Lease != nil && s.Lease.Model() != "" {
		s.Model = s.Lease.Model()
	} else if g.life != nil {
		s.Model = g.ActiveModel()
	}
	return nil
}

func (g *Gateway) beginRequest(ctx context.Context, workerID int, model string) (*RequestSession, *SessionError) {
	if ctx == nil {
		ctx = context.Background()
	}
	g.sharedPathHits.Add(1)

	// Admission is taken BEFORE any model-lifecycle lease. Acquiring the lease
	// first made every queued request pin lifecycle state for the whole
	// AdmitTimeout, which blocked drain/evict behind backpressure.
	wait0 := time.Now()
	serr := g.awaitAdmission(ctx, wait0)
	if serr != nil {
		g.rejected.Add(1)
		return nil, serr
	}
	qWait := float64(time.Since(wait0).Seconds() * 1000)
	infNow := int(atomic.LoadInt32(&g.inflight))

	// Draining waits for the lease to be released.
	var lifeLease *modellife.Lease
	var coordLease *modellife.WorkerLease
	releaseSlot := func() {
		atomic.AddInt32(&g.inflight, -1)
		g.mu.Lock()
		g.cond.Broadcast()
		g.mu.Unlock()
	}
	if g.coord != nil && workerID >= 0 {
		lease, err := g.coord.Acquire(ctx, workerID, model)
		if err != nil {
			releaseSlot()
			return nil, g.sessionLifeErr(err)
		}
		coordLease = lease
	}
	if g.life != nil {
		lease, err := g.life.Acquire(ctx)
		if err != nil {
			if coordLease != nil {
				coordLease.Release()
			}
			releaseSlot()
			return nil, g.sessionLifeErr(err)
		}
		lifeLease = lease
	}

	modelName := g.cfg.Model
	if coordLease != nil && coordLease.Model != "" {
		modelName = coordLease.Model
	}
	if lifeLease != nil && lifeLease.Model() != "" {
		modelName = lifeLease.Model()
	} else if g.life != nil {
		modelName = g.ActiveModel()
	}

	return &RequestSession{
		g:            g,
		PathID:       SharedPathID,
		Lease:        lifeLease,
		CoordLease:   coordLease,
		QueueWaitMs:  qWait,
		InflightSeen: infNow,
		Model:        modelName,
		admitted:     true,
	}, nil
}

// awaitAdmission blocks until an in-flight slot is free, the AdmitTimeout
// elapses, or ctx is done. On success the in-flight counter has been
// incremented and the caller owns one slot.
//
// Waiters use g.cond. Helpers wake them on cancellation or deadline expiry.
// Once the admission queue is full, new waiters receive admit_queue_full.
func (g *Gateway) awaitAdmission(ctx context.Context, wait0 time.Time) *SessionError {
	pol := g.cfg.Initial
	if g.cfg.Mode == ModeAdaptive {
		pol = g.ctrl.Policy()
	}

	g.mu.Lock()
	if !policyEqual(pol, g.applied) {
		g.applyPolicy(pol)
	}
	maxInf := g.applied.MaxInflight
	if maxInf < 1 {
		maxInf = 1
	}
	if int(atomic.LoadInt32(&g.inflight)) < maxInf {
		atomic.AddInt32(&g.inflight, 1)
		g.mu.Unlock()
		return nil
	}

	maxWaiters := g.effectiveMaxAdmitWaiters(maxInf)
	if g.waiting >= maxWaiters {
		g.mu.Unlock()
		g.queueFullShed.Add(1)
		return &SessionError{
			Kind: "admit", Code: "admit_queue_full", Class: "reject",
			Err:         errors.New("admission queue full"),
			QueueWaitMs: float64(time.Since(wait0).Seconds() * 1000),
		}
	}

	var admitDeadline time.Time
	if g.cfg.AdmitTimeout > 0 {
		admitDeadline = wait0.Add(g.cfg.AdmitTimeout)
	}

	g.waiting++
	if int64(g.waiting) > g.peakWaiting.Load() {
		g.peakWaiting.Store(int64(g.waiting))
	}
	g.mu.Unlock()

	// Wake this waiter on ctx cancellation and on deadline expiry.
	done := make(chan struct{})
	defer close(done)
	go func() {
		select {
		case <-ctx.Done():
		case <-done:
			return
		}
		g.mu.Lock()
		g.cond.Broadcast()
		g.mu.Unlock()
	}()
	if !admitDeadline.IsZero() {
		go func() {
			t := time.NewTimer(time.Until(admitDeadline))
			defer t.Stop()
			select {
			case <-t.C:
			case <-done:
				return
			}
			g.mu.Lock()
			g.cond.Broadcast()
			g.mu.Unlock()
		}()
	}

	g.mu.Lock()
	defer g.mu.Unlock()
	for {
		if g.cfg.Mode == ModeAdaptive {
			p := g.ctrl.Policy()
			if !policyEqual(p, g.applied) {
				g.applyPolicy(p)
			}
		}
		maxInf = g.applied.MaxInflight
		if maxInf < 1 {
			maxInf = 1
		}
		if int(atomic.LoadInt32(&g.inflight)) < maxInf {
			atomic.AddInt32(&g.inflight, 1)
			g.waiting--
			return nil
		}
		if ctx.Err() != nil {
			g.waiting--
			return &SessionError{
				Kind: "admit", Code: "admit_canceled", Class: "cancel",
				Err:         ctx.Err(),
				QueueWaitMs: float64(time.Since(wait0).Seconds() * 1000),
			}
		}
		if !admitDeadline.IsZero() && !time.Now().Before(admitDeadline) {
			g.waiting--
			return &SessionError{
				Kind: "admit", Code: "admit_timeout", Class: "reject",
				Err:         errors.New("admission timeout"),
				QueueWaitMs: float64(time.Since(wait0).Seconds() * 1000),
			}
		}
		g.cond.Wait()
	}
}

func (g *Gateway) sessionLifeErr(err error) *SessionError {
	g.lifeReject.Add(1)
	g.rejected.Add(1)
	code := "model_not_ready"
	class := "lifecycle"
	st := modellife.Snapshot{}
	if g.life != nil {
		st = g.life.Snapshot()
	}
	switch {
	case errors.Is(err, modellife.ErrDraining):
		code = "model_draining"
	case errors.Is(err, modellife.ErrEvicting):
		code = "model_evicting"
	case errors.Is(err, modellife.ErrLoading):
		code = "model_loading"
	case errors.Is(err, modellife.ErrFailed):
		code = "model_failed"
	case errors.Is(err, modellife.ErrStopped):
		code = "model_stopped"
	}
	return &SessionError{
		Kind: "lifecycle", Code: code, Class: class, Err: err, Life: st,
	}
}

// SharedPathHits counts calls that enter shared admission.
func (g *Gateway) SharedPathHits() int64 { return g.sharedPathHits.Load() }

// RecordSample feeds the adaptive controller after a completed (non-rejected) request.
// Used by both Handle and the HTTP endpoint so mesh traffic trains the same controller.
func (g *Gateway) RecordSample(sample Sample) Snapshot {
	if g.cfg.Mode != ModeAdaptive {
		return Snapshot{}
	}
	snap := g.ctrl.Observe(sample)
	if snap.Switched {
		g.mu.Lock()
		g.applyPolicy(snap.Policy)
		g.mu.Unlock()
	}
	return snap
}

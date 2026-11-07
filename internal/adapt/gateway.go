package adapt

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Priyanshuthapliyal2005/Liffo/internal/config"
	"github.com/Priyanshuthapliyal2005/Liffo/internal/modellife"
	"github.com/Priyanshuthapliyal2005/Liffo/internal/route"
)

// Mode of the gateway control plane.
type Mode string

const (
	ModeStatic   Mode = "static"   // freeze initial policy
	ModeAdaptive Mode = "adaptive" // controller may change policy
)

// GatewayConfig wires mesh components.
type GatewayConfig struct {
	Mode    Mode
	Workers []string
	// StaticPolicy used when ModeStatic (and as adaptive initial).
	Initial Policy
	Targets Targets
	Filter  FilterConfig
	// Request timeout default if ctx has no deadline.
	DefaultTimeout time.Duration
	// AdmitTimeout: max wait for an inflight slot; 0 = wait until ctx done only.
	// On timeout, request is rejected (counts as rejected admission).
	AdmitTimeout time.Duration
	// MaxAdmitWaiters bounds how many requests may queue for an admission slot
	// at once. Beyond it, requests are shed immediately with admit_queue_full
	// instead of parking a goroutine + socket for the whole AdmitTimeout.
	// <=0 selects a default of 8x MaxInflight (floor 64).
	MaxAdmitWaiters int
	MaxTokens       int
	Model           string
	// LongPromptTokens threshold for sample flag.
	LongPromptTokens int
	MultiSLO         route.MultiSLOConfig
	WorkerStatic     []route.WorkerStatic

	// Lifecycle (optional): when set, requests require model ready;
	// load/evict jobs drain in-flight then switch process-backed models.
	Life *modellife.Manager
	// BootstrapModel: if Life is set and non-empty, load this model at NewGateway.
	BootstrapModel string
	// Coordinator is the production per-worker lifecycle placement layer.
	Coordinator *modellife.Coordinator
}

// Request is one completion (messages held only for HTTP; not logged raw).
type Request struct {
	ID                string
	Model             string
	Messages          []config.Message
	ContentForRouting string
	// PromptTokens estimate for control (optional).
	PromptTokens int
	MaxTokens    int
	Class        string // "interactive" | "offline" (audit only)
}

// Result is privacy-safe outcome + control audit.
type Result struct {
	ID        string  `json:"id"`
	OK        bool    `json:"ok"`
	TTFTMs    float64 `json:"ttft_ms"`
	LatencyMs float64 `json:"latency_ms"` // stream wall after admit
	// EndToEndMs = queue wait + stream latency (or admit-only on reject).
	EndToEndMs    float64     `json:"end_to_end_ms"`
	TokensOut     int         `json:"tokens_out"`
	Error         string      `json:"error,omitempty"`
	ErrorCode     string      `json:"error_code,omitempty"`
	ErrorClass    string      `json:"error_class,omitempty"`
	WorkerID      int         `json:"worker_id"`
	AttemptCount  int         `json:"attempt_count"`
	SafeRetryUsed bool        `json:"safe_retry_used"`
	FinalStage    route.Stage `json:"final_stage"`
	Policy        Policy      `json:"policy"`
	Control       Snapshot    `json:"control,omitempty"`
	Mode          string      `json:"mode"`
	PromptTokens  int         `json:"prompt_tokens_est"`
	// QueueWaitMs time spent waiting for admission slot.
	QueueWaitMs float64 `json:"queue_wait_ms"`
	// Rejected true if never executed (admission timeout / cancel while waiting).
	Rejected bool `json:"rejected"`
	// Model / lifecycle audit
	Model         string `json:"model,omitempty"`
	ModelState    string `json:"model_state,omitempty"`
	LifecycleHold bool   `json:"lifecycle_hold,omitempty"`
}

// Gateway integrates router + safe retry + admission (max inflight) + adaptive policy
// + optional model lifecycle (drain → evict → load).
type Gateway struct {
	cfg    GatewayConfig
	router *route.Router
	exec   *route.Executor
	ctrl   *Controller
	life   *modellife.Manager
	coord  *modellife.Coordinator

	mu       sync.Mutex
	inflight int32
	// cond signals admission-slot release. Waiters block on it instead of
	// polling; Release/Broadcast is the only wakeup source besides the
	// per-request deadline/ctx wakers registered in beginRequest.
	cond *sync.Cond
	// waiting is the number of requests currently blocked for an admission
	// slot. It is bounded by effectiveMaxAdmitWaiters so an overload sheds
	// immediately instead of accumulating goroutines and sockets.
	waiting int

	// policy applied flags
	applied Policy

	// counters (atomic)
	rejected atomic.Int64
	served   atomic.Int64
	// lifecycle rejects (not ready / draining / etc.)
	lifeReject atomic.Int64
	// sharedPathHits counts BeginRequest entries (Handle + HTTP both use it).
	sharedPathHits atomic.Int64
	// queueFullShed counts requests rejected because the admission queue was
	// already at MaxAdmitWaiters (fast shed, distinct from admit_timeout).
	queueFullShed atomic.Int64
	// peakWaiting is the high-water mark of concurrent admission waiters.
	peakWaiting atomic.Int64
}

// NewGateway builds workers and control plane.
func NewGateway(cfg GatewayConfig) (*Gateway, error) {
	if len(cfg.Workers) < 1 {
		return nil, fmt.Errorf("adapt: need workers")
	}
	if cfg.Mode == "" {
		cfg.Mode = ModeAdaptive
	}
	if cfg.DefaultTimeout <= 0 {
		cfg.DefaultTimeout = 60 * time.Second
	}
	if cfg.MaxTokens <= 0 {
		cfg.MaxTokens = 32
	}
	if cfg.Model == "" {
		cfg.Model = "local"
	}
	if cfg.LongPromptTokens <= 0 {
		cfg.LongPromptTokens = 128
	}
	if cfg.Initial.RouteMethod == "" {
		cfg.Initial = StaticPolicy(route.MethodHybrid, 2, true)
	}

	ws := make([]*route.Worker, len(cfg.Workers))
	for i, u := range cfg.Workers {
		ws[i] = &route.Worker{ID: i, BaseURL: u, MaxInflight: cfg.Initial.MaxInflight}
	}
	rtr, err := route.New(route.Config{
		Method:       cfg.Initial.RouteMethod,
		FailCooldown: 3 * time.Second,
		PrefixBytes:  256,
	}, ws)
	if err != nil {
		return nil, err
	}
	// Capacity-aware policies read capacity/service estimates from multi state.
	// Previously only multi_slo (or an explicit SLO target) enabled it, so
	// capacity_normalized_least_inflight and shortest_estimated_workload
	// silently degraded to plain least_inflight with capacity=1 everywhere.
	if route.NeedsWorkerStatic(cfg.Initial.RouteMethod) || cfg.MultiSLO.SLOTargetMs > 0 || len(cfg.WorkerStatic) > 0 {
		rtr.EnableMulti(cfg.MultiSLO, cfg.WorkerStatic)
	}
	ex := route.NewExecutor(rtr)
	ex.Retry = route.RetryConfig{Enabled: cfg.Initial.RetryEnabled, MaxAttempts: len(ws)}

	ctrl := NewController(cfg.Targets, cfg.Filter, cfg.Initial)
	g := &Gateway{
		cfg:     cfg,
		router:  rtr,
		exec:    ex,
		ctrl:    ctrl,
		life:    cfg.Life,
		coord:   cfg.Coordinator,
		applied: cfg.Initial,
	}
	g.cond = sync.NewCond(&g.mu)
	g.mu.Lock()
	g.applyPolicy(cfg.Initial)
	g.mu.Unlock()
	if g.coord != nil {
		for _, w := range g.router.Workers() {
			worker := w
			g.coord.SetRuntime(worker.ID, func() modellife.RuntimeState {
				return modellife.RuntimeState{
					Healthy:  worker.Healthy(),
					Inflight: worker.InFlight(),
				}
			})
		}
	}

	// Bootstrap resident model when lifecycle is enabled.
	if g.life != nil && cfg.BootstrapModel != "" {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
		err := g.life.EnqueueLoad(ctx, cfg.BootstrapModel)
		cancel()
		if err != nil {
			return nil, fmt.Errorf("adapt: bootstrap model %q: %w", cfg.BootstrapModel, err)
		}
		g.cfg.Model = cfg.BootstrapModel
	}
	if g.coord != nil && cfg.BootstrapModel != "" {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
		err := g.coord.BootstrapAll(ctx, cfg.BootstrapModel)
		cancel()
		if err != nil {
			return nil, fmt.Errorf("adapt: bootstrap placement %q: %w", cfg.BootstrapModel, err)
		}
		g.cfg.Model = cfg.BootstrapModel
	}
	return g, nil
}

// Controller exposes the adapt controller (for history dump).
func (g *Gateway) Controller() *Controller { return g.ctrl }

// Router exposes router (tests / kill experiments).
func (g *Gateway) Router() *route.Router { return g.router }

// Life returns the model lifecycle manager (may be nil).
func (g *Gateway) Life() *modellife.Manager { return g.life }

func (g *Gateway) Coordinator() *modellife.Coordinator { return g.coord }

// ActiveModel returns lifecycle model if wired, else config model.
func (g *Gateway) ActiveModel() string {
	if g.life != nil {
		s := g.life.Snapshot()
		if s.Model != "" {
			return s.Model
		}
	}
	if g.coord != nil {
		for _, s := range g.coord.Status() {
			if s.State == modellife.StateReady && s.Resident != "" {
				return s.Resident
			}
		}
	}
	return g.cfg.Model
}

// ModelSnapshot returns lifecycle snapshot or a synthetic ready/stopped view.
func (g *Gateway) ModelSnapshot() modellife.Snapshot {
	if g.life != nil {
		return g.life.Snapshot()
	}
	if g.coord != nil {
		for _, s := range g.coord.Status() {
			if s.State == modellife.StateReady && s.Resident != "" {
				return modellife.Snapshot{
					State: s.State, Model: s.Resident, Inflight: s.Inflight,
					QueueDepth: s.QueueDepth, LastError: s.LastError, LoadGen: s.LoadGen,
				}
			}
		}
		return modellife.Snapshot{State: modellife.StateStopped}
	}
	return modellife.Snapshot{State: modellife.StateReady, Model: g.cfg.Model}
}

// LifeRejectCount is how many requests were denied for model lifecycle state.
func (g *Gateway) LifeRejectCount() int64 { return g.lifeReject.Load() }

// lifeRejectResult builds a rejected Result for lifecycle admission failure.
func (g *Gateway) lifeRejectResult(req Request, err error) Result {
	g.lifeReject.Add(1)
	g.rejected.Add(1)
	code := "model_not_ready"
	class := "reject"
	st := ""
	if g.life != nil {
		st = string(g.life.Snapshot().State)
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
	return Result{
		ID: req.ID, OK: false, Rejected: true,
		Error: err.Error(), ErrorClass: class, ErrorCode: code,
		Policy: g.appliedPolicy(), Mode: string(g.cfg.Mode),
		Model: g.ActiveModel(), ModelState: st,
	}
}

// effectiveMaxAdmitWaiters resolves the waiter bound. Caller holds g.mu.
func (g *Gateway) effectiveMaxAdmitWaiters(maxInflight int) int {
	if g.cfg.MaxAdmitWaiters > 0 {
		return g.cfg.MaxAdmitWaiters
	}
	n := maxInflight * 8
	if n < 64 {
		n = 64
	}
	return n
}

// appliedMaxInflight reads the admission capacity under g.mu. Callers outside
// this package must not touch g.applied directly (data race with applyPolicy).
func (g *Gateway) appliedMaxInflight() int {
	g.mu.Lock()
	n := g.applied.MaxInflight
	g.mu.Unlock()
	if n < 1 {
		n = 1
	}
	return n
}

// applyPolicy updates policy fields while g.mu is held by the caller.
func (g *Gateway) applyPolicy(p Policy) {
	g.router.SetMethod(p.RouteMethod)
	g.applied = p
}

func (g *Gateway) appliedPolicy() Policy {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.applied
}

// Handle admits (via shared BeginRequest), routes, streams with safe retry, then feeds the controller.
func (g *Gateway) Handle(ctx context.Context, req Request) Result {
	if ctx == nil {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(context.Background(), g.cfg.DefaultTimeout)
		defer cancel()
	}
	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, g.cfg.DefaultTimeout)
		defer cancel()
	}

	content := req.ContentForRouting
	if content == "" && len(req.Messages) > 0 {
		content = req.Messages[len(req.Messages)-1].Content
	}
	model := req.Model
	if model == "" {
		model = g.ActiveModel()
	}
	var allowed map[int]bool
	var sess *RequestSession
	var serr *SessionError
	if g.coord != nil {
		eligible := g.coord.ReadyWorkers(model)
		dec := g.router.PickEligible(content, eligible)
		if dec.WorkerID < 0 {
			return g.lifeRejectResult(req, modellife.ErrNotReady)
		}
		sess, serr = g.BeginRequestFor(ctx, dec.WorkerID, model)
		allowed = map[int]bool{dec.WorkerID: true}
	} else {
		sess, serr = g.BeginRequest(ctx)
	}
	if serr != nil {
		return g.resultFromSessionErr(req, serr)
	}
	defer sess.Release()

	mt := req.MaxTokens
	if mt <= 0 {
		mt = g.cfg.MaxTokens
	}

	rcfg := g.exec.Retry
	if g.cfg.Mode == ModeStatic {
		rcfg.Enabled = g.cfg.Initial.RetryEnabled
	} else {
		rcfg.Enabled = g.appliedPolicy().RetryEnabled
	}
	rcfg.MaxAttempts = len(g.router.Workers())

	modelName := sess.Model
	streamRes := g.exec.ExecuteWithRetry(ctx, route.StreamRequest{
		Messages:          req.Messages,
		Model:             modelName,
		MaxTokens:         mt,
		Temperature:       0,
		ContentForRouting: content,
		AllowedWorkers:    allowed,
	}, rcfg)

	// Count healthy workers
	healthy, total := 0, 0
	for _, w := range g.router.Workers() {
		total++
		if w.Healthy() {
			healthy++
		}
	}

	g.served.Add(1)
	out := Result{
		ID:            req.ID,
		OK:            streamRes.Error == "",
		TTFTMs:        streamRes.TTFTMs,
		LatencyMs:     streamRes.LatencyMs,
		EndToEndMs:    sess.QueueWaitMs + streamRes.LatencyMs,
		TokensOut:     streamRes.TokensOut,
		Error:         streamRes.Error,
		ErrorCode:     streamRes.ErrorCode,
		ErrorClass:    streamRes.ErrorClass,
		WorkerID:      streamRes.Decision.WorkerID,
		AttemptCount:  streamRes.AttemptCount,
		SafeRetryUsed: streamRes.SafeRetryUsed,
		FinalStage:    streamRes.FinalStage,
		Policy:        g.appliedPolicy(),
		Mode:          string(g.cfg.Mode),
		PromptTokens:  req.PromptTokens,
		QueueWaitMs:   sess.QueueWaitMs,
		Rejected:      false,
		Model:         modelName,
		LifecycleHold: sess.LifecycleHold(),
	}
	if g.life != nil {
		out.ModelState = string(g.life.Snapshot().State)
	}

	sample := Sample{
		TTFTMs:         streamRes.TTFTMs,
		LatencyMs:      streamRes.LatencyMs,
		Error:          streamRes.Error != "",
		InflightSeen:   sess.InflightSeen,
		PromptTokens:   req.PromptTokens,
		HealthyWorkers: healthy,
		TotalWorkers:   total,
	}

	if g.cfg.Mode == ModeAdaptive {
		snap := g.RecordSample(sample)
		out.Control = snap
		out.Policy = g.ctrl.Policy()
	}

	return out
}

// resultFromSessionErr maps shared BeginRequest failures to privacy-safe Results.
func (g *Gateway) resultFromSessionErr(req Request, serr *SessionError) Result {
	if serr == nil {
		return Result{ID: req.ID, OK: false, Rejected: true, Error: "unknown session error"}
	}
	if serr.Kind == "lifecycle" {
		// Counters already bumped in sessionLifeErr; build Result matching lifeRejectResult.
		st := string(serr.Life.State)
		return Result{
			ID: req.ID, OK: false, Rejected: true,
			Error: serr.Error(), ErrorClass: "reject", ErrorCode: serr.Code,
			Policy: g.appliedPolicy(), Mode: string(g.cfg.Mode),
			Model: g.ActiveModel(), ModelState: st,
		}
	}
	return Result{
		ID: req.ID, OK: false, Rejected: true,
		Error: serr.Error(), ErrorClass: serr.Class, ErrorCode: serr.Code,
		Policy: g.appliedPolicy(), Mode: string(g.cfg.Mode),
		QueueWaitMs: serr.QueueWaitMs, EndToEndMs: serr.QueueWaitMs,
	}
}

// HealthyCount returns healthy/total workers.
func (g *Gateway) HealthyCount() (healthy, total int) {
	for _, w := range g.router.Workers() {
		total++
		if w.Healthy() {
			healthy++
		}
	}
	return healthy, total
}

// RejectedCount returns admission rejects.
func (g *Gateway) QueueFullShedCount() int64 { return g.queueFullShed.Load() }

// PeakAdmitWaiters returns the high-water mark of concurrent admission waiters.
func (g *Gateway) PeakAdmitWaiters() int64 { return g.peakWaiting.Load() }

func (g *Gateway) RejectedCount() int64 { return g.rejected.Load() }

// ServedCount returns executed (non-rejected) requests.
func (g *Gateway) ServedCount() int64 { return g.served.Load() }

// Inflight returns current admitted in-flight count.
func (g *Gateway) Inflight() int { return int(atomic.LoadInt32(&g.inflight)) }

// RestoreWorkerURL sets worker base URL (soak recovery experiments) and clears unhealthy.
func (g *Gateway) RestoreWorkerURL(id int, baseURL string) {
	for _, w := range g.router.Workers() {
		if w.ID == id {
			w.BaseURL = baseURL
			w.MarkOK()
			return
		}
	}
}

// PoisonWorkerURL soft-disconnects a worker by pointing BaseURL at a dead address.
// It does NOT pre-mark the worker unhealthy — the router must observe real dial/stream
// failures via Release(false). Prefer killing the real worker process when one is owned
// (see killLocalWorkerProcess / chaos killPID); use this only when process kill is unavailable.
func (g *Gateway) PoisonWorkerURL(id int) {
	g.PoisonWorkerURLFor(id, 0)
}

// PoisonWorkerURLFor is PoisonWorkerURL. cooldown is retained for API compatibility but
// is not used to MarkFail (pre-marking skipped the real failure path).
func (g *Gateway) PoisonWorkerURLFor(id int, cooldown time.Duration) {
	_ = cooldown
	for _, w := range g.router.Workers() {
		if w.ID == id {
			w.BaseURL = "http://127.0.0.1:1"
			// deliberately no MarkFail — unhealthy only after real request failures
			return
		}
	}
}

// WorkerStillHealthy reports router healthy flag for id (used by proofs that must not pre-mark).
func (g *Gateway) WorkerStillHealthy(id int) bool {
	for _, w := range g.router.Workers() {
		if w.ID == id {
			return w.Healthy()
		}
	}
	return false
}

// ResetBetweenCycles restores control-plane + worker runtime for cycle isolation:
// controller EMAs/policy, worker health/inflight, applied admission policy.
// Does not cancel in-flight HTTP; caller should drain first.
func (g *Gateway) ResetBetweenCycles(initial Policy) {
	if initial.RouteMethod == "" {
		initial = g.cfg.Initial
	}
	if g.ctrl != nil {
		g.ctrl.Reset(initial)
	}
	g.router.ResetAllWorkers()
	g.mu.Lock()
	g.applyPolicy(initial)
	g.mu.Unlock()
}

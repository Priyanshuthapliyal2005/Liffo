// Package route implements multi-worker request routing for local llama-server
// instances. It does NOT implement model-level continuous batching or KV fusion.
//
// Privacy: only SHA-256 hashes of prompt prefixes are retained — never raw prompts.
// Workers are never restarted by this package; unhealthy peers are skipped until
// a cooldown elapses (caller may restart workers explicitly for experiments).
package route

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"hash/fnv"
	"math"
	"sync"
	"sync/atomic"
	"time"
)

// Method is a routing policy name.
type Method string

const (
	MethodRoundRobin    Method = "round_robin"
	MethodLeastInflight Method = "least_inflight"
	MethodPrefixSticky  Method = "prefix_sticky"
	MethodRandomHash    Method = "random_hash" // hash full prompt; no stickiness learning
	// MethodHybrid scores each worker: cache affinity + load + cache age + health.
	MethodHybrid Method = "hybrid"
	// MethodMultiSLO: cost + latency EMA + load + SLO headroom.
	MethodMultiSLO Method = "multi_slo"
	// MethodCapacityNormalizedLeastInflight minimizes inflight / static capacity.
	MethodCapacityNormalizedLeastInflight Method = "capacity_normalized_least_inflight"
	// MethodShortestEstimatedWorkload minimizes estimated queued service time.
	MethodShortestEstimatedWorkload Method = "shortest_estimated_workload"
)

// Worker is one local inference endpoint.
type Worker struct {
	ID      int
	BaseURL string
	// MaxInflight soft cap; 0 = unlimited. Sticky routes may fall back when exceeded.
	MaxInflight int

	inflight atomic.Int64
	fails    atomic.Int64
	// unhealthyUntil unix nano; 0 = healthy
	unhealthyUntil atomic.Int64
	// completed requests routed here
	served atomic.Int64
}

// InFlight returns current in-flight count.
func (w *Worker) InFlight() int64 { return w.inflight.Load() }

// Served returns completed count.
func (w *Worker) Served() int64 { return w.served.Load() }

// Healthy reports whether the worker is within cooldown after failures.
func (w *Worker) Healthy() bool {
	u := w.unhealthyUntil.Load()
	if u == 0 {
		return true
	}
	return time.Now().UnixNano() >= u
}

func (w *Worker) markFail(cooldown time.Duration) {
	w.fails.Add(1)
	if cooldown <= 0 {
		cooldown = 3 * time.Second
	}
	w.unhealthyUntil.Store(time.Now().Add(cooldown).UnixNano())
}

func (w *Worker) markOK() {
	w.unhealthyUntil.Store(0)
}

// MarkOK clears unhealthy cooldown (explicit recovery experiments).
func (w *Worker) MarkOK() { w.markOK() }

// MarkFail marks worker unhealthy for cooldown (chaos / obs failure experiments).
func (w *Worker) MarkFail(cooldown time.Duration) { w.markFail(cooldown) }

// ResetRuntime clears inflight/fails/unhealthy — cycle isolation for endurance chaos.
// Does not change ID/BaseURL/MaxInflight.
func (w *Worker) ResetRuntime() {
	w.inflight.Store(0)
	w.fails.Store(0)
	w.unhealthyUntil.Store(0)
}

// Config for Router.
type Config struct {
	Method Method
	// PrefixBytes: number of leading content bytes hashed for sticky routing (default 256).
	PrefixBytes int
	// MaxStickyEntries caps prefix→worker map size (default 1024). Never stores raw text.
	MaxStickyEntries int
	// SoftOverload: if sticky worker inflight >= MaxInflight, fall back to least_inflight.
	// FailCooldown after a request error (default 3s). Does not restart the process.
	FailCooldown time.Duration
	// Hybrid scoring weights (normalized at pick time if sum > 0). Defaults in Defaults().
	WeightCache  float64 // prefix affinity on this worker
	WeightLoad   float64 // lower inflight → higher score
	WeightAge    float64 // fresher cache hit → higher score
	WeightHealth float64 // healthy workers preferred
	// CacheAgeHalfLife: age score halves every this duration (default 5s).
	CacheAgeHalfLife time.Duration
}

// Defaults returns safe defaults.
func Defaults() Config {
	return Config{
		Method:           MethodPrefixSticky,
		PrefixBytes:      256,
		MaxStickyEntries: 1024,
		FailCooldown:     3 * time.Second,
		WeightCache:      0.35,
		WeightLoad:       0.35,
		WeightAge:        0.15,
		WeightHealth:     0.15,
		CacheAgeHalfLife: 5 * time.Second,
	}
}

// WorkerScore is the privacy-safe score breakdown for one candidate worker.
type WorkerScore struct {
	WorkerID    int     `json:"worker_id"`
	Total       float64 `json:"total"`
	Cache       float64 `json:"cache"`             // 0..1 affinity component (or cost in multi_slo)
	Load        float64 `json:"load"`              // 0..1 load component (pre-weight)
	Age         float64 `json:"age"`               // 0..1 freshness (or latency in multi_slo)
	Health      float64 `json:"health"`            // 0 or 1 (or SLO in multi_slo)
	Quality     float64 `json:"quality,omitempty"` // 0..1 quality component (multi_slo)
	Inflight    int64   `json:"inflight"`
	Healthy     bool    `json:"healthy"`
	HasAffinity bool    `json:"has_affinity"`
	CacheAgeSec float64 `json:"cache_age_sec"` // -1 if no affinity
	// Weighted contributions (weight * component) for audit.
	WCache  float64 `json:"w_cache"`
	WLoad   float64 `json:"w_load"`
	WAge    float64 `json:"w_age"`
	WHealth float64 `json:"w_health"`
	// Multi-SLO only: an uncalibrated EMA-based estimate captured at the
	// decision snapshot. It deliberately excludes raw prompt content.
	EstimatedTTFTMs float64 `json:"estimated_ttft_ms,omitempty"`
	HardRejected    bool    `json:"hard_rejected,omitempty"`
	RejectReason    string  `json:"reject_reason,omitempty"`
}

// Decision is a routing choice (privacy-safe).
type Decision struct {
	WorkerID  int    `json:"worker_id"`
	BaseURL   string `json:"base_url"`
	Method    string `json:"method"`
	PrefixSHA string `json:"prefix_sha256"` // empty if method does not use prefix
	PromptSHA string `json:"prompt_sha256"`
	StickyHit bool   `json:"sticky_hit"`
	Fallback  string `json:"fallback,omitempty"` // e.g. "overload", "unhealthy", "cold"
	Inflight  int64  `json:"worker_inflight"`
	// Reason is a short human-readable explanation of why this worker won.
	Reason string `json:"reason,omitempty"`
	// AffinityWorkerID is the worker that currently holds prefix affinity (-1 if none).
	AffinityWorkerID int `json:"affinity_worker_id"`
	// SwitchedAway is true when an affinity worker existed but a different worker won
	// (hybrid load/health/age beat pure cache stickiness).
	SwitchedAway bool `json:"switched_away_from_cache"`
	// Scores is the full candidate breakdown (hybrid; optional for other methods).
	Scores []WorkerScore `json:"scores,omitempty"`
	// ChosenScore is the winning total (hybrid).
	ChosenScore float64 `json:"chosen_score,omitempty"`
	// Multi-SLO only: uncalibrated EMA-based estimate and the feasible workers
	// captured with the decision. Detailed candidate telemetry is exported only
	// through server-side traces, not public response bodies.
	EstimatedTTFTMs float64 `json:"estimated_ttft_ms,omitempty"`
	EligibleWorkers []int   `json:"eligible_workers,omitempty"`
	RejectionCode   string  `json:"rejection_code,omitempty"`
}

// stickyEntry maps a prefix hash to a worker (no raw text).
type stickyEntry struct {
	workerID int
	lastUsed time.Time
	hits     int
}

// Router assigns requests to workers.
type Router struct {
	cfg        Config
	workers    []*Worker
	mu         sync.Mutex
	decisionMu sync.Mutex
	rr         uint64
	sticky     map[string]stickyEntry
	multi      *multiState
}

// New builds a Router. workers must be non-empty.
func New(cfg Config, workers []*Worker) (*Router, error) {
	if len(workers) == 0 {
		return nil, fmt.Errorf("route: at least one worker required")
	}
	if cfg.PrefixBytes <= 0 {
		cfg.PrefixBytes = 256
	}
	if cfg.MaxStickyEntries <= 0 {
		cfg.MaxStickyEntries = 1024
	}
	if cfg.FailCooldown <= 0 {
		cfg.FailCooldown = 3 * time.Second
	}
	if cfg.Method == "" {
		cfg.Method = MethodPrefixSticky
	}
	if cfg.WeightCache == 0 && cfg.WeightLoad == 0 && cfg.WeightAge == 0 && cfg.WeightHealth == 0 {
		cfg.WeightCache = 0.35
		cfg.WeightLoad = 0.35
		cfg.WeightAge = 0.15
		cfg.WeightHealth = 0.15
	}
	if cfg.CacheAgeHalfLife <= 0 {
		cfg.CacheAgeHalfLife = 5 * time.Second
	}
	cp := make([]*Worker, len(workers))
	copy(cp, workers)
	return &Router{
		cfg:     cfg,
		workers: cp,
		sticky:  make(map[string]stickyEntry),
	}, nil
}

// Workers returns the worker list (read-only use).
func (r *Router) Workers() []*Worker { return r.workers }

// Method returns the current routing method (thread-safe).
func (r *Router) Method() Method {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.cfg.Method
}

// SetMethod updates the routing method at runtime (adaptive control plane).
// Does not restart workers or clear sticky map.
func (r *Router) SetMethod(m Method) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if m == "" {
		return
	}
	r.cfg.Method = m
}

// SetFailCooldown updates unhealthy cooldown after failures.
func (r *Router) SetFailCooldown(d time.Duration) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if d > 0 {
		r.cfg.FailCooldown = d
	}
}

// PrefixSHA256 hashes the leading PrefixBytes of content (never stores content).
func PrefixSHA256(content string, n int) string {
	if n <= 0 {
		n = 256
	}
	b := []byte(content)
	if len(b) > n {
		b = b[:n]
	}
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

// PromptSHA256 hashes full content.
func PromptSHA256(content string) string {
	sum := sha256.Sum256([]byte(content))
	return hex.EncodeToString(sum[:])
}

// PickAndAcquire atomically reserves the selected worker for a request. Gateway
// handlers must use this instead of a separate Pick then Acquire sequence so
// concurrent requests see previous reservations in their routing snapshots.
func (r *Router) PickAndAcquire(content string) Decision {
	r.decisionMu.Lock()
	defer r.decisionMu.Unlock()
	d := r.Pick(content)
	if d.WorkerID >= 0 && d.BaseURL != "" {
		r.Acquire(d.WorkerID)
	}
	return d
}

// PickWithConstraintsAndAcquire is PickWithConstraints with an atomic worker reservation.
func (r *Router) PickWithConstraintsAndAcquire(content string, rc *RouteConstraints) Decision {
	r.decisionMu.Lock()
	defer r.decisionMu.Unlock()
	d := r.PickWithConstraints(content, rc)
	if d.WorkerID >= 0 && d.BaseURL != "" {
		r.Acquire(d.WorkerID)
	}
	return d
}

// PickEligibleWithConstraintsAndAcquire applies lifecycle eligibility and atomically reserves the winner.
func (r *Router) PickEligibleWithConstraintsAndAcquire(content string, eligible map[int]bool, rc *RouteConstraints) Decision {
	r.decisionMu.Lock()
	defer r.decisionMu.Unlock()
	d := r.PickEligibleWithConstraints(content, eligible, rc)
	if d.WorkerID >= 0 && d.BaseURL != "" {
		r.Acquire(d.WorkerID)
	}
	return d
}

// Pick selects a worker for the given prompt content.
// content is used only ephemerally to compute hashes; it is not stored.
func (r *Router) Pick(content string) Decision {
	method := r.Method() // snapshot mutable policy under the router lock
	prefixSHA := PrefixSHA256(content, r.cfg.PrefixBytes)
	promptSHA := PromptSHA256(content)
	d := Decision{
		Method:           string(method),
		PrefixSHA:        prefixSHA,
		PromptSHA:        promptSHA,
		AffinityWorkerID: -1,
	}

	switch method {
	case MethodRoundRobin:
		w := r.pickRoundRobin()
		hc := 0
		for _, ww := range r.workers {
			if ww.Healthy() {
				hc++
			}
		}
		r.fill(&d, w, false, "", fmt.Sprintf("round_robin chose worker %d (healthy_pool=%d/%d)", w.ID, hc, len(r.workers)))
	case MethodLeastInflight:
		w := r.pickLeastInflight(nil)
		r.fill(&d, w, false, "", fmt.Sprintf("least_inflight chose worker %d inflight=%d", w.ID, w.InFlight()))
	case MethodRandomHash:
		w := r.pickByHash(promptSHA)
		r.fill(&d, w, false, "", fmt.Sprintf("random_hash chose worker %d from prompt_sha", w.ID))
	case MethodPrefixSticky:
		r.pickSticky(content, prefixSHA, &d)
	case MethodHybrid:
		r.pickHybrid(prefixSHA, &d)
	case MethodMultiSLO:
		r.pickMultiSLO(&d, nil, nil)
	case MethodCapacityNormalizedLeastInflight:
		r.pickCapacityNormalizedLeastInflight(&d, nil)
	case MethodShortestEstimatedWorkload:
		r.pickShortestEstimatedWorkload(&d, nil)
	default:
		w := r.pickLeastInflight(nil)
		d.Method = string(MethodLeastInflight)
		r.fill(&d, w, false, "unknown_method", "unknown method → least_inflight")
	}
	return d
}

// PickWithConstraints evaluates the request with explicit routing constraints (cost, latency, quality).
func (r *Router) PickWithConstraints(content string, rc *RouteConstraints) Decision {
	method := r.Method()
	if method != MethodMultiSLO {
		return r.Pick(content)
	}
	prefixSHA := PrefixSHA256(content, r.cfg.PrefixBytes)
	promptSHA := PromptSHA256(content)
	d := Decision{
		Method:           string(method),
		PrefixSHA:        prefixSHA,
		PromptSHA:        promptSHA,
		AffinityWorkerID: -1,
	}
	r.pickMultiSLO(&d, rc, nil)
	return d
}

// PickEligible preserves the configured routing decision when it points at an
// eligible model replica; otherwise it selects the least-loaded eligible
// healthy worker. The caller supplies lifecycle/model eligibility.
func (r *Router) PickEligible(content string, eligible map[int]bool) Decision {
	return r.PickEligibleWithConstraints(content, eligible, nil)
}

// PickEligibleWithConstraints scores only model-ready workers for multi_slo.
// Infeasible quality/cost/model constraints are rejected instead of downgraded.
func (r *Router) PickEligibleWithConstraints(content string, eligible map[int]bool, rc *RouteConstraints) Decision {
	if r.Method() == MethodMultiSLO && r.multi != nil {
		d := Decision{Method: string(MethodMultiSLO), PrefixSHA: PrefixSHA256(content, r.cfg.PrefixBytes), PromptSHA: PromptSHA256(content), AffinityWorkerID: -1}
		r.pickMultiSLO(&d, rc, eligible)
		return d
	}
	dec := r.Pick(content)
	if eligible[dec.WorkerID] {
		if w := r.workerByID(dec.WorkerID); w != nil && w.Healthy() {
			return dec
		}
	}
	var best *Worker
	for _, w := range r.workers {
		if !eligible[w.ID] || !w.Healthy() {
			continue
		}
		if best == nil || w.InFlight() < best.InFlight() || (w.InFlight() == best.InFlight() && w.ID < best.ID) {
			best = w
		}
	}
	if best == nil {
		dec.BaseURL = ""
		dec.WorkerID = -1
		dec.Fallback = "model_unavailable"
		dec.Reason = "no healthy worker has the requested model ready"
		return dec
	}
	r.fill(&dec, best, false, "model_placement", fmt.Sprintf("model-aware fallback chose eligible worker %d inflight=%d", best.ID, best.InFlight()))
	return dec
}

func (r *Router) fill(d *Decision, w *Worker, hit bool, fb, reason string) {
	if w == nil {
		// last resort: first worker
		w = r.workers[0]
		fb = "empty_pool"
		reason = "empty_pool fallback worker 0"
	}
	d.WorkerID = w.ID
	d.BaseURL = w.BaseURL
	d.StickyHit = hit
	d.Fallback = fb
	d.Inflight = w.InFlight()
	d.Reason = reason
}

func (r *Router) pickSticky(_ string, prefixSHA string, d *Decision) {
	r.mu.Lock()
	ent, ok := r.sticky[prefixSHA]
	if ok {
		ent.lastUsed = time.Now()
		r.sticky[prefixSHA] = ent
	}
	r.mu.Unlock()

	if ok {
		d.AffinityWorkerID = ent.workerID
		w := r.workerByID(ent.workerID)
		if w != nil && w.Healthy() && !r.overloaded(w) {
			r.fill(d, w, true, "", fmt.Sprintf(
				"sticky_hit worker=%d affinity=true inflight=%d healthy=true",
				w.ID, w.InFlight()))
			return
		}
		// Prefer unhealthy over overload when both apply (health is harder fail).
		fb := "unhealthy"
		if w != nil && w.Healthy() && r.overloaded(w) {
			fb = "overload"
		} else if w != nil && !w.Healthy() {
			fb = "unhealthy"
		} else if w != nil && r.overloaded(w) {
			fb = "overload"
		}
		// Fall back without deleting sticky binding permanently if only overload;
		// still route elsewhere this request.
		alt := r.pickLeastInflight(map[int]bool{})
		if w != nil && !w.Healthy() {
			// drop sticky to dead worker
			r.mu.Lock()
			delete(r.sticky, prefixSHA)
			r.mu.Unlock()
		}
		d.SwitchedAway = alt != nil && alt.ID != ent.workerID
		r.fill(d, alt, false, fb, fmt.Sprintf(
			"sticky_fallback reason=%s affinity_worker=%d chose=%d switched_away=%v inflight_aff=%v",
			fb, ent.workerID, alt.ID, d.SwitchedAway, func() int64 {
				if w != nil {
					return w.InFlight()
				}
				return -1
			}()))
		// rebind sticky to chosen healthy worker for future hits
		if alt != nil && alt.Healthy() {
			r.bindSticky(prefixSHA, alt.ID)
		}
		return
	}

	// Cold prefix: stable placement by prefix hash so distinct prefixes fan out
	// across workers (least_inflight alone always ties to worker 0 when idle).
	w := r.pickByHash(prefixSHA)
	if w == nil || !w.Healthy() || r.overloaded(w) {
		w = r.pickLeastInflight(nil)
	}
	r.fill(d, w, false, "cold", fmt.Sprintf(
		"sticky_cold prefix_hash_place worker=%d inflight=%d", w.ID, w.InFlight()))
	if w != nil {
		r.bindSticky(prefixSHA, w.ID)
	}
}

// pickHybrid scores every worker and picks the max total.
// Components (each 0..1 before weights):
//   - cache: 1 if this worker holds prefix affinity, else 0
//   - load:  1/(1+inflight)  (and 0 if at/over MaxInflight when set)
//   - age:   exp2(-age/halfLife) when affinity, else 0
//   - health: 1 if healthy, else 0
//
// Logs full score table + reason when affinity is abandoned for load/health/age.
func (r *Router) pickHybrid(prefixSHA string, d *Decision) {
	now := time.Now()
	r.mu.Lock()
	ent, hasAff := r.sticky[prefixSHA]
	r.mu.Unlock()
	if hasAff {
		d.AffinityWorkerID = ent.workerID
	}

	wc, wl, wa, wh := r.cfg.WeightCache, r.cfg.WeightLoad, r.cfg.WeightAge, r.cfg.WeightHealth
	sumW := wc + wl + wa + wh
	if sumW <= 0 {
		sumW = 1
	}
	// normalize weights
	wc, wl, wa, wh = wc/sumW, wl/sumW, wa/sumW, wh/sumW
	half := r.cfg.CacheAgeHalfLife

	scores := make([]WorkerScore, 0, len(r.workers))
	var best *WorkerScore
	for _, w := range r.workers {
		inf := w.InFlight()
		healthy := w.Healthy()
		hs := 0.0
		if healthy {
			hs = 1.0
		}
		// load: prefer empty queues; hard-zero if soft-capped overloaded
		ls := 1.0 / (1.0 + float64(inf))
		if r.overloaded(w) {
			ls = 0.0
		}
		// cache + age from affinity map (hash only; no raw prompt)
		cs, as, ageSec := 0.0, 0.0, -1.0
		hasA := hasAff && ent.workerID == w.ID
		if hasA {
			cs = 1.0
			ageSec = now.Sub(ent.lastUsed).Seconds()
			if ageSec < 0 {
				ageSec = 0
			}
			// half-life decay: age=0 → 1, age=halfLife → 0.5
			hl := half.Seconds()
			if hl <= 0 {
				hl = 5
			}
			as = 1.0 / (1.0 + ageSec/hl) // smooth, ~0.5 at half-life
		}
		ws := WorkerScore{
			WorkerID:    w.ID,
			Cache:       cs,
			Load:        ls,
			Age:         as,
			Health:      hs,
			Inflight:    inf,
			Healthy:     healthy,
			HasAffinity: hasA,
			CacheAgeSec: ageSec,
			WCache:      wc * cs,
			WLoad:       wl * ls,
			WAge:        wa * as,
			WHealth:     wh * hs,
		}
		ws.Total = ws.WCache + ws.WLoad + ws.WAge + ws.WHealth
		// Hard disqualify unhealthy / soft-capped overload so hybrid can leave
		// a hot cache worker when queue pressure exceeds MaxInflight.
		if !healthy {
			ws.Total = -1
		} else if r.overloaded(w) {
			ws.Total = -0.5 // overloaded healthy still beats unhealthy
		}
		scores = append(scores, ws)
		if best == nil || ws.Total > best.Total || (ws.Total == best.Total && ws.Inflight < best.Inflight) {
			cp := ws
			best = &cp
		}
	}
	d.Scores = scores
	if best == nil {
		w := r.workers[0]
		r.fill(d, w, false, "empty_pool", "hybrid empty")
		return
	}
	w := r.workerByID(best.WorkerID)
	d.ChosenScore = best.Total
	hit := best.HasAffinity
	d.SwitchedAway = hasAff && best.WorkerID != ent.workerID

	reason := fmt.Sprintf(
		"hybrid chose worker=%d total=%.3f [cache=%.3f load=%.3f age=%.3f health=%.3f] inflight=%d affinity_worker=%d switched_away=%v",
		best.WorkerID, best.Total, best.WCache, best.WLoad, best.WAge, best.WHealth,
		best.Inflight, d.AffinityWorkerID, d.SwitchedAway,
	)
	if d.SwitchedAway {
		// Find affinity worker score for comparison in reason.
		var affScore float64
		var affInf int64
		for _, s := range scores {
			if s.WorkerID == ent.workerID {
				affScore = s.Total
				affInf = s.Inflight
				break
			}
		}
		reason += fmt.Sprintf(
			" | SWITCHED from cache affinity: aff_worker=%d aff_total=%.3f aff_inflight=%d winner_load=%.3f (load/health beat cache)",
			ent.workerID, affScore, affInf, best.WLoad,
		)
	} else if hit {
		reason += fmt.Sprintf(" | kept_cache_affinity age_sec=%.2f", best.CacheAgeSec)
	} else {
		reason += " | cold_or_no_affinity"
	}

	// Fallback tag must match aggregate counters (fallback_unhealthy / overload).
	// Previously hybrid always used "hybrid_switch", so metrics stayed 0 while
	// scores already showed healthy=false on the affinity worker.
	fb := ""
	if d.SwitchedAway {
		fb = "hybrid_switch"
		for _, s := range scores {
			if s.WorkerID != ent.workerID {
				continue
			}
			if !s.Healthy {
				fb = "unhealthy"
			} else if s.Total <= -0.5 {
				// overloaded affinity (hard cap → total -0.5)
				fb = "overload"
			}
			break
		}
	} else if !hasAff {
		fb = "cold"
	}
	// If the chosen worker itself is unhealthy (all-down last resort), tag it.
	if w != nil && !w.Healthy() && fb != "unhealthy" {
		fb = "unhealthy"
	}
	r.fill(d, w, hit, fb, reason)
	// Learn: bind prefix to chosen healthy worker for future cache component.
	if w != nil && w.Healthy() {
		r.bindSticky(prefixSHA, w.ID)
	}
}

func (r *Router) bindSticky(prefixSHA string, workerID int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.sticky) >= r.cfg.MaxStickyEntries {
		// Evict oldest.
		var oldestK string
		var oldestT time.Time
		first := true
		for k, e := range r.sticky {
			if first || e.lastUsed.Before(oldestT) {
				first = false
				oldestT = e.lastUsed
				oldestK = k
			}
		}
		if oldestK != "" {
			delete(r.sticky, oldestK)
		}
	}
	prev, ok := r.sticky[prefixSHA]
	hits := 1
	if ok && prev.workerID == workerID {
		hits = prev.hits + 1
	}
	r.sticky[prefixSHA] = stickyEntry{workerID: workerID, lastUsed: time.Now(), hits: hits}
}

// TouchAffinity refreshes lastUsed after a successful request on workerID
// (keeps cache age accurate without storing prompts).
func (r *Router) TouchAffinity(prefixSHA string, workerID int) {
	if prefixSHA == "" {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if ent, ok := r.sticky[prefixSHA]; ok && ent.workerID == workerID {
		ent.lastUsed = time.Now()
		ent.hits++
		r.sticky[prefixSHA] = ent
	}
}

func (r *Router) overloaded(w *Worker) bool {
	if w.MaxInflight <= 0 {
		return false
	}
	return w.InFlight() >= int64(w.MaxInflight)
}

func (r *Router) workerByID(id int) *Worker {
	for _, w := range r.workers {
		if w.ID == id {
			return w
		}
	}
	return nil
}

func (r *Router) pickRoundRobin() *Worker {
	// Collect healthy workers for fair rotate (avoid "skip unhealthy" collapsing to one worker).
	var healthy []*Worker
	for _, w := range r.workers {
		if w.Healthy() {
			healthy = append(healthy, w)
		}
	}
	if len(healthy) == 0 {
		return r.workers[0]
	}
	idx := int(atomic.AddUint64(&r.rr, 1)-1) % len(healthy)
	return healthy[idx]
}

// NeedsWorkerStatic reports whether a method consumes per-worker capacity or
// service-time metadata. Gateways must enable multi state for these methods or
// the policy degrades to plain least_inflight without saying so.
func NeedsWorkerStatic(m Method) bool {
	switch m {
	case MethodMultiSLO, MethodCapacityNormalizedLeastInflight, MethodShortestEstimatedWorkload:
		return true
	}
	return false
}

// pickLeastInflight chooses the lowest inflight healthy worker. Ties are broken
// by the lower estimated service time per unit capacity, then by ID for
// determinism. Index-order tie-breaking sent every request of an all-idle mesh
// to worker 0 even when worker 0 was the slowest member.
func (r *Router) pickLeastInflight(exclude map[int]bool) *Worker {
	var best *Worker
	var bestTie float64
	for _, w := range r.workers {
		if exclude != nil && exclude[w.ID] {
			continue
		}
		if !w.Healthy() {
			continue
		}
		capacity, serviceMs := r.capacityAndServiceEstimate(w.ID)
		tie := serviceMs / capacity
		switch {
		case best == nil,
			w.InFlight() < best.InFlight(),
			w.InFlight() == best.InFlight() && tie < bestTie,
			w.InFlight() == best.InFlight() && tie == bestTie && w.ID < best.ID:
			best, bestTie = w, tie
		}
	}
	if best == nil {
		// all unhealthy — pick lowest inflight anyway
		for _, w := range r.workers {
			if best == nil || w.InFlight() < best.InFlight() {
				best = w
			}
		}
	}
	return best
}

// capacityAndServiceEstimate returns a relative capacity and an online service
// estimate for one worker. Capacity is explicit configuration; the service
// estimate is the latency prior until observations are available. This keeps
// the two simpler policies independent of multi_slo's weighted score.
func (r *Router) capacityAndServiceEstimate(workerID int) (capacity, serviceMs float64) {
	capacity, serviceMs = 1, 1
	if r.multi == nil {
		return capacity, serviceMs
	}
	r.multi.mu.Lock()
	defer r.multi.mu.Unlock()
	st := r.multi.static[workerID]
	if st.Capacity > 0 {
		capacity = st.Capacity
	}
	if st.LatencyPriorMs > 0 {
		serviceMs = st.LatencyPriorMs
	}
	if lat := r.multi.lat[workerID]; lat != nil && lat.emaTTFT > 0 {
		serviceMs = lat.emaTTFT
	}
	return capacity, serviceMs
}

// pickCapacityNormalizedLeastInflight implements weighted least-connections:
// choose argmin_i inflight_i / capacity_i among healthy workers. A capacity of
// 1 is the default, preserving ordinary least-inflight if no calibration exists.
func (r *Router) pickCapacityNormalizedLeastInflight(d *Decision, eligible map[int]bool) {
	var best *Worker
	bestLoad := math.Inf(1)
	scores := make([]WorkerScore, 0, len(r.workers))
	for _, w := range r.workers {
		if eligible != nil && !eligible[w.ID] {
			continue
		}
		if !w.Healthy() {
			continue
		}
		capacity, serviceMs := r.capacityAndServiceEstimate(w.ID)
		load := float64(w.InFlight()) / capacity
		scores = append(scores, WorkerScore{WorkerID: w.ID, Total: -load, Load: 1 / (1 + load), Inflight: w.InFlight(), Healthy: true, EstimatedTTFTMs: serviceMs})
		if best == nil || load < bestLoad || (load == bestLoad && w.ID < best.ID) {
			best, bestLoad = w, load
		}
	}
	if best == nil {
		best = r.pickLeastInflight(nil)
		r.fill(d, best, false, "all_down", "capacity_normalized_least_inflight fallback: all workers unhealthy")
		return
	}
	d.Scores = scores
	d.ChosenScore = -bestLoad
	r.fill(d, best, false, "", fmt.Sprintf("capacity_normalized_least_inflight chose worker %d normalized_inflight=%.6f", best.ID, bestLoad))
}

// pickShortestEstimatedWorkload implements shortest expected queued work:
// choose argmin_i inflight_i * service_estimate_i / capacity_i. It estimates
// queue work at the decision snapshot; it deliberately does not claim a
// calibrated deadline and it makes no inference from prompt content.
func (r *Router) pickShortestEstimatedWorkload(d *Decision, eligible map[int]bool) {
	var best *Worker
	bestWork := math.Inf(1)
	scores := make([]WorkerScore, 0, len(r.workers))
	for _, w := range r.workers {
		if eligible != nil && !eligible[w.ID] {
			continue
		}
		if !w.Healthy() {
			continue
		}
		capacity, serviceMs := r.capacityAndServiceEstimate(w.ID)
		workMs := float64(w.InFlight()) * serviceMs / capacity
		scores = append(scores, WorkerScore{WorkerID: w.ID, Total: -workMs, Load: 1 / (1 + workMs), Inflight: w.InFlight(), Healthy: true, EstimatedTTFTMs: serviceMs})
		if best == nil || workMs < bestWork || (workMs == bestWork && w.ID < best.ID) {
			best, bestWork = w, workMs
		}
	}
	if best == nil {
		best = r.pickLeastInflight(nil)
		r.fill(d, best, false, "all_down", "shortest_estimated_workload fallback: all workers unhealthy")
		return
	}
	d.Scores = scores
	d.ChosenScore = -bestWork
	r.fill(d, best, false, "", fmt.Sprintf("shortest_estimated_workload chose worker %d estimated_queued_work_ms=%.6f", best.ID, bestWork))
}

func (r *Router) pickByHash(promptSHA string) *Worker {
	h := fnv.New32a()
	_, _ = h.Write([]byte(promptSHA))
	n := len(r.workers)
	start := int(h.Sum32()) % n
	for i := 0; i < n; i++ {
		w := r.workers[(start+i)%n]
		if w.Healthy() {
			return w
		}
	}
	return r.workers[start]
}

// ResetAllWorkers clears per-worker runtime counters (inflight/fails/unhealthy).
// Used between endurance chaos cycles so soft-fail cooldowns do not leak.
func (r *Router) ResetAllWorkers() {
	for _, w := range r.workers {
		if w != nil {
			w.ResetRuntime()
		}
	}
}

// Acquire increments inflight for the chosen worker (call before HTTP).
func (r *Router) Acquire(workerID int) {
	if w := r.workerByID(workerID); w != nil {
		w.inflight.Add(1)
	}
}

// CancelAcquire releases a reservation before any upstream attempt. It does not
// mark the worker failed because admission/payload cancellation is not evidence
// of worker health.
func (r *Router) CancelAcquire(workerID int) {
	if w := r.workerByID(workerID); w != nil {
		w.inflight.Add(-1)
	}
}

// Release decrements inflight and records success/failure (never restarts worker).
func (r *Router) Release(workerID int, success bool) {
	w := r.workerByID(workerID)
	if w == nil {
		return
	}
	w.inflight.Add(-1)
	if success {
		w.served.Add(1)
		w.markOK()
	} else {
		w.markFail(r.cfg.FailCooldown)
	}
}

// StickyCount returns number of prefix bindings (hashes only).
func (r *Router) StickyCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.sticky)
}

// ResetLearning clears sticky bindings and per-worker served/fail counters.
// Used after warmup so measured sticky% / served reflect the measured phase only.
// Does not restart any worker process.
func (r *Router) ResetLearning() {
	r.mu.Lock()
	r.sticky = make(map[string]stickyEntry)
	r.mu.Unlock()
	for _, w := range r.workers {
		w.served.Store(0)
		w.fails.Store(0)
		w.unhealthyUntil.Store(0)
	}
}

// Snapshot is privacy-safe router stats.
type Snapshot struct {
	Method      string           `json:"method"`
	StickyCount int              `json:"sticky_count"`
	WorkerStats []WorkerSnapshot `json:"workers"`
}

// WorkerSnapshot is per-worker counters (no prompts).
type WorkerSnapshot struct {
	ID       int    `json:"id"`
	BaseURL  string `json:"base_url"`
	Inflight int64  `json:"inflight"`
	Served   int64  `json:"served"`
	Fails    int64  `json:"fails"`
	Healthy  bool   `json:"healthy"`
}

// Stats returns a snapshot.
func (r *Router) Stats() Snapshot {
	s := Snapshot{
		Method:      string(r.Method()),
		StickyCount: r.StickyCount(),
	}
	for _, w := range r.workers {
		s.WorkerStats = append(s.WorkerStats, WorkerSnapshot{
			ID:       w.ID,
			BaseURL:  w.BaseURL,
			Inflight: w.InFlight(),
			Served:   w.Served(),
			Fails:    w.fails.Load(),
			Healthy:  w.Healthy(),
		})
	}
	return s
}

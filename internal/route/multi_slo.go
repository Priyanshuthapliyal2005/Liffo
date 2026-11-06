package route

import (
	"fmt"
	"math"
	"strings"
	"sync"
	"time"
)

// WorkerStatic is optional static economics / capacity / quality metadata (no secrets).
type WorkerStatic struct {
	// CostPerReq relative cost unit per request (higher = more expensive). Local GPU ~0.2, cloud ~1.0.
	CostPerReq float64
	// LatencyPriorMs cold-start prior when no observations yet (local ~25, remote ~120).
	LatencyPriorMs float64
	// QualityTier static quality score or tier in [0..1] (e.g. 0.5B Q4 ~0.6, 7B/frontier ~1.0). Default 1.0.
	QualityTier float64
	// Capacity is relative worker capacity (>0, default 1). It is used only by
	// established capacity-aware policies and is not a throughput claim by itself.
	Capacity float64
}

// RouteConstraints allows per-request cost, latency, and quality constraints.
type RouteConstraints struct {
	// MinQuality in [0..1]: workers with effective quality < MinQuality are hard-rejected if a compliant healthy worker exists.
	MinQuality float64
	// MaxCostPerReq: workers with CostPerReq > MaxCostPerReq (>0) are hard-rejected if a compliant healthy worker exists.
	MaxCostPerReq float64
	// MaxLatencyMs: hard cap on predicted EMA TTFT; also overrides SLOTargetMs when > 0.
	MaxLatencyMs float64
	// PreferQuality: when true, shifts weight toward quality + latency for accuracy-critical requests.
	PreferQuality bool
	// PreferCost: when true, shifts weight toward cost efficiency for budget-sensitive requests.
	PreferCost bool
}

// MultiSLOConfig weights and SLO target for MethodMultiSLO.
type MultiSLOConfig struct {
	// Weights are normalized at pick time.
	WeightCost    float64 // prefer lower cost
	WeightLatency float64 // prefer lower EMA TTFT
	WeightLoad    float64 // prefer lower inflight
	WeightSLO     float64 // prefer workers under SLO
	WeightQuality float64 // prefer higher quality tier * online reliability
	// SLOTargetMs: EMA client/backend TTFT target (ms). Above → SLO score collapses.
	SLOTargetMs float64
	// MinQuality: optional global minimum quality threshold in [0..1].
	MinQuality float64
	// MaxCostPerReq: optional global maximum cost per request (>0).
	MaxCostPerReq float64
	// LatencyEMAAlpha: 0..1, higher = more reactive (default 0.3).
	LatencyEMAAlpha float64
	// IdleEMADecayHalfLife returns an idle worker's EMA toward its configured
	// latency prior. This prevents a transient spike from permanently biasing
	// future decisions. Zero uses the default.
	IdleEMADecayHalfLife time.Duration
}

func validFiniteNonNegative(v float64) bool {
	return !math.IsNaN(v) && !math.IsInf(v, 0) && v >= 0
}

func validateMultiInputs(cfg MultiSLOConfig, static []WorkerStatic) error {
	for name, value := range map[string]float64{
		"weight_cost": cfg.WeightCost, "weight_latency": cfg.WeightLatency,
		"weight_load": cfg.WeightLoad, "weight_slo": cfg.WeightSLO,
		"weight_quality": cfg.WeightQuality, "slo_target_ms": cfg.SLOTargetMs,
		"min_quality": cfg.MinQuality, "max_cost_per_req": cfg.MaxCostPerReq,
		"latency_ema_alpha": cfg.LatencyEMAAlpha,
	} {
		if !validFiniteNonNegative(value) {
			return fmt.Errorf("multi_slo: %s must be finite and non-negative", name)
		}
	}
	if cfg.MinQuality > 1 || cfg.LatencyEMAAlpha > 1 {
		return fmt.Errorf("multi_slo: quality and alpha must be in [0,1]")
	}
	for i, st := range static {
		if !validFiniteNonNegative(st.CostPerReq) || !validFiniteNonNegative(st.LatencyPriorMs) || !validFiniteNonNegative(st.QualityTier) || !validFiniteNonNegative(st.Capacity) || st.QualityTier > 1 {
			return fmt.Errorf("multi_slo: invalid static metadata for worker index %d", i)
		}
	}
	return nil
}

func defaultMultiSLO() MultiSLOConfig {
	return MultiSLOConfig{
		WeightCost:           0.10,
		WeightLatency:        0.30,
		WeightLoad:           0.35,
		WeightSLO:            0.25,
		WeightQuality:        0.00,
		SLOTargetMs:          150,
		LatencyEMAAlpha:      0.30,
		IdleEMADecayHalfLife: 30 * time.Second,
	}
}

// latencyState is per-worker online stats (protected by Router.multi.mu).
type latencyState struct {
	emaTTFT        float64
	emaSuccessRate float64
	emaQuality     float64
	nQualityObs    int64
	qualitySum     float64
	nObs           int64
	nFail          int64
	lastTTFT       float64
	lastAt         time.Time
	sloBreaches    int64
}

// MultiSLOScore is privacy-safe score breakdown for one worker.
type MultiSLOScore struct {
	WorkerID int     `json:"worker_id"`
	Total    float64 `json:"total"`
	Cost     float64 `json:"cost"`    // 0..1 higher better (cheaper)
	Latency  float64 `json:"latency"` // 0..1 higher better (faster)
	Load     float64 `json:"load"`    // 0..1 higher better (less load)
	SLO      float64 `json:"slo"`     // 0..1 under target
	Quality  float64 `json:"quality"` // 0..1 higher better (tier * reliability)
	Inflight int64   `json:"inflight"`
	EMAMs    float64 `json:"ema_ttft_ms"`
	// EstimatedTTFTMs is a point estimate based on the latency EMA at the exact
	// decision snapshot. It is intentionally not treated as a calibrated SLO bound.
	EstimatedTTFTMs float64 `json:"estimated_ttft_ms"`
	SuccessRate     float64 `json:"ema_success_rate"`
	QualityTier     float64 `json:"quality_tier"`
	EMAQuality      float64 `json:"ema_quality"`
	NQualityObs     int64   `json:"n_quality_obs"`
	CostPerReq      float64 `json:"cost_per_req"`
	Healthy         bool    `json:"healthy"`
	HardRejected    bool    `json:"hard_rejected"`
	RejectReason    string  `json:"reject_reason,omitempty"`
}

// Ensure multi_slo fields on Router — attached via extension on existing Router.
// We store static attrs and EMA on Router with a side map keyed by worker ID.

// multiState holds multi_slo runtime (owned by Router).
type multiState struct {
	cfg    MultiSLOConfig
	static map[int]WorkerStatic // worker id → static
	lat    map[int]*latencyState
	mu     sync.Mutex
}

func newMultiState(cfg MultiSLOConfig, workers []*Worker, static []WorkerStatic) *multiState {
	if cfg.SLOTargetMs <= 0 {
		def := defaultMultiSLO()
		if cfg.WeightCost == 0 && cfg.WeightLatency == 0 && cfg.WeightLoad == 0 && cfg.WeightSLO == 0 && cfg.WeightQuality == 0 {
			cfg = def
		} else {
			cfg.SLOTargetMs = def.SLOTargetMs
			if cfg.LatencyEMAAlpha <= 0 {
				cfg.LatencyEMAAlpha = def.LatencyEMAAlpha
			}
		}
	}
	if cfg.LatencyEMAAlpha <= 0 || cfg.LatencyEMAAlpha > 1 {
		cfg.LatencyEMAAlpha = 0.3
	}
	if cfg.IdleEMADecayHalfLife <= 0 {
		cfg.IdleEMADecayHalfLife = 30 * time.Second
	}
	ms := &multiState{
		cfg:    cfg,
		static: make(map[int]WorkerStatic),
		lat:    make(map[int]*latencyState),
	}
	for i, w := range workers {
		st := WorkerStatic{CostPerReq: 1.0, LatencyPriorMs: 80, QualityTier: 1.0, Capacity: 1.0}
		if i < len(static) {
			if static[i].CostPerReq > 0 {
				st.CostPerReq = static[i].CostPerReq
			}
			if static[i].LatencyPriorMs > 0 {
				st.LatencyPriorMs = static[i].LatencyPriorMs
			}
			if static[i].QualityTier > 0 {
				st.QualityTier = static[i].QualityTier
			}
			if static[i].Capacity > 0 {
				st.Capacity = static[i].Capacity
			}
		} else if i == 0 {
			// sensible defaults: worker0 local cheap/fast prior
			st.CostPerReq = 0.25
			st.LatencyPriorMs = 30
			st.QualityTier = 0.85
		} else {
			st.CostPerReq = 1.0
			st.LatencyPriorMs = 55
			st.QualityTier = 1.0
		}
		if st.QualityTier > 1.0 {
			st.QualityTier = 1.0
		}
		ms.static[w.ID] = st
		ms.lat[w.ID] = &latencyState{
			emaTTFT:        st.LatencyPriorMs,
			emaSuccessRate: 1.0,
			emaQuality:     st.QualityTier,
		}
	}
	return ms
}

// ObserveQuality records verified task quality outcome in [0..1] for a worker.
// It updates the online quality EMA and empirical quality stats.
func (r *Router) ObserveQuality(workerID int, quality float64) bool {
	if r.multi == nil {
		return false
	}
	if quality < 0 || quality > 1 || math.IsNaN(quality) || math.IsInf(quality, 0) {
		return false
	}
	r.multi.mu.Lock()
	defer r.multi.mu.Unlock()
	st := r.multi.lat[workerID]
	if st == nil {
		return false
	}
	a := r.multi.cfg.LatencyEMAAlpha
	if a <= 0 || a > 1 {
		a = 0.3
	}
	st.emaQuality = (1-a)*st.emaQuality + a*quality
	st.nQualityObs++
	st.qualitySum += quality
	return true
}

// ObserveLatency updates EMA TTFT and online reliability after a completed request (ttftMs, success).
func (r *Router) ObserveLatency(workerID int, ttftMs float64, success bool) {
	if r.multi == nil {
		return
	}
	// A successful timing observation must be a finite, positive duration. Ignore
	// invalid telemetry rather than poisoning EMA state and downstream JSON traces.
	if success && (ttftMs <= 0 || math.IsNaN(ttftMs) || math.IsInf(ttftMs, 0)) {
		return
	}
	r.multi.mu.Lock()
	defer r.multi.mu.Unlock()
	st := r.multi.lat[workerID]
	if st == nil {
		return
	}
	a := r.multi.cfg.LatencyEMAAlpha
	if a <= 0 || a > 1 {
		a = 0.3
	}
	if !success || ttftMs < 0 {
		// failures handled by markFail; bump EMA TTFT and decay online reliability
		if !success {
			pen := r.multi.cfg.SLOTargetMs * 2
			if pen < 200 {
				pen = 200
			}
			st.emaTTFT = (1-a)*st.emaTTFT + a*pen
			st.emaSuccessRate = (1 - a) * st.emaSuccessRate
			st.nFail++
			st.lastAt = time.Now()
			st.sloBreaches++
		}
		return
	}
	st.emaTTFT = (1-a)*st.emaTTFT + a*ttftMs
	st.emaSuccessRate = (1-a)*st.emaSuccessRate + a*1.0
	st.nObs++
	st.lastTTFT = ttftMs
	st.lastAt = time.Now()
	if ttftMs > r.multi.cfg.SLOTargetMs {
		st.sloBreaches++
	}
}

func decayIdleEMA(st *latencyState, prior float64, halfLife time.Duration, now time.Time) {
	if st == nil || prior <= 0 || halfLife <= 0 || st.lastAt.IsZero() || st.emaTTFT <= prior {
		return
	}
	elapsed := now.Sub(st.lastAt)
	if elapsed <= 0 {
		return
	}
	factor := math.Pow(0.5, float64(elapsed)/float64(halfLife))
	st.emaTTFT = prior + (st.emaTTFT-prior)*factor
	st.lastAt = now
}

func (r *Router) pickMultiSLO(d *Decision, rc *RouteConstraints, eligible map[int]bool) {
	if r.multi == nil {
		w := r.pickLeastInflight(nil)
		r.fill(d, w, false, "multi_unconfigured", "multi_slo missing state → least_inflight")
		return
	}
	r.multi.mu.Lock()
	cfg := r.multi.cfg
	r.multi.mu.Unlock()
	wc, wl, wload, wslo, wqual := cfg.WeightCost, cfg.WeightLatency, cfg.WeightLoad, cfg.WeightSLO, cfg.WeightQuality
	if rc != nil {
		if rc.PreferQuality {
			if wqual < 0.50 {
				wqual = 0.50
			}
			wc *= 0.1
		}
		if rc.PreferCost {
			if wc < 0.50 {
				wc = 0.50
			}
			wqual *= 0.2
		}
		if rc.MaxLatencyMs > 0 {
			// The latency value is an uncalibrated hint, not a rejection bound.
			// Give it enough weight to override static cost for latency-sensitive work.
			if wl < 0.50 {
				wl = 0.50
			}
			if wslo < 0.25 {
				wslo = 0.25
			}
			wc *= 0.10
		}
	}
	sumW := wc + wl + wload + wslo + wqual
	if sumW <= 0 {
		sumW = 1
	}
	wc, wl, wload, wslo, wqual = wc/sumW, wl/sumW, wload/sumW, wslo/sumW, wqual/sumW
	sloT := cfg.SLOTargetMs
	if rc != nil && rc.MaxLatencyMs > 0 {
		sloT = rc.MaxLatencyMs
	}
	if sloT <= 0 {
		sloT = 150
	}
	minQual := cfg.MinQuality
	if rc != nil && rc.MinQuality > minQual {
		minQual = rc.MinQuality
	}
	maxBudget := cfg.MaxCostPerReq
	if rc != nil && rc.MaxCostPerReq > 0 && (maxBudget <= 0 || rc.MaxCostPerReq < maxBudget) {
		maxBudget = rc.MaxCostPerReq
	}

	// Normalize cost against the cheapest configured worker.
	minCost := math.MaxFloat64
	for _, st := range r.multi.static {
		if st.CostPerReq > 0 && st.CostPerReq < minCost {
			minCost = st.CostPerReq
		}
	}
	if minCost == math.MaxFloat64 {
		minCost = 1
	}

	scores := make([]MultiSLOScore, 0, len(r.workers))
	var best *MultiSLOScore
	var bestRejected *MultiSLOScore
	var eligibleIDs []int
	now := time.Now()
	r.multi.mu.Lock()
	for _, w := range r.workers {
		st := r.multi.static[w.ID]
		lat := r.multi.lat[w.ID]
		if w.InFlight() == 0 {
			decayIdleEMA(lat, st.LatencyPriorMs, cfg.IdleEMADecayHalfLife, now)
		}
		ema := st.LatencyPriorMs
		succRate := 1.0
		if lat != nil {
			if lat.emaTTFT > 0 {
				ema = lat.emaTTFT
			}
			if lat.nObs > 0 || lat.nFail > 0 {
				succRate = lat.emaSuccessRate
			}
		}
		qTier := st.QualityTier
		if qTier <= 0 {
			qTier = 1.0
		}
		if qTier > 1.0 {
			qTier = 1.0
		}
		qBase := qTier
		emaQual := qTier
		nQualObs := int64(0)
		if lat != nil && lat.nQualityObs > 0 {
			qBase = lat.emaQuality
			emaQual = lat.emaQuality
			nQualObs = lat.nQualityObs
		}
		qualScore := qBase * succRate

		inf := w.InFlight()
		healthy := w.Healthy()
		over := r.overloaded(w)

		var rejReasons []string
		if eligible != nil && !eligible[w.ID] {
			rejReasons = append(rejReasons, "model_unavailable")
		}
		if !healthy {
			rejReasons = append(rejReasons, "unhealthy")
		}
		if over {
			rejReasons = append(rejReasons, "overload")
		}
		// MaxLatencyMs changes the selection target, but is not an admission
		// contract until the estimator is calibrated. Historic traces show the
		// current EMA is not accurate enough to justify false rejections.
		if minQual > 0 && qualScore+1e-9 < minQual {
			rejReasons = append(rejReasons, fmt.Sprintf("min_quality(%.2f<%.2f)", qualScore, minQual))
		}
		if maxBudget > 0 && st.CostPerReq > maxBudget+1e-9 {
			rejReasons = append(rejReasons, fmt.Sprintf("max_cost(%.2f>%.2f)", st.CostPerReq, maxBudget))
		}
		hard := len(rejReasons) > 0
		rejStr := strings.Join(rejReasons, ",")

		// Scores 0..1 higher = better.
		// Cost: relative to cheapest worker (min/cost). Expensive workers stay >0
		// so they remain selectable under load/SLO pressure (not hard-zeroed).
		costScore := minCost / st.CostPerReq
		if st.CostPerReq <= 0 {
			costScore = 1
		}
		if costScore > 1 {
			costScore = 1
		}
		// Smooth bounded components retain relative ordering even when every
		// candidate is above the target; neither collapses to a static-cost tie.
		latScore := 1.0 / (1.0 + ema/sloT)
		// load: 1/(1+inflight)
		loadScore := 1.0 / (1.0 + float64(inf))
		if over {
			loadScore = 0
		}
		// SLO headroom uses the same continuous decreasing transform. The prior
		// branch rewarded a just-over-target worker more than an under-target one.
		sloScore := 1.0 / (1.0 + ema/sloT)
		if !healthy {
			sloScore = 0
			costScore, latScore, loadScore, qualScore = 0, 0, 0, 0
		}

		total := wc*costScore + wl*latScore + wload*loadScore + wslo*sloScore + wqual*qualScore
		if hard {
			total = -1 // never pick if any healthy compliant alternative exists
		}
		sc := MultiSLOScore{
			WorkerID:        w.ID,
			Total:           total,
			Cost:            costScore,
			Latency:         latScore,
			Load:            loadScore,
			SLO:             sloScore,
			Quality:         qualScore,
			Inflight:        inf,
			EMAMs:           ema,
			EstimatedTTFTMs: ema,
			SuccessRate:     succRate,
			QualityTier:     qTier,
			EMAQuality:      emaQual,
			NQualityObs:     nQualObs,
			CostPerReq:      st.CostPerReq,
			Healthy:         healthy,
			HardRejected:    hard,
			RejectReason:    rejStr,
		}
		scores = append(scores, sc)
		if sc.HardRejected {
			if bestRejected == nil || sc.Total > bestRejected.Total || (sc.Total == bestRejected.Total && sc.WorkerID < bestRejected.WorkerID) {
				cp := sc
				bestRejected = &cp
			}
			continue
		}
		eligibleIDs = append(eligibleIDs, sc.WorkerID)
		if best == nil || sc.Total > best.Total || (sc.Total == best.Total && sc.WorkerID < best.WorkerID) {
			cp := sc
			best = &cp
		}
	}
	r.multi.mu.Unlock()

	d.EligibleWorkers = eligibleIDs
	if best == nil {
		best = bestRejected
	}
	d.Scores = nil // hybrid field; multi uses reason string with full table
	var chosen *Worker
	if best != nil {
		chosen = r.workerByID(best.WorkerID)
		d.ChosenScore = best.Total
	}
	// A declared quality/cost ceiling is a contract: never silently route to a
	// worker outside it, even when the pool is overloaded or unhealthy.
	if best != nil && best.HardRejected && (eligible != nil || minQual > 0 || maxBudget > 0) {
		d.WorkerID = -1
		d.BaseURL = ""
		d.Fallback = "constraints_unavailable"
		d.RejectionCode = "constraints_unavailable"
		d.Reason = "no_constraints_compliant_worker"
		d.Scores = multiToWorkerScores(scores)
		return
	}
	if best != nil && best.HardRejected {
		alt := r.pickLeastInflight(nil)
		if alt != nil {
			chosen = alt
			for i := range scores {
				if scores[i].WorkerID == alt.ID {
					d.ChosenScore = scores[i].Total
					best = &scores[i]
					break
				}
			}
		}
	}

	if best != nil {
		d.EstimatedTTFTMs = best.EstimatedTTFTMs
	}

	// Build a compact public-safe reason; detailed candidate scores belong in
	// the server-side routing telemetry attached by the gateway.
	fb := ""
	reason := "multi_slo_weighted_score"
	if best != nil && best.HardRejected {
		fb = "fallback_after_hard_reject"
		reason = "multi_slo_fallback_after_reject"
	}
	r.fill(d, chosen, false, fb, reason)
	// Store detailed scores for server-side evidence/export only.
	d.Scores = multiToWorkerScores(scores)
}

func multiToWorkerScores(in []MultiSLOScore) []WorkerScore {
	out := make([]WorkerScore, len(in))
	for i, s := range in {
		out[i] = WorkerScore{
			WorkerID:        s.WorkerID,
			Total:           s.Total,
			Cache:           s.Cost, // reuse fields: cache slot = cost component
			Load:            s.Load,
			Age:             s.Latency, // age slot = latency component
			Health:          s.SLO,     // health slot = SLO component
			Quality:         s.Quality,
			Inflight:        s.Inflight,
			Healthy:         s.Healthy,
			EstimatedTTFTMs: s.EstimatedTTFTMs,
			HardRejected:    s.HardRejected,
			RejectReason:    s.RejectReason,
		}
	}
	return out
}

// MultiSnapshot returns EMA/cost/quality view for /health (optional).
func (r *Router) MultiSnapshot() []map[string]any {
	if r.multi == nil {
		return nil
	}
	r.multi.mu.Lock()
	defer r.multi.mu.Unlock()
	var out []map[string]any
	for _, w := range r.workers {
		st := r.multi.static[w.ID]
		lat := r.multi.lat[w.ID]
		ema := st.LatencyPriorMs
		succRate := 1.0
		n := int64(0)
		nf := int64(0)
		br := int64(0)
		if lat != nil {
			ema = lat.emaTTFT
			if lat.nObs > 0 || lat.nFail > 0 {
				succRate = lat.emaSuccessRate
			}
			n = lat.nObs
			nf = lat.nFail
			br = lat.sloBreaches
		}
		qt := st.QualityTier
		if qt <= 0 {
			qt = 1.0
		}
		emaQual := qt
		nQual := int64(0)
		meanQual := qt
		if lat != nil {
			ema = lat.emaTTFT
			if lat.nObs > 0 || lat.nFail > 0 {
				succRate = lat.emaSuccessRate
			}
			n = lat.nObs
			nf = lat.nFail
			br = lat.sloBreaches
			if lat.nQualityObs > 0 {
				emaQual = lat.emaQuality
				nQual = lat.nQualityObs
				meanQual = lat.qualitySum / float64(lat.nQualityObs)
			}
		}
		out = append(out, map[string]any{
			"worker_id":             w.ID,
			"cost_per_req":          st.CostPerReq,
			"latency_prior_ms":      st.LatencyPriorMs,
			"quality_tier":          qt,
			"ema_quality":           emaQual,
			"n_quality_obs":         nQual,
			"measured_quality_mean": meanQual,
			"ema_ttft_ms":           ema,
			"ema_success_rate":      succRate,
			"effective_quality":     emaQual * succRate,
			"n_obs":                 n,
			"n_fail":                nf,
			"slo_breaches":          br,
			"inflight":              w.InFlight(),
			"healthy":               w.Healthy(),
			"slo_target_ms":         r.multi.cfg.SLOTargetMs,
		})
	}
	return out
}

// silence unused import if math used
var _ = math.Exp

// NewWithMulti builds a Router with multi_slo static attrs and SLO config.
func NewWithMulti(cfg Config, workers []*Worker, multiCfg MultiSLOConfig, static []WorkerStatic) (*Router, error) {
	if err := validateMultiInputs(multiCfg, static); err != nil {
		return nil, err
	}
	r, err := New(cfg, workers)
	if err != nil {
		return nil, err
	}
	r.multi = newMultiState(multiCfg, r.workers, static)
	if cfg.Method == "" {
		r.cfg.Method = MethodMultiSLO
	}
	return r, nil
}

// EnableMulti attaches multi_slo state to an existing router.
func (r *Router) EnableMulti(multiCfg MultiSLOConfig, static []WorkerStatic) {
	if validateMultiInputs(multiCfg, static) != nil {
		return
	}
	r.multi = newMultiState(multiCfg, r.workers, static)
}

// SetMultiSLOConfig updates weights/target live. Invalid updates are ignored so
// a control-plane request cannot poison active routing state with NaN/Inf.
func (r *Router) SetMultiSLOConfig(multiCfg MultiSLOConfig) {
	if validateMultiInputs(multiCfg, nil) != nil {
		return
	}
	if r.multi == nil {
		r.EnableMulti(multiCfg, nil)
		return
	}
	r.multi.mu.Lock()
	r.multi.cfg = multiCfg
	r.multi.mu.Unlock()
}

// ResetAllHealth clears fail cooldowns (experiment / recovery).
func (r *Router) ResetAllHealth() {
	for _, w := range r.workers {
		w.MarkOK()
	}
}

// HealthSnapshot is per-worker health at pick time (privacy-safe).
type HealthSnapshot struct {
	ID       int     `json:"id"`
	Healthy  bool    `json:"healthy"`
	Inflight int64   `json:"inflight"`
	Served   int64   `json:"served"`
	EMAMs    float64 `json:"ema_ttft_ms,omitempty"`
}

// SnapshotHealth returns health for all workers (and multi EMA if present).
func (r *Router) SnapshotHealth() []HealthSnapshot {
	out := make([]HealthSnapshot, 0, len(r.workers))
	var ema map[int]float64
	if r.multi != nil {
		r.multi.mu.Lock()
		ema = make(map[int]float64, len(r.workers))
		for id, st := range r.multi.lat {
			if st != nil {
				ema[id] = st.emaTTFT
			}
		}
		r.multi.mu.Unlock()
	}
	for _, w := range r.workers {
		hs := HealthSnapshot{
			ID: w.ID, Healthy: w.Healthy(), Inflight: w.InFlight(), Served: w.Served(),
		}
		if ema != nil {
			hs.EMAMs = ema[w.ID]
		}
		out = append(out, hs)
	}
	return out
}

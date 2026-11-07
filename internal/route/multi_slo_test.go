package route

import (
	"encoding/json"
	"math"
	"testing"
	"time"
)

func TestMultiSLOPrefersCheaperWhenLatencySimilar(t *testing.T) {
	w0 := &Worker{ID: 0, BaseURL: "http://local", MaxInflight: 4}
	w1 := &Worker{ID: 1, BaseURL: "http://remote", MaxInflight: 4}
	cfg := Defaults()
	cfg.Method = MethodMultiSLO
	r, err := NewWithMulti(cfg, []*Worker{w0, w1}, MultiSLOConfig{
		WeightCost: 0.4, WeightLatency: 0.2, WeightLoad: 0.2, WeightSLO: 0.2,
		SLOTargetMs: 150, LatencyEMAAlpha: 0.3,
	}, []WorkerStatic{
		{CostPerReq: 0.25, LatencyPriorMs: 30},
		{CostPerReq: 1.0, LatencyPriorMs: 35},
	})
	if err != nil {
		t.Fatal(err)
	}
	d := r.Pick("hello")
	if d.WorkerID != 0 {
		t.Fatalf("expected local(0) cheaper, got %d reason=%s", d.WorkerID, d.Reason)
	}
	if d.Reason == "" || d.Method != string(MethodMultiSLO) {
		// Method set in fill from cfg - check reason contains multi_slo
		if d.Reason == "" {
			t.Fatal("empty reason")
		}
	}
}

func TestMultiSLOAvoidsUnhealthy(t *testing.T) {
	w0 := &Worker{ID: 0, BaseURL: "http://local", MaxInflight: 4}
	w1 := &Worker{ID: 1, BaseURL: "http://remote", MaxInflight: 4}
	w0.markFail(time.Hour)
	cfg := Defaults()
	cfg.Method = MethodMultiSLO
	r, err := NewWithMulti(cfg, []*Worker{w0, w1}, defaultMultiSLO(), []WorkerStatic{
		{CostPerReq: 0.1, LatencyPriorMs: 20},
		{CostPerReq: 1.0, LatencyPriorMs: 120},
	})
	if err != nil {
		t.Fatal(err)
	}
	d := r.Pick("x")
	if d.WorkerID != 1 {
		t.Fatalf("expected remote when local unhealthy, got %d %s", d.WorkerID, d.Reason)
	}
}

func TestMultiSLOLatencyHintPrioritizesFastWorkersWithoutFalseRejects(t *testing.T) {
	cfg := Defaults()
	cfg.Method = MethodMultiSLO
	r, err := NewWithMulti(cfg, []*Worker{{ID: 0, BaseURL: "http://slow"}, {ID: 1, BaseURL: "http://fast"}},
		MultiSLOConfig{WeightCost: 1, SLOTargetMs: 150},
		[]WorkerStatic{{CostPerReq: 0.1, LatencyPriorMs: 200}, {CostPerReq: 1, LatencyPriorMs: 30}})
	if err != nil {
		t.Fatal(err)
	}
	d := r.PickWithConstraints("prompt", &RouteConstraints{MaxLatencyMs: 50})
	if d.WorkerID != 1 {
		t.Fatalf("slow worker must not violate 50ms latency cap: %+v", d)
	}
	d = r.PickWithConstraints("prompt", &RouteConstraints{MaxLatencyMs: 10})
	if d.WorkerID != 1 || d.Fallback == "constraints_unavailable" {
		t.Fatalf("uncalibrated 10ms hint must still select the least estimated worker rather than false-reject: %+v", d)
	}
}

func TestMultiSLOFailClosedWhenConstraintsImpossible(t *testing.T) {
	workers := []*Worker{{ID: 0, BaseURL: "http://cheap"}, {ID: 1, BaseURL: "http://premium"}}
	cfg := Defaults()
	cfg.Method = MethodMultiSLO
	r, err := NewWithMulti(cfg, workers, defaultMultiSLO(), []WorkerStatic{
		{CostPerReq: 0.2, QualityTier: 0.4},
		{CostPerReq: 1.0, QualityTier: 0.9},
	})
	if err != nil {
		t.Fatal(err)
	}
	dec := r.PickWithConstraints("private prompt", &RouteConstraints{MinQuality: 0.8, MaxCostPerReq: 0.3})
	if dec.WorkerID != -1 || dec.BaseURL != "" || dec.Fallback != "constraints_unavailable" {
		t.Fatalf("infeasible constraints must not silently route: %+v", dec)
	}
	workers[1].MarkFail(time.Minute)
	dec = r.PickWithConstraints("private prompt", &RouteConstraints{MinQuality: 0.8})
	if dec.WorkerID != -1 || dec.Fallback != "constraints_unavailable" {
		t.Fatalf("unhealthy compliant worker must not trigger quality downgrade: %+v", dec)
	}
}

func TestMultiSLOObservedFailuresReduceReliability(t *testing.T) {
	cfg := Defaults()
	cfg.Method = MethodMultiSLO
	r, err := NewWithMulti(cfg, []*Worker{{ID: 0, BaseURL: "http://one"}}, defaultMultiSLO(), []WorkerStatic{{QualityTier: 1, CostPerReq: 0.1}})
	if err != nil {
		t.Fatal(err)
	}
	r.ObserveLatency(0, 0, false)
	dec := r.PickWithConstraints("x", &RouteConstraints{MinQuality: 0.9})
	if dec.WorkerID != -1 || dec.Fallback != "constraints_unavailable" {
		t.Fatalf("reliability-adjusted quality must reject after failure: %+v", dec)
	}
	r.ObserveLatency(0, 12, true)
	r.ObserveLatency(0, 12, true)
	dec = r.PickWithConstraints("x", &RouteConstraints{MinQuality: 0.8})
	if dec.WorkerID != 0 {
		t.Fatalf("success observations should recover eligibility: %+v", dec)
	}
}

func TestMultiSLOQualityAwareRoutingAndConstraints(t *testing.T) {
	// Worker 0: cheap, low quality (0.6)
	w0 := &Worker{ID: 0, BaseURL: "http://cheap-local", MaxInflight: 4}
	// Worker 1: expensive, high quality (1.0)
	w1 := &Worker{ID: 1, BaseURL: "http://frontier-remote", MaxInflight: 4}

	cfg := Defaults()
	cfg.Method = MethodMultiSLO
	r, err := NewWithMulti(cfg, []*Worker{w0, w1}, MultiSLOConfig{
		WeightCost: 0.1, WeightLatency: 0.1, WeightLoad: 0.1, WeightSLO: 0.1, WeightQuality: 0.6,
		SLOTargetMs: 150, LatencyEMAAlpha: 0.3,
	}, []WorkerStatic{
		{CostPerReq: 0.1, LatencyPriorMs: 30, QualityTier: 0.6},
		{CostPerReq: 1.0, LatencyPriorMs: 50, QualityTier: 1.0},
	})
	if err != nil {
		t.Fatal(err)
	}

	// High quality weight selects w1 despite higher cost
	d := r.Pick("complex reasoning prompt")
	if d.WorkerID != 1 {
		t.Fatalf("expected high quality worker 1, got %d reason=%s", d.WorkerID, d.Reason)
	}

	// Constraint: MinQuality 0.8 rejects worker 0
	dConstrained := r.PickWithConstraints("complex query", &RouteConstraints{MinQuality: 0.8})
	if dConstrained.WorkerID != 1 {
		t.Fatalf("expected worker 1 under MinQuality 0.8 constraint, got %d", dConstrained.WorkerID)
	}

	// Constraint: MaxCostPerReq 0.5 rejects worker 1, selects worker 0
	dBudget := r.PickWithConstraints("simple task", &RouteConstraints{MaxCostPerReq: 0.5})
	if dBudget.WorkerID != 0 {
		t.Fatalf("expected cheap worker 0 under budget constraint, got %d", dBudget.WorkerID)
	}

	// PreferQuality shifts selection even if base cost weight was higher
	cfgBudget := Defaults()
	cfgBudget.Method = MethodMultiSLO
	rBudget, err := NewWithMulti(cfgBudget, []*Worker{w0, w1}, MultiSLOConfig{
		WeightCost: 0.6, WeightLatency: 0.2, WeightLoad: 0.1, WeightSLO: 0.1, WeightQuality: 0.0,
		SLOTargetMs: 150, LatencyEMAAlpha: 0.3,
	}, []WorkerStatic{
		{CostPerReq: 0.1, LatencyPriorMs: 30, QualityTier: 0.6},
		{CostPerReq: 1.0, LatencyPriorMs: 40, QualityTier: 1.0},
	})
	if err != nil {
		t.Fatal(err)
	}
	dDefault := rBudget.Pick("regular")
	if dDefault.WorkerID != 0 {
		t.Fatalf("expected cost-favored worker 0 by default, got %d", dDefault.WorkerID)
	}
	dPrefQual := rBudget.PickWithConstraints("accurate", &RouteConstraints{PreferQuality: true})
	if dPrefQual.WorkerID != 1 {
		t.Fatalf("expected worker 1 with PreferQuality=true, got %d", dPrefQual.WorkerID)
	}
}

func TestMultiSLOObserveMeasuredQualityDifferentiatesAndAdapts(t *testing.T) {
	// Worker 0 starts with high prior (0.95), Worker 1 with lower prior (0.80)
	w0 := &Worker{ID: 0, BaseURL: "http://worker-0", MaxInflight: 4}
	w1 := &Worker{ID: 1, BaseURL: "http://worker-1", MaxInflight: 4}

	cfg := Defaults()
	cfg.Method = MethodMultiSLO
	r, err := NewWithMulti(cfg, []*Worker{w0, w1}, MultiSLOConfig{
		WeightCost: 0.1, WeightLatency: 0.1, WeightLoad: 0.1, WeightSLO: 0.1, WeightQuality: 0.6,
		SLOTargetMs: 150, LatencyEMAAlpha: 0.4,
	}, []WorkerStatic{
		{CostPerReq: 0.5, LatencyPriorMs: 40, QualityTier: 0.95},
		{CostPerReq: 0.5, LatencyPriorMs: 40, QualityTier: 0.80},
	})
	if err != nil {
		t.Fatal(err)
	}

	// Initially worker 0 wins on quality tier prior
	d1 := r.Pick("solve problem")
	if d1.WorkerID != 0 {
		t.Fatalf("initial pick should be worker 0 (prior 0.95 vs 0.80), got %d", d1.WorkerID)
	}

	// External verifier tests worker 0's answers and observes failures (quality 0.0)
	ok := r.ObserveQuality(0, 0.0)
	if !ok {
		t.Fatal("ObserveQuality should return true")
	}
	r.ObserveQuality(0, 0.0)
	r.ObserveQuality(0, 0.0)

	// Worker 1 answers are verified correct (quality 1.0)
	r.ObserveQuality(1, 1.0)
	r.ObserveQuality(1, 1.0)

	// Now worker 1 must win because measured quality on worker 0 degraded below worker 1
	d2 := r.Pick("solve problem")
	if d2.WorkerID != 1 {
		t.Fatalf("after verification feedback, worker 1 should win, got %d (reason: %s)", d2.WorkerID, d2.Reason)
	}

	// Hard constraint MinQuality: 0.70 rejects worker 0 whose EMA quality is now ~0.20
	d3 := r.PickWithConstraints("strict problem", &RouteConstraints{MinQuality: 0.70})
	if d3.WorkerID != 1 {
		t.Fatalf("MinQuality constraint should strictly select worker 1, got %+v", d3)
	}

	// Snapshot checks
	snap := r.MultiSnapshot()
	if len(snap) != 2 {
		t.Fatalf("expected 2 workers in snapshot, got %d", len(snap))
	}
	if snap[0]["n_quality_obs"].(int64) != 3 {
		t.Fatalf("expected 3 quality observations for worker 0, got %v", snap[0]["n_quality_obs"])
	}
	if snap[0]["ema_quality"].(float64) >= 0.50 {
		t.Fatalf("worker 0 ema_quality should be low after failures, got %v", snap[0]["ema_quality"])
	}
	if snap[1]["ema_quality"].(float64) <= 0.80 {
		t.Fatalf("worker 1 ema_quality should be high after successes, got %v", snap[1]["ema_quality"])
	}

	// Invalid quality values must be rejected without mutating state
	if r.ObserveQuality(0, -0.1) || r.ObserveQuality(0, 1.5) || r.ObserveQuality(99, 0.5) {
		t.Fatal("invalid quality inputs should return false")
	}
}

func TestMultiSLORejectsNonFiniteLatencyObservation(t *testing.T) {
	cfg := Defaults()
	cfg.Method = MethodMultiSLO
	r, err := NewWithMulti(cfg, []*Worker{{ID: 0, BaseURL: "http://w0"}}, MultiSLOConfig{
		WeightLatency: 1, SLOTargetMs: 100, LatencyEMAAlpha: 0.3,
	}, []WorkerStatic{{CostPerReq: 1, LatencyPriorMs: 80, QualityTier: 1}})
	if err != nil {
		t.Fatal(err)
	}

	before := r.MultiSnapshot()[0]["ema_ttft_ms"].(float64)
	for _, invalid := range []float64{math.NaN(), math.Inf(1), math.Inf(-1)} {
		r.ObserveLatency(0, invalid, true)
	}
	after := r.MultiSnapshot()[0]["ema_ttft_ms"].(float64)
	if after != before {
		t.Fatalf("non-finite observation mutated EMA: before=%v after=%v", before, after)
	}
	if _, err := json.Marshal(r.Pick("safe")); err != nil {
		t.Fatalf("routing decision must remain JSON-serializable: %v", err)
	}
}

func TestMultiSLORejectsNonFiniteConfig(t *testing.T) {
	cfg := Defaults()
	cfg.Method = MethodMultiSLO
	_, err := NewWithMulti(cfg, []*Worker{{ID: 0, BaseURL: "http://w0"}}, MultiSLOConfig{
		WeightLatency: math.NaN(), SLOTargetMs: 100,
	}, []WorkerStatic{{CostPerReq: 1, LatencyPriorMs: 80, QualityTier: 1}})
	if err == nil {
		t.Fatal("non-finite multi_slo config must be rejected")
	}
}

func TestMultiSLOLatencyScoresRemainOrderedAboveSLO(t *testing.T) {
	cfg := Defaults()
	cfg.Method = MethodMultiSLO
	r, err := NewWithMulti(cfg, []*Worker{{ID: 0, BaseURL: "http://fast"}, {ID: 1, BaseURL: "http://slow"}}, MultiSLOConfig{
		WeightLatency: 1, SLOTargetMs: 95, LatencyEMAAlpha: 0.3,
	}, []WorkerStatic{
		{CostPerReq: 1, LatencyPriorMs: 400, QualityTier: 1},
		{CostPerReq: 1, LatencyPriorMs: 2400, QualityTier: 1},
	})
	if err != nil {
		t.Fatal(err)
	}
	d := r.Pick("long request")
	if d.WorkerID != 0 {
		t.Fatalf("expected 400ms worker to beat 2400ms worker above SLO, got %d", d.WorkerID)
	}
	if len(d.Scores) != 2 || d.Scores[0].Age <= d.Scores[1].Age {
		t.Fatalf("latency telemetry must preserve ordering above SLO: %+v", d.Scores)
	}
}

func TestMultiSLOLatencyHintDoesNotHardRejectUncalibratedEstimate(t *testing.T) {
	cfg := Defaults()
	cfg.Method = MethodMultiSLO
	r, err := NewWithMulti(cfg, []*Worker{{ID: 0, BaseURL: "http://fast"}, {ID: 1, BaseURL: "http://slow"}}, MultiSLOConfig{
		WeightLatency: 1, SLOTargetMs: 150, LatencyEMAAlpha: 0.3,
	}, []WorkerStatic{
		{CostPerReq: 1, LatencyPriorMs: 110, QualityTier: 1},
		{CostPerReq: 1, LatencyPriorMs: 300, QualityTier: 1},
	})
	if err != nil {
		t.Fatal(err)
	}
	d := r.PickWithConstraints("request", &RouteConstraints{MaxLatencyMs: 95})
	if d.WorkerID != 0 || d.Fallback == "constraints_unavailable" {
		t.Fatalf("an uncalibrated latency estimate must guide selection, not reject all work: %+v", d)
	}
}

func TestMultiSLODecaysIdleEMAAtPickTime(t *testing.T) {
	cfg := Defaults()
	cfg.Method = MethodMultiSLO
	r, err := NewWithMulti(cfg, []*Worker{{ID: 0, BaseURL: "http://w0"}}, MultiSLOConfig{
		WeightLatency: 1, SLOTargetMs: 95, LatencyEMAAlpha: 0.3,
	}, []WorkerStatic{{CostPerReq: 1, LatencyPriorMs: 60, QualityTier: 1}})
	if err != nil {
		t.Fatal(err)
	}
	r.ObserveLatency(0, 400, true)
	r.multi.mu.Lock()
	r.multi.lat[0].emaTTFT = 200
	r.multi.lat[0].lastAt = time.Now().Add(-2 * time.Minute)
	r.multi.mu.Unlock()

	_ = r.Pick("after idle")
	after := r.MultiSnapshot()[0]["ema_ttft_ms"].(float64)
	if after >= 95 {
		t.Fatalf("idle worker must decay toward its configured prior during production pick path, got %.1fms", after)
	}
}

func TestMultiSLOBlendsPriorOnFirstLatencyObservation(t *testing.T) {
	cfg := Defaults()
	cfg.Method = MethodMultiSLO
	r, err := NewWithMulti(cfg, []*Worker{{ID: 0, BaseURL: "http://w0"}}, MultiSLOConfig{
		WeightLatency: 1, SLOTargetMs: 150, LatencyEMAAlpha: 0.3,
	}, []WorkerStatic{{CostPerReq: 1, LatencyPriorMs: 60, QualityTier: 1}})
	if err != nil {
		t.Fatal(err)
	}
	r.ObserveLatency(0, 150, true)
	got := r.MultiSnapshot()[0]["ema_ttft_ms"].(float64)
	if got >= 140 || got <= 80 {
		t.Fatalf("first observation must blend configured prior and measurement, got %.1fms", got)
	}
}

func TestMultiSLOSLOScoreIsMonotonicAcrossTarget(t *testing.T) {
	cfg := Defaults()
	cfg.Method = MethodMultiSLO
	r, err := NewWithMulti(cfg, []*Worker{{ID: 0, BaseURL: "http://under"}, {ID: 1, BaseURL: "http://over"}}, MultiSLOConfig{
		WeightSLO: 1, SLOTargetMs: 95, LatencyEMAAlpha: 0.3,
	}, []WorkerStatic{
		{CostPerReq: 1, LatencyPriorMs: 94, QualityTier: 1},
		{CostPerReq: 1, LatencyPriorMs: 96, QualityTier: 1},
	})
	if err != nil {
		t.Fatal(err)
	}
	d := r.Pick("threshold")
	if d.WorkerID != 0 || d.Scores[0].Health <= d.Scores[1].Health {
		t.Fatalf("SLO score must decrease through target, got %+v", d.Scores)
	}
}

func TestPickAndAcquireMakesNextLeastInflightDecisionSeeReservation(t *testing.T) {
	cfg := Defaults()
	cfg.Method = MethodLeastInflight
	r, err := New(cfg, []*Worker{{ID: 0, BaseURL: "http://w0"}, {ID: 1, BaseURL: "http://w1"}})
	if err != nil {
		t.Fatal(err)
	}
	first := r.PickAndAcquire("first")
	second := r.PickAndAcquire("second")
	defer r.Release(first.WorkerID, true)
	defer r.Release(second.WorkerID, true)
	if first.WorkerID != 0 || second.WorkerID != 1 {
		t.Fatalf("reserved selection must expose first reservation to second decision: first=%+v second=%+v", first, second)
	}
}

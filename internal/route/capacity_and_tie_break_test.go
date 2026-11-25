package route

import "testing"

// TestIdleTieBreakPrefersFasterWorker proves the all-idle tie no longer resolves
// by worker index. Worker 0 is declared slow (prior 100ms) and worker 1 fast
// (prior 10ms); least_inflight must choose worker 1 while both are idle.
func TestIdleTieBreakPrefersFasterWorker(t *testing.T) {
	workers := []*Worker{
		{ID: 0, BaseURL: "http://w0"},
		{ID: 1, BaseURL: "http://w1"},
	}
	r, err := New(Config{Method: MethodLeastInflight}, workers)
	if err != nil {
		t.Fatal(err)
	}
	r.EnableMulti(MultiSLOConfig{SLOTargetMs: 80}, []WorkerStatic{
		{LatencyPriorMs: 100, Capacity: 1, CostPerReq: 1, QualityTier: 1},
		{LatencyPriorMs: 10, Capacity: 1, CostPerReq: 1, QualityTier: 1},
	})
	d := r.Pick("prompt")
	if d.WorkerID != 1 {
		t.Fatalf("idle tie chose worker %d, want 1 (faster service estimate)", d.WorkerID)
	}
}

// TestIdleTieBreakHonorsCapacity proves capacity participates in the tie: equal
// service time but worker 1 has 4x capacity, so it must win.
func TestIdleTieBreakHonorsCapacity(t *testing.T) {
	workers := []*Worker{{ID: 0}, {ID: 1}}
	r, err := New(Config{Method: MethodLeastInflight}, workers)
	if err != nil {
		t.Fatal(err)
	}
	r.EnableMulti(MultiSLOConfig{SLOTargetMs: 80}, []WorkerStatic{
		{LatencyPriorMs: 50, Capacity: 1, CostPerReq: 1, QualityTier: 1},
		{LatencyPriorMs: 50, Capacity: 4, CostPerReq: 1, QualityTier: 1},
	})
	if d := r.Pick("p"); d.WorkerID != 1 {
		t.Fatalf("chose worker %d, want 1 (4x capacity)", d.WorkerID)
	}
}

// TestIdleTieBreakDeterministicWhenIdentical keeps behaviour stable when the
// workers are genuinely identical: lowest ID wins.
func TestIdleTieBreakDeterministicWhenIdentical(t *testing.T) {
	workers := []*Worker{{ID: 0}, {ID: 1}}
	r, err := New(Config{Method: MethodLeastInflight}, workers)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 5; i++ {
		if d := r.Pick("p"); d.WorkerID != 0 {
			t.Fatalf("iteration %d chose worker %d, want deterministic 0", i, d.WorkerID)
		}
	}
}

// TestNeedsWorkerStaticCoversCapacityPolicies guards the gateway wiring
// condition: every policy that reads capacity must be listed.
func TestNeedsWorkerStaticCoversCapacityPolicies(t *testing.T) {
	for _, m := range []Method{MethodMultiSLO, MethodCapacityNormalizedLeastInflight, MethodShortestEstimatedWorkload} {
		if !NeedsWorkerStatic(m) {
			t.Fatalf("NeedsWorkerStatic(%s) = false, want true", m)
		}
	}
	for _, m := range []Method{MethodRoundRobin, MethodLeastInflight} {
		if NeedsWorkerStatic(m) {
			t.Fatalf("NeedsWorkerStatic(%s) = true, want false", m)
		}
	}
}

// TestCapacityNormalizedUsesConfiguredCapacity proves the capacity-aware policy
// actually differentiates workers once multi state is enabled: worker 1 has 4x
// capacity and equal inflight, so normalized load is lower there.
func TestCapacityNormalizedUsesConfiguredCapacity(t *testing.T) {
	workers := []*Worker{{ID: 0}, {ID: 1}}
	r, err := New(Config{Method: MethodCapacityNormalizedLeastInflight}, workers)
	if err != nil {
		t.Fatal(err)
	}
	r.EnableMulti(MultiSLOConfig{SLOTargetMs: 80}, []WorkerStatic{
		{Capacity: 1, LatencyPriorMs: 50, CostPerReq: 1, QualityTier: 1},
		{Capacity: 4, LatencyPriorMs: 50, CostPerReq: 1, QualityTier: 1},
	})
	r.Acquire(0)
	r.Acquire(1)
	d := r.Pick("p")
	if d.WorkerID != 1 {
		t.Fatalf("capacity_normalized chose worker %d with inflight 1/1 and capacity 1/4, want 1", d.WorkerID)
	}
	if len(d.Scores) != 2 {
		t.Fatalf("expected per-worker scores, got %d", len(d.Scores))
	}
}

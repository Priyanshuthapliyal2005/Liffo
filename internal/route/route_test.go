package route

import (
	"fmt"
	"strings"
	"testing"
	"time"
)

func TestPrefixSHANoRawStorage(t *testing.T) {
	a := PrefixSHA256("hello world shared prefix stuff", 256)
	b := PrefixSHA256("hello world shared prefix stuff"+" and more suffix", 256)
	// Same first 256 bytes → same hash when prefix longer than content
	if a == "" || len(a) != 64 {
		t.Fatalf("bad sha %q", a)
	}
	// longer content with same 256-byte prefix
	long := strings.Repeat("x", 300)
	h1 := PrefixSHA256(long+"aaa", 256)
	h2 := PrefixSHA256(long+"bbb", 256)
	if h1 != h2 {
		t.Fatal("prefix hash should ignore beyond PrefixBytes")
	}
	_ = b
}

func TestPrefixStickyBindsAndHits(t *testing.T) {
	w0 := &Worker{ID: 0, BaseURL: "http://127.0.0.1:8081"}
	w1 := &Worker{ID: 1, BaseURL: "http://127.0.0.1:8082"}
	r, err := New(Config{Method: MethodPrefixSticky, PrefixBytes: 64}, []*Worker{w0, w1})
	if err != nil {
		t.Fatal(err)
	}
	// Cold
	d1 := r.Pick("PREFIX_A" + strings.Repeat("a", 80) + " Q1")
	if d1.StickyHit || d1.Fallback != "cold" {
		t.Fatalf("want cold, got %+v", d1)
	}
	id := d1.WorkerID
	// Same prefix → sticky hit
	d2 := r.Pick("PREFIX_A" + strings.Repeat("a", 80) + " Q2")
	if !d2.StickyHit || d2.WorkerID != id {
		t.Fatalf("want sticky hit same worker, got %+v", d2)
	}
	// Different prefix may go elsewhere
	d3 := r.Pick("PREFIX_B" + strings.Repeat("b", 80) + " Q1")
	if d3.StickyHit {
		t.Fatalf("new prefix should be cold: %+v", d3)
	}
	if r.StickyCount() < 2 {
		t.Fatalf("sticky count %d", r.StickyCount())
	}
}

func TestLeastInflight(t *testing.T) {
	w0 := &Worker{ID: 0, BaseURL: "u0"}
	w1 := &Worker{ID: 1, BaseURL: "u1"}
	w0.inflight.Store(5)
	w1.inflight.Store(1)
	r, _ := New(Config{Method: MethodLeastInflight}, []*Worker{w0, w1})
	d := r.Pick("anything")
	if d.WorkerID != 1 {
		t.Fatalf("want worker 1 least loaded, got %d", d.WorkerID)
	}
}

func TestCapacityNormalizedLeastInflightPrefersMoreCapableWorker(t *testing.T) {
	w0 := &Worker{ID: 0, BaseURL: "u0"}
	w1 := &Worker{ID: 1, BaseURL: "u1"}
	w0.inflight.Store(4)
	w1.inflight.Store(2)
	cfg := Defaults()
	cfg.Method = MethodCapacityNormalizedLeastInflight
	r, err := NewWithMulti(cfg, []*Worker{w0, w1}, defaultMultiSLO(), []WorkerStatic{
		{Capacity: 4, LatencyPriorMs: 50},
		{Capacity: 1, LatencyPriorMs: 50},
	})
	if err != nil {
		t.Fatal(err)
	}
	if d := r.Pick("anything"); d.WorkerID != 0 {
		t.Fatalf("capacity-normalized load should choose w0 (4/4 < 2/1), got %+v", d)
	}
}

func TestShortestEstimatedWorkloadPrefersLowerQueuedServiceTime(t *testing.T) {
	w0 := &Worker{ID: 0, BaseURL: "u0"}
	w1 := &Worker{ID: 1, BaseURL: "u1"}
	w0.inflight.Store(2)
	w1.inflight.Store(1)
	cfg := Defaults()
	cfg.Method = MethodShortestEstimatedWorkload
	r, err := NewWithMulti(cfg, []*Worker{w0, w1}, defaultMultiSLO(), []WorkerStatic{
		{Capacity: 1, LatencyPriorMs: 100},
		{Capacity: 1, LatencyPriorMs: 300},
	})
	if err != nil {
		t.Fatal(err)
	}
	if d := r.Pick("anything"); d.WorkerID != 0 {
		t.Fatalf("estimated workload should choose w0 (2*100ms < 1*300ms), got %+v", d)
	}
}

func TestStickyFallbackOverload(t *testing.T) {
	w0 := &Worker{ID: 0, BaseURL: "u0", MaxInflight: 1}
	w1 := &Worker{ID: 1, BaseURL: "u1", MaxInflight: 4}
	r, _ := New(Config{Method: MethodPrefixSticky, PrefixBytes: 32}, []*Worker{w0, w1})
	d1 := r.Pick("same-prefix-body-xxxxxx Q1")
	// Force sticky on w0 if not already
	r.bindSticky(d1.PrefixSHA, 0)
	w0.inflight.Store(1) // at cap
	d2 := r.Pick("same-prefix-body-xxxxxx Q2")
	if d2.WorkerID == 0 && d2.Fallback != "overload" {
		// should not stick to overloaded w0 without fallback flag
		t.Fatalf("expected overload fallback away from w0, got %+v", d2)
	}
	if d2.Fallback != "overload" && d2.WorkerID == 0 {
		t.Fatalf("stuck on overloaded worker: %+v", d2)
	}
}

func TestUnhealthyCooldownNoRestart(t *testing.T) {
	w0 := &Worker{ID: 0, BaseURL: "u0"}
	w1 := &Worker{ID: 1, BaseURL: "u1"}
	r, _ := New(Config{Method: MethodLeastInflight, FailCooldown: 50 * time.Millisecond}, []*Worker{w0, w1})
	r.Acquire(0)
	r.Release(0, false) // mark fail — no process kill
	if w0.Healthy() {
		t.Fatal("should be unhealthy briefly")
	}
	d := r.Pick("x")
	if d.WorkerID != 1 {
		t.Fatalf("should prefer healthy w1, got %d", d.WorkerID)
	}
	time.Sleep(60 * time.Millisecond)
	if !w0.Healthy() {
		t.Fatal("cooldown should expire without restart")
	}
}

func TestRoundRobin(t *testing.T) {
	w0 := &Worker{ID: 0, BaseURL: "u0"}
	w1 := &Worker{ID: 1, BaseURL: "u1"}
	r, _ := New(Config{Method: MethodRoundRobin}, []*Worker{w0, w1})
	seen := map[int]int{}
	for i := 0; i < 20; i++ {
		d := r.Pick("z")
		seen[d.WorkerID]++
	}
	if seen[0] == 0 || seen[1] == 0 {
		t.Fatalf("rr imbalance: %v", seen)
	}
}

func TestColdPrefixFanOut(t *testing.T) {
	w0 := &Worker{ID: 0, BaseURL: "u0"}
	w1 := &Worker{ID: 1, BaseURL: "u1"}
	r, _ := New(Config{Method: MethodPrefixSticky, PrefixBytes: 64}, []*Worker{w0, w1})
	// Two clearly different long prefixes should not always land on worker 0.
	seen := map[int]bool{}
	for i := 0; i < 8; i++ {
		p := fmt.Sprintf("COLD-PREFIX-%d-", i) + strings.Repeat(string(rune('a'+i)), 80)
		d := r.Pick(p + " Q")
		seen[d.WorkerID] = true
	}
	if len(seen) < 2 {
		t.Fatalf("expected cold prefixes to fan out across workers, got %v", seen)
	}
}

func TestResetLearning(t *testing.T) {
	w0 := &Worker{ID: 0, BaseURL: "u0"}
	r, _ := New(Config{Method: MethodPrefixSticky, PrefixBytes: 32}, []*Worker{w0})
	_ = r.Pick("abc" + strings.Repeat("x", 40))
	if r.StickyCount() == 0 {
		t.Fatal("expected sticky entry")
	}
	w0.served.Store(5)
	r.ResetLearning()
	if r.StickyCount() != 0 || w0.Served() != 0 {
		t.Fatalf("reset failed sticky=%d served=%d", r.StickyCount(), w0.Served())
	}
}

func TestHybridKeepsFreshAffinity(t *testing.T) {
	w0 := &Worker{ID: 0, BaseURL: "u0", MaxInflight: 4}
	w1 := &Worker{ID: 1, BaseURL: "u1", MaxInflight: 4}
	r, _ := New(Config{
		Method: MethodHybrid, PrefixBytes: 32,
		WeightCache: 0.35, WeightLoad: 0.35, WeightAge: 0.15, WeightHealth: 0.15,
		CacheAgeHalfLife: 5 * time.Second,
	}, []*Worker{w0, w1})
	prefix := "HYB-PREFIX-" + strings.Repeat("z", 40)
	d1 := r.Pick(prefix + " Q1")
	// Same prefix, both idle → should keep affinity on d1 worker
	d2 := r.Pick(prefix + " Q2")
	if d2.WorkerID != d1.WorkerID {
		t.Fatalf("expected keep affinity, d1=%d d2=%d reason=%s", d1.WorkerID, d2.WorkerID, d2.Reason)
	}
	if d2.SwitchedAway {
		t.Fatalf("should not switch when both idle: %s", d2.Reason)
	}
	if !d2.StickyHit {
		t.Fatalf("expected sticky/affinity hit: %+v", d2)
	}
	if d2.Reason == "" || len(d2.Scores) != 2 {
		t.Fatalf("expected reason and scores: reason=%q scores=%d", d2.Reason, len(d2.Scores))
	}
}

func TestHybridSwitchesOnOverload(t *testing.T) {
	w0 := &Worker{ID: 0, BaseURL: "u0", MaxInflight: 1}
	w1 := &Worker{ID: 1, BaseURL: "u1", MaxInflight: 4}
	r, _ := New(Config{
		Method: MethodHybrid, PrefixBytes: 32,
		WeightCache: 0.35, WeightLoad: 0.35, WeightAge: 0.15, WeightHealth: 0.15,
		CacheAgeHalfLife: 5 * time.Second,
	}, []*Worker{w0, w1})
	prefix := "HOT-PREFIX-" + strings.Repeat("h", 40)
	// Force affinity onto w0
	r.bindSticky(PrefixSHA256(prefix+" Q", 32), 0)
	w0.inflight.Store(1) // at max → overloaded
	d := r.Pick(prefix + " Q99")
	if d.WorkerID != 1 {
		t.Fatalf("expected switch to w1 under overload, got w%d reason=%s scores=%+v", d.WorkerID, d.Reason, d.Scores)
	}
	if !d.SwitchedAway {
		t.Fatalf("expected switched_away_from_cache true: %s", d.Reason)
	}
	if !strings.Contains(d.Reason, "SWITCHED") {
		t.Fatalf("reason should mention SWITCHED: %s", d.Reason)
	}
	if d.Fallback != "overload" {
		t.Fatalf("expected fallback=overload when leaving overloaded affinity, got %q", d.Fallback)
	}
}

func TestHybridAvoidsUnhealthy(t *testing.T) {
	w0 := &Worker{ID: 0, BaseURL: "u0", MaxInflight: 4}
	w1 := &Worker{ID: 1, BaseURL: "u1", MaxInflight: 4}
	r, _ := New(Config{Method: MethodHybrid, PrefixBytes: 32}, []*Worker{w0, w1})
	prefix := "UNH-" + strings.Repeat("u", 40)
	r.bindSticky(PrefixSHA256(prefix+" Q", 32), 0)
	w0.unhealthyUntil.Store(time.Now().Add(time.Hour).UnixNano())
	d := r.Pick(prefix + " Q")
	if d.WorkerID != 1 {
		t.Fatalf("expected healthy w1, got %d reason=%s", d.WorkerID, d.Reason)
	}
	if !d.SwitchedAway {
		t.Fatalf("expected switch from unhealthy affinity")
	}
	// Bugfix: scores showed healthy=false but fallback stayed hybrid_switch,
	// so ExpSummary.fallback_unhealthy never incremented.
	if d.Fallback != "unhealthy" {
		t.Fatalf("expected fallback=unhealthy, got %q reason=%s", d.Fallback, d.Reason)
	}
}

func TestStickyUnhealthyPreferredOverOverload(t *testing.T) {
	w0 := &Worker{ID: 0, BaseURL: "u0", MaxInflight: 1}
	w1 := &Worker{ID: 1, BaseURL: "u1", MaxInflight: 4}
	r, _ := New(Config{Method: MethodPrefixSticky, PrefixBytes: 32}, []*Worker{w0, w1})
	prefix := "BOTH-" + strings.Repeat("b", 40)
	sha := PrefixSHA256(prefix+" Q", 32)
	r.bindSticky(sha, 0)
	w0.inflight.Store(1)
	w0.unhealthyUntil.Store(time.Now().Add(time.Hour).UnixNano())
	d := r.Pick(prefix + " Q")
	if d.Fallback != "unhealthy" {
		t.Fatalf("unhealthy must win over overload, got fallback=%q worker=%d", d.Fallback, d.WorkerID)
	}
	if d.WorkerID != 1 {
		t.Fatalf("expected route to healthy w1, got %d", d.WorkerID)
	}
}

func TestStatsPrivacy(t *testing.T) {
	w0 := &Worker{ID: 0, BaseURL: "http://127.0.0.1:1"}
	r, _ := New(Defaults(), []*Worker{w0})
	_ = r.Pick("secret prompt text that must not appear in stats")
	st := r.Stats()
	raw, _ := st.WorkerStats[0].BaseURL, st.StickyCount
	if strings.Contains(raw, "secret") {
		t.Fatal("stats leaked prompt")
	}
	// sticky map keys are hashes only
	r.mu.Lock()
	for k := range r.sticky {
		if strings.Contains(k, "secret") || len(k) != 64 {
			t.Fatalf("sticky key not a pure hash: %q", k)
		}
	}
	r.mu.Unlock()
}

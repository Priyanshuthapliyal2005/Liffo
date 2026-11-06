package modellife

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func resolveResultsRaw() string {
	candidates := []string{
		filepath.Join("..", "..", "..", "..", "results", "raw"), // internal/modellife
		filepath.Join("..", "..", "..", "results", "raw"),
		filepath.Join("results", "raw"),
	}
	for _, c := range candidates {
		parent := filepath.Dir(c)
		if st, err := os.Stat(parent); err == nil && st.IsDir() {
			return c
		}
		if st, err := os.Stat(filepath.Dir(parent)); err == nil && st.IsDir() {
			return c
		}
	}
	return filepath.Join("results", "raw")
}

func writeEvidence(t *testing.T, name string, events []Event, summary map[string]any) (jsonlPath, sumPath string) {
	t.Helper()
	outDir := resolveResultsRaw()
	_ = os.MkdirAll(outDir, 0o755)
	ts := time.Now().UTC().Format("20060102-150405")
	jsonlPath = filepath.Join(outDir, name+"-"+ts+".jsonl")
	sumPath = filepath.Join(outDir, name+"-"+ts+"-summary.json")
	f, err := os.Create(jsonlPath)
	if err != nil {
		t.Fatalf("create evidence: %v", err)
	}
	enc := json.NewEncoder(f)
	for _, e := range events {
		_ = enc.Encode(e)
	}
	_ = f.Close()
	summary["path"] = jsonlPath
	summary["events"] = len(events)
	summary["ts"] = ts
	sb, _ := json.MarshalIndent(summary, "", "  ")
	if err := os.WriteFile(sumPath, sb, 0o644); err != nil {
		t.Fatalf("summary: %v", err)
	}
	t.Logf("evidence jsonl=%s summary=%s", jsonlPath, sumPath)
	return jsonlPath, sumPath
}

func stateOrder(events []Event) []string {
	var out []string
	for _, e := range events {
		if e.Kind == "transition" && e.To != "" {
			out = append(out, string(e.To))
		}
	}
	return out
}

func containsSeq(order []string, want ...string) bool {
	if len(want) == 0 {
		return true
	}
	wi := 0
	for _, s := range order {
		if s == want[wi] {
			wi++
			if wi == len(want) {
				return true
			}
		}
	}
	return false
}

func TestStatesCanonical(t *testing.T) {
	s := States()
	want := map[State]bool{
		StateLoading: true, StateReady: true, StateDraining: true,
		StateEvicting: true, StateFailed: true, StateStopped: true,
	}
	if len(s) != 6 {
		t.Fatalf("want 6 states, got %v", s)
	}
	for _, st := range s {
		if !want[st] {
			t.Fatalf("unexpected state %s", st)
		}
	}
}

func TestLoadReadyConcurrentRequests(t *testing.T) {
	fl := NewFakeLoader()
	fl.LoadDelay = 20 * time.Millisecond
	m, err := New(Config{LoadTimeout: 2 * time.Second}, fl)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()

	ctx := context.Background()
	if err := m.EnqueueLoad(ctx, "qwen-0.5b"); err != nil {
		t.Fatalf("load: %v", err)
	}
	if m.Snapshot().State != StateReady {
		t.Fatalf("want ready, got %+v", m.Snapshot())
	}

	const n = 16
	var wg sync.WaitGroup
	var ok atomic.Int64
	var fail atomic.Int64
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			lease, err := m.Acquire(ctx)
			if err != nil {
				fail.Add(1)
				return
			}
			time.Sleep(30 * time.Millisecond)
			lease.Release()
			ok.Add(1)
		}()
	}
	wg.Wait()
	if ok.Load() != n || fail.Load() != 0 {
		t.Fatalf("concurrent admit ok=%d fail=%d want ok=%d", ok.Load(), fail.Load(), n)
	}
	if m.Snapshot().Inflight != 0 {
		t.Fatalf("inflight leak: %d", m.Snapshot().Inflight)
	}

	events := m.Events()
	writeEvidence(t, "modellife-concurrent", events, map[string]any{
		"scenario":       "concurrent_requests_on_ready",
		"concurrent":     n,
		"admit_ok":       ok.Load(),
		"admit_fail":     fail.Load(),
		"final":          m.Snapshot(),
		"loads":          fl.Loads.Load(),
		"state_sequence": stateOrder(events),
	})
}

func TestDrainThenEvictBeforeQueuedLoad(t *testing.T) {
	// Inngest-style: requests running, model upload/load queued → drain → evict → load.
	fl := NewFakeLoader()
	fl.LoadDelay = 15 * time.Millisecond
	fl.UnloadDelay = 10 * time.Millisecond
	m, err := New(Config{LoadTimeout: 2 * time.Second, EvictTimeout: 2 * time.Second}, fl)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()

	ctx := context.Background()
	if err := m.EnqueueLoad(ctx, "model-a"); err != nil {
		t.Fatal(err)
	}

	// Hold concurrent inflight on model-a
	var leases []*Lease
	for i := 0; i < 4; i++ {
		l, err := m.Acquire(ctx)
		if err != nil {
			t.Fatalf("acquire %d: %v", i, err)
		}
		leases = append(leases, l)
	}
	if m.Snapshot().Inflight != 4 {
		t.Fatalf("inflight=%d", m.Snapshot().Inflight)
	}

	// Queue replace while busy — should not admit new, should drain then evict then load B
	loadDone := make(chan error, 1)
	go func() {
		loadDone <- m.EnqueueLoad(ctx, "model-b")
	}()

	// Give worker time to enter draining
	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		if m.Snapshot().State == StateDraining {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if m.Snapshot().State != StateDraining {
		t.Fatalf("expected draining after queued load, got %+v", m.Snapshot())
	}
	// New admits rejected during drain
	if _, err := m.Acquire(ctx); !errors.Is(err, ErrDraining) {
		t.Fatalf("want ErrDraining during drain, got %v", err)
	}

	// Finish in-flight slowly — order must stay drain before evict before ready B
	time.Sleep(40 * time.Millisecond)
	for _, l := range leases {
		l.Release()
		time.Sleep(5 * time.Millisecond)
	}

	select {
	case err := <-loadDone:
		if err != nil {
			t.Fatalf("queued load: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timeout waiting for model-b load after drain")
	}

	snap := m.Snapshot()
	if snap.State != StateReady || snap.Model != "model-b" {
		t.Fatalf("want ready model-b, got %+v", snap)
	}
	if fl.Resident() != "model-b" {
		t.Fatalf("loader resident=%q", fl.Resident())
	}
	if fl.Unloads.Load() < 1 {
		t.Fatalf("expected unload of model-a, unloads=%d", fl.Unloads.Load())
	}

	events := m.Events()
	order := stateOrder(events)
	// Must see draining before evicting before loading (for B) before ready
	if !containsSeq(order, string(StateDraining), string(StateEvicting), string(StateLoading), string(StateReady)) {
		t.Fatalf("state order missing drain→evict→load→ready: %v", order)
	}
	// Evidence: first drain_complete before first evict transition after queue of model-b
	var sawQueueB, sawDrainComplete, sawEvictAfterDrain bool
	for _, e := range events {
		if e.Kind == "queue" && e.Model == "model-b" {
			sawQueueB = true
		}
		if sawQueueB && e.Kind == "drain_complete" {
			sawDrainComplete = true
		}
		if sawDrainComplete && e.Kind == "transition" && e.To == StateEvicting {
			sawEvictAfterDrain = true
			break
		}
	}
	if !sawQueueB || !sawDrainComplete || !sawEvictAfterDrain {
		t.Fatalf("drain-before-evict evidence missing queueB=%v drain=%v evict=%v", sawQueueB, sawDrainComplete, sawEvictAfterDrain)
	}

	writeEvidence(t, "modellife-drain-evict-load", events, map[string]any{
		"scenario":           "inngest_queue_drain_then_evict_then_load",
		"inflight_at_queue":  4,
		"final":              snap,
		"state_sequence":     order,
		"unloads":            fl.Unloads.Load(),
		"loads":              fl.Loads.Load(),
		"drain_before_evict": true,
	})
}

func TestFailedLoad(t *testing.T) {
	fl := NewFakeLoader()
	fl.FailModels["bad-gguf"] = "checksum mismatch"
	m, err := New(Config{LoadTimeout: time.Second}, fl)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()

	err = m.EnqueueLoad(context.Background(), "bad-gguf")
	if err == nil {
		t.Fatal("expected load failure")
	}
	snap := m.Snapshot()
	if snap.State != StateFailed {
		t.Fatalf("want failed, got %+v", snap)
	}
	if snap.LastError == "" {
		t.Fatal("expected last_error set")
	}
	// Must not admit while failed
	if _, aerr := m.Acquire(context.Background()); !errors.Is(aerr, ErrFailed) {
		t.Fatalf("want ErrFailed, got %v", aerr)
	}

	events := m.Events()
	writeEvidence(t, "modellife-failed-load", events, map[string]any{
		"scenario":       "failed_load",
		"error":          err.Error(),
		"final":          snap,
		"state_sequence": stateOrder(events),
	})
}

func TestLoadTimeout(t *testing.T) {
	fl := NewFakeLoader()
	fl.HangModels["slow"] = true
	m, err := New(Config{LoadTimeout: 80 * time.Millisecond}, fl)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()

	start := time.Now()
	err = m.EnqueueLoad(context.Background(), "slow")
	elapsed := time.Since(start)
	if err == nil {
		t.Fatal("expected timeout error")
	}
	if !errors.Is(err, ErrLoadTimeout) && !errors.Is(err, context.DeadlineExceeded) {
		// wrapped: errors.Is should work on %w
		if !containsErr(err, ErrLoadTimeout) {
			t.Fatalf("want load timeout, got %v", err)
		}
	}
	if elapsed > 500*time.Millisecond {
		t.Fatalf("timeout too slow: %s", elapsed)
	}
	if m.Snapshot().State != StateFailed {
		t.Fatalf("want failed after timeout, got %+v", m.Snapshot())
	}

	events := m.Events()
	writeEvidence(t, "modellife-load-timeout", events, map[string]any{
		"scenario":       "load_timeout",
		"error":          err.Error(),
		"elapsed_ms":     elapsed.Milliseconds(),
		"load_timeout":   80,
		"final":          m.Snapshot(),
		"state_sequence": stateOrder(events),
	})
}

func containsErr(err error, target error) bool {
	for err != nil {
		if errors.Is(err, target) {
			return true
		}
		// unwrap single
		type unwrapper interface{ Unwrap() error }
		if u, ok := err.(unwrapper); ok {
			err = u.Unwrap()
			continue
		}
		// string fallback
		return false
	}
	return false
}

func TestRecoveryAfterFailedLoad(t *testing.T) {
	fl := NewFakeLoader()
	fl.FailModels["bad"] = "corrupt weights"
	m, err := New(Config{LoadTimeout: time.Second}, fl)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()

	ctx := context.Background()
	if err := m.EnqueueLoad(ctx, "bad"); err == nil {
		t.Fatal("expected fail")
	}
	if m.Snapshot().State != StateFailed {
		t.Fatalf("want failed: %+v", m.Snapshot())
	}

	// Recovery: load good model
	if err := m.EnqueueLoad(ctx, "good"); err != nil {
		t.Fatalf("recovery load: %v", err)
	}
	snap := m.Snapshot()
	if snap.State != StateReady || snap.Model != "good" {
		t.Fatalf("want ready good, got %+v", snap)
	}
	lease, err := m.Acquire(ctx)
	if err != nil {
		t.Fatalf("admit after recovery: %v", err)
	}
	lease.Release()

	events := m.Events()
	order := stateOrder(events)
	if !containsSeq(order, string(StateLoading), string(StateFailed), string(StateLoading), string(StateReady)) {
		t.Fatalf("recovery sequence missing: %v", order)
	}

	writeEvidence(t, "modellife-recovery", events, map[string]any{
		"scenario":       "failed_then_recovery_load",
		"final":          snap,
		"state_sequence": order,
		"loads":          fl.Loads.Load(),
	})
}

func TestEvictDrainsInflight(t *testing.T) {
	fl := NewFakeLoader()
	m, err := New(Config{}, fl)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	ctx := context.Background()
	_ = m.EnqueueLoad(ctx, "m1")
	l, _ := m.Acquire(ctx)

	evictDone := make(chan error, 1)
	go func() { evictDone <- m.EnqueueEvict(ctx) }()

	deadline := time.Now().Add(300 * time.Millisecond)
	for time.Now().Before(deadline) && m.Snapshot().State != StateDraining {
		time.Sleep(5 * time.Millisecond)
	}
	if m.Snapshot().State != StateDraining {
		t.Fatalf("want draining, got %+v", m.Snapshot())
	}
	l.Release()
	select {
	case err := <-evictDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("evict timeout")
	}
	if m.Snapshot().State != StateStopped {
		t.Fatalf("want stopped, got %+v", m.Snapshot())
	}

	events := m.Events()
	writeEvidence(t, "modellife-evict-drain", events, map[string]any{
		"scenario":       "evict_waits_for_inflight_drain",
		"final":          m.Snapshot(),
		"state_sequence": stateOrder(events),
	})
}

// TestLifecycleSuiteEvidence runs a multi-phase scenario and writes one combined evidence pack.
func TestLifecycleSuiteEvidence(t *testing.T) {
	fl := NewFakeLoader()
	fl.LoadDelay = 10 * time.Millisecond
	m, err := New(Config{LoadTimeout: 200 * time.Millisecond, EvictTimeout: time.Second}, fl)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	ctx := context.Background()

	phases := []string{}
	record := func(p string) { phases = append(phases, p) }

	// 1 concurrent ready
	record("load_ok")
	if err := m.EnqueueLoad(ctx, "suite-a"); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			l, err := m.Acquire(ctx)
			if err != nil {
				return
			}
			time.Sleep(15 * time.Millisecond)
			l.Release()
		}()
	}
	wg.Wait()
	record("concurrent_done")

	// 2 drain-evict-replace
	record("replace_start")
	l1, _ := m.Acquire(ctx)
	l2, _ := m.Acquire(ctx)
	done := make(chan error, 1)
	go func() { done <- m.EnqueueLoad(ctx, "suite-b") }()
	time.Sleep(20 * time.Millisecond)
	l1.Release()
	l2.Release()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	record("replace_done")

	// 3 fail + recover
	fl.FailModels["suite-bad"] = "boom"
	record("fail_load")
	_ = m.EnqueueLoad(ctx, "suite-bad")
	if m.Snapshot().State != StateFailed {
		t.Fatalf("want failed got %+v", m.Snapshot())
	}
	record("recover")
	if err := m.EnqueueLoad(ctx, "suite-c"); err != nil {
		t.Fatal(err)
	}
	// 4 timeout
	fl.HangModels["suite-hang"] = true
	record("timeout")
	terr := m.EnqueueLoad(ctx, "suite-hang")
	if terr == nil {
		t.Fatal("expected timeout")
	}
	// recover again
	record("recover_after_timeout")
	// clear hang not needed for other models
	if err := m.EnqueueLoad(ctx, "suite-final"); err != nil {
		t.Fatal(err)
	}
	if m.Snapshot().State != StateReady {
		t.Fatalf("final ready want, got %+v", m.Snapshot())
	}
	record("done")

	events := m.Events()
	writeEvidence(t, "modellife-suite", events, map[string]any{
		"scenario":       "suite_concurrent_drain_fail_timeout_recovery",
		"phases":         phases,
		"final":          m.Snapshot(),
		"state_sequence": stateOrder(events),
		"loads":          fl.Loads.Load(),
		"unloads":        fl.Unloads.Load(),
		"timeout_error":  terr.Error(),
	})
}

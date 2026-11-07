package adapt

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/Priyanshuthapliyal2005/Liffo/internal/route"
)

func TestNoThrashOnNoiseAndModeLabels(t *testing.T) {
	f := DefaultFilter()
	f.MinDwell = 200 * time.Millisecond
	f.CooldownCooldown = 200 * time.Millisecond
	f.CapacityMinDwell = 500 * time.Millisecond
	f.ConfirmTicks = 3
	f.MinSamples = 4
	f.EMAAlpha = 0.25
	c := NewController(Targets{TTFTP95Ms: 80, ErrorRate: 0.05, MaxInflightFloor: 1, MaxInflightCap: 6},
		f, Policy{RouteMethod: route.MethodHybrid, MaxInflight: 2, RetryEnabled: true, Mode: "steady"})

	// Mixed noise around target — should not thrash capacity/method
	for i := 0; i < 40; i++ {
		ttft := 50.0
		if i%5 == 0 {
			ttft = 90 // brief blip above enter
		}
		c.Observe(Sample{TTFTMs: ttft, HealthyWorkers: 2, TotalWorkers: 2, InflightSeen: 1})
	}
	// Without sustained confirm after dwell, effective switches should be few
	if c.EffectiveSwitchCount() > 3 {
		t.Fatalf("too many effective switches on noise: %d suppress=%d", c.EffectiveSwitchCount(), c.SuppressedCount())
	}
	if c.SuppressedCount() < 1 {
		t.Logf("warn: expected some suppressions on blips")
	}
}

func TestHysteresisBlocksFlipFlopNearThreshold(t *testing.T) {
	f := DefaultFilter()
	f.MinDwell = 100 * time.Millisecond
	f.CooldownCooldown = 100 * time.Millisecond
	f.CapacityMinDwell = 10 * time.Second // freeze capacity
	f.ConfirmTicks = 2
	f.MinSamples = 3
	f.EMAAlpha = 0.5
	f.TTFTEnterFrac = 1.0
	f.TTFTExitFrac = 0.7
	c := NewController(Targets{TTFTP95Ms: 100, ErrorRate: 0.1, MaxInflightFloor: 1, MaxInflightCap: 4},
		f, Policy{RouteMethod: route.MethodHybrid, MaxInflight: 2, RetryEnabled: true, Mode: "steady"})

	for i := 0; i < 5; i++ {
		c.Observe(Sample{TTFTMs: 40, HealthyWorkers: 2, TotalWorkers: 2})
	}
	// Push into protect
	time.Sleep(120 * time.Millisecond)
	var entered bool
	for i := 0; i < 10; i++ {
		s := c.Observe(Sample{TTFTMs: 150, HealthyWorkers: 2, TotalWorkers: 2, InflightSeen: 2})
		if s.EffectiveChange && s.Policy.Mode == "protect_ttft" {
			entered = true
			break
		}
	}
	if !entered {
		t.Fatalf("expected enter protect_ttft, pol=%+v", c.Policy())
	}
	// Oscillate slightly below enter but above exit — should NOT leave protect immediately
	time.Sleep(120 * time.Millisecond)
	switchesBefore := c.EffectiveSwitchCount()
	for i := 0; i < 15; i++ {
		c.Observe(Sample{TTFTMs: 85, HealthyWorkers: 2, TotalWorkers: 2, InflightSeen: 1}) // between exit(70) and enter(100)
	}
	if c.Policy().Mode != "protect_ttft" && c.Policy().RouteMethod != route.MethodLeastInflight {
		// still in protect band
		t.Logf("mode now %s method %s", c.Policy().Mode, c.Policy().RouteMethod)
	}
	// Should still be protect until under exit
	if c.Policy().RouteMethod != route.MethodLeastInflight {
		// if it flipped to hybrid, hysteresis failed
		if c.EffectiveSwitchCount() > switchesBefore+1 {
			t.Fatalf("flipped too eagerly in hysteresis band; switches %d→%d", switchesBefore, c.EffectiveSwitchCount())
		}
	}
	// Recover clearly under exit
	time.Sleep(120 * time.Millisecond)
	var recovered bool
	for i := 0; i < 12; i++ {
		s := c.Observe(Sample{TTFTMs: 40, HealthyWorkers: 2, TotalWorkers: 2})
		if s.EffectiveChange && s.Policy.RouteMethod == route.MethodHybrid {
			recovered = true
			break
		}
	}
	if !recovered {
		// allow more time
		for i := 0; i < 20; i++ {
			s := c.Observe(Sample{TTFTMs: 30, HealthyWorkers: 2, TotalWorkers: 2})
			if s.EffectiveChange && s.Policy.RouteMethod == route.MethodHybrid {
				recovered = true
				break
			}
			time.Sleep(50 * time.Millisecond)
		}
	}
	if !recovered {
		t.Fatalf("expected recovery to hybrid after clear under-exit; pol=%+v", c.Policy())
	}
}

func TestCapacityDoesNotOpenWhenCurrentLimitIsUnused(t *testing.T) {
	f := DefaultFilter()
	f.MinDwell = time.Nanosecond
	f.CooldownCooldown = time.Nanosecond
	f.CapacityMinDwell = time.Nanosecond
	f.ConfirmTicks = 1
	f.MinSamples = 1
	c := NewController(Targets{TTFTP95Ms: 100, ErrorRate: 0.1, MaxInflightFloor: 1, MaxInflightCap: 8},
		f, Policy{RouteMethod: route.MethodHybrid, MaxInflight: 2, RetryEnabled: true, Mode: "steady"})
	time.Sleep(time.Millisecond)
	for i := 0; i < 6; i++ {
		c.Observe(Sample{TTFTMs: 20, HealthyWorkers: 2, TotalWorkers: 2, InflightSeen: 0})
	}
	if got := c.Policy().MaxInflight; got != 2 {
		t.Fatalf("unused capacity must not open: got max_inflight=%d want=2", got)
	}
}

func TestCapacityNotOpenEveryDwell(t *testing.T) {
	f := DefaultFilter()
	f.MinDwell = 50 * time.Millisecond
	f.CooldownCooldown = 50 * time.Millisecond
	f.CapacityMinDwell = 300 * time.Millisecond
	f.ConfirmTicks = 2
	f.MinSamples = 3
	f.EMAAlpha = 0.4
	c := NewController(Targets{TTFTP95Ms: 100, ErrorRate: 0.1, MaxInflightFloor: 1, MaxInflightCap: 8},
		f, Policy{RouteMethod: route.MethodHybrid, MaxInflight: 1, RetryEnabled: true, Mode: "steady"})
	for i := 0; i < 5; i++ {
		c.Observe(Sample{TTFTMs: 20, HealthyWorkers: 2, TotalWorkers: 2, InflightSeen: 0})
	}
	// Rapid good samples — capacity should open slowly
	start := time.Now()
	for time.Since(start) < 400*time.Millisecond {
		c.Observe(Sample{TTFTMs: 20, HealthyWorkers: 2, TotalWorkers: 2, InflightSeen: 0})
		time.Sleep(20 * time.Millisecond)
	}
	inf := c.Policy().MaxInflight
	// In 400ms with CapMinDwell 300ms, at most +1 or +2 steps from 1
	if inf > 3 {
		t.Fatalf("capacity opened too fast: max_inflight=%d switches=%d", inf, c.EffectiveSwitchCount())
	}
}

func TestBurstAndRecoveryEvidence(t *testing.T) {
	f := DefaultFilter()
	f.MinDwell = 150 * time.Millisecond
	f.CooldownCooldown = 150 * time.Millisecond
	f.CapacityMinDwell = 250 * time.Millisecond
	f.ConfirmTicks = 3
	f.MinSamples = 4
	f.EMAAlpha = 0.3
	c := NewController(Targets{TTFTP95Ms: 60, ErrorRate: 0.05, MaxInflightFloor: 1, MaxInflightCap: 6},
		f, Policy{RouteMethod: route.MethodHybrid, MaxInflight: 3, RetryEnabled: true, Mode: "steady"})

	type row struct {
		I               int     `json:"i"`
		Phase           string  `json:"phase"`
		TTFT            float64 `json:"ttft_ms"`
		EMA             float64 `json:"ema_ttft_ms"`
		EMAErr          float64 `json:"ema_error_rate"`
		Healthy         int     `json:"healthy_workers"`
		InflightPolicy  int     `json:"policy_max_inflight"`
		Method          string  `json:"policy_route_method"`
		Mode            string  `json:"policy_mode"`
		Switched        bool    `json:"switched"`
		EffectiveChange bool    `json:"effective_change"`
		Suppressed      bool    `json:"suppressed"`
		SuppressWhy     string  `json:"suppress_why,omitempty"`
		ChangeReason    string  `json:"change_reason,omitempty"`
		ModeOnly        bool    `json:"mode_only,omitempty"`
		Urgency         string  `json:"urgency,omitempty"`
		FastPath        bool    `json:"fast_path,omitempty"`
		ElapsedMs       int64   `json:"elapsed_ms"`
	}
	var rows []row
	t0 := time.Now()
	observe := func(phase string, ttft float64, err bool, inf, healthy int) Snapshot {
		s := c.Observe(Sample{TTFTMs: ttft, Error: err, InflightSeen: inf, HealthyWorkers: healthy, TotalWorkers: 2})
		p := c.Policy()
		rows = append(rows, row{
			I: len(rows) + 1, Phase: phase, TTFT: ttft, EMA: s.EMA_TTFTMs, EMAErr: s.EMA_ErrorRate,
			Healthy: healthy, InflightPolicy: p.MaxInflight, Method: string(p.RouteMethod), Mode: p.Mode,
			Switched: s.Switched, EffectiveChange: s.EffectiveChange, Suppressed: s.Suppressed,
			SuppressWhy: s.SuppressWhy, ChangeReason: s.ChangeReason, ModeOnly: s.ModeOnly,
			Urgency: s.Urgency, FastPath: s.FastPath, ElapsedMs: time.Since(t0).Milliseconds(),
		})
		return s
	}

	// Warm steady (still hybrid)
	for i := 0; i < 8; i++ {
		observe("warm", 25, false, 1, 2)
	}

	// --- Critical: worker loss from hybrid — must FAST, not 3s dwell + 4 confirms ---
	workerFailStart := time.Now()
	var workerProtectMs int64 = -1
	var workerFast bool
	for i := 0; i < 4; i++ {
		s := observe("worker_fail", 35, false, 2, 1) // 1/2 healthy
		if s.EffectiveChange && s.Policy.Mode == "recover_worker" {
			workerProtectMs = time.Since(workerFailStart).Milliseconds()
			workerFast = s.FastPath || hasFASTPrefix(s.ChangeReason)
			break
		}
		// Mode-only if already least_inflight (should not happen from hybrid warm)
		if s.ModeOnly && c.Policy().Mode == "recover_worker" {
			workerProtectMs = time.Since(workerFailStart).Milliseconds()
			break
		}
	}
	if workerProtectMs < 0 {
		t.Fatalf("worker failure did not enter recover_worker; pol=%+v", c.Policy())
	}
	if workerProtectMs > 80 {
		t.Fatalf("worker failure protect too slow: %dms (expect FAST ~1 tick)", workerProtectMs)
	}
	if c.Policy().RouteMethod != route.MethodLeastInflight {
		t.Fatalf("worker fail want least_inflight, got %s", c.Policy().RouteMethod)
	}

	// --- Recovery after worker restored: do NOT pre-sleep dwell — leave-protect must wait MinDwell+confirm ---
	recoverStart := time.Now()
	var recoverMs int64 = -1
	recoverSuppress := 0
	for i := 0; i < 40; i++ {
		s := observe("recovery_worker", 28, false, 1, 2)
		if s.Suppressed {
			recoverSuppress++
		}
		if s.EffectiveChange && s.Policy.RouteMethod == route.MethodHybrid {
			recoverMs = time.Since(recoverStart).Milliseconds()
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if recoverMs < 0 {
		t.Fatalf("worker recovery did not return hybrid; pol=%+v", c.Policy())
	}
	// MinDwell=150ms + confirm ticks — must be clearly slower than worker FAST protect
	if recoverMs < int64(f.MinDwell/time.Millisecond) {
		t.Fatalf("leave-protect too eager: %dms < MinDwell=%dms (suppress=%d)", recoverMs, f.MinDwell.Milliseconds(), recoverSuppress)
	}
	if recoverMs <= workerProtectMs+50 {
		t.Fatalf("recovery (%dms) not slower than worker protect (%dms)", recoverMs, workerProtectMs)
	}
	if recoverSuppress < 2 {
		t.Fatalf("expected dwell/confirm suppressions on leave-protect, got %d", recoverSuppress)
	}

	// --- TTFT burst (mild/moderate): confirm suppressions OK; not critical FAST unless severe ---
	time.Sleep(160 * time.Millisecond)
	for i := 0; i < 18; i++ {
		observe("burst_ttft", 120, false, 4, 2)
		time.Sleep(20 * time.Millisecond)
	}

	// --- Final recovery + capacity opens ---
	time.Sleep(160 * time.Millisecond)
	for i := 0; i < 28; i++ {
		observe("recovery_ttft", 30, false, 1, 2)
		time.Sleep(20 * time.Millisecond)
	}

	eff := 0
	sup := 0
	modeOnly := 0
	fastEff := 0
	for _, r := range rows {
		if r.EffectiveChange {
			eff++
			if r.FastPath || hasFASTPrefix(r.ChangeReason) {
				fastEff++
			}
		}
		if r.Suppressed {
			sup++
		}
		if r.ModeOnly {
			modeOnly++
		}
	}
	// Burst+worker+recovery should not produce dozens of effective flips
	if eff > 10 {
		t.Fatalf("too many effective changes: %d (suppress=%d)", eff, sup)
	}
	if sup < 3 {
		t.Fatalf("expected multiple suppressions, got %d", sup)
	}
	if c.Policy().RouteMethod != route.MethodHybrid {
		t.Fatalf("after recovery want hybrid, got %+v", c.Policy())
	}

	// Resolve repo results/raw from package cwd (tools/bench/internal/adapt) or tools/bench
	outDir := resolveResultsRaw()
	_ = os.MkdirAll(outDir, 0o755)
	ts := time.Now().UTC().Format("20060102-150405")
	path := filepath.Join(outDir, "adapt-stability-burst-recovery-"+ts+".jsonl")
	wf, err := os.Create(path)
	if err != nil {
		path = filepath.Join(os.TempDir(), "adapt-stability-burst-recovery.jsonl")
		wf, err = os.Create(path)
		if err != nil {
			t.Fatal(err)
		}
	}
	enc := json.NewEncoder(wf)
	for _, r := range rows {
		_ = enc.Encode(r)
	}
	sum := map[string]any{
		"effective_switches":      eff,
		"fast_path_effective":     fastEff,
		"suppressions":            sup,
		"mode_only":               modeOnly,
		"final_policy":            c.Policy(),
		"rows":                    len(rows),
		"path":                    path,
		"worker_fail_protect_ms":  workerProtectMs,
		"worker_fail_fast":        workerFast,
		"recovery_to_hybrid_ms":   recoverMs,
		"recovery_suppress_count": recoverSuppress,
		"note":                    "suppressions are anti-thrash (confirm/dwell/capacity); critical worker loss uses FAST path; leave-protect stays slow",
	}
	_ = wf.Close()
	sb, _ := json.MarshalIndent(sum, "", "  ")
	sumPath := filepath.Join(filepath.Dir(path), "adapt-stability-burst-recovery-"+ts+"-summary.json")
	_ = os.WriteFile(sumPath, sb, 0o644)
	t.Logf("evidence raw=%s eff=%d sup=%d worker_protect_ms=%d worker_fast=%v recover_ms=%d final=%+v",
		path, eff, sup, workerProtectMs, workerFast, recoverMs, c.Policy())
}

func hasFASTPrefix(s string) bool {
	return len(s) >= 5 && s[:5] == "FAST:"
}

// resolveResultsRaw finds <repo>/results/raw from common test working directories.
func resolveResultsRaw() string {
	candidates := []string{
		filepath.Join("..", "..", "..", "..", "results", "raw"), // from internal/adapt
		filepath.Join("..", "..", "..", "results", "raw"),       // from tools/bench
		filepath.Join("results", "raw"),
	}
	for _, c := range candidates {
		// Prefer a path whose parent chain exists (repo has results/ or we can create raw)
		parent := filepath.Dir(c)
		if st, err := os.Stat(parent); err == nil && st.IsDir() {
			return c
		}
		// Also accept if we can create under an existing grandparent
		if st, err := os.Stat(filepath.Dir(parent)); err == nil && st.IsDir() {
			return c
		}
	}
	return filepath.Join("results", "raw")
}

// TestCriticalWorkerFailureNotSuppressed: worker loss must apply on first post-propose tick (FAST),
// not sit behind full MinDwell+ConfirmTicks.
func TestCriticalWorkerFailureNotSuppressed(t *testing.T) {
	f := DefaultFilter()
	// Production-like anti-thrash windows — critical must still be fast
	f.MinDwell = 3 * time.Second
	f.CooldownCooldown = 3 * time.Second
	f.CapacityMinDwell = 5 * time.Second
	f.ConfirmTicks = 4
	f.MinSamples = 4
	f.EMAAlpha = 0.25
	c := NewController(Targets{TTFTP95Ms: 80, ErrorRate: 0.05, MaxInflightFloor: 1, MaxInflightCap: 6},
		f, Policy{RouteMethod: route.MethodHybrid, MaxInflight: 4, RetryEnabled: true, Mode: "steady"})

	for i := 0; i < 6; i++ {
		c.Observe(Sample{TTFTMs: 30, HealthyWorkers: 2, TotalWorkers: 2, InflightSeen: 1})
	}
	// Still inside 3s dwell from construction — non-critical would be blocked; worker fail must not
	start := time.Now()
	var snap Snapshot
	for i := 0; i < 3; i++ {
		snap = c.Observe(Sample{TTFTMs: 40, HealthyWorkers: 1, TotalWorkers: 2, InflightSeen: 2})
		if snap.EffectiveChange {
			break
		}
	}
	elapsed := time.Since(start)
	if !snap.EffectiveChange {
		t.Fatalf("worker failure suppressed under dwell; pol=%+v hist_sup=%d", c.Policy(), c.SuppressedCount())
	}
	if c.Policy().RouteMethod != route.MethodLeastInflight {
		t.Fatalf("want least_inflight after worker loss, got %s", c.Policy().RouteMethod)
	}
	if c.Policy().Mode != "recover_worker" {
		t.Fatalf("want recover_worker mode, got %s", c.Policy().Mode)
	}
	if !snap.FastPath && !hasFASTPrefix(snap.ChangeReason) {
		t.Fatalf("expected FAST path for worker loss, got reason=%q fast=%v", snap.ChangeReason, snap.FastPath)
	}
	if elapsed > 100*time.Millisecond {
		t.Fatalf("worker failure response too slow under 3s dwell: %s", elapsed)
	}
	t.Logf("worker_fail protect elapsed=%s reason=%q", elapsed.Round(time.Millisecond), snap.ChangeReason)
}

// TestErrorSpikeFastProtectAndSlowRecover measures error-spike delay and recovery delay.
func TestErrorSpikeFastProtectAndSlowRecover(t *testing.T) {
	f := DefaultFilter()
	f.MinDwell = 400 * time.Millisecond
	f.CooldownCooldown = 400 * time.Millisecond
	f.CapacityMinDwell = 800 * time.Millisecond
	f.ConfirmTicks = 4
	f.MinSamples = 4
	f.EMAAlpha = 0.5 // climb error EMA quickly
	c := NewController(Targets{TTFTP95Ms: 80, ErrorRate: 0.05, MaxInflightFloor: 1, MaxInflightCap: 4},
		f, Policy{RouteMethod: route.MethodHybrid, MaxInflight: 3, RetryEnabled: true, Mode: "steady"})

	for i := 0; i < 6; i++ {
		c.Observe(Sample{TTFTMs: 25, Error: false, HealthyWorkers: 2, TotalWorkers: 2})
	}

	// Error spike
	spikeStart := time.Now()
	var protectMs int64 = -1
	var protectSnap Snapshot
	for i := 0; i < 12; i++ {
		s := c.Observe(Sample{TTFTMs: 40, Error: true, HealthyWorkers: 2, TotalWorkers: 2, InflightSeen: 2})
		if s.EffectiveChange && (s.Policy.Mode == "protect_errors" || s.Policy.RouteMethod == route.MethodLeastInflight) {
			protectMs = time.Since(spikeStart).Milliseconds()
			protectSnap = s
			break
		}
	}
	if protectMs < 0 {
		t.Fatalf("error spike never entered protect_errors; pol=%+v ema_err hist", c.Policy())
	}
	// Must not wait full 400ms dwell * 4 confirms (~1.6s+)
	if protectMs > 150 {
		t.Fatalf("error spike protect too slow: %dms (FAST should be sub-150ms); reason=%q", protectMs, protectSnap.ChangeReason)
	}
	if !protectSnap.FastPath && !hasFASTPrefix(protectSnap.ChangeReason) {
		t.Fatalf("expected FAST for error spike, got %q", protectSnap.ChangeReason)
	}

	// Clear errors — recovery should respect dwell+confirm (slow)
	time.Sleep(50 * time.Millisecond)
	recoverStart := time.Now()
	var recoverMs int64 = -1
	for i := 0; i < 40; i++ {
		s := c.Observe(Sample{TTFTMs: 25, Error: false, HealthyWorkers: 2, TotalWorkers: 2})
		if s.EffectiveChange && s.Policy.RouteMethod == route.MethodHybrid {
			recoverMs = time.Since(recoverStart).Milliseconds()
			break
		}
		time.Sleep(30 * time.Millisecond)
	}
	if recoverMs < 0 {
		t.Fatalf("never recovered to hybrid after error clear; pol=%+v", c.Policy())
	}
	if recoverMs < 300 {
		t.Fatalf("error recovery too fast (%dms); leave-protect must use dwell+confirm", recoverMs)
	}
	t.Logf("error_spike protect_ms=%d recover_ms=%d protect_reason=%q", protectMs, recoverMs, protectSnap.ChangeReason)

	// Evidence file under repo results/raw
	outDir := resolveResultsRaw()
	_ = os.MkdirAll(outDir, 0o755)
	ts := time.Now().UTC().Format("20060102-150405")
	sum := map[string]any{
		"scenario":           "error_spike_fast_protect_slow_recover",
		"protect_ms":         protectMs,
		"recover_ms":         recoverMs,
		"protect_fast_path":  protectSnap.FastPath,
		"protect_reason":     protectSnap.ChangeReason,
		"final_policy":       c.Policy(),
		"effective_switches": c.EffectiveSwitchCount(),
		"suppressions":       c.SuppressedCount(),
		"min_dwell_ms":       f.MinDwell.Milliseconds(),
		"confirm_ticks":      f.ConfirmTicks,
		"verdict":            "critical protect FAST; recovery delayed by anti-thrash (not accidental)",
	}
	sb, _ := json.MarshalIndent(sum, "", "  ")
	sumPath := filepath.Join(outDir, "adapt-critical-error-spike-"+ts+".json")
	_ = os.WriteFile(sumPath, sb, 0o644)
	histPath := filepath.Join(outDir, "adapt-critical-error-spike-"+ts+".jsonl")
	if hf, err := os.Create(histPath); err == nil {
		enc := json.NewEncoder(hf)
		for i, h := range c.History() {
			_ = enc.Encode(map[string]any{
				"i": i + 1, "ema_ttft": h.EMA_TTFTMs, "ema_err": h.EMA_ErrorRate,
				"mode": h.Policy.Mode, "method": h.Policy.RouteMethod, "inflight": h.Policy.MaxInflight,
				"effective": h.EffectiveChange, "suppressed": h.Suppressed, "suppress_why": h.SuppressWhy,
				"reason": h.ChangeReason, "urgency": h.Urgency, "fast_path": h.FastPath,
			})
		}
		_ = hf.Close()
	}
	t.Logf("evidence %s protect_ms=%d recover_ms=%d", sumPath, protectMs, recoverMs)
}

// TestMildTTFTStillConfirms: non-severe TTFT (under 1.5× enter) keeps confirm — not accidental suppress of critical.
func TestMildTTFTStillConfirms(t *testing.T) {
	f := DefaultFilter()
	f.MinDwell = 50 * time.Millisecond
	f.CooldownCooldown = 50 * time.Millisecond
	f.CapacityMinDwell = 10 * time.Second
	f.ConfirmTicks = 3
	f.MinSamples = 4
	f.EMAAlpha = 0.4
	// enter = 100*1.05 = 105; critical needs > 105*1.5 = 157.5
	c := NewController(Targets{TTFTP95Ms: 100, ErrorRate: 0.1, MaxInflightFloor: 1, MaxInflightCap: 4},
		f, Policy{RouteMethod: route.MethodHybrid, MaxInflight: 2, RetryEnabled: true, Mode: "steady"})
	for i := 0; i < 5; i++ {
		c.Observe(Sample{TTFTMs: 40, HealthyWorkers: 2, TotalWorkers: 2})
	}
	time.Sleep(60 * time.Millisecond)
	supBeforeApply := 0
	var applied bool
	for i := 0; i < 10; i++ {
		// mild breach ~120-130 EMA region, not severe
		s := c.Observe(Sample{TTFTMs: 130, HealthyWorkers: 2, TotalWorkers: 2, InflightSeen: 2})
		if s.Suppressed {
			supBeforeApply++
		}
		if s.EffectiveChange {
			applied = true
			if s.FastPath {
				t.Fatalf("mild TTFT should not use FAST path; reason=%q", s.ChangeReason)
			}
			break
		}
	}
	if !applied {
		t.Fatalf("mild TTFT never entered protect after confirms")
	}
	if supBeforeApply < 1 {
		t.Fatalf("expected confirm suppressions before mild protect, got %d", supBeforeApply)
	}
}

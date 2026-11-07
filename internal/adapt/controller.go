// Package adapt is the load-aware control plane that integrates routing,
// admission (max inflight), and safe-retry policy.
//
// It does NOT implement llama.cpp continuous batching. Decisions use EMA metrics
// with hysteresis bands, min dwell, post-switch cooldown, and capacity cooldown
// so policies do not thrash. Only *effective* changes (route/max_inflight/retry/hold)
// count as switches — mode labels alone do not.
package adapt

import (
	"fmt"
	"sync"
	"time"

	"github.com/Priyanshuthapliyal2005/Liffo/internal/route"
)

// Targets are SLOs the controller tries to protect under load spikes.
type Targets struct {
	TTFTP95Ms        float64
	ErrorRate        float64
	MaxInflightFloor int
	MaxInflightCap   int
}

// DefaultTargets for local 0.5B-class interactive.
func DefaultTargets() Targets {
	return Targets{
		TTFTP95Ms:        80,
		ErrorRate:        0.05,
		MaxInflightFloor: 1,
		MaxInflightCap:   8,
	}
}

// FilterConfig prevents continuous policy switching on unfiltered metrics.
type FilterConfig struct {
	// EMAAlpha in (0,1]; higher = more reactive. Default 0.2 (stable).
	EMAAlpha float64
	// MinDwell: minimum time between *effective* policy changes.
	MinDwell time.Duration
	// Cooldown: extra block after an effective switch (stacked with dwell check via lastChange).
	// If 0, defaults to MinDwell.
	CooldownCooldown time.Duration
	// CapacityMinDwell: min time between MaxInflight steps (open or shed).
	CapacityMinDwell time.Duration
	// ConfirmTicks: consecutive Observe evaluations that must agree before switch.
	ConfirmTicks int
	// MinSamples: ignore control until this many observations.
	MinSamples int
	// Hysteresis bands as fractions of targets.
	// Enter protect when ema > target*Enter; leave only when ema < target*Exit.
	TTFTEnterFrac float64 // default 1.05
	TTFTExitFrac  float64 // default 0.70
	ErrEnterFrac  float64 // default 1.0
	ErrExitFrac   float64 // default 0.5
	// Capacity open only when ttft < target*CapacityOpenFrac and err low.
	CapacityOpenFrac float64 // default 0.60
	Tick             time.Duration
}

// DefaultFilter returns anti-thrash defaults (stricter than early soak).
func DefaultFilter() FilterConfig {
	return FilterConfig{
		EMAAlpha:         0.20,
		MinDwell:         3 * time.Second,
		CooldownCooldown: 3 * time.Second,
		CapacityMinDwell: 5 * time.Second,
		ConfirmTicks:     4,
		MinSamples:       6,
		TTFTEnterFrac:    1.05,
		TTFTExitFrac:     0.70,
		ErrEnterFrac:     1.0,
		ErrExitFrac:      0.50,
		CapacityOpenFrac: 0.60,
		Tick:             200 * time.Millisecond,
	}
}

// Policy is the actuated control surface (privacy-safe).
type Policy struct {
	RouteMethod  route.Method `json:"route_method"`
	MaxInflight  int          `json:"max_inflight"`
	RetryEnabled bool         `json:"retry_enabled"`
	HoldMs       int          `json:"hold_ms"`
	// Mode is audit/label only — ignored for effective-change equality.
	Mode string `json:"mode"`
}

// Snapshot is filtered metrics + current policy (for JSONL).
type Snapshot struct {
	TS              time.Time `json:"ts"`
	Samples         int       `json:"samples"`
	EMA_TTFTMs      float64   `json:"ema_ttft_ms"`
	EMA_ErrorRate   float64   `json:"ema_error_rate"`
	RecentInflight  int       `json:"recent_inflight"`
	HealthyWorkers  int       `json:"healthy_workers"`
	TotalWorkers    int       `json:"total_workers"`
	LongPromptShare float64   `json:"long_prompt_share"`
	Policy          Policy    `json:"policy"`
	// Switched / EffectiveChange: true only when route/max_inflight/retry/hold changed.
	Switched        bool   `json:"switched"`
	EffectiveChange bool   `json:"effective_change"`
	ChangeReason    string `json:"change_reason,omitempty"`
	// ModeOnly: propose wanted a different Mode label with same effective policy (not counted).
	ModeOnly    bool   `json:"mode_only,omitempty"`
	Suppressed  bool   `json:"suppressed,omitempty"`
	SuppressWhy string `json:"suppress_why,omitempty"`
	// CandidateEffective describes what was proposed (for audit).
	CandidateMode string `json:"candidate_mode,omitempty"`
	Urgency       string `json:"urgency,omitempty"`
	FastPath      bool   `json:"fast_path,omitempty"`
}

// Sample is one completed request observation (no prompt text).
type Sample struct {
	TTFTMs         float64
	LatencyMs      float64
	Error          bool
	InflightSeen   int
	PromptTokens   int
	HealthyWorkers int
	TotalWorkers   int
}

// Controller owns filtered metrics and policy selection.
type Controller struct {
	targets Targets
	filter  FilterConfig

	mu sync.Mutex

	samples  int
	emaTTFT  float64
	emaErr   float64
	emaLong  float64
	ttftInit bool
	errInit  bool
	longInit bool

	policy     Policy
	lastChange time.Time
	lastCapChg time.Time

	pending     *Policy
	pendingSame int
	pendingWhy  string

	history []Snapshot
}

// NewController builds a controller with initial steady policy.
func NewController(t Targets, f FilterConfig, initial Policy) *Controller {
	if t.TTFTP95Ms <= 0 {
		t = DefaultTargets()
	}
	f = normalizeFilter(f)
	if t.MaxInflightFloor < 1 {
		t.MaxInflightFloor = 1
	}
	if t.MaxInflightCap < t.MaxInflightFloor {
		t.MaxInflightCap = 8
	}
	if initial.RouteMethod == "" {
		initial.RouteMethod = route.MethodHybrid
	}
	if initial.MaxInflight < 1 {
		initial.MaxInflight = 2
	}
	if initial.Mode == "" {
		initial.Mode = "steady"
	}
	now := time.Now()
	return &Controller{
		targets:    t,
		filter:     f,
		policy:     initial,
		lastChange: now,
		lastCapChg: now,
		history:    make([]Snapshot, 0, 256),
	}
}

func normalizeFilter(f FilterConfig) FilterConfig {
	if f.EMAAlpha <= 0 || f.EMAAlpha > 1 {
		f.EMAAlpha = 0.20
	}
	if f.MinDwell <= 0 {
		f.MinDwell = 3 * time.Second
	}
	if f.CooldownCooldown <= 0 {
		f.CooldownCooldown = f.MinDwell
	}
	if f.CapacityMinDwell <= 0 {
		f.CapacityMinDwell = 5 * time.Second
	}
	if f.ConfirmTicks < 1 {
		f.ConfirmTicks = 4
	}
	if f.MinSamples < 1 {
		f.MinSamples = 6
	}
	if f.TTFTEnterFrac <= 0 {
		f.TTFTEnterFrac = 1.05
	}
	if f.TTFTExitFrac <= 0 {
		f.TTFTExitFrac = 0.70
	}
	if f.TTFTExitFrac >= f.TTFTEnterFrac {
		f.TTFTExitFrac = f.TTFTEnterFrac * 0.7
	}
	if f.ErrEnterFrac <= 0 {
		f.ErrEnterFrac = 1.0
	}
	if f.ErrExitFrac <= 0 {
		f.ErrExitFrac = 0.5
	}
	if f.CapacityOpenFrac <= 0 {
		f.CapacityOpenFrac = 0.60
	}
	return f
}

// Policy returns a copy of the current policy.
func (c *Controller) Policy() Policy {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.policy
}

// Reset clears filtered EMAs, pending confirms, and restores policy — used between
// endurance chaos cycles so protect/shed state does not ratchet across cycles.
func (c *Controller) Reset(initial Policy) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if initial.RouteMethod == "" {
		initial = c.policy
	}
	if initial.MaxInflight < 1 {
		initial.MaxInflight = 2
	}
	if initial.Mode == "" {
		initial.Mode = "steady"
	}
	now := time.Now()
	c.samples = 0
	c.emaTTFT = 0
	c.emaErr = 0
	c.emaLong = 0
	c.ttftInit = false
	c.errInit = false
	c.longInit = false
	c.policy = initial
	c.lastChange = now
	c.lastCapChg = now
	c.pending = nil
	c.pendingSame = 0
	c.pendingWhy = ""
	// keep history for audit but cap
	if len(c.history) > 64 {
		c.history = c.history[len(c.history)-64:]
	}
}

// Observe updates EMAs and maybe switches policy. Returns snapshot for audit.
func (c *Controller) Observe(s Sample) Snapshot {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.samples++
	a := c.filter.EMAAlpha

	if !s.Error && s.TTFTMs > 0 {
		if !c.ttftInit {
			c.emaTTFT = s.TTFTMs
			c.ttftInit = true
		} else {
			c.emaTTFT = a*s.TTFTMs + (1-a)*c.emaTTFT
		}
	}
	e := 0.0
	if s.Error {
		e = 1.0
	}
	if !c.errInit {
		c.emaErr = e
		c.errInit = true
	} else {
		c.emaErr = a*e + (1-a)*c.emaErr
	}
	long := 0.0
	if s.PromptTokens >= 128 {
		long = 1.0
	}
	if !c.longInit {
		c.emaLong = long
		c.longInit = true
	} else {
		c.emaLong = a*long + (1-a)*c.emaLong
	}

	snap := Snapshot{
		TS:              time.Now().UTC(),
		Samples:         c.samples,
		EMA_TTFTMs:      c.emaTTFT,
		EMA_ErrorRate:   c.emaErr,
		RecentInflight:  s.InflightSeen,
		HealthyWorkers:  s.HealthyWorkers,
		TotalWorkers:    s.TotalWorkers,
		LongPromptShare: c.emaLong,
		Policy:          c.policy,
	}

	if c.samples < c.filter.MinSamples {
		c.pushHist(snap)
		return snap
	}

	cand, why := c.proposeLocked(s)
	snap.CandidateMode = cand.Mode
	critical := c.isCriticalLocked(s, cand)
	if critical {
		snap.Urgency = "critical"
	} else {
		snap.Urgency = "normal"
	}

	// Mode-only difference: update label quietly, do NOT count as switch.
	if effectiveEqual(cand, c.policy) {
		if cand.Mode != c.policy.Mode {
			c.policy.Mode = cand.Mode
			snap.Policy = c.policy
			snap.ModeOnly = true
		}
		c.pending = nil
		c.pendingSame = 0
		c.pushHist(snap)
		return snap
	}

	// Critical protective moves: fast path (1 confirm, skip dwell/cooldown).
	// Does NOT fast-path capacity opens or leaving protect (those stay slow).
	fast := critical && moreProtective(c.policy, cand)
	needConfirm := c.filter.ConfirmTicks
	if fast {
		needConfirm = 1
		snap.FastPath = true
	}

	// Block during min dwell OR post-switch cooldown — unless protective fast path.
	if !fast {
		blockFor := c.filter.MinDwell
		if c.filter.CooldownCooldown > blockFor {
			blockFor = c.filter.CooldownCooldown
		}
		if time.Since(c.lastChange) < blockFor {
			snap.Suppressed = true
			snap.SuppressWhy = fmt.Sprintf("dwell_cooldown remaining=%s want=%s",
				(blockFor - time.Since(c.lastChange)).Round(time.Millisecond), why)
			c.pushHist(snap)
			return snap
		}
	}

	// Capacity-only changes need CapacityMinDwell (even under critical).
	if capacityOnlyChange(c.policy, cand) && time.Since(c.lastCapChg) < c.filter.CapacityMinDwell {
		if c.policy.RouteMethod == cand.RouteMethod && c.policy.RetryEnabled == cand.RetryEnabled {
			snap.Suppressed = true
			snap.SuppressWhy = fmt.Sprintf("capacity_cooldown remaining=%s want=%s",
				(c.filter.CapacityMinDwell - time.Since(c.lastCapChg)).Round(time.Millisecond), why)
			c.pushHist(snap)
			return snap
		}
		// Keep method/retry change; defer capacity step.
		cand.MaxInflight = c.policy.MaxInflight
		if effectiveEqual(cand, c.policy) {
			snap.Suppressed = true
			snap.SuppressWhy = fmt.Sprintf("capacity_cooldown_only remaining=%s",
				(c.filter.CapacityMinDwell - time.Since(c.lastCapChg)).Round(time.Millisecond))
			c.pushHist(snap)
			return snap
		}
		why = why + " (capacity step deferred)"
	}

	// Confirm consecutive same *effective* candidate
	if c.pending != nil && effectiveEqual(*c.pending, cand) {
		c.pendingSame++
		cp := cand
		c.pending = &cp
		c.pendingWhy = why
	} else {
		cp := cand
		c.pending = &cp
		c.pendingSame = 1
		c.pendingWhy = why
	}
	if c.pendingSame < needConfirm {
		snap.Suppressed = true
		snap.SuppressWhy = fmt.Sprintf("confirm %d/%d urgency=%s: %s", c.pendingSame, needConfirm, snap.Urgency, why)
		c.pushHist(snap)
		return snap
	}

	// Apply effective change
	old := c.policy
	c.policy = cand
	c.lastChange = time.Now()
	if old.MaxInflight != cand.MaxInflight {
		c.lastCapChg = c.lastChange
	}
	c.pending = nil
	c.pendingSame = 0
	snap.Policy = c.policy
	snap.Switched = true
	snap.EffectiveChange = true
	snap.ChangeReason = why
	if fast {
		snap.ChangeReason = "FAST:" + why
	}
	c.pushHist(snap)
	return snap
}

// isCriticalLocked: worker loss, severe error, or severe TTFT breach.
func (c *Controller) isCriticalLocked(s Sample, cand Policy) bool {
	total := s.TotalWorkers
	if total <= 0 {
		total = 1
	}
	if s.HealthyWorkers < total {
		return true
	}
	if c.errInit && c.emaErr > c.targets.ErrorRate*c.filter.ErrEnterFrac*1.5 {
		return true
	}
	enter := c.targets.TTFTP95Ms * c.filter.TTFTEnterFrac
	if c.ttftInit && c.emaTTFT > enter*1.5 {
		return true
	}
	if cand.Mode == "recover_worker" || cand.Mode == "all_unhealthy" || cand.Mode == "protect_errors" {
		return true
	}
	return false
}

// moreProtective: cand tightens safety vs cur (not capacity open / relax to steady).
func moreProtective(cur, cand Policy) bool {
	if cand.Mode == "recover_worker" || cand.Mode == "all_unhealthy" || cand.Mode == "protect_errors" {
		return true
	}
	if cand.Mode == "protect_ttft" && cur.Mode != "protect_ttft" && cur.Mode != "protect_errors" && cur.Mode != "recover_worker" {
		return true
	}
	if cand.RouteMethod == route.MethodLeastInflight && cur.RouteMethod != route.MethodLeastInflight {
		return true
	}
	return false
}

func (c *Controller) proposeLocked(s Sample) (Policy, string) {
	p := c.policy
	ttft := c.emaTTFT
	errR := c.emaErr
	healthy := s.HealthyWorkers
	total := s.TotalWorkers
	if total <= 0 {
		total = 1
	}

	ttftEnter := c.targets.TTFTP95Ms * c.filter.TTFTEnterFrac
	ttftExit := c.targets.TTFTP95Ms * c.filter.TTFTExitFrac
	errEnter := c.targets.ErrorRate * c.filter.ErrEnterFrac
	errExit := c.targets.ErrorRate * c.filter.ErrExitFrac
	capOpen := c.targets.TTFTP95Ms * c.filter.CapacityOpenFrac

	// --- Recover: workers down ---
	if healthy < total && healthy >= 1 {
		p.RouteMethod = route.MethodLeastInflight
		p.RetryEnabled = true
		// capacity: only reduce toward healthy*2 with step limit handled by apply path
		want := max(c.targets.MaxInflightFloor, min(p.MaxInflight, healthy*2))
		if want < p.MaxInflight && time.Since(c.lastCapChg) >= c.filter.CapacityMinDwell {
			p.MaxInflight = p.MaxInflight - 1
			if p.MaxInflight < want {
				p.MaxInflight = want
			}
		}
		p.HoldMs = 0
		p.Mode = "recover_worker"
		return p, fmt.Sprintf("healthy_workers=%d/%d -> least_inflight+retry inflight=%d", healthy, total, p.MaxInflight)
	}
	if healthy == 0 {
		p.RouteMethod = route.MethodLeastInflight
		p.RetryEnabled = true
		if p.MaxInflight > c.targets.MaxInflightFloor && time.Since(c.lastCapChg) >= c.filter.CapacityMinDwell {
			p.MaxInflight = c.targets.MaxInflightFloor
		}
		p.Mode = "all_unhealthy"
		return p, "no healthy workers -> floor inflight+retry"
	}

	// --- Protect errors (hysteresis) ---
	inErrProtect := c.policy.Mode == "protect_errors"
	if errR > errEnter || (inErrProtect && errR > errExit) {
		p.RetryEnabled = true
		p.RouteMethod = route.MethodLeastInflight
		p.HoldMs = 0
		p.Mode = "protect_errors"
		if time.Since(c.lastCapChg) >= c.filter.CapacityMinDwell && p.MaxInflight > c.targets.MaxInflightFloor {
			// shed one step only when clearly bad or still elevated
			if errR > errEnter || s.InflightSeen >= p.MaxInflight {
				p.MaxInflight = p.MaxInflight - 1
			}
		}
		return p, fmt.Sprintf("ema_err=%.3f enter=%.3f exit=%.3f -> protect_errors inflight=%d", errR, errEnter, errExit, p.MaxInflight)
	}

	// --- Protect TTFT (hysteresis) ---
	inTTFTProtect := c.policy.Mode == "protect_ttft"
	if ttft > ttftEnter || (inTTFTProtect && ttft > ttftExit) {
		p.HoldMs = 0
		p.RetryEnabled = true
		p.RouteMethod = route.MethodLeastInflight
		p.Mode = "protect_ttft"
		// shed capacity only if still above enter band OR inflight saturated, and cooldown allows
		if time.Since(c.lastCapChg) >= c.filter.CapacityMinDwell && p.MaxInflight > c.targets.MaxInflightFloor {
			if ttft > ttftEnter || s.InflightSeen >= p.MaxInflight || ttft > 1.5*c.targets.TTFTP95Ms {
				p.MaxInflight = p.MaxInflight - 1
			}
		}
		return p, fmt.Sprintf("ema_ttft=%.1f enter=%.1f exit=%.1f -> protect_ttft inflight=%d", ttft, ttftEnter, ttftExit, p.MaxInflight)
	}

	// --- Steady: under exit bands ---
	p.RouteMethod = route.MethodHybrid
	p.RetryEnabled = true
	p.HoldMs = 0
	p.Mode = "steady"
	// Open only when the current limit is actually saturated. Opening while there
	// is spare admission capacity created the x86 soak regression: low-load samples
	// reported InflightSeen below the cap, so the controller ratcheted 2→8 before
	// the burst and then admitted too much work.
	if errR < errExit && ttft < capOpen && c.emaLong < 0.6 &&
		s.InflightSeen >= p.MaxInflight &&
		p.MaxInflight < c.targets.MaxInflightCap &&
		time.Since(c.lastCapChg) >= c.filter.CapacityMinDwell {
		p.MaxInflight = p.MaxInflight + 1
		return p, fmt.Sprintf("ttft=%.1f<%.1f err=%.3f -> steady open inflight=%d", ttft, capOpen, errR, p.MaxInflight)
	}
	return p, fmt.Sprintf("ttft=%.1f err=%.3f within hysteresis -> steady inflight=%d", ttft, errR, p.MaxInflight)
}

func (c *Controller) pushHist(s Snapshot) {
	c.history = append(c.history, s)
	if len(c.history) > 512 {
		c.history = c.history[len(c.history)-512:]
	}
}

// History returns a copy of recent snapshots.
func (c *Controller) History() []Snapshot {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]Snapshot, len(c.history))
	copy(out, c.history)
	return out
}

// SwitchCount counts *effective* policy switches in history.
func (c *Controller) SwitchCount() int {
	return c.EffectiveSwitchCount()
}

// EffectiveSwitchCount counts snapshots with EffectiveChange/Switched.
func (c *Controller) EffectiveSwitchCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	n := 0
	for _, h := range c.history {
		if h.EffectiveChange || h.Switched {
			n++
		}
	}
	return n
}

// SuppressedCount counts hysteresis suppressions.
func (c *Controller) SuppressedCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	n := 0
	for _, h := range c.history {
		if h.Suppressed {
			n++
		}
	}
	return n
}

// ModeOnlyCount counts mode-label-only updates (not effective).
func (c *Controller) ModeOnlyCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	n := 0
	for _, h := range c.history {
		if h.ModeOnly {
			n++
		}
	}
	return n
}

// effectiveEqual ignores Mode (audit label).
func effectiveEqual(a, b Policy) bool {
	return a.RouteMethod == b.RouteMethod &&
		a.MaxInflight == b.MaxInflight &&
		a.RetryEnabled == b.RetryEnabled &&
		a.HoldMs == b.HoldMs
}

func capacityOnlyChange(a, b Policy) bool {
	return a.RouteMethod == b.RouteMethod &&
		a.RetryEnabled == b.RetryEnabled &&
		a.HoldMs == b.HoldMs &&
		a.MaxInflight != b.MaxInflight
}

// policyEqual kept for tests/compat — now means effective equality (mode ignored).
func policyEqual(a, b Policy) bool {
	return effectiveEqual(a, b)
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

// StaticPolicy freezes a policy.
func StaticPolicy(method route.Method, maxInflight int, retry bool) Policy {
	if method == "" {
		method = route.MethodHybrid
	}
	if maxInflight < 1 {
		maxInflight = 2
	}
	return Policy{
		RouteMethod:  method,
		MaxInflight:  maxInflight,
		RetryEnabled: retry,
		HoldMs:       0,
		Mode:         "static",
	}
}

// ClampInflight ensures value in [floor,cap].
func ClampInflight(v, floor, capN int) int {
	if v < floor {
		return floor
	}
	if v > capN {
		return capN
	}
	return v
}

package route

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/Priyanshuthapliyal2005/Liffo/internal/config"
	"github.com/Priyanshuthapliyal2005/Liffo/internal/sse"
)

// StreamRequest is one routed completion (messages held only for the HTTP call).
type StreamRequest struct {
	Messages    []config.Message
	Model       string
	MaxTokens   int
	Temperature float64
	// ContentForRouting is the string used to compute hashes (typically user text).
	// Not persisted by Router.
	ContentForRouting string
	// AllowedWorkers restricts routing to workers with the requested model
	// ready. Nil means all workers.
	AllowedWorkers map[int]bool
}

// StreamResult is privacy-safe outcome of one request (includes retry audit trail).
type StreamResult struct {
	Decision     Decision `json:"decision"`
	FinalStage   Stage    `json:"final_stage"`
	TTFTMs       float64  `json:"ttft_ms"`
	LatencyMs    float64  `json:"latency_ms"`
	TokensOut    int      `json:"tokens_out"`
	CompletionCh int      `json:"completion_chars"`
	HTTPStatus   int      `json:"http_status"`
	Error        string   `json:"error"`
	ErrorClass   string   `json:"error_class"`
	// ErrorCode is a stable machine code for callers (e.g. not_safe_visible_output).
	ErrorCode    string    `json:"error_code,omitempty"`
	SSEEvents    int       `json:"sse_events"`
	Attempts     []Attempt `json:"attempts"`
	AttemptCount int       `json:"attempt_count"`
	Retried      bool      `json:"retried"`
	// SafeRetryUsed true if at least one safe retry was scheduled.
	SafeRetryUsed bool `json:"safe_retry_used"`
	// VisibleOutput true if any completion text was observed (blocks further retry).
	VisibleOutput   bool   `json:"visible_output"`
	PromptCacheHint string `json:"prompt_cache_hint,omitempty"`
}

// Executor routes and streams chat completions with optional safe retry.
type Executor struct {
	Router *Router
	Client *http.Client
	Retry  RetryConfig
}

// NewExecutor builds an Executor with a shared HTTP client and safe retry enabled.
func NewExecutor(r *Router) *Executor {
	return &Executor{
		Router: r,
		Retry:  DefaultRetry(),
		Client: &http.Client{
			Timeout: 0, // deadline owned by request context only
			Transport: &http.Transport{
				DisableKeepAlives:     true,
				MaxIdleConns:          64,
				MaxIdleConnsPerHost:   32,
				IdleConnTimeout:       90 * time.Second,
				ResponseHeaderTimeout: 0, // full stream bound by ctx, not header timer
			},
		},
	}
}

// Execute is ExecuteWithRetry with the executor's Retry config.
func (e *Executor) Execute(ctx context.Context, req StreamRequest) StreamResult {
	return e.ExecuteWithRetry(ctx, req, e.Retry)
}

// ExecuteWithRetry runs the stream with stage tracking and safe retry.
//
// Rules:
//   - Original ctx deadline is never extended (each attempt uses the same ctx).
//   - Retry only when SafeToRetryStage (no visible tokens yet).
//   - Every attempt is recorded with stage, error, reason, deadline left.
//   - Failed workers are Release(false) → unhealthy cooldown (no process restart).
func (e *Executor) ExecuteWithRetry(ctx context.Context, req StreamRequest, rcfg RetryConfig) StreamResult {
	content := req.ContentForRouting
	if content == "" && len(req.Messages) > 0 {
		content = req.Messages[len(req.Messages)-1].Content
	}

	maxAttempts := rcfg.MaxAttempts
	if maxAttempts <= 0 {
		maxAttempts = len(e.Router.Workers())
		if maxAttempts < 1 {
			maxAttempts = 1
		}
	}
	if !rcfg.Enabled {
		maxAttempts = 1
	}

	wall0 := time.Now()
	var attempts []Attempt
	tried := map[int]bool{}
	var lastDec Decision
	var firstTokenWall time.Time
	var tokens, chars, events, httpStatus int
	var finalStage Stage = StageQueued
	var lastErr error
	var visible bool
	safeRetryUsed := false

	for attemptIdx := 0; attemptIdx < maxAttempts; attemptIdx++ {
		// Respect original deadline before starting another attempt.
		if ctx.Err() != nil {
			reason := ReasonCanceled
			if ctx.Err() == context.DeadlineExceeded {
				reason = ReasonDeadline
			}
			if len(attempts) == 0 {
				attempts = append(attempts, Attempt{
					Index:          0,
					StageReached:   StageQueued,
					Error:          ctx.Err().Error(),
					ErrorClass:     classify(ctx.Err()),
					Reason:         reason,
					DeadlineLeftMs: deadlineLeftMs(ctx),
				})
			} else {
				attempts[len(attempts)-1].Reason = reason
			}
			lastErr = ctx.Err()
			break
		}

		if !hasUnusedHealthy(e.Router, tried, req.AllowedWorkers) && !hasAnyHealthy(e.Router, req.AllowedWorkers) {
			att := Attempt{
				Index:          attemptIdx,
				StageReached:   StageQueued,
				Error:          "all workers unavailable",
				ErrorClass:     "unavailable",
				Reason:         ReasonAllWorkersDown,
				DeadlineLeftMs: deadlineLeftMs(ctx),
			}
			attempts = append(attempts, att)
			lastErr = fmt.Errorf("%s", ReasonAllWorkersDown)
			finalStage = StageQueued
			break
		}

		// Prefer workers not yet tried this request; reservation occurs as part of
		// the same decision critical section so concurrent attempts observe it.
		dec := e.pickAndReserveForAttempt(content, tried, req.AllowedWorkers)
		lastDec = dec
		finalStage = StageAssigned

		w := e.Router.workerByID(dec.WorkerID)
		if w == nil || !w.Healthy() {
			e.Router.CancelAcquire(dec.WorkerID)
			// pick returned a dead worker with no alternative
			att := Attempt{
				Index:          attemptIdx,
				WorkerID:       dec.WorkerID,
				BaseURL:        dec.BaseURL,
				StageReached:   StageAssigned,
				Error:          "no healthy worker",
				ErrorClass:     "unavailable",
				Reason:         ReasonNoHealthyWorker,
				DeadlineLeftMs: deadlineLeftMs(ctx),
			}
			attempts = append(attempts, att)
			lastErr = fmt.Errorf("%s", ReasonNoHealthyWorker)
			break
		}

		tried[dec.WorkerID] = true
		t0 := time.Now()
		var attFirst time.Time
		var attTokens, attChars, attEvents, attStatus int
		stage := StageAssigned

		err := e.streamStaged(ctx, dec.BaseURL, req, &stage, &attFirst, &attTokens, &attChars, &attEvents, &attStatus)
		dur := time.Since(t0)
		success := err == nil
		e.Router.Release(dec.WorkerID, success)

		if attTokens > 0 || attChars > 0 {
			visible = true
			if firstTokenWall.IsZero() && !attFirst.IsZero() {
				firstTokenWall = attFirst
			}
			// Only keep tokens from the successful final attempt; partial visible
			// from a failed attempt still counts as visible (no retry).
			if success {
				tokens, chars = attTokens, attChars
			} else {
				// partial — still report what was seen on this attempt
				tokens, chars = attTokens, attChars
			}
		}
		if success {
			tokens, chars, events, httpStatus = attTokens, attChars, attEvents, attStatus
			if !attFirst.IsZero() && firstTokenWall.IsZero() {
				firstTokenWall = attFirst
			}
		} else if attStatus != 0 {
			httpStatus = attStatus
		}
		events += attEvents
		finalStage = stage
		if success {
			finalStage = StageCompleted
			e.Router.TouchAffinity(dec.PrefixSHA, dec.WorkerID)
		}

		att := Attempt{
			Index:          attemptIdx,
			WorkerID:       dec.WorkerID,
			BaseURL:        dec.BaseURL,
			StageReached:   stage,
			HTTPStatus:     attStatus,
			DurationMs:     float64(dur.Seconds() * 1000),
			DeadlineLeftMs: deadlineLeftMs(ctx),
		}
		if err != nil {
			att.Error = err.Error()
			att.ErrorClass = classify(err)
			lastErr = err
		} else {
			att.Reason = ReasonOK
			attempts = append(attempts, att)
			lastErr = nil
			break
		}

		// Decide retry using original ctx (deadline preserved).
		workersLeft := hasUnusedHealthy(e.Router, tried, req.AllowedWorkers)
		retry, reason := ShouldRetry(ctx, rcfg, stage, err, attemptIdx, maxAttempts, workersLeft)
		att.Reason = reason
		att.Retried = retry
		attempts = append(attempts, att)
		if retry {
			safeRetryUsed = true
			continue
		}
		break
	}

	lat := time.Since(wall0)
	out := StreamResult{
		Decision:      lastDec,
		FinalStage:    finalStage,
		LatencyMs:     float64(lat.Seconds() * 1000),
		TokensOut:     tokens,
		CompletionCh:  chars,
		HTTPStatus:    httpStatus,
		SSEEvents:     events,
		Attempts:      attempts,
		AttemptCount:  len(attempts),
		Retried:       len(attempts) > 1,
		SafeRetryUsed: safeRetryUsed,
		VisibleOutput: visible,
	}
	if !firstTokenWall.IsZero() {
		out.TTFTMs = float64(firstTokenWall.Sub(wall0).Seconds() * 1000)
	} else if lastErr == nil && out.LatencyMs > 0 {
		out.TTFTMs = out.LatencyMs
	}
	if lastErr != nil {
		out.Error = lastErr.Error()
		out.ErrorClass = classify(lastErr)
		out.ErrorCode = errorCodeFromAttempts(attempts, lastErr)
	}
	return out
}

func errorCodeFromAttempts(attempts []Attempt, err error) string {
	if len(attempts) > 0 {
		r := attempts[len(attempts)-1].Reason
		if r != "" && r != ReasonOK && r != ReasonRetrySafeStage {
			return r
		}
		// last attempt failed without a stop reason other than retry_safe exhausted path
		if attempts[len(attempts)-1].Error != "" {
			if r == ReasonRetrySafeStage || r == "" {
				// fell through max attempts
				return ReasonMaxAttempts
			}
			return r
		}
	}
	if err == context.DeadlineExceeded {
		return ReasonDeadline
	}
	if err == context.Canceled {
		return ReasonCanceled
	}
	return classify(err)
}

func (e *Executor) pickAndReserveForAttempt(content string, tried map[int]bool, allowed map[int]bool) Decision {
	e.Router.decisionMu.Lock()
	defer e.Router.decisionMu.Unlock()
	dec := e.pickForAttempt(content, tried, allowed)
	if dec.WorkerID >= 0 && dec.BaseURL != "" {
		e.Router.Acquire(dec.WorkerID)
	}
	return dec
}

func (e *Executor) pickForAttempt(content string, tried map[int]bool, allowed map[int]bool) Decision {
	// Temporarily prefer untried healthy workers via least_inflight among them.
	// Still use Router.Pick for sticky/hybrid affinity, then re-pick if already tried.
	dec := e.Router.PickEligible(content, allowedOrAll(e.Router, allowed))
	if !tried[dec.WorkerID] && eligibleWorker(dec.WorkerID, allowed) {
		w := e.Router.workerByID(dec.WorkerID)
		if w != nil && w.Healthy() {
			return dec
		}
	}
	// Affinity already tried or unhealthy → least inflight among untried healthy.
	var best *Worker
	for _, w := range e.Router.Workers() {
		if tried[w.ID] || !w.Healthy() || !eligibleWorker(w.ID, allowed) {
			continue
		}
		if best == nil || w.InFlight() < best.InFlight() {
			best = w
		}
	}
	if best == nil {
		// last resort: any healthy even if tried (only if MaxAttempts > workers)
		for _, w := range e.Router.Workers() {
			if !w.Healthy() || !eligibleWorker(w.ID, allowed) {
				continue
			}
			if best == nil || w.InFlight() < best.InFlight() {
				best = w
			}
		}
	}
	if best == nil {
		// all down — return first for audit URL
		w := e.Router.Workers()[0]
		e.Router.fill(&dec, w, false, "all_down", "all workers unavailable")
		return dec
	}
	hit := dec.StickyHit && dec.WorkerID == best.ID
	fb := dec.Fallback
	if best.ID != dec.WorkerID {
		fb = "retry_reassign"
		hit = false
	}
	e.Router.fill(&dec, best, hit, fb, fmt.Sprintf(
		"retry_pick worker=%d tried=%v reason=avoid_failed_or_unhealthy", best.ID, keys(tried)))
	return dec
}

func keys(m map[int]bool) []int {
	out := make([]int, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

func hasUnusedHealthy(r *Router, tried map[int]bool, allowed map[int]bool) bool {
	for _, w := range r.Workers() {
		if !tried[w.ID] && w.Healthy() && eligibleWorker(w.ID, allowed) {
			return true
		}
	}
	return false
}

func hasAnyHealthy(r *Router, allowed map[int]bool) bool {
	for _, w := range r.Workers() {
		if w.Healthy() && eligibleWorker(w.ID, allowed) {
			return true
		}
	}
	return false
}

func eligibleWorker(id int, allowed map[int]bool) bool {
	return allowed == nil || allowed[id]
}

func allowedOrAll(r *Router, allowed map[int]bool) map[int]bool {
	if allowed != nil {
		return allowed
	}
	out := make(map[int]bool, len(r.Workers()))
	for _, w := range r.Workers() {
		out[w.ID] = true
	}
	return out
}

// streamStaged performs one HTTP stream and advances *stage.
func (e *Executor) streamStaged(
	ctx context.Context,
	baseURL string,
	req StreamRequest,
	stage *Stage,
	first *time.Time,
	tokens, chars, events, httpStatus *int,
) error {
	baseURL = strings.TrimRight(baseURL, "/")
	url := baseURL + "/v1/chat/completions"
	model := req.Model
	if model == "" {
		model = "local"
	}
	body := map[string]any{
		"model":       model,
		"messages":    req.Messages,
		"stream":      true,
		"max_tokens":  req.MaxTokens,
		"temperature": req.Temperature,
		"stream_options": map[string]any{
			"include_usage": true,
		},
	}
	if req.MaxTokens <= 0 {
		body["max_tokens"] = 32
	}
	raw, err := json.Marshal(body)
	if err != nil {
		return err
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(raw))
	if err != nil {
		return err
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Accept", "text/event-stream")

	// Stage stays "assigned" until headers arrive.
	resp, err := e.Client.Do(httpReq)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	*httpStatus = resp.StatusCode
	*stage = StageConnected

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return fmt.Errorf("http %d: %s", resp.StatusCode, string(b))
	}

	sawMeta := false
	st, err := sse.ParseStream(resp.Body, nil, func(td sse.TokenDelta) {
		if td.Done {
			return
		}
		// Role-only / empty content deltas count as metadata, not visible output.
		if td.Content == "" {
			if !sawMeta && !HasVisibleOutput(*stage) {
				sawMeta = true
				if StageOrder(*stage) < StageOrder(StageMetadataReceived) {
					*stage = StageMetadataReceived
				}
			}
			if td.UsageTokens > 0 {
				*tokens = td.UsageTokens
			}
			return
		}
		// Visible content
		if first.IsZero() {
			*first = time.Now()
			*stage = StageFirstVisibleToken
		} else {
			*stage = StageStreaming
		}
		*chars += len(td.Content)
		*tokens++
		if td.UsageTokens > 0 {
			*tokens = td.UsageTokens
		}
	})
	*events = st.Events
	// If we got events but no content, ensure metadata stage is set.
	if st.Events > 0 && first.IsZero() && StageOrder(*stage) < StageOrder(StageMetadataReceived) {
		*stage = StageMetadataReceived
	}
	if err != nil {
		return err
	}
	// Success requires valid content + data: [DONE]. Clean EOF alone is not OK
	// (false success on truncated bodies / empty [DONE]-only streams).
	if cerr := sse.RequireComplete(st, *tokens, *chars); cerr != nil {
		if !first.IsZero() {
			*stage = StageStreaming
			if !st.DoneSeen {
				return fmt.Errorf("stream truncated after visible output")
			}
		}
		if !st.DoneSeen {
			return fmt.Errorf("stream closed before completion (stage=%s)", *stage)
		}
		return cerr
	}
	if !first.IsZero() {
		*stage = StageStreaming
	}
	return nil
}

func classify(err error) string {
	if err == nil {
		return ""
	}
	if err == context.DeadlineExceeded {
		return "timeout"
	}
	if err == context.Canceled {
		return "cancel"
	}
	s := strings.ToLower(err.Error())
	switch {
	case strings.Contains(s, "timeout"), strings.Contains(s, "deadline"), strings.Contains(s, "deadlineexceeded"):
		return "timeout"
	case strings.Contains(s, "canceled"), strings.Contains(s, "cancelled"), strings.Contains(s, "context canceled"):
		return "cancel"
	case strings.Contains(s, "http "):
		return "http"
	case strings.Contains(s, "all_workers"), strings.Contains(s, "all workers"), strings.Contains(s, "unavailable"):
		return "unavailable"
	default:
		return "other"
	}
}

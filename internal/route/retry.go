package route

import (
	"context"
	"strings"
	"time"
)

// Retry decision / stop reasons (privacy-safe; no prompt text).
const (
	ReasonOK               = "ok"
	ReasonRetrySafeStage   = "retry_safe_stage"        // failure before visible output
	ReasonNotSafeVisible   = "not_safe_visible_output" // would replicate tokens
	ReasonDeadline         = "deadline_exceeded"
	ReasonCanceled         = "canceled"
	ReasonMaxAttempts      = "max_attempts"
	ReasonAllWorkersDown   = "all_workers_unavailable"
	ReasonNoHealthyWorker  = "no_healthy_worker"
	ReasonHTTPNonRetryable = "http_non_retryable"
)

// Attempt is one try against one worker (privacy-safe).
type Attempt struct {
	Index        int    `json:"index"`
	WorkerID     int    `json:"worker_id"`
	BaseURL      string `json:"base_url"`
	StageReached Stage  `json:"stage_reached"`
	HTTPStatus   int    `json:"http_status,omitempty"`
	Error        string `json:"error,omitempty"`
	ErrorClass   string `json:"error_class,omitempty"`
	// Reason: why this attempt ended and/or why the next was scheduled.
	Reason     string  `json:"reason"`
	Retried    bool    `json:"retried"` // true if a subsequent attempt was started
	DurationMs float64 `json:"duration_ms"`
	// DeadlineLeftMs remaining on original context when this attempt finished.
	DeadlineLeftMs float64 `json:"deadline_left_ms"`
}

// RetryConfig controls safe streaming recovery.
type RetryConfig struct {
	// MaxAttempts caps total tries (default = number of workers, min 1).
	MaxAttempts int
	// Enabled turns retry on (default true when using ExecuteWithRetry).
	Enabled bool
}

// DefaultRetry returns sensible defaults.
func DefaultRetry() RetryConfig {
	return RetryConfig{MaxAttempts: 0, Enabled: true} // 0 → len(workers)
}

// ShouldRetry decides whether to start another attempt.
// originalCtx must be the caller's context (deadline is never extended).
func ShouldRetry(originalCtx context.Context, cfg RetryConfig, stage Stage, err error, attemptIndex, maxAttempts int, workersLeft bool) (retry bool, reason string) {
	if err == nil {
		return false, ReasonOK
	}
	if !cfg.Enabled {
		return false, "retry_disabled"
	}
	// Preserve original deadline: never retry if already expired/canceled.
	if originalCtx.Err() != nil {
		if originalCtx.Err() == context.DeadlineExceeded {
			return false, ReasonDeadline
		}
		return false, ReasonCanceled
	}
	// Timeout/cancel classification from the attempt error under a still-valid parent
	// usually means the attempt's derived context hit the same deadline — stop.
	ec := classify(err)
	if ec == "timeout" {
		return false, ReasonDeadline
	}
	if ec == "cancel" {
		return false, ReasonCanceled
	}
	if !SafeToRetryStage(stage) {
		return false, ReasonNotSafeVisible
	}
	// Non-retryable HTTP (4xx except 408/429) after headers — still "safe" stage-wise
	// but usually wrong to spam; only retry 5xx / connection-class failures.
	if ec == "http" && !isRetryableHTTPError(err) {
		return false, ReasonHTTPNonRetryable
	}
	if attemptIndex+1 >= maxAttempts {
		return false, ReasonMaxAttempts
	}
	if !workersLeft {
		return false, ReasonAllWorkersDown
	}
	return true, ReasonRetrySafeStage
}

func isRetryableHTTPError(err error) bool {
	if err == nil {
		return false
	}
	s := strings.ToLower(err.Error())
	// http NNN: ...
	if strings.Contains(s, "http 408") || strings.Contains(s, "http 429") {
		return true
	}
	if strings.Contains(s, "http 5") {
		return true
	}
	// connection-level often not prefixed with http
	return false
}

func deadlineLeftMs(ctx context.Context) float64 {
	dl, ok := ctx.Deadline()
	if !ok {
		return -1
	}
	left := time.Until(dl).Seconds() * 1000
	if left < 0 {
		return 0
	}
	return left
}

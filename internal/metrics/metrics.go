package metrics

import (
	"math"
	"sort"
	"strings"
	"time"
)

// RangeStat holds min/avg/max for sampled values.
type RangeStat struct {
	Min float64 `json:"min"`
	Avg float64 `json:"avg"`
	Max float64 `json:"max"`
	N   int     `json:"n,omitempty"`
}

// FromSamples computes RangeStat from a slice of samples.
func FromSamples(xs []float64) RangeStat {
	if len(xs) == 0 {
		return RangeStat{}
	}
	min, max, sum := xs[0], xs[0], 0.0
	for _, v := range xs {
		if v < min {
			min = v
		}
		if v > max {
			max = v
		}
		sum += v
	}
	return RangeStat{Min: min, Avg: sum / float64(len(xs)), Max: max, N: len(xs)}
}

// Result is one measured request (privacy-safe: no prompt/completion text).
type Result struct {
	RunID           string    `json:"run_id"`
	TS              time.Time `json:"ts"`
	Mode            string    `json:"mode"`
	Endpoint        string    `json:"endpoint"`
	Model           string    `json:"model"`
	PromptID        string    `json:"prompt_id"`
	PromptSHA256    string    `json:"prompt_sha256"`
	Stream          bool      `json:"stream"`
	Iteration       int       `json:"iteration"`
	Concurrency     int       `json:"concurrency"`
	WorkerID        int       `json:"worker_id,omitempty"`
	Warmup          bool      `json:"warmup,omitempty"`
	TTFTMs          float64   `json:"ttft_ms"`
	LatencyMs       float64   `json:"latency_ms"`
	TokensOut       int       `json:"tokens_out"`
	TokensPerSec    float64   `json:"tokens_per_sec"`
	CompletionChars int       `json:"completion_chars"`
	HTTPStatus      int       `json:"http_status"`
	Error           string    `json:"error"`
	ErrorClass      string    `json:"error_class"`
	RAMMb           RangeStat `json:"ram_mb"`
	GPUMemMb        RangeStat `json:"gpu_mem_mb"`
	GPUUtilPct      RangeStat `json:"gpu_util_pct"`
	SSEEvents       int       `json:"sse_events"`
	SSEParseErrors  int       `json:"sse_parse_errors"`
	SSEComments     int       `json:"sse_comments"`
	DoneSeen        bool      `json:"done_seen"`
}

// Error class constants.
const (
	ClassTimeout  = "timeout"
	ClassHTTP     = "http"
	ClassSSEParse = "sse_parse"
	ClassCancel   = "cancel"
	ClassEmpty    = "empty"
	ClassOther    = "other"
)

// ClassifyError maps an error string + HTTP status to a stable class.
func ClassifyError(err error, httpStatus int) string {
	if err == nil && (httpStatus == 0 || (httpStatus >= 200 && httpStatus < 300)) {
		return ""
	}
	if err != nil {
		s := strings.ToLower(err.Error())
		switch {
		case containsAny(s, "timeout", "deadline exceeded", "context deadline", "i/o timeout", "client.timeout"):
			return ClassTimeout
		case containsAny(s, "context canceled", "context cancelled", "canceled", "cancelled"):
			return ClassCancel
		case containsAny(s, "connection refused", "connectex", "no such host", "actively refused",
			"wsarecv", "forcibly closed", "connection reset", "closed idle connection",
			"broken pipe", "connection was aborted", "use of closed network connection",
			"server closed idle connection", "unexpected eof"):
			return ClassHTTP
		// Empty before generic "sse" match — "no SSE events" must stay ClassEmpty.
		case containsAny(s, "empty response", "no tokens", "empty stream"):
			return ClassEmpty
		case containsAny(s, "stream incomplete", "missing [done]", "stream truncated", "stream closed before"):
			return ClassOther
		case containsAny(s, "sse", "parse", "invalid character"):
			return ClassSSEParse
		default:
			if httpStatus >= 400 {
				return ClassHTTP
			}
			return ClassOther
		}
	}
	if httpStatus >= 400 {
		return ClassHTTP
	}
	return ""
}

func containsAny(s string, parts ...string) bool {
	for _, p := range parts {
		if strings.Contains(s, strings.ToLower(p)) {
			return true
		}
	}
	return false
}

// TokensPerSecond computes decode-focused tokens/s.
// When tokens_out > 1 and TTFT is known, uses (tokens-1)/(latency-ttft)
// so the first-token wait is excluded from decode rate.
// Otherwise falls back to tokens/latency.
func TokensPerSecond(tokens int, latencyMs, ttftMs float64) float64 {
	if tokens <= 0 || latencyMs <= 0 {
		return 0
	}
	latS := latencyMs / 1000.0
	if tokens > 1 && ttftMs > 0 && latencyMs > ttftMs {
		decodeS := (latencyMs - ttftMs) / 1000.0
		if decodeS > 0 {
			return float64(tokens-1) / decodeS
		}
	}
	return float64(tokens) / latS
}

// Aggregate summarizes a slice of non-warmup results.
type Aggregate struct {
	Count           int                   `json:"count"`
	Errors          int                   `json:"errors"`
	ErrorRate       float64               `json:"error_rate"`
	TTFTMs          Percentiles           `json:"ttft_ms"`
	LatencyMs       Percentiles           `json:"latency_ms"`
	TokensPerSec    Percentiles           `json:"tokens_per_sec"`
	TokensOutAvg    float64               `json:"tokens_out_avg"`
	TokensOutSum    int                   `json:"tokens_out_sum,omitempty"`
	SSEParseErrors  int                   `json:"sse_parse_errors_total"`
	WallSec         float64               `json:"wall_sec,omitempty"`
	ThroughputRPS   float64               `json:"throughput_rps,omitempty"`
	ThroughputTokPS float64               `json:"throughput_tok_per_sec,omitempty"`
	ByMode          map[string]*Aggregate `json:"by_mode,omitempty"`
	ByConcurrency   map[int]*Aggregate    `json:"by_concurrency,omitempty"`
	ByErrorClass    map[string]int        `json:"by_error_class,omitempty"`
	RAMMb           RangeStat             `json:"ram_mb,omitempty"`
	GPUMemMb        RangeStat             `json:"gpu_mem_mb,omitempty"`
	GPUUtilPct      RangeStat             `json:"gpu_util_pct,omitempty"`
}

// Percentiles holds p50/p90/p99/mean/min/max.
type Percentiles struct {
	Min  float64 `json:"min"`
	P50  float64 `json:"p50"`
	P90  float64 `json:"p90"`
	P99  float64 `json:"p99"`
	Mean float64 `json:"mean"`
	Max  float64 `json:"max"`
	N    int     `json:"n"`
}

func percentile(sorted []float64, p float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	if len(sorted) == 1 {
		return sorted[0]
	}
	// Nearest-rank with linear interpolation between adjacent ranks.
	idx := p * float64(len(sorted)-1)
	lo := int(math.Floor(idx))
	hi := int(math.Ceil(idx))
	if lo < 0 {
		lo = 0
	}
	if hi >= len(sorted) {
		hi = len(sorted) - 1
	}
	if lo == hi {
		return sorted[lo]
	}
	frac := idx - float64(lo)
	return sorted[lo]*(1-frac) + sorted[hi]*frac
}

// ComputePercentiles from values.
func ComputePercentiles(xs []float64) Percentiles {
	if len(xs) == 0 {
		return Percentiles{}
	}
	cp := append([]float64(nil), xs...)
	sort.Float64s(cp)
	sum := 0.0
	for _, v := range cp {
		sum += v
	}
	return Percentiles{
		Min:  cp[0],
		P50:  percentile(cp, 0.50),
		P90:  percentile(cp, 0.90),
		P99:  percentile(cp, 0.99),
		Mean: sum / float64(len(cp)),
		Max:  cp[len(cp)-1],
		N:    len(cp),
	}
}

// IsSuccess reports whether a measured row should count as success.
func IsSuccess(r Result) bool {
	return r.Error == "" && r.ErrorClass == ""
}

// Summarize builds Aggregate from results (skips warmup).
func Summarize(results []Result) Aggregate {
	var kept []Result
	for _, r := range results {
		if r.Warmup {
			continue
		}
		kept = append(kept, r)
	}
	agg := summarizeFlat(kept)
	modeResults := map[string][]Result{}
	concResults := map[int][]Result{}
	for _, r := range kept {
		mode := r.Mode
		if mode == "" {
			mode = "unknown"
		}
		modeResults[mode] = append(modeResults[mode], r)
		c := r.Concurrency
		if c < 1 {
			c = 1
		}
		concResults[c] = append(concResults[c], r)
	}
	agg.ByMode = map[string]*Aggregate{}
	for mode, rs := range modeResults {
		sub := summarizeFlat(rs)
		agg.ByMode[mode] = &sub
	}
	agg.ByConcurrency = map[int]*Aggregate{}
	for c, rs := range concResults {
		sub := summarizeFlat(rs)
		agg.ByConcurrency[c] = &sub
	}
	return agg
}

// AttachWallClock sets phase wall-clock throughput on an aggregate.
func AttachWallClock(agg *Aggregate, wall time.Duration, successTokens int) {
	if agg == nil || wall <= 0 {
		return
	}
	sec := wall.Seconds()
	agg.WallSec = sec
	if sec > 0 {
		agg.ThroughputRPS = float64(agg.Count) / sec
		agg.ThroughputTokPS = float64(successTokens) / sec
	}
}

func summarizeFlat(kept []Result) Aggregate {
	agg := Aggregate{ByErrorClass: map[string]int{}}
	agg.Count = len(kept)
	var ttfts, lats, tps []float64
	var tokSum float64
	var ram, gmem, gutil []float64
	tokOutSum := 0
	for _, r := range kept {
		if !IsSuccess(r) {
			agg.Errors++
			cls := r.ErrorClass
			if cls == "" {
				cls = ClassOther
			}
			agg.ByErrorClass[cls]++
		} else {
			if r.TTFTMs > 0 {
				ttfts = append(ttfts, r.TTFTMs)
			}
			if r.LatencyMs > 0 {
				lats = append(lats, r.LatencyMs)
			}
			if r.TokensPerSec > 0 {
				tps = append(tps, r.TokensPerSec)
			}
			tokSum += float64(r.TokensOut)
			tokOutSum += r.TokensOut
		}
		agg.SSEParseErrors += r.SSEParseErrors
		if r.RAMMb.N > 0 {
			ram = append(ram, r.RAMMb.Max)
		}
		if r.GPUMemMb.N > 0 {
			gmem = append(gmem, r.GPUMemMb.Max)
		}
		if r.GPUUtilPct.N > 0 {
			gutil = append(gutil, r.GPUUtilPct.Max)
		}
	}
	agg.TokensOutSum = tokOutSum
	if agg.Count > 0 {
		agg.ErrorRate = float64(agg.Errors) / float64(agg.Count)
		successN := agg.Count - agg.Errors
		if successN > 0 {
			agg.TokensOutAvg = tokSum / float64(successN)
		}
	}
	agg.TTFTMs = ComputePercentiles(ttfts)
	agg.LatencyMs = ComputePercentiles(lats)
	agg.TokensPerSec = ComputePercentiles(tps)
	agg.RAMMb = FromSamples(ram)
	agg.GPUMemMb = FromSamples(gmem)
	agg.GPUUtilPct = FromSamples(gutil)
	return agg
}

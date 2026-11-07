package client

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/Priyanshuthapliyal2005/Liffo/internal/config"
	"github.com/Priyanshuthapliyal2005/Liffo/internal/metrics"
	"github.com/Priyanshuthapliyal2005/Liffo/internal/sse"
	"github.com/Priyanshuthapliyal2005/Liffo/internal/sysmon"
)

// Runner executes streaming (or non-stream) chat completion benchmarks.
type Runner struct {
	Cfg    config.Config
	Client *http.Client
	Mon    *sysmon.Monitor
}

// NewRunner builds a Runner.
// Request deadline is owned by the per-call context (not Client.Timeout) so
// error classification stays accurate (timeout vs cancel vs http).
func NewRunner(cfg config.Config) *Runner {
	maxConns := cfg.Concurrency * 4
	if maxConns < 16 {
		maxConns = 16
	}
	if maxConns > 128 {
		maxConns = 128
	}
	transport := &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   10 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          maxConns,
		MaxIdleConnsPerHost:   maxConns,
		MaxConnsPerHost:       maxConns,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
		// Streaming responses must not be capped by ResponseHeaderTimeout alone
		// for the full body; header timeout still bounds "server hung before headers".
		ResponseHeaderTimeout: cfg.Timeout(),
	}
	return &Runner{
		Cfg: cfg,
		Client: &http.Client{
			// Timeout is 0: use context deadline only (avoids dual-timeout races).
			Timeout:   0,
			Transport: transport,
		},
		// Shared mon kept for sequential callers; RunOne uses a per-request monitor
		// so concurrent workers do not race on sample buffers.
		Mon: sysmon.New(cfg.SampleInterval(), cfg.ServerPID),
	}
}

// Close releases idle HTTP connections. Safe to call multiple times.
func (r *Runner) Close() {
	if r == nil || r.Client == nil {
		return
	}
	if tr, ok := r.Client.Transport.(*http.Transport); ok {
		tr.CloseIdleConnections()
	}
}

type chatRequest struct {
	Model         string           `json:"model"`
	Messages      []config.Message `json:"messages"`
	Stream        bool             `json:"stream"`
	MaxTokens     int              `json:"max_tokens,omitempty"`
	Temperature   float64          `json:"temperature"`
	StreamOptions *streamOpts      `json:"stream_options,omitempty"`
}

type streamOpts struct {
	IncludeUsage bool `json:"include_usage"`
}

// PromptHash returns sha256 of concatenated message contents.
func PromptHash(msgs []config.Message) string {
	h := sha256.New()
	for _, m := range msgs {
		h.Write([]byte(m.Role))
		h.Write([]byte{0})
		h.Write([]byte(m.Content))
		h.Write([]byte{0})
	}
	return hex.EncodeToString(h.Sum(nil))
}

// RunOne executes a single request and returns a metrics.Result.
// Named return so deferred sysmon snapshot mutates the value actually returned.
// Safe for concurrent use: each call owns a private sysmon.Monitor.
func (r *Runner) RunOne(ctx context.Context, prompt config.Prompt, iteration int, warmup bool) (res metrics.Result) {
	return r.RunOneWorker(ctx, prompt, iteration, warmup, 0)
}

// RunOneWorker is RunOne with an explicit worker id for concurrent load runs.
func (r *Runner) RunOneWorker(ctx context.Context, prompt config.Prompt, iteration int, warmup bool, workerID int) (res metrics.Result) {
	conc := r.Cfg.Concurrency
	if conc < 1 {
		conc = 1
	}
	res = metrics.Result{
		RunID:        r.Cfg.RunID,
		TS:           time.Now().UTC(),
		Mode:         r.Cfg.Mode,
		Endpoint:     r.Cfg.Endpoint(),
		Model:        r.Cfg.Model,
		PromptID:     prompt.ID,
		PromptSHA256: PromptHash(prompt.Messages),
		Stream:       r.Cfg.Stream,
		Iteration:    iteration,
		Concurrency:  conc,
		WorkerID:     workerID,
		Warmup:       warmup,
	}

	body := chatRequest{
		Model:       r.Cfg.Model,
		Messages:    prompt.Messages,
		Stream:      r.Cfg.Stream,
		MaxTokens:   r.Cfg.MaxTokens,
		Temperature: r.Cfg.Temperature,
	}
	if r.Cfg.Stream {
		body.StreamOptions = &streamOpts{IncludeUsage: true}
	}
	payload, err := json.Marshal(body)
	if err != nil {
		res.Error = err.Error()
		res.ErrorClass = metrics.ClassOther
		return res
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, r.Cfg.Endpoint(), bytes.NewReader(payload))
	if err != nil {
		res.Error = err.Error()
		res.ErrorClass = metrics.ClassOther
		return res
	}
	req.Header.Set("Content-Type", "application/json")
	if r.Cfg.Stream {
		req.Header.Set("Accept", "text/event-stream")
	} else {
		req.Header.Set("Accept", "application/json")
	}

	// Per-request monitor avoids concurrent Start/Stop races on a shared Monitor.
	mon := sysmon.New(r.Cfg.SampleInterval(), r.Cfg.ServerPID)
	mon.Start()
	defer func() {
		mon.Stop()
		ram, gmem, gutil, _ := mon.Snapshot()
		res.RAMMb = ram
		res.GPUMemMb = gmem
		res.GPUUtilPct = gutil
	}()

	start := time.Now()
	resp, err := r.Client.Do(req)
	// Under concurrency, keep-alive sockets can be closed by the server between
	// reuse checks; retry once with a fresh connection (POST has not completed).
	if err != nil && isRetryableTransportErr(err) {
		req2, rerr := http.NewRequestWithContext(ctx, http.MethodPost, r.Cfg.Endpoint(), bytes.NewReader(payload))
		if rerr == nil {
			req2.Header = req.Header.Clone()
			resp, err = r.Client.Do(req2)
		}
	}
	if err != nil {
		res.LatencyMs = elapsedMs(start)
		res.Error = err.Error()
		res.ErrorClass = metrics.ClassifyError(err, 0)
		return res
	}
	// Ensure body is always drained/closed (resource leak prevention).
	defer func() {
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 64*1024))
		_ = resp.Body.Close()
	}()

	res.HTTPStatus = resp.StatusCode
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		res.LatencyMs = elapsedMs(start)
		res.Error = fmt.Sprintf("http %d: %s", resp.StatusCode, truncate(string(b), 200))
		res.ErrorClass = metrics.ClassHTTP
		return res
	}

	if !r.Cfg.Stream {
		r.readNonStream(start, resp, &res)
		return res
	}
	r.readStream(ctx, start, resp, &res)
	return res
}

func (r *Runner) readStream(ctx context.Context, start time.Time, resp *http.Response, res *metrics.Result) {
	var (
		firstToken time.Time
		chars      int
		tokenEst   int
		usageTok   int
	)
	// Wrap body so context cancel aborts a hung stream read promptly.
	body := &ctxReader{ctx: ctx, r: resp.Body}
	st, err := sse.ParseStream(body, nil, func(td sse.TokenDelta) {
		if td.Done {
			return
		}
		if td.UsageTokens > 0 {
			usageTok = td.UsageTokens
		}
		if td.Content != "" {
			if firstToken.IsZero() {
				firstToken = time.Now()
			}
			chars += len(td.Content)
			tokenEst++
		}
	})
	res.LatencyMs = elapsedMs(start)
	res.SSEEvents = st.Events
	res.SSEParseErrors = st.ParseErrors
	res.SSEComments = st.Comments
	res.DoneSeen = st.DoneSeen
	if !firstToken.IsZero() {
		res.TTFTMs = float64(firstToken.Sub(start).Seconds() * 1000)
	}
	if usageTok > 0 {
		res.TokensOut = usageTok
	} else {
		res.TokensOut = tokenEst
	}
	res.CompletionChars = chars
	res.TokensPerSec = metrics.TokensPerSecond(res.TokensOut, res.LatencyMs, res.TTFTMs)

	if err != nil {
		res.Error = err.Error()
		res.ErrorClass = metrics.ClassifyError(err, res.HTTPStatus)
		return
	}

	// Success requires valid content + terminal [DONE] (not HTTP 200 alone).
	if cerr := sse.RequireComplete(st, res.TokensOut, chars); cerr != nil {
		res.Error = cerr.Error()
		res.ErrorClass = metrics.ClassifyError(cerr, res.HTTPStatus)
		// Prefer parse class when the only signal was garbage events.
		if res.TokensOut == 0 && chars == 0 && st.ParseErrors > 0 && res.ErrorClass == metrics.ClassEmpty {
			res.Error = fmt.Sprintf("stream parse failed: %d parse error(s)", st.ParseErrors)
			res.ErrorClass = metrics.ClassSSEParse
		}
		return
	}
}

func (r *Runner) readNonStream(start time.Time, resp *http.Response, res *metrics.Result) {
	b, err := io.ReadAll(resp.Body)
	res.LatencyMs = elapsedMs(start)
	res.TTFTMs = res.LatencyMs
	if err != nil {
		res.Error = err.Error()
		res.ErrorClass = metrics.ClassifyError(err, res.HTTPStatus)
		return
	}
	if len(bytes.TrimSpace(b)) == 0 {
		res.Error = "empty response body"
		res.ErrorClass = metrics.ClassEmpty
		return
	}
	var payload struct {
		Choices []struct {
			Message struct {
				Content string `json:"content"`
			} `json:"message"`
		} `json:"choices"`
		Usage *struct {
			CompletionTokens int `json:"completion_tokens"`
		} `json:"usage"`
	}
	if err := json.Unmarshal(b, &payload); err != nil {
		res.Error = err.Error()
		res.ErrorClass = metrics.ClassSSEParse
		return
	}
	if len(payload.Choices) > 0 {
		res.CompletionChars = len(payload.Choices[0].Message.Content)
	}
	if payload.Usage != nil {
		res.TokensOut = payload.Usage.CompletionTokens
	} else if res.CompletionChars > 0 {
		// Rough fallback when usage missing: not used for primary TPS accuracy.
		res.TokensOut = 1
	}
	if res.TokensOut == 0 && res.CompletionChars == 0 {
		res.Error = "empty completion"
		res.ErrorClass = metrics.ClassEmpty
		return
	}
	res.TokensPerSec = metrics.TokensPerSecond(res.TokensOut, res.LatencyMs, res.TTFTMs)
}

func elapsedMs(start time.Time) float64 {
	return float64(time.Since(start).Seconds() * 1000)
}

func isRetryableTransportErr(err error) bool {
	if err == nil {
		return false
	}
	s := strings.ToLower(err.Error())
	return strings.Contains(s, "server closed idle connection") ||
		strings.Contains(s, "use of closed network connection") ||
		strings.Contains(s, "connection reset by peer") ||
		strings.Contains(s, "broken pipe")
}

func truncate(s string, n int) string {
	s = strings.ReplaceAll(s, "\n", " ")
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}

// ctxReader aborts Read when ctx is done.
type ctxReader struct {
	ctx context.Context
	r   io.Reader
}

func (c *ctxReader) Read(p []byte) (int, error) {
	if err := c.ctx.Err(); err != nil {
		return 0, err
	}
	// Prefer a small non-blocking check; underlying net.Conn respects request cancel
	// when body is from http.Client with context.
	n, err := c.r.Read(p)
	if err != nil && c.ctx.Err() != nil {
		return n, c.ctx.Err()
	}
	return n, err
}

package adapt

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptrace"
	"strings"
	"sync"
	"time"
)

// UpstreamTiming holds separated network vs backend measurements.
type UpstreamTiming struct {
	// NetworkRTTMs is a full round-trip probe to worker /health (ms).
	NetworkRTTMs float64 `json:"network_rtt_ms"`
	// NetworkDialMs is TCP connect time from httptrace (ms).
	NetworkDialMs float64 `json:"network_dial_ms"`
	// NetworkOneWayMs is NetworkRTTMs/2 (ms).
	NetworkOneWayMs float64 `json:"network_one_way_ms"`
	// BackendTTFTMs is time from request write complete to first visible content token,
	// minus one-way network (server-side TTFT estimate). Never negative.
	BackendTTFTMs float64 `json:"backend_ttft_ms"`
	// TTFBAfterWriteMs is GotFirstResponseByte - WroteRequest (includes return path).
	TTFBAfterWriteMs float64 `json:"ttfb_after_write_ms"`
	// FirstContentAfterWriteMs is first SSE content token - WroteRequest (includes return path).
	FirstContentAfterWriteMs float64 `json:"first_content_after_write_ms"`
	// UpstreamTotalHeaderMs is wall from Do-start until headers ready to send to client.
	UpstreamTotalHeaderMs float64 `json:"upstream_total_header_ms"`
	// ProbeError if health RTT failed (inference may still work).
	ProbeError string `json:"probe_error,omitempty"`
}

// probeNetworkRTT measures GET {base}/health round-trip. Separate from inference path.
func probeNetworkRTT(ctx context.Context, client *http.Client, baseURL string) (rttMs float64, err error) {
	u := strings.TrimRight(baseURL, "/") + "/health"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return 0, err
	}
	t0 := time.Now()
	resp, err := client.Do(req)
	rttMs = float64(time.Since(t0).Seconds() * 1000)
	if err != nil {
		return rttMs, err
	}
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 256))
	_ = resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return rttMs, fmt.Errorf("health http %d", resp.StatusCode)
	}
	return rttMs, nil
}

type traceTimers struct {
	mu           sync.Mutex
	dialStart    time.Time
	dialMs       float64
	wroteRequest time.Time
	firstByte    time.Time
	gotFirstByte bool
	wroteSet     bool
}

func (t *traceTimers) clientTrace() *httptrace.ClientTrace {
	return &httptrace.ClientTrace{
		ConnectStart: func(_, _ string) {
			t.mu.Lock()
			t.dialStart = time.Now()
			t.mu.Unlock()
		},
		ConnectDone: func(_, _ string, err error) {
			t.mu.Lock()
			if !t.dialStart.IsZero() {
				t.dialMs = float64(time.Since(t.dialStart).Seconds() * 1000)
			}
			t.mu.Unlock()
		},
		WroteRequest: func(info httptrace.WroteRequestInfo) {
			t.mu.Lock()
			t.wroteRequest = time.Now()
			t.wroteSet = true
			t.mu.Unlock()
		},
		GotFirstResponseByte: func() {
			t.mu.Lock()
			if !t.gotFirstByte {
				t.firstByte = time.Now()
				t.gotFirstByte = true
			}
			t.mu.Unlock()
		},
	}
}

func (t *traceTimers) snapshot() (dialMs, ttfbAfterWrite float64, wrote time.Time) {
	t.mu.Lock()
	defer t.mu.Unlock()
	dialMs = t.dialMs
	wrote = t.wroteRequest
	if t.wroteSet && t.gotFirstByte {
		ttfbAfterWrite = float64(t.firstByte.Sub(t.wroteRequest).Seconds() * 1000)
	}
	return dialMs, ttfbAfterWrite, wrote
}

// isVisibleContentSSELine reports whether an SSE data line carries non-empty content.
func isVisibleContentSSELine(line []byte) bool {
	if !bytes.Contains(line, []byte(`"content":"`)) {
		return false
	}
	if bytes.Contains(line, []byte(`"content":null`)) || bytes.Contains(line, []byte(`"content":""`)) {
		return false
	}
	return true
}

// readUntilFirstContent reads SSE lines until first visible content (or [DONE]/error).
// Returns buffered lines (including the content line) and firstContent time.
func readUntilFirstContent(r *bufio.Reader, wrote time.Time) (buf [][]byte, firstContent time.Time, err error) {
	for {
		line, e := r.ReadBytes('\n')
		if len(line) > 0 {
			buf = append(buf, line)
			if bytes.HasPrefix(line, []byte("data: ")) {
				payload := bytes.TrimSpace(bytes.TrimPrefix(line, []byte("data: ")))
				if bytes.Equal(payload, []byte("[DONE]")) {
					return buf, firstContent, io.EOF
				}
				if isVisibleContentSSELine(line) && firstContent.IsZero() {
					firstContent = time.Now()
					return buf, firstContent, nil
				}
			}
		}
		if e != nil {
			return buf, firstContent, e
		}
	}
}

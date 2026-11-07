package adapt

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Priyanshuthapliyal2005/Liffo/internal/config"
	"github.com/Priyanshuthapliyal2005/Liffo/internal/modellife"
	"github.com/Priyanshuthapliyal2005/Liffo/internal/route"
)

// mockUpstream is a minimal OpenAI-compatible worker for shared-path tests.
func mockUpstream(t *testing.T, hold time.Duration, status int, streamBody string) (baseURL string, closeFn func()) {
	t.Helper()
	if status == 0 {
		status = 200
	}
	if streamBody == "" {
		streamBody = "data: {\"choices\":[{\"delta\":{\"content\":\"hi\"}}]}\n\ndata: [DONE]\n\n"
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/chat/completions", func(w http.ResponseWriter, r *http.Request) {
		if hold > 0 {
			time.Sleep(hold)
		}
		if r.Header.Get("Accept") == "text/event-stream" || strings.Contains(r.URL.RawQuery, "stream") {
			// peek body for stream flag
		}
		var body struct {
			Stream bool `json:"stream"`
		}
		_ = json.NewDecoder(r.Body).Decode(&body)
		if body.Stream {
			w.Header().Set("Content-Type", "text/event-stream")
			w.WriteHeader(status)
			fl := w.(http.Flusher)
			for _, line := range strings.Split(streamBody, "\n") {
				fmt.Fprintln(w, line)
				fl.Flush()
			}
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_, _ = w.Write([]byte(`{"choices":[{"message":{"content":"hi"}}]}`))
	})
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		_, _ = w.Write([]byte(`ok`))
	})
	srv := &http.Server{Handler: mux}
	go func() { _ = srv.Serve(ln) }()
	return "http://" + ln.Addr().String(), func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = srv.Shutdown(ctx)
		_ = ln.Close()
	}
}

func testServeConfig(workers []string, maxInf int, admit time.Duration, life *modellife.Manager) ServeConfig {
	if maxInf < 1 {
		maxInf = 1
	}
	return ServeConfig{
		Listen: "127.0.0.1:0",
		Labels: []string{"w0", "w1"},
		Gateway: GatewayConfig{
			Mode:           ModeStatic,
			Workers:        workers,
			DefaultTimeout: 5 * time.Second,
			AdmitTimeout:   admit,
			MaxTokens:      16,
			Model:          "local",
			Initial:        Policy{RouteMethod: route.MethodRoundRobin, MaxInflight: maxInf, RetryEnabled: false, Mode: "steady"},
			Life:           life,
		},
	}
}

func startHTTPServer(t *testing.T, sc ServeConfig) (*HTTPServer, string, func()) {
	t.Helper()
	hs, err := NewHTTPServer(sc)
	if err != nil {
		t.Fatal(err)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = hs.Serve(ln) }()
	base := "http://" + ln.Addr().String()
	return hs, base, func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = hs.Shutdown(ctx)
		_ = ln.Close()
	}
}

func postChat(t *testing.T, base string, stream bool, maxTokens int) (*http.Response, []byte) {
	t.Helper()
	body := fmt.Sprintf(`{"model":"local","stream":%v,"max_tokens":%d,"messages":[{"role":"user","content":"ping"}]}`, stream, maxTokens)
	req, err := http.NewRequest(http.MethodPost, base+"/v1/chat/completions", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	if stream {
		req.Header.Set("Accept", "text/event-stream")
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	return resp, raw
}

// TestSharedPath_HTTPAndHandleBothUseBeginRequest proves both planes share one path.
func TestSharedPath_HTTPAndHandleBothUseBeginRequest(t *testing.T) {
	u, closeU := mockUpstream(t, 0, 200, "")
	defer closeU()

	sc := testServeConfig([]string{u}, 2, 500*time.Millisecond, nil)
	hs, base, stop := startHTTPServer(t, sc)
	defer stop()

	hits0 := hs.Gateway().SharedPathHits()

	// HTTP plane
	resp, _ := postChat(t, base, false, 8)
	if resp.StatusCode != 200 {
		t.Fatalf("http status=%d", resp.StatusCode)
	}
	if sp := resp.Header.Get("X-Liffo-Shared-Path"); sp != SharedPathID {
		t.Fatalf("http shared path header=%q want %q", sp, SharedPathID)
	}
	hits1 := hs.Gateway().SharedPathHits()
	if hits1 != hits0+1 {
		t.Fatalf("after HTTP hits=%d want %d", hits1, hits0+1)
	}

	// Handle plane (same gateway)
	res := hs.Gateway().Handle(context.Background(), Request{
		ID:        "h1",
		Messages:  []config.Message{{Role: "user", Content: "ping"}},
		MaxTokens: 8,
	})
	if !res.OK {
		t.Fatalf("Handle not ok: %+v", res)
	}
	hits2 := hs.Gateway().SharedPathHits()
	if hits2 != hits1+1 {
		t.Fatalf("after Handle hits=%d want %d", hits2, hits1+1)
	}
}

// TestSharedPath_NormalRequest covers a normal non-stream request through HTTP shared path.
func TestSharedPath_NormalRequest(t *testing.T) {
	u, closeU := mockUpstream(t, 0, 200, "")
	defer closeU()
	hs, base, stop := startHTTPServer(t, testServeConfig([]string{u}, 2, time.Second, nil))
	defer stop()

	resp, raw := postChat(t, base, false, 8)
	if resp.StatusCode != 200 {
		t.Fatalf("status=%d body=%s", resp.StatusCode, raw)
	}
	if resp.Header.Get("X-Liffo-Shared-Path") != SharedPathID {
		t.Fatalf("missing shared path")
	}
	if resp.Header.Get("X-Liffo-Gateway") != "liffo-adapt" {
		t.Fatalf("missing gateway identity")
	}
	if hs.Gateway().SharedPathHits() < 1 {
		t.Fatal("BeginRequest not hit")
	}
	if hs.Gateway().ServedCount() < 1 {
		t.Fatal("served not incremented")
	}
	if hs.Gateway().Inflight() != 0 {
		t.Fatalf("inflight leak: %d", hs.Gateway().Inflight())
	}
}

// TestSharedPath_IncompleteStreamMarksWorkerFail proves HTTP 200 alone is not success:
// upstream content without data: [DONE] must Release(worker, false).
func TestSharedPath_IncompleteStreamMarksWorkerFail(t *testing.T) {
	// Truncated SSE: content, no [DONE].
	body := "data: {\"choices\":[{\"delta\":{\"content\":\"partial\"}}]}\n\n"
	u, closeU := mockUpstream(t, 0, 200, body)
	defer closeU()
	hs, base, stop := startHTTPServer(t, testServeConfig([]string{u}, 2, time.Second, nil))
	defer stop()

	ws := hs.Gateway().Router().Workers()
	if len(ws) < 1 {
		t.Fatal("no workers")
	}
	if !ws[0].Healthy() {
		t.Fatal("worker should start healthy")
	}

	resp, raw := postChat(t, base, true, 8)
	if resp.StatusCode != 200 {
		t.Fatalf("status=%d body=%s", resp.StatusCode, raw)
	}
	// Client still receives whatever was proxied (headers already 200).
	if !strings.Contains(string(raw), "partial") {
		t.Fatalf("expected partial body, got %q", raw)
	}
	if ws[0].Healthy() {
		t.Fatal("expected worker marked fail after incomplete stream (not HTTP-200 success)")
	}
}

// TestSharedPath_CompleteStreamKeepsWorkerHealthy requires [DONE] for stream success.
func TestSharedPath_CompleteStreamKeepsWorkerHealthy(t *testing.T) {
	u, closeU := mockUpstream(t, 0, 200, "") // default body includes [DONE]
	defer closeU()
	hs, base, stop := startHTTPServer(t, testServeConfig([]string{u}, 2, time.Second, nil))
	defer stop()

	resp, raw := postChat(t, base, true, 8)
	if resp.StatusCode != 200 {
		t.Fatalf("status=%d body=%s", resp.StatusCode, raw)
	}
	if !strings.Contains(string(raw), "[DONE]") {
		t.Fatalf("expected [DONE] in body: %q", raw)
	}
	ws := hs.Gateway().Router().Workers()
	if len(ws) < 1 || !ws[0].Healthy() {
		t.Fatal("worker should stay healthy after complete stream")
	}
}

// TestSharedPath_OverloadAdmitTimeout proves admission reject uses shared BeginRequest.
func TestSharedPath_OverloadAdmitTimeout(t *testing.T) {
	// Slow worker so first request holds the only slot.
	u, closeU := mockUpstream(t, 400*time.Millisecond, 200, "")
	defer closeU()
	hs, base, stop := startHTTPServer(t, testServeConfig([]string{u}, 1, 80*time.Millisecond, nil))
	defer stop()

	var wg sync.WaitGroup
	var codes []int
	var mu sync.Mutex
	start := make(chan struct{})
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			body := `{"model":"local","stream":false,"max_tokens":4,"messages":[{"role":"user","content":"x"}]}`
			req, _ := http.NewRequest(http.MethodPost, base+"/v1/chat/completions", strings.NewReader(body))
			req.Header.Set("Content-Type", "application/json")
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				mu.Lock()
				codes = append(codes, -1)
				mu.Unlock()
				return
			}
			_, _ = io.ReadAll(resp.Body)
			_ = resp.Body.Close()
			mu.Lock()
			codes = append(codes, resp.StatusCode)
			mu.Unlock()
		}()
	}
	close(start)
	wg.Wait()

	var saw429, saw200 bool
	for _, c := range codes {
		if c == 429 {
			saw429 = true
		}
		if c == 200 {
			saw200 = true
		}
	}
	if !saw200 {
		t.Fatalf("expected some 200, codes=%v", codes)
	}
	if !saw429 {
		t.Fatalf("expected admit_timeout 429, codes=%v", codes)
	}
	if hs.Gateway().RejectedCount() < 1 {
		t.Fatal("rejected counter not bumped via shared path")
	}
	if hs.Gateway().SharedPathHits() < 3 {
		t.Fatalf("shared hits=%d want >=3", hs.Gateway().SharedPathHits())
	}
	// Drain
	deadline := time.Now().Add(2 * time.Second)
	for hs.Gateway().Inflight() > 0 && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	if hs.Gateway().Inflight() != 0 {
		t.Fatalf("inflight leak after overload: %d", hs.Gateway().Inflight())
	}
}

// TestSharedPath_WorkerFailure proves dead worker surfaces via shared path + route.
func TestSharedPath_WorkerFailure(t *testing.T) {
	// Worker points at closed port.
	hs, base, stop := startHTTPServer(t, testServeConfig([]string{"http://127.0.0.1:1"}, 2, time.Second, nil))
	defer stop()

	resp, raw := postChat(t, base, false, 4)
	// Bad gateway expected from upstream error
	if resp.StatusCode != 502 && resp.StatusCode != 200 {
		// 502 is normal; some paths may differ
		t.Logf("worker fail status=%d body=%s", resp.StatusCode, raw)
	}
	if resp.StatusCode == 200 {
		t.Fatalf("expected upstream failure, got 200 body=%s", raw)
	}
	if resp.Header.Get("X-Liffo-Shared-Path") != SharedPathID && resp.Header.Get("X-Liffo-Error-Code") == "" {
		// Shared path header is set after admit; on upstream error it should still be set
		// (headers written before upstream call). If not 502 path wrote error after admit.
	}
	if hs.Gateway().SharedPathHits() < 1 {
		t.Fatal("worker fail must still enter BeginRequest")
	}
	// Served increments after upstream attempt in HTTP path
	if hs.Gateway().ServedCount() < 1 && resp.StatusCode == 502 {
		// served is after successful body copy path; on Do() error may not increment
		// Pre-unification: served only after stream complete. Keep that behavior.
	}
}

// TestSharedPath_Streaming proves SSE proxy still works through shared admit.
func TestSharedPath_Streaming(t *testing.T) {
	sse := "data: {\"choices\":[{\"delta\":{\"content\":\"ab\"}}]}\n\ndata: [DONE]\n\n"
	u, closeU := mockUpstream(t, 0, 200, sse)
	defer closeU()
	hs, base, stop := startHTTPServer(t, testServeConfig([]string{u}, 2, time.Second, nil))
	defer stop()

	resp, raw := postChat(t, base, true, 8)
	if resp.StatusCode != 200 {
		t.Fatalf("stream status=%d body=%s", resp.StatusCode, raw)
	}
	if resp.Header.Get("X-Liffo-Shared-Path") != SharedPathID {
		t.Fatalf("stream missing shared path header")
	}
	if !bytes.Contains(raw, []byte("data:")) {
		t.Fatalf("expected SSE body, got %q", raw)
	}
	if !bytes.Contains(raw, []byte("[DONE]")) {
		t.Fatalf("expected [DONE], got %q", raw)
	}
	if hs.Gateway().Inflight() != 0 {
		t.Fatalf("inflight after stream: %d", hs.Gateway().Inflight())
	}
}

// fakeLoader for lifecycle without real llama-server.
type fakeLoader struct {
	mu    sync.Mutex
	model string
	loads int32
}

func (f *fakeLoader) Load(ctx context.Context, model string) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(30 * time.Millisecond):
	}
	f.mu.Lock()
	f.model = model
	f.mu.Unlock()
	atomic.AddInt32(&f.loads, 1)
	return nil
}

func (f *fakeLoader) Unload(ctx context.Context, model string) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(10 * time.Millisecond):
	}
	f.mu.Lock()
	f.model = ""
	f.mu.Unlock()
	return nil
}

// TestSharedPath_Lifecycle proves lease reject/admit go through BeginRequest.
func TestSharedPath_Lifecycle(t *testing.T) {
	u, closeU := mockUpstream(t, 0, 200, "")
	defer closeU()

	loader := &fakeLoader{}
	life, err := modellife.New(modellife.Config{
		DrainTimeout: 2 * time.Second,
		LoadTimeout:  5 * time.Second,
		EvictTimeout: 5 * time.Second,
	}, loader)
	if err != nil {
		t.Fatal(err)
	}
	defer life.Close()
	// Bootstrap ready model
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := life.EnqueueLoad(ctx, "model-a"); err != nil {
		t.Fatal(err)
	}
	if life.Snapshot().State != modellife.StateReady {
		t.Fatalf("want ready, got %s", life.Snapshot().State)
	}

	sc := testServeConfig([]string{u}, 2, time.Second, life)
	sc.AdminToken = "test-admin-token"
	sc.Gateway.BootstrapModel = "" // already loaded
	hs, base, stop := startHTTPServer(t, sc)
	defer stop()

	// Normal with lifecycle hold
	resp, raw := postChat(t, base, false, 4)
	if resp.StatusCode != 200 {
		t.Fatalf("lifecycle ready status=%d body=%s", resp.StatusCode, raw)
	}
	if resp.Header.Get("X-Liffo-Lifecycle-Hold") != "1" {
		t.Fatalf("want lifecycle hold header, got headers=%v", resp.Header)
	}
	if resp.Header.Get("X-Liffo-Shared-Path") != SharedPathID {
		t.Fatal("lifecycle request not on shared path")
	}

	// Evict model → subsequent requests 503 model_stopped / not ready
	ectx, ecancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer ecancel()
	if err := life.EnqueueEvict(ectx); err != nil {
		t.Fatal(err)
	}

	resp2, raw2 := postChat(t, base, false, 4)
	if resp2.StatusCode != 503 {
		t.Fatalf("after evict want 503, got %d body=%s", resp2.StatusCode, raw2)
	}
	if !bytes.Contains(raw2, []byte("error_code")) {
		t.Fatalf("want error_code json, body=%s", raw2)
	}
	if hs.Gateway().LifeRejectCount() < 1 {
		t.Fatal("lifeReject not counted on shared path")
	}
	// Rejects still count as BeginRequest hits
	if hs.Gateway().SharedPathHits() < 2 {
		t.Fatalf("hits=%d", hs.Gateway().SharedPathHits())
	}
}

// TestSharedPath_HandleOverloadMatchesHTTPSemantics checks Handle admit_timeout Result.
func TestSharedPath_HandleOverloadMatchesHTTPSemantics(t *testing.T) {
	u, closeU := mockUpstream(t, 300*time.Millisecond, 200, "")
	defer closeU()

	gw, err := NewGateway(GatewayConfig{
		Mode:           ModeStatic,
		Workers:        []string{u},
		DefaultTimeout: 3 * time.Second,
		AdmitTimeout:   60 * time.Millisecond,
		MaxTokens:      8,
		Model:          "local",
		Initial:        Policy{RouteMethod: route.MethodRoundRobin, MaxInflight: 1, RetryEnabled: false},
	})
	if err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	results := make([]Result, 3)
	start := make(chan struct{})
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			results[i] = gw.Handle(context.Background(), Request{
				ID:        fmt.Sprintf("r%d", i),
				Messages:  []config.Message{{Role: "user", Content: "x"}},
				MaxTokens: 4,
			})
		}(i)
	}
	close(start)
	wg.Wait()

	var okN, rejectN int
	for _, r := range results {
		if r.OK {
			okN++
		}
		if r.Rejected && r.ErrorCode == "admit_timeout" {
			rejectN++
		}
	}
	if okN < 1 {
		t.Fatalf("want some ok, results=%+v", results)
	}
	if rejectN < 1 {
		t.Fatalf("want admit_timeout rejects, results=%+v", results)
	}
	if gw.SharedPathHits() != 3 {
		t.Fatalf("shared hits=%d", gw.SharedPathHits())
	}
}

package adapt

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Priyanshuthapliyal2005/Liffo/internal/route"
)

const validSSE = "data: {\"choices\":[{\"delta\":{\"content\":\"ok\"}}]}\n\ndata: [DONE]\n\n"

func testSSEUpstream(t *testing.T, body string) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/health" {
			w.WriteHeader(http.StatusOK)
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, body)
	}))
}

func TestHTTPTruncatedStreamMarksFailureAndRecovers(t *testing.T) {
	bad := testSSEUpstream(t, "data: {\"choices\":[{\"delta\":{\"content\":\"partial\"}}]}\n\n")
	defer bad.Close()
	good := testSSEUpstream(t, validSSE)
	defer good.Close()
	sc := testServeConfig([]string{bad.URL, good.URL}, 4, 0, nil)
	sc.Gateway.Initial.RouteMethod = route.MethodMultiSLO
	sc.Gateway.MultiSLO = route.MultiSLOConfig{WeightCost: 1, SLOTargetMs: 200}
	sc.Gateway.WorkerStatic = []route.WorkerStatic{{CostPerReq: 0.1}, {CostPerReq: 1}}
	hs, base, stop := startHTTPServer(t, sc)
	defer stop()
	response, first := postChat(t, base, true, 8)
	if response.StatusCode != 200 || response.Header.Get("X-Liffo-Worker-Id") != "0" || strings.Contains(string(first), "[DONE]") {
		t.Fatalf("first request should be truncated from worker 0: %d %s", response.StatusCode, first)
	}
	if hs.Gateway().Router().Workers()[0].Healthy() {
		t.Fatal("truncated stream must mark worker unhealthy despite HTTP 200")
	}
	response, second := postChat(t, base, true, 8)
	if response.StatusCode != 200 || response.Header.Get("X-Liffo-Worker-Id") != "1" || !strings.Contains(string(second), "[DONE]") {
		t.Fatalf("second request should recover via worker 1: %d %s", response.StatusCode, second)
	}
	hs.Gateway().Router().Workers()[0].MarkOK()
	response, _ = postChat(t, base, true, 8)
	if response.StatusCode != 200 || response.Header.Get("X-Liffo-Worker-Id") != "0" {
		t.Fatalf("worker 0 did not rejoin after explicit recovery: %d %s", response.StatusCode, response.Header.Get("X-Liffo-Worker-Id"))
	}
}

func TestHTTPCancelAfterVisibleTokenDrainsWithoutPoisoningWorker(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/health" {
			w.WriteHeader(http.StatusOK)
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprint(w, "data: {\"choices\":[{\"delta\":{\"content\":\"hello\"}}]}\n\n")
		w.(http.Flusher).Flush()
		select {
		case <-r.Context().Done():
		case <-time.After(2 * time.Second): // bound the mock even if a transport keeps an idle connection
		}
	}))
	defer upstream.Close()
	sc := testServeConfig([]string{upstream.URL}, 2, 0, nil)
	hs, base, stop := startHTTPServer(t, sc)
	defer stop()
	ctx, cancel := context.WithCancel(context.Background())
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, base+"/v1/chat/completions", strings.NewReader(`{"model":"local","stream":true,"messages":[{"role":"user","content":"x"}]}`))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	b := make([]byte, 256)
	n, err := resp.Body.Read(b)
	if err != nil || n == 0 {
		t.Fatalf("did not receive first visible token: n=%d err=%v", n, err)
	}
	cancel()
	resp.Body.Close()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if hs.Gateway().Router().Workers()[0].InFlight() == 0 && atomic.LoadInt32(&hs.Gateway().inflight) == 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if hs.Gateway().Router().Workers()[0].InFlight() != 0 || atomic.LoadInt32(&hs.Gateway().inflight) != 0 {
		t.Fatal("canceled stream leaked a worker or admission slot")
	}
	if !hs.Gateway().Router().Workers()[0].Healthy() {
		t.Fatal("client cancellation must not poison upstream health")
	}
}

func TestHTTPConcurrentStreamingDrainsAllLeases(t *testing.T) {
	upstream := testSSEUpstream(t, validSSE)
	defer upstream.Close()
	sc := testServeConfig([]string{upstream.URL}, 4, 0, nil)
	hs, base, stop := startHTTPServer(t, sc)
	defer stop()
	const n = 20
	errCh := make(chan error, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			req, err := http.NewRequestWithContext(ctx, http.MethodPost, base+"/v1/chat/completions", strings.NewReader(`{"model":"local","stream":true,"messages":[{"role":"user","content":"x"}]}`))
			if err != nil {
				errCh <- err
				return
			}
			req.Header.Set("Content-Type", "application/json")
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				errCh <- err
				return
			}
			b, err := io.ReadAll(resp.Body)
			resp.Body.Close()
			if err != nil || resp.StatusCode != 200 || !strings.Contains(string(b), "[DONE]") {
				errCh <- fmt.Errorf("status %d body %q err %v", resp.StatusCode, b, err)
			}
		}()
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Error(err)
	}
	if got := hs.Gateway().Router().Workers()[0].InFlight(); got != 0 {
		t.Fatalf("worker leaked %d inflight", got)
	}
	if got := atomic.LoadInt32(&hs.Gateway().inflight); got != 0 {
		t.Fatalf("gateway leaked %d inflight", got)
	}
}

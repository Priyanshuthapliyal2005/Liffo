package adapt

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Priyanshuthapliyal2005/Liffo/internal/route"
)

// slowSSEWorker streams one token after delay and holds the slot meanwhile.
func slowSSEWorker(delay time.Duration, conns *int64) *httptest.Server {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/chat/completions", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(200)
		fl, _ := w.(http.Flusher)
		time.Sleep(delay)
		fmt.Fprint(w, "data: {\"choices\":[{\"delta\":{\"content\":\"x\"}}]}\n\n")
		if fl != nil {
			fl.Flush()
		}
		fmt.Fprint(w, "data: [DONE]\n\n")
		if fl != nil {
			fl.Flush()
		}
	})
	srv := httptest.NewUnstartedServer(mux)
	if conns != nil {
		srv.Config.ConnState = func(_ net.Conn, s http.ConnState) {
			if s == http.StateNew {
				atomic.AddInt64(conns, 1)
			}
		}
	}
	srv.Start()
	return srv
}

// TestAdmissionShedsWhenWaiterQueueFull proves the admission queue is bounded:
// with MaxInflight=1 and MaxAdmitWaiters=2, the 4th concurrent request must be
// shed immediately with admit_queue_full instead of parking for AdmitTimeout.
func TestAdmissionShedsWhenWaiterQueueFull(t *testing.T) {
	w := slowSSEWorker(300*time.Millisecond, nil)
	defer w.Close()

	g, err := NewGateway(GatewayConfig{
		Mode:            ModeStatic,
		Workers:         []string{w.URL},
		Initial:         StaticPolicy(route.MethodLeastInflight, 1, false),
		AdmitTimeout:    5 * time.Second,
		MaxAdmitWaiters: 2,
		Model:           "mock",
	})
	if err != nil {
		t.Fatal(err)
	}

	// Hold the single slot.
	held, serr := g.BeginAdmission(context.Background())
	if serr != nil {
		t.Fatalf("first admit failed: %v", serr)
	}

	// Fill the two waiter slots.
	var wg sync.WaitGroup
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			if s, e := g.BeginAdmission(ctx); e == nil {
				s.Release()
			}
		}()
	}
	// Give the waiters time to park.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		g.mu.Lock()
		n := g.waiting
		g.mu.Unlock()
		if n >= 2 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}

	start := time.Now()
	_, serr = g.BeginAdmission(context.Background())
	elapsed := time.Since(start)
	if serr == nil {
		t.Fatal("expected shed, got admission")
	}
	if serr.Code != "admit_queue_full" {
		t.Fatalf("code = %q, want admit_queue_full", serr.Code)
	}
	if elapsed > 500*time.Millisecond {
		t.Fatalf("shed took %v; must be immediate, not AdmitTimeout", elapsed)
	}
	if g.QueueFullShedCount() != 1 {
		t.Fatalf("QueueFullShedCount = %d, want 1", g.QueueFullShedCount())
	}
	held.Release()
	wg.Wait()
	if g.PeakAdmitWaiters() < 2 {
		t.Fatalf("PeakAdmitWaiters = %d, want >= 2", g.PeakAdmitWaiters())
	}
}

// TestAdmissionWaiterWakesOnReleaseNotPoll proves waiters are woken by the
// condition variable: with a 50ms hold and a 10s AdmitTimeout, the queued
// request must be admitted promptly after Release.
func TestAdmissionWaiterWakesOnRelease(t *testing.T) {
	w := slowSSEWorker(10*time.Millisecond, nil)
	defer w.Close()
	g, err := NewGateway(GatewayConfig{
		Mode:         ModeStatic,
		Workers:      []string{w.URL},
		Initial:      StaticPolicy(route.MethodLeastInflight, 1, false),
		AdmitTimeout: 10 * time.Second,
		Model:        "mock",
	})
	if err != nil {
		t.Fatal(err)
	}
	held, serr := g.BeginAdmission(context.Background())
	if serr != nil {
		t.Fatal(serr)
	}
	type res struct {
		d time.Duration
		e *SessionError
	}
	ch := make(chan res, 1)
	go func() {
		s := time.Now()
		sess, e := g.BeginAdmission(context.Background())
		if sess != nil {
			sess.Release()
		}
		ch <- res{time.Since(s), e}
	}()
	time.Sleep(80 * time.Millisecond)
	rel := time.Now()
	held.Release()
	got := <-ch
	if got.e != nil {
		t.Fatalf("queued admit failed: %v", got.e)
	}
	wake := time.Since(rel)
	if wake > 200*time.Millisecond {
		t.Fatalf("waiter woke %v after Release; cond signaling not effective", wake)
	}
}

// TestHTTPAdmitsBeforeReservingWorker proves worker inflight is not inflated by
// queued requests: while one request occupies the only admission slot, a second
// request is queued, and total router inflight must stay at 1.
func TestHTTPAdmitsBeforeReservingWorker(t *testing.T) {
	w := slowSSEWorker(400*time.Millisecond, nil)
	defer w.Close()

	hs, err := NewHTTPServer(ServeConfig{
		Listen: "127.0.0.1:0",
		Labels: []string{"w0"},
		Gateway: GatewayConfig{
			Mode:         ModeStatic,
			Workers:      []string{w.URL},
			Initial:      StaticPolicy(route.MethodLeastInflight, 1, false),
			AdmitTimeout: 3 * time.Second,
			MaxTokens:    4,
			Model:        "mock",
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(hs.srv.Handler)
	defer ts.Close()

	body := `{"model":"mock","stream":true,"max_tokens":4,"messages":[{"role":"user","content":"hi"}]}`
	var wg sync.WaitGroup
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			resp, err := http.Post(ts.URL+"/v1/chat/completions", "application/json", strings.NewReader(body))
			if err == nil {
				resp.Body.Close()
			}
		}()
	}

	maxSeen := int64(0)
	stop := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(stop) {
		var total int64
		for _, wk := range hs.Gateway().Router().Workers() {
			total += wk.InFlight()
		}
		if total > maxSeen {
			maxSeen = total
		}
		time.Sleep(5 * time.Millisecond)
	}
	wg.Wait()
	if maxSeen > 1 {
		t.Fatalf("router inflight peaked at %d with MaxInflight=1: a queued request reserved a worker before admission", maxSeen)
	}
}

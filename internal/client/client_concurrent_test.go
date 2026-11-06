package client

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Priyanshuthapliyal2005/Liffo/internal/config"
	"github.com/Priyanshuthapliyal2005/Liffo/internal/metrics"
)

func TestConcurrentStreams(t *testing.T) {
	var inflight int32
	var maxInflight int32
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		cur := atomic.AddInt32(&inflight, 1)
		for {
			old := atomic.LoadInt32(&maxInflight)
			if cur <= old || atomic.CompareAndSwapInt32(&maxInflight, old, cur) {
				break
			}
		}
		defer atomic.AddInt32(&inflight, -1)

		w.Header().Set("Content-Type", "text/event-stream")
		flusher, _ := w.(http.Flusher)
		// Simulate small generation delay under load
		time.Sleep(30 * time.Millisecond)
		fmt.Fprint(w, "data: {\"choices\":[{\"delta\":{\"content\":\"x\"}}]}\n\n")
		if flusher != nil {
			flusher.Flush()
		}
		fmt.Fprint(w, "data: {\"usage\":{\"completion_tokens\":1}}\n\n")
		fmt.Fprint(w, "data: [DONE]\n\n")
	})
	srv := httptest.NewServer(h)
	defer srv.Close()

	cfg := config.Defaults()
	cfg.BaseURL = srv.URL
	cfg.Stream = true
	cfg.TimeoutSec = 10
	cfg.Concurrency = 4
	cfg.Mode = "test"
	cfg.SampleIntervalMs = 100
	run := NewRunner(cfg)
	defer run.Close()

	const n = 16
	var wg sync.WaitGroup
	errs := make(chan metrics.Result, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			res := run.RunOneWorker(ctx, testPrompt(), i+1, false, (i%4)+1)
			cancel()
			errs <- res
		}(i)
	}
	wg.Wait()
	close(errs)

	ok := 0
	for res := range errs {
		if res.Error != "" {
			t.Errorf("worker error: %s class=%s", res.Error, res.ErrorClass)
			continue
		}
		if res.Concurrency != 4 {
			t.Errorf("concurrency field=%d", res.Concurrency)
		}
		ok++
	}
	if ok != n {
		t.Fatalf("ok=%d want=%d", ok, n)
	}
	if atomic.LoadInt32(&maxInflight) < 2 {
		t.Fatalf("expected concurrent inflight, max=%d", maxInflight)
	}
}

func TestConcurrentServerBusyStillClassifies(t *testing.T) {
	// Server returns 503 under overload
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "busy", http.StatusServiceUnavailable)
	})
	srv := httptest.NewServer(h)
	defer srv.Close()

	cfg := config.Defaults()
	cfg.BaseURL = srv.URL
	cfg.Stream = true
	cfg.TimeoutSec = 5
	cfg.Concurrency = 4
	run := NewRunner(cfg)
	defer run.Close()

	var wg sync.WaitGroup
	var httpErrs int32
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			res := run.RunOne(context.Background(), testPrompt(), 1, false)
			if res.ErrorClass == metrics.ClassHTTP {
				atomic.AddInt32(&httpErrs, 1)
			}
		}()
	}
	wg.Wait()
	if httpErrs < 8 {
		t.Fatalf("httpErrs=%d", httpErrs)
	}
}

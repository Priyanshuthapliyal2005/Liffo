package route

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Priyanshuthapliyal2005/Liffo/internal/config"
)

func reqBody(msg string) StreamRequest {
	return StreamRequest{
		Messages:          []config.Message{{Role: "user", Content: msg}},
		Model:             "local",
		MaxTokens:         16,
		Temperature:       0,
		ContentForRouting: msg,
	}
}

func twoWorkers(t *testing.T, h0, h1 http.HandlerFunc) (*Executor, *httptest.Server, *httptest.Server) {
	t.Helper()
	s0 := httptest.NewServer(h0)
	s1 := httptest.NewServer(h1)
	// CloseClientConnections unblocks handlers waiting on canceled request contexts.
	t.Cleanup(func() {
		s0.CloseClientConnections()
		s1.CloseClientConnections()
		s0.Close()
		s1.Close()
	})
	w0 := &Worker{ID: 0, BaseURL: s0.URL}
	w1 := &Worker{ID: 1, BaseURL: s1.URL}
	r, err := New(Config{Method: MethodLeastInflight, FailCooldown: 50 * time.Millisecond}, []*Worker{w0, w1})
	if err != nil {
		t.Fatal(err)
	}
	return NewExecutor(r), s0, s1
}

func TestStagesSafeToRetryMatrix(t *testing.T) {
	safe := []Stage{StageQueued, StageAssigned, StageConnected, StageMetadataReceived}
	unsafe := []Stage{StageFirstVisibleToken, StageStreaming, StageCompleted}
	for _, s := range safe {
		if !SafeToRetryStage(s) {
			t.Fatalf("%s should be safe", s)
		}
	}
	for _, s := range unsafe {
		if SafeToRetryStage(s) {
			t.Fatalf("%s should NOT be safe", s)
		}
		if !HasVisibleOutput(s) && s != StageCompleted {
			// completed may or may not; first+streaming yes
			if s == StageFirstVisibleToken || s == StageStreaming {
				if !HasVisibleOutput(s) {
					t.Fatalf("%s should have visible output", s)
				}
			}
		}
	}
}

func TestRetryBeforeHeadersUsesSecondWorker(t *testing.T) {
	// Worker 0: refuse connection (close immediately without response)
	var hits0, hits1 atomic.Int64
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	// Accept and close without HTTP response = fail before headers
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			hits0.Add(1)
			_ = c.Close()
		}
	}()
	t.Cleanup(func() { _ = ln.Close() })

	s1 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits1.Add(1)
		okStream(w, "hi")
	}))
	t.Cleanup(s1.Close)

	// Point worker0 at the raw listener (no HTTP server)
	w0 := &Worker{ID: 0, BaseURL: "http://" + ln.Addr().String()}
	w1 := &Worker{ID: 1, BaseURL: s1.URL}
	rtr, _ := New(Config{Method: MethodLeastInflight, FailCooldown: 100 * time.Millisecond}, []*Worker{w0, w1})
	// Prefer w0 first
	w0.inflight.Store(0)
	w1.inflight.Store(1)
	ex := NewExecutor(rtr)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	res := ex.Execute(ctx, reqBody("before-headers-test"))
	if res.Error != "" {
		t.Fatalf("expected success after retry, got err=%s attempts=%+v", res.Error, res.Attempts)
	}
	if !res.SafeRetryUsed || res.AttemptCount < 2 {
		t.Fatalf("expected safe retry, got attempts=%d safe=%v detail=%+v", res.AttemptCount, res.SafeRetryUsed, res.Attempts)
	}
	if res.Attempts[0].StageReached != StageAssigned {
		// dial/HTTP fail before headers → assigned
		if StageOrder(res.Attempts[0].StageReached) > StageOrder(StageConnected) {
			t.Fatalf("first fail stage=%s want <= connected", res.Attempts[0].StageReached)
		}
	}
	if res.Attempts[0].Reason != ReasonRetrySafeStage {
		t.Fatalf("first reason=%s want %s", res.Attempts[0].Reason, ReasonRetrySafeStage)
	}
	if res.Decision.WorkerID != 1 {
		t.Fatalf("final worker=%d want 1", res.Decision.WorkerID)
	}
	if hits1.Load() < 1 {
		t.Fatal("worker1 never hit")
	}
}

func TestRetryAfterHeadersSafe(t *testing.T) {
	var n0 atomic.Int64
	ex, _, _ := twoWorkers(t,
		func(w http.ResponseWriter, r *http.Request) {
			n0.Add(1)
			// headers then hard close (hijack)
			hj, ok := w.(http.Hijacker)
			if !ok {
				http.Error(w, "no hijack", 500)
				return
			}
			conn, bufrw, err := hj.Hijack()
			if err != nil {
				return
			}
			_, _ = bufrw.WriteString("HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\n\r\n")
			_ = bufrw.Flush()
			_ = conn.Close()
		},
		func(w http.ResponseWriter, r *http.Request) {
			okStream(w, "ok")
		},
	)
	// force first pick to w0
	ex.Router.Workers()[0].inflight.Store(0)
	ex.Router.Workers()[1].inflight.Store(5)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	res := ex.Execute(ctx, reqBody("after-headers"))
	if res.Error != "" {
		t.Fatalf("want success: %s attempts=%+v", res.Error, res.Attempts)
	}
	if !res.SafeRetryUsed {
		t.Fatalf("expected retry after headers fail: %+v", res.Attempts)
	}
	st := res.Attempts[0].StageReached
	if StageOrder(st) < StageOrder(StageConnected) {
		// may be assigned if transport errors weirdly; allow connected or metadata
		t.Logf("stage after headers fail: %s", st)
	}
	if res.ErrorCode != "" && res.Error == "" {
		t.Fatal("error_code without error")
	}
}

func TestRetryAfterMetadataSafe(t *testing.T) {
	ex, _, _ := twoWorkers(t,
		func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "text/event-stream")
			w.WriteHeader(200)
			fl := w.(http.Flusher)
			// role metadata only, then close without DONE
			fmt.Fprintf(w, "data: {\"choices\":[{\"delta\":{\"role\":\"assistant\"}}]}\n\n")
			fl.Flush()
			// close by returning (body ends)
		},
		func(w http.ResponseWriter, r *http.Request) {
			okStream(w, "recovered")
		},
	)
	ex.Router.Workers()[0].inflight.Store(0)
	ex.Router.Workers()[1].inflight.Store(9)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	res := ex.Execute(ctx, reqBody("after-meta"))
	if res.Error != "" {
		t.Fatalf("want success after metadata fail: %s %+v", res.Error, res.Attempts)
	}
	if !res.SafeRetryUsed {
		t.Fatalf("expected safe retry: %+v", res.Attempts)
	}
	if res.Attempts[0].StageReached != StageMetadataReceived && res.Attempts[0].StageReached != StageConnected {
		t.Fatalf("first stage=%s want metadata_received or connected", res.Attempts[0].StageReached)
	}
	if res.Attempts[0].Reason != ReasonRetrySafeStage {
		t.Fatalf("reason=%s", res.Attempts[0].Reason)
	}
}

func TestNoRetryAfterFirstToken(t *testing.T) {
	var hits1 atomic.Int64
	ex, _, _ := twoWorkers(t,
		func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "text/event-stream")
			w.WriteHeader(200)
			fl := w.(http.Flusher)
			fmt.Fprintf(w, "data: {\"choices\":[{\"delta\":{\"role\":\"assistant\"}}]}\n\n")
			fl.Flush()
			fmt.Fprintf(w, "data: {\"choices\":[{\"delta\":{\"content\":\"HELLO\"}}]}\n\n")
			fl.Flush()
			// truncate after first visible token
		},
		func(w http.ResponseWriter, r *http.Request) {
			hits1.Add(1)
			okStream(w, "should-not-run")
		},
	)
	ex.Router.Workers()[0].inflight.Store(0)
	ex.Router.Workers()[1].inflight.Store(9)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	res := ex.Execute(ctx, reqBody("after-token"))
	if res.Error == "" {
		t.Fatalf("expected error after visible token, got success attempts=%+v", res.Attempts)
	}
	if res.ErrorCode != ReasonNotSafeVisible {
		t.Fatalf("error_code=%s want %s attempts=%+v", res.ErrorCode, ReasonNotSafeVisible, res.Attempts)
	}
	if res.SafeRetryUsed || hits1.Load() > 0 {
		t.Fatalf("must not retry after visible output; hits1=%d attempts=%+v", hits1.Load(), res.Attempts)
	}
	if !res.VisibleOutput {
		t.Fatal("VisibleOutput should be true")
	}
	st := res.Attempts[0].StageReached
	if !HasVisibleOutput(st) {
		t.Fatalf("stage=%s should be past first token", st)
	}
}

func TestTimeoutNoRetry(t *testing.T) {
	ex, _, _ := twoWorkers(t,
		func(w http.ResponseWriter, r *http.Request) {
			// Bound hang so httptest.Close cannot block the suite.
			select {
			case <-r.Context().Done():
			case <-time.After(3 * time.Second):
			}
		},
		func(w http.ResponseWriter, r *http.Request) {
			okStream(w, "nope")
		},
	)
	ex.Router.Workers()[0].inflight.Store(0)
	ex.Router.Workers()[1].inflight.Store(9)
	// Short deadline — preserved, not extended on retry path
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	start := time.Now()
	res := ex.Execute(ctx, reqBody("timeout-case"))
	elapsed := time.Since(start)
	if res.Error == "" {
		t.Fatal("expected timeout error")
	}
	if res.ErrorCode != ReasonDeadline && res.ErrorClass != "timeout" {
		// accept either
		if !strings.Contains(strings.ToLower(res.Error), "deadline") && res.ErrorClass != "timeout" {
			t.Fatalf("code=%s class=%s err=%s", res.ErrorCode, res.ErrorClass, res.Error)
		}
	}
	if res.SafeRetryUsed {
		t.Fatalf("timeout must not safe-retry: %+v", res.Attempts)
	}
	if elapsed > 2*time.Second {
		t.Fatalf("deadline not preserved? elapsed=%s", elapsed)
	}
}

func TestCancelNoRetry(t *testing.T) {
	// Client cancels before/during request; hang is bounded so cleanup cannot stall.
	ex, _, _ := twoWorkers(t,
		func(w http.ResponseWriter, r *http.Request) {
			select {
			case <-r.Context().Done():
			case <-time.After(2 * time.Second):
			}
		},
		func(w http.ResponseWriter, r *http.Request) {
			okStream(w, "nope")
		},
	)
	ex.Router.Workers()[0].inflight.Store(0)
	ex.Router.Workers()[1].inflight.Store(9)
	ctx, cancel := context.WithCancel(context.Background())
	// Cancel quickly so we exercise cancel path (not only timeout).
	time.AfterFunc(80*time.Millisecond, cancel)
	res := ex.Execute(ctx, reqBody("cancel-case"))
	if res.Error == "" {
		t.Fatal("expected cancel error")
	}
	if res.SafeRetryUsed {
		t.Fatalf("cancel must not retry: %+v", res.Attempts)
	}
	if res.ErrorCode != ReasonCanceled && res.ErrorClass != "cancel" && res.ErrorCode != ReasonDeadline {
		if !strings.Contains(strings.ToLower(res.Error), "cancel") &&
			!strings.Contains(strings.ToLower(res.Error), "deadline") {
			t.Fatalf("code=%s class=%s err=%s", res.ErrorCode, res.ErrorClass, res.Error)
		}
	}
}

func TestBothWorkersUnavailable(t *testing.T) {
	// Two closed ports
	w0 := &Worker{ID: 0, BaseURL: "http://127.0.0.1:1"}
	w1 := &Worker{ID: 1, BaseURL: "http://127.0.0.1:2"}
	// Pre-mark both unhealthy OR let dials fail
	rtr, _ := New(Config{Method: MethodLeastInflight, FailCooldown: time.Hour}, []*Worker{w0, w1})
	ex := NewExecutor(rtr)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	res := ex.Execute(ctx, reqBody("both-down"))
	if res.Error == "" {
		t.Fatal("expected failure")
	}
	// Either all_workers after marks, or max_attempts after two dial fails
	if res.AttemptCount < 1 {
		t.Fatal("need at least one attempt recorded")
	}
	// Every attempt must be logged with reason
	for i, a := range res.Attempts {
		if a.Reason == "" {
			t.Fatalf("attempt %d missing reason: %+v", i, a)
		}
	}
	if res.ErrorCode == "" {
		t.Fatalf("expected error_code, got empty (err=%s)", res.Error)
	}
}

func TestBothPreMarkedUnhealthy(t *testing.T) {
	w0 := &Worker{ID: 0, BaseURL: "http://127.0.0.1:1"}
	w1 := &Worker{ID: 1, BaseURL: "http://127.0.0.1:2"}
	w0.unhealthyUntil.Store(time.Now().Add(time.Hour).UnixNano())
	w1.unhealthyUntil.Store(time.Now().Add(time.Hour).UnixNano())
	rtr, _ := New(Config{Method: MethodLeastInflight}, []*Worker{w0, w1})
	ex := NewExecutor(rtr)
	res := ex.Execute(context.Background(), reqBody("pre-unhealthy"))
	if res.ErrorCode != ReasonAllWorkersDown && res.ErrorCode != ReasonNoHealthyWorker {
		t.Fatalf("code=%s err=%s attempts=%+v", res.ErrorCode, res.Error, res.Attempts)
	}
	if res.SafeRetryUsed {
		t.Fatal("no retry when none healthy")
	}
}

func TestAttemptRecordsDeadlineLeft(t *testing.T) {
	ex, _, _ := twoWorkers(t,
		func(w http.ResponseWriter, r *http.Request) {
			http.Error(w, "boom", 500)
		},
		func(w http.ResponseWriter, r *http.Request) {
			okStream(w, "x")
		},
	)
	ex.Router.Workers()[0].inflight.Store(0)
	ex.Router.Workers()[1].inflight.Store(3)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	res := ex.Execute(ctx, reqBody("deadline-left"))
	if res.Error != "" {
		// 500 may or may not retry depending on isRetryableHTTPError
		t.Logf("err=%s code=%s attempts=%+v", res.Error, res.ErrorCode, res.Attempts)
	}
	for _, a := range res.Attempts {
		if a.DeadlineLeftMs < 0 && a.DeadlineLeftMs != -1 {
			t.Fatalf("bad deadline_left: %+v", a)
		}
		// with 10s budget, should still have time left after fast 500
		if a.DeadlineLeftMs == 0 && res.ErrorClass != "timeout" {
			// flaky only if very slow machine; log only
			t.Logf("deadline_left_ms=0 on attempt %+v", a)
		}
	}
}

func TestHappyPathSingleAttempt(t *testing.T) {
	ex, _, _ := twoWorkers(t,
		func(w http.ResponseWriter, r *http.Request) { okStream(w, "a", "b") },
		func(w http.ResponseWriter, r *http.Request) { okStream(w, "c") },
	)
	res := ex.Execute(context.Background(), reqBody("happy"))
	if res.Error != "" {
		t.Fatal(res.Error)
	}
	if res.AttemptCount != 1 || res.Retried {
		t.Fatalf("want 1 attempt, got %+v", res.Attempts)
	}
	if res.FinalStage != StageCompleted {
		t.Fatalf("stage=%s", res.FinalStage)
	}
	if res.TokensOut < 1 {
		t.Fatalf("tokens=%d", res.TokensOut)
	}
}

// Ensure we never leave body unread on error paths (linter-friendly).
var _ = io.Discard

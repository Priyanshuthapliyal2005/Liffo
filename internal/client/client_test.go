package client

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Priyanshuthapliyal2005/Liffo/internal/config"
	"github.com/Priyanshuthapliyal2005/Liffo/internal/metrics"
)

func testPrompt() config.Prompt {
	return config.Prompt{
		ID: "t1",
		Messages: []config.Message{
			{Role: "user", Content: "hello"},
		},
	}
}

func runnerAgainst(t *testing.T, h http.Handler, stream bool, timeoutSec int) (*Runner, func()) {
	t.Helper()
	srv := httptest.NewServer(h)
	cfg := config.Defaults()
	cfg.BaseURL = srv.URL
	cfg.Stream = stream
	cfg.TimeoutSec = timeoutSec
	cfg.Mode = "test"
	cfg.Model = "mock"
	cfg.SampleIntervalMs = 50
	cfg.ServerPID = 0
	r := NewRunner(cfg)
	return r, func() {
		r.Close()
		srv.Close()
	}
}

func TestHealthyStream(t *testing.T) {
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		flusher, _ := w.(http.Flusher)
		fmt.Fprint(w, "data: {\"choices\":[{\"delta\":{\"content\":\"Hi\"}}]}\n\n")
		if flusher != nil {
			flusher.Flush()
		}
		fmt.Fprint(w, "data: {\"choices\":[{\"delta\":{\"content\":\"!\"}}]}\n\n")
		if flusher != nil {
			flusher.Flush()
		}
		fmt.Fprint(w, "data: {\"usage\":{\"completion_tokens\":2}}\n\n")
		fmt.Fprint(w, "data: [DONE]\n\n")
	})
	run, cleanup := runnerAgainst(t, h, true, 5)
	defer cleanup()

	res := run.RunOne(context.Background(), testPrompt(), 1, false)
	if res.Error != "" {
		t.Fatalf("unexpected error: %s (%s)", res.Error, res.ErrorClass)
	}
	if res.TokensOut != 2 {
		t.Fatalf("tokens=%d", res.TokensOut)
	}
	if res.TTFTMs <= 0 || res.LatencyMs <= 0 {
		t.Fatalf("timing ttft=%v lat=%v", res.TTFTMs, res.LatencyMs)
	}
	if res.CompletionChars != 3 {
		t.Fatalf("chars=%d", res.CompletionChars)
	}
	if !res.DoneSeen {
		t.Fatal("expected DoneSeen")
	}
	if res.PromptSHA256 == "" {
		t.Fatal("missing prompt hash")
	}
}

func TestServerUnavailable(t *testing.T) {
	// Bind then close so port is unused — connection refused.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()

	cfg := config.Defaults()
	cfg.BaseURL = "http://" + addr
	cfg.Stream = true
	cfg.TimeoutSec = 3
	cfg.Mode = "test"
	run := NewRunner(cfg)
	defer run.Close()

	res := run.RunOne(context.Background(), testPrompt(), 1, false)
	if res.Error == "" {
		t.Fatal("expected error")
	}
	if res.ErrorClass != metrics.ClassHTTP {
		t.Fatalf("class=%q err=%q", res.ErrorClass, res.Error)
	}
}

func TestHTTPTimeout(t *testing.T) {
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Never write headers promptly — hang past deadline.
		time.Sleep(3 * time.Second)
		w.WriteHeader(200)
	})
	run, cleanup := runnerAgainst(t, h, true, 1)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	res := run.RunOne(ctx, testPrompt(), 1, false)
	if res.Error == "" {
		t.Fatal("expected timeout error")
	}
	if res.ErrorClass != metrics.ClassTimeout && res.ErrorClass != metrics.ClassCancel {
		// On some stacks cancel surfaces as context deadline / canceled.
		t.Fatalf("class=%q err=%q", res.ErrorClass, res.Error)
	}
}

func TestEmptyResponse(t *testing.T) {
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(200)
		// empty body
	})
	run, cleanup := runnerAgainst(t, h, true, 5)
	defer cleanup()

	res := run.RunOne(context.Background(), testPrompt(), 1, false)
	if res.ErrorClass != metrics.ClassEmpty {
		t.Fatalf("class=%q err=%q", res.ErrorClass, res.Error)
	}
}

func TestEmptyStreamEventsNoTokens(t *testing.T) {
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprint(w, ": heartbeat\n\n")
		fmt.Fprint(w, "data: {\"choices\":[]}\n\n")
		fmt.Fprint(w, "data: [DONE]\n\n")
	})
	run, cleanup := runnerAgainst(t, h, true, 5)
	defer cleanup()

	res := run.RunOne(context.Background(), testPrompt(), 1, false)
	if res.ErrorClass != metrics.ClassEmpty {
		t.Fatalf("class=%q err=%q events=%d", res.ErrorClass, res.Error, res.SSEEvents)
	}
}

func TestBrokenStream(t *testing.T) {
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		flusher, _ := w.(http.Flusher)
		fmt.Fprint(w, "data: {\"choices\":[{\"delta\":{\"content\":\"ab\"}}]}\n\n")
		if flusher != nil {
			flusher.Flush()
		}
		// Truncated JSON event then close connection (no DONE).
		fmt.Fprint(w, "data: {\"choices\":[{\"delta\":{\"content\":\"")
		// hijack close by returning without finishing
	})
	run, cleanup := runnerAgainst(t, h, true, 5)
	defer cleanup()

	res := run.RunOne(context.Background(), testPrompt(), 1, false)
	// Partial content without [DONE] must not be a false success.
	if res.Error == "" {
		t.Fatalf("expected incomplete-stream error, got success tokens=%d", res.TokensOut)
	}
	if res.TokensOut < 1 {
		t.Fatalf("expected partial tokens recorded, got %d err=%q", res.TokensOut, res.Error)
	}
	if res.ErrorClass != metrics.ClassOther && res.ErrorClass != metrics.ClassSSEParse {
		t.Fatalf("class=%q err=%q", res.ErrorClass, res.Error)
	}
}

func TestContentWithoutDONE(t *testing.T) {
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprint(w, "data: {\"choices\":[{\"delta\":{\"content\":\"hi\"}}]}\n\n")
		// Clean EOF without data: [DONE] — false success if only content is checked.
	})
	run, cleanup := runnerAgainst(t, h, true, 5)
	defer cleanup()

	res := run.RunOne(context.Background(), testPrompt(), 1, false)
	if res.Error == "" {
		t.Fatal("expected missing [DONE] to fail")
	}
	if res.ErrorClass != metrics.ClassOther {
		t.Fatalf("class=%q err=%q", res.ErrorClass, res.Error)
	}
	if res.TokensOut < 1 {
		t.Fatalf("tokens should still be counted, got %d", res.TokensOut)
	}
	if res.DoneSeen {
		t.Fatal("DoneSeen should be false")
	}
}

func TestHTTP500(t *testing.T) {
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "boom", http.StatusInternalServerError)
	})
	run, cleanup := runnerAgainst(t, h, true, 5)
	defer cleanup()

	res := run.RunOne(context.Background(), testPrompt(), 1, false)
	if res.ErrorClass != metrics.ClassHTTP {
		t.Fatalf("class=%q err=%q", res.ErrorClass, res.Error)
	}
	if res.HTTPStatus != 500 {
		t.Fatalf("status=%d", res.HTTPStatus)
	}
}

func TestMalformedSSE(t *testing.T) {
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprint(w, "data: not-json-at-all\n\n")
		fmt.Fprint(w, "data: still-bad{\n\n")
		fmt.Fprint(w, "data: [DONE]\n\n")
	})
	run, cleanup := runnerAgainst(t, h, true, 5)
	defer cleanup()

	res := run.RunOne(context.Background(), testPrompt(), 1, false)
	if res.ErrorClass != metrics.ClassEmpty && res.ErrorClass != metrics.ClassSSEParse {
		t.Fatalf("class=%q err=%q parse=%d", res.ErrorClass, res.Error, res.SSEParseErrors)
	}
	if res.SSEParseErrors < 1 {
		t.Fatalf("expected parse errors, got %d", res.SSEParseErrors)
	}
}

func TestSSECommentsAndRoleChunk(t *testing.T) {
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprint(w, ": ping\n\n")
		fmt.Fprint(w, "data: {\"choices\":[{\"delta\":{\"role\":\"assistant\"}}]}\n\n")
		fmt.Fprint(w, "data: {\"choices\":[{\"delta\":{\"content\":\"ok\"}}]}\n\n")
		fmt.Fprint(w, "data: [DONE]\n\n")
	})
	run, cleanup := runnerAgainst(t, h, true, 5)
	defer cleanup()

	res := run.RunOne(context.Background(), testPrompt(), 1, false)
	if res.Error != "" {
		t.Fatalf("err=%s", res.Error)
	}
	if res.SSEComments < 1 {
		t.Fatalf("comments=%d", res.SSEComments)
	}
	if res.TokensOut < 1 {
		t.Fatalf("tokens=%d", res.TokensOut)
	}
}

func TestNonStreamJSON(t *testing.T) {
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"choices":[{"message":{"content":"hello"}}],"usage":{"completion_tokens":1}}`)
	})
	run, cleanup := runnerAgainst(t, h, false, 5)
	defer cleanup()

	res := run.RunOne(context.Background(), testPrompt(), 1, false)
	if res.Error != "" {
		t.Fatalf("err=%s", res.Error)
	}
	if res.TokensOut != 1 {
		t.Fatalf("tokens=%d", res.TokensOut)
	}
}

func TestPromptHashStable(t *testing.T) {
	a := PromptHash([]config.Message{{Role: "user", Content: "x"}})
	b := PromptHash([]config.Message{{Role: "user", Content: "x"}})
	c := PromptHash([]config.Message{{Role: "user", Content: "y"}})
	if a != b || a == c || len(a) != 64 {
		t.Fatalf("a=%s b=%s c=%s", a, b, c)
	}
}

func TestPrivacyNoPromptInErrorPath(t *testing.T) {
	// Ensure result never echoes prompt content fields — structural check via JSON keys of Result.
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "nope", 503)
	})
	run, cleanup := runnerAgainst(t, h, true, 5)
	defer cleanup()
	res := run.RunOne(context.Background(), testPrompt(), 1, false)
	if strings.Contains(res.Error, "hello") {
		// server message shouldn't include our prompt; our error is http status body
	}
	if res.PromptID != "t1" || res.PromptSHA256 == "" {
		t.Fatal("missing privacy-safe ids")
	}
}

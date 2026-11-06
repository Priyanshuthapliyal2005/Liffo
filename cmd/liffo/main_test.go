package main

import (
	"bytes"
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Priyanshuthapliyal2005/Liffo/internal/adapt"
	"github.com/Priyanshuthapliyal2005/Liffo/internal/route"
)

func TestGatewayConfigAndStreamingSmoke(t *testing.T) {
	worker := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/health":
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w, `{"status":"ok"}`)
		case "/v1/chat/completions":
			w.Header().Set("Content-Type", "text/event-stream")
			_, _ = io.WriteString(w, "data: {\"choices\":[{\"delta\":{\"content\":\"hello\"}}]}\n\ndata: [DONE]\n\n")
		default:
			http.NotFound(w, r)
		}
	}))
	defer worker.Close()

	sc, cleanup, err := buildGatewayConfig(gatewayFile{
		Mode: string(adapt.ModeStatic), RouteMethod: route.MethodLeastInflight,
		MaxInflight: 2, Workers: []workerFile{{URL: worker.URL, Label: "smoke", Capacity: 1}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if cleanup != nil {
		defer cleanup()
	}
	hs, err := adapt.NewHTTPServer(sc)
	if err != nil {
		t.Fatal(err)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	serveDone := make(chan error, 1)
	go func() { serveDone <- hs.Serve(ln) }()

	payload := []byte(`{"model":"local","stream":true,"messages":[{"role":"user","content":"smoke"}]}`)
	resp, err := http.Post("http://"+ln.Addr().String()+"/v1/chat/completions", "application/json", bytes.NewReader(payload))
	if err != nil {
		t.Fatal(err)
	}
	body, readErr := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if readErr != nil {
		t.Fatal(readErr)
	}
	if resp.StatusCode != http.StatusOK || !strings.Contains(string(body), `"hello"`) {
		t.Fatalf("status=%d body=%q", resp.StatusCode, body)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := hs.GracefulShutdown(ctx); err != nil {
		t.Fatal(err)
	}
	if err := <-serveDone; err != nil && err != http.ErrServerClosed {
		t.Fatal(err)
	}
}

package adapt

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestIsSSEDoneLine(t *testing.T) {
	cases := []struct {
		in   string
		want bool
	}{
		{"data: [DONE]\n", true},
		{"data:[DONE]\r\n", true},
		{"data: [done]\n", true},
		{"data: {\"choices\":[]}\n", false},
		{": ping\n", false},
		{"data: almost [DONE]\n", false},
	}
	for _, tc := range cases {
		if got := isSSEDoneLine([]byte(tc.in)); got != tc.want {
			t.Fatalf("isSSEDoneLine(%q)=%v want %v", tc.in, got, tc.want)
		}
	}
}

func TestClassifyStreamSuccess(t *testing.T) {
	type want struct {
		req, worker bool
		code        string
	}
	cases := []struct {
		name   string
		httpOK bool
		stream bool
		ctxErr error
		pump   streamPumpResult
		want   want
	}{
		{
			name:   "ok complete stream",
			httpOK: true, stream: true,
			pump: streamPumpResult{SawDone: true},
			want: want{true, true, "none"},
		},
		{
			name:   "http 200 incomplete no DONE",
			httpOK: true, stream: true,
			pump: streamPumpResult{},
			want: want{false, false, "stream_incomplete"},
		},
		{
			name:   "upstream stream read error",
			httpOK: true, stream: true,
			pump: streamPumpResult{Err: errors.New("read tcp: connection reset by peer"), SawDone: false},
			want: want{false, false, "upstream_stream"},
		},
		{
			name:   "client write disconnect does not poison worker",
			httpOK: true, stream: true,
			pump: streamPumpResult{Err: errors.New("write: broken pipe"), ClientWrite: true, SawDone: false},
			want: want{false, true, "client_write"},
		},
		{
			name:   "canceled context",
			httpOK: true, stream: true,
			ctxErr: context.Canceled,
			pump:   streamPumpResult{SawDone: false},
			want:   want{false, true, "canceled"},
		},
		{
			name:   "upstream non-2xx",
			httpOK: false, stream: true,
			pump: streamPumpResult{},
			want: want{false, false, "upstream_http"},
		},
		{
			name:   "non-stream ok",
			httpOK: true, stream: false,
			pump: streamPumpResult{SawDone: true},
			want: want{true, true, "none"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			reqOK, workerOK, code := classifyStreamSuccess(tc.httpOK, tc.stream, tc.ctxErr, tc.pump)
			if reqOK != tc.want.req || workerOK != tc.want.worker || code != tc.want.code {
				t.Fatalf("got req=%v worker=%v code=%q want %+v", reqOK, workerOK, code, tc.want)
			}
		})
	}
}

func TestPumpUpstreamToClient_DoneAndIncomplete(t *testing.T) {
	// Complete stream
	{
		body := "data: {\"choices\":[{\"delta\":{\"content\":\"hi\"}}]}\n\ndata: [DONE]\n\n"
		rec := httptest.NewRecorder()
		br := bufio.NewReader(strings.NewReader(body))
		pump := pumpUpstreamToClient(rec, br, nil, true)
		if pump.Err != nil {
			t.Fatalf("err=%v", pump.Err)
		}
		if !pump.SawDone {
			t.Fatal("expected SawDone")
		}
		if !strings.Contains(rec.Body.String(), "[DONE]") {
			t.Fatalf("body=%q", rec.Body.String())
		}
	}
	// Incomplete: content only
	{
		body := "data: {\"choices\":[{\"delta\":{\"content\":\"hi\"}}]}\n\n"
		rec := httptest.NewRecorder()
		br := bufio.NewReader(strings.NewReader(body))
		pump := pumpUpstreamToClient(rec, br, nil, true)
		if pump.Err != nil {
			t.Fatalf("err=%v", pump.Err)
		}
		if pump.SawDone {
			t.Fatal("must not SawDone without [DONE]")
		}
	}
	// Prebuf already has DONE
	{
		rec := httptest.NewRecorder()
		pre := [][]byte{[]byte("data: {\"choices\":[{\"delta\":{\"content\":\"x\"}}]}\n"), []byte("data: [DONE]\n")}
		br := bufio.NewReader(bytes.NewReader(nil))
		pump := pumpUpstreamToClient(rec, br, pre, true)
		if !pump.SawDone || pump.Err != nil {
			t.Fatalf("pump=%+v", pump)
		}
	}
}

func TestPumpUpstreamToClient_WriteError(t *testing.T) {
	// ResponseWriter that fails on write.
	w := &failWriter{err: errors.New("broken pipe")}
	br := bufio.NewReader(strings.NewReader("data: {\"choices\":[{\"delta\":{\"content\":\"hi\"}}]}\n\ndata: [DONE]\n\n"))
	pump := pumpUpstreamToClient(w, br, nil, true)
	if pump.Err == nil {
		t.Fatal("expected write error")
	}
	if !pump.ClientWrite {
		t.Fatal("expected ClientWrite")
	}
	if pump.SawDone {
		t.Fatal("should not mark done after write fail")
	}
}

type failWriter struct {
	err error
	n   int
}

func (f *failWriter) Header() http.Header        { return http.Header{} }
func (f *failWriter) WriteHeader(statusCode int) {}
func (f *failWriter) Write(p []byte) (int, error) {
	f.n++
	return 0, f.err
}

// Ensure io.EOF path is covered when body empty after prebuf.
func TestPumpUpstreamToClient_EmptyAfterPrebuf(t *testing.T) {
	rec := httptest.NewRecorder()
	pre := [][]byte{[]byte("data: {\"choices\":[{\"delta\":{\"content\":\"a\"}}]}\n")}
	br := bufio.NewReader(strings.NewReader(""))
	pump := pumpUpstreamToClient(rec, br, pre, true)
	if pump.Err != nil && pump.Err != io.EOF {
		t.Fatalf("err=%v", pump.Err)
	}
	if pump.SawDone {
		t.Fatal("no DONE")
	}
}

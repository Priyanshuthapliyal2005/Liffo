package metrics

import (
	"errors"
	"testing"
)

func TestTokensPerSecond(t *testing.T) {
	// 65 tokens, 1000ms latency, 200ms TTFT => 64 tokens in 800ms => 80 t/s
	got := TokensPerSecond(65, 1000, 200)
	if got < 79 || got > 81 {
		t.Fatalf("got %v", got)
	}
	if TokensPerSecond(0, 1000, 0) != 0 {
		t.Fatal("zero tokens")
	}
	// single token: tokens/latency
	got1 := TokensPerSecond(1, 500, 500)
	if got1 < 1.9 || got1 > 2.1 {
		t.Fatalf("single token tps=%v", got1)
	}
}

func TestFromSamples(t *testing.T) {
	r := FromSamples([]float64{1, 2, 3, 4})
	if r.Min != 1 || r.Max != 4 || r.Avg != 2.5 || r.N != 4 {
		t.Fatalf("%+v", r)
	}
	if FromSamples(nil).N != 0 {
		t.Fatal("empty")
	}
}

func TestClassifyError(t *testing.T) {
	cases := []struct {
		err    error
		status int
		want   string
	}{
		{errors.New("context deadline exceeded"), 0, ClassTimeout},
		{errors.New("i/o timeout"), 0, ClassTimeout},
		{errors.New("context canceled"), 0, ClassCancel},
		{errors.New("connectex: connection refused"), 0, ClassHTTP},
		{errors.New(`Post "http://x": http: server closed idle connection`), 0, ClassHTTP},
		{errors.New("empty stream: no tokens produced"), 200, ClassEmpty},
		{errors.New("stream incomplete: missing [DONE]"), 200, ClassOther},
		{errors.New("stream truncated after visible output"), 200, ClassOther},
		{errors.New("json: parse"), 200, ClassSSEParse},
		{nil, 500, ClassHTTP},
		{nil, 200, ""},
	}
	for _, tc := range cases {
		got := ClassifyError(tc.err, tc.status)
		if got != tc.want {
			t.Fatalf("err=%v status=%d got=%q want=%q", tc.err, tc.status, got, tc.want)
		}
	}
}

func TestSummarize(t *testing.T) {
	rs := []Result{
		{Mode: "gpu", TTFTMs: 100, LatencyMs: 1000, TokensPerSec: 50, TokensOut: 32},
		{Mode: "gpu", TTFTMs: 200, LatencyMs: 2000, TokensPerSec: 40, TokensOut: 32},
		{Mode: "cpu", Error: "fail", ErrorClass: ClassHTTP},
		{Warmup: true, TTFTMs: 1},
	}
	agg := Summarize(rs)
	if agg.Count != 3 {
		t.Fatalf("count=%d", agg.Count)
	}
	if agg.Errors != 1 {
		t.Fatalf("errors=%d", agg.Errors)
	}
	if agg.ByMode["gpu"] == nil || agg.ByMode["gpu"].Count != 2 {
		t.Fatalf("gpu mode %+v", agg.ByMode["gpu"])
	}
	if agg.ByErrorClass[ClassHTTP] != 1 {
		t.Fatalf("error class map %+v", agg.ByErrorClass)
	}
}

func TestPercentilesOrder(t *testing.T) {
	p := ComputePercentiles([]float64{10, 20, 30, 40, 50})
	if p.Min != 10 || p.Max != 50 || p.P50 != 30 {
		t.Fatalf("%+v", p)
	}
}

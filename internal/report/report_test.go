package report

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/Priyanshuthapliyal2005/Liffo/internal/metrics"
)

func TestLoadJSONLAndBuild(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "one.jsonl")
	line := `{"run_id":"r1","ts":"2026-07-18T00:00:00Z","mode":"gpu","prompt_id":"p","prompt_sha256":"abc","stream":true,"iteration":1,"ttft_ms":10,"latency_ms":100,"tokens_out":5,"tokens_per_sec":50}`
	if err := os.WriteFile(p, []byte(line+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	rs, err := LoadJSONL(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(rs) != 1 {
		t.Fatalf("len=%d", len(rs))
	}
	doc := Build(dir, rs, map[string]string{"k": "v"})
	if doc.Aggregate.Count != 1 {
		t.Fatalf("agg=%+v", doc.Aggregate)
	}
	out := filepath.Join(dir, "out")
	if err := WriteMarkdown(filepath.Join(out, "r.md"), doc); err != nil {
		t.Fatal(err)
	}
	if err := WriteJSON(filepath.Join(out, "r.json"), doc); err != nil {
		t.Fatal(err)
	}
	_ = time.Now()
	_ = metrics.Result{}
}

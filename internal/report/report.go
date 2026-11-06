package report

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/Priyanshuthapliyal2005/Liffo/internal/metrics"
)

// LoadJSONL reads all Result lines from a file or directory of .jsonl files.
func LoadJSONL(path string) ([]metrics.Result, error) {
	info, err := os.Stat(path)
	if err != nil {
		return nil, err
	}
	var files []string
	if info.IsDir() {
		entries, err := os.ReadDir(path)
		if err != nil {
			return nil, err
		}
		for _, e := range entries {
			if e.IsDir() {
				continue
			}
			name := e.Name()
			// Only JSONL lines — avoid accidentally loading report .json objects.
			if strings.HasSuffix(name, ".jsonl") {
				files = append(files, filepath.Join(path, name))
			}
		}
		sort.Strings(files)
	} else {
		files = []string{path}
	}
	var out []metrics.Result
	for _, f := range files {
		rs, err := readFile(f)
		if err != nil {
			return out, fmt.Errorf("%s: %w", f, err)
		}
		out = append(out, rs...)
	}
	return out, nil
}

func readFile(path string) ([]metrics.Result, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	var out []metrics.Result
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 64*1024), 4*1024*1024)
	lineNo := 0
	for sc.Scan() {
		lineNo++
		line := strings.TrimSpace(sc.Text())
		if line == "" {
			continue
		}
		var r metrics.Result
		if err := json.Unmarshal([]byte(line), &r); err != nil {
			return out, fmt.Errorf("line %d: %w", lineNo, err)
		}
		out = append(out, r)
	}
	return out, sc.Err()
}

// Document is the full report payload.
type Document struct {
	GeneratedAt string            `json:"generated_at"`
	Source      string            `json:"source"`
	TotalRows   int               `json:"total_rows"`
	Aggregate   metrics.Aggregate `json:"aggregate"`
	Hardware    map[string]string `json:"hardware,omitempty"`
	Notes       []string          `json:"notes,omitempty"`
}

// Build creates a Document from results.
func Build(source string, results []metrics.Result, hardware map[string]string) Document {
	notes := []string{
		"Raw results omit prompt and completion text (privacy-safe).",
		"TTFT is time to first non-empty content delta in the SSE stream.",
		"tokens_per_sec uses decode-window (latency - ttft) when tokens > 1.",
		"gpu_* metrics from nvidia-smi sampling when available.",
	}
	return Document{
		GeneratedAt: time.Now().UTC().Format(time.RFC3339),
		Source:      source,
		TotalRows:   len(results),
		Aggregate:   metrics.Summarize(results),
		Hardware:    hardware,
		Notes:       notes,
	}
}

// WriteJSON writes report JSON.
func WriteJSON(path string, doc Document) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	b, err := json.MarshalIndent(doc, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, b, 0o644)
}

// WriteMarkdown writes a human-readable report.
func WriteMarkdown(path string, doc Document) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	var b strings.Builder
	b.WriteString("# Liffo Inference Benchmark Report\n\n")
	b.WriteString(fmt.Sprintf("- **Generated:** %s\n", doc.GeneratedAt))
	b.WriteString(fmt.Sprintf("- **Source:** `%s`\n", doc.Source))
	b.WriteString(fmt.Sprintf("- **Total rows:** %d (including warmups if present)\n", doc.TotalRows))
	b.WriteString(fmt.Sprintf("- **Measured (non-warmup):** %d\n", doc.Aggregate.Count))
	b.WriteString(fmt.Sprintf("- **Errors:** %d (%.1f%%)\n\n", doc.Aggregate.Errors, doc.Aggregate.ErrorRate*100))

	if len(doc.Hardware) > 0 {
		b.WriteString("## Hardware\n\n| Key | Value |\n|-----|-------|\n")
		keys := make([]string, 0, len(doc.Hardware))
		for k := range doc.Hardware {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			b.WriteString(fmt.Sprintf("| %s | %s |\n", k, doc.Hardware[k]))
		}
		b.WriteString("\n")
	}

	b.WriteString("## Overall (successful requests)\n\n")
	writePctTable(&b, "TTFT (ms)", doc.Aggregate.TTFTMs)
	writePctTable(&b, "Latency (ms)", doc.Aggregate.LatencyMs)
	writePctTable(&b, "Tokens/sec", doc.Aggregate.TokensPerSec)
	b.WriteString(fmt.Sprintf("\n- Avg tokens out: **%.1f**\n", doc.Aggregate.TokensOutAvg))
	b.WriteString(fmt.Sprintf("- SSE parse errors (total): **%d**\n\n", doc.Aggregate.SSEParseErrors))

	if len(doc.Aggregate.ByMode) > 0 {
		b.WriteString("## By mode (CPU vs GPU)\n\n")
		modes := make([]string, 0, len(doc.Aggregate.ByMode))
		for m := range doc.Aggregate.ByMode {
			modes = append(modes, m)
		}
		sort.Strings(modes)
		b.WriteString("| Mode | N | Errors | TTFT p50 | Latency p50 | tok/s p50 | tok/s mean | RAM max | VRAM max | GPU util max |\n")
		b.WriteString("|------|---|--------|----------|-------------|-----------|------------|---------|----------|-------------|\n")
		for _, m := range modes {
			a := doc.Aggregate.ByMode[m]
			b.WriteString(fmt.Sprintf("| %s | %d | %d | %.1f | %.1f | %.1f | %.1f | %.0f | %.0f | %.0f |\n",
				m, a.Count, a.Errors, a.TTFTMs.P50, a.LatencyMs.P50, a.TokensPerSec.P50, a.TokensPerSec.Mean,
				a.RAMMb.Max, a.GPUMemMb.Max, a.GPUUtilPct.Max))
		}
		b.WriteString("\n")
		for _, m := range modes {
			a := doc.Aggregate.ByMode[m]
			b.WriteString(fmt.Sprintf("### Mode `%s`\n\n", m))
			writePctTable(&b, "TTFT (ms)", a.TTFTMs)
			writePctTable(&b, "Latency (ms)", a.LatencyMs)
			writePctTable(&b, "Tokens/sec", a.TokensPerSec)
			b.WriteString("\n")
		}
	}

	if len(doc.Aggregate.ByConcurrency) > 0 {
		b.WriteString("## By concurrency\n\n")
		concs := make([]int, 0, len(doc.Aggregate.ByConcurrency))
		for c := range doc.Aggregate.ByConcurrency {
			concs = append(concs, c)
		}
		sort.Ints(concs)
		b.WriteString("| Conc | N | Errors | Err% | TTFT p50 | Latency p50 | tok/s p50 | RAM max | VRAM max | GPU util max |\n")
		b.WriteString("|------|---|--------|------|----------|-------------|-----------|---------|----------|-------------|\n")
		for _, c := range concs {
			a := doc.Aggregate.ByConcurrency[c]
			b.WriteString(fmt.Sprintf("| %d | %d | %d | %.1f | %.1f | %.1f | %.1f | %.0f | %.0f | %.0f |\n",
				c, a.Count, a.Errors, a.ErrorRate*100, a.TTFTMs.P50, a.LatencyMs.P50, a.TokensPerSec.P50,
				a.RAMMb.Max, a.GPUMemMb.Max, a.GPUUtilPct.Max))
		}
		b.WriteString("\n")
	}

	if len(doc.Aggregate.ByErrorClass) > 0 {
		b.WriteString("## Errors by class\n\n| Class | Count |\n|-------|-------|\n")
		for k, v := range doc.Aggregate.ByErrorClass {
			b.WriteString(fmt.Sprintf("| %s | %d |\n", k, v))
		}
		b.WriteString("\n")
	}

	b.WriteString("## Notes\n\n")
	for _, n := range doc.Notes {
		b.WriteString(fmt.Sprintf("- %s\n", n))
	}
	b.WriteString("\n## Privacy\n\nRaw JSONL stores `prompt_id` + `prompt_sha256` only - no prompt or completion text.\n")

	return os.WriteFile(path, []byte(b.String()), 0o644)
}

func writePctTable(b *strings.Builder, title string, p metrics.Percentiles) {
	if p.N == 0 {
		b.WriteString(fmt.Sprintf("### %s\n\n_no successful samples_\n\n", title))
		return
	}
	b.WriteString(fmt.Sprintf("### %s\n\n", title))
	b.WriteString("| min | p50 | p90 | p99 | mean | max | n |\n")
	b.WriteString("|-----|-----|-----|-----|------|-----|---|\n")
	b.WriteString(fmt.Sprintf("| %.2f | %.2f | %.2f | %.2f | %.2f | %.2f | %d |\n\n",
		p.Min, p.P50, p.P90, p.P99, p.Mean, p.Max, p.N))
}

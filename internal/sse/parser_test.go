package sse

import (
	"io"
	"strings"
	"testing"
)

func TestParseBasicTokensAndDone(t *testing.T) {
	body := strings.Join([]string{
		`data: {"choices":[{"delta":{"content":"Hello"}}]}`,
		"",
		`data: {"choices":[{"delta":{"content":" world"}}]}`,
		"",
		"data: [DONE]",
		"",
	}, "\n")
	var contents []string
	var done bool
	st, err := ParseStream(strings.NewReader(body), nil, func(td TokenDelta) {
		if td.Done {
			done = true
			return
		}
		if td.Content != "" {
			contents = append(contents, td.Content)
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	if !done || !st.DoneSeen {
		t.Fatalf("expected done, st=%+v", st)
	}
	if got := strings.Join(contents, ""); got != "Hello world" {
		t.Fatalf("content=%q", got)
	}
	if st.Events != 3 {
		t.Fatalf("events=%d", st.Events)
	}
}

func TestCommentsAndMultiLineData(t *testing.T) {
	body := strings.Join([]string{
		": keep-alive",
		`data: {"choices":[{"delta":{"content":"A"}}]}`,
		"",
		// multi-line data field reassembly (joined with \n) — still valid JSON
		`data: {"choices":[{"delta":{"content":`,
		`data: "B"}}]}`,
		"",
		"data: [DONE]",
		"",
	}, "\n")
	var n int
	st, err := ParseStream(strings.NewReader(body), nil, func(td TokenDelta) {
		if td.Content != "" {
			n++
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	if st.Comments != 1 {
		t.Fatalf("comments=%d", st.Comments)
	}
	if n < 1 {
		t.Fatalf("tokens=%d parseErr=%d", n, st.ParseErrors)
	}
}

func TestEmptyChoicesNoPanic(t *testing.T) {
	body := "data: {\"choices\":[]}\n\ndata: [DONE]\n\n"
	st, err := ParseStream(strings.NewReader(body), nil, func(td TokenDelta) {})
	if err != nil {
		t.Fatal(err)
	}
	if st.ParseErrors != 0 {
		t.Fatalf("parse errors=%d", st.ParseErrors)
	}
}

func TestMalformedJSONCounted(t *testing.T) {
	body := "data: {not-json\n\ndata: [DONE]\n\n"
	st, err := ParseStream(strings.NewReader(body), nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if st.ParseErrors != 1 {
		t.Fatalf("parse errors=%d", st.ParseErrors)
	}
	if !st.DoneSeen {
		t.Fatal("expected DONE")
	}
}

func TestCRLF(t *testing.T) {
	body := "data: {\"choices\":[{\"delta\":{\"content\":\"X\"}}]}\r\n\r\ndata: [DONE]\r\n\r\n"
	var c string
	_, err := ParseStream(strings.NewReader(body), nil, func(td TokenDelta) {
		if td.Content != "" {
			c = td.Content
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	if c != "X" {
		t.Fatalf("got %q", c)
	}
}

func TestNullDeltaContent(t *testing.T) {
	body := "data: {\"choices\":[{\"delta\":{\"content\":null,\"role\":\"assistant\"}}]}\n\n" +
		"data: {\"choices\":[{\"delta\":{\"content\":\"hi\"}}]}\n\n" +
		"data: [DONE]\n\n"
	var parts []string
	_, err := ParseStream(strings.NewReader(body), nil, func(td TokenDelta) {
		if td.Content != "" {
			parts = append(parts, td.Content)
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	if strings.Join(parts, "") != "hi" {
		t.Fatalf("parts=%v", parts)
	}
}

func TestUsageOnlyChunk(t *testing.T) {
	body := "data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n" +
		"data: {\"choices\":[],\"usage\":{\"completion_tokens\":12}}\n\n" +
		"data: [DONE]\n\n"
	var usage int
	st, err := ParseStream(strings.NewReader(body), nil, func(td TokenDelta) {
		if td.UsageTokens > 0 {
			usage = td.UsageTokens
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	if usage != 12 {
		t.Fatalf("usage=%d", usage)
	}
	if !st.DoneSeen {
		t.Fatal("done")
	}
}

func TestBOMAndNoTrailingNewline(t *testing.T) {
	// BOM + final event without blank line before EOF
	body := "\xef\xbb\xbfdata: {\"choices\":[{\"delta\":{\"content\":\"Z\"}}]}\n\ndata: [DONE]"
	var c string
	var done bool
	st, err := ParseStream(strings.NewReader(body), nil, func(td TokenDelta) {
		if td.Done {
			done = true
		}
		if td.Content != "" {
			c = td.Content
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	if c != "Z" || !done || !st.DoneSeen {
		t.Fatalf("c=%q done=%v st=%+v", c, done, st)
	}
}

func TestBrokenMidJSON(t *testing.T) {
	// Incomplete event then EOF — parse error counted, no fatal unless reader errors
	body := "data: {\"choices\":[{\"delta\":{\"content\":\"partial\n"
	st, err := ParseStream(strings.NewReader(body), nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if st.ParseErrors < 1 {
		t.Fatalf("expected parse error, st=%+v", st)
	}
}

type errReader struct {
	data string
	n    int
	err  error
}

func (e *errReader) Read(p []byte) (int, error) {
	if e.n >= len(e.data) {
		return 0, e.err
	}
	n := copy(p, e.data[e.n:])
	e.n += n
	if e.n >= len(e.data) {
		return n, e.err
	}
	return n, nil
}

func TestReaderErrorPropagates(t *testing.T) {
	r := &errReader{data: "data: {\"choices\":[]}\n\n", err: io.ErrUnexpectedEOF}
	// After consuming data, next Read returns unexpected EOF
	// Fill so first events process then error
	r2 := io.MultiReader(strings.NewReader("data: {\"choices\":[{\"delta\":{\"content\":\"a\"}}]}\n\n"),
		&errAfter{err: io.ErrUnexpectedEOF})
	_, err := ParseStream(r2, nil, nil)
	if err == nil {
		t.Fatal("expected error")
	}
	_ = r
}

type errAfter struct{ err error }

func (e *errAfter) Read(p []byte) (int, error) {
	return 0, e.err
}

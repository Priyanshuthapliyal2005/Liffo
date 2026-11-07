package sse

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"strings"
)

// Event is one logical SSE event after multi-line data reassembly.
type Event struct {
	Event string
	Data  string
	ID    string
	Retry string
}

// TokenDelta is a minimal OpenAI-style stream chunk extract.
type TokenDelta struct {
	Content      string
	FinishReason string
	RawJSON      bool
	Done         bool
	UsageTokens  int // completion tokens if present in usage
}

// Stats captures parser bookkeeping for a single stream.
type Stats struct {
	Events      int
	ParseErrors int
	Comments    int
	DoneSeen    bool
	// BytesRead is approximate payload size consumed from the reader.
	BytesRead int64
}

// ParseStream reads an SSE body, invokes onEvent for each logical event,
// and onToken for each successfully decoded content delta.
//
// Edge cases handled:
//   - comment / heartbeat lines starting with ':'
//   - multi-line data: fields (joined with '\n' per SSE spec)
//   - CRLF and LF line endings
//   - data: [DONE]
//   - empty choices / null delta.content
//   - truncated JSON (counted as parse error, stream continues)
//   - final event without trailing blank line (flushed on EOF)
//   - BOM at start of stream
func ParseStream(r io.Reader, onEvent func(Event), onToken func(TokenDelta)) (Stats, error) {
	var st Stats
	br := bufio.NewReaderSize(r, 64*1024)

	// Drop UTF-8 BOM if present
	if b, err := br.Peek(3); err == nil && len(b) == 3 && b[0] == 0xEF && b[1] == 0xBB && b[2] == 0xBF {
		_, _ = br.Discard(3)
		st.BytesRead += 3
	}

	var (
		eventName strings.Builder
		dataLines []string
		id        string
		retry     string
	)

	flush := func() {
		if len(dataLines) == 0 && eventName.Len() == 0 && id == "" && retry == "" {
			return
		}
		data := strings.Join(dataLines, "\n")
		ev := Event{
			Event: eventName.String(),
			Data:  data,
			ID:    id,
			Retry: retry,
		}
		st.Events++
		if onEvent != nil {
			onEvent(ev)
		}

		eventName.Reset()
		dataLines = nil
		id = ""
		retry = ""

		trimmed := strings.TrimSpace(data)
		if trimmed == "" {
			return
		}
		// Some servers send "data: [DONE]" with extra whitespace.
		if trimmed == "[DONE]" || strings.EqualFold(trimmed, "[DONE]") {
			st.DoneSeen = true
			if onToken != nil {
				onToken(TokenDelta{Done: true})
			}
			return
		}

		td, err := decodeTokenDelta(trimmed)
		if err != nil {
			st.ParseErrors++
			return // non-fatal: keep streaming
		}
		if onToken != nil {
			onToken(td)
		}
	}

	for {
		line, err := br.ReadBytes('\n')
		st.BytesRead += int64(len(line))

		if len(line) > 0 {
			// Strip CR for CRLF; also handle bare CR (rare)
			line = bytes.TrimRight(line, "\r\n")
			s := string(line)

			switch {
			case s == "":
				flush()
			case strings.HasPrefix(s, ":"):
				// comment / heartbeat (may be ": " or ":keep-alive")
				st.Comments++
			case strings.HasPrefix(s, "data:"):
				payload := strings.TrimPrefix(s, "data:")
				// Single optional leading space after colon (SSE spec).
				if strings.HasPrefix(payload, " ") {
					payload = payload[1:]
				}
				dataLines = append(dataLines, payload)
			case strings.HasPrefix(s, "event:"):
				eventName.Reset()
				eventName.WriteString(strings.TrimSpace(strings.TrimPrefix(s, "event:")))
			case strings.HasPrefix(s, "id:"):
				id = strings.TrimSpace(strings.TrimPrefix(s, "id:"))
			case strings.HasPrefix(s, "retry:"):
				retry = strings.TrimSpace(strings.TrimPrefix(s, "retry:"))
			default:
				// Unknown field — ignore per SSE flexibility.
			}
		}

		if err == io.EOF {
			flush()
			return st, nil
		}
		if err != nil {
			flush()
			return st, err
		}
	}
}

type openAIChunk struct {
	Choices []struct {
		Delta struct {
			// pointer so explicit null is distinct from missing
			Content *string `json:"content"`
			Role    string  `json:"role"`
		} `json:"delta"`
		Message struct {
			Content *string `json:"content"`
		} `json:"message"`
		Text         string  `json:"text"`
		FinishReason *string `json:"finish_reason"`
	} `json:"choices"`
	Usage *struct {
		CompletionTokens int `json:"completion_tokens"`
		TotalTokens      int `json:"total_tokens"`
	} `json:"usage"`
}

func decodeTokenDelta(data string) (TokenDelta, error) {
	var chunk openAIChunk
	if err := json.Unmarshal([]byte(data), &chunk); err != nil {
		return TokenDelta{}, fmt.Errorf("json: %w", err)
	}
	td := TokenDelta{RawJSON: true}
	if chunk.Usage != nil && chunk.Usage.CompletionTokens > 0 {
		td.UsageTokens = chunk.Usage.CompletionTokens
	}
	if len(chunk.Choices) == 0 {
		return td, nil
	}
	c0 := chunk.Choices[0]
	if c0.FinishReason != nil {
		td.FinishReason = *c0.FinishReason
	}
	// Prefer delta.content; tolerate null / missing.
	if c0.Delta.Content != nil {
		td.Content = *c0.Delta.Content
	} else if c0.Message.Content != nil {
		td.Content = *c0.Message.Content
	} else if c0.Text != "" {
		td.Content = c0.Text
	}
	return td, nil
}

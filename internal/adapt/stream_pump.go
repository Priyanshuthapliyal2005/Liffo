package adapt

import (
	"bufio"
	"bytes"
	"io"
	"net/http"
	"strings"
)

// streamPumpResult is the outcome of copying an upstream body to the client.
// Gateway success must not be inferred from HTTP status alone.
type streamPumpResult struct {
	// SawDone is true when a data: [DONE] line was written (SSE only).
	SawDone bool
	// BytesWritten counts bytes successfully written to the client.
	BytesWritten int64
	// Err is the first fatal pump error (client write or unexpected upstream read).
	// Clean io.EOF from upstream is not an error.
	Err error
	// ClientWrite is true when Err came from writing to the client (disconnect).
	ClientWrite bool
}

// isSSEDoneLine reports whether a raw SSE line is the terminal [DONE] event.
func isSSEDoneLine(line []byte) bool {
	// Accept "data: [DONE]" / "data:[DONE]" with optional CR.
	s := string(bytes.TrimSpace(line))
	if !strings.HasPrefix(s, "data:") {
		return false
	}
	payload := strings.TrimSpace(strings.TrimPrefix(s, "data:"))
	return payload == "[DONE]" || strings.EqualFold(payload, "[DONE]")
}

// isClientWriteErr heuristically detects client disconnect / write-side failures.
func isClientWriteErr(err error) bool {
	if err == nil {
		return false
	}
	s := strings.ToLower(err.Error())
	return strings.Contains(s, "broken pipe") ||
		strings.Contains(s, "connection reset") ||
		strings.Contains(s, "wsasend") ||
		strings.Contains(s, "forcibly closed") ||
		strings.Contains(s, "use of closed network connection") ||
		strings.Contains(s, "connection was aborted") ||
		strings.Contains(s, "client disconnected")
}

// pumpUpstreamToClient copies prebuf + remaining upstream body to the client.
// For stream=true it tracks data: [DONE] and does not treat clean EOF as success by itself.
func pumpUpstreamToClient(w http.ResponseWriter, br *bufio.Reader, prebuf [][]byte, stream bool) streamPumpResult {
	var out streamPumpResult
	flusher, _ := w.(http.Flusher)

	writeLine := func(line []byte) error {
		if len(line) == 0 {
			return nil
		}
		n, err := w.Write(line)
		out.BytesWritten += int64(n)
		if err != nil {
			return err
		}
		if flusher != nil {
			flusher.Flush()
		}
		if stream && isSSEDoneLine(line) {
			out.SawDone = true
		}
		return nil
	}

	for _, line := range prebuf {
		if err := writeLine(line); err != nil {
			out.Err = err
			out.ClientWrite = true
			return out
		}
	}

	if !stream {
		n, err := io.Copy(w, br)
		out.BytesWritten += n
		if err != nil {
			out.Err = err
			if isClientWriteErr(err) {
				out.ClientWrite = true
			}
		}
		// Non-stream: complete copy with no error is success (SawDone unused).
		out.SawDone = err == nil
		return out
	}

	for {
		line, err := br.ReadBytes('\n')
		if len(line) > 0 {
			if werr := writeLine(line); werr != nil {
				out.Err = werr
				out.ClientWrite = true
				return out
			}
		}
		if err != nil {
			if err == io.EOF {
				return out
			}
			out.Err = err
			return out
		}
	}
}

// classifyStreamSuccess decides request vs worker success for adaptive control.
//
// Request success (obs / sample): delivery complete — no pump error, and for SSE [DONE] seen.
// Worker success (Release): do not mark worker failed for client disconnects; incomplete
// upstream (EOF without [DONE]) and upstream read errors do mark fail.
func classifyStreamSuccess(httpOK bool, stream bool, ctxErr error, pump streamPumpResult) (reqOK, workerOK bool, errCode string) {
	if !httpOK {
		return false, false, "upstream_http"
	}
	if ctxErr != nil {
		return false, true, "canceled" // cancel is client/deadline; worker may still be fine
	}
	if pump.Err != nil {
		// Only ClientWrite is trusted for disconnect (do not re-scan err text:
		// "connection reset" also appears on upstream reads).
		if pump.ClientWrite {
			// Client gone mid-stream: request failed, do not poison worker.
			return false, true, "client_write"
		}
		return false, false, "upstream_stream"
	}
	if stream && !pump.SawDone {
		return false, false, "stream_incomplete"
	}
	return true, true, "none"
}

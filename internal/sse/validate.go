package sse

import "fmt"

// RequireComplete enforces that a streamed chat completion is not a false success.
//
// A stream is successful only when:
//  1. Valid content was produced (tokensOut > 0 and/or completionChars > 0), and
//  2. The stream completed with a terminal data: [DONE] event (DoneSeen).
//
// HTTP 200 + SSE comments/role chunks without content, or content without [DONE]
// (truncated body), must not be treated as OK.
func RequireComplete(st Stats, tokensOut, completionChars int) error {
	if tokensOut <= 0 && completionChars <= 0 {
		if st.Events == 0 {
			return fmt.Errorf("empty response: no SSE events")
		}
		return fmt.Errorf("empty stream: no tokens produced")
	}
	if !st.DoneSeen {
		return fmt.Errorf("stream incomplete: missing [DONE]")
	}
	return nil
}

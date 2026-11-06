package sse

import "testing"

func TestRequireComplete(t *testing.T) {
	cases := []struct {
		name    string
		st      Stats
		tokens  int
		chars   int
		wantErr string
	}{
		{
			name:    "ok content and done",
			st:      Stats{Events: 3, DoneSeen: true},
			tokens:  2,
			chars:   4,
			wantErr: "",
		},
		{
			name:    "empty no events",
			st:      Stats{},
			wantErr: "empty response: no SSE events",
		},
		{
			name:    "events but no content",
			st:      Stats{Events: 2, DoneSeen: true},
			wantErr: "empty stream: no tokens produced",
		},
		{
			name:    "content without DONE",
			st:      Stats{Events: 2, DoneSeen: false},
			tokens:  1,
			chars:   2,
			wantErr: "stream incomplete: missing [DONE]",
		},
		{
			name:    "usage tokens without DONE",
			st:      Stats{Events: 1},
			tokens:  5,
			wantErr: "stream incomplete: missing [DONE]",
		},
		{
			name:    "chars only with DONE",
			st:      Stats{Events: 1, DoneSeen: true},
			chars:   3,
			wantErr: "",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := RequireComplete(tc.st, tc.tokens, tc.chars)
			if tc.wantErr == "" {
				if err != nil {
					t.Fatalf("unexpected err: %v", err)
				}
				return
			}
			if err == nil || err.Error() != tc.wantErr {
				t.Fatalf("err=%v want %q", err, tc.wantErr)
			}
		})
	}
}

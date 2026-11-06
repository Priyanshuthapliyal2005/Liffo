package sysmon

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"runtime"
	"testing"
	"time"
)

func TestParseCSVLine(t *testing.T) {
	cases := []struct {
		input string
		want  []string
	}{
		{`"a","b","c"`, []string{"a", "b", "c"}},
		{`a,b,c`, []string{"a", "b", "c"}},
		{`"hello, world",test,"12,345 K"`, []string{"hello, world", "test", "12,345 K"}},
		{``, nil},
	}

	for _, tc := range cases {
		got := parseCSVLine(tc.input)
		if len(got) != len(tc.want) {
			t.Fatalf("parseCSVLine(%q) len = %d, want %d (%v vs %v)", tc.input, len(got), len(tc.want), got, tc.want)
		}
		for i := range got {
			if got[i] != tc.want[i] {
				t.Errorf("parseCSVLine(%q)[%d] = %q, want %q", tc.input, i, got[i], tc.want[i])
			}
		}
	}
}

func TestReadRAMMb(t *testing.T) {
	val := readRAMMb(0)
	if val <= 0 {
		t.Errorf("readRAMMb(0) expected > 0, got %f", val)
	}

	valPid := readRAMMb(os.Getpid())
	if valPid <= 0 {
		t.Errorf("readRAMMb(pid) expected > 0, got %f", valPid)
	}

	if missing := readRAMMb(2147483647); missing != 0 {
		t.Errorf("readRAMMb(missing pid) expected 0, got %f", missing)
	}
}

func TestReadLinuxRSSMb(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("skipping linux-specific RSS test on non-linux OS")
	}
	rss := readLinuxRSSMb(0)
	if rss <= 0 {
		t.Errorf("readLinuxRSSMb(0) = %f, expected > 0", rss)
	}
	rssSelf := readLinuxRSSMb(os.Getpid())
	if rssSelf <= 0 {
		t.Errorf("readLinuxRSSMb(self) = %f, expected > 0", rssSelf)
	}
	if missing := readLinuxRSSMb(2147483647); missing != 0 {
		t.Errorf("readLinuxRSSMb(missing pid) = %f, expected 0", missing)
	}
}

func TestMonitorLifecycle(t *testing.T) {
	mon := New(50*time.Millisecond, 0)
	if mon == nil {
		t.Fatal("New returned nil")
	}

	mon.Start()
	time.Sleep(120 * time.Millisecond)

	ram, gpuMem, gpuUtil, n := mon.Snapshot()
	if n == 0 {
		t.Error("expected at least 1 sample from running monitor")
	}
	if ram.Min <= 0 && ram.Max <= 0 {
		t.Errorf("expected ram stats > 0, got min=%f max=%f", ram.Min, ram.Max)
	}
	_ = gpuMem
	_ = gpuUtil

	// Test Reset
	mon.Reset()
	_, _, _, nAfterReset := mon.Snapshot()
	if nAfterReset != 0 {
		t.Errorf("expected 0 samples after Reset(), got %d", nAfterReset)
	}

	// Test double Start (should handle gracefully)
	mon.Start()
	time.Sleep(60 * time.Millisecond)

	mon.Stop()
	// Test double Stop
	mon.Stop()
}

func TestNvidiaAvailable(t *testing.T) {
	_ = nvidiaAvailable()
}

func TestReadRAMMbFromRemoteHealthURL(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"status":"ok","pid":4242,"rss_mb":512.75}`)
	}))
	defer srv.Close()

	t.Setenv("LIFFO_SYSMON_HEALTH_URL", srv.URL)
	got := readRAMMb(0)
	if got != 512.75 {
		t.Fatalf("readRAMMb(0) with LIFFO_SYSMON_HEALTH_URL = %v, want 512.75", got)
	}
}

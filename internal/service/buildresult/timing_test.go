package buildresult

import (
	"testing"
	"time"
)

func TestTargetExecutionTimeUsesReceiptTimestamps(t *testing.T) {
	got, ok := targetExecutionTime("2026-09-28T01:00:00.125Z", "2026-09-28T01:00:03.875Z")
	if !ok || got != 3750*time.Millisecond {
		t.Fatalf("duration=%s ok=%t", got, ok)
	}
	for _, pair := range [][2]string{{"", ""}, {"not-time", "2026-09-28T01:00:03Z"}, {"2026-09-28T01:00:03Z", "2026-09-28T01:00:00Z"}} {
		if duration, ok := targetExecutionTime(pair[0], pair[1]); ok {
			t.Fatalf("invalid receipt fabricated duration=%s", duration)
		}
	}
}

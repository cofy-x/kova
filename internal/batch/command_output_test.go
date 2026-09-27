package batch

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
)

func TestBoundedTailBufferRetainsFinalDiagnostics(t *testing.T) {
	var output boundedTailBuffer
	if n, err := output.Write(bytes.Repeat([]byte("x"), maxBuildOutputBytes+8192)); err != nil || n != maxBuildOutputBytes+8192 {
		t.Fatalf("write = %d, %v", n, err)
	}
	if _, err := output.Write([]byte("fatal: registry denied push\n")); err != nil {
		t.Fatal(err)
	}
	got := output.String()
	if !strings.HasPrefix(got, truncatedBuildOutputMarker) || !strings.HasSuffix(got, "fatal: registry denied push\n") {
		t.Fatalf("bounded output lost truncation marker or exit diagnostic: prefix=%q suffix=%q", got[:min(len(got), 64)], got[max(0, len(got)-64):])
	}
	if len(got) != len(truncatedBuildOutputMarker)+maxBuildOutputBytes {
		t.Fatalf("bounded output size = %d", len(got))
	}
}

func TestBoundedTailBufferConcurrentWriters(t *testing.T) {
	var output boundedTailBuffer
	var workers sync.WaitGroup
	for i := 0; i < 8; i++ {
		workers.Add(1)
		go func(i int) {
			defer workers.Done()
			chunk := bytes.Repeat([]byte{byte('a' + i)}, 4096)
			for j := 0; j < 128; j++ {
				_, _ = output.Write(chunk)
				_ = output.String()
			}
		}(i)
	}
	workers.Wait()
	if got := output.String(); len(got) != len(truncatedBuildOutputMarker)+maxBuildOutputBytes {
		t.Fatalf("concurrent output size = %d", len(got))
	}
}

func TestBoundedTailBufferRetainsOOMHintAfterTruncation(t *testing.T) {
	var output boundedTailBuffer
	_, _ = output.Write([]byte("transient Connec"))
	_, _ = output.Write([]byte("tion Refused while contacting BuildKit\n"))
	_, _ = output.Write(bytes.Repeat([]byte("z"), maxBuildOutputBytes+1))
	if !output.ContainsConnectionRefused() {
		t.Fatal("streamed OOM-style hint was lost after the output tail was truncated")
	}
	if strings.Contains(strings.ToLower(output.String()), connectionRefusedHint) {
		t.Fatal("test did not exercise a hint outside the retained tail")
	}
}

func TestRunCommandBoundsNoisyFailureAndKeepsExitDiagnostic(t *testing.T) {
	if os.Getenv("KOVA_TEST_NOISY_COMMAND") == "1" {
		_, _ = os.Stdout.Write(bytes.Repeat([]byte("n"), maxBuildOutputBytes*2))
		_, _ = fmt.Fprintln(os.Stderr, "fatal: build command rejected the source")
		os.Exit(17)
	}
	t.Setenv("KOVA_TEST_NOISY_COMMAND", "1")
	var output boundedTailBuffer
	if err := runCommand(context.Background(), false, &output, os.Args[0], "-test.run=^TestRunCommandBoundsNoisyFailureAndKeepsExitDiagnostic$"); err == nil {
		t.Fatal("expected command exit failure")
	}
	got := output.String()
	if !strings.HasPrefix(got, truncatedBuildOutputMarker) || !strings.HasSuffix(got, "fatal: build command rejected the source\n") || len(got) > len(truncatedBuildOutputMarker)+maxBuildOutputBytes {
		t.Fatalf("noisy failure output lost bounded tail: prefix=%q suffix=%q len=%d", got[:min(len(got), 64)], got[max(0, len(got)-64):], len(got))
	}
}

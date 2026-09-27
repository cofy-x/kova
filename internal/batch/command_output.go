package batch

import (
	"io"
	"sync"
)

const maxBuildOutputBytes = 1 << 20
const truncatedBuildOutputMarker = "...[earlier command output truncated]\n"
const connectionRefusedHint = "connection refused"

// boundedTailBuffer keeps the latest command output needed for failure
// diagnostics without allowing noisy concurrent build commands to exhaust the
// runner's memory. stdout and stderr may write from separate exec goroutines.
type boundedTailBuffer struct {
	mu                   sync.Mutex
	data                 []byte
	truncated            bool
	sawConnectionRefused bool
}

var _ io.Writer = (*boundedTailBuffer)(nil)

func (b *boundedTailBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	written := len(p)
	if !b.sawConnectionRefused {
		var boundary [2 * len(connectionRefusedHint)]byte
		previous := min(len(b.data), len(connectionRefusedHint)-1)
		incoming := min(len(p), len(connectionRefusedHint)-1)
		copy(boundary[:previous], b.data[len(b.data)-previous:])
		copy(boundary[previous:previous+incoming], p[:incoming])
		b.sawConnectionRefused = containsASCIIFold(boundary[:previous+incoming], connectionRefusedHint) || containsASCIIFold(p, connectionRefusedHint)
	}
	if written >= maxBuildOutputBytes {
		b.data = append(b.data[:0], p[written-maxBuildOutputBytes:]...)
		b.truncated = true
		return written, nil
	}
	if discard := len(b.data) + written - maxBuildOutputBytes; discard > 0 {
		copy(b.data, b.data[discard:])
		b.data = b.data[:len(b.data)-discard]
		b.truncated = true
	}
	b.data = append(b.data, p...)
	return written, nil
}

func (b *boundedTailBuffer) ContainsConnectionRefused() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.sawConnectionRefused
}

func containsASCIIFold(data []byte, pattern string) bool {
	for start := 0; start+len(pattern) <= len(data); start++ {
		matched := true
		for offset := range pattern {
			value := data[start+offset]
			if value >= 'A' && value <= 'Z' {
				value += 'a' - 'A'
			}
			if value != pattern[offset] {
				matched = false
				break
			}
		}
		if matched {
			return true
		}
	}
	return false
}

func (b *boundedTailBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.truncated {
		return truncatedBuildOutputMarker + string(b.data)
	}
	return string(b.data)
}

package logging

import (
	"context"
	"fmt"
	"math"
	"os"
	"sync/atomic"
	"time"

	"github.com/cofy-x/kova/internal/observability"
)

var commandStartUnixNano atomic.Int64

var logProgressState struct {
	current atomic.Int64
	total   atomic.Int64
}

func ResetProgress(total int) {
	logProgressState.current.Store(0)
	logProgressState.total.Store(int64(total))
}

func AdvanceProgress() int64 {
	return logProgressState.current.Add(1)
}

func ClearProgress() {
	logProgressState.current.Store(0)
	logProgressState.total.Store(0)
}

func ResetCommandStartTime(start time.Time) {
	commandStartUnixNano.Store(start.UnixNano())
}

func CurrentCommandStartTime() time.Time {
	unixNano := commandStartUnixNano.Load()
	if unixNano == 0 {
		return time.Now()
	}
	return time.Unix(0, unixNano)
}

func formatLogElapsed(d time.Duration) string {
	if d <= 0 {
		return "0s"
	}
	return d.Round(100 * time.Millisecond).String()
}

func FormatElapsed(d time.Duration) string {
	s := roundElapsedSeconds(d)
	if s < 60 {
		return fmt.Sprintf("%.1fs", s)
	}
	m := int(s) / 60
	rs := s - float64(m*60)
	return fmt.Sprintf("%dm%.1fs", m, rs)
}

func roundElapsedSeconds(d time.Duration) float64 {
	return math.Round(d.Seconds()*10) / 10
}

func Infof(format string, args ...any) {
	logWithLevel("INFO", format, args...)
}

func Errorf(format string, args ...any) {
	logWithLevel("ERROR", format, args...)
}

func logWithLevel(level string, format string, args ...any) {
	now := time.Now().Format(time.RFC3339)
	elapsed := formatLogElapsed(time.Since(CurrentCommandStartTime()))
	current := logProgressState.current.Load()
	total := logProgressState.total.Load()
	prefix := fmt.Sprintf("[%s] [%s] [%d/%d] [%s] ", now, elapsed, current, total, level)
	message := fmt.Sprintf(format, args...)
	fmt.Fprintln(os.Stderr, prefix+message)
	observability.EmitLog(context.Background(), level, message)
}

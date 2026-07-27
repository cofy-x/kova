package batch

import (
	"strings"

	"github.com/cofy-x/kova/internal/logging"
	"github.com/cofy-x/kova/internal/store"
)

func printBuildResult(entry store.Entry, succeededCount, totalCount, failedCount int) {
	status := "OK"
	if !entry.Success {
		status = "FAIL"
	}
	nodeIP := entry.NodeIP
	if strings.TrimSpace(nodeIP) == "" {
		nodeIP = "unknown"
	}
	logging.Infof("[%s] success=%d/%d fail=%d target=%s node-ip=%s elapsed=%s", status, succeededCount, totalCount, failedCount, entry.Target, nodeIP, entry.Elapsed)
}

func printSummary(results []store.Entry) {
	var succeeded, failed int
	for _, r := range results {
		if r.Success {
			succeeded++
		} else {
			failed++
		}
	}
	logging.Infof("Summary: %d total, %d succeeded, %d failed", len(results), succeeded, failed)
}

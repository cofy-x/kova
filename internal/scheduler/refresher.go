package scheduler

import (
	"context"
	"strings"
	"time"

	"github.com/cofy-x/kova/internal/logging"
)

const DefaultBuildkitAddrRefreshInterval = 15 * time.Second

func StartRefresher(ctx context.Context, pool *Pool, addrsRaw string, oomCooldown time.Duration) {
	if strings.TrimSpace(addrsRaw) == "" {
		return
	}

	go func() {
		ticker := time.NewTicker(DefaultBuildkitAddrRefreshInterval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}

			refreshedAddrs, err := ParseAddrs(addrsRaw)
			if err != nil {
				logging.Errorf("Failed to refresh buildkit addresses from %q: %v", addrsRaw, err)
				continue
			}
			for _, addr := range refreshedAddrs {
				if addr != nil {
					addr.Cooldown = oomCooldown
				}
			}

			before := pool.Addresses()
			pool.Replace(refreshedAddrs)
			after := pool.Addresses()
			if !sameStringSlice(before, after) {
				logging.Infof("Refreshed buildkit address pool: %d -> %d endpoint(s): [%s] -> [%s]",
					len(before), len(after), strings.Join(before, ", "), strings.Join(after, ", "))
			}
		}
	}()
}

func sameStringSlice(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}
	return true
}

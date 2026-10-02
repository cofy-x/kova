package service

import (
	"context"

	"github.com/cofy-x/kova/internal/logging"
	"github.com/cofy-x/kova/internal/service/admissiongenesis"
)

// startServiceComponents is the shared startup barrier. A future Genesis
// caller must pass the same guard to this barrier and the HTTP/controller
// components; nil retains the current legacy startup until all gates exist.
func startServiceComponents(ctx context.Context, stop context.CancelFunc, guard admissiongenesis.Checker,
	managerStart, httpStart func(context.Context) error) error {
	if guard != nil {
		if err := guard.Check(ctx); err != nil {
			return err
		}
	}
	go func() {
		if err := httpStart(ctx); err != nil {
			logging.Errorf("Kova Service HTTP server stopped: %v", err)
			stop()
		}
	}()
	return managerStart(ctx)
}

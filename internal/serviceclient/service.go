package serviceclient

import (
	"context"
	"fmt"

	"github.com/cofy-x/kova/internal/serviceapi"
)

func (c *Client) Version(ctx context.Context) (serviceapi.VersionInfo, error) {
	var info serviceapi.VersionInfo
	err := c.getJSON(ctx, "/version", &info)
	return info, err
}

func (c *Client) CheckCompatible(ctx context.Context) error {
	info, err := c.Version(ctx)
	if err != nil {
		return fmt.Errorf("query Kova service version: %w", err)
	}
	if info.APIVersion != serviceapi.APIVersion {
		return fmt.Errorf("incompatible Kova service API %q; client requires %q", info.APIVersion, serviceapi.APIVersion)
	}
	return nil
}

func (c *Client) Ready(ctx context.Context) error {
	var status struct {
		Status string `json:"status"`
	}
	if err := c.getJSON(ctx, "/readyz", &status); err != nil {
		return err
	}
	if status.Status != "ready" {
		return fmt.Errorf("service readiness status is %q", status.Status)
	}
	return nil
}

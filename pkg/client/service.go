package client

import (
	"context"
	"fmt"

	apiv1 "github.com/cofy-x/kova/pkg/api/v1"
)

func (c *Client) Version(ctx context.Context) (apiv1.VersionInfo, error) {
	var info apiv1.VersionInfo
	err := c.getJSON(ctx, "/version", &info)
	return info, err
}

func (c *Client) CheckCompatible(ctx context.Context) error {
	info, err := c.Version(ctx)
	if err != nil {
		return fmt.Errorf("query Kova service version: %w", err)
	}
	if info.APIVersion != apiv1.APIVersion {
		return fmt.Errorf("incompatible Kova service API %q; client requires %q", info.APIVersion, apiv1.APIVersion)
	}
	return nil
}

func (c *Client) Ready(ctx context.Context) error {
	var status apiv1.ReadyStatus
	if err := c.getJSON(ctx, "/readyz", &status); err != nil {
		return err
	}
	if status.Status != "ready" {
		return fmt.Errorf("service readiness status is %q", status.Status)
	}
	return nil
}

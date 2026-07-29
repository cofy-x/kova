package artifactgc

import (
	"context"
	"fmt"
	"strings"
	"time"

	kovav1 "github.com/cofy-x/kova/internal/apis/kova/v1alpha1"
	"github.com/cofy-x/kova/internal/artifactstore"

	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type Collector struct {
	Reader    client.Reader
	Store     artifactstore.Store
	Namespace string
	Interval  time.Duration
	OrphanTTL time.Duration
	Now       func() time.Time
}

func (*Collector) NeedLeaderElection() bool { return true }

func (c *Collector) Start(ctx context.Context) error {
	if c.Interval <= 0 || c.OrphanTTL <= 0 {
		return nil
	}
	logger := ctrl.LoggerFrom(ctx).WithName("artifact-gc")
	if err := c.Collect(ctx); err != nil {
		logger.Error(err, "artifact garbage collection failed")
	}
	ticker := time.NewTicker(c.Interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			if err := c.Collect(ctx); err != nil {
				logger.Error(err, "artifact garbage collection failed")
			}
		}
	}
}

func (c *Collector) Collect(ctx context.Context) error {
	if c.Reader == nil || c.Store == nil {
		return fmt.Errorf("artifact garbage collection requires a Kubernetes reader and artifact store")
	}
	var builds kovav1.KovaBuildList
	if err := c.Reader.List(ctx, &builds, client.InNamespace(c.Namespace)); err != nil {
		return fmt.Errorf("list KovaBuild resources for artifact garbage collection: %w", err)
	}
	retained := make(map[string]struct{}, len(builds.Items))
	for i := range builds.Items {
		retained[builds.Items[i].Name] = struct{}{}
	}
	artifacts, err := c.Store.List(ctx, "builds")
	if err != nil {
		return fmt.Errorf("list artifacts for garbage collection: %w", err)
	}
	now := time.Now()
	if c.Now != nil {
		now = c.Now()
	}
	cutoff := now.Add(-c.OrphanTTL)
	for _, artifact := range artifacts {
		buildID := artifactBuildID(artifact.Key)
		if _, ok := retained[buildID]; ok || artifact.Modified.After(cutoff) {
			continue
		}
		if err := c.Store.Delete(ctx, artifact.URI); err != nil {
			return fmt.Errorf("delete orphan artifact %q: %w", artifact.Key, err)
		}
	}
	return nil
}

func artifactBuildID(key string) string {
	parts := strings.SplitN(strings.TrimPrefix(key, "/"), "/", 3)
	if len(parts) < 3 || parts[0] != "builds" {
		return ""
	}
	return parts[1]
}

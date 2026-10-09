package service

import (
	"strings"
	"testing"

	"github.com/cofy-x/kova/internal/service/config"
)

func TestGenesisRuntimeExecutionIdentityMustMatchImmutableReceipt(t *testing.T) {
	receipt, cfg := genesisTestReceiptAndConfig()
	for name, mutate := range map[string]func(*config.Config){
		"missing pool":   func(c *config.Config) { c.WorkerPoolID = "" },
		"different pool": func(c *config.Config) { c.WorkerPoolID = "new-pool" },
		"mutable runner": func(c *config.Config) { c.RunnerImage = "example.com/kova/runner:latest" },
		"different manifest": func(c *config.Config) {
			c.RunnerImage = "example.com/kova/runner@sha256:" + strings.Repeat("b", 64)
			c.RunnerImageDigest = "sha256:" + strings.Repeat("b", 64)
		},
		"digest not image digest": func(c *config.Config) { c.RunnerImageDigest = "sha256:" + strings.Repeat("c", 64) },
	} {
		t.Run(name, func(t *testing.T) {
			changed := cfg
			mutate(&changed)
			if err := validateGenesisRuntimeConfig(changed, receipt); err == nil {
				t.Fatal("runtime identity drift accepted")
			}
		})
	}
}

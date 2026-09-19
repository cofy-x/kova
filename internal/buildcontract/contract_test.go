package buildcontract

import (
	"fmt"
	"strings"
	"testing"
)

func TestNormalizeTargetRequiresTaggedPushDestination(t *testing.T) {
	valid, err := NormalizeTarget("registry.example.com/team/image:dev")
	if err != nil || valid != "registry.example.com/team/image:dev" {
		t.Fatalf("NormalizeTarget() = %q, %v", valid, err)
	}
	dockerHub, err := NormalizeTarget("alpine:3.20")
	if err != nil || dockerHub != "index.docker.io/library/alpine:3.20" {
		t.Fatalf("NormalizeTarget() = %q, %v", dockerHub, err)
	}
	for _, value := range []string{
		"", " registry.example.com/team/image:dev", "registry.example.com/team/image",
		"registry.example.com/team/image@sha256:" + strings.Repeat("a", 64),
		"oci://registry.example.com/team/image:dev", "not a target:dev",
		strings.Repeat("a", MaxTargetLength+1),
	} {
		t.Run(fmt.Sprintf("%q", value), func(t *testing.T) {
			if _, err := NormalizeTarget(value); err == nil {
				t.Fatalf("expected %q to be rejected", value)
			}
		})
	}
}

func TestNormalizeTargetsIsSortedUniqueAndBounded(t *testing.T) {
	targets, err := NormalizeTargets([]string{"registry.example.com/team/b:dev", "registry.example.com/team/a:dev"})
	if err != nil {
		t.Fatal(err)
	}
	if targets[0] != "registry.example.com/team/a:dev" || targets[1] != "registry.example.com/team/b:dev" {
		t.Fatalf("targets = %#v", targets)
	}
	if _, err := NormalizeTargets([]string{targets[0], targets[0]}); err == nil {
		t.Fatal("expected duplicate target rejection")
	}
	if _, err := NormalizeTargets([]string{"alpine:3.20", "index.docker.io/library/alpine:3.20"}); err == nil {
		t.Fatal("expected canonical duplicate target rejection")
	}
	tooMany := make([]string, MaxLogicalTargets+1)
	for index := range tooMany {
		tooMany[index] = fmt.Sprintf("registry.example.com/team/image-%03d:dev", index)
	}
	if _, err := NormalizeTargets(tooMany); err == nil {
		t.Fatal("expected target limit rejection")
	}
}

func TestValidateConcurrencyIsBoundedByTargetCount(t *testing.T) {
	if err := ValidateConcurrency(2, 2); err != nil {
		t.Fatal(err)
	}
	for _, concurrency := range []int{0, 3, MaxBuildConcurrency + 1} {
		if err := ValidateConcurrency(concurrency, 2); err == nil {
			t.Fatalf("expected concurrency %d to fail", concurrency)
		}
	}
}

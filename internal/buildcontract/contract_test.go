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

func TestNormalizeLogicalTargetRejectsReservedNydusSuffix(t *testing.T) {
	if _, err := NormalizeLogicalTarget("registry.example.com/team/image:dev_nydus_v3"); err == nil {
		t.Fatal("expected the derived Nydus output suffix to be rejected for a logical target")
	}
	if output, err := NormalizeTarget("registry.example.com/team/image:dev_nydus_v3"); err != nil || output == "" {
		t.Fatalf("concrete Nydus output must remain a valid tagged reference: output=%q err=%v", output, err)
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

func TestNormalizeTargetSpecsRequiresCanonicalSupportedPlatform(t *testing.T) {
	valid, err := NormalizeTargetSpecs([]TargetSpec{{Target: "alpine:3.20", Platform: PlatformLinuxARM64}})
	if err != nil {
		t.Fatal(err)
	}
	if valid[0].Target != "index.docker.io/library/alpine:3.20" || valid[0].Platform != PlatformLinuxARM64 {
		t.Fatalf("target specs = %#v", valid)
	}
	for _, platform := range []string{"", "linux/s390x", "LINUX/AMD64", " linux/amd64"} {
		if _, err := NormalizeTargetSpecs([]TargetSpec{{Target: "alpine:3.20", Platform: platform}}); err == nil {
			t.Fatalf("expected platform %q to fail", platform)
		}
	}
	if _, err := NormalizeTargetSpecs([]TargetSpec{
		{Target: "alpine:3.20", Platform: PlatformLinuxAMD64},
		{Target: "index.docker.io/library/alpine:3.20", Platform: PlatformLinuxARM64},
	}); err == nil {
		t.Fatal("expected a destination shared by two platforms to fail")
	}
}

func TestEqualTargetSpecSetsIgnoresOrderButNotPlatform(t *testing.T) {
	left := []TargetSpec{{Target: "example.com/a:dev", Platform: PlatformLinuxAMD64}, {Target: "example.com/b:dev", Platform: PlatformLinuxARM64}}
	right := []TargetSpec{{Target: "example.com/b:dev", Platform: PlatformLinuxARM64}, {Target: "example.com/a:dev", Platform: PlatformLinuxAMD64}}
	if !EqualTargetSpecSets(left, right) {
		t.Fatal("expected order-independent equality")
	}
	right[1].Platform = PlatformLinuxARM64
	if EqualTargetSpecSets(left, right) {
		t.Fatal("expected platform mismatch to fail equality")
	}
}

func TestParsePlatformAddressesRequiresUniqueCanonicalPools(t *testing.T) {
	pools, err := ParsePlatformAddresses([]string{
		"linux/amd64=tcp://amd64-a:9094,tcp://amd64-b:9094",
		"linux/arm64=tcp://arm64:9094",
	})
	if err != nil {
		t.Fatal(err)
	}
	if pools[PlatformLinuxAMD64] != "tcp://amd64-a:9094,tcp://amd64-b:9094" || pools[PlatformLinuxARM64] != "tcp://arm64:9094" {
		t.Fatalf("pools = %#v", pools)
	}
	for _, values := range [][]string{
		nil,
		{"linux/s390x=tcp://worker:9094"},
		{"linux/amd64="},
		{"linux/amd64=tcp://a:9094", "linux/amd64=tcp://b:9094"},
	} {
		if _, err := ParsePlatformAddresses(values); err == nil {
			t.Fatalf("expected %#v to fail", values)
		}
	}
}

# Kova Documentation

Start with the [installation and first-build guide](quickstart.md). Continue
with the [complete CLI workflow](cli-workflow.md) for build operations or the
[runtime architecture overview](architecture.md) for the system model.

## Core

- [Installation and first build](quickstart.md): install the OCI Helm chart and
  matching CLI, then verify a build.
- [CLI workflow](cli-workflow.md): prepare, build, logs, wait, export, and
  cleanup.
- [Runtime design](architecture.md): roles, topology, build/export,
  preheat, and scaling flows.
- [Service API](service.md): optional HTTP gateway that creates runner Pods
  for service-style usage.
- [Validation matrix](testing.md): static checks, E2E targets, and runtime smoke
  expectations.
- [Release artifacts](releases.md): versioning, CLI archives, runtime images,
  checksums, SBOMs, and provenance.
- [Telemetry operations](observability.md): production OpenTelemetry stack and
  local Compose LGTM validation.

## Deployment

- [Kubernetes deployment](deployment/kubernetes.md): prerequisites, Helm
  installation, registry credentials, scaling, and production configuration.
- [Local kind registry](deployment/local-kind-registry.md): local registry
  address mapping and kind containerd mirror behavior.

## Runtime

- [Dragonfly and Nydus integration](runtime/dragonfly-nydus.md): Nydus build,
  Dragonfly preheat, and local runtime smoke topology.

# Kova Documentation

Start with the [installation quick start](quickstart.md), then use the
[complete CLI workflow](cli-workflow.md) for build operations and the
[runtime architecture overview](architecture.md) for the system model.

## Core

- [Public installation](quickstart.md): install the OCI Helm chart and matching
  CLI.
- [CLI workflow](cli-workflow.md): prepare, build, logs, wait, export, and
  cleanup.
- [Runtime design](architecture.md): roles, topology, build/export,
  preheat, and scaling flows.
- [Design contract](design.md): runtime, API, storage, security, scheduling,
  observability, and release boundaries.
- [Service Daemon](service.md): optional HTTP gateway that creates runner Pods
  for service-style usage.
- [Validation matrix](testing.md): static checks, E2E targets, and runtime smoke
  expectations.
- [Release artifacts](releases.md): versioning, CLI archives, runtime images,
  checksums, SBOMs, and provenance.
- [Telemetry operations](observability.md): production OpenTelemetry stack and
  local Compose LGTM validation.

## Deployment

- [Kubernetes Deployment](deployment/kubernetes.md): prerequisites, Helm
  installation, registry credentials, scaling, and production configuration.
- [Local Kind Registry](deployment/local-kind-registry.md): local registry
  address mapping and kind containerd mirror behavior.

## Runtime

- [Dragonfly And Nydus](runtime/dragonfly-nydus.md): Nydus build, Dragonfly
  preheat, and local runtime smoke topology.

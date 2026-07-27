# Kova Documentation

Start with the [CLI quickstart guide](quickstart.md) for the daily flow and the
[runtime architecture overview](architecture.md) for the system model.

## Core

- [CLI workflow](quickstart.md): prepare, build, logs, wait, export, and
  cleanup.
- [Runtime design](architecture.md): roles, topology, build/export,
  preheat, and scaling flows.
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

# Kova Documentation

Start with the [installation and first-build guide](quickstart.md). Continue
with the [complete CLI workflow](cli-workflow.md) for build operations or the
[runtime architecture overview](architecture.md) for the system model.

## Core

- [Installation and first build](quickstart.md): install the OCI Helm chart and
  matching CLI, then verify a build.
- [Direct runner workflow](cli-workflow.md): prepare, build, logs, wait,
  export, and cleanup for development.
- [Runtime design](architecture.md): roles, explicit platform worker pools,
  build/export, preheat, and scaling flows.
- [Service API and official SDKs](service.md): immutable source contract, authenticated job CLI, Python and Go clients, structured errors, RBAC, and bounded results.
- [Service admission design](service-admission-design.md): durable active and queued reservations, event-driven capacity wakes, and recovery requirements.
- [Committed-loss drain-only slice](recovery-drain.md): receipt and external-permit contracts, monotonic name occupancy, and why occupancy is not settlement.
- [Executable Go and Python SDK examples](../examples/service-sdk/README.md): submit one immutable source, wait for terminal state, and persist every verified output in a caller-owned receipt.
- [Machine-readable Service API](../api/openapi.yaml): stable OpenAPI 3.1 operations, schemas, authentication, and error responses.
- [Caller-owned seed build receipt](examples/seed-build-receipt-v1.json): reference shape that records recipe, source, Kova version, terminal status, role, platform, and immutable output facts outside Kova.
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

# Scripts

Shell scripts are grouped by the workflow they support. Make targets should stay
as the stable entrypoints for common workflows; direct script calls should use
the categorized paths below.

## Shared

- `common.sh`: shared helpers for repository paths, command checks, Docker arch,
  kind worker discovery, and local proxy detection.

## Documentation

- `docs/check.sh`: validate relative links in public and contributor Markdown.

## Deployment

- `deployment/verify-kovabuild-crd.sh`: block a controller upgrade until the
  live KovaBuild CRD is Established and its retry status fields have the expected schema.
- `deployment/test-verify-kovabuild-crd.sh`: exercise the upgrade gate against
  new, old, incompatible, and unavailable mock CRDs without a cluster.
- `deployment/probe-kovabuild-status.sh`: prove retry status fields are pruned
  by a legacy CRD and retained by the current CRD using a temporary namespace.
- `deployment/test-probe-kovabuild-status.sh`: exercise the probe and its
  namespace cleanup against a mock Kubernetes API.
- `deployment/verify-kovabuild-drained.sh`: require all KovaBuilds terminal,
  no runner or Service Pods, and a stopped Service Deployment before a
  cross-version controller upgrade; submission must remain frozen separately.
- `deployment/test-verify-kovabuild-drained.sh`: cluster-free fail-closed
  checks for active, unknown, unavailable, and malformed drain states.

## CI

- `ci/public-go-consumer.sh`: compile a clean external module against the local public Go SDK or an exact release tag, and verify versioned `go install` for releases.

## Build

- `build/build-image.sh`: build the controller, runner, and worker images.
- `build/build-python-smoke-base.sh`: build the local Python smoke base image.

## Release

- `release/build-cli.sh`: build the CGO-free `kova` release archives for Linux,
  macOS, and Windows.
- `release/package-chart.sh`: lint and package a version-aligned Helm chart.

## Package

- `package/package-example.sh`: package selected examples into a source zip.
- `package/package-concurrent-example.sh`: generate and package concurrent
  examples.

## Local Kind

- `kind/kind-registry.sh`: create or attach the local Docker registry.
- `kind/kind-create.sh`: create the local kind cluster.
- `kind/kind-load.sh`: load the Kova image into kind.
- `kind/deploy-kind.sh`: apply and verify the current KovaBuild CRD before
  upgrading the local controller; historical-chart tests explicitly skip the
  current retry-schema gate.
- `kind/diagnose-kind.sh`: print local kind, registry, Kova, Dragonfly/Nydus,
  runtime smoke, and result summaries.
- `kind/clean-kind.sh`: delete local kind resources.

## Observability

- `observability/local-up.sh`: start the local Compose LGTM stack.
- `observability/local-down.sh`: stop and remove the local Compose LGTM stack.
- `observability/local-status.sh`: verify the local LGTM service and Grafana health.

## Runtime Infrastructure

- `runtime/dragonfly-nydus-install.sh`: install Dragonfly/Nydus into local
  kind.

## E2E

- `e2e/e2e-runtime-preflight.sh`: validate local tools and registry readiness
  before the full runtime smoke.
- `e2e/e2e-helm-quickstart.sh`: install a packaged chart into an ephemeral
  minimal kind cluster, run the public CLI build flow, and clean up the
  test-owned cluster on exit. `KEEP_KIND_CLUSTER=true` retains a cluster
  created by the test for debugging; `REUSE_KIND_CLUSTER=true` explicitly
  reuses a caller-owned cluster without deleting it.
- `e2e/e2e.sh`: run the basic local OCI build smoke.
- `e2e/e2e-service.sh`: run the service daemon HTTP build smoke.
- `e2e/e2e-crd-upgrade.sh`: migrate from the pinned public pre-retry CRD in
  an isolated Kind cluster, check live status pruning/persistence, assert the
  drain gate, and verify an injected old Starting runner fails closed before
  running the Service smoke.
- `e2e/e2e-concurrent.sh`: run concurrent local builds.
- `e2e/e2e-dragonfly-nydus.sh`: run local Dragonfly/Nydus validation.
- `e2e/e2e-runtime.sh`: run OCI and Nydus runtime validation.
- `e2e/e2e-observability.sh`: run local telemetry validation.

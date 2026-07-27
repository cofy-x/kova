# Scripts

Shell scripts are grouped by the workflow they support. Make targets should stay
as the stable entrypoints for common workflows; direct script calls should use
the categorized paths below.

## Shared

- `common.sh`: shared helpers for repository paths, command checks, Docker arch,
  kind worker discovery, and local proxy detection.

## Build

- `build/build-binary-linux.sh`: cross-build the Linux `kova` binary.
- `build/build-image.sh`: build the Kova runtime image.
- `build/build-python-smoke-base.sh`: build the local Python smoke base image.

## Package

- `package/package-example.sh`: package selected examples into a source zip.
- `package/package-concurrent-example.sh`: generate and package concurrent
  examples.

## Local Kind

- `kind/kind-registry.sh`: create or attach the local Docker registry.
- `kind/kind-create.sh`: create the local kind cluster.
- `kind/kind-load.sh`: load the Kova image into kind.
- `kind/deploy-kind.sh`: deploy Kova into the local kind cluster.
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
- `e2e/e2e.sh`: run the basic local OCI build smoke.
- `e2e/e2e-service.sh`: run the service daemon HTTP build smoke.
- `e2e/e2e-concurrent.sh`: run concurrent local builds.
- `e2e/e2e-dragonfly-nydus.sh`: run local Dragonfly/Nydus validation.
- `e2e/e2e-runtime.sh`: run OCI and Nydus runtime validation.
- `e2e/e2e-observability.sh`: run local telemetry validation.

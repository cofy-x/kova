# Kova

[![CI](https://github.com/cofy-x/kova/actions/workflows/ci.yml/badge.svg)](https://github.com/cofy-x/kova/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)

Kova is a Kubernetes-native, cloud-provider-neutral image build service powered
by BuildKit. It builds batches of Dockerfile contexts into OCI or Nydus images,
pushes them to OCI registries, and can preheat successful results through a
Dragonfly P2P cluster.

## Install The CLI

Download a provenance-attested archive for Linux, macOS, or Windows from the
[GitHub releases](https://github.com/cofy-x/kova/releases), verify it with the
published `checksums.txt`, and place `kova` on `PATH`. Linux and macOS builds
are available for `amd64` and `arm64`; Windows builds are available as `.zip`
archives for both architectures.

Go users can install the latest tagged client directly:

```bash
go install github.com/cofy-x/kova/cmd/kova@latest
kova version
```

Use an explicit release tag instead of `@latest` when the installed version
must be reproducible. Contributors can install the current checkout with:

```bash
make install
kova version
```

The client is CGO-free and runs on the workstation. Linux runtime images are
split into controller, runner, and rootless BuildKit worker roles.

## Documentation

- [Documentation Index](docs/README.md): organized guide to the project docs.
- [CLI workflow](docs/quickstart.md): daily usage, local contexts, prepare,
  build, logs, wait, export, and cleanup.
- [Runtime design](docs/architecture.md): roles, topology, build/export,
  preheat, and scaling flows.
- [Kubernetes Deployment](docs/deployment/kubernetes.md): Helm installation,
  registry credentials, worker sizing, and production configuration.
- [Validation matrix](docs/testing.md): static checks, E2E targets, and runtime
  smoke expectations.
- [Release process](docs/releases.md): CLI archives, runtime images, SBOMs,
  provenance, and version tags.
- [Examples](examples/README.md): build input examples and runtime smoke
  service details.

## Local Development Requirements

- Docker
- kind 0.32.0 or newer
- Helm
- kubectl
- curl
- `zip`
- The Go version declared in `go.mod` available as `go`
- Network access for Docker to pull the pinned Ubuntu base image

## Quick Start

Build the role images, create a dedicated kind cluster, deploy the Helm chart,
run a sample build, and verify the pushed image:

```bash
make image
make e2e
```

The E2E target builds `examples/simple`, pushes it to the local registry, exports
`result.jsonl`, and verifies the image can be pulled from the host:

```bash
docker pull localhost:5002/kova-examples/simple:dev
```

Clean the local environment:

```bash
make clean-kind
```

Run broader validation targets as needed:

```bash
make e2e-concurrent
make e2e-dragonfly-nydus
make e2e-runtime
```

See the [target coverage guide](docs/testing.md) for when to run each check.

## Contributing

Contributions are welcome. Read the [contribution guide](CONTRIBUTING.md) for
the development workflow and the [security policy](SECURITY.md) before
reporting a vulnerability.

Kova is licensed under the [Apache License 2.0](LICENSE).

## Daily CLI Flow

Use the [operator quickstart](docs/quickstart.md) for the normal sequence:
`prepare`, `build`, `logs`, `wait`, `export`, and `destroy`.

## Build Input Format

`kova build` accepts a single build context directory for one-off builds:

```bash
kova build ./image-1 --target host.docker.internal:5002/kova-examples/simple:dev
```

For batch builds, `kova build` also reads a zip stream from stdin. The zip root
must contain one directory per image:

```text
image-1/
  Dockerfile
  metadata.json
  ...
image-2/
  Dockerfile
  metadata.json
  ...
```

Each `metadata.json` must contain a `target` field:

```json
{
  "target": "$KOVA_IMAGE_REGISTRY/kova-examples/simple:dev"
}
```

Dockerfiles and metadata can use `$KOVA_*` variables. Pass values with
repeatable `--var KEY=value` flags.

## Development Commands

Run checks:

```bash
make test
make helm-template
```

Build a host-platform binary for local debugging:

```bash
make kova
```

`kova-controller` and `kovad` are Linux runtime components distributed in the
controller and runner images rather than as workstation binaries.

The local `kova` CLI defaults to `tcp://kova.kova.svc:9094`. Override it with
`--buildkit-addr` or `KOVA_BUILDKIT_ADDR` for another release, namespace, or
Kubernetes cluster.

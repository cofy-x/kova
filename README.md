# Kova

[![CI](https://github.com/cofy-x/kova/actions/workflows/ci.yml/badge.svg)](https://github.com/cofy-x/kova/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)

Kova is a Kubernetes-native, cloud-provider-neutral image build service powered
by BuildKit. It builds batches of Dockerfile contexts into OCI or Nydus images,
pushes them to OCI registries, and can preheat successful results through a
Dragonfly P2P cluster.

## Install the CLI

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

## Install Kova

Choose a tag from [GitHub releases](https://github.com/cofy-x/kova/releases),
then install that exact OCI Helm chart without cloning the repository:

```bash
export KOVA_VERSION=vX.Y.Z

helm show crds oci://ghcr.io/cofy-x/charts/kova \
  --version "${KOVA_VERSION#v}" | kubectl apply -f -
helm upgrade --install kova oci://ghcr.io/cofy-x/charts/kova \
  --version "${KOVA_VERSION#v}" \
  --namespace kova \
  --create-namespace \
  --wait
```

Applying the release CRD before every Helm upgrade is required because Helm
does not upgrade files from a chart's `crds/` directory.

The chart selects matching controller, runner, and worker images automatically.
Continue with the [installation and first-build guide](docs/quickstart.md).

For shared environments, enable the authenticated Service and use the native
job workflow:

```bash
kova job submit ./image --target registry.example.com/team/image:dev
kova job list
kova job wait <job-id>
kova job results <job-id>
```

The [Service security and CLI guide](docs/service.md) covers identity, RBAC,
contexts, artifact storage, and job operations. Direct runner commands remain
available for local development and low-level debugging.

## Documentation

- [Documentation map](docs/README.md): choose the guide for a task.
- [Installation and first build](docs/quickstart.md): install the public OCI
  Helm chart and matching CLI, then verify a build.
- [CLI workflow](docs/cli-workflow.md): contexts, prepare,
  direct runner builds, logs, export, and cleanup.
- [Service job workflow](docs/service.md): authenticated shared builds,
  authorization, storage, and native CLI operations.
- [Runtime design](docs/architecture.md): roles, topology, build/export,
  preheat, and scaling flows.
- [Kubernetes deployment](docs/deployment/kubernetes.md): Helm installation,
  registry credentials, worker sizing, and production configuration.
- [Validation matrix](docs/testing.md): static checks, E2E targets, and runtime
  smoke expectations.
- [Release process](docs/releases.md): CLI archives, OCI Helm charts, runtime
  images, SBOMs, provenance, and version tags.
- [Examples](examples/README.md): build input examples and runtime smoke
  service details.

## Develop Kova

The repository requires the Go version declared in `go.mod`, Docker, kind,
Helm, kubectl, curl, zip, and LMDB development headers. Run the fast checks
with:

```bash
make test
make lint-scripts
make helm-template
```

Run the released-chart installation path locally with:

```bash
make e2e-helm-quickstart
```

Use the [validation guide](docs/testing.md) to choose broader E2E coverage.
Contributions are welcome; the [contribution workflow](CONTRIBUTING.md) covers
the full setup and pull request process. Report vulnerabilities through the
private process in the [security policy](SECURITY.md).

Kova is licensed under the [Apache License 2.0](LICENSE).

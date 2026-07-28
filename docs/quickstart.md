# Quick Start

This guide installs a released Kova Helm chart and verifies its rootless
BuildKit worker. The chart is cloud-provider-neutral and uses public images from
GitHub Container Registry by default.

## Prerequisites

- a Kubernetes cluster that permits Kova's rootless BuildKit security profile
- Helm with OCI registry support
- `kubectl` access that can create namespaced workloads

Choose a tag from the
[GitHub release page](https://github.com/cofy-x/kova/releases), then set it once
so the chart, CLI, and runtime images remain aligned. Replace `vX.Y.Z` with the
selected tag, including a prerelease suffix when applicable:

```bash
export KOVA_VERSION=vX.Y.Z
export KOVA_CHART_VERSION=${KOVA_VERSION#v}
```

## Install Kova

Install the public OCI chart without cloning the repository:

```bash
helm upgrade --install kova oci://ghcr.io/cofy-x/charts/kova \
  --version "${KOVA_CHART_VERSION}" \
  --namespace kova \
  --create-namespace \
  --wait
```

The default installation creates one rootless BuildKit worker and its headless
discovery Service. Published charts bind the controller, runner, and worker
image tags to the same Kova release automatically.

Verify the installation:

```bash
kubectl -n kova rollout status deployment/kova
kubectl -n kova get pods,service
```

## Install the CLI

Install the matching workstation client with Go:

```bash
go install "github.com/cofy-x/kova/cmd/kova@${KOVA_VERSION}"
kova version
```

Release archives for Linux, macOS, and Windows are also available from the
[GitHub release page](https://github.com/cofy-x/kova/releases).

## Run a Build

Kova pushes build results to an OCI registry. Before the first build, choose a
target registry reachable from the cluster. Make it reachable from the
workstation as well when host-side pull verification is required. When the
registry requires authentication, create a Docker registry Secret in the
runner namespace:

```bash
kubectl -n kova create secret docker-registry kova-registry \
  --docker-server REGISTRY_HOST \
  --docker-username REGISTRY_USERNAME \
  --docker-password REGISTRY_PASSWORD
```

Keep the command out of shared shell history in real environments. Prefer the
environment's external secret controller for durable credentials.

Set the Secret name, or leave it empty for an anonymous development registry:

```bash
export KOVA_REGISTRY_SECRET=kova-registry
export KOVA_TARGET=REGISTRY_HOST/kova-quickstart/hello:dev
```

Replace the uppercase registry placeholders before running these commands. For
an anonymous registry, omit the Secret creation and set
`KOVA_REGISTRY_SECRET` to an empty value.

Create a CLI context for the installed worker Service:

```bash
export KOVA_KUBECONFIG=${KUBECONFIG:-$HOME/.kube/config}

kova ctx set \
  --mode direct \
  --kubeconfig "${KOVA_KUBECONFIG}" \
  --namespace kova \
  --buildkit-addr tcp://kova.kova.svc:9094 \
  --image "ghcr.io/cofy-x/kova:runner-${KOVA_VERSION}" \
  --image-pull-policy IfNotPresent \
  --image-pull-secret "${KOVA_REGISTRY_SECRET}" \
  --use \
  quickstart
```

Prepare a runner, create a minimal build context, and build it:

```bash
kova --name quickstart prepare

mkdir -p .work/kova-quickstart
printf 'FROM scratch\nCOPY hello.txt /\n' \
  > .work/kova-quickstart/Dockerfile
printf 'hello from kova\n' > .work/kova-quickstart/hello.txt

kova --name quickstart build .work/kova-quickstart \
  --target "${KOVA_TARGET}" \
  --format oci \
  --concurrency 1 \
  --fail-fast
kova --name quickstart wait --timeout 600
kova --name quickstart export \
  --result .work/kova-quickstart-result.jsonl \
  --target "${KOVA_TARGET}" \
  --oci
```

Verify that `.work/kova-quickstart-result.jsonl` reports a successful build.
Pull `${KOVA_TARGET}` as an additional registry-path check when the workstation
can reach the target registry.

The [direct runner workflow](cli-workflow.md) covers contexts, logs, batch
input, Nydus output, and result selection. Use the
[authenticated Service workflow](service.md) for shared or platform-operated
environments. The
[Kubernetes deployment guide](deployment/kubernetes.md) covers private registry
credentials, service mode, artifact storage, capacity, and production overlays.

## Uninstall

```bash
kova --name quickstart destroy
helm uninstall kova --namespace kova
rm -rf .work/kova-quickstart .work/kova-quickstart-result.jsonl
```

The chart does not create clusters, cloud accounts, output registries, or
provider credentials. Those resources remain owned by the consuming
environment.

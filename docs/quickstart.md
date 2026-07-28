# Quick Start

This guide installs a released Kova Helm chart and verifies its rootless
BuildKit worker. The chart is cloud-provider-neutral and uses public images from
GitHub Container Registry by default.

## Prerequisites

- a Kubernetes cluster that permits Kova's rootless BuildKit security profile
- Helm with OCI registry support
- `kubectl` access that can create namespaced workloads

Set the release once so the chart and CLI remain aligned:

```bash
export KOVA_VERSION=v0.1.0-rc.3
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

## Install The CLI

Install the matching workstation client with Go:

```bash
go install "github.com/cofy-x/kova/cmd/kova@${KOVA_VERSION}"
kova version
```

Release archives for Linux, macOS, and Windows are also available from the
[GitHub release page](https://github.com/cofy-x/kova/releases).

## Run A Build

Kova pushes build results to an OCI registry. Before the first build, choose a
target registry reachable from both the cluster and your workstation. When it
requires authentication, create a Docker registry Secret in the runner
namespace:

```bash
kubectl -n kova create secret docker-registry kova-registry \
  --docker-server <registry> \
  --docker-username <username> \
  --docker-password <password>
```

Keep the command out of shared shell history in real environments. Prefer the
environment's external secret controller for durable credentials.

Set the Secret name, or leave it empty for an anonymous development registry:

```bash
export KOVA_REGISTRY_SECRET=kova-registry
```

Create a CLI context for the installed worker Service:

```bash
export KOVA_KUBECONFIG=${KUBECONFIG:-$HOME/.kube/config}

kova ctx set \
  --kubeconfig "${KOVA_KUBECONFIG}" \
  --namespace kova \
  --buildkit-addr tcp://kova.kova.svc:9094 \
  --image "ghcr.io/cofy-x/kova:runner-${KOVA_VERSION}" \
  --image-pull-policy IfNotPresent \
  --image-pull-secret "${KOVA_REGISTRY_SECRET}" \
  --use \
  quickstart
```

Then follow the [complete CLI workflow](cli-workflow.md) for `prepare`, `build`,
`wait`, `export`, and `destroy`. The
[Kubernetes deployment guide](deployment/kubernetes.md) covers private registry
credentials, service mode, artifact storage, capacity, and production overlays.

## Uninstall

```bash
helm uninstall kova --namespace kova
```

The chart does not create clusters, cloud accounts, output registries, or
provider credentials. Those resources remain owned by the consuming
environment.

# Quick Start

This guide installs a released Kova Service and completes an authenticated OCI
build. The chart is cloud-provider-neutral and uses public images from GitHub
Container Registry by default.

## Prerequisites

- a Kubernetes cluster that permits Kova's rootless BuildKit security profile
- Helm with OCI registry support
- `kubectl` access that can create namespaced workloads
- `openssl` for generating the development Service token

Choose a tag from the
[GitHub release page](https://github.com/cofy-x/kova/releases), then set it once
so the chart, CLI, and runtime images remain aligned. Replace `vX.Y.Z` with the
selected tag, including a prerelease suffix when applicable:

```bash
export KOVA_VERSION=vX.Y.Z
export KOVA_CHART_VERSION=${KOVA_VERSION#v}
```

## Install Kova

Create the namespace and a development authentication token. Keep the token out
of shared shell history in real environments:

```bash
kubectl create namespace kova --dry-run=client -o yaml | kubectl apply -f -
export KOVA_SERVICE_TOKEN=$(openssl rand -hex 32)
kubectl -n kova create secret generic kova-service-auth \
  --from-literal=token="${KOVA_SERVICE_TOKEN}"
```

Apply the selected release CRD, then install the public OCI chart with the
Service and a filesystem artifact PVC enabled:

```bash
helm show crds oci://ghcr.io/cofy-x/charts/kova \
  --version "${KOVA_CHART_VERSION}" | kubectl apply -f -
helm upgrade --install kova oci://ghcr.io/cofy-x/charts/kova \
  --version "${KOVA_CHART_VERSION}" \
  --namespace kova \
  --create-namespace \
  --set serviceDaemon.enabled=true \
  --set serviceDaemon.authentication.mode=static \
  --set serviceDaemon.authentication.staticPrincipal=kova:quickstart \
  --set serviceDaemon.authentication.staticTokenSecret.name=kova-service-auth \
  --set artifactStore.filesystem.pvc.create=true \
  --wait
```

The explicit CRD apply is required on upgrades because Helm does not update
files in a chart's `crds/` directory.

Grant the quick-start principal permission to submit through the Service. This
Role does not grant direct access to `KovaBuild`, Pods, or Secrets:

```bash
kubectl -n kova create rolebinding kova-quickstart \
  --role=kova-service-submitter \
  --user=kova:quickstart
```

The installation creates the Service controller, one rootless BuildKit worker,
and the artifact PVC. Published charts bind controller, runner, and worker image
tags to the same Kova release automatically.

Verify the installation:

```bash
kubectl -n kova rollout status deployment/kova-service
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

When the Secret is non-empty, attach it to new runners and controller-side
result verification:

```bash
if [ -n "${KOVA_REGISTRY_SECRET}" ]; then
  helm upgrade kova oci://ghcr.io/cofy-x/charts/kova \
    --version "${KOVA_CHART_VERSION}" \
    --namespace kova \
    --reuse-values \
    --set serviceDaemon.runnerImagePullSecret="${KOVA_REGISTRY_SECRET}" \
    --set serviceDaemon.registrySecret="${KOVA_REGISTRY_SECRET}" \
    --wait
fi
```

Forward the cluster-internal Service, create a Service context, and verify the
API version, readiness, authentication, and authorization:

```bash
kubectl -n kova port-forward service/kova-service 8080:8080

kova ctx set \
  --mode service \
  --service-url http://127.0.0.1:8080 \
  --use \
  quickstart

kova doctor
```

Create a minimal build context and submit it:

```bash
mkdir -p .work/kova-quickstart
printf 'FROM scratch\nCOPY hello.txt /\n' \
  > .work/kova-quickstart/Dockerfile
printf 'hello from kova\n' > .work/kova-quickstart/hello.txt

kova job submit .work/kova-quickstart \
  --target "${KOVA_TARGET}" \
  --format oci \
  --concurrency 1 \
  --fail-fast
```

Copy the returned job ID, then inspect the complete lifecycle:

```bash
kova job wait <job-id> --timeout 10m
kova job get <job-id>
kova job results <job-id>
kova job logs <job-id>
```

Pull `${KOVA_TARGET}` as an additional registry-path check when the workstation
can reach the target registry. Logs and typed results remain available from the
artifact store after the short-lived runner Pod disappears.

The [authenticated Service workflow](service.md) covers TokenReview, batch
archives, cancellation, Nydus output, and production artifact storage. The
[direct runner workflow](cli-workflow.md) is reserved for development and
low-level debugging. The
[Kubernetes deployment guide](deployment/kubernetes.md) covers private registry
credentials, service mode, artifact storage, capacity, and production overlays.

## Uninstall

```bash
helm uninstall kova --namespace kova
kubectl delete namespace kova
rm -rf .work/kova-quickstart
```

The chart does not create clusters, cloud accounts, output registries, or
provider credentials. Those resources remain owned by the consuming
environment.

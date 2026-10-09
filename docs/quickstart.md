# Quick Start

This guide installs a released Kova Service and completes an authenticated OCI
build. The chart is cloud-provider-neutral and uses public images from GitHub
Container Registry by default.

## Prerequisites

- a Kubernetes cluster that permits Kova's rootless BuildKit security profile
- Helm with OCI registry support
- `kubectl` access that can create namespaced workloads
- `openssl` for generating the development Service token
- `jq` and an explicit absolute `KOVA_KUBECONFIG` for create-only installation receipts

Choose a tag from the
[GitHub release page](https://github.com/cofy-x/kova/releases), then set it once
so the chart, CLI, and runtime images remain aligned. Replace `vX.Y.Z` with the
selected tag, including a prerelease suffix when applicable:

```bash
export KOVA_VERSION=vX.Y.Z
export KOVA_CHART_VERSION=${KOVA_VERSION#v}
export KOVA_PLATFORM=linux/amd64 # or linux/arm64 for the worker nodes
export KOVA_KUBECONFIG=/absolute/path/to/the-selected-cluster.kubeconfig
export KUBECONFIG="${KOVA_KUBECONFIG}"
```

Install the matching CLI before provisioning admission authority:

```bash
go install "github.com/cofy-x/kova/cmd/kova@${KOVA_VERSION}"
kova version
```

This source guide describes the required Genesis contract in the next candidate.
The v3 installation pins an externally assigned worker capacity identity and the runner's exact OCI manifest/index digest; older v1/v2 receipts cannot be adopted.
Do not combine it with an older release that lacks `kova admission-genesis`; use that release's versioned guide instead.

## Install Kova

Create the namespace and a development authentication token. Keep the token out
of shared shell history in real environments:

```bash
kubectl create namespace kova --dry-run=client -o yaml | kubectl apply -f -
export KOVA_SERVICE_TOKEN=$(openssl rand -hex 32)
kubectl -n kova create secret generic kova-service-auth \
  --from-literal=token="${KOVA_SERVICE_TOKEN}"
```

Use that same explicit kubeconfig for every Kubernetes and Helm command.

This is a fresh installation, not an in-place upgrade.
For an existing Service, first follow the [stop-and-drain migration and Genesis installation gates](deployment/kubernetes.md).
Never reuse an old runner namespace name or run old and new writers together.

Apply the selected release CRD, then externally create distinct, fresh runner and recovery-receipt namespaces, Genesis, and an immutable installation receipt.
The helper only reads Kubernetes; the explicit `kubectl create` operations own installation.
Run this block in Bash; keep its private receipt directory outside Git.
If any create has an unknown result, stop and inspect the retained original identities; do not rerun or replace it.

```bash
set -euo pipefail
: "${KOVA_WORKER_POOL_ID:?supply the externally assigned dedicated worker pool identity}"
: "${KOVA_RUNNER_IMAGE:?supply the reviewed repository@sha256 manifest reference}"
helm show crds oci://ghcr.io/cofy-x/charts/kova \
  --version "${KOVA_CHART_VERSION}" | kubectl apply -f -
umask 077
export KOVA_RUNNER_NAMESPACE=kova-jobs-$(openssl rand -hex 6)
export KOVA_RECEIPT_NAMESPACE=kova-receipts-$(openssl rand -hex 6)
genesis_directory=$(mktemp -d)
genesis_generation=$(openssl rand -hex 16)
kubectl create namespace "${KOVA_RUNNER_NAMESPACE}" -o json >"${genesis_directory}/namespace.json"
kubectl create namespace "${KOVA_RECEIPT_NAMESPACE}" -o json >"${genesis_directory}/receipt-namespace.json"
genesis_inputs=(
  --namespace-uid "$(jq -r .metadata.uid "${genesis_directory}/namespace.json")"
  --receipt-namespace "${KOVA_RECEIPT_NAMESPACE}"
  --receipt-namespace-uid "$(jq -r .metadata.uid "${genesis_directory}/receipt-namespace.json")"
  --worker-pool-id "${KOVA_WORKER_POOL_ID}" --runner-image "${KOVA_RUNNER_IMAGE}"
  --generation "${genesis_generation}"
  --max-active-jobs 20 --max-active-jobs-per-requester 4
  --worker-slots 20 --max-queued-jobs 1000 --max-queued-jobs-per-requester 100
)
kova --kubeconfig "${KOVA_KUBECONFIG}" --namespace "${KOVA_RUNNER_NAMESPACE}" \
  admission-genesis render-genesis "${genesis_inputs[@]}" >"${genesis_directory}/genesis.json"
kubectl create -f "${genesis_directory}/genesis.json" -o json >"${genesis_directory}/genesis-created.json"
kova --kubeconfig "${KOVA_KUBECONFIG}" --namespace "${KOVA_RUNNER_NAMESPACE}" \
  admission-genesis export-receipt-secret "${genesis_inputs[@]}" \
  --genesis-uid "$(jq -r .metadata.uid "${genesis_directory}/genesis-created.json")" \
  --service-namespace kova --secret-name kova-admission-receipt >"${genesis_directory}/receipt.json"
kubectl create -f "${genesis_directory}/receipt.json" -o json >"${genesis_directory}/receipt-created.json"
genesis_secret_uid=$(jq -r .metadata.uid "${genesis_directory}/receipt-created.json")
```

Install the public OCI chart using those original identities:

```bash
helm upgrade --install kova oci://ghcr.io/cofy-x/charts/kova \
  --version "${KOVA_CHART_VERSION}" \
  --namespace kova \
  --create-namespace \
  --set serviceDaemon.enabled=true \
  --set-string "serviceDaemon.runnerNamespace=${KOVA_RUNNER_NAMESPACE}" \
  --set-string "serviceDaemon.workerPoolID=${KOVA_WORKER_POOL_ID}" \
  --set-string "images.runner.repository=${KOVA_RUNNER_IMAGE%@*}" \
  --set-string "images.runner.digest=${KOVA_RUNNER_IMAGE##*@}" \
  --set serviceDaemon.admissionGenesis.enabled=true \
  --set-string "serviceDaemon.admissionGenesis.recoveryReceiptNamespace=${KOVA_RECEIPT_NAMESPACE}" \
  --set serviceDaemon.admissionGenesis.receiptSecret.name=kova-admission-receipt \
  --set-string "serviceDaemon.admissionGenesis.receiptSecret.uid=${genesis_secret_uid}" \
  --set serviceDaemon.authentication.mode=static \
  --set serviceDaemon.authentication.staticPrincipal=kova:quickstart \
  --set serviceDaemon.authentication.staticTokenSecret.name=kova-service-auth \
  --set-string worker.platform="${KOVA_PLATFORM}" \
  --wait
```

The explicit CRD apply is required on upgrades because Helm does not update
files in a chart's `crds/` directory.

Grant the quick-start principal permission to submit through the Service. This
Role does not grant direct access to `KovaBuild`, Pods, or Secrets:

```bash
kubectl -n "${KOVA_RUNNER_NAMESPACE}" create rolebinding kova-quickstart \
  --role=kova-service-submitter \
  --user=kova:quickstart
```

The installation creates the Service controller and one rootless BuildKit worker.
The worker pool identity names the environment's capacity domain; it is not a BuildKit address or proof that old workers have stopped.
Resolve the runner digest from the reviewed registry artifact, never from `docker image inspect .Id` (an image configuration digest).
Published charts bind controller, runner, and worker image tags to the same Kova release automatically.
Kova needs no object store or shared PVC.

Verify the installation:

```bash
kubectl -n kova rollout status deployment/kova-service
kubectl -n kova rollout status deployment/kova
kubectl -n kova get pods,service
```

## Verify the CLI

The CLI installed before Genesis provisioning must still match the runtime release:

```bash
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
for namespace in kova "${KOVA_RUNNER_NAMESPACE}"; do
  kubectl -n "${namespace}" create secret docker-registry kova-registry \
    --docker-server REGISTRY_HOST \
    --docker-username REGISTRY_USERNAME \
    --docker-password REGISTRY_PASSWORD
done
```

Keep the command out of shared shell history in real environments. Prefer the
environment's external secret controller for durable credentials.

Set the Secret name, or leave it empty for an anonymous development registry:

```bash
export KOVA_REGISTRY_SECRET=kova-registry
export KOVA_TARGET=REGISTRY_HOST/kova-quickstart/hello:dev
export KOVA_SOURCE_REPOSITORY=REGISTRY_HOST/kova-quickstart/source:dev
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

kova job submit \
  --source-repository "${KOVA_SOURCE_REPOSITORY}" \
  --target "${KOVA_TARGET}" \
  --platform "${KOVA_PLATFORM}" \
  --format oci \
  --concurrency 1 \
  --fail-fast \
  .work/kova-quickstart
```

Copy the returned job ID, then inspect the complete lifecycle:

```bash
kova job wait <job-id> --timeout 10m
kova job get <job-id>
kova job results <job-id>
kova job logs <job-id>
```

Pull `${KOVA_TARGET}` as an additional registry-path check when the workstation can reach the target registry.
The successful result is the verified manifest digest returned by `kova job results`.
Logs are available only while the runner Pod is active; long-term log collection belongs to the caller.

The [authenticated Service workflow](service.md) covers TokenReview, immutable source bundles, cancellation, bounded target batches, and Nydus output. The
[direct runner workflow](cli-workflow.md) is reserved for development and
low-level debugging. The
[Kubernetes deployment guide](deployment/kubernetes.md) covers private registry credentials, service mode, capacity, and production overlays.

## Uninstall

Stop submissions and drain all builds and runners before removing their controller or workers.
Preserve caller-owned results and the installation receipts first.
After the [drain gate](deployment/kubernetes.md#cross-version-service-upgrade) passes:

```bash
helm uninstall kova --namespace kova
```

The separate runner and recovery-receipt namespaces, original Genesis, and receipts remain as evidence.
An environment owner may dispose of them only after verifying their original UIDs and resolving every unknown operation.
Do not reuse either namespace name for a later installation.

The chart does not create clusters, cloud accounts, output registries, or
provider credentials. Those resources remain owned by the consuming
environment.

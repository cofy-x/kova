# Kubernetes Deployment

The Helm chart deploys rootless BuildKit workers, a headless discovery
Service, and an optional Kova controller. It contains no cloud-provider
accounts, cluster lifecycle logic, registry provisioning, or load-balancer
policy; those inputs belong to the consuming environment.

## Prerequisites

- a Kubernetes cluster that permits the rootless BuildKit security profile
- Helm and credentials allowed to install the release
- an OCI registry reachable from runner and worker Pods
- published controller, runner, and worker images for the cluster architecture
- external Secrets for private images and source or output registries

Rootless BuildKit runs as UID/GID 1000 without a privileged container. It uses
unconfined seccomp and AppArmor plus `--oci-worker-no-process-sandbox`, as
required by the upstream rootless runtime. Controller and runner containers
run as UID/GID 65532 with all capabilities dropped.

## Local Kind

```bash
make kind-create
make image
make deploy-kind
```

The [Kind deployment values](../../deploy/kind-values.yaml) use three local
role tags and the registry mapping described in the
[local registry guide](local-kind-registry.md).

## Cluster Installation

Install an exact public OCI chart and add an environment-owned values file when
the defaults need to change. For an existing Service release crossing the
runner protocol boundary, complete the [drain procedure](#cross-version-service-upgrade)
before running this installation sequence:

```bash
export KOVA_VERSION=vX.Y.Z

helm show crds oci://ghcr.io/cofy-x/charts/kova \
  --version "${KOVA_VERSION#v}" | kubectl apply -f -
./scripts/deployment/verify-kovabuild-crd.sh
```

Run the gate from the matching Kova checkout, using the same `KUBECONFIG` as
the Helm upgrade. Proceed only if it exits zero; it blocks when the CRD is not
Established, cannot be read, or lacks the `v1alpha1` status retry fields,
`Verifying` phase, and bounded verification receipt/timing schema.
Then upgrade the controller:

```bash
helm upgrade --install kova oci://ghcr.io/cofy-x/charts/kova \
  --version "${KOVA_VERSION#v}" \
  --namespace kova \
  --create-namespace \
  --set-string worker.platform=linux/amd64 \
  -f <environment-values.yaml>
```

### Cross-Version Service Upgrade

This drain is mandatory when crossing from `v0.1.0-rc.9` to a controller that
requires idempotent runner submission and immutable result digests. Do not
upgrade while old Starting, Running, or otherwise nonterminal builds remain:
the old runner rejects the new `request-id` query and does not export the
digest required for result verification. The new controller detects an idle
runner that does not advertise `idempotent-build-request-v1` and fails its
KovaBuild with `RunnerProtocolIncompatible` before POST, but that fail-closed
safety net does not make an in-flight cross-version build safe to migrate.
An old already-running runner can still be observed without resubmission;
its result is subject to the new controller's verification contract and may
fail. Investigate and resolve unexpected old builds rather than bypassing the
drain gate or deleting their status to make it pass.

The admission-ledger protocol also requires a fresh runner namespace. Old
Service replicas do not honor its queue intents or Pod Create nonces, and a
late old request may still reach the API server after the old Pod exits. Keep
the Service Deployment in its existing namespace, but set
`serviceDaemon.runnerNamespace` to a previously absent namespace for the new
release. All new replicas must use the same limits and runner namespace.
Do not copy old KovaBuilds or ledgers into it, or roll old and new Service Pods
together.

Freeze all Service submitters first. While the old controller is still running,
wait for all KovaBuilds and their runner Pods to be removed. Preserve any
needed terminal receipts outside Kova, then wait through the configured
`jobTTL`; if retention is disabled, explicitly remove old terminal resources
only after confirming the caller no longer needs them. A terminal result from
the old controller may contain a digest read from a mutable tag, so it cannot
be served under the new exact-push-digest guarantee. Then scale the old
Service Deployment to zero, wait until all old
Service Pods are gone, and run the drain gate. Keep submitters frozen and the
old controller stopped while applying and verifying the new CRD; run the drain
gate again immediately before upgrading the chart. After the new Service
rollout is ready, resume submitters.

```bash
export NAMESPACE=kova RUNNER_NAMESPACE=kova RELEASE_NAME=kova
# Inspect until there are no KovaBuilds (including terminal receipts) or runner Pods.
kubectl -n "${RUNNER_NAMESPACE}" get kovabuilds,pods
kubectl -n "${NAMESPACE}" scale deployment/kova-service --replicas=0
kubectl -n "${NAMESPACE}" rollout status deployment/kova-service
./scripts/deployment/verify-kovabuild-drained.sh

# Apply and verify the CRD as shown above while the old Service remains stopped.
./scripts/deployment/verify-kovabuild-crd.sh
./scripts/deployment/probe-kovabuild-status.sh --expect-persisted
./scripts/deployment/verify-kovabuild-drained.sh
# Create an unused namespace and set serviceDaemon.runnerNamespace to it in
# the Helm upgrade values; do not reuse the old runner namespace.
export NEW_RUNNER_NAMESPACE=kova-runner-v2
kubectl create namespace "${NEW_RUNNER_NAMESPACE}"
# Now run the helm upgrade shown above with:
# --set-string "serviceDaemon.runnerNamespace=${NEW_RUNNER_NAMESPACE}"
# Then verify the new Service and resume submissions:
kubectl -n "${NAMESPACE}" rollout status deployment/kova-service
```

Run the drain gate from the matching checkout and `KUBECONFIG`; it takes
`NAMESPACE`, `RUNNER_NAMESPACE` (when `serviceDaemon.runnerNamespace` differs),
and `RELEASE_NAME`, all defaulting to `kova`. Set `SERVICE_DEPLOYMENT_NAME` when
the chart uses `fullnameOverride`; by default the expected Deployment is
`<release>-service`. It blocks on any KovaBuild (including terminal receipts),
any runner Pod, any Service Pod, a missing or ambiguous named Service
Deployment, a Service Deployment with replicas, or an unreadable Kubernetes
response. It is a point-in-time
check, not a submission lock. The Helm upgrade should set the new Service
replica count to the intended positive value; the old scale-down must not be
carried into its values. If a separate operator or autoscaler can restart the
old Service, suspend that controller for this maintenance window as well.
The example scale command assumes the chart's default fullname; adjust it and
`SERVICE_DEPLOYMENT_NAME` when the release uses `fullnameOverride`.

Apply the CRD for every selected release before the Helm upgrade. Helm creates
objects from `crds/` during initial installation but does not upgrade them.
Verify both the live CRD schema and an actual `/status` write/read round trip
for `pollFailureSince`, `pollFailureCount`, `Verifying`, and all bounded
verification timing/receipt fields before starting the new controller. The
schema check alone does not prove that the API serving path has picked up the
new schema; an older serving schema can prune retry/deadline/receipt state and
prevent bounded recovery. The probe uses a dedicated, previously absent
namespace and removes it afterward. Keep the old Service stopped and direct
submissions frozen through both checks.
The optional `BASELINE_CHART` path in `scripts/e2e/e2e-service.sh` exercises this
order with an older chart before upgrading to the current controller; run
`./scripts/deployment/test-verify-kovabuild-crd.sh` for a cluster-free gate test.
For a live, isolated old-CRD-to-new-controller smoke, see
[CRD upgrade testing](../testing.md#crd-upgrade-smoke).

Replace `vX.Y.Z` with an exact tag from the
[GitHub release page](https://github.com/cofy-x/kova/releases). Keep the same
value for the CLI and runtime images used with this deployment.

The packaged chart automatically selects controller, runner, and worker image
tags from its application version. Override a role only when an environment
mirrors or pins the published image:

```yaml
images:
  controller:
    repository: ghcr.io/cofy-x/kova
    tag: controller-vX.Y.Z
  runner:
    repository: ghcr.io/cofy-x/kova
    tag: runner-vX.Y.Z
  worker:
    repository: ghcr.io/cofy-x/kova
    tag: worker-vX.Y.Z
```

The default chart mode installs one worker only. Use the
[provider-neutral production baseline](../../deploy/production-values.yaml) as
a starting point for capacity and availability settings. Enable
`serviceDaemon` for an authenticated HTTP API and managed `KovaBuild` jobs.

## Source and Result Storage

Kova reads immutable source bundles and pushes results through OCI registries.
It does not require an object store, shared filesystem, or RWX PVC.
Each runner materializes a digest-verified source into job-local `emptyDir` storage.
The source volume has a 4 GiB default and minimum `sizeLimit`; it also backs runner `/tmp`, where the uploaded zip copy and extracted context live.
The immutable source archive and its runner upload copy each use at most 512 MiB; the extracted job-local tree uses at most 2 GiB and is prepared in place, not copied again.
The runner retains at most 256 MiB of ephemeral failure logs, leaving about 768 MiB of the default source-volume budget for result state, command metadata, and filesystem overhead at the source limits.
Top-level `Dockerfile` and `metadata.json` files are each limited to 1 MiB before and after variable substitution; captured command output is limited to the latest 1 MiB per target.
Nydus conversion and unusually large build-tool scratch files can need more headroom; measure their peak usage and raise both the source volume and runner ephemeral-storage request/limit together for such workloads.
Each runner defaults to 5 GiB ephemeral storage and 2 GiB memory limits; its source fetch init container defaults to 1 GiB ephemeral storage and 1 GiB memory limits.
Their memory and ephemeral-storage requests equal their limits, so scheduling reserves the full per-job capacity.
Set `serviceDaemon.runnerResources`, `serviceDaemon.sourceFetchResources`, and `serviceDaemon.sourceVolumeSizeLimit` for the runner node pool, keeping the runner ephemeral storage limit above the source volume limit.
Before increasing active jobs, account for the sum of runner requests and possible per-Pod limits on every eligible node; Kova does not reserve the full limit in advance.
Kubernetes [local ephemeral storage enforcement](https://kubernetes.io/docs/concepts/storage/ephemeral-storage/) depends on kubelet accounting and node filesystem layout, so the source byte and entry limits remain enforced by Kova itself.
Source and output retention are controlled by registry policy or the caller.

## Authentication

TokenReview is the default service mode. The chart creates only the RBAC needed
to submit TokenReview and SubjectAccessReview requests. It also creates
unbound `kova-service-submitter` and `kova-service-admin` Roles. These Roles
authorize the virtual Service API resource and do not grant direct CRD access.
Bind users or groups to the submitter Role to create jobs and manage only their
own jobs; bind platform operators to the admin Role for namespace-wide access.
Static authentication is available when an external Secret is more appropriate
and maps the token to `serviceDaemon.authentication.staticPrincipal`.
`unsafe-none` must be selected explicitly and should never be exposed outside
an isolated development cluster.

See the [Service contract guide](../service.md) for complete values.

## Registry Credentials

Reference an externally managed `kubernetes.io/dockerconfigjson` Secret:

```yaml
imagePullSecrets:
  create: false
  name: kova-registry
```

Place the Secret in both the release namespace and runner namespace when they
differ. Service mode uses the same Secret to verify output image descriptors;
set `serviceDaemon.registrySecret` to use a different Docker config Secret in
the release namespace. The chart can render credentials from
`imageRegistries`, but production environments should keep secrets out of Helm
values and release history.

Registry transport is HTTPS by default. Only isolated development registries
that do not support TLS should be listed under
`serviceDaemon.registryPlainHTTP`.

One build may push targets to multiple registries when this Docker config, network policy, and TLS configuration authorize every destination.
Those pushes are not transactional: a terminally failed build can retain verified digests for outputs that were already pushed successfully.

## Capacity And Placement

Each chart release is one explicit BuildKit platform pool. Set `worker.platform` to `linux/amd64` or `linux/arm64`; the chart derives standard `kubernetes.io/os=linux` and `kubernetes.io/arch` placement and rejects conflicting node selectors. It does not use provider-specific labels or infer the platform from controller placement.

Worker replicas are shared BuildKit capacity. Direct CLI jobs set their own
`build --concurrency`. Service jobs reserve slots through fair admission:

```yaml
serviceDaemon:
  maxActiveJobs: 20
  maxActiveJobsPerRequester: 4
  maxQueuedJobs: 1000
  maxQueuedJobsPerRequester: 100
  workerSlots: 40
```

The controller interleaves queued jobs by requester, allocates available worker
slots without leaving usable capacity idle, and records each fixed allocation
in job status. Runners resolve the headless Service into worker Pod IPs, avoid
busy or cooling endpoints, and refresh DNS as replicas change.

Active grants and HTTP queue intents use separate Kubernetes ConfigMap CAS ledgers. The active grant precedes Pod Create; a unique in-flight nonce fences a late old-leader Create. HTTP submission reserves one bounded queue intent before CR Create, and active grant commits before that intent is released. Do not delete either `kova-service-admission` or `kova-service-queue-admission` while the Service is running. Direct/admin CR writes are outside the HTTP queue quota, though active limits still apply. Unknown Create results may retain capacity until exact operator evidence resolves them.

This protocol **requires a stop-and-drain upgrade**: stop submissions, finish/delete old queued and active builds, verify all old runner Pods are gone, stop every old controller and HTTP replica, apply the updated CRD, and start the new version with a new runner namespace and fresh ledgers. A mixed-version or in-place rolling upgrade can bypass the fence. See the [admission design and recovery rules](../service-admission-design.md); real-apiserver migration validation is still required before claiming a deployed strict quota.

A Service that accepts both platforms maps each platform to an explicit BuildKit Service. The additional worker pool can be a separate Helm release with its Service endpoint listed in the Service release:

```yaml
serviceDaemon:
  buildkitPlatformAddrs:
    linux/amd64: tcp://kova-amd64.kova.svc:9094
    linux/arm64: tcp://kova-arm64.kova.svc:9094
```

One logical target remains single-platform in v1. Kova does not build a multi-platform index. A supported target platform without a configured pool fails deterministically before BuildKit submission.

The chart exposes worker resources, topology spread, disruption budget, HPA,
node selectors, tolerations, affinity, priority class, and runtime class.
Environment overlays should set these according to cluster policy.

### Worker Cache Capacity

The chart now renders the BuildKit OCI worker GC budget from `worker.cache` instead of relying on BuildKit's filesystem-sized defaults.
The default and production starting point reserve 2 GB, target at most 8 GB of reclaimable cache, and target 2 GB of filesystem free space; the 8 GiB kind overlays use a 3 GB maximum.
These are absolute BuildKit `GB` values, not percentages of the node filesystem or the Kubernetes `emptyDir`; BuildKit v0.31.2 reports `8GB` as about 8 GiB.

```yaml
worker:
  cache:
    reservedSpaceGB: 2
    maxUsedSpaceGB: 8
    minFreeSpaceGB: 2
```

The chart requires exactly one bounded `lib-buildkit` `emptyDir` volume and an explicit worker ephemeral-storage limit. It rejects a reserved budget at or above the maximum and a maximum above 80% of either limit.
For this check, cache volume and ephemeral-storage quantities use integer `Mi`, `Gi`, or `Ti` values.
Keep additional space for in-flight build references, the writable container layer, logs, and filesystem metadata; BuildKit GC can reclaim only eligible unused records and is not a hard disk or memory limit.
Set worker CPU and memory requests/limits and admission slots from measured build costs and node headroom, rather than treating the GC budget as a substitute for resource isolation.

`buildkitdConfig` remains available for non-OCI-worker settings such as registry mirrors, but must not declare `[worker.oci]` or its subtables; the chart owns that section to keep GC policy explicit.
Existing environment overlays that set `[worker.oci]` in raw TOML must move their GC values to `worker.cache` before upgrading.
BuildKit's [daemon configuration](https://github.com/moby/buildkit/blob/v0.31.2/docs/buildkitd.toml.md) documents these GC controls.

A cache-budget change updates the worker Pod template checksum and rolls the Deployment.
Because each worker uses Pod-scoped `emptyDir`, replacement discards that worker's cache and may temporarily increase cold-build latency.
Before rollout, record per-worker `buildctl du`, cache directory size, cgroup `file` and `anon` memory, node memory/disk, and active builds; roll with spare capacity and verify that cache usage plateaus below the guard under sustained unique-target builds.
Validate cache-hit latency and OCI/Nydus correctness before increasing concurrency.

Worker ingress is restricted to Kova runner Pods by default. Add trusted peers
only when another component intentionally calls BuildKit. Service ingress is
opt-in because gateway topology differs by environment; enabling it with an
empty peer list denies all ingress:

```yaml
networkPolicy:
  worker:
    enabled: true
    additionalIngressFrom: []
  service:
    enabled: true
    ingressFrom:
      - namespaceSelector:
          matchLabels:
            kubernetes.io/metadata.name: platform-clients
```

## Direct Runner Lifecycle

The CLI can create a short-lived runner without the service API:

```bash
kova --kubeconfig <kubeconfig> \
  --name <runner-name> \
  prepare \
  --image "ghcr.io/cofy-x/kova:runner-${KOVA_VERSION}"
```

Delete it after the batch:

```bash
kova --kubeconfig <kubeconfig> --name <runner-name> destroy
```

This path is useful for development and explicit CI control. Platform
integrations should prefer the authenticated service and immutable job API.

## Validation

Render an environment overlay before installation:

```bash
helm template kova oci://ghcr.io/cofy-x/charts/kova \
  --version "${KOVA_VERSION#v}" \
  --namespace kova \
  -f <environment-values.yaml>
```

After installation:

```bash
kubectl -n kova rollout status deployment/kova
kubectl -n kova get pods,svc
```

The [validation matrix](../testing.md) describes local E2E coverage. The
[Dragonfly and Nydus guide](../runtime/dragonfly-nydus.md) documents optional
runtime integration.

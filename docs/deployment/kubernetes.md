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
- external Secrets for private images, output registries, or artifact storage

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

Start with the
[provider-neutral deployment baseline](../../deploy/kubernetes-values.yaml)
and add an environment-owned overlay:

```bash
helm upgrade --install kova ./charts/kova \
  --namespace kova \
  --create-namespace \
  -f deploy/kubernetes-values.yaml \
  -f <environment-values.yaml>
```

Set each role explicitly:

```yaml
images:
  controller:
    repository: ghcr.io/cofy-x/kova
    tag: controller-v0.1.0
  runner:
    repository: ghcr.io/cofy-x/kova
    tag: runner-v0.1.0
  worker:
    repository: ghcr.io/cofy-x/kova
    tag: worker-v0.1.0
```

The default chart mode installs workers only. Enable `serviceDaemon` for a
stable authenticated HTTP API and managed `KovaBuild` jobs.

## Artifact Storage

Production service deployments should use an S3-compatible artifact store:

```yaml
artifactStore:
  driver: s3
  secretName: kova-artifact-credentials
  s3:
    endpoint: objects.example.com
    bucket: kova-builds
    region: us-east-1
    secure: true
```

The external Secret uses `KOVA_S3_ACCESS_KEY`, `KOVA_S3_SECRET_KEY`, and an
optional `KOVA_S3_SESSION_TOKEN`. The controller and source-fetch init
containers receive the Secret through environment references.

Filesystem storage remains available for local or deliberately shared-volume
environments. A ReadWriteOnce PVC may require matching
`serviceDaemon.nodeSelector` and `serviceDaemon.runnerNodeSelector` values.

## Authentication

TokenReview is the default service mode. The chart creates only the RBAC needed
to submit TokenReview requests. Static authentication is available when an
external Secret is more appropriate. `unsafe-none` must be selected explicitly
and should never be exposed outside an isolated development cluster.

See the [service API and storage guide](../service.md) for complete values.

## Registry Credentials

Reference an externally managed `kubernetes.io/dockerconfigjson` Secret:

```yaml
imagePullSecrets:
  create: false
  name: kova-registry
```

Place the Secret in both the release namespace and runner namespace when they
differ. The chart can render credentials from `imageRegistries`, but production
environments should keep secrets out of Helm values and release history.

## Capacity And Placement

Worker replicas are shared BuildKit capacity. Direct CLI jobs set their own
`build --concurrency`. Service jobs additionally use FIFO admission:

```yaml
serviceDaemon:
  maxActiveJobs: 20
  workerSlots: 40
```

The controller divides worker slots across admitted jobs and records the
allocation in job status. Runners resolve the headless Service into worker Pod
IPs, avoid busy or cooling endpoints, and refresh DNS as replicas change.

The chart exposes worker resources, topology spread, disruption budget, HPA,
node selectors, tolerations, affinity, priority class, and runtime class.
Environment overlays should set these according to cluster policy.

## Direct Runner Lifecycle

The CLI can create a short-lived runner without the service API:

```bash
kova --kubeconfig <kubeconfig> \
  --name <runner-name> \
  prepare \
  --image ghcr.io/cofy-x/kova:runner-v0.1.0
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
helm template kova ./charts/kova \
  --namespace kova \
  -f deploy/kubernetes-values.yaml \
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

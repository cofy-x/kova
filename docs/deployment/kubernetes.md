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

Install an exact public OCI chart and add an environment-owned values file when
the defaults need to change:

```bash
export KOVA_VERSION=vX.Y.Z

helm show crds oci://ghcr.io/cofy-x/charts/kova \
  --version "${KOVA_VERSION#v}" | kubectl apply -f -
helm upgrade --install kova oci://ghcr.io/cofy-x/charts/kova \
  --version "${KOVA_VERSION#v}" \
  --namespace kova \
  --create-namespace \
  -f <environment-values.yaml>
```

Apply the CRD for every selected release before the Helm upgrade. Helm creates
objects from `crds/` during initial installation but does not upgrade them.

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
[provider-neutral production baseline](../../deploy/kubernetes-values.yaml) as
a starting point for capacity and availability settings. Enable
`serviceDaemon` for an authenticated HTTP API and managed `KovaBuild` jobs.

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
to submit TokenReview and SubjectAccessReview requests. It also creates
unbound `kova-service-submitter` and `kova-service-admin` Roles. Bind users or
groups to the submitter Role to create jobs and manage only their own jobs;
bind platform operators to the admin Role for namespace-wide access. Static
authentication is available when an external Secret is more appropriate and
maps the token to `serviceDaemon.authentication.staticPrincipal`.
`unsafe-none` must be selected explicitly and should never be exposed outside
an isolated development cluster.

See the [service API and storage guide](../service.md) for complete values.

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

Enable `networkPolicy` only with explicit ingress peers. An empty peer list
denies ingress to the selected worker or Service endpoint:

```yaml
networkPolicy:
  enabled: true
  workerIngressFrom:
    - podSelector:
        matchLabels:
          app.kubernetes.io/name: kova-runner
  serviceIngressFrom:
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

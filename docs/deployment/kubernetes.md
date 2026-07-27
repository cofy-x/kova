# Kubernetes Deployment

Kova is deployed with Helm. A deployment consists of BuildKit worker Pods, a
headless Service used for worker discovery, and optional service-daemon,
registry, scheduling, autoscaling, and observability configuration.

## Prerequisites

- a Kubernetes cluster whose nodes can run privileged BuildKit containers
- Helm 3 and cluster credentials with permission to install the release
- an OCI registry reachable from workers and runner Pods
- a published Kova runtime image for the cluster architecture
- a Kubernetes image pull Secret when the runtime image is private

The HTTP service daemon additionally requires a source PVC. Use ReadWriteMany
storage for multiple service replicas; ReadWriteOnce is suitable only when the
service and all runner Pods are pinned to one node.

## Local kind Deployment

The local kind environment is the default development deployment.

```bash
make kind-create
make image
make deploy-kind
```

The [local Kind configuration](../../deploy/kind-values.yaml) is tuned for a
multi-worker development cluster:

- multiple `buildkitd` replicas
- `imagePullPolicy: IfNotPresent`
- local registry image `localhost:5002/kova:dev`
- HTTP registry configuration for the local `host.docker.internal:5002`
  development registry

Use [Local Kind Registry](local-kind-registry.md) for the registry address
mapping.

The same runtime image is used for Helm-managed worker Pods and short-lived
runner Pods. Helm sets the worker Pod command to `buildkitd`, while
`kova prepare --image <image>` creates a runner Pod from that image with
the command `kovad daemon`.

## Install on a Kubernetes Cluster

Use the [production deployment baseline](../../deploy/kubernetes-values.yaml)
for a cluster. Supply a separate values overlay for the target
registry, storage classes, scheduling policy, resource limits, and ingress or
load-balancer configuration.

Common overrides:

- image repository and tag
- worker replica count
- CPU and memory requests/limits
- registry auth secret name
- registry auth entries
- tolerations, affinity, and scheduling constraints when needed
- Pod disruption budget and autoscaling policy when needed
- service account settings when the cluster requires a dedicated identity

Example:

```bash
helm upgrade --install kova ./charts/kova \
  --namespace kova \
  --create-namespace \
  -f deploy/kubernetes-values.yaml \
  --set image.repository=<registry>/<repo>/kova \
  --set image.tag=<tag>
```

The default chart mode installs shared BuildKit workers. Operators and CI jobs
use the Kova CLI to create one short-lived runner Pod per build batch. Enable
`serviceDaemon` when clients need a stable HTTP API that creates and manages
runner Pods on their behalf.

## Registry Credentials

For a private runtime image, provision a `kubernetes.io/dockerconfigjson`
Secret named `kova-registry` in the release namespace with your secret manager
or Kubernetes deployment tooling. Reference it without transferring
credentials through Helm:

```yaml
imagePullSecrets:
  create: false
  name: kova-registry
```

Runner Pods must be able to pull the Kova runtime image and authenticate to
every private output registry used by a build. Place the referenced Secret in
the runner namespace as well as the Helm release namespace when they differ.
The chart can create a Secret from `imageRegistries`, but an externally managed
Secret is preferred for production because credentials do not enter Helm
values or release history.

## Production Configuration

The chart keeps production controls in values instead of hard-coding an
environment:

- `resources`: CPU, memory, and ephemeral storage requests and limits
- `container.livenessProbe` and `container.readinessProbe`: BuildKit worker
  health checks
- `podSecurityContext` and `container.securityContext`: privileged BuildKit
  runtime settings and Pod-level security defaults
- `nodeSelector`, `tolerations`, `affinity`, and
  `topologySpreadConstraints`: worker placement across cluster nodes
- `podDisruptionBudget`: optional protection during node maintenance or
  voluntary disruptions
- `autoscaling`: optional HPA configuration for environments with metrics
  support
- `serviceAccount`: optional dedicated service account and token mounting

`deploy/kubernetes-values.yaml` enables a conservative PDB by default and
leaves HPA disabled until the target cluster has the metrics signal needed to
scale worker Pods predictably.

## Worker Scaling

Workers are the shared BuildKit capacity. Increase worker replicas when a batch
needs more concurrent build capacity.

```bash
kova --kubeconfig <kubeconfig> scale --target 6
```

The runner-level `build --concurrency` value controls how many build tasks a
single batch submits concurrently. In practice, scale worker replicas and build
concurrency together.

The `kova` Service is headless, so runner Pods resolve `kova.kova.svc` to the
ready worker Pod IPs and schedule build targets across those resolved
endpoints. Autoscaling can be enabled through chart values, but the local kind
deployment uses a fixed replica count.

## Runner Lifecycle

A runner should usually be created per build batch:

```bash
kova --kubeconfig <kubeconfig> \
  --name <runner-name> \
  prepare \
  --image <registry>/<repo>/kova:<tag>
```

`prepare` runs from the local `kova` CLI and uses the supplied kubeconfig to
create the runner Pod in the target cluster. That target can be the local kind
cluster or any Kubernetes cluster with compatible permissions. The image
passed to `--image` must be reachable from that cluster;
for local kind this is usually `localhost:5002/kova:dev`, while other
deployments should use a registry available from the cluster network.

Delete a runner when the batch is complete:

```bash
kova --kubeconfig <kubeconfig> \
  --name <runner-name> \
  destroy
```

Runner Pods own per-batch state such as `result.lmdb`, `logs.jsonl`, build
status, and exported result streams.

## Registry Configuration

The chart supports private registries through values and image pull secrets.
Keep registry-specific details in environment values files.

For remote Kubernetes environments:

- push the runtime image to the target registry before Helm deployment
- set the chart `image` value to that image
- configure image pull secrets if the cluster cannot pull anonymously
- pass build output registry information through metadata variables or runner
  build variables

For local kind, use the default local registry flow from `make image` and
`make deploy-kind`.

Cluster creation, registry provisioning, node bootstrap, storage classes, and
load-balancer annotations are inputs to the Kova release. Express them in the
target environment's infrastructure configuration and Helm values overlay;
the Kova chart itself remains Kubernetes- and OCI-registry-oriented.

## Validation

Render the release before installation:

```bash
helm template kova ./charts/kova \
  --namespace kova \
  -f deploy/kubernetes-values.yaml \
  -f <environment-values.yaml>
```

After installation, verify worker readiness and service discovery:

```bash
kubectl -n kova rollout status deployment/kova
kubectl -n kova get pods,svc
```

Local end-to-end targets are documented in the
[validation matrix](../testing.md).

```bash
make e2e
make e2e-concurrent
make e2e-dragonfly-nydus
make e2e-runtime
```

Use [Dragonfly And Nydus](../runtime/dragonfly-nydus.md) for the runtime
topology and registry mirror details behind the Nydus and Dragonfly targets.

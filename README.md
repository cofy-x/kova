<p align="center">
  <img src="./assets/readme/hero.svg" width="100%" alt="Kova — a Kubernetes-native image build service: the CLI submits jobs to the controller's KovaBuild API, a per-job runner drives shared rootless BuildKit workers, and images are pushed to an OCI registry and preheated over Dragonfly P2P.">
</p>

<p align="center">
  <a href="https://github.com/cofy-x/kova/actions/workflows/ci.yml"><img src="https://github.com/cofy-x/kova/actions/workflows/ci.yml/badge.svg" alt="CI"></a>
  <a href="LICENSE"><img src="https://img.shields.io/badge/license-Apache--2.0-blue.svg" alt="License: Apache-2.0"></a>
</p>

<p align="center">
  English | <a href="README.zh-CN.md">中文</a>
</p>

Kova turns batches of Dockerfile contexts into OCI or Nydus images on your own Kubernetes cluster.
Powered by BuildKit, it pushes results to any OCI registry and can preheat them across a Dragonfly P2P cluster — without tying you to a cloud provider.

## Quick start

You need a Kubernetes cluster, Helm with OCI support, and `kubectl`.
Choose a tag from [GitHub releases](https://github.com/cofy-x/kova/releases) so the chart, CLI, and runtime images stay aligned.
Provenance-attested CLI archives for Linux, macOS, and Windows are published there as well.

Install the CLI:

```bash
go install github.com/cofy-x/kova/cmd/kova@latest
kova version
```

Install the service.
The quick-start profile uses a generated static token and a filesystem PVC; shared environments should use TokenReview and S3-compatible storage instead:

```bash
export KOVA_VERSION=vX.Y.Z
export KOVA_SERVICE_TOKEN=$(openssl rand -hex 32)

kubectl create namespace kova --dry-run=client -o yaml | kubectl apply -f -
kubectl -n kova create secret generic kova-service-auth \
  --from-literal=token="${KOVA_SERVICE_TOKEN}"

helm show crds oci://ghcr.io/cofy-x/charts/kova \
  --version "${KOVA_VERSION#v}" | kubectl apply -f -
helm upgrade --install kova oci://ghcr.io/cofy-x/charts/kova \
  --version "${KOVA_VERSION#v}" \
  --namespace kova \
  --create-namespace \
  --set serviceDaemon.enabled=true \
  --set serviceDaemon.authentication.mode=static \
  --set serviceDaemon.authentication.staticPrincipal=kova:quickstart \
  --set serviceDaemon.authentication.staticTokenSecret.name=kova-service-auth \
  --set artifactStore.filesystem.pvc.create=true \
  --wait

kubectl -n kova create rolebinding kova-quickstart \
  --role=kova-service-submitter \
  --user=kova:quickstart
```

Applying the release CRD before every Helm upgrade is required because Helm does not upgrade files from a chart's `crds/` directory.

Run your first build:

```bash
kubectl -n kova port-forward service/kova-service 8080:8080 &

kova ctx set --mode service --service-url http://127.0.0.1:8080 --use quickstart
kova doctor
kova job submit ./image --target registry.example.com/team/image:dev
kova job wait <job-id>
kova job results <job-id>
```

Registry credentials, Nydus output, and batch archives are covered in the [installation and first-build guide](docs/quickstart.md).

## Why Kova

- **Batch in, images out** — one job builds many Dockerfile targets into OCI or Nydus images; typed per-target results and logs persist in the artifact store after the short-lived runner Pod is gone.
- **A real job model** — the `KovaBuild` CRD has an immutable spec, SHA-256-pinned source artifacts, and caller-scoped idempotency keys.
- **Fair, work-conserving scheduling** — queued jobs interleave by authenticated requester; admission reserves actual BuildKit worker slots.
- **Isolated execution** — one runner Pod per job drives shared upstream rootless BuildKit workers; controller and runner run as non-root with all capabilities dropped.
- **Kubernetes-native auth** — TokenReview and SubjectAccessReview by default; submitters never touch Pods, Secrets, or other users' jobs.
- **Cloud-provider-neutral** — registry, artifact, and API credentials are external Secret inputs. The chart creates no clusters, cloud accounts, or registries.
- **Observable** — stable OpenTelemetry metrics for queue delay, job duration, and capacity waits.

## Documentation

- [Documentation map](docs/README.md): choose the guide for a task.
- [Installation and first build](docs/quickstart.md): OCI chart, matching CLI, and a verified build.
- [Service job workflow](docs/service.md): identity, RBAC, contexts, artifact storage, and job operations.
- [CLI workflow](docs/cli-workflow.md): direct runner builds for development and low-level debugging.
- [Runtime design](docs/architecture.md): roles, topology, build/export, preheat, and scaling flows.
- [Kubernetes deployment](docs/deployment/kubernetes.md): registry credentials, worker sizing, and production configuration.
- [Release process](docs/releases.md): CLI archives, OCI charts, runtime images, SBOMs, and provenance.
- [Examples](examples/README.md): build input examples and runtime smoke services.

## Develop

The repository requires the Go version declared in `go.mod`, Docker, kind, Helm, kubectl, curl, zip, and LMDB development headers.

```bash
make test
make lint-scripts
make helm-template
make e2e-helm-quickstart   # released-chart install path on kind
```

Use the [validation matrix](docs/testing.md) to choose broader E2E coverage.
Contributions are welcome; the [contribution workflow](CONTRIBUTING.md) covers the full setup and pull request process.
Report vulnerabilities through the private process in the [security policy](SECURITY.md).

## License

Kova is licensed under the [Apache License 2.0](LICENSE).

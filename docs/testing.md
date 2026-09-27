# Testing

Kova uses layered checks. Run the narrowest check that matches a change, and run
the full runtime smoke after changing build, registry, Dragonfly, Nydus, or
container runtime behavior.

## Static Checks

```bash
go test ./...
make sdk-smoke
make python-sdk
make sdk-examples
make docs-check
make lint-scripts
make helm-template
git diff --check
```

`make sdk-smoke` compiles a clean external module against the public `pkg/api/v1` and `pkg/client` packages.
Tagged releases run the same consumer check against the exact module version and verify `go install github.com/cofy-x/kova/cmd/kova@vX.Y.Z`.
`make python-sdk` runs sync and async clients against a local fake HTTP server, checks Python/OpenAPI drift, lints and type-checks the package and Python Service SDK example, and validates the exact wheel and source-distribution contents.
CI executes this gate on the minimum supported Python 3.10 and current Python 3.14 runtimes.
`make sdk-examples` builds and truly executes the public Go and Python examples against a local fake Service.
It verifies immutable source and target requests, caller idempotency, stable multi-output receipts, server-returned immutable references, typed API errors, timeout, caller cancellation, terminal cancellation, partial failure, reference receipt shape, and bearer-token secrecy without Kubernetes or a registry.

## Network Overrides

Kova defaults to the official Ubuntu image, upstream GitHub release URLs,
`go.dev`, and `proxy.golang.org`. `scripts/build/build-image.sh` forwards the
standard `HTTP_PROXY`, `HTTPS_PROXY`, and `NO_PROXY` variables and accepts these
optional source overrides:

- `UBUNTU_IMAGE`
- `APT_MIRROR`
- `BUILDKIT_ROOTLESS_IMAGE`
- `GRPCURL_DOWNLOAD_BASE_URL`
- `NYDUS_DOWNLOAD_BASE_URL`
- `GO_DOWNLOAD_BASE_URL`
- `GOPROXY`

These inputs are generic build controls. Keep provider-specific mirror values
and environment policy in the consuming workspace rather than this repository.

## E2E Matrix

| Target | Coverage | Main Artifacts |
| --- | --- | --- |
| `make e2e` | Low-level zip-stream OCI build, push, export, and host pull. | `examples/simple`, `.work/result.jsonl` |
| `make e2e-helm-quickstart` | Packages the chart, installs it into an ephemeral minimal kind cluster, runs the authenticated Service workflow, and deletes the cluster on exit. Set `KEEP_KIND_CLUSTER=true` to retain a cluster created by the test, or `REUSE_KIND_CLUSTER=true` to explicitly use a caller-owned cluster that the test will not delete. | Helm archive, `examples/simple`, `.work/result-service.jsonl` |
| `make e2e-service-admission` | Read-only admission preflight by default; with `ADMISSION_E2E_MODE=run` and a test token, checks two Service Pods on an existing dedicated `kova-admission-*` Kind cluster, then sends a 40-way HTTP burst and cleans up only its exact CR IDs. Requires preinstalled active=1, global queue=3, requester queue=2, and `never=true` runner selector. | Existing dedicated Kind cluster; no install or cluster-wide cleanup |
| `make e2e-service-admission-failover` | Read-only preflight by default; live mode replaces only the verified current Service leader Pod, then proves a successor reconciles a new queued build without duplicating the original runner or active grant. Preserves run-scoped receipts on success and failure; exact two-CR cleanup only on success. | Same empty, dedicated two-replica Kind admission cluster and caps as above |
| `make e2e-source-capacity` | Read-only preflight by default; live mode publishes one 128 MiB incompressible immutable source, builds one OCI output, verifies its digest and host pull, and samples Kind node/runner state. Preserves run-scoped receipts and tags; never deletes the cluster or registry content. | Existing empty, dedicated `kova-source-capacity` Kind quickstart cluster and localhost registry |
| `make e2e-service` | RBAC isolation, immutable OCI source publication, pre-build target-and-platform contract failure, platform-scoped worker dispatch, safe caller retry, verified output platform and manifest digest, ephemeral logs, TTL cleanup, and host pull. | `examples/simple`, `.work/result-service.jsonl` |
| `make e2e-crd-upgrade` | Isolated pre-retry CRD to current CRD/controller migration, including live status pruning/persistence, a quiescence gate, and an injected old Starting runner that must fail before POST. | Public `v0.1.0-rc.9` chart and role images, dedicated Kind cluster and registry |
| `make e2e-release KOVA_VERSION=vX.Y.Z` | Downloads and verifies the exact public CLI and OCI chart, pulls matching public role images, then runs the immutable-source Service lifecycle in a clean kind cluster. | GitHub release files, public OCI packages, `.work/result-released.jsonl` |
| `make e2e-concurrent` | Multi-image OCI build with worker distribution checks. | generated concurrent examples, `.work/result-concurrent.jsonl` |
| `make e2e-dragonfly-nydus` | Nydus conversion, export, Dragonfly preheat, and Pod startup. | `examples/nydus-smoke`, `.work/result-nydus.jsonl` |
| `make e2e-runtime-preflight` | Local tool and registry readiness checks for runtime validation. | Docker, kind, Helm, kubectl, local registry |
| `make e2e-runtime` | OCI and Nydus service images running behind Kubernetes Services with in-cluster HTTP probes. | `examples/service-oci`, `examples/service-nydus`, runtime result JSONL files |
| `make e2e-observability` | Starts local Compose LGTM, runs a build, and checks Prometheus telemetry. | Grafana/LGTM, `examples/simple` |

Tune the concurrent check with `EXAMPLE_COUNT`, `BUILD_CONCURRENCY`, and
`MIN_BUILDKIT_NODE_IPS`.

## Isolated Source-Capacity Acceptance

Run this only on an isolated Linux Kind host after other Kind tests have released their clusters, with enough local storage, and never from a Mac or against a cloud kubeconfig.
It exercises a 128 MiB incompressible source, immutable OCI source fetch, and the private runner extraction path; the unit tests cover near-limit rejection and the absence of a second expanded-tree copy.
It does not establish the maximum safe production payload size.
Prepare the candidate images and isolated cluster using the ordinary quickstart, but give its setup outputs run-scoped tags:

```bash
SETUP_RUN=source-capacity-setup-$(date -u +%Y%m%dt%H%M%sz)
QUICKSTART_KIND_CLUSTER=kova-source-capacity KEEP_KIND_CLUSTER=true \
  SERVICE_TARGET="kind-registry:5000/kova-examples/source-capacity-setup:$SETUP_RUN" \
  SERVICE_PULL_TARGET="localhost:5002/kova-examples/source-capacity-setup:$SETUP_RUN" \
  SOURCE_REPOSITORY="localhost:5002/kova-sources/source-capacity-setup:$SETUP_RUN" \
  make e2e-helm-quickstart
```

The quickstart must complete and its terminal KovaBuilds must expire before the source-capacity preflight can pass.
The preflight verifies that the host has only the named Kind cluster, that its dedicated kubeconfig exactly matches the live Kind credentials/server, that both nodes and Kova deployments are healthy, that the registry image/port/network are exact, and that no KovaBuild or runner Pod is active.
It makes no cluster changes.

```bash
make e2e-source-capacity
SOURCE_CAPACITY_E2E_MODE=run SERVICE_AUTH_TOKEN=service-e2e-token make e2e-source-capacity
```

The live mode requires at least 20 GiB free disk, 1 GiB free temporary storage, and 8 GiB available RAM.
It uses the quickstart's `service-e2e-token` only through the process environment, never a command argument or evidence file.
It rejects existing source/output tags for its random run ID, then saves the source receipt, exact job ID, terminal and result receipts, live runner log and Pod samples, node health and Docker resource samples, events, and host-pull digest under `.work/source-capacity/<run-id>/`.
The expected outcome is one succeeded build with a verified manifest digest and a host pull of `localhost:5002/kova-examples/source-capacity:<run-id>` yielding that same digest.
On failure it retains all evidence and any exact-run KovaBuild and registry tags for inspection; no cluster-wide or repository-wide cleanup runs.
After evidence review, remove only the named `kova-source-capacity` Kind cluster and its kubeconfig if no longer needed; review run-scoped source/output tags before any registry cleanup.

## CRD Upgrade Smoke

Run `make e2e-crd-upgrade` only on a host where Docker and Kind are available.
It refuses to reuse an existing cluster, registry, or kubeconfig.
The default pinned old baseline is the public `v0.1.0-rc.9` chart at
`oci://ghcr.io/cofy-x/charts/kova` and its matching
`ghcr.io/cofy-x/kova:{controller,runner,worker}-v0.1.0-rc.9` images.
That CRD lacks `status.pollFailureSince`, `status.pollFailureCount`, and durable verification fields.
The candidate chart, CLI, and images come from the current checkout.
Only the candidate images are pushed, to the test-owned localhost registry;
the old GHCR role images are pulled, never pushed.
The baseline chart starts its old Service, then the test scales it to zero and
waits for its Pod to disappear before the new CRD and status checks.

The smoke installs the old chart, verifies that Kubernetes prunes retry and verification
status fields in an isolated probe namespace, applies the current CRD, waits
for Established, proves the new retry fields, verification receipts, and
`Verifying` phase persist through `/status`, and checks the old release is
drained. It then deliberately injects an old `Starting` KovaBuild and idle
`v0.1.0-rc.9` runner into the old namespace, proving the drain gate blocks
that state. The new controller starts only in a fresh runner namespace and
must leave the old fixture untouched before the test removes it. Finally it
runs authenticated OCI/Nydus Service E2E with current images. Production
upgrades must never bypass the drain gate; see the
[upgrade runbook](deployment/kubernetes.md#cross-version-service-upgrade).
The baseline rollout and Service E2E use the existing `BASELINE_CHART` path.

It owns only Kind cluster `kova-crd-upgrade`, registry container
`kind-registry-crd-upgrade` on host port `5003`, kubeconfig
`.kind/kova-crd-upgrade.kubeconfig`, Service port-forward `18081`, and the
temporary `kova-crd-upgrade-probe-*` namespaces inside that cluster.
On success it removes its cluster, registry, kubeconfig, and downloaded chart.
On failure it retains the cluster, registry, and kubeconfig for inspection;
`KEEP_KIND_CLUSTER=true` also retains them on success. Remove retained resources
only after inspection, with these exact commands from the Kova checkout:

```bash
kind delete cluster --name kova-crd-upgrade
docker rm -f kind-registry-crd-upgrade
rm -f .kind/kova-crd-upgrade.kubeconfig
```

The built `bin/kova` binary and candidate/result images may remain in the host
Docker image cache. The script does not run `make clean` or remove other Kind
clusters, registries, or host images.

Cluster-free gate tests are `./scripts/deployment/test-verify-kovabuild-crd.sh`,
`./scripts/deployment/test-verify-kovabuild-drained.sh`, and
`./scripts/deployment/test-probe-kovabuild-status.sh`.

## Runtime Smoke

`make e2e-runtime` is the strongest local validation target. It:

- verifies local tools and starts the local registry when needed
- builds and publishes the local controller, runner, and worker tags
- builds and publishes `localhost:5002/kova-examples/python-smoke-base:dev`
- deploys Kova workers
- installs Dragonfly and Nydus
- builds OCI and Nydus Python service images
- preheats both result sets through Dragonfly
- deploys both images as Kubernetes Deployments and Services
- validates both Services with in-cluster curl probes

The expected probe responses are:

```json
{"service":"kova-runtime-smoke","format":"oci","path":"/healthz","ok":true}
```

and:

```json
{"service":"kova-runtime-smoke","format":"nydus","path":"/healthz","ok":true}
```

## Generated Files

Runtime and E2E targets generate source archives and result JSONL files in the
repository root. These are ignored by git and can be removed with:

```bash
make clean
```

Use `make clean-kind` when the local kind cluster itself needs to be recreated.
Use `make diagnose-kind` to collect local registry, kind, Kova, Dragonfly/Nydus,
runtime smoke, event, log, and result summaries while debugging failures.

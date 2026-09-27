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
| `make e2e-service-admission` | Read-only admission preflight by default; with `ADMISSION_E2E_MODE=run` and a test token, checks two Service Pods on an existing dedicated `kova-admission-*` Kind cluster, then sends a 40-way HTTP burst, preserves run-scoped request/response receipts, and cleans up only its exact CR IDs. Requires preinstalled active=1, global queue=3, requester queue=2, and `never=true` runner selector. | Existing dedicated Kind cluster; no install or cluster-wide cleanup |
| `make e2e-service-admission-failover` | Read-only preflight by default; normal live mode proves leader handoff while a challenger remains queued. An explicit `ADMISSION_FAILOVER_PROMOTION=true` fixture additionally releases the exact blocker and verifies the challenger acquires the sole active grant and Pending runner. Receipts survive ambiguous outcomes. | Same empty, dedicated two-replica Kind admission caps; promotion is restricted further to the sole `kova-admission-pump` Kind on `wayne-hk-kvm`. |
| `make e2e-service-admission-fairness` | Read-only preflight by default; opt-in live mode uses two TokenReview-authenticated callers to prove requester and global queue caps, durable fair rotation across leader handoff, and exact UID cleanup. | Sole dedicated `kova-admission-fairness` Kind on `wayne-hk-kvm`; run receipts under `/data/forge-artifacts/kova-admission-fairness/`. |
| `make e2e-service-auth-read-probe` | Read-only preflight by default; opt-in bounded two-principal empty-list GET probe records TokenReview/SAR API counters, per-request latency, and Kind health without creating a KovaBuild. | Existing idle `kova-admission-fairness` Kind on `wayne-hk-kvm`; private JSON receipts under `/data/forge-artifacts/kova-admission-fairness/auth-read-*/`. |
| `make e2e-service-admission-ledger-loss` | Read-only preflight by default; live mode deliberately deletes only the exact active admission ConfigMap in a disposable dedicated Kind cluster, then proves both Service replicas return 503 for readiness and new submissions without creating a CR. Optional exact follower Pod replacement proves startup does not silently recreate the ledger. Preserves receipts and never repairs the fault. | Same empty, dedicated two-replica Kind admission cluster and caps as above; retire cluster after live mode |
| `make e2e-service-admission-deep-queue` | Read-only preflight by default; live mode holds one unschedulable blocker, grows a real API-server-backed queue through 100, 500, and 1000 CRs, records independent HTTP/API/resource receipts, and performs exact-ID cleanup only after every stage passes. | The only Kind cluster must be `kova-deep-queue`, with two ready Service replicas, empty ledgers/workloads, active=1, queued global/requester=1000, and `never=true` runner selector. |
| `make e2e-service-partial-output` | Read-only preflight by default; live mode executes a real two-target OCI+Nydus runner build whose later target fails in BuildKit, then checks final `Failed` and the first target's exact digest-pinned partial receipts. | Existing empty `kova-partial-output-41` Kind on `wayne-hk-kvm`; run-scoped source/output tags and private receipts under `.work/partial-output/`. |
| `make e2e-source-capacity` | Read-only preflight by default; live mode publishes one 128 MiB incompressible immutable source, builds one OCI output, verifies its digest and host pull, and samples Kind node/runner state. Preserves run-scoped receipts and tags; never deletes the cluster or registry content. | Existing empty, dedicated `kova-source-capacity` Kind quickstart cluster and localhost registry |
| `make e2e-source-oci-oversize` | Read-only preflight by default; live mode streams one exact 512 MiB + 1 byte sparse ZIP as an immutable OCI source, submits one public Service build, and requires an `InvalidSource` size-limit failure with no output tag. | Existing empty, dedicated `kova-source-capacity` Kind quickstart cluster and localhost registry |
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

## Two-Requester Admission Fairness on Kind

Issue #44's existing static-token admission tests have one authenticated username, so they cannot prove fairness or the global HTTP queue cap across two requesters.
This fixture uses two short TokenRequest bearers for the test-only `kova-fair-alice` and `kova-fair-bob` ServiceAccounts.
It runs only on `wayne-hk-kvm` after other Kind work has finished; the host must have exactly one Kind cluster, named `kova-admission-fairness`.
The script does not create the cluster, change Helm values, apply RBAC, edit either admission ledger, or push images.

Prepare a clean candidate checkout and build the three role images with revision tags on the KVM host using the existing image entrypoint.
After the exact tags are present locally, create the two-node Kind, push/load only those three candidate tags from KVM, install the current CRD and chart with `deploy/quickstart-kind-values.yaml` followed by `deploy/admission-fairness-kind-values.yaml`, and apply `deploy/admission-fairness-rbac.yaml`.
The chart must be installed with TokenReview authentication from its first Service startup, two Service replicas, active job/worker cap 1, global queue cap 3, requester queue cap 2, a two-hour build deadline, and the unmatched `never=true` runner selector.
For example, after confirming `kind get clusters` is empty on KVM:

```bash
fair_revision=$(git rev-parse --short=12 HEAD)
CONTROLLER_IMAGE=localhost:5002/kova:controller-${fair_revision} \
  RUNNER_IMAGE=localhost:5002/kova:runner-${fair_revision} \
  WORKER_IMAGE=localhost:5002/kova:worker-${fair_revision} \
  IMAGE_PLATFORM=linux/amd64 make image
KIND_CLUSTER=kova-admission-fairness KIND_CONFIG=deploy/quickstart-kind-cluster.yaml \
  KIND_WORKERS=1 KIND_KUBECONFIG=.kind/kova-admission-fairness.kubeconfig \
  CONTROLLER_IMAGE=localhost:5002/kova:controller-${fair_revision} \
  RUNNER_IMAGE=localhost:5002/kova:runner-${fair_revision} \
  WORKER_IMAGE=localhost:5002/kova:worker-${fair_revision} make kind-load
helm show crds charts/kova | kubectl --kubeconfig .kind/kova-admission-fairness.kubeconfig apply -f -
helm upgrade --install kova charts/kova \
  --kubeconfig .kind/kova-admission-fairness.kubeconfig --namespace kova \
  --create-namespace --wait --timeout 180s \
  -f deploy/quickstart-kind-values.yaml -f deploy/admission-fairness-kind-values.yaml \
  --set-string images.controller.tag=controller-${fair_revision} \
  --set-string images.runner.tag=runner-${fair_revision} \
  --set-string images.worker.tag=worker-${fair_revision} \
  --set-string worker.platform=linux/amd64
kubectl --kubeconfig .kind/kova-admission-fairness.kubeconfig \
  apply -f deploy/admission-fairness-rbac.yaml
make e2e-service-admission-fairness
```

Review the read-only check before opting into the run:

```bash
ADMISSION_FAIRNESS_ACK=kova-admission-fairness/kova/kova-service \
  ADMISSION_FAIRNESS_MODE=run make e2e-service-admission-fairness
```

The Make target reads the `ADMISSION_FAIRNESS_MODE` environment variable: `check` is the default, `run` submits work, and `recover` requires `ADMISSION_FAIRNESS_RUN_DIR`.
The live sequence holds an unschedulable Alice build, fills Alice's two queue slots, verifies Alice's next request receives 429 while a global slot remains, fills the last slot with Bob, then verifies Bob's next request receives 429 at the global cap.
It deletes the exact leader Pod with a Kubernetes UID precondition while all three requests remain queued, then releases one verified build at a time and requires the single active grant to rotate Alice → Bob → Alice → Bob → Alice even though Alice has older queued work.
Each stage preserves requester, CR UID, queue nonce, active grant, fairness cursor, runner Pod ownership, Lease, and timing receipts.
The preflight also binds the locally built OCI config digest to the Kind CRI image and both Service Pod image IDs; subsequent observations recheck the pinned config on each Service Pod.
The submissions are separated beyond Kubernetes' creation-timestamp precision and the run proves Bob's request was newer than Alice's backlog before testing fair promotion.
All runners remain Pending, so no source or output tag is published.

An uncertain POST, unknown CR/Pod, drifted identity, unresolved Pod Create nonce, or timed-out transition stops the run and preserves its evidence directory without deleting a ledger or another workload.
Inspect `state.json` and the saved stage receipts before recovery.
Only recorded accepted CR UIDs can be removed by the guarded recovery path; a POST with no proven result must be investigated manually.
If every candidate and cluster identity is accounted for, use:

```bash
ADMISSION_FAIRNESS_ACK=kova-admission-fairness/kova/kova-service \
  ADMISSION_FAIRNESS_MODE=recover \
  ADMISSION_FAIRNESS_RUN_DIR=/data/forge-artifacts/kova-admission-fairness/fairness-20260927t120000z-0123abcd \
  make e2e-service-admission-fairness
```

Replace the example run directory with the exact directory printed by the failed run.

The five grants and four release timings are functional evidence, not a latency distribution or production SLA.

### Bounded Authentication Read Probe

After the fairness run and exact UID cleanup, the two-principal read probe may reuse the same idle Kind fixture.
Its `check` mode verifies the sole Kind identity, clean checkout, deployed candidate commit and local/CRI/Pod image chain, two Ready Service Pods, unchanged RBAC and deployment, empty active and queue entries, no KovaBuild or runner anywhere, healthy nodes, and Pod/memory headroom.
A nonempty fairness cursor from the completed run is allowed only when it matches Alice or Bob, and it must remain unchanged throughout the probe.
Set the full Git SHA of the actually deployed image candidate; a later script-only commit may differ from that image commit.
For the currently installed candidate:

```bash
ADMISSION_AUTH_READ_CANDIDATE_COMMIT=e03ab4cab17477014608e6da974f8fc0ca11f067 \
  make e2e-service-auth-read-probe
ADMISSION_AUTH_READ_CANDIDATE_COMMIT=e03ab4cab17477014608e6da974f8fc0ca11f067 \
  ADMISSION_AUTH_READ_ACK=kova-admission-fairness/kova/kova-service/auth-read \
  ADMISSION_AUTH_READ_MODE=run make e2e-service-auth-read-probe
```

Update the candidate SHA after any redeployment; the probe refuses an image mismatch.
The default live load is 40 empty-list GET requests per principal at two requests per second, sent directly through separate loopback port-forwards to the two pinned Service Pods.
The hard caps are 120 requests and four requests per second per principal, plus a three-minute wall-clock limit.
Run mode uses short TokenRequest bearers only in process memory, shares the fairness fixture lock, writes one private JSON receipt per request, and saves before/after API Server `apiserver_request_total` TokenReview/SAR series and repeated node/Service health samples.
An unexpected HTTP response, workload, Pod replacement, identity drift, resource cap, or API counter reset stops the probe and preserves receipts.
The Service's empty-list GET performs both TokenReview and list SubjectAccessReview; this probe does not test the owner GET path's SAR bypass, which needs an owned KovaBuild.
The API Server counters are cluster-wide, so their deltas are a lower-bound consistency check for this path, not exclusive attribution or a production SLA.

## Admission Ledger-Loss Fault Acceptance

Use only an existing disposable `kova-admission-*` Kind cluster after the ordinary two-replica admission preflight passes.
The script additionally matches the kubeconfig server and credentials to the live Kind cluster, requires that it be the only Kind cluster on the host, checks for zero KovaBuilds and runner Pods across all namespaces, and verifies each Service Pod's ReplicaSet and Deployment ownership.
Check mode is read-only:

```bash
KIND_CLUSTER=kova-admission-44 KIND_KUBECONFIG=.kind/kova-admission-44.kubeconfig make e2e-service-admission-ledger-loss
```

Live mode is an intentional fault injection.
It requires an explicit acknowledgement naming the exact disposable cluster, namespace, and ConfigMap.
It keeps receipts under `/tmp/kova-admission-ledger-loss.*`, leaves `kova-service-admission` absent, and never attempts to recreate the ledger or clean up the cluster.
The optional restart deletes only one verified Service follower Pod; leave it disabled unless startup recovery is part of the acceptance being run.
Stop after any unexpected response or cloud error, inspect the receipts, and retire only the test-owned Kind cluster after evidence has been saved.

```bash
ADMISSION_E2E_MODE=run \
ADMISSION_LEDGER_LOSS_ACK=kova-admission-44/kova/kova-service-admission \
ADMISSION_LEDGER_LOSS_RESTART=true \
KIND_CLUSTER=kova-admission-44 KIND_KUBECONFIG=.kind/kova-admission-44.kubeconfig \
SERVICE_AUTH_TOKEN=service-e2e-token make e2e-service-admission-ledger-loss
```

This test does not establish production availability or a safe ledger recovery procedure.
Do not restore the deleted ConfigMap from a blank template: an apparently empty namespace cannot prove that no admission or Pod-create operation had an unknown outcome.

## Admission Handoff and Capacity-Release Fixture

The existing `make e2e-service-admission-failover` check and normal live path retain the #44 behavior: a replacement leader must reconcile a challenger to `Queued/WaitingForCapacity` while the original blocker still owns the one active grant.
The optional #46 promotion path runs only on `wayne-hk-kvm` against the sole disposable Kind cluster named `kova-admission-pump`, with the same exact active=1, global queue=3, requester queue=2, two-Service-replica, `never=true` fixture.
It additionally requires `--wait=2h` and `--max-build-duration=2h`, two Ready nodes, the `kova-e2e-token/token` Secret reference on the Deployment, and empty workloads/ledgers before any write.
Check mode remains read-only and does not decode the Secret payload:

```bash
KIND_CLUSTER=kova-admission-pump KIND_KUBECONFIG=.kind/kova-admission-pump.kubeconfig \
ADMISSION_FAILOVER_PROMOTION=true make e2e-service-admission-failover
```

Only after the check and exact fixture identity have been reviewed, start the opt-in live path with an explicit cluster/namespace/Deployment acknowledgement:

```bash
KIND_CLUSTER=kova-admission-pump KIND_KUBECONFIG=.kind/kova-admission-pump.kubeconfig \
ADMISSION_E2E_MODE=run ADMISSION_FAILOVER_PROMOTION=true \
ADMISSION_FAILOVER_PROMOTION_ACK=kova-admission-pump/kova/kova-service \
make e2e-service-admission-failover
```

The opt-in path reads the bearer only from the dedicated Kind Secret into short-lived process memory; it ignores any inherited `SERVICE_AUTH_TOKEN` and never puts the bearer in a command argument or receipt.
Before each opt-in write, it verifies the dedicated kubeconfig bytes exactly match `kind get kubeconfig` for the sole live Kind cluster; the test-only Secret UID, Deployment template, Lease UID/holder, both ledger UIDs, Service/runner Pod UIDs, and CR UIDs are pinned at their respective gates.
Opt-in snapshots project KovaBuild, Service Deployment, and Pod identity/status fields instead of writing arbitrary unexpected workload specs; the old #44 default receipt shape is unchanged.
The default #44 live path still accepts its original `SERVICE_AUTH_TOKEN` input, but passes it to curl via stdin configuration rather than curl arguments.
Both paths create a fresh fake source URI and no registry tag; the unschedulable `never=true` runner never executes a build.
The opt-in path deletes the verified leader Pod using an atomic UID precondition, proves handoff and stable queue retention, then deletes only the exact blocker CR with an atomic UID precondition.
It uses a 120-second poll deadline with individually bounded API reads for the challenger UID to become the sole `Starting` grant and sole unscheduled `Pending` runner, with the queue intent gone and the original runner absent.
The observed release-to-grant delay does not, by itself, prove that the ConfigMap watch woke the controller because the admission pump also has a bounded periodic audit fallback.
Only after a second identity check does it UID-delete the challenger and use another 120-second poll deadline for both ledgers, all CRs, and runner Pods to empty.
It then keeps a read-only, at-least-120-second empty-state observation, sampling all-namespace CR/runner emptiness and exact ledger/Lease/Service identities every ten seconds while saving API Server `apiserver_request_total` series at both ends.
This 3/2-cap empty-cluster window is an idle baseline, not a substitute for a quiet 1000-build deep-queue measurement; the Service's local metrics endpoint is disabled in the normal admission fixture and is not silently enabled here.
Any 409, timeout, unknown write result, identity drift, extra workload, or incomplete cleanup stops the test and preserves the run-scoped receipts and objects for review; do not manually delete a ledger or guess at a retry.
This isolated fixture is not a production availability or latency SLA.

Pure-local safety checks run with `python3 -m unittest scripts.e2e.tests.test_admission_failover_io -v`, `bash -n scripts/e2e/e2e-service-admission-failover.sh`, and `make lint-scripts`.

## Deep-Queue Admission Benchmark

Use only a disposable, otherwise idle Linux Kind cluster named `kova-deep-queue` with a single worker and its exact `.kind/kova-deep-queue.kubeconfig`.
Prepare the candidate chart and two same-image Service replicas before this test, with `serviceDaemon.maxActiveJobs=1`, `maxActiveJobsPerRequester=1`, `workerSlots=1`, `maxQueuedJobs=1000`, `maxQueuedJobsPerRequester=1000`, `kubeClientQPS=20`, `kubeClientBurst=40`, `pollInterval=5s`, `wait=2h`, `maxBuildDuration=2h`, static principal `kova:e2e`, leader election, and `runnerNodeSelector.never=true`.
The two-hour `wait` is essential: the default three-minute wait would fail the Pending blocker and could allow a queued build to acquire a runner during a later stage.
These caps are a stress fixture, not recommended production defaults.
Do not use quickstart's build/tag setup for this benchmark if preserving the existing local registry is required; the benchmark itself neither contacts the registry nor creates source/output tags.
The following one-time setup runs on the isolated Linux host only after all other Kind clusters have been removed and the intended candidate role images have been built locally from one reviewed Kova revision.
It uses the pinned two-node Kind config, loads local image objects directly into Kind, and sets `imagePullPolicy=Never`; do not run `make kind-load` here because that target pushes the images to the local registry.
Before setup, record the clean checkout commit, `sha256sum deploy/quickstart-kind-cluster.yaml`, the pinned Kind node image digest, and each role's `docker image inspect ... --format '{{.Id}} {{index .Config.Labels "org.opencontainers.image.revision"}}'`.
All three 12-character revision labels must equal the clean checkout's HEAD prefix; matching one another is insufficient.
If an image is stale, run `make image` at that final clean HEAD and inspect the new image IDs before `kind load docker-image`.
`make image` builds local Docker objects only; do not run `make kind-load`, which pushes to the local registry.

```bash
kind create cluster --name kova-deep-queue \
  --image kindest/node:v1.36.1@sha256:3489c7674813ba5d8b1a9977baea8a6e553784dab7b84759d1014dbd78f7ebd5 \
  --config deploy/quickstart-kind-cluster.yaml \
  --kubeconfig .kind/kova-deep-queue.kubeconfig
kind load docker-image localhost:5002/kova:controller-dev localhost:5002/kova:runner-dev localhost:5002/kova:worker-dev \
  --name kova-deep-queue
kubectl --kubeconfig .kind/kova-deep-queue.kubeconfig create namespace kova
printf '%s' "$SERVICE_AUTH_TOKEN" | kubectl --kubeconfig .kind/kova-deep-queue.kubeconfig -n kova \
  create secret generic kova-e2e-token --from-file=token=/dev/stdin
helm install kova charts/kova --kubeconfig .kind/kova-deep-queue.kubeconfig -n kova \
  -f deploy/quickstart-kind-values.yaml --wait --timeout 10m \
  --set images.controller.pullPolicy=Never --set images.runner.pullPolicy=Never --set images.worker.pullPolicy=Never \
  --set serviceDaemon.enabled=true --set serviceDaemon.replicas=2 \
  --set serviceDaemon.authentication.mode=static \
  --set serviceDaemon.authentication.staticTokenSecret.name=kova-e2e-token \
  --set-string serviceDaemon.authentication.staticPrincipal=kova:e2e \
  --set serviceDaemon.maxActiveJobs=1 --set serviceDaemon.maxActiveJobsPerRequester=1 \
  --set serviceDaemon.workerSlots=1 --set serviceDaemon.maxQueuedJobs=1000 \
  --set serviceDaemon.maxQueuedJobsPerRequester=1000 \
  --set serviceDaemon.kubeClientQPS=20 --set serviceDaemon.kubeClientBurst=40 \
  --set serviceDaemon.pollInterval=5s \
  --set serviceDaemon.wait=2h --set serviceDaemon.maxBuildDuration=2h \
  --set-string serviceDaemon.runnerNodeSelector.never=true
kubectl --kubeconfig .kind/kova-deep-queue.kubeconfig -n kova \
  create rolebinding kova-e2e-submitter --role=kova-service-submitter --user=kova:e2e
```

Provision a strong, test-only `SERVICE_AUTH_TOKEN` in the setup shell's environment for the one-time Secret creation, then unset it; do not paste its value into commands, Helm values, receipts, or Git.
The live benchmark reads `kova-e2e-token/token` from this same verified Kind cluster directly into process memory after locked preflight, so the long-running launcher needs no token file or command-line argument.
The setup should record the exact Helm install values above, Kind config SHA-256 and pinned node image digest, Kind node UIDs, and deployed Pod image IDs alongside the Docker image IDs.
The benchmark's `identity.json` records the clean checkout commit, each role's Docker object/index digest, Linux/amd64 manifest/config digest chain and revision, the image IDs preloaded on both Kind nodes, actual Service/worker Pod image IDs, and exact Kind kubeconfig fingerprint.
Preflight streams a platform-filtered local Docker OCI export with bounded metadata, verifies index→manifest→config digests, and compares its config digest with each node's CRI image ID; the OCI index/object ID alone is not comparable with a CRI config ID.
For each Pod it queries CRI by the exact deployed tag (some Kind CRI builds cannot look up the import repoDigest shown in Pod status), requires that tag to resolve only to the verified config digest, and requires the Pod imageID to equal that config digest or one of the image's exact CRI repoDigests.
It also checks the runner image is preloaded on both nodes despite never creating a runner container.
If the runtime does not expose comparable CRI IDs/digests, preflight stops instead of assuming the cached image is current.
No setup command above pushes to the local or cloud registry, and no HK ACK context is used.

The default entrypoint reads the cluster, kubeconfig identity, ledgers, readiness, API-server request counters, and kubelet summary metrics without writing Kubernetes objects or local receipts:

```bash
make e2e-service-admission-deep-queue
```

For an SSH-disconnect-safe live run on `wayne-hk-kvm`, use the dedicated launcher after the read-only check passes; `start` creates a private `0700` control directory, `0600` PID/log/launch receipts, and a detached controller that holds the host lock for preflight and the full run.
The launcher records the exact clean candidate commit, Kind config SHA-256, and pinned tool binary SHA-256 values.
It does not accept or log a token.

```bash
python3 scripts/e2e/deep-queue-launcher.py start
python3 scripts/e2e/deep-queue-launcher.py status /data/forge-artifacts/kova-deep-queue/control-<exact-id>
python3 scripts/e2e/deep-queue-launcher.py stop /data/forge-artifacts/kova-deep-queue/control-<exact-id>
```

The `start` JSON prints the exact control directory, PID, and private log path. `status` shows the PID's original process start time, run receipt directory, build counts/phases, Service replica count, and any final result; the log remains on the KVM host for `tail -f`.
`stop` sends SIGTERM to only that PID and waits for the benchmark to confirm a stop. If the controller already died or its automatic stop was unconfirmed, it invokes the run-receipt-bound manual stop; it never sends SIGKILL or deletes CRs.
For independent read-only inspection or exact manual stop when the launcher is unavailable:

```bash
DEEP_QUEUE_E2E_MODE=status DEEP_QUEUE_E2E_RUN_DIR=/data/forge-workspace/kova/.work/deep-queue/<exact-run-id> \
  python3 scripts/e2e/e2e-service-admission-deep-queue.py
DEEP_QUEUE_E2E_MODE=stop DEEP_QUEUE_E2E_ACK=kova-deep-queue/kova/kova-service \
  DEEP_QUEUE_E2E_RUN_DIR=/data/forge-workspace/kova/.work/deep-queue/<exact-run-id> \
  python3 scripts/e2e/e2e-service-admission-deep-queue.py
```

Only the first, test-owned blocker may have a runner Pod object; its owner UID, `never=true` selector, `Pending` phase, absent node assignment, and single active grant are checked throughout.
Every queued CR must remain runner-less, with a matching queue-intent nonce.
Each stage waits for all CRs to reach `Queued`, then measures 30 seconds of API request rates by verb/resource (including the benchmark's own read-only probes), Service Pod and Kind node CPU/memory, readiness, and HTTP POST throughput/p50/p95/p99.
The host must retain 8 GiB available memory and 20 GiB free workspace/Docker disk; each Kind node must remain Ready without pressure, below 80% allocatable CPU, and above 2 GiB available memory; each Service Pod must remain below 2 vCPU and 2 GiB working memory; measured API traffic must stay below 1200 requests/s with no 429 or 5xx.
Missing metrics, transport errors, limits, identity drift, unrecorded CRs, ledger disagreement, or incomplete cleanup stop the benchmark immediately and preserve the cluster for inspection.
After any attempted POST, a failure or SIGINT/SIGTERM/SIGHUP triggers an exact-identity, UID/resourceVersion-tested JSON Patch scaling only the dedicated Service Deployment from two replicas to zero; the result distinguishes `confirmed` from `unconfirmed` and records whether all Service Pods disappeared.
If API access or identity checks make that stop unconfirmed, do not leave the cluster unattended or start another Kind test; use the exact manual stop and inspect the run evidence.

Receipts are kept separately for the blocker, each 100/500/1000 stage, and exact-ID cleanup under `.work/deep-queue/<run-id>/`.
On success, only the recorded queued CRs and blocker CR are deleted through a loopback Kubernetes API proxy with each exact CR UID as an atomic DeleteOptions precondition; the Kind cluster, local registry, and all registry tags remain untouched.
If a submission or measurement fails before exact cleanup starts, no CR or ledger cleanup is attempted because an uncertain HTTP Create may still be in flight.
If a deletion or its verification fails after exact cleanup starts, some recorded CRs may already have been deleted; the script stops and preserves the delete-batch receipts for operator inspection rather than guessing at a repair.
The default controller does **not** expose per-reconcile latency p95/p99 to this black-box script: POST latency and queue-status convergence are separate measurements and must not be reported as reconcile latency.
For a future diagnostic rerun, the Service can opt in to loopback-only controller-runtime metrics with `serviceDaemon.metricsBindAddress=127.0.0.1:8081`; the endpoint is disabled by default and the black-box script does not yet scrape it. Record per-Pod histogram buckets before and after each stage and compute deltas rather than treating a lifetime cumulative histogram as one stage. First verify loopback collection in the isolated Kind; do not expose the unauthenticated metrics listener through a cluster Service.
This bounded Kind profile is a design diagnostic, not production SLO or SLA evidence.

## Isolated Source-Capacity Acceptance

Run this only on an isolated Linux Kind host after other Kind tests have released their clusters, with enough local storage, and never from a Mac or against a cloud kubeconfig.
It exercises a 128 MiB incompressible source, immutable OCI source fetch, and the private runner extraction path; the unit tests cover near-limit rejection and the absence of a second expanded-tree copy.
It does not establish the maximum safe production payload size.
Prepare a **clean committed** candidate checkout and Linux CLI. Build the three
role images from its exact 12-character Git revision, then create a fresh Kind
quickstart (never reuse an occupied cluster). Give setup outputs run-scoped
tags; the quickstart will build and load the named role images:

```bash
REV=$(git rev-parse --short=12 HEAD)
SETUP_RUN=source-capacity-setup-$(date -u +%Y%m%dt%H%M%Sz)
QUICKSTART_KIND_CLUSTER=kova-source-capacity KEEP_KIND_CLUSTER=true \
  CONTROLLER_IMAGE="localhost:5002/kova:controller-$REV" \
  RUNNER_IMAGE="localhost:5002/kova:runner-$REV" \
  WORKER_IMAGE="localhost:5002/kova:worker-$REV" \
  SERVICE_AUTH_SECRET=kova-e2e-token \
  SERVICE_TARGET="kind-registry:5000/kova-examples/source-capacity-setup:$SETUP_RUN" \
  SERVICE_PULL_TARGET="localhost:5002/kova-examples/source-capacity-setup:$SETUP_RUN" \
  SOURCE_REPOSITORY="localhost:5002/kova-sources/source-capacity-setup:$SETUP_RUN" \
  make e2e-helm-quickstart
```

The quickstart must complete and its terminal KovaBuilds must expire before the source-capacity preflight can pass.
The generic quickstart default `kova-service-auth` Secret name is insufficient for this isolated fixture; use `SERVICE_AUTH_SECRET=kova-e2e-token` as shown above.
The preflight verifies that the host has only the named Kind cluster, that its dedicated kubeconfig exactly matches the live Kind credentials/server, that both nodes and Kova deployments are healthy, that the registry image/port/network are exact, and that no KovaBuild or runner Pod is active. It also requires a clean checkout, same-revision Linux CLI, exact role image tags and OCI revision labels, and matching local Linux/amd64 image config IDs, ready Pod image IDs, and Kind CRI image identities. The live mode repeats these identity checks after source publication and checks both `source-fetch` and `runner` containers on the exact owned runner Pod.
It makes no cluster changes.

```bash
make e2e-source-capacity
SOURCE_CAPACITY_E2E_MODE=run make e2e-source-capacity
```

The live mode requires at least 20 GiB free disk, 1 GiB free temporary storage, and 8 GiB available RAM.
It requires the exact disposable `kova-e2e-token/token` Secret reference and fixed quickstart credential, reads the token into process memory, and rejects inherited Service tokens; do not put any token in an SSH command or evidence file.
It rejects existing source/output tags for its random run ID, then saves the source receipt, exact job ID, terminal and result receipts, live runner log and Pod samples, node health and Docker resource samples, events, and host-pull digest under `.work/source-capacity/<run-id>/`. During the build, a node pressure/unready condition, runner failure/restart, or node/runner/Docker collection error signals the test controller and enters exact-ID/UID stop; the final gate requires nonempty complete samples from both nodes, a real owned runner sample, and a complete nonempty bounded runner log capture.
Runner and job log stdout/stderr evidence retains at most 1 MiB/64 KiB per capture, with explicit head/tail truncation, full-stream SHA-256 and byte counts, child exit status, and signal outcome in `*.capture.json`; the terminal job/results receipts, not a truncated log, determine acceptance.
The expected outcome is one succeeded build with a verified manifest digest and a host pull of `localhost:5002/kova-examples/source-capacity:<run-id>` yielding that same digest.
After a submission might have reached the Service, failure or HUP/INT/TERM triggers a supervised stop of only the deterministic run ID. The stop checks the saved Kind identity and the complete KovaBuild source, requester, target, and options contract, verifies runner ownership, then sends a Kubernetes API DELETE with an atomic CR UID precondition. It waits for the exact CR, runner, and admission ledgers to clear. It never deletes the Kind cluster or registry tags. A missing CR after a timed-out submit is **uncertain**, not proof of no write; a changed identity, foreign build/Pod, API error, or non-converged cleanup also fails closed and preserves `stop.err`, `stop-outcome.json` (when available), and all other receipts. Inspect the exact run before retrying or clearing tags; a known failed run may be retried with `python3 scripts/e2e/source-capacity-guard.py stop .work/source-capacity/<run-id>` after checking the same dedicated cluster identity. SIGKILL and host loss cannot run the trap, so the operator must inspect the exact ID before a new run.
After evidence review, remove only the named `kova-source-capacity` Kind cluster and its kubeconfig if no longer needed; review run-scoped source/output tags before any registry cleanup.

The source-pressure companion uses the same empty dedicated Kind and exact candidate preflight, but does not submit a build.
Run its default read-only check first, then explicitly acknowledge the isolated registry repository:

```bash
./scripts/e2e/e2e-source-pressure-rejection.sh
SOURCE_PRESSURE_E2E_MODE=run \
  SOURCE_PRESSURE_E2E_ACK=kova-source-capacity/kova/kova-sources/source-pressure-rejection \
  ./scripts/e2e/e2e-source-pressure-rejection.sh
```

It requires 20 GiB workspace/Docker disk, 2 GiB temporary disk, and 8 GiB memory headroom, then expects four malformed or oversized archives to be rejected before publication with each exact run tag absent.
Run it in a persistent session; HUP/INT/TERM fail the campaign and retain its private `.work/source-pressure-rejection/<run-id>/` evidence.
The Service-side oversized immutable source test reuses the same dedicated Kind, image identity preflight, and exact UID cleanup guard.
It publishes a valid sparse ZIP through the local registry API in 1 MiB streaming chunks, bypassing only the Kova client's pre-publication size check so the runner's OCI fetch limit is exercised.
Its source is exactly 512 MiB + 1 byte; the source tag and all receipts remain available for inspection.
Run it only after the earlier source-capacity test has finished and all KovaBuilds, runner Pods, and admission ledgers are empty:

```bash
make e2e-source-oci-oversize
SOURCE_OCI_OVERSIZE_MODE=run \
  SOURCE_OCI_OVERSIZE_ACK=kova-source-capacity/kova/kova-sources/source-capacity \
  make e2e-source-oci-oversize
```

The live mode requires at least 20 GiB free workspace and Docker disk, 2 GiB free temporary disk, and 8 GiB available memory.
It refuses existing source and output tags for its unique run ID and retains the sparse local ZIP, OCI manifest and blob digests, Service and KovaBuild receipts, bounded `source-fetch` logs, runner and two-node samples, registry tag headers, and exact UID stop receipt under `.work/source-oci-oversize/<run-id>/`.
Expected success means the public Service reports `invalid_source`, the terminal KovaBuild reason is `InvalidSource`, the captured init log identifies the 512 MiB compressed limit, no target tag appears, and the exact CR UID, runner, and ledgers are cleared.
The Service intentionally exposes a generic public error string; the precise size diagnostic comes from the bounded init log, and kubelet's termination message is only supplementary evidence.
If any check fails, the script attempts the existing full-contract UID-precondition stop and preserves evidence; a timed-out submission with no observed CR remains uncertain and requires inspection before another run.
The test does not delete registry blobs or tags, Kind, deployment images, or unrelated workloads.
Passing this case and the source-pressure client rejection cases does not establish kubelet eviction behavior; that is a separate gate.

## Isolated Partial-Output Receipt Acceptance (#41)

This is a sequential Kind acceptance for the real runner path, not a soak or a production SLA claim.
Run it only on `wayne-hk-kvm` after the other Kind test has finished and released its cluster.
The script itself never creates or deletes Kind, installs Helm, pushes role images, or deletes registry content.
Its default mode is read-only and requires a clean Kova checkout, a Linux CLI whose version matches the checkout's 12-character commit, candidate role-image tags and labels from that commit, the dedicated kubeconfig matching the only live Kind cluster, and exact Pod-to-CRI image identity. A Pod image ID may be the CRI config digest or one of its repo digests, but the CRI config digest must also equal the reviewed local Linux/amd64 image config digest under the exact tag.
It also requires two healthy nodes, eight free Pod slots per node, empty KovaBuild/runner and admission ledgers, the `FailedVerifying` CRD enum, and the test-owned `kova-e2e-token/token` Secret reference.

Prepare a disposable two-node quickstart with reviewed candidate images, `SERVICE_AUTH_SECRET=kova-e2e-token`, and unique setup tags. The quickstart's short JobTTL allows its own builds to expire; afterwards, upgrade only the test release to `serviceDaemon.jobTTL=2h` before the partial-output preflight. Keep the checkout clean after the image/CLI build and verify the Service still has `--max-build-duration=2h` and `--verification-window=5m`.

```bash
REV=$(git rev-parse --short=12 HEAD)
SETUP_RUN=partial-41-setup-$(date -u +%Y%m%dt%H%M%Sz)
QUICKSTART_KIND_CLUSTER=kova-partial-output-41 KEEP_KIND_CLUSTER=true \
  CONTROLLER_IMAGE="localhost:5002/kova:controller-$REV" \
  RUNNER_IMAGE="localhost:5002/kova:runner-$REV" \
  WORKER_IMAGE="localhost:5002/kova:worker-$REV" COMMIT="$REV" \
  SERVICE_AUTH_SECRET=kova-e2e-token SERVICE_JOB_TTL=60s \
  SERVICE_TARGET="kind-registry:5000/kova-examples/$SETUP_RUN:dev" \
  SERVICE_PULL_TARGET="localhost:5002/kova-examples/$SETUP_RUN:dev" \
  SOURCE_REPOSITORY="localhost:5002/kova-sources/$SETUP_RUN:dev" \
  make e2e-helm-quickstart
helm upgrade kova ./charts/kova --kubeconfig .kind/kova-partial-output-41.kubeconfig \
  -n kova --reuse-values --wait --set-string serviceDaemon.jobTTL=2h
PARTIAL_OUTPUT_EXPECTED_REVISION="$REV" make e2e-service-partial-output
```

Only after that read-only check passes, explicitly acknowledge the exact cluster and revision. The script reads the token from the exact Kubernetes Secret into process memory; do not put the token in the SSH command or a long-lived environment variable. Use a persistent remote session; SIGINT, SIGTERM, and SIGHUP enter the bounded exact-UID stop path, but SIGKILL cannot be recovered by the process.

```bash
PARTIAL_OUTPUT_EXPECTED_REVISION="$REV" PARTIAL_OUTPUT_MODE=run \
  PARTIAL_OUTPUT_ACK="kova-partial-output-41/kova/$REV" \
  make e2e-service-partial-output
```

The fixture ZIP has two top-level image directories with valid `{target,platform}` metadata.
`a-pass` is lexically first and completes both OCI and Nydus with `concurrency=1`; `z-fail` has a deliberately missing Dockerfile `COPY` source, which fails only during BuildKit execution.
The script reads the exact runner Pod's bounded, structured failure export and requires its failed second Nydus target to diagnose the fixture-owned `/missing` source path; it does not match BuildKit's changeable English error wording.
If that path is absent, the Pod or export disappears before capture, or the diagnostic format changes, the acceptance fails closed and retains evidence for manual review rather than attributing an unrelated failure to the fixture.
The only accepted result is final `Failed/BuildFailed`, with two first-target `status.outputs` whose manifest digests equal their durable runner `pushedDigest` receipts and the exact registry tags; the later target must have failed verification results and no tags.
The public job/results must agree with durable source identity, idempotency key, output format, platform, manifest digest, and immutable reference.
A 20-minute overall deadline, five-minute failed-verification window, host memory/disk and node pressure/Pod guards, and runtime-image checks stop abnormal runs.
This tests the ordinary partial-failure path; controller fault tests separately cover transient registry/export errors, leader restart, and terminal status-write ambiguity.

Private evidence is saved under `.work/partial-output/<run-id>/`: the source archive and digest, run-scoped tag names, projected status/runner/node/ledger samples, bounded runner log excerpts and failure export with its `/missing` proof, public job/results, registry digest comparisons, and exact CR deletion receipt.
Unknown objects are recorded by name/UID only, and an exact token reflected in a response blocks its write.
On success the script deletes only its own KovaBuild using an API-server UID precondition; on an abnormal nonterminal result it attempts the same exact stop and reports if that cannot be proven.
All source/output tags, the registry, Kind cluster, and receipts remain for review.
Never use repository-wide or digest-wide registry deletion to clean these tags; review exact run receipts first.

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

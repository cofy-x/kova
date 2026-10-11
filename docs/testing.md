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

## Opt-in Genesis bootstrap process gate

`TestRealAPIGenesisProcessBootstrap` exercises the actual runtime bootstrap function at five write points, before and after each known API response, using ten killed-and-joined child processes followed by ten successor processes.
It does not start a Service listener, manager, leader election, runner, BuildKit worker, registry, or build workload.
An after-response pause is not an unknown-response or network-loss simulation.

First use the external create-only installer to prepare ten distinct fresh runner/recovery-receipt Namespace pairs, ten Initializing Genesises, and ten immutable installation receipt Secrets in a separate control Namespace.
The operator must attest that no old or competing writer can use these identities.
Do not reuse a Namespace from an earlier run, including a successful run.
The gate only accepts dedicated `kind-kova-genesis-api-*` or `kind-kova-genesis-process-*` contexts with an exact owned control-plane container ID and a pinned loopback HTTPS API binding.

The private absolute manifest requires `kubeconfig`, its `kubeconfigSHA256`, `context`, `kubeSystemUID`, `controlPlaneID`, an explicit `cleanupOnSuccess` boolean, and exactly ten `cases`.
Each case pins `stage` (`active-create`, `active-pin`, `queue-create`, `queue-pin`, or `commit`), `point` (`before` or `after`), `namespace`, `namespaceUID`, `receiptNamespace`, `receiptNamespaceUID`, `genesisUID`, absolute `receiptFile`, `receiptSHA256`, `secretNamespace`, `secretNamespaceUID`, `secretName`, and `secretUID`.
Each recovery-receipt Namespace must be unique and disjoint from every runner and control Namespace, and its original name/UID must match the immutable v3 receipt.
The receipt also pins an externally assigned worker pool identity and the exact runner OCI manifest/index reference; the gate derives no authority from a mutable tag or image configuration ID.
All ten fixtures are preflighted before the first runtime write; bounded receipt-Namespace inventory rejects existing receipts, workloads, Secrets, pagination, or read failures, allowing only the system `kube-root-ca.crt` ConfigMap if present.
The strict schema and opt-in-free refusal tests live in the [process gate implementation](../internal/service/genesis_process_realapi_test.go).

```bash
KOVA_GENESIS_PROCESS_API_MANIFEST=/absolute/private/matrix.json \
  go test ./internal/service -run '^TestRealAPIGenesisProcessBootstrap$' -count=1 -timeout=15m -v
```

Use `cleanupOnSuccess=false` to retain every test object for exact-UID inspection.
No API cleanup occurs on a failed test path before cleanup; an explicitly opted-in successful cleanup is sequential, and a later deletion failure can leave a partially cleaned fixture that requires inspection.
Each child has a 45-second absolute deadline and the parent has a 12-minute budget; a timeout retains all unresolved API state.
The v3 fixture/schema adaptation is not itself live v3 qualification, and earlier v1 process-gate evidence cannot certify the changed contract.
The historical waiting-only queue fixtures below also need explicit adaptation before they can qualify the active-inclusive HTTP queue cap introduced in v2 and retained in v3.

## Source-Bound Actual Service Control-Plane Fault Gate

`TestRealAPIActualServiceControlPlane` is a separate, opt-in seven-fixture gate.
Each child invokes the same complete Service CLI Action as the shipped command, including receipt qualification, bootstrap, real controller-runtime manager/cache/reconciler, HTTP listener, and leader election.
Only the unexported Kubernetes configuration loader changes: the test uses its pinned dedicated Kind endpoint through a parent-owned loopback fault proxy instead of in-cluster credentials.
The normal CLI still requires in-cluster credentials and has no fault-injection flags.
This test binary is not a candidate container image, and this control-plane gate is not runner, build-result, recovery-drain, deployed-RBAC, or final API-load qualification.

The matrix requires these seven distinct case names:

- `lost-active-create`, `lost-active-pin`, `lost-queue-create`, `lost-queue-pin`, and `lost-commit`: the proxy consumes a successful original API response, closes the actual downstream connection without delivering it, and requires the Service to qualify the committed pair with exactly one upstream attempt for that mutation.
- `late-active-create`: the parent captures one exact original Create body, kills and joins its caller, lets successors commit, and forwards that captured request exactly once; the still-present committed ledger must produce a conflict without UID changes.
- `late-active-create-after-loss`: after successor commit and leader handoff, the parent UID-deletes only this case's newly committed active ledger and forwards the captured old Create once; a distinct replacement UID must not rebind immutable Genesis, the survivor must return readiness 503, and another actual Service startup must refuse without a listener or API writes.

The deletion case requires explicit, separately reviewed live-fault authorization for the exact disposable fixture, as well as its manifest acknowledgement.
Adding the test or setting its acknowledgement is not evidence of a live PASS or permission to mutate a shared cluster.
Never run it against HK, old preserved ledger-loss fixtures, reused runner namespace names, or the earlier ten-process bootstrap fixtures.
All seven runner/receipt/control namespace triples, original UIDs, receipts, empty-workload vetoes, and absent Lease identities are checked before the first child starts.
Namespace freshness and stopped external writers remain the external installer's responsibility, not facts inferred from empty Lists.

Create a private JSON manifest outside Git, selected only with `KOVA_GENESIS_SERVICE_MANIFEST=/absolute/private/service-matrix.json`.
The strict root fields are `schemaVersion: 1`, the full lowercase 40-character `sourceCommit`, `testBinarySHA256`, absolute `kubeconfig`, `kubeconfigSHA256`, `context`, `kubeSystemUID`, `controlPlaneID`, `oldWritersStopped: true`, `cleanupOnSuccess: false`, and `cases`.
The exact dedicated context must satisfy the existing Kind process-target identity check, including original running control-plane container ID and its loopback API port.
Each case has `name`, explicit `allowCommittedLedgerDeletion` (true only for the final deletion case), and `fixture`.
The fixture fields are `namespace`, `namespaceUID`, `receiptNamespace`, `receiptNamespaceUID`, `genesisUID`, absolute `receiptFile`, `receiptSHA256`, `secretNamespace`, `secretNamespaceUID`, `secretName`, and `secretUID`; do not include the earlier process gate's `stage` or `point` fields.
All twenty-one runner/receipt/control namespace names must be distinct, begin `kova-genesis-svc-`, and retain distinct original UIDs.
The v3 receipt pins the worker pool and runner manifest reference used in the actual Service arguments; the proxy still forbids all runner Pod or build creation.
Each receipt Secret is immutable and its UID and exact mounted bytes are pinned.

Compile only a clean reviewed source candidate into a private test executable, explicitly stamping `github.com/cofy-x/kova/internal/version.Commit` with that same full source SHA through Go's `-ldflags=-X` option.
Record that source SHA, the exact build command, and the executable SHA256 in the external acceptance receipt; the manifest binds both stamp and executable hash, and every child verifies them before using Kubernetes.
Run the executable with `-test.run=^TestRealAPIActualServiceControlPlane$ -test.timeout=25m -test.v` only after the fixture and destructive-case plan have been approved.
Do not use an unstamped `go test` invocation as the live gate.

Each case has a 140-second deadline, each child an independent at-most-three-minute absolute deadline, and the parent a twenty-minute deadline.
The parent owns and joins only the exact PIDs it spawned; Lease UID and holder identities are mapped to the actual successful Lease writers before killing the leader and proving survivor handoff on the same Lease UID.
Held request bodies remain in bounded parent memory, bound to a SHA256 and one forwarding authority; cancellation never releases them automatically.
The proxy permits only the scoped bootstrap, Lease/event operations, and necessary reads/watches, and records any rejected operation as a test failure.
All Pod/CR mutation, Pod Exec, arbitrary resource access, and source/registry activity are outside this gate; HTTP probes are readiness and empty-list GET only.
No API object is automatically cleaned up, including on success; the intentionally missing original ledger and its replacement remain evidence.
Any unknown outcome, unexpected operation, identity drift, or failed process join stops the matrix, and its partially used namespaces must not be rerun as fresh fixtures.

This gate does not collect Service client budget histograms or measure loaded job stages.
Its proxy one-attempt assertions establish fault semantics only, not a production QPS budget or reconcile-latency distribution.
Focused local safety tests use `go test ./internal/service -run 'TestActualService|TestServiceFactory' -count=1`; these fake-upstream safety checks must not be reported as the opt-in real-API result.

## Source-Bound Pod Template API Defaulting Gate

`TestRealAPIPodTemplateDigestRoundTrip` is an independent, opt-in real-API gate for the canonical Pod Spec digest.
It prepares three Pods using the production `runner.PreparePod` and Genesis defaulting helpers: the source-fetch/Secret-volume baseline, limits-only CPU/memory/ephemeral-storage resources on regular and init containers, and a negative default-ServiceAccount pull-secret injection.
The positive before/after digests must match; the negative digest must differ, with the declared injected pull secret accounting for the difference.
The existing digest annotation is not trusted in place of recomputing the complete canonical template.

An externally approved installer must create a fresh, dedicated single-node `kind-kova-pod-template-*` cluster, two distinct `kova-pod-template-*` namespaces, their default ServiceAccounts, and two immutable dummy dockerconfig Secrets containing only `{"auths":{}}`.
The positive ServiceAccount has no pull secret; the negative one references its exact dummy Secret.
No CRD, KovaBuild, real credential, image publication, or registry is needed.
Pods permanently retain a scheduling gate, contradictory node selector/affinity, token automount disabled, and `imagePullPolicy: Never`.
Each uses its real namespace as owner, so this is not production KovaBuild-owner, status persistence, runner, or recovery-drain qualification.

Both `KOVA_POD_TEMPLATE_REAL_API_MANIFEST=/absolute/private/manifest.json` and `KOVA_POD_TEMPLATE_REAL_API_MANIFEST_SHA256=<sha256>` are required; partial opt-in fails.
The strict manifest fields are absolute `sourceDirectory`, clean full `sourceCommit`, `testBinarySHA256`, absolute private `kubeconfig`, `kubeconfigSHA256`, `context`, `kubeSystemUID`, `controlPlaneID`, `preparedAfter`, `expiresAt`, a new absolute `receiptPath`, `positive`, `negative`, `baselinePod`, `limitsPod`, and `negativePod`.
Each namespace object contains `name`, `uid`, `serviceAccountUID`, `serviceAccountRV`, `secretName`, `secretUID`, and `secretRV`.
The cluster and namespaces must have been created in the declared fresh time window, which is at most one hour; time is only a fixture restriction, never retirement proof.
Pin the cached or published immutable Kind node reference in the external run plan, and pin the exact running control-plane container ID and loopback HTTPS port through the kubeconfig and manifest.

Compile a clean reviewed `./internal/service/buildcontroller` test binary with `github.com/cofy-x/kova/internal/version.Commit` stamped to the exact source SHA, and record that command and binary hash outside Git.
After explicit fixture approval, run only `-test.run=^TestRealAPIPodTemplateDigestRoundTrip$ -test.timeout=3m -test.v`.
The test has a two-minute context and ten-second API calls, a transport-level read allowlist, and one permitted POST per declared Pod; unknown, rejected, or lost-response Creates are never reissued.
It rechecks source, manifest, cluster, namespace, ServiceAccount and Secret identities before each Create and fsyncs append-only private JSONL intent/readback checkpoints.
All API objects remain on success and failure; the receipt file cannot be overwritten or reused.
Source safety tests and a compiled executable are not a live PASS, and this narrow gate never qualifies the complete committed-loss disposal path.

## Source-Bound Genesis v3 Capacity Gate (PLAN)

The older `TestRealAPIActiveFullTable` and `TestRealAPIQueuedLedgerFullTable` deliberately retain their legacy schema-1 coverage; they do not certify the current Genesis v3/schema-2 ledgers.
Normal CI now also runs `TestGenesisV3FullTableLifecycle` and raw-byte/Unicode boundary tests.
These check both mutually exclusive 128-entry maximum active states (one in-flight issue versus cleanup-ready), maximum 1000-entry queue receipt/cleanup state, the 65,535-slot sum, JSON escaping, exact 768 KiB raw data and one-byte-over refusal.
Worst-case synthetic identity strings are **storage probes**, not fabricated API-assigned CR or receipt UIDs or claims that 128/1000 real jobs ran.

`TestRealAPIGenesisV3FullTableLifecycle` is a separate, source-bound opt-in gate.
An external, separately approved installer supplies one new single-node `kind-kova-v3-capacity-*` cluster with the current KovaBuild CRD, three distinct fresh `kova-v3-capacity-*` namespaces, an original unstarted Initializing v3 Genesis and its immutable installation receipt Secret in the control namespace.
Pin the immutable Kind node reference in the external plan and the actual full control-plane Docker ID in the manifest.
The receipt must select active/queue schema 2, active/requester limits 128/128, worker slots 65535, queue/requester limits 1000/1000, an explicit worker-pool identity and an exact runner manifest/index reference.
No Service, worker, runner, registry, workload controller or Pod is started; an old-work List is a veto, never initialization authority.
The installer must attest that no old or competing writer can use these fresh identities.

Both `KOVA_V3_CAPACITY_REAL_API_MANIFEST=/absolute/private/manifest.json` and `KOVA_V3_CAPACITY_REAL_API_MANIFEST_SHA256=<sha256>` are required; partial opt-in fails before networking.
The private strict manifest requires `schemaVersion: 1`, absolute `sourceDirectory`, clean full `sourceCommit`, `testBinarySHA256`, absolute private `kubeconfig`, `kubeconfigSHA256`, exact `context`, `kubeSystemUID`, `controlPlaneID`, RFC3339 `preparedAfter` and `expiresAt` (a fresh window of at most one hour), a new absolute `receiptPath`, `runner`, `control`, `receipts`, absolute `originalReceipt`, `originalSHA256`, `secretName`, `secretUID`, `buildName`, `oldWritersStopped: true` and `retainOnSuccess: true`.
Each namespace object has only its original `name` and `uid`; names, UIDs and paths must not be borrowed from a prior epoch.
The kubeconfig has only the exact dedicated current context and embedded CA/client certificates, verified direct `https://127.0.0.1:<port>`, and no proxy, executable credential helper or redirect.
Source, binary, manifest, Kind identity and immutable receipt pins are checked before use and rechecked at fixed lifecycle checkpoints; namespace/receipt authority is rechecked at product write edges.

Compile a clean reviewed `./internal/service/buildcontroller` test executable, stamping `github.com/cofy-x/kova/internal/version.Commit` to that same full source SHA, and record its build command and SHA256 outside Git.
Only **after separate fixture/operation approval**, run that executable with `-test.run=^TestRealAPIGenesisV3FullTableLifecycle$ -test.timeout=5m -test.v` and the two manifest variables.
Its context is three minutes, individual HTTP calls are ten seconds, and the transport caps 3500 calls and 64 mutations, six-MiB request bodies and two-MiB responses.
All typed and controller-runtime clients share one explicit 20-QPS, burst-20 token bucket; this test-only policy does not change production client limits.
The call budget covers the full, repeatedly direct-qualified lifecycle, not 3500 calls guaranteed to finish within the deadline; timeout still stops and retains the fixture.
Only the two fresh ledger ConfigMaps, one actual API-assigned-UID CR and at most three queue/grant/Pod-intent receipt ConfigMaps can be created.
Genesis and ledger patches require original UID/resourceVersion tests and exact data-only operations; each mutation body has at most one wire attempt, with unknown, redirect, throttle or server-error outcomes stopping this gate.
For the two original owned ledgers, the sealed PATCH path additionally recognizes Kubernetes' exact generic 422 `Invalid` Status with empty-object metadata/details and its fixed message; the product's direct readback must still prove advancement of that same valid original ledger before classifying CAS contention.
The prior nil-details error contract remains unchanged; null is not normalized into this additional empty-object branch.
PATCH old-data and proposed-data CAS values must be JSON strings; null is refused before any upstream call.
The dedicated verified HTTP/1 transport disables proxies, keep-alive reuse, HTTP/2 and redirects so an upstream call cannot hide a transport retry.
Duplicate JSON and unqualified responses stop all later wire calls: named absence needs an exact NotFound Status, and successful mutations must acknowledge the original UID and proposed body; a collection 404 is never an empty List.
Every Pod POST is forbidden, including an unschedulable one.

The gate round-trips maximum v3 ledger bytes, replaces one synthetic storage probe with that one real CR, records genuine queue/grant/Pod-intent receipt UIDs, persists the spent nonce and in-flight issue, proves stale CAS refusal and fence wrap, and holds capacity while the issue is unresolved.
It then settles only the **definitively locally never-sent** Pod issue, performs the product's ordered exact-UID receipt cleanup, verifies active/queue release and idempotent settled reads, and reserves the freed queue slot again.
The direct terminal `Cancelled` status is a test fixture: this is **not** runtime cancellation, Pod admission/defaulting, successful builds, HTTP 429/admission/FIFO, leader handoff, unknown Pod-Create recovery, or loaded Service/client-budget evidence.
The final queue reserve has no CR/effect and is retained as a charged intent.
All namespaces, Genesis, ledgers and the terminal CR remain on success and failure; only ordinary effect receipts may be UID-deleted by the tested product protocol.
An ambiguous deletion acknowledgement stops further writes; unresolved state and append-only private JSONL checkpoints remain for inspection.
For an ordinary current-run-owned receipt, the DELETE acknowledgement accepts only an identity-bound Kubernetes `v1/Status` success (HTTP 200, zero code, `details.name`, plural `details.kind: configmaps`, and the original UID) or a strictly decoded ConfigMap response with that receipt's identity and data. A generic unbound `Success`, null/malformed details, or HTTP 202 does not qualify. The product must still directly read the exact receipt after DELETE and require NotFound before release; the acknowledgement alone never proves absence.
Each successful Create's original API-assigned UID and each effect receipt's canonical data digest are fsynced before any subsequent API call; a failed assignment journal stops the transport.
Every lifecycle checkpoint persists current call/mutation counts; the bounded final journal records measured maximum-state and post-release ledger bytes, the original CR/ledger UIDs, single-attempt upstream call/mutation counts and zero permitted Pod POSTs (not inferred worker/runtime activity).
The failure journal reserves eight KiB within the unchanged 64-KiB journal limit for a safe local timeout/rate-wait enum, final counters, owned UIDs and receipt data digests, never a raw exception or Secret data.
The call count is also the HTTP wire-attempt upper bound, not proof every request was sent: a dial/TLS failure can send no HTTP request.
Never rerun the same fixture as fresh, and never report source/mock PASS or an absent final pass checkpoint as live API qualification.
This narrow gate supplies capacity evidence only; it does not by itself settle the separate deep-queue or worker/client-budget acceptance requirements.

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
| `make e2e-helm-quickstart` | Packages the chart, installs it into an ephemeral minimal kind cluster, runs the authenticated Service workflow, and deletes the cluster on exit. Set `KEEP_KIND_CLUSTER=true` to retain a cluster created by the test, or `REUSE_KIND_CLUSTER=true` to explicitly use a caller-owned cluster that the test will not delete. The default quickstart does not write a Service receipt; set `RESULT_JSONL=.work/result-service.jsonl` explicitly to opt in. | Helm archive, `examples/simple`; optional run-scoped Service JSONL |
| `make e2e-service-admission` | Read-only admission preflight by default; with `ADMISSION_E2E_MODE=run` and a test token, checks two Service Pods on an existing dedicated `kova-admission-*` Kind cluster, then sends a 40-way HTTP burst, preserves run-scoped request/response receipts, and cleans up only its exact CR IDs. Requires preinstalled active=1, global queue=3, requester queue=2, and `never=true` runner selector. | Existing dedicated Kind cluster; no install or cluster-wide cleanup |
| `make e2e-service-admission-failover` | Read-only preflight by default; normal live mode proves leader handoff while a challenger remains queued. An explicit `ADMISSION_FAILOVER_PROMOTION=true` fixture additionally releases the exact blocker and verifies the challenger acquires the sole active grant and Pending runner. Receipts survive ambiguous outcomes. | Same empty, dedicated two-replica Kind admission caps; promotion is restricted further to the sole `kova-admission-pump` Kind on `wayne-hk-kvm`. |
| `make e2e-service-admission-fairness` | Read-only preflight by default; opt-in live mode uses two TokenReview-authenticated callers to prove requester and global queue caps, durable fair rotation across leader handoff, and exact UID cleanup. | Sole dedicated `kova-admission-fairness` Kind on `wayne-hk-kvm`; run receipts under `/data/forge-artifacts/kova-admission-fairness/`. |
| `make e2e-service-auth-read-probe` | Read-only preflight by default; opt-in bounded two-principal empty-list GET probe records TokenReview/SAR API counters, per-request latency, and Kind health without creating a KovaBuild. | Existing idle `kova-admission-fairness` Kind on `wayne-hk-kvm`; private JSON receipts under `/data/forge-artifacts/kova-admission-fairness/auth-read-*/`. |
| `make e2e-service-auth-owner-probe` | Read-only fresh-fixture preflight by default; opt-in bounded owner and denied non-owner GET probe creates one unschedulable build, compares exact TokenReview/SAR counter deltas, and uses UID-preconditioned cleanup. | Newly created, sole `kova-admission-fairness` TokenReview Kind on `wayne-hk-kvm`; private JSON receipts under `/data/forge-artifacts/kova-admission-fairness/owner-get-*/`. |
| `make e2e-service-admission-ledger-loss` | Read-only preflight by default; live mode deliberately deletes only the exact active admission ConfigMap in a disposable dedicated Kind cluster, then proves both Service replicas return 503 for readiness and new submissions without creating a CR. Optional exact follower Pod replacement proves startup does not silently recreate the ledger. Preserves receipts and never repairs the fault. | Same empty, dedicated two-replica Kind admission cluster and caps as above; retire cluster after live mode |
| `make e2e-service-admission-deep-queue` | Zero-command local PLAN by default; explicit check is read-only. The fresh Genesis-v3 short profile tests 100 total charges (1 active + 99 Queued), full rejection, original-UID release/readmission, leader handoff and normal receipt retirement. | An owner-pinned fresh two-node Kind, three distinct namespaces, two same-image Service replicas, one worker, original v3 Genesis/ledger/receipt identities and permanently unschedulable runner. The optional supported-capacity profile measures total charges 100/500/1000, not that many Queued CRs. |
| `make e2e-service-partial-output` | Read-only preflight by default; live mode executes a real two-target OCI+Nydus runner build whose later target fails in BuildKit, then checks final `Failed` and the first target's exact digest-pinned partial receipts. | Existing empty `kova-partial-output-41` Kind on `wayne-hk-kvm`; run-scoped source/output tags and private receipts under `.work/partial-output/`. |
| `make e2e-service-digest-collision` | Read-only preflight by default; live mode holds A's two pushed OCI/Nydus digests in verification while B overwrites the same two tags, then verifies A's partial-failure and B's successful immutable receipts. | Dedicated `kova-digest-collision-41` Kind and loopback-only `kind-registry-digest-41` on port 5004; private receipts under `.work/digest-collision/`. |
| `make e2e-source-capacity` | Read-only preflight by default; live mode publishes one 128 MiB incompressible immutable source, builds one OCI output, verifies its digest and host pull, and samples Kind node/runner state. Preserves run-scoped receipts and tags; never deletes the cluster or registry content. | Existing empty, dedicated `kova-source-capacity` Kind quickstart cluster and localhost registry |
| `make e2e-source-oci-oversize` | Read-only preflight by default; live mode streams one exact 512 MiB + 1 byte sparse ZIP as an immutable OCI source, submits one public Service build, and requires an `InvalidSource` size-limit failure with no output tag. | Existing empty, dedicated `kova-source-capacity` Kind quickstart cluster and localhost registry |
| `make e2e-service` | RBAC isolation, immutable OCI source publication, pre-build target-and-platform contract failure, platform-scoped worker dispatch, safe caller retry, verified output platform and manifest digest, ephemeral logs, TTL cleanup, and host pull. | `examples/simple`, `.work/result-service-service-e2e-<timestamp>-<nonce>.jsonl` |
| `make e2e-crd-upgrade` | Isolated pre-retry CRD to current CRD/controller migration, including live status pruning/persistence, a quiescence gate, and an injected old Starting runner that must fail before POST. | Public `v0.1.0-rc.9` chart and role images, dedicated Kind cluster and registry |
| `make e2e-release KOVA_VERSION=vX.Y.Z` | Downloads and verifies the exact public CLI and OCI chart, pulls matching public role images, then runs the immutable-source Service lifecycle in a clean kind cluster. | GitHub release files and public OCI packages; no Service JSONL by default |
| `make e2e-concurrent` | Multi-image OCI build with worker distribution checks. | generated concurrent examples, `.work/result-concurrent.jsonl` |
| `make e2e-dragonfly-nydus` | Nydus conversion, export, Dragonfly preheat, and Pod startup. | `examples/nydus-smoke`, `.work/result-nydus.jsonl` |
| `make e2e-runtime-preflight` | Local tool and registry readiness checks for runtime validation. | Docker, kind, Helm, kubectl, local registry |
| `make e2e-runtime` | OCI and Nydus service images running behind Kubernetes Services with in-cluster HTTP probes. | `examples/service-oci`, `examples/service-nydus`, runtime result JSONL files |
| `make e2e-observability` | Starts local Compose LGTM, runs a build, and checks Prometheus telemetry. | Grafana/LGTM, `examples/simple` |

Tune the concurrent check with `EXAMPLE_COUNT`, `BUILD_CONCURRENCY`, and
`MIN_BUILDKIT_NODE_IPS`.

The Service E2E shell script writes receipts only when `RESULT_JSONL` is set.
The value is a non-existing base path inside this checkout's `.work/`; each
invocation creates a private `0600` file by adding a unique run ID before the
`.jsonl` suffix. It refuses an existing base/scoped file, symlink, directory,
or path outside `.work/`. Each submitted job records a pre-POST attempt,
confirmed job ID, terminal status, and (for the successful retry) digest-pinned
outputs. A failed or ambiguous POST leaves a `submit_unconfirmed` event and
stops without retrying. The final `verified` event means host pull, TTL cleanup,
and any required rollback also passed. Raw API errors, logs, and bearer tokens
are not copied into the receipt. Keep the private JSONL when debugging; a
terminal result by itself is not proof that the full E2E completed.

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

### Bounded Owned-Build Authentication Probe

The owner GET path needs a real authenticated owner and an existing `KovaBuild`.
Do not reuse the ordinary quickstart Service E2E fixture: that entrypoint explicitly installs static authentication, so it cannot prove TokenReview or SubjectAccessReview behavior.
Use a newly created, sole `kova-admission-fairness` Kind with the two-replica TokenReview overlay and its two test ServiceAccounts; the script neither creates nor deletes the Kind cluster.
The default `check` requires a fresh, empty fairness cursor, zero builds/runners and empty admission ledgers, both exact candidate image bindings, two Ready Service Pods, healthy nodes, Pod/memory/CPU headroom, and no virtual `get servicebuilds` RBAC for either principal.
On a new API server the TokenReview/SAR counter series may not exist yet; this probe accepts an absent review series only when the general API request counter family is present, treats its baseline as zero, and still requires exact review deltas after requests.
The RBAC check impersonates each ServiceAccount username, exact UID, and its standard authenticated and ServiceAccount groups; a name-only impersonation is insufficient to prove denial.
Supply the full SHA of the commit used to build the deployed controller image, even when the checkout contains a later script-only commit:

```bash
ADMISSION_AUTH_OWNER_CANDIDATE_COMMIT=<deployed-full-sha> \
  make e2e-service-auth-owner-probe
ADMISSION_AUTH_OWNER_CANDIDATE_COMMIT=<deployed-full-sha> \
  ADMISSION_AUTH_OWNER_ACK=kova-admission-fairness/kova/kova-service/auth-owner \
  ADMISSION_AUTH_OWNER_MODE=run make e2e-service-auth-owner-probe
```

Live mode uses a short TokenRequest bearer for each test ServiceAccount, retained only in memory.
It submits one run-scoped, unschedulable build through the Service API, waits for its exact `Starting`/Pending runner state, then sends 30 owner and 10 non-owner GETs at two total requests per second across the two pinned Service Pods.
Hard caps are 60 owner GETs, 30 non-owner GETs, four total requests per second, and five minutes before cleanup.
Each next GET waits at least one rate interval after the previous GET completes; slow health samples never trigger catch-up bursts.
The owner must receive only its exact build with zero SAR requests during its measured phase; the non-owner must receive a non-disclosing `403 forbidden` with one SAR per measured request.
Both phases require one TokenReview per measured request; exact API Server counter deltas fail closed on unrelated review traffic or counter resets because these counters are cluster-wide.
Every request has a private allowlisted receipt, and repeated samples enforce Service Pod UID/image stability, healthy nodes, CPU/memory and Pod capacity.
After measurement, only the recorded KovaBuild UID is deleted with a Kubernetes UID precondition; the script waits for the CR, runner, and both ledgers to empty.
If cleanup is interrupted, keep the Kind and use its exact recorded run ID:

```bash
ADMISSION_AUTH_OWNER_ACK=kova-admission-fairness/kova/kova-service/auth-owner \
  ADMISSION_AUTH_OWNER_MODE=recover ADMISSION_AUTH_OWNER_RUN_ID=<exact-owner-get-run-id> \
  make e2e-service-auth-owner-probe
```

An uncertain POST with no matching exact CR remains blocked for operator review; `recover` never deletes a guessed object.
These results describe only this low-rate Kind read path, not build throughput or production SLA.

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

Use a new, otherwise idle Linux Kind fixture; never resume an UNKNOWN fixture or borrow its identities. The probe does not prepare Kind, bootstrap Genesis, push/pull images, create registry tags, or mutate any ledger directly. Existing unrelated Kind clusters may remain, but the owner must qualify host headroom and isolation before preparing the new fixture. Do not delete old clusters to satisfy this test.

The owner supplies a canonical absolute, owner-owned `0600` JSON file (at most 32 KiB), with the exact schema enforced by `scripts/e2e/deep_queue_v3.py`. Its fields are `schema_version=1`, `run_id=v3-YYYYMMDD-<fresh suffix>`, `profile`, exact 40-character `candidate_commit`, `pod_template_binary` and `pod_template_binary_sha256`, `host_boot_id`, `cluster=kova-deep-queue-<run_id>`, private `kubeconfig` and its `kubeconfig_sha256`, `kube_system_namespace_uid`, three distinct run-suffixed `control_namespace` / `runner_namespace` / `receipt_namespace` names and their `_uid` fields, `release`, `auth_secret_uid`, `service_deployment_uid`, `worker_deployment_uid`, original `node_uids` (two nodes), `service_pods` (two), `worker_pods` (one), `role_images` (controller/runner/worker immutable manifest references), `genesis`, and `ports`. No credential value belongs in this file.

`pod_template_binary` is a canonical absolute path to the same clean candidate's Linux/amd64 `go test -c ./internal/service/buildcontroller` binary, linked with the full `internal/version.Commit`; the owner pins its whole-file SHA before running the probe. The bounded, owned executable must not be writable by group or others. The harness first checks its candidate/platform identity in read-only preflight, then sends each directly observed Pod's JSON to the explicitly opt-in, offline `TestE2EPodTemplateDigestStdin` entrypoint. This entrypoint strictly decodes a typed Kubernetes Pod and calls the product's `recoveryreceipt.CanonicalPodTemplateDigest`, without a Kubernetes client or any API operation. Its result binds the exact input bytes, original Pod UID and candidate, and the recomputed template digest must equal the original pre-Create receipt and witness; an unchanged annotation cannot certify changed labels or Spec. The test entrypoint skips by default, and a terminating Pending Pod cannot qualify as the active blocker.

`genesis` contains exactly `secret_name`, `secret_uid`, `receipt_sha256` (original canonical installation-receipt bytes), `genesis_uid`, `active_ledger_uid`, `queue_ledger_uid` and a run-scoped `worker_pool_id`. `ports` contains distinct `service`, `api_proxy` and `metrics_remote` ports. Preparation must install the current Genesis-v3 contract using the normal CLI and chart, not infer original identities from a later probe. The receipt Secret resides in the control namespace, is immutable, contains only `receipt.json`, and is mounted read-only at `/etc/kova/admission-genesis`; the receipt namespace is separate from the runner namespace. Both original schema2 ledgers must be empty and carry the exact v3 installation stamps.

For `representative-short`, the v3 limits are active=1, active-per-requester=1, worker-slots=1, total queued-admission charges=100 and per-requester charges=100. The active blocker retains a charge, so 100 total means **1 active + 99 genuinely Queued**. This profile has one shared 900-second deadline, starting before read-only preflight, and one 30-second steady window. Preflight image-save subprocesses (at most 90 seconds each), other commands, HTTP calls, polling and the live lifecycle all consume that same budget; starting live never renews it. `supported-capacity` instead uses limits 1000/1000 and stages 100/500/1000 total (99/499/999 Queued), with the same end-to-end accounting within 1800 seconds. Neither profile is a production default or SLO certification; short-profile receipts explicitly mark 500/1000-request HTTP performance unmeasured. Unit/fake-client capacity matrices and real storage-lifecycle evidence remain separate evidence, not invented HTTP measurements.

Both Service replicas must use one pinned candidate image and static principal `kova:e2e`, `pollInterval=5s`, `wait=2h`, `maxBuildDuration=2h`, leader election in the control namespace, a loopback-only metrics listener, `runnerNodeSelector.never=true`, and `imagePullPolicy=Never` for every role. The older profiles fix `kubeClientQPS=20` and `kubeClientBurst=40`; the retained full profile below explicitly selects the supported operator configuration 100/200 per Pod. Product defaults, flag ceilings, queue capacity and identity gates are unchanged. The worker is one ready local/preloaded candidate replica. The dummy source and targets use `unused.invalid`; no runner may acquire a node or execute. A real pending runner Pod is created for each admitted active build, but the queued builds remain runner-less. The test therefore measures API admission and queue behavior, not build throughput or registry behavior.

Default PLAN performs no external commands or writes. Explicit check validates the original candidate, fixture bytes, kubeconfig/context, original UIDs, local Docker manifest→config chain, both nodes' CRI identities, unschedulability, readiness, empty ledgers/receipt inventory and metrics. It reads the dedicated `kova-e2e-token/token` into memory only, rejects malformed credentials and never prints or persists them.

```bash
make e2e-service-admission-deep-queue
DEEP_QUEUE_E2E_FIXTURE=/canonical/private/fresh-fixture.json \
  DEEP_QUEUE_E2E_MODE=check make e2e-service-admission-deep-queue
DEEP_QUEUE_E2E_FIXTURE=/canonical/private/fresh-fixture.json \
  DEEP_QUEUE_E2E_MODE=run DEEP_QUEUE_E2E_ACK='<run_id>:<exact fixture file SHA256>' \
  timeout --kill-after=5s 920s python3 scripts/e2e/e2e-service-admission-deep-queue.py
```

Use the shown outer bound for the short profile: 920 seconds for the entire foreground process, including read-only preflight and local port-forward shutdown, followed by at most five seconds before a forced kill. It is not 920 seconds added after preflight. The optional supported-capacity profile needs a separately approved outer bound of 1820 seconds with the same accounting. An outer timeout is UNKNOWN and retains the fixture; a prior result file alone cannot turn it into PASS. Do not use the legacy fixed-cluster detached launcher's `start`, `status` or emergency `stop` as authority for this v3 fixture. The owner controls the one foreground run and its private receipt directory; the entrypoint rejects legacy status/stop modes. There is no automatic Deployment scaling or uncertain-create cleanup on failure.

These duration ceilings are not evidence that a profile can finish. Offline diagnostic tests `TestGenesisHTTPFreshAdmissionCallCountBaseline` and `TestGenesisRuntimeHTTPReadersShareOneRateLimiter` run the concrete admission/receipt path and actual client constructors without a Kubernetes server. The current no-contention, fresh-idempotency-key path observes 113 named GETs, two queue PATCHes, one receipt Create, one build Create and one SubjectAccessReview: 118 calls sharing the HTTP traffic class. Its ten full Genesis checks account for 100 of those GETs. At the default single-Pod 20 QPS / burst 40, admitting 1,000 requests alone therefore has an ideal token-budget lower bound of `(118000 - 40) / 20 = 5898` seconds, before preflight, observation, controller work or retirement. This is source-path accounting, not measured Kubernetes latency or a capacity claim. The counting regression permits safely reduced call counts; it does not make redundant reads a required contract. Do not start the full profile under a shorter frozen budget merely because tool subprocess overhead was reduced. Any operator-selected higher rate must be independently fixed and reported as that configuration, not default-rate performance.

For a finite full matrix without an additional 1,000-object bulk-retirement benchmark, the owner may predeclare `profile=supported-capacity-retained`. It requires the same original 1000/1000 v3 contract and measures **100/500/1000 total charges, with 1 active and 99/499/999 genuinely Queued**. Its fixture `ports` has exactly four distinct fields: `service`, `service_second`, `api_proxy`, and `metrics_remote`. Two independent port-forwards address the two original named Service Pod UIDs; requests alternate **serially**, not concurrently, across their HTTP servers. Each Pod's HTTP and controller traffic classes use the supported 100 QPS / burst 200 operator setting. Leader/readiness classes remain separately limited. This configuration does not certify default 20/40 throughput or justify raising the product's rate/capacity ceilings.

The retained profile fixes `DEEP_QUEUE_E2E_QUIET_SECONDS=0` and a **single 1500-second preflight-plus-live deadline**, with outer `timeout --kill-after=5s 1520s`. The three 30-second observation windows, preflight image saves, all requests, handoff, representative retirement/promotion, planned stop and one shared ten-second local-child shutdown consume that same budget. It never renews the budget after launch. At the current observed 118-call path, two 100/200 HTTP limiters have an ideal 1000-admission token-accounting lower bound near 588 seconds, excluding API RTT, controller work and preflight. This estimate is not a promised completion time. Original host/node resource limits and the existing measured aggregate API ceiling remain unchanged; absence of required evidence or a deadline overrun is UNKNOWN, not permission to increase QPS, extend time or rerun.

During live execution, only the original three namespace GETs and original Genesis/active/queue ConfigMap named GETs use the already-qualified loopback Kubernetes proxy instead of spawning thousands of `kubectl` processes. They remain fresh, uncached, single bounded reads with strict JSON decoding and the same original UID/schema/contract checks; a 404, malformed result or transport failure is UNKNOWN. Read-only preflight still uses the existing command path. The retained profile requires a measured limiter wait above 10 ms specifically from `traffic_class=http,outcome=ok`; leader/readiness waits and failed waits cannot satisfy this gate. An expected application capacity 429 is not an API-server 429; API-server 429/5xx remain forbidden. Reconcile percentiles, Service CPU/memory, total API rate and stage latency are still reported at all three plateaus.

Each accepted request is checked against its original CR UID, immutable spec, queue intent and immutable queue receipt; the blocker additionally requires the grant and Pod-create pre-effect receipts, original Genesis/ledger UIDs, durable CR runner witness, exact runner image and Downward-API UID fence. The original ledger identities and contract are checked throughout. Reconcile and client-rate-limiter histogram receipts report sample counts and percentile **bucket upper bounds**, never exact per-reconcile latency. At least one actual client-limiter wait over 10 ms must be observed. The Service's bounded wire/client counters are separate from API-server request counts, which include the observer and system traffic and must not be attributed wholly to Kova.

After the steady window, the live flow submits one fresh request at the real configured cap and requires the exact nonretrying 429 capacity response, no extra CR, and unchanged original accepted UIDs. It then deletes one originally qualified queued CR with its exact UID precondition, directly confirms that CR and its ordinary queue receipt absent after normal product cleanup, and admits exactly one fresh replacement. A single planned deletion of the original leader Pod is also UID-bound, owner-qualified and same-image; the original Lease UID must survive with a distinct holder, two ready same-image replicas, all unrelated original Pod UIDs preserved and every original queued/blocker UID unchanged. A naturally changed leader before this operation stops the test before deletion rather than adopting it.

Finally it normally UID-deletes all other qualified queued CRs, releases the original blocker, proves an original queued challenger acquires the released slot with a new qualified pending runner and all three receipts, then normally retires that challenger's receipts. PASS requires direct absence of the retired original CRs/receipts/runner, zero CRs/runners, both original empty ledgers and no leftover effect receipts. It never deletes a namespace, Genesis, ledger, Secret, Kind cluster, worker, registry, or image. Every uncertain mutation stops immediately without replay or repair; UNKNOWN is not a product-defect diagnosis or PASS.

That final bulk retirement is **not** part of `supported-capacity-retained`. After the same cap rejection, one original queued UID release/readmission, and original leader UID handoff with full queue recovery, it normally releases only the original active blocker and proves the oldest original queued challenger promotes with the original CR UID, a qualified Pending/never-schedulable runner and all three receipts. All other original queued CR UIDs/specs/queue receipt UIDs remain unchanged. The resulting retained state is exactly **999 total charges: 1 active + 998 Queued, with one free charge slot**, not 1000 and not idle. It reports `all_capacity_released=false`, `bulk_retirement_performed=false`, remaining counts and all retained receipt UIDs.

Only after every preceding gate passes may this predeclared success path stop the original Service Deployment: direct GET must match its preflight UID, labels and spec excluding replicas, and the original/replacement Pod set from the planned handoff. It records that GET's current opaque resourceVersion in a private intent, then performs **one** JSONPatch testing UID, resourceVersion and replicas=2 before changing replicas to zero. No startup RV equality is assumed and no conflict or uncertain ACK is retried. PASS additionally requires direct same-UID/spec replicas-zero convergence, an empty complete owner/labelled Service Pod inventory, direct absence of both original and the qualified handoff replacement Pod names, and unchanged retained ledger/CR/receipt identities. It does not delete the remaining Pending runner, worker, namespaces, Genesis, ledgers, receipts, registry or cluster. UNKNOWN never invokes this stop as automatic cleanup; if its one planned write has already occurred, the actual attempted/ACK/readback facts are preserved without additional writes.

The retained profile covers planned original-leader restart/handoff recovery and observed HTTP client limiting. It explicitly leaves default-rate performance, injected API-server 429 faults, simultaneous restart of all Service replicas, long quiet/soak, bulk retirement and an idle baseline unmeasured. These exclusions must accompany any issue decision; neither a mock trace nor a partial live run is a full matrix PASS.

Receipts remain under `.work/deep-queue/<exact run directory>/`. The original host/node CPU, memory, disk, pressure and measured API-traffic bounds remain in force, including no API-server 429/5xx; the expected Service-level full-capacity 429 is a separate application response. `make lint-scripts` runs the local qualifier and directed truth-flow mocks automatically. These mocks do not qualify a live fixture or replace this real API test.

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

## Isolated Same-Tag Digest Collision Acceptance (#41)

This acceptance is deliberately separate from the ordinary partial-output test and from HK ACK. It runs only on `wayne-hk-kvm` after the host is free of other Kind work. Before creating the fixture, inspect running processes, Docker containers, ports and `kind get clusters`; stop if any owner or task is unclear. The test has its own `kova-digest-collision-41` Kind and `kind-registry-digest-41` container bound only to `127.0.0.1:5004`. Neither setup nor the test may use the shared `kind-registry`/`localhost:5002`, HK ACK/ACR, or Axern images.

After review and authorization, build the frozen candidate's three role images with `localhost:5004/kova:{controller,runner,worker}-$REV` tags and `IMAGE_PLATFORM=linux/amd64` on the KVM host. Use `deploy/digest-collision-kind-cluster.yaml` and `deploy/digest-collision-kind-values.yaml` with the existing `e2e-helm-quickstart` entrypoint, the dedicated registry variables, a unique setup source/output tag, `SERVICE_AUTH_SECRET=kova-e2e-token`, active capacity 2, and `KEEP_KIND_CLUSTER=true`. Only the dedicated registry port may be opened. After its setup build and TTL have drained, upgrade only that Helm release to `serviceDaemon.jobTTL=2h` and the exact `serviceDaemon.registryPlainHTTP` pair `kind-registry-digest-41:5000` and `kova-digest-fault-proxy.kova.svc.cluster.local:5000`. Keep `serviceDaemon.maxBuildDuration=2h` and `serviceDaemon.verificationWindow=5m`. The second host is not created by Helm; the one-shot test creates it after the empty-workload preflight. Do not run the shared-registry quickstart defaults for this fixture.

The reviewed setup shape is below; check the actual KVM state, image SHA and resource headroom immediately before running it. Do not run setup if any Kind cluster, test registry name, or port 5004 owner already exists. The unique setup tag is intentionally outside the collision run's repositories.

```bash
REV=$(git rev-parse --short=12 HEAD)
SETUP_RUN=digest-41-setup-$(date -u +%Y%m%dt%H%M%Sz)
CONTROLLER_IMAGE="localhost:5004/kova:controller-$REV" \
  RUNNER_IMAGE="localhost:5004/kova:runner-$REV" \
  WORKER_IMAGE="localhost:5004/kova:worker-$REV" \
  IMAGE_PLATFORM=linux/amd64 make image
QUICKSTART_KIND_CLUSTER=kova-digest-collision-41 \
  QUICKSTART_KIND_CONFIG=deploy/digest-collision-kind-cluster.yaml \
  QUICKSTART_KIND_VALUES=deploy/digest-collision-kind-values.yaml \
  KEEP_KIND_CLUSTER=true \
  REGISTRY_NAME=kind-registry-digest-41 REGISTRY_PORT=5004 \
  REGISTRY_HOST=localhost:5004 CLUSTER_REGISTRY=kind-registry-digest-41:5000 \
  CONTROLLER_IMAGE="localhost:5004/kova:controller-$REV" \
  RUNNER_IMAGE="localhost:5004/kova:runner-$REV" \
  WORKER_IMAGE="localhost:5004/kova:worker-$REV" COMMIT="$REV" \
  SERVICE_AUTH_SECRET=kova-e2e-token SERVICE_JOB_TTL=60s \
  SERVICE_MAX_ACTIVE_JOBS=2 SERVICE_MAX_ACTIVE_JOBS_PER_REQUESTER=2 \
  SERVICE_MAX_QUEUED_JOBS=2 SERVICE_MAX_QUEUED_JOBS_PER_REQUESTER=2 \
  SERVICE_WORKER_SLOTS=2 \
  SERVICE_TARGET="kind-registry-digest-41:5000/kova-examples/$SETUP_RUN:dev" \
  SERVICE_PULL_TARGET="localhost:5004/kova-examples/$SETUP_RUN:dev" \
  SOURCE_REPOSITORY="localhost:5004/kova-sources/$SETUP_RUN:dev" \
  make e2e-helm-quickstart
helm upgrade kova ./charts/kova \
  --kubeconfig .kind/kova-digest-collision-41.kubeconfig -n kova \
  --reuse-values --wait --timeout=180s \
  --set-string serviceDaemon.jobTTL=2h \
  --set-string serviceDaemon.maxBuildDuration=2h \
  --set-string serviceDaemon.verificationWindow=5m \
  --set-json 'serviceDaemon.registryPlainHTTP=["kind-registry-digest-41:5000","kova-digest-fault-proxy.kova.svc.cluster.local:5000"]'
```

The isolated preflight is read-only, requires a clean checkout and matching Linux CLI/role-image revision, one exact Kind, 2/2 healthy nodes, empty build/runner/admission state, exact image IDs and Service/worker values, test token Secret, the pinned loopback registry, and no orphan proxy Pod or ReplicaSet. Run it before considering the opt-in live mode:

```bash
REV=$(git rev-parse --short=12 HEAD)
DIGEST_COLLISION_EXPECTED_REVISION="$REV" make e2e-service-digest-collision
DIGEST_COLLISION_EXPECTED_REVISION="$REV" \
  DIGEST_COLLISION_MODE=run \
  DIGEST_COLLISION_ACK="kova-digest-collision-41/kova/$REV" \
  make e2e-service-digest-collision
```

Live mode first pins the only Ready Service verifier Pod's Deployment owner, Pod UID and unique Pod IP. The proxy faults GET-by-digest only when the actual TCP peer is that pinned Service Pod IP; runner-side Nydusify reads, blob uploads, manifest PUTs and tag HEADs remain forwarded. Pod UID/IP or proxy Pod UID drift aborts the run. The proxy records a bounded source-IP/digest/status history; before host probes, the acceptance requires actual 503 responses for A's two digests from the Service Pod and a forwarded 200 for A's OCI digest from a non-Service peer. Host port-forward probes must obtain digest-matching 200 responses even while the verifier-only fault is active. No shell, debug container or forged HTTP header is used to impersonate the verifier.

The run publishes a unique immutable source and submits A with `format=both`, one successful OCI/Nydus target and a later BuildKit `/missing` failure. It requires A to stay in `FailedVerifying` with its own two durable `pushedDigest` values and no output; the runner's exact `/missing` failure export must be captured. The proxy is then narrowed to fault only A's two immutable digests, with its Deployment, Service and Pod UIDs, Service selector and ConfigMap proxy-code SHA unchanged. B publishes a different immutable source and pushes both OCI/Nydus outputs to A's same tags while A remains pending. B must reach verified `Succeeded`; both shared tag HEADs must now equal B's different digests, its two verifier-source reads must be forwarded with 200, host probes must return digest-matching 200 responses for both A and B, and A's older manifests must still be readable by digest from the dedicated backend. The proxy is restored to healthy through the same exact-UID ConfigMap mode gate. A must finish `Failed/BuildFailed` with two partial outputs bound to A's digests and two recovered verifier-source reads; B's two outputs must remain bound to B's digests. Both public results and immutable refs must agree. A missed overlap, missing old digest, proxy write failure, unexpected object, digest drift, resource guard, or timeout is an inconclusive/failed acceptance, never a silent success.

The run has a 20-minute pre-recovery deadline, a bounded verification recovery wait, per-node and host headroom guards, and exact candidate image checks. Evidence under `.work/digest-collision/<run-id>/` includes source archives, exact target names, projected pending/overlap/final statuses, runner failure proof, proxy identity/mode, public results, registry digest comparisons, and UID-preconditioned CR deletion receipts. On success it removes only A and B CRs; the healthy proxy, dedicated Kind, registry, tags and evidence remain for inspection. An abnormal or uncertain outcome retains the isolated fixture and attempts only proven UID-preconditioned emergency stops. Do not retry or retire the fixture until its receipts are reviewed. Old-digest deletion/GC is intentionally **off** in this real-runner test; the cluster-free regression separately proves fail-closed behavior when an overwritten digest becomes unavailable. This is a functional race acceptance, not a throughput or production SLA test.

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

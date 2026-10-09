# Issue #51: bounded Service burst attribution

`scripts/e2e/e2e-service-burst.py` measures one finite workload on an already prepared, isolated, run-owned Linux Kind fixture.
It does not create a cluster, namespace, deployment, source, registry or retention service.
The default `check` validates local contracts and artifact pins only; it makes no live API call and cannot certify fixture readiness.
One approved `run` contains one warmup and four serial waves: cached 8, cached 12, uncached 8 and uncached 12.
These are 41 distinct single-target OCI jobs, not a soak or a production capacity/SLA qualification.
The injection deadline is 1,500 seconds, including at most 300 seconds for source verification.
Warmup and each measured wave have independent 120/240-second limits; exact-UID cleanup has a separate 300-second limit.

## Measurement meaning

Plan/runtime schema 2 replaces the old test-branch's permanently missing `buildkit_execute_ms` and inferred `worker_occupancy` checklist with actual, precisely named observations.
The target wall interval still includes BuildKit execution, export and push.
The runner's bounded rawjson observation supplies vertex/export/push interval unions, cached interval counts and export/push overlap.
These unions can overlap, and the vertex union includes exporter vertices.
Do not add the unions or subtract push from target/vertex wall time to manufacture pure execution time.
The coordinator copies this internal v1 observation from the exact KovaBuild UID's `status.verificationResults`, matching its target, platform and pushed digest to the public verified output.
It saves these finite allowlisted observations before deleting any CR; it does not extend public SDKs or store raw progress traces.
Missing or malformed optional telemetry cannot change a successful artifact into a failed build, but it cannot satisfy attribution.

Per-worker occupancy comes independently from the pinned BuildKit v0.31.2 daemon's [read-only history API](https://github.com/moby/buildkit/blob/v0.31.2/api/services/control/control.proto).
The [solver emits STARTED after creating and validating its job](https://github.com/moby/buildkit/blob/v0.31.2/solver/llbsolver/solver.go), before frontend solve execution.
The [history queue tracks those active original Solve Refs and removes them on COMPLETE](https://github.com/moby/buildkit/blob/v0.31.2/solver/llbsolver/history/buildhistory.go).
The coordinator uses the image's `buildctl debug workers` and `debug histories --format '{{json .}}'` against each exact worker Pod's loopback listener, not a load-balanced Service.
These are ListWorkers and ListenBuildHistory calls, never Solve, Session, Prune or UpdateBuildHistory.
It binds each record to the original worker ID, Pod UID, container ID, image ID and container start time, and joins its unique exporter target and pushed digest to the exact campaign build UID.
Its finite snapshots retain only original Ref, target, timestamps and digest; arbitrary attributes, error messages, logs and trace descriptors are discarded.
A history snapshot may legally contain both active and completed records for the same Ref because those snapshots are not atomic; matching identity is merged, not counted twice.
Final completed histories cover solves shorter than the ten-second sampling interval.
Repeated solves for one target, missing completion or missing history remain incomplete, not inferred as one accepted solve per CLI invocation.
Half-open CreatedAt–CompletedAt intervals reconstruct peak accepted non-internal solve occupancy on each daemon's own clock; no cross-daemon-clock peak is claimed.
[COMPLETE recording includes history finalization](https://github.com/moby/buildkit/blob/v0.31.2/solver/llbsolver/history.go), so this is daemon solve lifetime, not pure CPU busy time.
BuildKit's [transport Session manager is a different private map](https://github.com/moby/buildkit/blob/v0.31.2/session/manager.go); its original Session IDs are not exposed by this history API and remain explicitly unavailable.
Internal solves are not included in build history; this profile concerns the 41 external single-target build solves only.

## Operator-supplied contracts

Create the private contracts and all fixture resources outside Git after approving their exact identities and write/cleanup scope.
Schema 2 is intentionally incompatible with the older unpublished schema 1 plan/runtime; regenerate those documents rather than silently interpreting old successful jobs as complete attribution.
All JSON documents reject duplicate keys and unknown contract fields and retain canonical SHA-256 cross-pins.

- `plan.json`: schema version 2; exact full candidate commit and RC or matching `dev-<sha>` version; pinned CLI/chart/values/source-template and immutable base/role image references; exact host/boot, dedicated `kind-kova-issue51-<run_id>` context, kubeconfig SHA and kube-system UID; three distinct run-scoped control, runner and recovery-receipt Namespace UIDs; Deployment generations/templates, Service/worker Pod UIDs; worker Service and worker configuration ConfigMap UIDs and exact `buildkitd.toml` SHA; dedicated registry image/container/volume/network identities; and frozen budgets/checklist.
- `sources.json`: schema version 1 with 41 ordered deterministic source/output tags, OCI source manifest and ZIP blob digests, the 32 MiB payload member and SHA, and declared cache cohort. Warmup/cached jobs share one payload; the 20 uncached jobs have distinct payloads. Intended cohorts are not themselves observed cache hits/misses.
- `runtime.json`: schema version 2 with canonical plan/source SHAs; exact Service/worker names and UIDs, headless worker Service/port, `worker_configmap_name`, registry identities and loopback URL, requester, API/metrics ports, original Genesis Secret/ConfigMap/ledger UIDs and receipt hash, and the externally assigned `genesis.worker_pool_id`.
- `counter-schema.json`: a frozen SHA-pinned checklist only, never observed evidence. Its required names are the exact schema-2 `REQUIRED_COUNTERS` vocabulary in the coordinator.

The immutable Genesis v3 receipt must pin the original fresh runner/recovery-receipt Namespaces, external worker pool and actual runner manifest reference.
Both Service Pods must mount that same receipt Secret and use the exact pool, runner digest, namespace, requester, caps and private worker route.
The worker headless Service and its complete EndpointSlice set must reach exactly the three pinned Ready workers, with no foreign route, DNS override or entrypoint override.
The worker configuration must contain exactly one explicit `[history]` section with `maxAge = "1h"` and `maxEntries = 64`.
Prepare and freeze that configuration before starting the fresh worker containers; unchanged currently mounted bytes alone do not prove what a previously running daemon loaded.
The 64-record budget fits all 41 jobs on one worker; the one-hour retention exceeds this profile's total run/cleanup window.
The observer requires BuildKit version `v0.31.2` and official tag commit `e42e1bfd389af7203238cce77b1f7dad447285e9`.
Every worker must have exactly one daemon worker ID, zero restarts, and empty history before the first POST.
Each history read is bounded to 1 MiB, 82 records (up to 41 active-plus-completed duplicates), and 64 KiB per record.
No more than 41 unique accepted Refs may exist across the campaign.
All role incarnations, configuration pins and worker histories are checked repeatedly; a failure or limit never becomes an empty/zero sample.

The fixed workload caps are two Service replicas, three workers, active/worker-slots 12, global queue 1000, controller concurrency 8 and Kubernetes client 20/40.
Requester caps must be explicit and no larger than those global caps.
Leader/readiness budgets stay 5/10.
The fixture owns its registry container, exclusive volume, repository and all 41 source/output tags; do not point it at the existing local or shared registry.
Prepublish and verify sources separately; this coordinator reads them only.

## Invocation and outcomes

`check` hashes the supplied artifacts without inspecting the live host:

```sh
python3 scripts/e2e/e2e-service-burst.py check \
  --plan /private/issue51/plan.json --runtime /private/issue51/runtime.json \
  --sources /private/issue51/sources.json --cli /private/issue51/kova \
  --chart /private/issue51/chart.tgz --values /private/issue51/values.yaml \
  --source-template /private/issue51/source-template.zip \
  --counter-schema /private/issue51/counter-schema.json \
  --kubeconfig /private/issue51/kubeconfig
```

Only after independent approval of the frozen Linux fixture, source publication, worker read-only Exec observations, finite load and exact CR cleanup, use the same arguments with `run`, adding:

```text
--token-file /private/issue51/token \
--out-dir /absolute/path/to/kova-checkout/.work/issue51-<fresh-id> \
--ack <run_id>/<control_namespace>/<runner_namespace>/<first-12-characters-of-plan-sha>
```

The token file must be private and contain one bearer; the new output directory must be a direct child of the existing checkout `.work/`.
No bearer or unbounded job logs are retained.
Receipts contain one-attempt submissions, exact CR UIDs/results, phase observations, worker-side solve histories/joins, Pod placement/requests, node/worker usage, per-Service API/client metrics and bounded stage logs.
The measurements include observer overhead; they are not an exclusive product-only API or CPU profile.

Exit 0 `COMPLETE` requires 41 exact successful build/output identities, all 41 complete phase observations, 41 unique closed worker-side solves with matching output digests, required API/client and startup/target observations, and proven exact cleanup.
It means this finite attribution is complete, not that #51, another issue, an RC gate or a production capacity claim is automatically approved.
Successfully built jobs with missing/ambiguous optional attribution produce `INCOMPLETE` (exit 4).
An identified build failure produces `FAILED` (exit 1).
An uncertain POST, unreadable measurement, observer failure/limit, changed identity/ownership or ambiguous cleanup produces the stronger `UNKNOWN` quarantine (exit 3).
Contract refusal before run uses exit 2.
Unknown writes are read once by exact deterministic CR ID but never retried.

Only fully identified attempted CR UIDs are eligible for server-side UID-preconditioned cleanup.
The final bounded timing reads are qualified before any CR cleanup; an unreadable measurement or changed identity retains all accepted CRs, including when an identified build already failed.
Each DELETE success body must be duplicate-safe JSON identifying the same KovaBuild GVK/name/namespace/UID and build contract, or a Kubernetes Success Status with the exact UID/name/group/resource and matching or omitted/zero HTTP code.
An empty, malformed or foreign success response quarantines immediately, before any later DELETE.
The coordinator then requires two empty runner/build/ledger reads and a final identity preflight.
It never deletes Namespaces, registry/source/output tags, ledgers, unknown CRs or the Kind cluster.
Unknown outcomes retain the private receipts and fixture for manual exact-identity review.
Do not reuse an incomplete or quarantined fixture as a fresh install.

Pure-local safety regression (no Kubernetes, registry or build workload):

```sh
python3 -m unittest scripts/e2e/tests/test_service_burst.py
ruff check scripts/e2e/e2e-service-burst.py scripts/e2e/tests/test_service_burst.py
ruff format --check scripts/e2e/e2e-service-burst.py scripts/e2e/tests/test_service_burst.py
```

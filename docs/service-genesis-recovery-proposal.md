# Fresh Admission Bootstrap Recovery Proposal

Status: local candidate for [#57](https://github.com/cofy-x/kova/issues/57); bootstrap and runtime are wired and deterministic checks pass, but isolated product acceptance and committed-loss recovery remain incomplete.
The existing two-ledger admission contract remains authoritative until this protocol, [#58](https://github.com/cofy-x/kova/issues/58), migration, and isolated runtime gates pass.
Do not use this note to repair a released or live namespace.

## Decision and trust root

Retain the separate active and HTTP-queue ConfigMap ledgers and add one externally provisioned Genesis ConfigMap.
This deliberately revises the September 28 issue-comment preference for one unified quota ledger: the current #58 candidate independently budgets at most 128 active grants and 1000 queue intents under separate 768 KiB guards.
The earlier #58 schemas measured separately encoded synthetic maximal states of 434,767 and 503,066 bytes; their 937,833-byte sum already exceeded a 768 KiB unified guard by 151,401 bytes.
Those historical sizes are not the v3 receipt-instrumented bounds: the current maximal active test, including grant and Pod receipt fields, measures 709,253 bytes under its 776,192-byte reserved bound and 786,432-byte data guard.
A unified wire shape, server acceptance, metadata/cleanup headroom, and CAS contention remain unproved.
Keeping the two current CAS domains avoids making every queue mutation compete with every grant, Pod nonce, and cleanup fence.
The numbers are candidate measurements; the separate #58 maximal payloads round-tripped on an isolated API server on 2026-10-02, but a unified wire shape, contention, and product recovery remain unmeasured. Reconsider this choice if those measurements justify unification.

The environment installer, not Kova, first creates previously unused, uniquely named and distinct runner and recovery-receipt Namespaces, then an `Initializing` Genesis named `kova-service-admission-genesis` in the runner Namespace.
After direct named reads, the installer publishes a trusted installation receipt to the Service Deployment, for example through an immutable Secret mounted as a file.
The receipt contains the **original** runner and recovery-receipt Namespace names and UIDs, Genesis name and UID, a random installation generation, the active and queue ledger names and schema versions, and all five capacity limits (`maxActiveJobs`, `maxActiveJobsPerRequester`, `workerSlots`, `maxQueuedJobs`, `maxQueuedJobsPerRequester`).
Contract v3 also pins the externally assigned `workerPoolID` and canonical runner `repository@sha256` OCI manifest/index reference, and requires active ledger schema 2 plus queue ledger schema 2.
All replicas must use those exact execution identities; v1/v2 receipts cannot be adopted or rewritten in place.
The capacity identity is not derived from a BuildKit endpoint and does not itself prove physical worker retirement.
These identifiers are not credentials, but the receipt's source and write access are part of the installation trust boundary.
A Kova process must have the receipt before it starts; it never manufactures a receipt from a discovered object or an empty List, creates/replaces Genesis, or follows a changed UID.

Genesis has one strictly decoded `genesis.json` data value with exactly the receipt's two Namespace UIDs, worker pool and runner image identities, generation, ledger schemas, and limits, plus `phase`, `activeLedgerUID`, and `queueLedgerUID`.
The externally created `Initializing` Genesis starts with both ledger UIDs empty and treats an absent/nil `immutable` field and explicit `false` as semantically equivalent.
While still `Initializing`, each ledger UID may only move once from empty to the first directly observed canonical empty object UID; these are durable provisional pins, not admission authority.
`Committed` requires both nonempty provisional UIDs to match the actual ledgers and explicit `immutable=true`.
Genesis metadata UID must equal the receipt; both Namespaces must retain their receipt-pinned UIDs, be Active, and have no deletion timestamp.
Reject unknown/missing/duplicate fields, extra ConfigMap data, changed limits/schema/generation, unexpected immutable state, deletion, or replacement.
Receipt/Genesis disagreement is an actionable refusal, not an invitation to discover a new installation.

## Bootstrap and monotonic commit

Run one shared startup gate before either the controller manager or HTTP listener begins work.
Any replica may continue the original `Initializing` generation after another replica crashes, but none may admit a CR, create a runner Pod, submit to a runner, complete cleanup, or return ready until the commit is qualified.
Bootstrap uses direct named API reads, never cache, List emptiness, lease expiration, or elapsed time as authority.
CR/runner Lists remain one-way vetoes for observed pre-existing work; external old-writer quiescence is an installation precondition, not inferred from those Lists.

Each missing ledger may be created only while the original Genesis is `Initializing`, with its own canonical empty schema-validated state and annotations binding the original Namespace UID, Genesis UID, generation, ledger role/schema, limits digest, and a random per-Create attempt nonce.
A direct read observing the exact attempt nonce proves an uncertain Create was persisted; an absent read does not prove it cannot arrive later.
A peer may adopt an already-created ledger only when the original Genesis still qualifies, any existing provisional UID pin equals that object's UID, and the entire object is a canonical empty state with matching pins.
A peer's nonce is provenance, not an exclusive lease or permission to replace a ledger.
Nonempty, malformed, or differently pinned state refuses; ledger deletion is prohibited, and any observed UID replacement also refuses rather than being normalized as fresh state.
After first observation, one Genesis UID/resourceVersion CAS records the role's provisional UID only if that field is still empty; an already equal pin may be reused, but a nonempty different pin is never overwritten.
An unknown pin-write response requires direct reread of the original Genesis and the original ledger before continuation; a missing read is not proof a delayed pin cannot commit.
The irreducible pre-pin crash plus privileged ledger delete/recreate boundary still depends on no precommit side effects and the installation rule forbidding ledger deletion; this protocol does not claim resistance to an evidence-erasing privileged actor.
An `AlreadyExists` response triggers fresh qualification, never overwrite or deletion.

Once both real ledger UIDs are directly observed, their matching provisional pins are durable, and both states are still canonical empty, one conditional Genesis JSON Patch tests its original UID, current opaque resourceVersion, and exact original `genesis.json` value (including `Initializing`, both pinned UIDs, and the fixed contract), then atomically sets `phase=Committed` and `immutable=true` without changing either UID.
The patch must be constructed for the actual API representation of absent/nil versus explicit false `immutable`, using only operations proven by a real API test; it must not assume `/immutable` already exists or silently ignore a failed test.
A conflict causes a fresh direct read, not a blind retry.
An unknown commit response is resolved by rereading the **original** Genesis UID: only its exact committed pair and unchanged contract qualify.
A successful response is likewise followed by direct qualification before work begins.
The committed Genesis data cannot be changed in place under Kubernetes' immutable-ConfigMap contract; a privileged deletion/recreation changes UID and fails the receipt pin.
On 2026-10-02 an isolated Kind mechanism check accepted this conditional data-plus-`immutable=true` JSON Patch for both absent and explicit-false `immutable`, kept the original UID, rejected a stale resourceVersion patch without mutation, and rejected post-commit data edits. That check did not exercise Kova's receipt, two-ledger bootstrap, crash recovery, or runtime gates.

After `Committed`, missing or replacement Genesis, Namespace, or either ledger always refuses admission and cleanup completion.
Never reconstruct a ledger, rebind a UID, reset a phase, or copy work into a new generation.
A delayed old ledger Create after a committed ledger is deleted may produce a new object, but its new UID cannot satisfy the committed binding.
No late Create result permits capacity release.
The active and queue ledger writes remain separate: a grant may temporarily count in both, and that conservative handoff remains intentional.

## Runtime gates and non-atomic limits

Use one direct client-go qualification path for the original Namespace, Genesis, and both exact ledger UIDs and schemas.
Every ledger mutation must be conditional on its own current UID and opaque resourceVersion; do not rely on a name-only Update or a successful 2xx without directly checking the exact persisted proposal before a new side effect.
The current client-go `Update` paths must preserve nonempty original UID and current resourceVersion and pass a real-API replacement-race test; if the API does not enforce both, replace them with conditional JSON Patch and its named RBAC verb before calling the implementation complete.
Requalify on conflict and before each new CR/Pod/runner side effect.
A read sandwich is only change detection, not an atomic snapshot, lease, or reusable permission: Namespace/Genesis/ledgers may change after the reads, while already-sent name-addressed requests may finish later.
RBAC must deny ordinary ledger/Genesis deletion, and any privileged mutation outside the protocol remains an explicit operator incident.

The gate must cover all current entry points, not only `/readyz` and HTTP POST:

| Path | Required behavior |
| --- | --- |
| Service startup and `/readyz` | Manager and listener begin only after qualified `Committed`; readiness directly checks original identities and both ledgers without mutation. |
| HTTP reserve, replay, and CR Create | Qualify before queue CAS and again before the one authorized CR Create. Preserve an unknown CR Create nonce/intention; read-only GET may remain available during refusal. |
| Admission pump and queued reconcile | Qualify before selecting/waking or granting, including direct/admin CRs that intentionally bypass the HTTP queue quota. Active grant and queue release use their existing conservative order. |
| Pod begin, Create, and `Starting` | Qualify before the active-ledger nonce CAS and before Pod Create. A direct/admin or recovered `Starting` CR needs an exact live, nonclosing grant for its CR UID/name/requester/slots and an owned Pod/nonce; Pod presence or status alone cannot authorize runner submission. |
| `Starting`/`Running` observation and verification | Never issue a new runner POST without the qualified original grant. For an already accepted job, continue read-only runner status observation and registry digest verification; persist an exact result only with independent durable proof of the original grant, CR UID, and owned Pod/request identity. If that proof is unavailable, retain an unknown/recovery-required outcome rather than invent success or discard the observed evidence. |
| Cancel, terminal, delete, and cleanup | Under a healthy original pair, read the exact accepted runner status before an annotation-driven cancel or timeout; a completed result wins, while an uncertain read retains the charge. Fence the exact bound active ledger, persist and directly read back a stop intent bound to the original CR/Pod UID and request before UID-preconditioned Pod deletion. A direct Pod `NotFound` completes only that durable stop or an already durable terminal receipt; a bare `NotFound`, missing/mismatched ledger, unresolved Create nonce, or changed Pod retains charge/finalizer and recovery evidence. Pair loss blocks cleanup completion even after a stop intent. |

All replicas must use the same receipt and limits.
Before any bootstrap write or manager/listener start, runtime wiring must assert `cfg.Namespace == receipt.Namespace`, the separately configured recovery-receipt Namespace equals its original contract name, both Namespaces retain their pinned UIDs, and all five configured capacities equal the receipt; a commit in Namespace A must never coexist with legacy admission in Namespace B.
Construct the Genesis direct clientset with the existing `singleAttemptWrites(rest.Config)` transport (or an equivalent one-wire-attempt transport), and test Retry-After plus unknown POST/PATCH responses; an arbitrary client-go clientset may replay a mutation.
In Genesis mode, disable or remove the legacy `Server.Start` call to `initializeAdmission`; merely prepending `EnsureFresh` allows a committed pair deleted in the handoff window to be recreated by the old bootstrap path. Test loss of both committed ledgers between `EnsureFresh` and listener/manager start and require refusal without any replacement.
A Genesis implementation must stamp the original Genesis and both ledger UIDs, CR UID, and durable Pod-Create nonce on the Pod before Create, then directly observe its Pod UID and persist the stable runner request identity with the `Starting` CR before any runner POST; these pre-loss facts are the independent witness for later evidence-only result recording if a ledger disappears.
An exact terminal result may be recorded against that witness while cleanup remains blocked; missing witness keeps the result unknown rather than losing an already accepted job or fabricating success.
The stop intent is sticky across a lost status-write or Pod-delete response: a new leader directly rechecks the original CR witness and intended outcome, retries only the original Pod UID, and requires direct same-name absence before recording the forced-stop terminal status. A same-owner replacement Pod is not the original witness. Explicit CR deletion requests discard the result and use this stop path; annotation cancellation and timeout first observe the exact runner status.
A successful direct original-pair/grant/Pod-witness check authorizes the next runner operation, but it cannot atomically revoke an operation already in flight when a ledger is deleted after that check. Treat the response as unknown or possibly accepted, never as permission to retry or release charge without independent evidence; do not claim that ConfigMap deletion instantaneously stops runner work.
Runner Exec is addressed by Pod **name**, so checking the Pod UID only before and after Exec detects a replacement too late to prevent a delayed POST from reaching it. Before enabling Genesis routing, the runner transport must reject an expected original Pod UID mismatch inside the target container, before opening the daemon socket or input; Genesis Pods must require this fence, while legacy runner behavior stays unchanged.
A committed read does not prove a later side effect is atomic with Genesis; the durable per-ledger CAS/nonce/fence rules remain essential.
Namespace UID checking cannot fence an already-sent CR or Pod Create addressed only by Namespace **name**.
Never reuse a runner Namespace name while an old writer or delayed request may still target it.

## Installation, RBAC, and recovery boundary

The installer records the original Namespace/Genesis UIDs and receipt outside runtime before starting Service replicas.
The Service account needs direct `get` for both exact Namespaces and the installation receipt Secret, named `get`/conditional `patch` for Genesis and both ledgers, plus ledger Create permission in the runner Namespace. The separately pinned recovery-receipt Namespace grants dynamic ConfigMap `create`/`get`/`delete` only there; runner Namespace RBAC must not grant broad ConfigMap `get` or `delete`, and must not grant ledger or Genesis `delete`.
The candidate uses UID/resourceVersion/data-tested JSON Patch for ledger CAS, so its named RBAC grants `patch`, not the old `update` verb.
Kubernetes RBAC cannot restrict `create configmaps` by `resourceNames`, so the application must fix the two allowed names and validate every object; installation and other privileged actors must be separately controlled.
The installer must withhold new submission/direct-admin routes during bootstrap.
A bypassing direct CR is not trusted merely because it exists, especially if it arrives in `Starting` phase.

For upgrade or reviewed recovery: stop ingress and direct/admin writers; drain old queued and active work, verify old runner Pods are gone, and stop **every** old HTTP/controller/leader binary before provisioning.
Keep the old runner and recovery-receipt Namespaces, ledgers, and receipts as evidence.
Use different, never-before-used runner and recovery-receipt Namespace **names**, provision the new Genesis and installation receipt externally, deploy only new binaries with the new route, verify the exact committed UIDs and readiness, then reopen traffic.
Apply any required CRD/schema changes before the new Service starts.
Do not perform an in-place rolling upgrade, reuse the old name, let old and new binaries overlap, or silently accept old schema/caps.
If old work cannot drain, keep it isolated and let callers resubmit immutable inputs only when safe.
Rollback also requires a reviewed drained namespace boundary, not pointing an old binary at the new ledgers.

## Acceptance gates before closing #57

- Deterministic failpoints around every ledger Create, provisional UID pin, and Genesis commit request/response: no ledger, either single ledger, both before commit, lost Create/pin/commit response, crash/restart, stale/conflicting peers, and a delayed old Create after commit. Fresh valid intent must eventually commit exact UIDs; invalid/ambiguous or previously committed-loss state must refuse without a replacement.
- Deterministic side-effect gates for a direct/admin `Queued` and `Starting` CR, cached stale reconcile, existing grant reuse, Pod begin/late Create, runner POST, evidence-only observation and exact-result preservation after ledger loss, cancellation, finalizer cleanup, committed loss of one/both ledgers, Genesis or Namespace replacement, limit/schema drift, and unknown writes. Preserve the #44 no-overbooking, fairness, idempotence, and nonce/fence regressions and merge/re-run #58 capacity and cleanup-headroom checks.
- Static chart/RBAC/receipt and migration checks, focused race tests, and an operator runbook that records exact UIDs and refusal reason without deleting evidence.
- Narrow isolated Kind acceptance against exact candidate images and owned resources: pause/crash between writes, restart and leader/startup handoff, uncertain responses, late old writer, direct `Starting` bypass, and committed-ledger loss. Require exact identity/UID receipts, zero excess work, bounded actionable refusal for only unsafe states, and evidence-preserving cleanup. Do not inject faults into HK, touch the preserved old ledger-loss fixture, or call synthetic/fake-client tests real crash recovery.

Current implementation checkpoint: the Service entrypoint requires v3 Genesis authority, and the read-only CLI helper renders create-only Genesis and immutable receipt Secret manifests.
The fresh candidate records HTTP queue, active-grant, and Pod-Create receipts in its distinct pinned recovery-receipt Namespace before their respective effects.
The grant nonce, grant fence, directly observed opaque ledger resourceVersion, and receipt UID/digest are durable; only the fresh CAS owner can issue the receipt Create.
Pod creation spends a permanent attempt nonce, arms the canonical template digest, pins the receipt, then commits a separate in-flight issue fence before its sole Create.
Restarts observe uncertain attempts without replaying them, and normal terminal cleanup settles the Pod receipt before the grant receipt and finally the HTTP queue receipt.
The queue quota stays charged through admitted active work until exact terminal cleanup; this is more conservative than the earlier active-grant handoff.
The pre-loss CR status witness also pins the full canonical Pod template digest, so evidence-only result observation after ledger loss cannot trust a changed image/spec or a restamped annotation.
The Kind-only external installer creates fresh identities and keeps private installation receipts; it never adopts or repairs an existing namespace.
The Service requires the original Secret UID and exact mounted bytes before startup and each bootstrap write, with separate controller, HTTP and readiness Guard clients that retain one original binding without sharing rate-limit budgets.
Deterministic fake-API tests cover partial fresh `Initializing` restart, exact ledger templates, runtime side-effect fences, accepted-result preservation and terminal-first stop; they do not prove that all external writers were stopped.
An opt-in registry-free real-API gate covers Genesis/ledger conditional writes and the witness/stop-intent serving-CRD round-trip, but adding or compiling that gate is not a live PASS.
If an original committed ledger is lost, terminal and Delete cleanup deliberately retain the charge/finalizer; the three pre-effect receipts do not by themselves authorize disposal or release capacity.
The separate `recoverydisposal` package validates externally signed, independently pinned disposal and closure evidence; verification alone is not execution permission.
Its opt-in exact-object library executor additionally requires a separately verified mutation grant and archived body qualification after both original namespaces are Terminating.
The separate opt-in `kova-recovery dispose-exact` operator executable now supplies bounded artifact loading, independently pinned API/TLS connection identity and explicit grant-gated invocation of that executor; it is not registered in the ordinary user client or Service startup.
It still provides no signer, namespace deletion, route writer, capacity-release method or physical-retirement detector, and is not a complete incident procedure.
Its signatures authenticate the exact record, not the truth of namespace retirement, complete inventory, durable result escrow, or one-successor capacity transfer.
The source-bound actual Service control-plane gate is implemented and covered by local safety tests, but still requires separately approved fresh real-API fixtures; it does not qualify runner execution or full drain.
That separate recovery gap remains a hard no-go for the next RC.

Sources for the proposed Kubernetes mechanisms: [API resourceVersion and conditional JSON Patch](https://kubernetes.io/docs/reference/using-api/api-concepts/#updates-to-existing-resources) and [immutable ConfigMap behavior](https://kubernetes.io/docs/concepts/configuration/configmap/#configmap-immutable).

# Service Admission Design

Issue [#44](https://github.com/cofy-x/kova/issues/44) remains open.
The current controller slice serializes active grants in a namespaced ConfigMap, `kova-service-admission`.
Each grant records the build UID, requester, and worker-slot allocation through a resourceVersion compare-and-swap before runner Pod creation.
The controller reads the ledger and queued builds directly from the API server, rather than from its informer cache.
On first use it adopts active build status and matching runner Pods; a later active build or runner Pod missing from the ledger blocks new grants.
An uncertain ledger write never authorizes a Pod: a retry or replacement leader first reads the recorded grant.
Before every Pod Create, the controller persists a unique in-flight nonce in that grant and stamps it on the Pod.
Only a definitive Create response or observation of the Pod bearing that nonce resolves the attempt.
Terminal and deletion reconciliation retain the grant until the matching runner Pod is confirmed gone and no in-flight attempt remains.
Ownership mismatch or malformed ledger state blocks release and requires operator investigation.
An unresolved attempt sets `AdmissionRecoveryRequired=True` on the build and `recovery_required=true` in its HTTP representation; the ledger keeps the exact nonce as operator evidence.

This is a coherent controller-side safety improvement, not completion of #44.
The HTTP create path still performs a read-then-create queued count, which can overaccept concurrent requests from one or multiple replicas.
There is no global queued limit, and a caller creating `KovaBuild` directly bypasses the HTTP check.
The Pod nonce protocol fences late Create calls made by this version of the controller; a prior binary does not follow it.
Do not describe the configured values as proven strict limits for all high-concurrency and queue schedules.

## Remaining implementation

1. Extend the authoritative ledger with queued intents, a global queued limit, per-requester counts, an immutable request digest, build UID, and explicit `pending`, `queued`, `active`, and `release-pending` states.
   Every HTTP replica must reserve by compare-and-swap before it creates a CR; a duplicate idempotency key must reuse the same intent and compare its digest, even while the first Create is in flight.
   A quota rejection must consistently return `queue_capacity_exceeded` with HTTP 429 and `Retry-After`.
2. Make the API server reject a build without a matching admitted intent, including direct CR writes, or explicitly define and document direct CR writes outside the quota contract.
   A fail-closed admission webhook is one possible route; its availability and upgrade behavior need their own tests.
   A bare ledger plus a later CR Create cannot make an uncertain Create outcome safe to reclaim automatically: a late successful Create may otherwise arrive after its slot was released.
3. Move a queued intent to active in one ledger compare-and-swap, preserving the existing Pod Create nonce fence.
   Record and inspect unknown outcomes instead of expiring reservations by wall-clock time.
4. Reconcile pending intents against authoritative CR and Pod reads, release queued counts only after an observed transition or confirmed deletion, and retain an explicit recovery-required state for outcomes that cannot be proved absent.
   Exercise an operator resolution path for such cases; otherwise a safety-preserving reservation can consume capacity indefinitely.
5. Test simultaneous Create calls from at least two API Server instances, both with the same requester and with different requesters.
   Inject stale informer lists, write conflicts, successful writes with lost responses, process termination at each state boundary, leader replacement, late Pod Create responses, terminal cleanup failure, and deletion.
   Assert both per-requester and global queued counts, active jobs, worker slots, round-robin fairness, idempotent responses, and stable 429 behavior.

Capacity configuration should be identical across Service replicas.
Do not remove the ledger or force-delete its namespace while runners may exist.
An unresolved Pod Create nonce may be removed only when the exact nonce is observed on its Pod or the original Create call has a definitive non-persisted outcome.
Absence of a Pod, elapsed time, an expired leader lease, or a restarted controller is not such evidence.
If neither fact can be established, retain the reservation and escalate the `AdmissionRecoveryRequired` condition rather than silently reclaiming capacity.

## Upgrade barrier

A mixed version has no shared reservation protocol and cannot enforce these bounds.
Before enabling this controller, stop new HTTP submissions, let old active builds reach terminal state, confirm every old runner Pod is gone, and stop every old controller instance.
Apply the updated CRD schema before starting the new controller, because recovery adds a second status condition.
For migration to the fenced controller, use a new runner namespace and a fresh admission ledger after the old workload is drained; late Pod requests from an old binary are then confined to its old namespace.
Only resume submissions after the old execution path is quiescent and the new instances share identical capacity settings.
If old builds cannot drain, keep the old service isolated until their caller-owned immutable inputs can be resubmitted under the new service contract; do not copy an in-flight build into the new namespace or delete the old ledger as a shortcut.

# Service Admission Design

Issue [#44](https://github.com/cofy-x/kova/issues/44) remains open.
The current controller slice serializes active grants in a namespaced ConfigMap, `kova-service-admission`.
Each grant records the build UID, requester, and worker-slot allocation through a resourceVersion compare-and-swap before runner Pod creation.
The controller reads the ledger and queued builds directly from the API server, rather than from its informer cache.
On first use it adopts active build status and matching runner Pods; a later active build or runner Pod missing from the ledger blocks new grants.
An uncertain ledger write never authorizes a Pod: a retry or replacement leader first reads the recorded grant.
Terminal and deletion reconciliation retain the grant until the matching runner Pod is confirmed gone.
Ownership mismatch or malformed ledger state blocks release and requires operator investigation.

This is a coherent controller-side safety improvement, not completion of #44.
The HTTP create path still performs a read-then-create queued count, which can overaccept concurrent requests from one or multiple replicas.
There is no global queued limit, and a caller creating `KovaBuild` directly bypasses the HTTP check.
The ConfigMap ledger has no cross-object transaction with Pod creation; a stale leader with an in-flight Pod create could race a replacement leader's terminal cleanup and release.
Do not describe the configured values as proven strict limits for all high-concurrency and failover schedules.

## Remaining implementation

1. Extend the authoritative ledger with queued intents, a global queued limit, per-requester counts, an immutable request digest, build UID, and explicit `pending`, `queued`, `active`, and `release-pending` states.
   Every HTTP replica must reserve by compare-and-swap before it creates a CR; a duplicate idempotency key must reuse the same intent and compare its digest, even while the first Create is in flight.
   A quota rejection must consistently return `queue_capacity_exceeded` with HTTP 429 and `Retry-After`.
2. Make the API server reject a build without a matching admitted intent, including direct CR writes, or explicitly define and document direct CR writes outside the quota contract.
   A fail-closed admission webhook is one possible route; its availability and upgrade behavior need their own tests.
   A bare ledger plus a later CR Create cannot make an uncertain Create outcome safe to reclaim automatically: a late successful Create may otherwise arrive after its slot was released.
3. Move a queued intent to active in one ledger compare-and-swap, then create the Pod.
   Fence a losing or stale controller before external Pod creation and before release so an old in-flight Create cannot produce a new runner after the replacement has freed its slot.
   Record and inspect unknown outcomes instead of expiring reservations by wall-clock time.
4. Reconcile pending intents against authoritative CR and Pod reads, release queued counts only after an observed transition or confirmed deletion, and retain an explicit recovery-required state for outcomes that cannot be proved absent.
   Exercise an operator resolution path for such cases; otherwise a safety-preserving reservation can consume capacity indefinitely.
5. Test simultaneous Create calls from at least two API Server instances, both with the same requester and with different requesters.
   Inject stale informer lists, write conflicts, successful writes with lost responses, process termination at each state boundary, leader replacement, late Pod Create responses, terminal cleanup failure, and deletion.
   Assert both per-requester and global queued counts, active jobs, worker slots, round-robin fairness, idempotent responses, and stable 429 behavior.

Capacity configuration should be identical across Service replicas.
Do not remove the ledger or force-delete its namespace while runners may exist.
A version transition from an older controller needs an explicit quiescent migration barrier; a mixed version has no shared reservation protocol.

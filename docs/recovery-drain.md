# Committed-loss recovery: drain-only slice

This page documents a finite implementation slice for [#62](https://github.com/cofy-x/kova/issues/62), not a completed recovery procedure or a production acceptance result.
The fresh Genesis v3 candidate records queue, active-grant, and Pod-Create pre-effect receipts, and pins the external worker-pool identity and runner OCI manifest/index digest.
The complete committed-loss incident procedure and its isolated runtime acceptance remain incomplete.
An opt-in exact-object executor is available as a library slice, not an automatic recovery controller or an operator CLI.
Do not invoke the drain coordinator against a live installation from this branch.

## Boundary

`recoveryreceipt` creates immutable, unowned pre-effect ConfigMaps for a queue reservation, active grant, and Pod Create attempt in a dedicated, externally pinned recovery-receipt Namespace, separate from the runner and Service control Namespaces.
Each records both original Namespace UIDs, Genesis/ledger identity, exact build/request/source facts, and a one-use nonce.
A successful direct readback qualifies the receipt as evidence; it never grants a second Kubernetes Create, replay, or capacity release.
The v3 queue path retains its queue charge through admitted active work and UID-preconditioned receipt cleanup after an exact terminal CR disposition.
This conservatively makes `maxQueuedJobs` count all outstanding HTTP-submitted builds, not only waiting builds.
Grant and Pod attempts use durable CAS fences, direct receipt readback, and a permanent spent Pod-attempt nonce; an armed but unresolved attempt remains charged across restart.
Normal terminal cleanup settles the Pod receipt, grant receipt, active charge, and then HTTP queue receipt in that order.
Incident evidence must remain until an authorized disposition; no TTL or scavenger is provided.

`recoverypermit` verifies an externally issued Ed25519, drain-only permit against an independently pinned whole-envelope digest, issuer public key, incident/epoch identity, complete ordered receipt-set digest, original worker-pool identity, and typed old-writer-stop assertion. The external assertion pins old ingress route and control Deployment UIDs, every old Service Pod/process generation, and a detailed evidence digest; it asserts old writers are joined and no uninstrumented or post-confirmation side effects remain. A signature does not itself prove that physical claim. Kova has no permit signer or authority to retire workers, and a locally cancelled buildctl process is not proof of remote BuildKit cancellation.

`recoverydrain.Drain` accepts an uncached direct API client and an operator-pinned expectation. Before writing it verifies the permit, both original Namespace UIDs, immutable stop intent, a bounded complete receipt-namespace ConfigMap List, and direct GET of every exact receipt UID and data digest. An empty current CR/Pod List is never proof of receipt completeness or of absent in-flight Create effects. An omitted, replaced, or extra old receipt blocks all occupancy writes. Queue/grant/Pod receipts must link the same original epoch and build; the coordinator will not infer a missing link.

For each pre-receipted KovaBuild or Pod name, the coordinator retains a qualified original with a recovery-hold finalizer. If absent, it durably records a single immutable, finalizer-held name-specific occupancy attempt and may issue one same-kind inert tombstone Create. The old Create or the tombstone may win Kubernetes' atomic name conflict. A lost attempt response, unknown Create result without a directly qualified occupant, changed UID, foreign occupant, deleting object, or failed hold remains unknown and charged. A restart never reissues a tombstone Create for a recorded attempt. The build controller and admission candidate selector ignore only a precisely shaped inert KovaBuild tombstone; a marker label alone cannot disable ordinary build cleanup. Pod tombstones carry a scheduling gate, inert image/command, no token mount, and a hold finalizer.

The coordinator rechecks both original Namespace UIDs and the immutable stop intent immediately before each attempt/tombstone Create or original-object hold Update, after each occupant readback, and before its final report. A changed or missing identity changes the result to unknown and prevents later writes. These are observations, **not** a cross-object atomic lock: an external operator must quarantine both old Namespaces and prohibit deletion/recreation or privileged metadata/finalizer mutation throughout the drain and subsequent evidence-retention period. A reused namespace name between a guard read and an API write cannot be made safe by this client alone.

The only successful stage is **`occupied-not-drained`**. It proves name occupancy at the direct readback, not runner termination, remote BuildKit quiescence, registry publication settlement, namespace disposal, or permission to admit new capacity. A worker-pool retirement permit does not settle a still-running runner Pod. The coordinator has no Delete, Exec, ledger mutation, status mutation, finalizer removal, or capacity-release operation. Originals and tombstones remain in the quarantined old namespace until a separate externally reviewed disposal gate; deleting an original before a tombstone exists would open an unsafe name gap.

## Still required for #62

- Complete real-API acceptance of the three pre-effect receipt paths and canonical Pod defaulting, including uncertain writes and normal bounded cleanup. Existing deployments are not retroactively instrumented.
- Join each exact daemon build request through its retire barrier and independently prove old runner/Service/BuildKit resources and network paths cannot resume. Local cancellation or a stopped HTTP process alone is insufficient.
- Define and test the external issuer's evidence collection and signing procedure; prove stop ordering, complete receipt inventory, and worker/runner isolation against a real API server.
- Specify the separate namespace-disposal and new-epoch admission gate. This slice intentionally never frees old capacity or silently recreates a ledger.
- Run dedicated real-API failure-chain acceptance, including lost Create responses, late side effects, replacement UIDs, and process/leader crashes. Fake-client/race tests are not a #62 PASS.

## External disposal boundary

`recoverydisposal` verifies externally signed, independently pinned authorization, exact-object execution-plan, and closure records.
Those pure verifiers never authorize a Kubernetes mutation by themselves.
The separate opt-in `Execute` library also requires a differently scoped, independently pinned mutation grant and qualified archived object bodies.
It has no signer, physical-retirement detector, route writer, namespace-delete method, capacity-release method, or automatic Service/CLI invocation.
Its receipt-oriented authorization cannot authorize broad deletion of direct/admin objects, tombstones, or runner-namespace occupancy-attempt ConfigMaps.
The independent execution-plan schema covers those physical identities and their separately approved dispositions, without converting verification into mutation permission.
It separates historical original identities from current tombstones and separates all archived records from the narrower set of actionable targets.
Its independently pinned limits allow at most 8,192 archived objects and 8,192 targets; larger inventories fail closed and are not silently split into supposedly complete batches.
Only known namespaced resources and the specified Kova-owned finalizers can appear in a target, and all namespace, control-object, occupant, signature-domain, and canonical digest checks remain required.

`recoveryinventory.Collect` provides a separate read-only archival observation: three namespace identity reads, six unfiltered Lists (KovaBuilds, Pods, and ConfigMaps in each old namespace), and three final identity reads.
It preserves unstructured fields and non-target objects, including unlabeled ConfigMaps, and returns no partial snapshot on error, pagination, identity drift, or exceeded object/byte bounds.
Its ceilings are 8,192 objects including the two namespace records, 2 MiB per object, and 64 MiB total; each of its twelve API calls has a ten-second context bound, and the caller must also use an uncached client with bounded transport responses.
The output is `inventory-observed-not-retired`, not an atomic cross-resource snapshot, a durable archive, all Kubernetes resource kinds, or proof that no old effect can still arrive.

Before any namespace deletion, an external observer must archive the complete bounded CR, Pod, and relevant ConfigMap inventory, immutable receipts, stop/genesis/ledger evidence, terminal results, and explicit Unknown/discard approvals outside both old namespaces.
The observer must prove retirement of old Service, runner, worker, network, and already-admitted in-flight effects; a namespace's `Terminating` state does not supply that proof.
Object-count, per-object byte, total archive byte, and API-call bounds must fail closed rather than truncate the inventory.

The finite executor can act only on the separately signed exact namespace/name/UID and qualified archived projection, after the external authority starts normal deletion of both original namespace UIDs.
It preflights every target and archive before any mutation, guards the original cluster and both Terminating namespace identities, and uses fresh UID/resourceVersion conditional Delete and finalizer-only JSON Patch.
Only explicitly allowed Kova finalizers may be removed; foreign finalizers, spec, status, data, labels, annotations and owner references remain qualified against the archive.
It never force-finalizes a namespace, adopts a replacement UID, creates a successor or releases capacity.
API-interface calls, object bytes and grant lifetime are bounded; the supplied uncached client must independently bound transport responses and prohibit automatic mutation retries.
The interface-call count is not a wire-request count or proof of API latency.
An unknown response stops that invocation without retry or polling; a later invocation starts with fresh direct qualification, not a remembered resourceVersion.
Pending objects are reported as pending, not disposed; even observing all exact targets absent is not an incident closure or capacity-release proof.
Even observing both old namespace UIDs absent is only an API observation: physical retirement, persistent name non-reuse, one-successor allocation, and route compare-and-swap remain independently verified external closure requirements.

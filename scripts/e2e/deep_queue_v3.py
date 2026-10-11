"""Pure Genesis-v3 qualifications shared by the directed deep-queue probe.

This module neither prepares a fixture nor sends Kubernetes/registry requests.
The owner supplies original identities; observation never backfills those pins.
"""

from __future__ import annotations

import hashlib
import json
import math
import os
import re
import stat
from collections.abc import Callable
from pathlib import Path

UID = re.compile(r"[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}\Z")
SHA = re.compile(r"[0-9a-f]{64}\Z")
DIGEST = re.compile(r"sha256:[0-9a-f]{64}\Z")
NONCE = re.compile(r"[0-9a-f]{32}\Z")
DNS = re.compile(r"[a-z0-9](?:[-a-z0-9]*[a-z0-9])?\Z")
PREFIX = "kova.cofy.dev/"
GENESIS = "kova-service-admission-genesis"
ACTIVE = "kova-service-admission"
QUEUE = "kova-service-queue-admission"
LIMIT_KEYS = (
    "maxActiveJobs",
    "maxActiveJobsPerRequester",
    "workerSlots",
    "maxQueuedJobs",
    "maxQueuedJobsPerRequester",
)
CONTRACT_KEYS = (
    "version",
    "namespaceUID",
    "receiptNamespace",
    "receiptNamespaceUID",
    "workerPoolID",
    "runnerImage",
    "generation",
    "activeLedgerName",
    "activeLedgerSchema",
    "queueLedgerName",
    "queueLedgerSchema",
    "limits",
)


class QualificationError(ValueError):
    pass


def need(condition: bool, reason: str) -> None:
    if not condition:
        raise QualificationError(reason)


def keys(value: object, wanted: set[str], reason: str) -> dict:
    need(type(value) is dict and set(value) == wanted, reason)
    return value


def pairs(values: list[tuple[str, object]]) -> dict:
    result = {}
    for key, value in values:
        need(key not in result, "duplicate JSON field")
        result[key] = value
    return result


def decode(raw: str | bytes) -> dict:
    value = json.loads(raw, object_pairs_hook=pairs)
    need(type(value) is dict, "JSON must be an object")
    return value


def encoded(value: object, *, sorted_keys: bool = True) -> bytes:
    # Fixture identities and data below are ASCII. Match Go's sorted map keys;
    # struct keys are deliberately put in the declaration order before hashing.
    return json.dumps(
        value,
        sort_keys=sorted_keys,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode()


def digest_data(data: dict) -> str:
    need(
        type(data) is dict
        and all(type(k) is str and type(v) is str for k, v in data.items()),
        "receipt Data must contain only strings",
    )
    return "sha256:" + hashlib.sha256(encoded(data)).hexdigest()


def load_fixture(path: Path) -> tuple[dict, str]:
    need(
        path.is_absolute() and path.resolve() == path and not path.is_symlink(),
        "fixture path must be canonical and absolute",
    )
    info = path.stat()
    need(
        stat.S_ISREG(info.st_mode)
        and info.st_uid == os.getuid()
        and info.st_mode & 0o077 == 0
        and info.st_size <= 32768,
        "fixture must be a private bounded regular file",
    )
    raw = path.read_bytes()
    value = decode(raw)
    validate_fixture(value)
    return value, hashlib.sha256(raw).hexdigest()


def validate_fixture(value: dict) -> None:
    keys(
        value,
        {
            "schema_version",
            "run_id",
            "profile",
            "candidate_commit",
            "pod_template_binary",
            "pod_template_binary_sha256",
            "host_boot_id",
            "cluster",
            "kubeconfig",
            "kubeconfig_sha256",
            "kube_system_namespace_uid",
            "control_namespace",
            "control_namespace_uid",
            "runner_namespace",
            "runner_namespace_uid",
            "receipt_namespace",
            "receipt_namespace_uid",
            "release",
            "auth_secret_uid",
            "service_deployment_uid",
            "worker_deployment_uid",
            "node_uids",
            "service_pods",
            "worker_pods",
            "role_images",
            "genesis",
            "ports",
        },
        "fixture fields differ",
    )
    run_id = value["run_id"]
    need(
        type(run_id) is str
        and re.fullmatch(r"v3-[0-9]{8}-[a-z0-9]{2,12}", run_id) is not None,
        "run ID must identify a fresh v3 fixture",
    )
    need(
        value["schema_version"] == 1 and type(value["schema_version"]) is int,
        "fixture schema differs",
    )
    need(
        type(value["profile"]) is str
        and value["profile"] in {
            "representative-short", "supported-capacity", "supported-capacity-retained"
        },
        "unknown profile",
    )
    need(
        type(value["candidate_commit"]) is str
        and re.fullmatch(r"[0-9a-f]{40}", value["candidate_commit"]) is not None,
        "candidate is not pinned",
    )
    need(
        type(value["pod_template_binary"]) is str
        and Path(value["pod_template_binary"]).is_absolute()
        and ".." not in Path(value["pod_template_binary"]).parts
        and type(value["pod_template_binary_sha256"]) is str
        and SHA.fullmatch(value["pod_template_binary_sha256"]),
        "native Pod-template binary is not pinned",
    )
    need(
        value["cluster"] == "kova-deep-queue-" + run_id,
        "cluster is not fresh run-scoped Kind",
    )
    names = [value[role + "_namespace"] for role in ("control", "runner", "receipt")]
    need(
        all(
            type(n) is str
            and len(n) <= 63
            and DNS.fullmatch(n)
            and n.endswith("-" + run_id)
            for n in names
        )
        and len(set(names)) == 3,
        "namespaces are not distinct/fresh",
    )
    need(
        type(value["release"]) is str and DNS.fullmatch(value["release"]) is not None,
        "release name is invalid",
    )
    for field in (
        "host_boot_id",
        "kube_system_namespace_uid",
        "control_namespace_uid",
        "runner_namespace_uid",
        "receipt_namespace_uid",
        "service_deployment_uid",
        "worker_deployment_uid",
        "auth_secret_uid",
    ):
        need(
            type(value[field]) is str and UID.fullmatch(value[field]) is not None,
            "invalid original UID",
        )
    need(
        type(value["kubeconfig"]) is str
        and Path(value["kubeconfig"]).is_absolute()
        and ".." not in Path(value["kubeconfig"]).parts,
        "kubeconfig is not an explicit absolute path",
    )
    need(
        type(value["kubeconfig_sha256"]) is str
        and SHA.fullmatch(value["kubeconfig_sha256"]),
        "kubeconfig bytes are not pinned",
    )
    keys(
        value["node_uids"],
        {value["cluster"] + "-control-plane", value["cluster"] + "-worker"},
        "node pins differ",
    )
    for field, count in (("node_uids", 2), ("service_pods", 2), ("worker_pods", 1)):
        group = value[field]
        need(
            type(group) is dict
            and len(group) == count
            and all(
                type(name) is str
                and DNS.fullmatch(name)
                and type(uid) is str
                and UID.fullmatch(uid)
                for name, uid in group.items()
            )
            and len(set(group.values())) == count,
            "Pod/node original identity differs",
        )
    keys(
        value["role_images"],
        {"controller", "runner", "worker"},
        "role image fields differ",
    )
    for image in value["role_images"].values():
        need(
            type(image) is str
            and len(image) <= 512
            and re.fullmatch(r"[a-z0-9][a-z0-9.:/-]+@sha256:[0-9a-f]{64}", image)
            is not None,
            "role image must be a manifest digest reference",
        )
    pins = keys(
        value["genesis"],
        {
            "secret_name",
            "secret_uid",
            "receipt_sha256",
            "genesis_uid",
            "active_ledger_uid",
            "queue_ledger_uid",
            "worker_pool_id",
        },
        "Genesis pin fields differ",
    )
    need(
        type(pins["secret_name"]) is str and DNS.fullmatch(pins["secret_name"]),
        "invalid receipt Secret name",
    )
    for field in ("secret_uid", "genesis_uid", "active_ledger_uid", "queue_ledger_uid"):
        need(
            type(pins[field]) is str and UID.fullmatch(pins[field]),
            "invalid original Genesis UID",
        )
    need(
        type(pins["receipt_sha256"]) is str and SHA.fullmatch(pins["receipt_sha256"]),
        "installation receipt bytes are not pinned",
    )
    need(
        type(pins["worker_pool_id"]) is str
        and len(pins["worker_pool_id"]) <= 128
        and re.fullmatch(r"[A-Za-z0-9._:/@+-]+", pins["worker_pool_id"]) is not None
        and pins["worker_pool_id"].endswith(run_id),
        "worker pool is not fresh/pinned",
    )
    port_fields = {"service", "api_proxy", "metrics_remote"}
    if value["profile"] == "supported-capacity-retained":
        port_fields.add("service_second")
    ports = keys(value["ports"], port_fields, "port fields differ")
    need(
        all(type(port) is int and 1024 <= port <= 65535 for port in ports.values())
        and len(set(ports.values())) == len(port_fields),
        "ports are not distinct/bounded",
    )
    original_uids = [
        value[field]
        for field in (
            "kube_system_namespace_uid",
            "control_namespace_uid",
            "runner_namespace_uid",
            "receipt_namespace_uid",
            "service_deployment_uid",
            "worker_deployment_uid",
            "auth_secret_uid",
        )
    ] + [
        pins[field]
        for field in (
            "secret_uid",
            "genesis_uid",
            "active_ledger_uid",
            "queue_ledger_uid",
        )
    ]
    original_uids += [
        uid
        for field in ("node_uids", "service_pods", "worker_pods")
        for uid in value[field].values()
    ]
    need(len(original_uids) == len(set(original_uids)), "original UIDs overlap")


def limits(fixture: dict) -> dict:
    cap = 100 if fixture["profile"] == "representative-short" else 1000
    return dict(zip(LIMIT_KEYS, (1, 1, 1, cap, cap)))


def identity(obj: dict, name: str, namespace: str | None, uid: str) -> None:
    metadata = obj.get("metadata", {})
    need(
        metadata.get("name") == name
        and metadata.get("uid") == uid
        and (namespace is None or metadata.get("namespace") == namespace)
        and not metadata.get("deletionTimestamp")
        and type(metadata.get("resourceVersion")) is str
        and re.fullmatch(r"[1-9][0-9]{0,31}", metadata["resourceVersion"]) is not None,
        "original API object identity changed",
    )


def qualify_contract(receipt: dict, fixture: dict) -> dict:
    pins = fixture["genesis"]
    keys(
        receipt,
        {"namespace", "genesisName", "genesisUID", "contract"},
        "receipt fields differ",
    )
    need(
        receipt["namespace"] == fixture["runner_namespace"]
        and receipt["genesisName"] == GENESIS
        and receipt["genesisUID"] == pins["genesis_uid"],
        "original installation receipt differs",
    )
    contract = keys(receipt["contract"], set(CONTRACT_KEYS), "contract fields differ")
    keys(contract["limits"], set(LIMIT_KEYS), "limit fields differ")
    expected = {
        "version": 3,
        "namespaceUID": fixture["runner_namespace_uid"],
        "receiptNamespace": fixture["receipt_namespace"],
        "receiptNamespaceUID": fixture["receipt_namespace_uid"],
        "workerPoolID": pins["worker_pool_id"],
        "runnerImage": fixture["role_images"]["runner"],
        "activeLedgerName": ACTIVE,
        "activeLedgerSchema": 2,
        "queueLedgerName": QUEUE,
        "queueLedgerSchema": 2,
        "limits": limits(fixture),
    }
    need(
        all(
            type(contract[key]) is type(wanted) and contract[key] == wanted
            for key, wanted in expected.items()
        ),
        "v3 contract differs from frozen fixture",
    )
    need(
        all(type(contract["limits"][key]) is int for key in LIMIT_KEYS),
        "noninteger limits",
    )
    need(
        type(contract["generation"]) is str and NONCE.fullmatch(contract["generation"]),
        "invalid generation",
    )
    return contract


def contract_digest(contract: dict) -> str:
    ordered = {
        key: (
            {k: contract[key][k] for k in LIMIT_KEYS}
            if key == "limits"
            else contract[key]
        )
        for key in CONTRACT_KEYS
    }
    return hashlib.sha256(encoded(ordered, sorted_keys=False)).hexdigest()


def qualify_ledger(obj: dict, role: str, fixture: dict, contract: dict) -> dict:
    pin = fixture["genesis"][role + "_ledger_uid"]
    name, data_key = (
        (ACTIVE, "reservations.json") if role == "active" else (QUEUE, "queue.json")
    )
    identity(obj, name, fixture["runner_namespace"], pin)
    need(
        obj.get("immutable") is None or obj.get("immutable") is False,
        "ledger mutability differs",
    )
    need(not obj.get("binaryData"), "ledger storage shape differs")
    annotations = obj["metadata"].get("annotations", {})
    expected = {
        PREFIX + "admission-namespace-uid": contract["namespaceUID"],
        PREFIX + "admission-genesis-uid": fixture["genesis"]["genesis_uid"],
        PREFIX + "admission-generation": contract["generation"],
        PREFIX + "admission-contract": contract_digest(contract),
        PREFIX + "admission-role": role,
        PREFIX + "admission-schema": "2",
    }
    need(
        type(annotations) is dict
        and len(annotations) == 7
        and all(annotations.get(key) == value for key, value in expected.items())
        and type(annotations.get(PREFIX + "admission-bootstrap")) is str
        and NONCE.fullmatch(annotations[PREFIX + "admission-bootstrap"]),
        "ledger installation stamps differ",
    )
    data = keys(obj.get("data"), {data_key}, "ledger data keys differ")
    need(
        type(data[data_key]) is str and 0 < len(data[data_key].encode()) <= 768 * 1024,
        "ledger data size differs",
    )
    state = decode(data[data_key])
    need(
        type(state.get("version")) is int and state["version"] == 2,
        "ledger schema is not v3 schema2",
    )
    if role == "active":
        required = {
            "version",
            "fence",
            "maxJobs",
            "maxPerRequester",
            "workerSlots",
            "active",
        }
        need(
            required <= set(state) <= required | {"lastGrantedRequesterHash"},
            "active state fields differ",
        )
        need(
            all(
                type(state[k]) is int and state[k] >= 0
                for k in ("fence", "maxJobs", "maxPerRequester", "workerSlots")
            )
            and type(state["active"]) is dict
            and len(state["active"]) <= 1,
            "active state shape differs",
        )
        if "lastGrantedRequesterHash" in state:
            need(
                type(state["lastGrantedRequesterHash"]) is str
                and SHA.fullmatch(state["lastGrantedRequesterHash"]),
                "active requester cursor differs",
            )
        allowed = {
            "buildName",
            "requester",
            "slots",
            "grantNonce",
            "grantFence",
            "grantObservedRV",
            "grantReceiptUID",
            "grantReceiptDigest",
            "grantCleanupReady",
            "podAttemptNonce",
            "podTemplateDigest",
            "podReceiptUID",
            "podReceiptDigest",
            "podCleanupReady",
            "closing",
            "inFlight",
        }
        for uid, entry in state["active"].items():
            need(
                type(uid) is str
                and UID.fullmatch(uid)
                and type(entry) is dict
                and {"buildName", "requester", "slots"} <= set(entry) <= allowed
                and type(entry["slots"]) is int
                and entry["slots"] == 1
                and all(
                    type(entry[k]) is bool
                    for k in ("closing", "grantCleanupReady", "podCleanupReady")
                    if k in entry
                ),
                "active entry fields differ",
            )
    else:
        keys(
            state,
            {"version", "globalLimit", "requesterLimit", "intents"},
            "queue state fields differ",
        )
        need(
            type(state["globalLimit"]) is int
            and type(state["requesterLimit"]) is int
            and type(state["intents"]) is dict
            and len(state["intents"]) <= limits(fixture)["maxQueuedJobs"],
            "queue state shape differs",
        )
        required = {"requesterHash", "requestDigest", "nonce", "createdAtUnix"}
        for name, intent in state["intents"].items():
            need(
                type(name) is str
                and re.fullmatch(r"idem-[0-9a-f]{20}", name)
                and type(intent) is dict
                and required
                <= set(intent)
                <= required | {"receiptUID", "receiptDigest", "cleanupKind"}
                and all(
                    type(intent[k]) is str and SHA.fullmatch(intent[k])
                    for k in ("requesterHash", "requestDigest")
                )
                and type(intent["nonce"]) is str
                and NONCE.fullmatch(intent["nonce"])
                and type(intent["createdAtUnix"]) is int
                and intent["createdAtUnix"] > 0
                and intent.get("cleanupKind", "") in ("", "rejected", "terminal"),
                "queue intent fields differ",
            )
    return state


def qualify_queue_receipt(
    obj: dict, build: dict, intent: dict, fixture: dict, contract: dict
) -> None:
    metadata, spec = build["metadata"], build["spec"]
    annotations = metadata.get("annotations", {})
    nonce = intent.get("nonce")
    need(type(nonce) is str and NONCE.fullmatch(nonce), "invalid queue nonce")
    name = "kova-admission-intent-" + nonce
    identity(obj, name, fixture["receipt_namespace"], intent.get("receiptUID", ""))
    requester = spec["requester"]
    keys(
        spec,
        {"requester", "targets", "source", "build", "idempotencyKey"},
        "build spec fields differ",
    )
    need(
        type(requester) is dict
        and requester.get("username") == "kova:e2e"
        and set(requester) <= {"username", "uid"}
        and type(requester.get("uid", "")) is str
        and spec["build"] == {"format": "oci", "concurrency": 1}
        and type(spec["build"].get("concurrency")) is int,
        "immutable probe requester/build differs",
    )
    keys(spec["source"], {"uri", "digest"}, "probe source fields differ")
    need(
        spec["source"]
        == {
            "uri": f"oci://unused.invalid/deep-queue/{fixture['run_id']}@sha256:"
            + "a" * 64,
            "digest": "sha256:" + "a" * 64,
        }
        and type(spec["targets"]) is list
        and len(spec["targets"]) == 1
        and type(spec["idempotencyKey"]) is str
        and re.fullmatch(r"[A-Za-z0-9_-]{1,128}", spec["idempotencyKey"]),
        "immutable probe source/idempotency differs",
    )
    for item in spec["targets"]:
        keys(item, {"target", "platform"}, "probe target fields differ")
        need(
            type(item["target"]) is str
            and re.fullmatch(
                re.escape(f"unused.invalid/deep-queue/{fixture['run_id']}:")
                + r"[A-Za-z0-9_.-]{1,128}",
                item["target"],
            )
            and item["platform"] == "linux/amd64",
            "probe target/platform differs",
        )
    canonical_spec = {
        "requester": {"username": requester["username"]},
        "targets": [
            {"target": item["target"], "platform": item["platform"]}
            for item in spec["targets"]
        ],
        "source": {"uri": spec["source"]["uri"], "digest": spec["source"]["digest"]},
        "build": {"format": "oci", "concurrency": 1},
        "idempotencyKey": spec["idempotencyKey"],
    }
    need(
        intent.get("requestDigest")
        == hashlib.sha256(encoded(canonical_spec, sorted_keys=False)).hexdigest(),
        "queue request digest does not bind the original CR spec",
    )
    expected = {
        "version": "1",
        "namespaceName": fixture["runner_namespace"],
        "namespaceUID": contract["namespaceUID"],
        "receiptNamespaceName": fixture["receipt_namespace"],
        "receiptNamespaceUID": contract["receiptNamespaceUID"],
        "genesisName": GENESIS,
        "genesisUID": fixture["genesis"]["genesis_uid"],
        "generation": contract["generation"],
        "activeLedgerUID": fixture["genesis"]["active_ledger_uid"],
        "queueLedgerUID": fixture["genesis"]["queue_ledger_uid"],
        "buildName": metadata["name"],
        "requesterName": requester["username"],
        "requesterUID": requester.get("uid", ""),
        "requesterHash": hashlib.sha256(requester["username"].encode()).hexdigest(),
        "requestDigest": intent.get("requestDigest"),
        "sourceDigest": spec["source"]["digest"],
        "queueNonce": nonce,
    }
    need(
        obj.get("immutable") is True
        and not obj.get("binaryData")
        and not obj["metadata"].get("ownerReferences")
        and obj.get("data") == expected
        and obj["metadata"].get("labels")
        == {PREFIX + "admission-receipt": "queue-intent"},
        "queue pre-effect receipt differs",
    )
    need(
        type(expected["requestDigest"]) is str
        and SHA.fullmatch(expected["requestDigest"])
        and intent.get("requesterHash") == expected["requesterHash"]
        and intent.get("receiptDigest") == digest_data(expected)
        and annotations.get(PREFIX + "queue-intent") == nonce
        and annotations.get(PREFIX + "queue-receipt-uid") == intent["receiptUID"]
        and annotations.get(PREFIX + "queue-receipt-digest") == intent["receiptDigest"],
        "CR, intent and queue receipt links differ",
    )


def qualify_active_receipts(
    build: dict,
    pod: dict,
    entry: dict,
    intent: dict,
    objects: dict[str, dict],
    fixture: dict,
    contract: dict,
    canonical_pod_digest: Callable[[dict], str],
) -> None:
    metadata, spec = build["metadata"], build["spec"]
    pinned = {
        "namespace": fixture["runner_namespace"],
        "namespaceUid": contract["namespaceUID"],
        "receiptNamespace": fixture["receipt_namespace"],
        "receiptNamespaceUid": contract["receiptNamespaceUID"],
        "genesisName": GENESIS,
        "genesisUid": fixture["genesis"]["genesis_uid"],
        "generation": contract["generation"],
        "activeLedgerUid": fixture["genesis"]["active_ledger_uid"],
        "queueLedgerUid": fixture["genesis"]["queue_ledger_uid"],
        "buildName": metadata["name"],
        "buildUid": metadata["uid"],
        "requesterName": spec["requester"]["username"],
        "requesterUid": spec["requester"].get("uid", ""),
        "requesterHash": intent["requesterHash"],
        "requestDigest": intent["requestDigest"],
        "sourceDigest": spec["source"]["digest"],
        "workerPoolId": contract["workerPoolID"],
    }
    grant_name = "kova-grant-intent-" + entry["grantNonce"]
    pod_name = "kova-pod-create-intent-" + entry["podAttemptNonce"]
    payloads = []
    for name, kind, prefix in (
        (grant_name, "grant", "grant"),
        (pod_name, "pod-create", "pod"),
    ):
        obj = objects.get(name, {})
        identity(obj, name, fixture["receipt_namespace"], entry[prefix + "ReceiptUID"])
        data = keys(
            obj.get("data"),
            {"version", "kind", "payload"},
            "effect receipt data differs",
        )
        need(
            obj.get("immutable") is True
            and not obj.get("binaryData")
            and not obj["metadata"].get("ownerReferences")
            and obj["metadata"].get("labels") == {PREFIX + "recovery-receipt": kind}
            and data["version"] == "1"
            and data["kind"] == kind
            and digest_data(data) == entry[prefix + "ReceiptDigest"],
            "effect receipt identity/digest differs",
        )
        payload = decode(data["payload"])
        need(payload.get("build") == pinned, "effect receipt build identity differs")
        payloads.append(payload)
    grant, create = payloads
    keys(
        grant,
        {
            "build",
            "admissionMode",
            "queueReceipt",
            "grantNonce",
            "activeLedgerFence",
            "activeLedgerResourceVersion",
            "allocatedWorkerSlots",
        },
        "grant payload fields differ",
    )
    need(
        grant["admissionMode"] == "queued"
        and grant["grantNonce"] == entry["grantNonce"]
        and type(grant["activeLedgerFence"]) is int
        and grant["activeLedgerFence"] > 0
        and grant["activeLedgerFence"] == entry.get("grantFence")
        and grant["activeLedgerResourceVersion"] == entry.get("grantObservedRV")
        and type(grant["allocatedWorkerSlots"]) is int
        and grant["allocatedWorkerSlots"] == 1
        and grant["queueReceipt"]
        == {
            "name": "kova-admission-intent-" + intent["nonce"],
            "uid": intent["receiptUID"],
            "dataDigest": intent["receiptDigest"],
        },
        "grant pre-effect facts differ",
    )
    keys(
        create,
        {
            "build",
            "grantReceipt",
            "podName",
            "podAttemptNonce",
            "podTemplateDigest",
            "runnerImageDigest",
            "buildRequestId",
        },
        "Pod-create payload fields differ",
    )
    need(
        create["grantReceipt"]
        == {
            "name": grant_name,
            "uid": entry["grantReceiptUID"],
            "dataDigest": entry["grantReceiptDigest"],
        }
        and create["podName"]
        == pod["metadata"]["name"]
        == "kova-job-" + metadata["name"]
        and create["podAttemptNonce"] == entry["podAttemptNonce"]
        and create["podTemplateDigest"] == entry["podTemplateDigest"]
        and canonical_pod_digest(pod) == entry["podTemplateDigest"]
        and create["runnerImageDigest"] == contract["runnerImage"].rsplit("@", 1)[1]
        and create["buildRequestId"] == metadata["uid"],
        "Pod-create pre-effect facts differ",
    )
    witness = {
        "namespaceUID": contract["namespaceUID"],
        "genesisUID": fixture["genesis"]["genesis_uid"],
        "generation": contract["generation"],
        "activeLedgerUID": fixture["genesis"]["active_ledger_uid"],
        "queueLedgerUID": fixture["genesis"]["queue_ledger_uid"],
        "buildUID": metadata["uid"],
        "podName": pod["metadata"]["name"],
        "podUID": pod["metadata"]["uid"],
        "podCreateAttempt": entry["podAttemptNonce"],
        "podTemplateDigest": entry["podTemplateDigest"],
        "runnerRequestID": metadata["uid"],
    }
    need(
        build.get("status", {}).get("admissionGenesisWitness") == witness
        and build.get("status", {}).get("runnerPodName") == pod["metadata"]["name"],
        "CR runner witness differs",
    )
    annotations = pod["metadata"].get("annotations", {})
    stamps = {
        "runner-namespace-uid": contract["namespaceUID"],
        "runner-genesis-uid": witness["genesisUID"],
        "runner-generation": contract["generation"],
        "runner-active-ledger-uid": witness["activeLedgerUID"],
        "runner-queue-ledger-uid": witness["queueLedgerUID"],
        "runner-build-uid": metadata["uid"],
        "runner-request-id": metadata["uid"],
        "create-attempt": entry["podAttemptNonce"],
        "pod-template-digest": entry["podTemplateDigest"],
    }
    need(
        all(annotations.get(PREFIX + key) == value for key, value in stamps.items()),
        "Pod Genesis stamps differ",
    )
    runners = [
        item
        for item in pod["spec"].get("containers", [])
        if item.get("name") == "runner"
    ]
    need(
        len(runners) == 1
        and runners[0].get("image") == contract["runnerImage"]
        and runners[0].get("imagePullPolicy") == "Never",
        "runner image differs",
    )
    fences = [
        item
        for item in runners[0].get("env", [])
        if item.get("name") == "KOVA_RUNNER_POD_UID"
    ]
    need(
        len(fences) == 1
        and fences[0].get("value", "") == ""
        and fences[0].get("valueFrom")
        == {"fieldRef": {"apiVersion": "v1", "fieldPath": "metadata.uid"}},
        "runner lacks exact Downward API UID fence",
    )


def histogram_deltas(before: list[dict], after: list[dict], family: str) -> dict:
    """Keep zero buckets: sparse positive-only deltas cannot prove percentile bounds."""
    need(
        [s["pod"] for s in before] == [s["pod"] for s in after],
        "metric Pod identity changed",
    )
    result = {}
    for old, new in zip(before, after):

        def parse(sample: dict) -> dict:
            rows = {}
            for line in sample["series"]:
                name, raw = line.rsplit(" ", 1)
                if not name.startswith(family + "_"):
                    continue
                need(name not in rows, "duplicate histogram series")
                value = float(raw)
                need(math.isfinite(value) and value >= 0, "invalid histogram counter")
                rows[name] = value
            return rows

        previous, current = parse(old), parse(new)
        need(set(previous) <= set(current), "histogram series disappeared")
        need(
            any(
                name.startswith(family + "_bucket{") and 'le="0.01"' in name
                for name in current
            )
            if family == "kova_service_kube_limiter_wait_seconds"
            else bool(current),
            "histogram buckets missing",
        )
        for name, value in current.items():
            need(value >= previous.get(name, 0), "histogram counter reset")
            result[new["pod"] + "::" + name] = round(value - previous.get(name, 0), 6)
    return result


def histogram_summary(deltas: dict, family: str, labels: dict | None = None) -> dict:
    """Report bucket upper bounds, not exact sampled percentile latencies."""
    buckets = {}
    count = 0.0
    total = 0.0
    for series, amount in deltas.items():
        name = series.split("::", 1)[-1]
        if labels is not None:
            # These bounded product labels contain no Prometheus escapes. Do
            # not let leader/readiness waits certify the HTTP traffic class.
            need(labels == {"traffic_class": "http", "outcome": "ok"},
                 "unsupported histogram selection")
            if not all(
                re.search(r'(?:\{|,)' + re.escape(key) + '="' + value + r'"(?:,|\})', name)
                for key, value in labels.items()
            ):
                continue
        if name.startswith(family + "_bucket{"):
            found = re.search(r'(?:\{|,)le="([^"]+)"', name)
            need(found is not None, "histogram lacks bucket boundary")
            boundary = float(found.group(1))
            buckets[boundary] = buckets.get(boundary, 0.0) + amount
        elif name.startswith(family + "_count"):
            count += amount
        elif name.startswith(family + "_sum"):
            total += amount
    need(
        all(math.isfinite(v) and v >= 0 for v in [count, total, *buckets.values()]),
        "invalid histogram delta",
    )
    ordered = sorted(buckets.items())
    need(
        not ordered or all(a[1] <= b[1] for a, b in zip(ordered, ordered[1:])),
        "histogram bucket deltas are not monotonic",
    )
    need(count == 0 or buckets.get(math.inf) == count, "histogram count/buckets differ")
    result = {
        "samples": count,
        "sum_seconds": total,
        "p95_upper_bound_seconds": None,
        "p99_upper_bound_seconds": None,
        "unbounded_percentile": False,
    }
    for quantile in (95, 99):
        if count:
            boundary = next(
                bound
                for bound, cumulative in ordered
                if cumulative >= count * quantile / 100
            )
            result[f"p{quantile}_upper_bound_seconds"] = (
                boundary if math.isfinite(boundary) else None
            )
            result["unbounded_percentile"] |= not math.isfinite(boundary)
    # A Wait >10ms is inside the client's limiter, not network RTT. It is a
    # bounded observed rate-wait sample, not an API-server 429 injection.
    result["waits_over_10ms"] = count - buckets.get(0.01, count)
    return result

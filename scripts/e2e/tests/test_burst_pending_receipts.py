"""Pure original-receipt classification; no API, retirement, or admission authority."""

from __future__ import annotations

import copy
import hashlib
import importlib.util
import json
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

SCRIPT = Path(__file__).resolve().parents[1] / "e2e-service-burst.py"
SPEC = importlib.util.spec_from_file_location("burst_pending_receipts", SCRIPT)
assert SPEC is not None and SPEC.loader is not None
burst = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(burst)

UIDS = [f"10000000-0000-4000-8000-{i:012d}" for i in range(1, 14)]
GENESIS = "kova-service-admission-genesis"
QUEUE_NONCE, GRANT_NONCE, POD_NONCE = "1" * 32, "2" * 32, "3" * 32
SOURCE, TEMPLATE, RUNNER = ("sha256:" + value * 64 for value in "abc")


def digest(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def go_json(value: object, *, map_keys: bool = False) -> str:
    # Fixtures use safe ASCII. Go structs retain declaration order; map keys
    # are sorted by encoding/json. Do not use the classifier as an oracle.
    return json.dumps(value, sort_keys=map_keys, separators=(",", ":"), ensure_ascii=False)


def data_digest(data: dict) -> str:
    return "sha256:" + digest(go_json(data, map_keys=True).encode())


def receipt(kind: str, nonce: str, uid: str, data: dict) -> dict:
    queue = kind == "queue"
    prefix = "kova-admission-intent-" if queue else f"kova-{kind}-intent-"
    return {
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {
            "namespace": "receipts-original",
            "name": prefix + nonce,
            "uid": uid,
            "resourceVersion": "271",
            "labels": {
                "kova.cofy.dev/admission-receipt"
                if queue
                else "kova.cofy.dev/recovery-receipt": "queue-intent" if queue else kind,
            },
        },
        "immutable": True,
        "data": data,
    }


def evidence() -> dict:
    """Independent QueueIntent/PinnedBuild/GrantIntent/PodCreateIntent wire shapes."""
    requester, requester_uid, key = "acceptance-original", "requester-original", "warm-original"
    name = "idem-" + digest((requester + "\x00" + key).encode())[:20]
    spec = {
        "requester": {"username": requester, "uid": requester_uid},
        "targets": [{"target": "registry.invalid/campaign:original", "platform": "linux/amd64"}],
        "source": {"uri": "https://source.invalid/original.zip", "digest": SOURCE},
        "build": {"format": "oci", "concurrency": 1, "timeout": 300},
        "idempotencyKey": key,
    }
    # queueadmission.DigestBuild clears Requester.UID (omitempty) before the
    # Go declaration-order marshal, unlike a sorted JSON object digest.
    digest_spec = copy.deepcopy(spec)
    del digest_spec["requester"]["uid"]
    request_digest = digest(go_json(digest_spec).encode())
    plan = {
        "schema_version": 2,
        "fixture": {
            "runner_namespace": "runner-original",
            "runner_namespace_uid": UIDS[0],
            "receipt_namespace": "receipts-original",
            "receipt_namespace_uid": UIDS[1],
        },
        "candidate": {"role_images": {"runner": "registry.invalid/runner@" + RUNNER}},
    }
    runtime = {
        "schema_version": 2,
        "requester": requester,
        "requester_uid": requester_uid,
        "genesis": {
            "genesis_uid": UIDS[2],
            "active_ledger_uid": UIDS[3],
            "queue_ledger_uid": UIDS[4],
            "worker_pool_id": "pool-original",
            "runner_image_digest": RUNNER,
        },
    }
    contract = {
        "version": 3,
        "generation": "4" * 32,
        "maxActiveJobs": 12,
        "maxQueuedJobs": 1000,
    }
    queue_data = {
        "version": "1",
        "namespaceName": "runner-original",
        "namespaceUID": UIDS[0],
        "receiptNamespaceName": "receipts-original",
        "receiptNamespaceUID": UIDS[1],
        "genesisName": GENESIS,
        "genesisUID": UIDS[2],
        "generation": contract["generation"],
        "activeLedgerUID": UIDS[3],
        "queueLedgerUID": UIDS[4],
        "buildName": name,
        "requesterName": requester,
        "requesterHash": digest(requester.encode()),
        "requesterUID": requester_uid,
        "requestDigest": request_digest,
        "sourceDigest": SOURCE,
        "queueNonce": QUEUE_NONCE,
    }
    queue = receipt("queue", QUEUE_NONCE, UIDS[6], queue_data)
    queue_link = {
        "name": queue["metadata"]["name"],
        "uid": UIDS[6],
        "dataDigest": data_digest(queue_data),
    }
    # Exact PinnedBuild JSON tags/order from recoveryreceipt/effect.go.
    binding = {
        "namespace": "runner-original",
        "namespaceUid": UIDS[0],
        "receiptNamespace": "receipts-original",
        "receiptNamespaceUid": UIDS[1],
        "genesisName": GENESIS,
        "genesisUid": UIDS[2],
        "generation": contract["generation"],
        "activeLedgerUid": UIDS[3],
        "queueLedgerUid": UIDS[4],
        "buildName": name,
        "buildUid": UIDS[5],
        "requesterName": requester,
        "requesterUid": requester_uid,
        "requesterHash": digest(requester.encode()),
        "requestDigest": request_digest,
        "sourceDigest": SOURCE,
        "workerPoolId": "pool-original",
    }
    grant_payload = {
        "build": binding,
        "admissionMode": "queued",
        "queueReceipt": queue_link,
        "grantNonce": GRANT_NONCE,
        "activeLedgerFence": 9,
        "activeLedgerResourceVersion": "266",
        "allocatedWorkerSlots": 1,
    }
    grant_data = {"version": "1", "kind": "grant", "payload": go_json(grant_payload)}
    grant = receipt("grant", GRANT_NONCE, UIDS[7], grant_data)
    pod_payload = {
        "build": copy.deepcopy(binding),
        "grantReceipt": {
            "name": grant["metadata"]["name"],
            "uid": UIDS[7],
            "dataDigest": data_digest(grant_data),
        },
        "podName": "kova-job-" + name,
        "podAttemptNonce": POD_NONCE,
        "podTemplateDigest": TEMPLATE,
        "runnerImageDigest": RUNNER,
        "buildRequestId": UIDS[5],
    }
    pod_data = {"version": "1", "kind": "pod-create", "payload": go_json(pod_payload)}
    pod = receipt("pod-create", POD_NONCE, UIDS[8], pod_data)
    build = {
        "apiVersion": "kova.cofy.dev/v1alpha1",
        "kind": "KovaBuild",
        "metadata": {
            "namespace": "runner-original",
            "name": name,
            "uid": UIDS[5],
            "resourceVersion": "280",
            "annotations": {
                "kova.cofy.dev/queue-intent": QUEUE_NONCE,
                "kova.cofy.dev/queue-receipt-uid": UIDS[6],
                "kova.cofy.dev/queue-receipt-digest": queue_link["dataDigest"],
            },
        },
        "spec": spec,
        "status": {
            "phase": "Succeeded",
            "admissionGenesisWitness": {
                "namespaceUID": UIDS[0],
                "genesisUID": UIDS[2],
                "generation": contract["generation"],
                "activeLedgerUID": UIDS[3],
                "queueLedgerUID": UIDS[4],
                "buildUID": UIDS[5],
                "podName": pod_payload["podName"],
                "podUID": UIDS[9],
                "podCreateAttempt": POD_NONCE,
                "podTemplateDigest": TEMPLATE,
                "runnerRequestID": UIDS[5],
            },
        },
    }
    allocation = {
        "buildName": name,
        "requester": requester,
        "slots": 1,
        "grantNonce": GRANT_NONCE,
        "grantFence": 9,
        "grantObservedRV": "266",
        "grantReceiptUID": UIDS[7],
        "grantReceiptDigest": data_digest(grant_data),
        "podAttemptNonce": POD_NONCE,
        "podTemplateDigest": TEMPLATE,
        "podReceiptUID": UIDS[8],
        "podReceiptDigest": data_digest(pod_data),
        "closing": True,
    }
    active = {
        "metadata": {"name": "kova-active-reservations", "uid": UIDS[3]},
        "data": {
            "reservations.json": go_json(
                {"version": 2, "fence": 10, "active": {UIDS[5]: allocation}}
            )
        },
    }
    return {
        "receipts": [queue, grant, pod],
        "builds": {name: build},
        "active": active,
        "plan": plan,
        "runtime": runtime,
        "contract": contract,
    }


def allocation_of(case: dict) -> dict:
    return json.loads(case["active"]["data"]["reservations.json"])["active"][UIDS[5]]


def replace_allocation(case: dict, entry: dict) -> None:
    state = json.loads(case["active"]["data"]["reservations.json"])
    state["active"][UIDS[5]] = entry
    case["active"]["data"]["reservations.json"] = go_json(state)


def replace_payload(case: dict, index: int, payload: dict, *, repin: bool = False) -> None:
    cm = case["receipts"][index]
    cm["data"]["payload"] = go_json(payload)
    if repin:
        entry = allocation_of(case)
        entry["grantReceiptDigest" if index == 1 else "podReceiptDigest"] = data_digest(cm["data"])
        replace_allocation(case, entry)


def campaign(case: dict) -> tuple[dict, dict]:
    build = next(iter(case["builds"].values()))
    name, spec = build["metadata"]["name"], build["spec"]
    known = {
        name: {
            "uid": UIDS[5],
            "terminal": True,
            "entry": {
                "key": spec["idempotencyKey"],
                "source_uri": spec["source"]["uri"],
                "source_digest": SOURCE,
                "output_tag": "registry.invalid/campaign:original",
            },
        },
    }
    pod = {
        "metadata": {
            "name": "kova-job-" + name,
            "namespace": "runner-original",
            "uid": UIDS[9],
            "labels": {"app.kubernetes.io/name": "kova-runner"},
            "ownerReferences": [
                {
                    "apiVersion": "kova.cofy.dev/v1alpha1",
                    "kind": "KovaBuild",
                    "name": name,
                    "uid": UIDS[5],
                    "controller": True,
                }
            ],
        },
    }
    return known, pod


class PendingReceiptTests(unittest.TestCase):
    def classify(self, case: dict) -> None:
        # This helper may only inspect supplied evidence: never query, mutate,
        # sleep, or manufacture an acknowledgement of receipt retirement.
        with (
            patch.object(burst, "command", side_effect=AssertionError("external command")),
            patch.object(burst.Kube, "get", side_effect=AssertionError("API read")),
            patch.object(burst.http.client, "HTTPConnection", side_effect=AssertionError("HTTP")),
            patch.object(burst.time, "sleep", side_effect=AssertionError("sleep")),
        ):
            return burst.pending_receipts_for_completed_builds(**case)

    def rejected(self, case: dict) -> None:
        with self.assertRaises((burst.UnknownOutcome, burst.ContractError)):
            self.classify(case)

    def test_all_original_receipts_only_classify_and_preserve_evidence(self):
        case = evidence()
        before = copy.deepcopy(case)
        self.assertIsNone(self.classify(case))
        self.assertEqual(case, before)

    def test_each_actual_kind_and_partial_retirement_order(self):
        for indexes in ((0,), (1,), (2,), (2, 0), (1, 2), (2, 1, 0), ()):
            with self.subTest(indexes=indexes):
                case = evidence()
                case["receipts"] = [case["receipts"][i] for i in indexes]
                self.assertIsNone(self.classify(case))

    def test_go_field_case_order_and_digest_are_independent(self):
        case = evidence()
        queue, grant, pod = case["receipts"]
        self.assertEqual(len(queue["data"]), 17)
        raw = grant["data"]["payload"]
        self.assertTrue(raw.startswith('{"build":{"namespace":"runner-original","namespaceUid":'))
        self.assertIn('"activeLedgerResourceVersion":"266","allocatedWorkerSlots":1}', raw)
        self.assertIn('"buildRequestId":"' + UIDS[5] + '"}', pod["data"]["payload"])
        self.assertNotEqual(raw, go_json(json.loads(raw), map_keys=True))
        self.assertEqual(allocation_of(case)["podReceiptDigest"], data_digest(pod["data"]))

    def test_root_ca_is_ignored_only_in_original_receipt_namespace(self):
        case = evidence()
        cm = {"metadata": {"name": "kube-root-ca.crt", "namespace": "receipts-original"}}
        case["receipts"].append(cm)
        self.classify(case)
        cm["metadata"]["namespace"] = "foreign"
        self.rejected(case)

    def test_all_kinds_reject_foreign_or_mutable_object_metadata(self):
        changes = (
            ("namespace", "foreign"),
            ("uid", UIDS[10]),
            ("uid", "not-a-uuid"),
            ("resourceVersion", ""),
            ("deletionTimestamp", "2026-10-11T00:00:00Z"),
            ("ownerReferences", [{"uid": UIDS[10], "kind": "KovaBuild"}]),
            ("name", "foreign"),
        )
        for index in range(3):
            for key, value in changes:
                with self.subTest(kind=index, key=key, value=value):
                    case = evidence()
                    case["receipts"][index]["metadata"][key] = value
                    self.rejected(case)
            for key, value in (
                ("immutable", False),
                ("immutable", None),
                ("binaryData", {"x": "eA=="}),
            ):
                with self.subTest(kind=index, key=key):
                    case = evidence()
                    case["receipts"][index][key] = value
                    self.rejected(case)

    def test_all_kinds_reject_extra_or_unknown_labels(self):
        for index in range(3):
            case = evidence()
            case["receipts"][index]["metadata"]["labels"]["foreign"] = "true"
            self.rejected(case)
        case = evidence()
        case["receipts"][2]["metadata"]["labels"] = {"kova.cofy.dev/recovery-receipt": "adopt"}
        self.rejected(case)

    def test_queue_rejects_every_epoch_source_requester_and_original_pin_drift(self):
        fields = (
            "namespaceName",
            "namespaceUID",
            "receiptNamespaceName",
            "receiptNamespaceUID",
            "genesisName",
            "genesisUID",
            "generation",
            "activeLedgerUID",
            "queueLedgerUID",
            "requesterName",
            "requesterHash",
            "requesterUID",
            "sourceDigest",
            "queueNonce",
            "requestDigest",
            "version",
            "buildName",
        )
        for key in fields:
            with self.subTest(key=key):
                case = evidence()
                case["receipts"][0]["data"][key] = "foreign"
                self.rejected(case)
        for key in (
            "kova.cofy.dev/queue-intent",
            "kova.cofy.dev/queue-receipt-uid",
            "kova.cofy.dev/queue-receipt-digest",
        ):
            case = evidence()
            next(iter(case["builds"].values()))["metadata"]["annotations"][key] = "foreign"
            self.rejected(case)

    def test_queue_schema_case_extra_and_nonhex_request_digest(self):
        for operation in ("case", "extra", "missing", "upper", "prefixed"):
            with self.subTest(operation=operation):
                case = evidence()
                data = case["receipts"][0]["data"]
                if operation == "case":
                    data["namespaceUid"] = data.pop("namespaceUID")
                elif operation == "extra":
                    data["adopt"] = "true"
                elif operation == "missing":
                    del data["version"]
                else:
                    data["requestDigest"] = (
                        "A" * 64 if operation == "upper" else "sha256:" + "a" * 64
                    )
                self.rejected(case)

    def test_effect_rejects_every_binding_drift_even_with_matching_data_digest(self):
        for index in (1, 2):
            for key in json.loads(evidence()["receipts"][index]["data"]["payload"])["build"]:
                with self.subTest(kind=index, key=key):
                    case = evidence()
                    payload = json.loads(case["receipts"][index]["data"]["payload"])
                    payload["build"][key] = "foreign"
                    replace_payload(case, index, payload, repin=True)
                    self.rejected(case)

    def test_effect_rejects_closed_schema_and_payload_bound(self):
        for index in (1, 2):
            for key, value in (
                ("version", "2"),
                ("kind", "adopt"),
                ("payload", {}),
                ("payload", " " * 16385),
            ):
                case = evidence()
                case["receipts"][index]["data"][key] = value
                self.rejected(case)
            case = evidence()
            case["receipts"][index]["data"]["extra"] = "forbidden"
            self.rejected(case)

    def test_effect_requires_original_active_entry_uid_digest_name_and_nonce(self):
        for index, prefix in ((1, "grant"), (2, "pod")):
            for key in (
                prefix + "ReceiptUID",
                prefix + "ReceiptDigest",
                "buildName",
                "grantNonce" if index == 1 else "podAttemptNonce",
            ):
                with self.subTest(kind=index, key=key):
                    case = evidence()
                    entry = allocation_of(case)
                    entry[key] = "foreign"
                    replace_allocation(case, entry)
                    self.rejected(case)
        case = evidence()
        case["active"]["data"]["reservations.json"] = '{"version":2,"active":{}}'
        case["receipts"] = case["receipts"][1:]
        self.rejected(case)

    def test_grant_rejects_original_queue_link_and_allocation_drift(self):
        for key in ("name", "uid", "dataDigest"):
            case = evidence()
            payload = json.loads(case["receipts"][1]["data"]["payload"])
            payload["queueReceipt"][key] = "foreign"
            replace_payload(case, 1, payload, repin=True)
            self.rejected(case)
        for key in (
            "admissionMode",
            "grantNonce",
            "activeLedgerFence",
            "activeLedgerResourceVersion",
            "allocatedWorkerSlots",
        ):
            case = evidence()
            payload = json.loads(case["receipts"][1]["data"]["payload"])
            payload[key] = "foreign"
            replace_payload(case, 1, payload, repin=True)
            self.rejected(case)

    def test_pod_rejects_original_grant_link_witness_template_request_and_image_drift(self):
        for key in ("name", "uid", "dataDigest"):
            case = evidence()
            payload = json.loads(case["receipts"][2]["data"]["payload"])
            payload["grantReceipt"][key] = "foreign"
            replace_payload(case, 2, payload, repin=True)
            self.rejected(case)
        for key in (
            "podName",
            "podAttemptNonce",
            "podTemplateDigest",
            "runnerImageDigest",
            "buildRequestId",
        ):
            case = evidence()
            payload = json.loads(case["receipts"][2]["data"]["payload"])
            payload[key] = "foreign"
            replace_payload(case, 2, payload, repin=True)
            self.rejected(case)

    def test_pod_witness_cannot_replace_original_attempt_template_or_request_id(self):
        for payload_key, witness_key, wrong in (
            ("podAttemptNonce", "podCreateAttempt", "5" * 32),
            ("podTemplateDigest", "podTemplateDigest", "sha256:" + "d" * 64),
            ("buildRequestId", "runnerRequestID", UIDS[10]),
        ):
            with self.subTest(key=payload_key):
                case = evidence()
                payload = json.loads(case["receipts"][2]["data"]["payload"])
                payload[payload_key] = wrong
                next(iter(case["builds"].values()))["status"]["admissionGenesisWitness"][
                    witness_key
                ] = wrong
                replace_payload(case, 2, payload, repin=True)
                self.rejected(case)

    def test_original_source_and_epoch_replacement_are_not_new_evidence(self):
        for target, key, wrong in (
            ("genesis", "genesis_uid", UIDS[10]),
            ("genesis", "active_ledger_uid", UIDS[10]),
            ("genesis", "queue_ledger_uid", UIDS[10]),
            ("genesis", "worker_pool_id", "pool-foreign"),
            ("runtime", "requester", "foreign"),
            ("runtime", "requester_uid", "foreign"),
            ("contract", "generation", "5" * 32),
        ):
            case = evidence()
            where = case["runtime"]["genesis"] if target == "genesis" else case[target]
            where[key] = wrong
            self.rejected(case)
        case = evidence()
        next(iter(case["builds"].values()))["spec"]["source"]["digest"] = "sha256:" + "e" * 64
        self.rejected(case)

    def test_effect_json_case_payload_extra_and_duplicate_keys_fail_closed(self):
        for index in (1, 2):
            for operation in ("case", "extra", "duplicate-root", "duplicate-build"):
                with self.subTest(kind=index, operation=operation):
                    case = evidence()
                    cm = case["receipts"][index]
                    payload = json.loads(cm["data"]["payload"])
                    if operation == "case":
                        payload["build"]["buildUID"] = payload["build"].pop("buildUid")
                    elif operation == "extra":
                        payload["adopt"] = True
                    if operation.startswith("duplicate"):
                        raw = cm["data"]["payload"]
                        if operation == "duplicate-root":
                            raw = raw[:-1] + ',"build":' + go_json(payload["build"]) + "}"
                        else:
                            raw = raw.replace(
                                '"namespace":"runner-original"',
                                '"namespace":"runner-original","namespace":"runner-original"',
                                1,
                            )
                        cm["data"]["payload"] = raw
                        entry = allocation_of(case)
                        entry["grantReceiptDigest" if index == 1 else "podReceiptDigest"] = (
                            data_digest(cm["data"])
                        )
                        replace_allocation(case, entry)
                    else:
                        replace_payload(case, index, payload, repin=True)
                    self.rejected(case)

    def test_duplicate_active_ledger_json_is_not_silently_deduplicated(self):
        case = evidence()
        raw = case["active"]["data"]["reservations.json"]
        case["active"]["data"]["reservations.json"] = raw[:-1] + ',"active":{}}'
        self.rejected(case)

    def original_filter(self, case: dict, *, builds: list | None = None, pod: dict | None = None):
        known, original_pod = campaign(case)
        kube = Mock(spec=burst.Kube)
        kube.list.side_effect = [
            list(case["builds"].values()) if builds is None else builds,
            [original_pod if pod is None else pod],
        ]
        result = burst.campaign_objects_preflight(kube, case["plan"], case["runtime"], known, 90)
        self.assertEqual(kube.list.call_count, 2)
        self.assertEqual(
            [call.args[1:] for call in kube.list.call_args_list],
            [("kovabuild", "runner-original"), ("pod", "runner-original")],
        )
        return result

    def test_actual_original_successful_cr_and_runner_filter_then_classifies(self):
        case = evidence()
        originals = self.original_filter(case)
        self.assertEqual(originals, case["builds"])
        case["builds"] = originals
        self.assertIsNone(self.classify(case))

    def test_original_filter_rejects_any_non_successful_terminal_phase(self):
        for phase in ("Failed", "Cancelled", "Running", "Queued", None):
            with self.subTest(phase=phase):
                case = evidence()
                next(iter(case["builds"].values()))["status"]["phase"] = phase
                with self.assertRaises(burst.UnknownOutcome):
                    self.original_filter(case)

    def test_original_filter_rejects_unknown_duplicate_and_missing_cr(self):
        case = evidence()
        build = next(iter(case["builds"].values()))
        foreign = copy.deepcopy(build)
        foreign["metadata"]["name"] = "unknown-build"
        for builds in ([], [build, copy.deepcopy(build)], [build, foreign], [foreign]):
            with self.subTest(count=len(builds)):
                with self.assertRaises(burst.UnknownOutcome):
                    self.original_filter(case, builds=builds)

    def test_original_filter_rejects_replaced_uid_deletion_and_mutated_request(self):
        for key, wrong in (
            ("uid", UIDS[10]),
            ("namespace", "foreign"),
            ("deletionTimestamp", "2026-10-11T00:00:00Z"),
        ):
            case = evidence()
            next(iter(case["builds"].values()))["metadata"][key] = wrong
            with self.assertRaises(burst.UnknownOutcome):
                self.original_filter(case)
        for section, key, wrong in (
            ("source", "digest", "sha256:" + "e" * 64),
            ("requester", "username", "foreign"),
            ("requester", "uid", "foreign"),
            ("build", "concurrency", 2),
        ):
            case = evidence()
            next(iter(case["builds"].values()))["spec"][section][key] = wrong
            with self.assertRaises(burst.UnknownOutcome):
                self.original_filter(case)

    def test_original_filter_rejects_foreign_runner_namespace_or_owner(self):
        for section, key, wrong in (
            ("metadata", "namespace", "foreign"),
            ("owner", "uid", UIDS[10]),
            ("owner", "controller", False),
            ("owner", "kind", "Deployment"),
        ):
            case = evidence()
            _, pod = campaign(case)
            target = (
                pod["metadata"] if section == "metadata" else pod["metadata"]["ownerReferences"][0]
            )
            target[key] = wrong
            with self.assertRaises(burst.UnknownOutcome):
                self.original_filter(case, pod=pod)

    def test_pod_runtime_claim_cannot_override_candidate_manifest_digest(self):
        case = evidence()
        wrong = "sha256:" + "e" * 64
        case["runtime"]["genesis"]["runner_image_digest"] = wrong
        payload = json.loads(case["receipts"][2]["data"]["payload"])
        payload["runnerImageDigest"] = wrong
        replace_payload(case, 2, payload, repin=True)
        self.rejected(case)


if __name__ == "__main__":
    unittest.main()

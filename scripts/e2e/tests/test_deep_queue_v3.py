"""Pure-local directed Genesis-v3 probe regressions; no live acceptance claims."""

from __future__ import annotations

import copy
import hashlib
import importlib.util
import io
import json
import os
import tempfile
import time
import types
import unittest
from pathlib import Path
from unittest.mock import patch

SCRIPT = Path(__file__).resolve().parents[1] / "e2e-service-admission-deep-queue.py"
SPEC = importlib.util.spec_from_file_location("deep_queue_v3_test_bench", SCRIPT)
BENCH = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(BENCH)
V3 = BENCH.V3


def uid(index: int) -> str:
    return f"00000000-0000-4000-8000-{index:012x}"


def fixture() -> dict:
    run = "v3-20261011-t1"
    cluster = "kova-deep-queue-" + run
    return {
        "schema_version": 1,
        "run_id": run,
        "profile": "representative-short",
        "candidate_commit": "a" * 40,
        "pod_template_binary": "/private/fixture-capacity.test",
        "pod_template_binary_sha256": "9" * 64,
        "host_boot_id": uid(1),
        "cluster": cluster,
        "kubeconfig": "/private/fixture.kubeconfig",
        "kubeconfig_sha256": "b" * 64,
        "kube_system_namespace_uid": uid(2),
        "control_namespace": "control-" + run,
        "control_namespace_uid": uid(3),
        "runner_namespace": "runner-" + run,
        "runner_namespace_uid": uid(4),
        "receipt_namespace": "receipt-" + run,
        "receipt_namespace_uid": uid(5),
        "release": "kova",
        "auth_secret_uid": uid(6),
        "service_deployment_uid": uid(7),
        "worker_deployment_uid": uid(8),
        "node_uids": {cluster + "-control-plane": uid(9), cluster + "-worker": uid(10)},
        "service_pods": {"kova-service-one": uid(11), "kova-service-two": uid(12)},
        "worker_pods": {"kova-worker-one": uid(13)},
        "role_images": {
            role: f"example.invalid/kova-{role}@sha256:" + digit * 64
            for role, digit in (("controller", "c"), ("runner", "d"), ("worker", "e"))
        },
        "genesis": {
            "secret_name": "installation-receipt",
            "secret_uid": uid(14),
            "receipt_sha256": "f" * 64,
            "genesis_uid": uid(15),
            "active_ledger_uid": uid(16),
            "queue_ledger_uid": uid(17),
            "worker_pool_id": "pool-" + run,
        },
        "ports": {"service": 18086, "api_proxy": 18087, "metrics_remote": 18088},
    }


def contract(fix: dict) -> dict:
    return {
        "version": 3,
        "namespaceUID": fix["runner_namespace_uid"],
        "receiptNamespace": fix["receipt_namespace"],
        "receiptNamespaceUID": fix["receipt_namespace_uid"],
        "workerPoolID": fix["genesis"]["worker_pool_id"],
        "runnerImage": fix["role_images"]["runner"],
        "generation": "1" * 32,
        "activeLedgerName": V3.ACTIVE,
        "activeLedgerSchema": 2,
        "queueLedgerName": V3.QUEUE,
        "queueLedgerSchema": 2,
        "limits": V3.limits(fix),
    }


def metadata(name: str, namespace: str, identity: str) -> dict:
    return {
        "name": name,
        "namespace": namespace,
        "uid": identity,
        "resourceVersion": "22",
    }


def queued(fix: dict, con: dict, key: str, index: int = 100) -> tuple[dict, dict, dict]:
    name = BENCH.build_id(key)
    nonce = f"{index:032x}"
    spec = {
        "requester": {"username": "kova:e2e"},
        "targets": [
            {
                "target": f"unused.invalid/deep-queue/{fix['run_id']}:request-{index}",
                "platform": "linux/amd64",
            }
        ],
        "source": {
            "uri": f"oci://unused.invalid/deep-queue/{fix['run_id']}@sha256:"
            + "a" * 64,
            "digest": "sha256:" + "a" * 64,
        },
        "build": {"format": "oci", "concurrency": 1},
        "idempotencyKey": key,
    }
    build = {
        "metadata": metadata(name, fix["runner_namespace"], uid(index)),
        "spec": spec,
        "status": {"phase": "Queued"},
    }
    request_digest = hashlib.sha256(
        json.dumps(spec, separators=(",", ":")).encode()
    ).hexdigest()
    data = {
        "version": "1",
        "namespaceName": fix["runner_namespace"],
        "namespaceUID": con["namespaceUID"],
        "receiptNamespaceName": fix["receipt_namespace"],
        "receiptNamespaceUID": con["receiptNamespaceUID"],
        "genesisName": V3.GENESIS,
        "genesisUID": fix["genesis"]["genesis_uid"],
        "generation": con["generation"],
        "activeLedgerUID": fix["genesis"]["active_ledger_uid"],
        "queueLedgerUID": fix["genesis"]["queue_ledger_uid"],
        "buildName": name,
        "requesterName": "kova:e2e",
        "requesterUID": "",
        "requesterHash": hashlib.sha256(b"kova:e2e").hexdigest(),
        "requestDigest": request_digest,
        "sourceDigest": spec["source"]["digest"],
        "queueNonce": nonce,
    }
    receipt = {
        "metadata": metadata(
            "kova-admission-intent-" + nonce,
            fix["receipt_namespace"],
            uid(index + 2000),
        ),
        "immutable": True,
        "data": data,
    }
    receipt["metadata"]["labels"] = {V3.PREFIX + "admission-receipt": "queue-intent"}
    receipt_digest = V3.digest_data(data)
    intent = {
        "requesterHash": data["requesterHash"],
        "requestDigest": request_digest,
        "nonce": nonce,
        "createdAtUnix": 1791680000,
        "receiptUID": receipt["metadata"]["uid"],
        "receiptDigest": receipt_digest,
    }
    build["metadata"]["annotations"] = {
        V3.PREFIX + "queue-intent": nonce,
        V3.PREFIX + "queue-receipt-uid": intent["receiptUID"],
        V3.PREFIX + "queue-receipt-digest": receipt_digest,
    }
    return build, intent, receipt


def mock_pod_digest(pod: dict) -> str:
    # A directed mock oracle, NOT an implementation of typed Go PodSpec
    # canonicalization. The native Go regressions separately exercise the real
    # CanonicalPodTemplateDigest implementation and its defaults/drift rules.
    projection = {"metadata": copy.deepcopy(pod["metadata"]), "spec": pod["spec"]}
    for field in ("uid", "resourceVersion"):
        projection["metadata"].pop(field, None)
    projection["metadata"]["annotations"].pop(V3.PREFIX + "pod-template-digest", None)
    return "sha256:" + hashlib.sha256(V3.encoded(projection)).hexdigest()


def active(fix: dict, con: dict) -> tuple[dict, dict, dict, dict, dict]:
    build, intent, queue_receipt = queued(fix, con, "blocker", 101)
    name, build_uid = build["metadata"]["name"], build["metadata"]["uid"]
    pod_uid = uid(102)
    entry = {
        "buildName": name,
        "requester": "kova:e2e",
        "slots": 1,
        "grantNonce": "2" * 32,
        "grantFence": 1,
        "grantObservedRV": "22",
        "grantReceiptUID": uid(103),
        "podAttemptNonce": "3" * 32,
        "podTemplateDigest": "sha256:" + "4" * 64,
        "podReceiptUID": uid(104),
    }
    pinned = {
        "namespace": fix["runner_namespace"],
        "namespaceUid": con["namespaceUID"],
        "receiptNamespace": fix["receipt_namespace"],
        "receiptNamespaceUid": con["receiptNamespaceUID"],
        "genesisName": V3.GENESIS,
        "genesisUid": fix["genesis"]["genesis_uid"],
        "generation": con["generation"],
        "activeLedgerUid": fix["genesis"]["active_ledger_uid"],
        "queueLedgerUid": fix["genesis"]["queue_ledger_uid"],
        "buildName": name,
        "buildUid": build_uid,
        "requesterName": "kova:e2e",
        "requesterUid": "",
        "requesterHash": intent["requesterHash"],
        "requestDigest": intent["requestDigest"],
        "sourceDigest": build["spec"]["source"]["digest"],
        "workerPoolId": con["workerPoolID"],
    }
    grant_name = "kova-grant-intent-" + entry["grantNonce"]
    grant = {
        "build": pinned,
        "admissionMode": "queued",
        "queueReceipt": {
            "name": queue_receipt["metadata"]["name"],
            "uid": intent["receiptUID"],
            "dataDigest": intent["receiptDigest"],
        },
        "grantNonce": entry["grantNonce"],
        "activeLedgerFence": 1,
        "activeLedgerResourceVersion": "22",
        "allocatedWorkerSlots": 1,
    }
    grant_data = {
        "version": "1",
        "kind": "grant",
        "payload": json.dumps(grant, separators=(",", ":")),
    }
    entry["grantReceiptDigest"] = V3.digest_data(grant_data)
    create = {
        "build": pinned,
        "grantReceipt": {
            "name": grant_name,
            "uid": entry["grantReceiptUID"],
            "dataDigest": entry["grantReceiptDigest"],
        },
        "podName": "kova-job-" + name,
        "podAttemptNonce": entry["podAttemptNonce"],
        "podTemplateDigest": entry["podTemplateDigest"],
        "runnerImageDigest": con["runnerImage"].rsplit("@", 1)[1],
        "buildRequestId": build_uid,
    }
    create_data = {
        "version": "1",
        "kind": "pod-create",
        "payload": json.dumps(create, separators=(",", ":")),
    }
    entry["podReceiptDigest"] = V3.digest_data(create_data)
    objects = {queue_receipt["metadata"]["name"]: queue_receipt}
    for kind, data, prefix, receipt_name in (
        ("grant", grant_data, "grant", grant_name),
        (
            "pod-create",
            create_data,
            "pod",
            "kova-pod-create-intent-" + entry["podAttemptNonce"],
        ),
    ):
        obj = {
            "metadata": metadata(
                receipt_name, fix["receipt_namespace"], entry[prefix + "ReceiptUID"]
            ),
            "immutable": True,
            "data": data,
        }
        obj["metadata"]["labels"] = {V3.PREFIX + "recovery-receipt": kind}
        objects[receipt_name] = obj
    witness = {
        "namespaceUID": con["namespaceUID"],
        "genesisUID": fix["genesis"]["genesis_uid"],
        "generation": con["generation"],
        "activeLedgerUID": fix["genesis"]["active_ledger_uid"],
        "queueLedgerUID": fix["genesis"]["queue_ledger_uid"],
        "buildUID": build_uid,
        "podName": "kova-job-" + name,
        "podUID": pod_uid,
        "podCreateAttempt": entry["podAttemptNonce"],
        "podTemplateDigest": entry["podTemplateDigest"],
        "runnerRequestID": build_uid,
    }
    build["status"] = {
        "phase": "Starting",
        "admissionGenesisWitness": witness,
        "runnerPodName": witness["podName"],
    }
    stamps = {
        "runner-namespace-uid": con["namespaceUID"],
        "runner-genesis-uid": witness["genesisUID"],
        "runner-generation": con["generation"],
        "runner-active-ledger-uid": witness["activeLedgerUID"],
        "runner-queue-ledger-uid": witness["queueLedgerUID"],
        "runner-build-uid": build_uid,
        "runner-request-id": build_uid,
        "create-attempt": entry["podAttemptNonce"],
        "pod-template-digest": entry["podTemplateDigest"],
    }
    pod = {
        "metadata": metadata(witness["podName"], fix["runner_namespace"], pod_uid),
        "spec": {
            "containers": [
                {
                    "name": "runner",
                    "image": con["runnerImage"],
                    "imagePullPolicy": "Never",
                    "env": [
                        {
                            "name": "KOVA_RUNNER_POD_UID",
                            "valueFrom": {
                                "fieldRef": {
                                    "apiVersion": "v1",
                                    "fieldPath": "metadata.uid",
                                }
                            },
                        }
                    ],
                }
            ]
        },
    }
    pod["metadata"]["annotations"] = {
        V3.PREFIX + key: value for key, value in stamps.items()
    }
    digest = mock_pod_digest(pod)
    entry["podTemplateDigest"] = digest
    witness["podTemplateDigest"] = digest
    pod["metadata"]["annotations"][V3.PREFIX + "pod-template-digest"] = digest
    create["podTemplateDigest"] = digest
    create_data["payload"] = json.dumps(create, separators=(",", ":"))
    entry["podReceiptDigest"] = V3.digest_data(create_data)
    return build, pod, entry, intent, objects


def ledger(fix: dict, con: dict, role: str, state: dict) -> dict:
    name, key = (
        (V3.ACTIVE, "reservations.json")
        if role == "active"
        else (V3.QUEUE, "queue.json")
    )
    obj = {
        "metadata": metadata(
            name, fix["runner_namespace"], fix["genesis"][role + "_ledger_uid"]
        ),
        "data": {key: json.dumps(state, separators=(",", ":"))},
    }
    stamps = {
        "namespace-uid": con["namespaceUID"],
        "genesis-uid": fix["genesis"]["genesis_uid"],
        "generation": con["generation"],
        "contract": V3.contract_digest(con),
        "role": role,
        "schema": "2",
        "bootstrap": "5" * 32,
    }
    obj["metadata"]["annotations"] = {
        V3.PREFIX + "admission-" + key: value for key, value in stamps.items()
    }
    return obj


class V3QualificationTest(unittest.TestCase):
    def setUp(self) -> None:
        self.fix = fixture()
        self.con = contract(self.fix)

    def test_original_fixture_closed_pins_and_caps(self) -> None:
        V3.validate_fixture(self.fix)
        self.assertEqual(V3.limits(self.fix)["maxQueuedJobs"], 100)
        other = dict(self.fix, profile="supported-capacity")
        self.assertEqual(V3.limits(other)["maxQueuedJobs"], 1000)
        for field, value in (
            ("schema_version", True),
            ("profile", None),
            ("runner_namespace", "old"),
            ("candidate_commit", "a" * 12),
            ("control_namespace_uid", uid(5)),
        ):
            with self.subTest(field=field), self.assertRaises(V3.QualificationError):
                V3.validate_fixture(dict(self.fix, **{field: value}))

    def test_pending_terminating_runner_cannot_qualify(self) -> None:
        build, pod, _, _, _ = active(self.fix, self.con)
        name, identity = build["metadata"]["name"], build["metadata"]["uid"]
        pod["metadata"]["ownerReferences"] = [
            {"kind": "KovaBuild", "name": name, "uid": identity, "controller": True}
        ]
        pod["spec"]["nodeSelector"] = {"never": "true"}
        pod["status"] = {"phase": "Pending"}
        pod.update(apiVersion="v1", kind="Pod")

        def response(*args: str) -> dict:
            return (
                {"apiVersion": "v1", "kind": "List", "items": [pod]}
                if args[3] == "pods"
                else pod
            )

        with (
            patch.object(BENCH, "NAMESPACE", self.fix["runner_namespace"]),
            patch.object(BENCH, "kjson", side_effect=response),
        ):
            self.assertIs(BENCH.check_runner(name, identity), pod)
            for timestamp in ("2026-10-11T00:00:00Z", "", False):
                pod["metadata"]["deletionTimestamp"] = timestamp
                with (
                    self.subTest(timestamp=timestamp),
                    self.assertRaises((BENCH.BenchError, V3.QualificationError)),
                ):
                    BENCH.check_runner(name, identity)

    def test_one_budget_spent_by_preflight_is_not_renewed_for_live(self) -> None:
        clock = [100.0]
        with (
            patch.object(BENCH, "EXECUTION_DEADLINE", None),
            patch.object(BENCH, "RUN_DEADLINE_SECONDS", 900),
            patch.object(BENCH.time, "monotonic", side_effect=lambda: clock[0]),
        ):
            self.assertEqual(BENCH.start_execution_budget(), 1000.0)
            clock[0] += 250  # all three image saves and other read-only preflight work
            self.assertEqual(BENCH.live_deadline(), 1000.0)
            self.assertEqual(BENCH.budget_timeout(900), 650.0)
            clock[0] = 999.5
            completed = types.SimpleNamespace(returncode=0, stdout="ok")
            with patch.object(BENCH.subprocess, "run", return_value=completed) as run:
                self.assertEqual(BENCH.command(["mock-read-only"], timeout=90), "ok")
                self.assertEqual(run.call_args.kwargs["timeout"], 0.5)
            clock[0] = 1000
            with (
                patch.object(BENCH.subprocess, "run") as run,
                patch.object(BENCH.OPENER, "open") as wire,
            ):
                for action in (
                    lambda: BENCH.command(["mock-read-only"]),
                    lambda: BENCH.request_json(18087, "DELETE", "/never-sent"),
                    BENCH.live_deadline,
                ):
                    with self.assertRaises(BENCH.BenchError):
                        action()
                run.assert_not_called()
                wire.assert_not_called()

    def test_streamed_image_save_is_bounded_by_original_budget(self) -> None:
        process = types.SimpleNamespace(
            stdout=io.BytesIO(b"mock-archive"),
            wait=lambda **_: 0,
        )
        with (
            patch.object(BENCH, "EXECUTION_DEADLINE", 1000.0),
            patch.object(BENCH.time, "monotonic", return_value=999.0),
            patch.object(BENCH.subprocess, "Popen", return_value=process) as popen,
            patch.object(BENCH, "parse_oci_saved_image", return_value={"mock": True}),
        ):
            BENCH.saved_image_fact("mock-image", "sha256:" + "a" * 64, "b" * 40)
            self.assertEqual(popen.call_args.args[0][2], "1.000000s")

    def test_template_binary_read_ignores_atime_but_binds_bytes_and_stable_identity(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as directory:
            binary = Path(directory).resolve() / "candidate.test"
            binary.write_bytes(b"private-offline-mock-binary")
            binary.chmod(0o700)
            fix = dict(
                self.fix,
                pod_template_binary=str(binary),
                pod_template_binary_sha256=hashlib.sha256(
                    binary.read_bytes()
                ).hexdigest(),
            )
            info = binary.stat()
            fake = types.SimpleNamespace(
                **{
                    key: getattr(info, key)
                    for key in (
                        "st_dev",
                        "st_ino",
                        "st_mode",
                        "st_uid",
                        "st_gid",
                        "st_size",
                        "st_mtime_ns",
                        "st_ctime_ns",
                    )
                },
                st_atime_ns=0,
            )

            def changed_atime(*_, **__) -> object:
                fake.st_atime_ns += 1
                return fake

            with (
                patch.object(BENCH, "FIXTURE", fix),
                patch.object(BENCH, "EXECUTION_DEADLINE", None),
                patch.object(Path, "stat", side_effect=changed_atime),
            ):
                self.assertEqual(BENCH.qualify_template_binary(), binary)
                fake.st_mtime_ns += 1
                with self.assertRaises(BENCH.BenchError):
                    BENCH.qualify_template_binary()
            with patch.object(
                BENCH, "FIXTURE", dict(fix, pod_template_binary_sha256="0" * 64)
            ):
                with self.assertRaises(BENCH.BenchError):
                    BENCH.qualify_template_binary()

    def test_local_child_shutdown_has_one_shared_bound_and_no_api_operation(
        self,
    ) -> None:
        clock = [100.0]
        calls = []

        class Child:
            def __init__(self, name: str, slow: bool = False):
                self.name, self.slow, self.done = name, slow, False
                self.stdout = io.BytesIO()

            def poll(self) -> int | None:
                return 0 if self.done else None

            def terminate(self) -> None:
                calls.append(("terminate", self.name))

            def kill(self) -> None:
                calls.append(("kill", self.name))
                self.done = True

            def wait(self, *, timeout: float) -> int:
                calls.append(("wait", self.name, timeout))
                if self.slow and not self.done:
                    clock[0] += timeout
                    raise BENCH.subprocess.TimeoutExpired("own-local-child", timeout)
                self.done = True
                return 0

        one, two = Child("one", True), Child("two")
        with (
            patch.object(BENCH.time, "monotonic", side_effect=lambda: clock[0]),
            patch.object(BENCH, "kctl") as api,
        ):
            BENCH.close_local_processes(
                [types.SimpleNamespace(process=one)], two, None, 102
            )
            self.assertEqual(clock[0], 102)
            self.assertEqual(calls[:2], [("terminate", "one"), ("terminate", "two")])
            self.assertTrue(all(call[2] <= 2 for call in calls if call[0] == "wait"))
            self.assertTrue(one.done and two.done)
            api.assert_not_called()

    def test_runner_template_uses_named_get_joined_to_original_list_uid(self) -> None:
        build, pod, _, _, _ = active(self.fix, self.con)
        name, identity = build["metadata"]["name"], build["metadata"]["uid"]
        pod.update(apiVersion="v1", kind="Pod", status={"phase": "Pending"})
        pod["spec"]["nodeSelector"] = {"never": "true"}
        pod["metadata"]["ownerReferences"] = [
            {"kind": "KovaBuild", "name": name, "uid": identity, "controller": True}
        ]
        listing = {
            "apiVersion": "v1",
            "kind": "List",
            "items": [{"metadata": copy.deepcopy(pod["metadata"])}],
        }
        with (
            patch.object(BENCH, "NAMESPACE", self.fix["runner_namespace"]),
            patch.object(BENCH, "kjson", side_effect=[listing, pod]) as reads,
        ):
            self.assertIs(BENCH.check_runner(name, identity), pod)
            self.assertEqual(
                reads.call_args_list[1].args[2:5], ("get", "pod", "kova-job-" + name)
            )
        changed = copy.deepcopy(pod)
        changed["metadata"]["uid"] = uid(999)
        with (
            patch.object(BENCH, "NAMESPACE", self.fix["runner_namespace"]),
            patch.object(BENCH, "kjson", side_effect=[listing, changed]),
            self.assertRaises(V3.QualificationError),
        ):
            BENCH.check_runner(name, identity)

    def test_private_fixture_bytes_and_duplicate_fields(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory).resolve() / "fixture.json"
            path.write_bytes(V3.encoded(self.fix))
            path.chmod(0o600)
            loaded, digest = V3.load_fixture(path)
            self.assertEqual(loaded, self.fix)
            self.assertEqual(digest, hashlib.sha256(path.read_bytes()).hexdigest())
            path.chmod(0o644)
            with self.assertRaises(V3.QualificationError):
                V3.load_fixture(path)
        with self.assertRaises(V3.QualificationError):
            V3.decode('{"a":1,"a":2}')

    def test_contract_order_and_limits_are_not_aliased(self) -> None:
        receipt = {
            "namespace": self.fix["runner_namespace"],
            "genesisName": V3.GENESIS,
            "genesisUID": self.fix["genesis"]["genesis_uid"],
            "contract": self.con,
        }
        V3.qualify_contract(receipt, self.fix)
        ordered = {key: self.con[key] for key in V3.CONTRACT_KEYS}
        self.assertEqual(
            V3.contract_digest(self.con),
            hashlib.sha256(
                json.dumps(ordered, separators=(",", ":")).encode()
            ).hexdigest(),
        )
        for field, value in (
            ("version", True),
            ("generation", ""),
            ("queueLedgerSchema", 1),
        ):
            bad = copy.deepcopy(receipt)
            bad["contract"][field] = value
            with self.assertRaises(V3.QualificationError):
                V3.qualify_contract(bad, self.fix)

    def test_queue_receipt_binds_original_spec_digest_and_namespace(self) -> None:
        build, intent, receipt = queued(self.fix, self.con, "queued-key")
        V3.qualify_queue_receipt(receipt, build, intent, self.fix, self.con)
        for target in ("UID", "source", "data", "alias", "bool"):
            b, i, r = copy.deepcopy((build, intent, receipt))
            if target == "UID":
                r["metadata"]["uid"] = uid(999)
            elif target == "source":
                b["spec"]["source"]["digest"] = "sha256:" + "b" * 64
            elif target == "data":
                r["data"]["queueNonce"] = "f" * 32
            elif target == "alias":
                r["data"]["NamespaceUID"] = r["data"]["namespaceUID"]
            else:
                b["spec"]["build"]["concurrency"] = True
            with self.subTest(target=target), self.assertRaises(V3.QualificationError):
                V3.qualify_queue_receipt(r, b, i, self.fix, self.con)

    def test_three_receipts_and_original_runner_uid_fence(self) -> None:
        values = active(self.fix, self.con)
        V3.qualify_active_receipts(
            *values[:4], values[4], self.fix, self.con, mock_pod_digest
        )
        for target in (
            "grant",
            "pod",
            "witness",
            "fence",
            "manifest",
            "label",
            "extra-spec",
        ):
            build, pod, entry, intent, objects = copy.deepcopy(values)
            if target in ("grant", "pod"):
                entry[target + "ReceiptUID"] = uid(999)
            elif target == "witness":
                build["status"]["admissionGenesisWitness"]["podUID"] = uid(999)
            elif target == "fence":
                pod["spec"]["containers"][0]["env"][0]["value"] = pod["metadata"]["uid"]
            elif target == "label":
                pod["metadata"]["labels"] = {"injected": "true"}
            elif target == "extra-spec":
                pod["spec"]["hostNetwork"] = True
            else:
                pod["spec"]["containers"][0]["image"] = (
                    "example.invalid/other@sha256:" + "d" * 64
                )
            with self.subTest(target=target), self.assertRaises(V3.QualificationError):
                V3.qualify_active_receipts(
                    build,
                    pod,
                    entry,
                    intent,
                    objects,
                    self.fix,
                    self.con,
                    mock_pod_digest,
                )

    def test_native_digest_result_binds_input_uid_and_candidate(self) -> None:
        _, pod, _, _, _ = active(self.fix, self.con)
        raw = V3.encoded(pod)
        result = {
            "candidate_commit": self.fix["candidate_commit"],
            "input_sha256": hashlib.sha256(raw).hexdigest(),
            "pod_uid": pod["metadata"]["uid"],
            "pod_template_digest": "sha256:" + "8" * 64,
        }
        output = "KOVA_E2E_POD_TEMPLATE " + json.dumps(result) + "\nPASS\n"
        with (
            patch.object(BENCH, "FIXTURE", self.fix),
            patch.object(
                BENCH,
                "qualify_template_binary",
                return_value=Path(self.fix["pod_template_binary"]),
            ) as qualify,
            patch.object(BENCH, "command", return_value=output) as command,
        ):
            self.assertEqual(
                BENCH.canonical_pod_digest(pod), result["pod_template_digest"]
            )
            self.assertEqual(command.call_args.kwargs["input_text"].encode(), raw)
            self.assertEqual(
                command.call_args.kwargs["environment"],
                {"KOVA_E2E_POD_TEMPLATE_STDIN": "1"},
            )
            self.assertEqual(qualify.call_count, 2)
            for field, value in (
                ("candidate_commit", "b" * 40),
                ("input_sha256", "0" * 64),
                ("pod_uid", uid(999)),
                ("pod_template_digest", None),
            ):
                command.return_value = (
                    "KOVA_E2E_POD_TEMPLATE "
                    + json.dumps(dict(result, **{field: value}))
                    + "\nPASS\n"
                )
                with self.subTest(field=field), self.assertRaises(BENCH.BenchError):
                    BENCH.canonical_pod_digest(pod)

    def test_schema2_empty_and_active_charge_are_not_legacy_queued_count(self) -> None:
        build, _, entry, intent, _ = active(self.fix, self.con)
        states = {
            "active": {
                "version": 2,
                "fence": 1,
                "maxJobs": 1,
                "maxPerRequester": 1,
                "workerSlots": 1,
                "active": {build["metadata"]["uid"]: entry},
            },
            "queue": {
                "version": 2,
                "globalLimit": 100,
                "requesterLimit": 100,
                "intents": {build["metadata"]["name"]: intent},
            },
        }
        for role, state in states.items():
            obj = ledger(self.fix, self.con, role, state)
            V3.qualify_ledger(obj, role, self.fix, self.con)
            for target in ("schema", "unknown", "uid", "boolean"):
                bad = copy.deepcopy(obj)
                key = next(iter(bad["data"]))
                if target == "uid":
                    bad["metadata"]["uid"] = uid(999)
                else:
                    parsed = json.loads(bad["data"][key])
                    if target == "schema":
                        parsed["version"] = 1
                    elif target == "unknown":
                        parsed["unknown"] = 1
                    else:
                        parsed["version"] = True
                    bad["data"][key] = json.dumps(parsed)
                with (
                    self.subTest(role=role, target=target),
                    self.assertRaises(V3.QualificationError),
                ):
                    V3.qualify_ledger(bad, role, self.fix, self.con)

    def test_histogram_keeps_zero_buckets_and_reports_upper_bounds(self) -> None:
        family = "kova_service_kube_limiter_wait_seconds"
        before = [
            {
                "pod": "one",
                "series": [
                    family + '_bucket{le="0.01"} 0',
                    family + '_bucket{le="0.1"} 0',
                    family + '_bucket{le="+Inf"} 0',
                    family + "_count 0",
                    family + "_sum 0",
                ],
            }
        ]
        after = [
            {
                "pod": "one",
                "series": [
                    family + '_bucket{le="0.01"} 0',
                    family + '_bucket{le="0.1"} 10',
                    family + '_bucket{le="+Inf"} 10',
                    family + "_count 10",
                    family + "_sum 0.6",
                ],
            }
        ]
        result = V3.histogram_summary(
            V3.histogram_deltas(before, after, family), family
        )
        self.assertEqual(result["waits_over_10ms"], 10)
        self.assertEqual(result["p99_upper_bound_seconds"], 0.1)
        bad = copy.deepcopy(after)
        bad[0]["series"] = bad[0]["series"][1:]
        with self.assertRaises(V3.QualificationError):
            V3.histogram_deltas(before, bad, family)
        with self.assertRaises(V3.QualificationError):
            V3.histogram_deltas(after, before, family)


class DirectedFlowTest(unittest.TestCase):
    def setUp(self) -> None:
        self.fix = fixture()
        self.con = contract(self.fix)

    def test_bad_ack_stops_before_preflight_commands_or_lock(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory).resolve() / "fixture.json"
            path.write_bytes(V3.encoded(self.fix))
            path.chmod(0o600)
            environment = {
                "DEEP_QUEUE_E2E_FIXTURE": str(path),
                "DEEP_QUEUE_E2E_MODE": "run",
                "DEEP_QUEUE_E2E_ACK": "wrong",
            }
            globals_to_restore = {
                key: getattr(BENCH, key)
                for key in (
                    "FIXTURE",
                    "FIXTURE_SHA",
                    "CLUSTER",
                    "NAMESPACE",
                    "CONTROL_NAMESPACE",
                    "RECEIPT_NAMESPACE",
                    "RELEASE",
                    "KUBECONFIG",
                    "STAGES",
                    "PORT",
                    "API_PROXY_PORT",
                    "RUN_DEADLINE_SECONDS",
                )
            }
            with (
                patch.dict(os.environ, environment, clear=True),
                patch.multiple(BENCH, **globals_to_restore),
                patch.object(BENCH, "preflight") as preflight,
                patch.object(BENCH, "command") as command,
                patch.object(BENCH, "request_json") as request,
                patch.object(BENCH.os, "open") as open_lock,
            ):
                with self.assertRaises(BENCH.BenchError):
                    BENCH.main()
                for mocked in (preflight, command, request, open_lock):
                    mocked.assert_not_called()

    def test_plan_from_fixture_is_explicit_1_active_99_queued(self) -> None:
        with (
            patch.object(BENCH, "configure_fixture", return_value=self.fix),
            patch.object(BENCH, "FIXTURE", self.fix),
            patch.object(BENCH, "STAGES", (100,)),
            patch.object(BENCH, "RUN_DEADLINE_SECONDS", 900),
            patch.dict(
                os.environ,
                {
                    "DEEP_QUEUE_E2E_MODE": "plan",
                    "DEEP_QUEUE_E2E_FIXTURE": "/private/no-open-needed",
                },
                clear=True,
            ),
            patch.object(BENCH, "command") as command,
            patch.object(BENCH, "request_json") as request,
            patch("sys.stdout", new_callable=io.StringIO) as output,
        ):
            BENCH.main()
            plan = json.loads(output.getvalue())
            self.assertEqual(plan["genuinely_queued"], [99])
            self.assertEqual(plan["api_writes"], 0)
            command.assert_not_called()
            request.assert_not_called()

    def test_wait_state_includes_blocker_charge_and_rejects_missing_original_charge(
        self,
    ) -> None:
        blocker, _, _, blocker_intent, _ = active(self.fix, self.con)
        queued_build, queue_intent, _ = queued(self.fix, self.con, "queued-key", 105)
        by_name = {item["metadata"]["name"]: item for item in (blocker, queued_build)}
        intents = {
            blocker["metadata"]["name"]: blocker_intent,
            queued_build["metadata"]["name"]: queue_intent,
        }
        with (
            tempfile.TemporaryDirectory() as directory,
            patch.object(BENCH, "FIXTURE", self.fix),
            patch.object(
                BENCH, "kjson", return_value={"items": list(by_name.values())}
            ),
            patch.object(BENCH, "check_runner"),
            patch.object(BENCH, "queue_ledgers", return_value=({}, intents)),
            patch.object(BENCH, "qualify_build_receipts") as receipts,
        ):
            got = BENCH.wait_queue_state(
                {queued_build["metadata"]["name"]: "queued-key"},
                blocker["metadata"]["name"],
                blocker["metadata"]["uid"],
                Path(directory),
                time.monotonic() + 2,
            )
            self.assertEqual(len(got["items"]), 2)
            receipts.assert_called_once()
        # A legacy interpretation dropping the active request's charge may
        # never reach PASS: even though every waiting CR already says Queued.
        with (
            tempfile.TemporaryDirectory() as directory,
            patch.object(BENCH, "FIXTURE", self.fix),
            patch.object(
                BENCH, "kjson", return_value={"items": list(by_name.values())}
            ),
            patch.object(BENCH, "check_runner"),
            patch.object(
                BENCH,
                "queue_ledgers",
                return_value=({}, {queued_build["metadata"]["name"]: queue_intent}),
            ),
            patch.object(BENCH.time, "sleep"),
            patch.object(BENCH, "qualify_build_receipts") as receipts,
        ):
            with self.assertRaises(BENCH.BenchError):
                BENCH.wait_queue_state(
                    {queued_build["metadata"]["name"]: "queued-key"},
                    blocker["metadata"]["name"],
                    blocker["metadata"]["uid"],
                    Path(directory),
                    time.monotonic() + 0.01,
                )
            receipts.assert_not_called()

    def test_full_rejection_release_and_fresh_readmission_single_attempt(self) -> None:
        blocker, _, _, _, _ = active(self.fix, self.con)
        records = [
            queued(self.fix, self.con, f"queued-{index}", index + 300)
            for index in range(99)
        ]
        builds = {b["metadata"]["name"]: b for b, _, _ in records}
        expected = {
            name: build["spec"]["idempotencyKey"] for name, build in builds.items()
        }
        originals = {name: build["metadata"]["uid"] for name, build in builds.items()}
        intents = {b["metadata"]["name"]: i for b, i, _ in records}
        victim = next(reversed(expected))
        receipt = records[-1][2]
        posts = []
        reads = []

        def wait_state(current: dict, *_args: object, **_kwargs: object) -> dict:
            result = [blocker]
            for name, key in current.items():
                result.append(
                    builds.get(name) or queued(self.fix, self.con, key, 800)[0]
                )
            return {"items": result}

        def request(_port: int, method: str, path: str, **kwargs: object) -> tuple:
            if method == "POST":
                posts.append(copy.deepcopy(kwargs["payload"]))
                if len(posts) == 1:
                    return 429, {"retry-after": "5"}, 0.1, {"queue_capacity_full": True}
                return (
                    202,
                    {
                        "x-kova-build-id": BENCH.build_id(
                            kwargs["payload"]["idempotency_key"]
                        )
                    },
                    0.1,
                    {},
                )
            reads.append(path)
            return 404, {}, 0.1, {}

        with (
            tempfile.TemporaryDirectory() as directory,
            patch.object(BENCH, "FIXTURE", self.fix),
            patch.object(BENCH, "CONTRACT", self.con),
            patch.object(BENCH, "wait_queue_state", side_effect=wait_state),
            patch.object(
                BENCH,
                "queue_ledgers",
                side_effect=[
                    ({}, intents),
                    ({}, {k: v for k, v in intents.items() if k != victim}),
                ],
            ),
            patch.object(BENCH, "kjson", return_value=receipt),
            patch.object(BENCH, "qualify_epoch"),
            patch.object(BENCH, "request_json", side_effect=request),
            patch.object(
                BENCH, "delete_build_with_uid", return_value={"accepted": True}
            ) as delete,
        ):
            result = BENCH.full_capacity_recovery(
                {"platform": "linux/amd64", "kubeconfig_sha256": "b" * 64},
                Path(directory),
                expected,
                originals,
                blocker["metadata"]["name"],
                blocker["metadata"]["uid"],
                "test-only-token",
                time.monotonic() + 5,
            )
            self.assertEqual(
                (result["active"], result["queued"], result["charged_total"]),
                (1, 99, 100),
            )
            self.assertEqual(len(posts), 2)
            self.assertNotEqual(
                posts[0]["idempotency_key"], posts[1]["idempotency_key"]
            )
            self.assertNotIn(victim, expected)
            delete.assert_called_once_with(
                victim, builds[victim]["metadata"]["uid"], "b" * 64
            )
            self.assertTrue(any("/configmaps/" in path for path in reads))

    def test_unknown_full_post_does_not_release_or_retry(self) -> None:
        expected = {BENCH.build_id(f"queued-{i}"): f"queued-{i}" for i in range(99)}
        with (
            tempfile.TemporaryDirectory() as directory,
            patch.object(BENCH, "FIXTURE", self.fix),
            patch.object(BENCH, "wait_queue_state", return_value={"items": []}),
            patch.object(BENCH, "qualify_epoch"),
            patch.object(
                BENCH,
                "request_json",
                return_value=(429, {}, 0.1, {"queue_capacity_full": False}),
            ) as request,
            patch.object(BENCH, "delete_build_with_uid") as delete,
        ):
            with self.assertRaises(BENCH.BenchError):
                BENCH.full_capacity_recovery(
                    {"platform": "linux/amd64"},
                    Path(directory),
                    expected,
                    {},
                    "blocker",
                    uid(101),
                    "test-only-token",
                    time.monotonic() + 5,
                )
            request.assert_called_once()
            delete.assert_not_called()

    def test_natural_leader_change_is_zero_delete_not_adopted(self) -> None:
        pre = {
            "leader_lease_uid": uid(700),
            "initial_leader": "original",
            "leader_holder": "original_old",
        }
        lease = {
            "metadata": {"uid": uid(700)},
            "spec": {"holderIdentity": "different_new"},
        }
        with (
            tempfile.TemporaryDirectory() as directory,
            patch.object(BENCH, "qualify_epoch"),
            patch.object(BENCH, "wait_queue_state"),
            patch.object(
                BENCH, "leader_observation", return_value=(lease, "different")
            ),
            patch.object(BENCH, "request_json") as request,
            patch.object(BENCH, "kjson") as kjson,
        ):
            with self.assertRaises(BENCH.BenchError):
                BENCH.leader_handoff(
                    pre,
                    Path(directory),
                    {},
                    {},
                    "blocker",
                    uid(101),
                    time.monotonic() + 5,
                )
            request.assert_not_called()
            kjson.assert_not_called()

    def test_original_leader_uid_delete_then_distinct_lease_holder(self) -> None:
        original = "kova-service-one"
        survivor = "kova-service-two"
        replacement = "kova-service-new"
        spec = {"replicas": 2, "template": {"fixed": True}}
        pre = {
            "leader_lease_uid": uid(700),
            "initial_leader": original,
            "leader_holder": original + "_old",
            "service_pods": {original: uid(11), survivor: uid(12)},
            "service_deployment": {
                "uid": uid(7),
                "spec_except_replicas_sha256": BENCH.object_sha256(
                    {"template": {"fixed": True}}
                ),
            },
        }
        old_lease = {
            "metadata": {"uid": uid(700)},
            "spec": {"holderIdentity": original + "_old"},
        }
        new_lease = {
            "metadata": {"uid": uid(700)},
            "spec": {"holderIdentity": survivor + "_new"},
        }
        old_pod = {
            "metadata": metadata(original, self.fix["control_namespace"], uid(11))
        }
        pods = [
            {
                "metadata": metadata(name, self.fix["control_namespace"], identity),
                "status": {
                    "containerStatuses": [
                        {"name": "kova-service", "imageID": "sha256:" + "c" * 64}
                    ]
                },
            }
            for name, identity in ((survivor, uid(12)), (replacement, uid(701)))
        ]
        deployment = {"metadata": {"uid": uid(7)}, "spec": spec}
        with (
            tempfile.TemporaryDirectory() as directory,
            patch.object(BENCH, "CONTROL_NAMESPACE", self.fix["control_namespace"]),
            patch.object(BENCH, "qualify_epoch"),
            patch.object(BENCH, "wait_queue_state") as wait,
            patch.object(BENCH, "qualify_service_owner") as owner,
            patch.object(BENCH, "kjson", side_effect=[old_pod, deployment]),
            patch.object(BENCH, "service_pods", return_value=pods),
            patch.object(
                BENCH,
                "leader_observation",
                side_effect=[
                    (old_lease, original),
                    (old_lease, original),
                    (new_lease, survivor),
                ],
            ),
            patch.object(
                BENCH,
                "request_json",
                side_effect=[(200, {}, 0.1, {}), (404, {}, 0.1, {})],
            ) as request,
        ):
            result = BENCH.leader_handoff(
                pre,
                Path(directory),
                {"queued": "key"},
                {"queued": uid(400)},
                "blocker",
                uid(101),
                time.monotonic() + 5,
            )
            self.assertEqual(result["deleted_uid"], uid(11))
            self.assertEqual(result["new_holder"], survivor + "_new")
            self.assertEqual(
                request.call_args_list[0].kwargs["payload"]["preconditions"],
                {"uid": uid(11)},
            )
            self.assertEqual(
                [call.args[1] for call in request.call_args_list], ["DELETE", "GET"]
            )
            self.assertEqual(wait.call_count, 2)
            self.assertEqual(owner.call_count, 3)
            self.assertEqual(
                pre["service_pods"], {survivor: uid(12), replacement: uid(701)}
            )
            self.assertTrue(
                pre["leader_handoff_effect"]["original_leader_direct_absent"]
            )
            self.assertEqual(
                pre["leader_handoff_effect"]["other_original_uids_last_verified"],
                {survivor: uid(12)},
            )

    def test_unknown_leader_delete_keeps_attempt_uid_without_absence_claim(
        self,
    ) -> None:
        original = "kova-service-one"
        pre = {
            "leader_lease_uid": uid(700),
            "initial_leader": original,
            "leader_holder": original + "_old",
            "service_pods": {original: uid(11), "kova-service-two": uid(12)},
            "service_deployment": {
                "uid": uid(7),
                "spec_except_replicas_sha256": BENCH.object_sha256(
                    {"template": {"fixed": True}}
                ),
            },
        }
        lease = {
            "metadata": {"uid": uid(700)},
            "spec": {"holderIdentity": original + "_old"},
        }
        pod = {"metadata": metadata(original, self.fix["control_namespace"], uid(11))}
        deployment = {
            "metadata": {"uid": uid(7)},
            "spec": {"replicas": 2, "template": {"fixed": True}},
        }
        with (
            tempfile.TemporaryDirectory() as directory,
            patch.object(BENCH, "CONTROL_NAMESPACE", self.fix["control_namespace"]),
            patch.object(BENCH, "qualify_epoch"),
            patch.object(BENCH, "wait_queue_state"),
            patch.object(BENCH, "qualify_service_owner"),
            patch.object(BENCH, "kjson", side_effect=[pod, deployment]),
            patch.object(BENCH, "leader_observation", return_value=(lease, original)),
            patch.object(
                BENCH,
                "request_json",
                side_effect=BENCH.BenchError("private transport detail"),
            ) as request,
        ):
            with self.assertRaises(BENCH.BenchError):
                BENCH.leader_handoff(
                    pre,
                    Path(directory),
                    {},
                    {},
                    "blocker",
                    uid(101),
                    time.monotonic() + 5,
                )
            request.assert_called_once()
            effect = pre["leader_handoff_effect"]
            self.assertEqual(effect["qualified_original_leader_uid"], uid(11))
            self.assertTrue(effect["delete_wire_attempted"])
            self.assertIsNone(effect["http_ack_code"])
            self.assertFalse(effect["original_leader_direct_absent"])
            self.assertIsNone(effect["other_original_uids_last_verified"])

    def test_safe_error_does_not_echo_payload(self) -> None:
        self.assertEqual(
            BENCH.safe_error(BENCH.BenchError("test-only-token"), "test-only-token"),
            "BenchError",
        )


if __name__ == "__main__":
    unittest.main()

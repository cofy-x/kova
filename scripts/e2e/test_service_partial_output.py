"""Cluster-free safety checks for the real-runner #41 Kind acceptance script."""

from __future__ import annotations

import importlib.util
import json
import tempfile
import threading
import unittest
import zipfile
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import Mock, patch

SCRIPT = Path(__file__).with_name("e2e-service-partial-output.py")
SPEC = importlib.util.spec_from_file_location("e2e_service_partial_output", SCRIPT)
assert SPEC and SPEC.loader
acceptance = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(acceptance)


def failed_build() -> dict:
    first = "kind-registry:5000/kova-examples/partial-41-20260927t000000z-deadbeef-a:dev"
    second = first.replace("-a:dev", "-z:dev")
    started = datetime(2026, 9, 27, tzinfo=timezone.utc)  # noqa: UP017 (Python 3.10)
    results = [
        {
            "image": first,
            "format": "oci",
            "platform": "linux/amd64",
            "state": "succeeded",
            "pushedDigest": "sha256:" + "a" * 64,
        },
        {
            "image": first + "_nydus_v3",
            "format": "nydus",
            "platform": "linux/amd64",
            "state": "succeeded",
            "pushedDigest": "sha256:" + "b" * 64,
        },
        {"image": second, "format": "oci", "platform": "linux/amd64", "state": "failed"},
        {
            "image": second + "_nydus_v3",
            "format": "nydus",
            "platform": "linux/amd64",
            "state": "failed",
        },
    ]
    outputs = [
        {
            "image": item["image"],
            "format": item["format"],
            "platform": item["platform"],
            "manifestDigest": item["pushedDigest"],
        }
        for item in results[:2]
    ]
    return {
        "status": {
            "phase": "Failed",
            "reason": "BuildFailed",
            "finishedAt": started.isoformat(),
            "verificationStartedAt": started.isoformat(),
            "verificationDeadlineAt": (started + timedelta(minutes=5)).isoformat(),
            "verificationAttempts": 1,
            "verificationResults": results,
            "outputs": outputs,
        }
    }


def public_receipts(build: dict) -> tuple[dict, dict]:
    build["metadata"] = {"name": "idem-" + "c" * 20}
    build["spec"] = {
        "source": {
            "uri": "oci://kind-registry:5000/kova-sources/example@sha256:" + "d" * 64,
            "digest": "sha256:" + "e" * 64,
        },
        "idempotencyKey": "partial-41-test",
    }
    job = {
        "id": build["metadata"]["name"],
        "status": "failed",
        "failure_code": "build_failed",
        "source_uri": build["spec"]["source"]["uri"],
        "source_digest": build["spec"]["source"]["digest"],
        "idempotency_key": build["spec"]["idempotencyKey"],
    }
    results = {
        "id": job["id"],
        "source_uri": job["source_uri"],
        "source_digest": job["source_digest"],
        "idempotency_key": job["idempotency_key"],
        "outputs": [
            {
                "format": item["format"],
                "image": item["image"],
                "platform": item["platform"],
                "manifest_digest": item["manifestDigest"],
                "immutable_ref": item["image"].rsplit(":", 1)[0] + "@" + item["manifestDigest"],
            }
            for item in build["status"]["outputs"]
        ],
    }
    return job, results


class PartialOutputSafetyTest(unittest.TestCase):
    def test_archive_is_two_target_with_later_buildkit_fault(self) -> None:
        first = "kind-registry:5000/kova-examples/partial-41-20260927t000000z-deadbeef-a:dev"
        second = first.replace("-a:dev", "-z:dev")
        with tempfile.TemporaryDirectory() as temporary:
            archive = acceptance.make_archive(Path(temporary), first, second)
            with zipfile.ZipFile(archive) as source:
                self.assertEqual(len(source.namelist()), 5)
                self.assertEqual(json.loads(source.read("a-pass/metadata.json"))["target"], first)
                self.assertEqual(json.loads(source.read("z-fail/metadata.json"))["target"], second)
                self.assertIn(b"COPY payload", source.read("a-pass/Dockerfile"))
                self.assertIn(b"COPY missing", source.read("z-fail/Dockerfile"))
                self.assertNotIn("z-fail/missing", source.namelist())

    def test_exact_partial_failed_receipt(self) -> None:
        build = failed_build()
        first = build["status"]["outputs"][0]["image"]
        second = first.replace("-a:dev", "-z:dev")
        self.assertEqual(
            acceptance.verify_failed_receipt(build, first, second),
            {
                first: "sha256:" + "a" * 64,
                first + "_nydus_v3": "sha256:" + "b" * 64,
            },
        )
        build["status"]["phase"] = "Succeeded"
        with self.assertRaises(acceptance.AcceptanceError):
            acceptance.verify_failed_receipt(build, first, second)

    def test_digest_or_deadline_drift_is_rejected(self) -> None:
        build = failed_build()
        first = build["status"]["outputs"][0]["image"]
        second = first.replace("-a:dev", "-z:dev")
        build["status"]["outputs"][0]["manifestDigest"] = "sha256:" + "c" * 64
        with self.assertRaises(acceptance.AcceptanceError):
            acceptance.verify_failed_receipt(build, first, second)
        build = failed_build()
        build["status"]["verificationDeadlineAt"] = "2026-09-27T00:06:00+00:00"
        with self.assertRaises(acceptance.AcceptanceError):
            acceptance.verify_failed_receipt(build, first, second)

    def test_no_later_output_allowed(self) -> None:
        build = failed_build()
        first = build["status"]["outputs"][0]["image"]
        second = first.replace("-a:dev", "-z:dev")
        build["status"]["verificationResults"][2]["state"] = "succeeded"
        build["status"]["verificationResults"][2]["pushedDigest"] = "sha256:" + "c" * 64
        with self.assertRaises(acceptance.AcceptanceError):
            acceptance.verify_failed_receipt(build, first, second)

    def test_copy_missing_export_requires_fixture_source_path_diagnostic(self) -> None:
        build = failed_build()
        second = build["status"]["verificationResults"][2]["image"]
        failed = {
            "target": second.replace(":dev", ":dev_nydus_v3"),
            "success": False,
            "reason": "exit status 1",
            "logs": 'COPY missing /must-not-exist\nfailed to solve: "/missing": not found\n',
        }
        proof = acceptance.verify_copy_missing_export(json.dumps(failed) + "\n", second)
        self.assertEqual(proof["target"], failed["target"])
        self.assertEqual(proof["source_path"], "/missing")
        self.assertEqual(
            acceptance.verify_copy_missing_export(
                json.dumps({**failed, "logs": "BuildKit diagnostic: /missing"}) + "\n", second
            )["source_path"],
            "/missing",
        )
        self.assertIsNone(acceptance.verify_copy_missing_export("", second))

        for drift in (
            {"target": "kind-registry:5000/unrelated:dev_nydus_v3"},
            {"logs": "COPY missing /must-not-exist\nnetwork timeout\n"},
            {"success": True},
            {"manifest_digest": "sha256:" + "f" * 64},
            {"reason": ""},
        ):
            with self.subTest(drift=drift):
                with self.assertRaises(acceptance.AcceptanceError):
                    acceptance.verify_copy_missing_export(
                        json.dumps({**failed, **drift}) + "\n", second
                    )

    def test_copy_failure_capture_is_bound_to_runner_uid(self) -> None:
        build = failed_build()
        second = build["status"]["verificationResults"][2]["image"]
        raw = (
            json.dumps(
                {
                    "target": second.replace(":dev", ":dev_nydus_v3"),
                    "success": False,
                    "reason": "exit status 1",
                    "logs": 'failed to solve: "/missing": not found\n',
                }
            )
            + "\n"
        )
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            with patch.object(acceptance, "kctl", return_value=raw) as exec_command:
                with patch.object(
                    acceptance, "kjson", return_value={"metadata": {"uid": "runner-uid"}}
                ):
                    self.assertTrue(
                        acceptance.capture_copy_failure(
                            directory, "kova-job-idem-" + "a" * 20, "runner-uid", second
                        )
                    )
            query = exec_command.call_args.args[-1]
            self.assertIn("with-fail=true", query)
            self.assertIn("target=kind-registry%3A5000%2F", query)
            self.assertEqual((directory / "runner-failure-export.jsonl").read_text(), raw)
            self.assertEqual(
                json.loads((directory / "copy-missing-proof.json").read_text())["runner_pod_uid"],
                "runner-uid",
            )
            with patch.object(acceptance, "kctl", return_value=raw):
                with patch.object(acceptance, "kjson", return_value={"metadata": {"uid": "other"}}):
                    with self.assertRaises(acceptance.AcceptanceError):
                        acceptance.capture_copy_failure(
                            directory, "kova-job-idem-" + "a" * 20, "runner-uid", second
                        )

    def test_copy_failure_observer_accepts_only_exact_runner_identity(self) -> None:
        runner = "kova-job-idem-" + "a" * 20
        second = "kind-registry:5000/kova-examples/partial-41-test-z:dev"
        pod = {
            "metadata": {
                "uid": "runner-uid",
                "ownerReferences": [{"controller": True, "uid": "build-uid"}],
            }
        }
        with tempfile.TemporaryDirectory() as temporary:
            result: dict = {}
            with patch.object(acceptance, "kctl", return_value=json.dumps(pod)):
                with patch.object(acceptance, "capture_copy_failure", return_value=True) as capture:
                    acceptance.watch_copy_failure(
                        Path(temporary),
                        runner,
                        "runner-uid",
                        "build-uid",
                        second,
                        threading.Event(),
                        result,
                    )
            self.assertEqual(result, {"proven": True, "end": "exact-runner-export"})
            capture.assert_called_once()
            result = {}
            with patch.object(
                acceptance,
                "kctl",
                return_value=json.dumps(
                    {"metadata": {**pod["metadata"], "uid": "replaced-runner"}}
                ),
            ):
                acceptance.watch_copy_failure(
                    Path(temporary),
                    runner,
                    "runner-uid",
                    "build-uid",
                    second,
                    threading.Event(),
                    result,
                )
            self.assertIn("runner identity changed", result["error"])
            result = {}
            with patch.object(acceptance, "kctl", return_value=""):
                acceptance.watch_copy_failure(
                    Path(temporary),
                    runner,
                    "runner-uid",
                    "build-uid",
                    second,
                    threading.Event(),
                    result,
                )
            self.assertEqual(result, {"end": "runner-pod-absent"})

    def test_public_partial_fields_match_durable_receipts(self) -> None:
        build = failed_build()
        job, results = public_receipts(build)
        digests = {item["image"]: item["manifestDigest"] for item in build["status"]["outputs"]}
        acceptance.verify_public_partial(job, results, build, digests)
        for side, field, value in (
            ("job", "id", "idem-" + "f" * 20),
            ("job", "source_digest", "sha256:" + "f" * 64),
            ("job", "idempotency_key", "wrong"),
            ("results", "id", "idem-" + "f" * 20),
            ("results", "source_digest", "sha256:" + "f" * 64),
            ("results", "source_uri", "oci://wrong"),
            ("output", "format", "oci"),
            ("output", "platform", "linux/arm64"),
            ("output", "immutable_ref", "kind-registry:5000/wrong@sha256:" + "a" * 64),
            ("output", "manifest_digest", "sha256:" + "f" * 64),
        ):
            with self.subTest(side=side, field=field):
                observed_job, observed_results = deepcopy(job), deepcopy(results)
                selected = observed_job if side == "job" else observed_results
                if side == "output":
                    selected = observed_results["outputs"][1]
                selected[field] = value
                with self.assertRaises(acceptance.AcceptanceError):
                    acceptance.verify_public_partial(observed_job, observed_results, build, digests)

    def test_absence_requires_404_not_registry_error(self) -> None:
        with patch.object(acceptance, "registry_request", return_value=(503, {})):
            with self.assertRaises(acceptance.AcceptanceError):
                acceptance.manifest_digest("kova-examples/partial-41-test-a", "dev", absent=True)

    def test_revision_and_run_scope_are_exact(self) -> None:
        self.assertEqual(
            acceptance.exact_images("a" * 12)["runner"], "localhost:5002/kova:runner-" + "a" * 12
        )
        with self.assertRaises(acceptance.AcceptanceError):
            acceptance.exact_images("dev")
        with self.assertRaises(acceptance.AcceptanceError):
            acceptance.run_id_from_target("kind-registry:5000/kova-examples/unrelated:dev")

    def test_reflected_token_is_never_persisted(self) -> None:
        previous = acceptance.PRIVATE_TOKEN
        acceptance.PRIVATE_TOKEN = "private-test-token"
        try:
            with tempfile.TemporaryDirectory() as temporary:
                path = Path(temporary) / "receipt.json"
                with self.assertRaises(acceptance.AcceptanceError):
                    acceptance.save_json(path, {"nested": ["prefix private-test-token suffix"]})
                self.assertFalse(path.exists())
                with self.assertRaises(acceptance.AcceptanceError):
                    acceptance.save_text(path, "private-test-token")
                self.assertFalse(path.exists())
        finally:
            acceptance.PRIVATE_TOKEN = previous

    def test_pod_runtime_requires_exact_cri_repo_digest(self) -> None:
        image = "localhost:5002/kova:runner-" + "a" * 12
        digest = "localhost:5002/kova@sha256:" + "b" * 64
        config_id = "sha256:" + "a" * 64
        pod = {
            "metadata": {"name": "kova-job-idem-" + "c" * 20, "uid": "uid"},
            "spec": {
                "nodeName": "kova-partial-output-41-worker",
                "containers": [{"name": "runner", "image": image}],
            },
            "status": {
                "containerStatuses": [{"name": "runner", "image": image, "imageID": digest}]
            },
        }
        inspection = {"status": {"id": config_id, "repoTags": [image], "repoDigests": [digest]}}
        with patch.object(acceptance, "command", return_value=json.dumps(inspection)):
            self.assertEqual(
                acceptance.runtime_image_fact(pod, "runner", image, config_id)["image_id"],
                digest,
            )
            pod["status"]["containerStatuses"][0]["imageID"] = config_id
            self.assertEqual(
                acceptance.runtime_image_fact(pod, "runner", image, config_id)["image_id"],
                config_id,
            )
            pod["status"]["containerStatuses"][0]["imageID"] = "sha256:" + "d" * 64
            with self.assertRaises(acceptance.AcceptanceError):
                acceptance.runtime_image_fact(pod, "runner", image, config_id)
            with self.assertRaises(acceptance.AcceptanceError):
                acceptance.runtime_image_fact(pod, "runner", image, "sha256:" + "c" * 64)

    def test_local_platform_config_must_match_revision(self) -> None:
        image = "localhost:5002/kova:runner-" + "a" * 12
        selected = {
            "Id": "sha256:" + "c" * 64,
            "Os": "linux",
            "Architecture": "amd64",
            "Config": {"Labels": {"org.opencontainers.image.revision": "a" * 12}},
        }
        with patch.object(acceptance, "command", return_value=json.dumps([selected])) as run:
            self.assertEqual(acceptance.local_config_id(image, "a" * 12), selected["Id"])
            self.assertEqual(
                run.call_args.args[0],
                ["docker", "image", "inspect", "--platform", "linux/amd64", image],
            )
            with self.assertRaises(acceptance.AcceptanceError):
                acceptance.local_config_id(image, "b" * 12)

    def test_unknown_objects_are_identity_only_in_snapshot(self) -> None:
        def fake_kjson(*args: str) -> dict:
            joined = " ".join(args)
            if "kovabuilds" in joined:
                return {
                    "items": [
                        {
                            "metadata": {"name": "foreign", "uid": "foreign-uid"},
                            "spec": {"source": "unrelated-sensitive-source"},
                        }
                    ]
                }
            if "pods" in joined:
                return {
                    "items": [
                        {
                            "metadata": {"name": "foreign-pod", "uid": "pod-uid"},
                            "spec": {"secret": "unrelated-sensitive-pod"},
                        }
                    ]
                }
            if "kova-service-admission" in joined:
                return {
                    "data": {
                        "reservations.json": json.dumps(
                            {"active": {"foreign": "unrelated-sensitive-ledger"}}
                        )
                    }
                }
            if "kova-service-queue-admission" in joined:
                return {
                    "data": {
                        "queue.json": json.dumps(
                            {"intents": {"foreign": "unrelated-sensitive-queue"}}
                        )
                    }
                }
            if "nodes" in joined:
                return {"items": [node]}
            raise AssertionError(args)

        node = {
            "metadata": {"name": "node", "uid": "node-uid"},
            "status": {"conditions": [], "allocatable": {"pods": "110"}},
        }
        with tempfile.TemporaryDirectory() as temporary:
            with patch.object(acceptance, "kjson", side_effect=fake_kjson):
                acceptance.safe_snapshot(Path(temporary), "before")
            all_evidence = "".join(path.read_text() for path in Path(temporary).iterdir())
            self.assertIn("foreign-uid", all_evidence)
            self.assertNotIn("unrelated-sensitive", all_evidence)

    def test_exact_delete_sends_uid_precondition(self) -> None:
        class FakeResponse:
            status = 200

            def __enter__(self):
                return self

            def __exit__(self, *_args):
                return False

            def read(self, _size):
                return b"{}"

        uid = "12345678-1234-1234-1234-123456789abc"
        job_id = "idem-" + "a" * 20
        with patch.object(acceptance.HTTP, "open", return_value=FakeResponse()) as opened:
            receipt = acceptance.api_delete_with_uid(18087, job_id, uid)
        request = opened.call_args.args[0]
        self.assertEqual(request.get_method(), "DELETE")
        self.assertEqual(json.loads(request.data)["preconditions"], {"uid": uid})
        self.assertEqual(receipt["uid_precondition"], uid)
        with self.assertRaises(acceptance.AcceptanceError):
            acceptance.api_delete_with_uid(18087, "not-run-owned", uid)

    def test_signal_enters_failure_path_without_repeated_interrupts(self) -> None:
        with patch.object(acceptance.signal, "signal") as register:
            with self.assertRaisesRegex(acceptance.AcceptanceError, "received signal"):
                acceptance.interrupted(acceptance.signal.SIGTERM, None)
        self.assertEqual(register.call_count, 3)
        self.assertTrue(
            all(call.args[1] == acceptance.signal.SIG_IGN for call in register.call_args_list)
        )

    def test_port_forward_stop_failure_cannot_mask_build_cleanup(self) -> None:
        process = Mock()
        process.poll.return_value = None
        process.terminate.side_effect = OSError("simulated process race")
        stream = Mock()
        stream.close.side_effect = OSError("simulated disk error")
        acceptance.stop_process(process, stream)
        process.kill.assert_called_once_with()
        stream.close.assert_called_once_with()


if __name__ == "__main__":
    unittest.main()

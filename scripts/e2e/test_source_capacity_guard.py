"""Synthetic safety gates for the isolated #43 acceptance guard (no Kind needed)."""

from __future__ import annotations

import importlib.util
import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

SCRIPT = Path(__file__).with_name("source-capacity-guard.py")
sys.path.insert(0, str(SCRIPT.parent))
SPEC = importlib.util.spec_from_file_location("source_capacity_guard", SCRIPT)
assert SPEC and SPEC.loader
guard = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(guard)

REV = "a" * 12
DIGEST = "sha256:" + "b" * 64
CONFIG = "sha256:" + "c" * 64
UID = "12345678-1234-1234-1234-123456789abc"
RUN = "source-capacity-20260927t010203z-abcdef12"
JOB = "idem-" + guard.sha(b"kova:e2e\0" + RUN.encode())[:20]
IMAGE = guard.image_for("runner", REV)


def contract() -> dict:
    return {
        "run_id": RUN,
        "expected_job_id": JOB,
        "source_uri": f"oci://kind-registry:5000/kova-sources/source-capacity@{DIGEST}",
        "source_manifest_digest": DIGEST,
        "source_digest": DIGEST,
        "target": f"kind-registry:5000/kova-examples/source-capacity:{RUN}",
    }


def write_run(run_dir: Path) -> None:
    item = contract()
    guard.save(run_dir / "source-contract.json", item)
    guard.save(run_dir / "candidate-images.json", {"kind": {"identity": "same"}})
    guard.save(
        run_dir / "run.json",
        {
            "run_id": RUN,
            "cluster": guard.CLUSTER,
            "target": item["target"],
            "source_repository": f"localhost:5002/kova-sources/source-capacity:{RUN}",
        },
    )
    guard.save(
        run_dir / "source-receipt.json",
        {"uri": f"oci://localhost:5002/kova-sources/source-capacity@{DIGEST}", "digest": DIGEST},
    )
    (run_dir / "expected-job-id.txt").write_text(JOB + "\n", encoding="utf-8")


def build() -> dict:
    item = contract()
    return {
        "metadata": {
            "name": JOB,
            "namespace": "kova",
            "uid": UID,
            "labels": {
                "app.kubernetes.io/name": "kova-build",
                "kova.cofy.dev/requester-id": guard.sha(b"kova:e2e")[:32],
            },
        },
        "spec": {
            "requester": {"username": "kova:e2e"},
            "source": {"uri": item["source_uri"], "digest": item["source_digest"]},
            "idempotencyKey": RUN,
            "targets": [{"target": item["target"], "platform": "linux/amd64"}],
            "build": {
                "format": "oci",
                "concurrency": 1,
                "timeout": 900,
                "vars": ["KOVA_MARKER=capacity"],
                "failFast": False,
            },
        },
    }


def pod() -> dict:
    return {
        "metadata": {
            "name": f"kova-job-{JOB}",
            "namespace": "kova",
            "uid": UID,
            "ownerReferences": [{"kind": "KovaBuild", "name": JOB, "uid": UID, "controller": True}],
        },
        "spec": {
            "nodeName": "kova-source-capacity-worker",
            "containers": [{"name": "runner", "image": IMAGE}],
            "initContainers": [{"name": "source-fetch", "image": IMAGE}],
        },
        "status": {
            "containerStatuses": [{"name": "runner", "image": IMAGE, "imageID": CONFIG}],
            "initContainerStatuses": [{"name": "source-fetch", "image": IMAGE, "imageID": CONFIG}],
        },
    }


class ImageIdentityTests(unittest.TestCase):
    def test_preflight_refuses_service_runner_image_arg_drift(self) -> None:
        with (
            patch.object(guard, "candidate_revision", return_value=REV),
            patch.object(guard, "local_config_id", return_value=CONFIG),
            patch.object(
                guard,
                "check_deployment",
                side_effect=[(1, {"args": ["--runner-image=foreign"]}), (1, {})],
            ),
        ):
            with self.assertRaisesRegex(guard.GuardError, "runner image argument"):
                guard.image_preflight()

    def test_local_config_requires_exact_oci_revision(self) -> None:
        with patch.object(
            guard, "local_platform_image_fact", return_value={"config_digest": CONFIG}
        ) as inspect:
            self.assertEqual(guard.local_config_id(IMAGE, REV), CONFIG)
            self.assertEqual(inspect.call_args.args, (IMAGE, REV, guard.command))
        with patch.object(
            guard, "local_platform_image_fact", side_effect=guard.ImageIdentityError("drift")
        ):
            with self.assertRaisesRegex(guard.GuardError, "drift"):
                guard.local_config_id(IMAGE, REV)

    def test_role_pod_requires_spec_status_and_cri_config_identity(self) -> None:
        cri = {"status": {"id": CONFIG, "repoTags": [IMAGE], "repoDigests": []}}
        with patch.object(guard, "command", return_value=json.dumps(cri)):
            self.assertEqual(
                guard.runtime_image_fact(pod(), "runner", IMAGE, CONFIG)["cri_id"], CONFIG
            )
            self.assertEqual(
                guard.runtime_image_fact(pod(), "source-fetch", IMAGE, CONFIG, init=True)["cri_id"],
                CONFIG,
            )
            cri["status"]["id"] = DIGEST
            with patch.object(guard, "command", return_value=json.dumps(cri)):
                with self.assertRaises(guard.GuardError):
                    guard.runtime_image_fact(pod(), "runner", IMAGE, CONFIG)
        changed = pod()
        changed["status"]["containerStatuses"][0]["imageID"] = DIGEST
        with patch.object(
            guard,
            "command",
            return_value=json.dumps(
                {"status": {"id": CONFIG, "repoTags": [IMAGE], "repoDigests": []}}
            ),
        ):
            with self.assertRaises(guard.GuardError):
                guard.runtime_image_fact(changed, "runner", IMAGE, CONFIG)

    def test_repo_digest_image_id_must_appear_in_cri(self) -> None:
        ref = IMAGE.split(":runner-")[0] + "@" + DIGEST
        selected = pod()
        selected["status"]["containerStatuses"][0]["imageID"] = "docker-pullable://" + ref
        cri = {"status": {"id": CONFIG, "repoTags": [IMAGE], "repoDigests": [ref]}}
        with patch.object(guard, "command", return_value=json.dumps(cri)):
            self.assertEqual(
                guard.runtime_image_fact(selected, "runner", IMAGE, CONFIG)["image_id"], ref
            )
        cri["status"]["repoDigests"] = []
        with patch.object(guard, "command", return_value=json.dumps(cri)):
            with self.assertRaises(guard.GuardError):
                guard.runtime_image_fact(selected, "runner", IMAGE, CONFIG)


class ExactStopTests(unittest.TestCase):
    def test_runner_check_proves_both_init_and_main_image(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            run_dir = Path(directory)
            write_run(run_dir)
            baseline = {
                "kind": {"identity": "same"},
                "images": {"runner": IMAGE},
                "config_ids": {"runner": CONFIG},
            }
            guard.save(run_dir / "candidate-images.json", baseline)
            with (
                patch.object(guard, "image_preflight", return_value=baseline),
                patch.object(guard, "kjson", side_effect=[build(), {"items": [pod()]}]),
                patch.object(guard, "list_owned_state"),
                patch.object(
                    guard, "runtime_image_fact", return_value={"cri_id": CONFIG}
                ) as runtime,
                patch.object(guard.time, "monotonic", side_effect=[0, 1]),
            ):
                result = guard.runner_preflight(run_dir)
            self.assertEqual(result["build_uid"], UID)
            self.assertEqual(runtime.call_count, 2)
            self.assertEqual(runtime.call_args_list[0].args[1], "source-fetch")
            self.assertEqual(runtime.call_args_list[1].args[1], "runner")

    def test_contract_and_cr_require_all_exact_run_fields(self) -> None:
        self.assertEqual(guard.validate_build(build(), contract()), UID)
        for section, key, value in (
            ("metadata", "name", "foreign"),
            ("metadata", "labels", {"app.kubernetes.io/name": "foreign"}),
            ("spec", "idempotencyKey", "other"),
            ("spec", "source", {"uri": "other", "digest": DIGEST}),
            ("spec", "targets", [{"target": "other", "platform": "linux/amd64"}]),
            (
                "spec",
                "build",
                {"format": "oci", "concurrency": 1, "timeout": 900, "vars": ["OTHER=1"]},
            ),
        ):
            changed = build()
            changed[section][key] = value
            with self.subTest(section=section, key=key), self.assertRaises(guard.GuardError):
                guard.validate_build(changed, contract())

    def test_contract_cross_checks_original_run_and_source_receipt(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            run_dir = Path(directory)
            write_run(run_dir)
            self.assertEqual(guard.load_contract(run_dir), contract())
            guard.save(run_dir / "source-receipt.json", {"uri": "other", "digest": DIGEST})
            with self.assertRaises(guard.GuardError):
                guard.load_contract(run_dir)

    def test_runner_must_be_owned_by_exact_uid(self) -> None:
        guard.check_runner_ownership(pod(), contract(), UID)
        changed = pod()
        changed["metadata"]["ownerReferences"][0]["uid"] = "foreign"
        with self.assertRaises(guard.GuardError):
            guard.check_runner_ownership(changed, contract(), UID)

    def test_delete_options_have_atomic_uid_precondition(self) -> None:
        payload = json.loads(guard.delete_options(UID))
        self.assertEqual(payload["preconditions"], {"uid": UID})
        self.assertEqual(payload["kind"], "DeleteOptions")

    def test_uncertain_submit_never_deletes_and_keeps_receipt(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            run_dir = Path(directory)
            write_run(run_dir)
            with (
                patch.object(guard, "kind_facts", return_value={"identity": "same"}),
                patch.object(guard, "kjson", return_value={"items": []}),
                patch.object(guard, "exact_delete") as delete,
                patch.object(guard.time, "monotonic", side_effect=[0, 1, 61]),
                patch.object(guard.time, "sleep"),
            ):
                with self.assertRaisesRegex(guard.GuardError, "uncertain"):
                    guard.exact_stop(run_dir)
                delete.assert_not_called()
            receipt = json.loads((run_dir / "stop-outcome.json").read_text())
            self.assertEqual(receipt["status"], "submit_outcome_uncertain")

    def test_drift_refuses_delete(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            run_dir = Path(directory)
            write_run(run_dir)
            changed = build()
            changed["spec"]["source"]["digest"] = CONFIG
            with (
                patch.object(guard, "kind_facts", return_value={"identity": "same"}),
                patch.object(guard, "kjson", return_value={"items": [changed]}),
                patch.object(guard, "exact_delete") as delete,
                patch.object(guard.time, "monotonic", side_effect=[0, 1]),
            ):
                with self.assertRaises(guard.GuardError):
                    guard.exact_stop(run_dir)
                delete.assert_not_called()

    def test_exact_stop_deletes_only_verified_uid_and_waits_for_empty_ledgers(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            run_dir = Path(directory)
            write_run(run_dir)
            calls = iter([{"items": [build()]}, {"items": []}, {"items": []}])
            with (
                patch.object(guard, "kind_facts", return_value={"identity": "same"}),
                patch.object(guard, "kjson", side_effect=lambda *args: next(calls)),
                patch.object(guard, "list_owned_state"),
                patch.object(guard, "exact_delete") as delete,
                patch.object(guard, "admission_empty", return_value=True),
                patch.object(guard.time, "monotonic", side_effect=[0, 1, 2, 3]),
            ):
                self.assertEqual(guard.exact_stop(run_dir)["status"], "exact_uid_stopped")
                delete.assert_called_once_with(run_dir, JOB, UID)


if __name__ == "__main__":
    unittest.main()

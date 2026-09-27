"""Pure-local checks for bounded #43 evidence and fixture safety gates."""

from __future__ import annotations

import importlib.util
import json
import os
import signal
import subprocess
import sys
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import patch

DIRECTORY = Path(__file__).parent
CAPTURE_SCRIPT = DIRECTORY / "capture-bounded-logs.py"
SPEC = importlib.util.spec_from_file_location("capture_bounded_logs", CAPTURE_SCRIPT)
assert SPEC and SPEC.loader
capture = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(capture)
EVIDENCE_SPEC = importlib.util.spec_from_file_location(
    "source_capacity_evidence", DIRECTORY / "source-capacity-evidence.py"
)
assert EVIDENCE_SPEC and EVIDENCE_SPEC.loader
evidence = importlib.util.module_from_spec(EVIDENCE_SPEC)
EVIDENCE_SPEC.loader.exec_module(evidence)


class EvidenceTests(unittest.TestCase):
    def write_valid_run(self, directory: Path) -> None:
        job_id = "idem-" + "a" * 20
        digest = "sha256:" + "b" * 64
        source = "sha256:" + "c" * 64
        target = "kind-registry:5000/kova-examples/source-capacity:test-run"

        def write_json(name: str, value: dict) -> None:
            (directory / name).write_text(json.dumps(value) + "\n")

        write_json("run.json", {"cluster": "kova-source-capacity"})
        (directory / "expected-job-id.txt").write_text(job_id + "\n")
        write_json("source-contract.json", {"source_digest": source, "target": target})
        write_json(
            "results.json",
            {
                "source_digest": source,
                "outputs": [
                    {
                        "image": target,
                        "format": "oci",
                        "platform": "linux/amd64",
                        "manifest_digest": digest,
                        "immutable_ref": "kind-registry:5000/kova-examples/source-capacity@"
                        + digest,
                    }
                ],
            },
        )
        write_json("runner-image-identity.json", {"runner_uid": "owned-uid"})
        names = ("kova-source-capacity-control-plane", "kova-source-capacity-worker")
        nodes = [
            {
                "name": name,
                "conditions": [
                    {"type": "Ready", "status": "True"},
                    *[{"type": kind, "status": "False"} for kind in evidence.PRESSURE],
                ],
            }
            for name in names
        ]
        (directory / "node-health.jsonl").write_text(
            json.dumps({"at": "2026-09-27T12:00:00Z", "nodes": nodes}) + "\n"
        )
        docker = [
            {
                "at": "2026-09-27T12:00:00Z",
                "name": name,
                "cpu_percent": "1.00%",
                "memory_usage": "1GiB / 8GiB",
                "memory_percent": "12.50%",
                "pids": "10",
                "block_io": "1MB / 1MB",
            }
            for name in names
        ]
        (directory / "node-docker-stats.jsonl").write_text(
            "".join(json.dumps(item) + "\n" for item in docker)
        )
        (directory / "runner-pod-samples.jsonl").write_text(
            json.dumps(
                {
                    "at": "2026-09-27T12:00:00Z",
                    "name": "kova-job-" + job_id,
                    "uid": "owned-uid",
                    "node": names[1],
                    "phase": "Running",
                    "reason": None,
                    "initContainerStatuses": [
                        {
                            "name": "source-fetch",
                            "restartCount": 0,
                            "state": {"terminated": {"exitCode": 0}},
                        }
                    ],
                    "containerStatuses": [
                        {"name": "runner", "restartCount": 0, "state": {"running": {}}}
                    ],
                }
            )
            + "\n"
        )
        (directory / "runner-logs.txt").write_text("runner completed\n")
        write_json(
            "runner-logs.capture.json",
            {
                "capture_complete": True,
                "command_exit_code": 0,
                "forwarded_signal": None,
                "stdout": {"total_bytes": 17, "retained_bytes": 17},
            },
        )

    def test_complete_owned_monitoring_and_exact_result_pass(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            self.write_valid_run(directory)
            self.assertEqual(evidence.validate_monitoring(directory)["runner_samples"], 1)
            self.assertEqual(evidence.validate_result(directory), "sha256:" + "b" * 64)

    def test_missing_or_unhealthy_samples_fail_closed(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            self.write_valid_run(directory)
            (directory / "node-docker-stats.jsonl").write_text("")
            with self.assertRaises(evidence.EvidenceError):
                evidence.validate_monitoring(directory)
            self.write_valid_run(directory)
            node_path = directory / "node-health.jsonl"
            node = json.loads(node_path.read_text())
            node["nodes"][0]["conditions"][1]["status"] = "True"
            node_path.write_text(json.dumps(node) + "\n")
            with self.assertRaises(evidence.EvidenceError):
                evidence.validate_monitoring(directory)
            self.write_valid_run(directory)
            runner_path = directory / "runner-pod-samples.jsonl"
            runner = json.loads(runner_path.read_text())
            runner["containerStatuses"][0]["restartCount"] = 1
            runner_path.write_text(json.dumps(runner) + "\n")
            with self.assertRaises(evidence.EvidenceError):
                evidence.validate_monitoring(directory)

    def test_missing_runner_logs_and_wrong_immutable_repository_fail(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            self.write_valid_run(directory)
            (directory / "runner-logs.capture.json").unlink()
            with self.assertRaises(evidence.EvidenceError):
                evidence.validate_monitoring(directory)
            self.write_valid_run(directory)
            results_path = directory / "results.json"
            results = json.loads(results_path.read_text())
            results["outputs"][0]["immutable_ref"] = (
                "kind-registry:5000/other/repository@" + "sha256:" + "b" * 64
            )
            results_path.write_text(json.dumps(results) + "\n")
            with self.assertRaises(evidence.EvidenceError):
                evidence.validate_result(directory)

    def test_missing_runner_sample_and_incomplete_log_capture_fail(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            self.write_valid_run(directory)
            (directory / "runner-pod-samples.jsonl").write_text(
                json.dumps({"at": "2026-09-27T12:00:00Z", "missing": True}) + "\n"
            )
            with self.assertRaises(evidence.EvidenceError):
                evidence.validate_monitoring(directory)
            self.write_valid_run(directory)
            receipt_path = directory / "runner-logs.capture.json"
            receipt = json.loads(receipt_path.read_text())
            receipt["capture_complete"] = False
            receipt_path.write_text(json.dumps(receipt) + "\n")
            with self.assertRaises(evidence.EvidenceError):
                evidence.validate_monitoring(directory)

    def test_sampler_error_receipt_blocks_acceptance(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            self.write_valid_run(directory)
            (directory / "sampler-errors.txt").write_text("node collection failed\n")
            with self.assertRaises(evidence.EvidenceError):
                evidence.validate_monitoring(directory)


class BoundedLogTests(unittest.TestCase):
    def test_head_tail_and_child_failure_are_explicit(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            stdout, stderr, receipt = (
                directory / "stdout.log",
                directory / "stderr.log",
                directory / "capture.json",
            )
            program = (
                "import sys; "
                f"sys.stdout.write('a'*{capture.STDOUT_LIMIT}+'END-DIAGNOSTIC'); "
                f"sys.stderr.write('b'*{capture.STDERR_LIMIT}+'FAIL-DIAGNOSTIC'); "
                "sys.exit(7)"
            )
            code = capture.run_capture([sys.executable, "-c", program], stdout, stderr, receipt)
            self.assertEqual(code, 7)
            facts = json.loads(receipt.read_text())
            self.assertEqual(facts["command_exit_code"], 7)
            self.assertTrue(facts["capture_complete"])
            self.assertTrue(facts["stdout"]["truncated"])
            self.assertTrue(facts["stderr"]["truncated"])
            self.assertGreater(facts["stdout"]["omitted_bytes"], 0)
            self.assertLessEqual(stdout.stat().st_size, capture.STDOUT_LIMIT)
            self.assertLessEqual(stderr.stat().st_size, capture.STDERR_LIMIT)
            self.assertIn(b"END-DIAGNOSTIC", stdout.read_bytes())
            self.assertIn(b"FAIL-DIAGNOSTIC", stderr.read_bytes())
            self.assertIn(capture.MARKER, stdout.read_bytes())

    def test_private_capture_token_is_redacted_and_not_passed_to_child(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            stdout, stderr, receipt = (
                directory / "stdout.log",
                directory / "stderr.log",
                directory / "capture.json",
            )
            token = "private-test-token"
            program = (
                "import os,sys; "
                "sys.stdout.write('private-test-token\\n'+"
                "str(os.environ.get('KOVA_E2E_REDACT_TOKEN')))"
            )
            with patch.dict(os.environ, {"KOVA_E2E_REDACT_TOKEN": token}):
                code = capture.run_capture([sys.executable, "-c", program], stdout, stderr, receipt)
            self.assertEqual(code, 0)
            self.assertNotIn(token.encode(), stdout.read_bytes())
            self.assertIn(b"None", stdout.read_bytes())
            self.assertTrue(json.loads(receipt.read_text())["stdout"]["test_token_redacted"])
            self.assertNotIn(token, receipt.read_text())

    def test_term_forwards_to_child_and_writes_interrupted_receipt(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            started = directory / "child-started"
            stdout, stderr, receipt = (
                directory / "stdout.log",
                directory / "stderr.log",
                directory / "capture.json",
            )
            process = subprocess.Popen(
                [
                    sys.executable,
                    str(CAPTURE_SCRIPT),
                    "--stdout",
                    str(stdout),
                    "--stderr",
                    str(stderr),
                    "--receipt",
                    str(receipt),
                    "--",
                    sys.executable,
                    "-c",
                    "from pathlib import Path; import time; "
                    f"Path({str(started)!r}).write_text('started'); time.sleep(30)",
                ]
            )
            try:
                deadline = time.monotonic() + 5
                while not started.exists() and time.monotonic() < deadline:
                    time.sleep(0.05)
                self.assertTrue(started.exists(), "capture child did not start")
                if process.poll() is None:
                    process.send_signal(signal.SIGTERM)
                self.assertEqual(process.wait(timeout=12), 128 + signal.SIGTERM)
                self.assertEqual(
                    json.loads(receipt.read_text())["forwarded_signal"], signal.SIGTERM
                )
            finally:
                if process.poll() is None:
                    process.kill()
                    process.wait(timeout=5)


class ShellSafetyTests(unittest.TestCase):
    def test_run_ids_use_utc_seconds_not_epoch_seconds(self) -> None:
        for script in DIRECTORY.glob("e2e-*"):
            if script.suffix not in {".sh", ".py"}:
                continue
            source = script.read_text()
            self.assertNotIn("%Y%m%dt%H%M%sz", source, script.name)
        for script in ("e2e-source-capacity.sh", "e2e-source-pressure-rejection.sh"):
            self.assertIn("%Y%m%dt%H%M%Sz", (DIRECTORY / script).read_text())

    def test_live_token_is_read_from_exact_disposable_secret(self) -> None:
        source = (DIRECTORY / "e2e-source-capacity.sh").read_text()
        self.assertIn("get secret kova-e2e-token -o json", source)
        self.assertIn('"name":"kova-e2e-token","key":"token"', source)
        self.assertIn("[[ ${observed_token} == service-e2e-token ]]", source)
        self.assertIn("[[ ! ${SERVICE_AUTH_TOKEN+x} && ! ${KOVA_SERVICE_TOKEN+x} ]]", source)

    def test_sampler_failure_signals_controller_for_exact_stop(self) -> None:
        source = (DIRECTORY / "e2e-source-capacity.sh").read_text()
        self.assertIn('kill -TERM "${controller_pid}"', source)
        self.assertIn('sampler_fail "node became unready or pressured during the build"', source)
        self.assertIn('sampler_fail "Docker stats collection failed"', source)
        self.assertIn('sampler_fail "runner failed or restarted during the build"', source)

    def test_source_pressure_hup_fails_and_retains_evidence(self) -> None:
        source = (DIRECTORY / "e2e-source-pressure-rejection.sh").read_text()
        self.assertIn("trap 'exit 129' HUP", source)
        self.assertIn("trap finish EXIT", source)
        self.assertIn('kill "${active_pid}"', source)
        self.assertIn("exec timeout --signal=TERM --kill-after=5s", source)
        for script in ("e2e-source-capacity.sh", "e2e-source-pressure-rejection.sh"):
            result = subprocess.run(["bash", "-n", str(DIRECTORY / script)], check=False)
            self.assertEqual(result.returncode, 0)


if __name__ == "__main__":
    unittest.main()

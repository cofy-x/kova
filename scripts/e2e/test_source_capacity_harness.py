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

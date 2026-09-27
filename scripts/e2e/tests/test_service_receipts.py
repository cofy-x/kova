"""Pure-local Service E2E receipt contract; no Kind, registry, or credentials."""

from __future__ import annotations

import json
import os
import stat
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


SCRIPT = Path(__file__).resolve().parents[1] / "service-receipts.py"
RUN_ID = "service-e2e-20260927t012345z-deadbeef"
SOURCE_DIGEST = "sha256:" + "a" * 64
MANIFEST_DIGEST = "sha256:" + "c" * 64
SOURCE_URI = "oci://registry.local/source@" + MANIFEST_DIGEST
TARGET = "registry.local/demo:run-012345"
SECRET = "private-bearer-for-test"


class ServiceReceiptTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.env = {**os.environ, "KOVA_SERVICE_E2E_RECEIPT_SECRET": SECRET}

    def invoke(self, *args: str, body: dict | None = None) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            [sys.executable, str(SCRIPT), *args],
            input=json.dumps(body) if body is not None else "",
            text=True,
            capture_output=True,
            env=self.env,
            check=False,
        )

    def init(self, base: str = ".work/result-service.jsonl", run_id: str = RUN_ID) -> subprocess.CompletedProcess[str]:
        return self.invoke(
            "init", "--root", str(self.root), "--base", base, "--run-id", run_id,
            "--revision", "a" * 40, "--cluster", "dedicated-kind", "--namespace", "kova",
            "--runner-namespace", "kova",
        )

    def event(
        self,
        path: Path,
        event: str,
        stage: str,
        *,
        body: dict | None = None,
        job_id: str = "",
        target: str = TARGET,
    ) -> subprocess.CompletedProcess[str]:
        return self.invoke(
            "event", "--root", str(self.root), "--path", str(path), "--run-id", RUN_ID,
            "--event", event, "--stage", stage, "--target", target,
            "--key", RUN_ID + "-" + stage, "--source-uri", SOURCE_URI,
            "--source-digest", SOURCE_DIGEST, "--job-id", job_id,
            body=body,
        )

    def job(self, stage: str, *, status: str, job_id: str) -> dict:
        return {
            "id": job_id,
            "status": status,
            "source_uri": SOURCE_URI,
            "source_digest": SOURCE_DIGEST,
            "idempotency_key": RUN_ID + "-" + stage,
            "requester": "kova:e2e",
            "failure_code": "invalid_targets" if status == "failed" else "",
            "error": "untrusted " + SECRET,
            "verification_last_error": "untrusted " + SECRET,
        }

    def test_two_jobs_have_scoped_private_allowlisted_receipts(self) -> None:
        created = self.init()
        self.assertEqual(created.returncode, 0, created.stderr)
        path = Path(created.stdout.strip())
        self.assertEqual(path.name, "result-service-" + RUN_ID + ".jsonl")
        self.assertEqual(stat.S_IMODE(path.stat().st_mode), 0o600)
        self.assertFalse((self.root / ".work/result-service.jsonl").exists())

        negative_target = TARGET + "-expected-failure"
        for stage, target, job_id, status in (
            ("negative", negative_target, "job-negative", "failed"),
            ("positive", TARGET, "job-positive", "succeeded"),
        ):
            attempted = self.event(path, "submit_attempt", stage, target=target)
            self.assertEqual(attempted.returncode, 0, attempted.stderr)
            submitted = self.event(
                path, "submitted", stage, body=self.job(stage, status="queued", job_id=job_id),
                target=target,
            )
            self.assertEqual(submitted.returncode, 0, submitted.stderr)
            self.assertEqual(submitted.stdout.strip(), job_id)
            terminal = self.event(
                path, "terminal", stage, body=self.job(stage, status=status, job_id=job_id),
                job_id=job_id, target=target,
            )
            self.assertEqual(terminal.returncode, 0, terminal.stderr)

        output_digest = "sha256:" + "b" * 64
        results = {
            "id": "job-positive", "source_uri": SOURCE_URI, "source_digest": SOURCE_DIGEST,
            "idempotency_key": RUN_ID + "-positive", "untrusted": SECRET,
            "outputs": [
                {
                    "format": fmt, "image": image, "platform": "linux/amd64",
                    "manifest_digest": output_digest,
                    "immutable_ref": image.rsplit(":", 1)[0] + "@" + output_digest,
                    "extra": SECRET,
                }
                for fmt, image in (("oci", TARGET), ("nydus", TARGET + "_nydus_v3"))
            ],
        }
        recorded = self.event(path, "results", "positive", body=results, job_id="job-positive")
        self.assertEqual(recorded.returncode, 0, recorded.stderr)
        verified = self.event(path, "verified", "positive", job_id="job-positive")
        self.assertEqual(verified.returncode, 0, verified.stderr)
        raw = path.read_text()
        self.assertNotIn(SECRET, raw)
        self.assertNotIn("untrusted", raw)
        records = [json.loads(line) for line in raw.splitlines()]
        self.assertEqual(
            [(record["event"], record.get("stage")) for record in records],
            [
                ("run_started", None),
                ("submit_attempt", "negative"), ("submitted", "negative"), ("terminal", "negative"),
                ("submit_attempt", "positive"), ("submitted", "positive"), ("terminal", "positive"),
                ("results", "positive"), ("verified", "positive"),
            ],
        )
        self.assertEqual(records[3]["failure_code"], "invalid_targets")
        self.assertEqual([item["format"] for item in records[-2]["outputs"]], ["oci", "nydus"])

    def test_ambiguous_submit_is_preserved_without_raw_response(self) -> None:
        path = Path(self.init().stdout.strip())
        self.assertEqual(self.event(path, "submit_attempt", "negative").returncode, 0)
        self.assertEqual(self.event(path, "submit_unconfirmed", "negative").returncode, 0)
        records = [json.loads(line) for line in path.read_text().splitlines()]
        self.assertEqual([record["event"] for record in records], ["run_started", "submit_attempt", "submit_unconfirmed"])
        self.assertTrue(all("job_id" not in record for record in records))

    def test_invalid_or_mismatched_api_responses_do_not_append(self) -> None:
        path = Path(self.init().stdout.strip())
        original = path.read_bytes()
        mismatched = self.job("negative", status="queued", job_id="job-negative")
        mismatched["idempotency_key"] = "another-run"
        self.assertNotEqual(self.event(path, "submitted", "negative", body=mismatched).returncode, 0)
        self.assertEqual(path.read_bytes(), original)
        bad_output = {
            "id": "job-positive", "source_uri": SOURCE_URI, "source_digest": SOURCE_DIGEST,
            "idempotency_key": RUN_ID + "-positive",
            "outputs": [{
                "format": "oci", "image": "registry.local/unrelated:dev", "platform": "linux/amd64",
                "manifest_digest": "sha256:" + "b" * 64,
                "immutable_ref": "registry.local/unrelated@sha256:" + "b" * 64,
            }],
        }
        self.assertNotEqual(self.event(path, "results", "positive", body=bad_output, job_id="job-positive").returncode, 0)
        self.assertEqual(path.read_bytes(), original)

    def test_existing_base_scoped_target_and_unsafe_paths_fail_closed(self) -> None:
        work = self.root / ".work"
        work.mkdir()
        outside = self.root / "outside"
        outside.mkdir()
        bad_bases = [str(outside / "receipt.jsonl"), ".work/../outside/receipt.jsonl"]
        for base in bad_bases:
            with self.subTest(base=base):
                self.assertNotEqual(self.init(base).returncode, 0)
        base = work / "result-service.jsonl"
        base.write_text("existing")
        self.assertNotEqual(self.init().returncode, 0)
        base.unlink()
        base.symlink_to(outside / "receipt.jsonl")
        self.assertNotEqual(self.init().returncode, 0)
        base.unlink()
        scoped = work / ("result-service-" + RUN_ID + ".jsonl")
        scoped.write_text("existing")
        self.assertNotEqual(self.init().returncode, 0)
        scoped.unlink()
        scoped.mkdir()
        self.assertNotEqual(self.init().returncode, 0)
        scoped.rmdir()
        base.mkdir()
        self.assertNotEqual(self.init().returncode, 0)
        base.rmdir()
        (work / "linked").symlink_to(outside, target_is_directory=True)
        self.assertNotEqual(self.init(".work/linked/receipt.jsonl").returncode, 0)
        self.assertFalse((outside / "receipt.jsonl").exists())

    def test_append_rejects_replaced_header_or_public_file(self) -> None:
        path = Path(self.init().stdout.strip())
        path.write_text('{"run_id":"other"}\n')
        self.assertNotEqual(self.event(path, "submit_attempt", "negative").returncode, 0)
        path.write_text('{"run_id":"' + RUN_ID + '"}\n')
        path.chmod(0o644)
        self.assertNotEqual(self.event(path, "submit_attempt", "negative").returncode, 0)
        path.unlink()
        outside = self.root / "unrelated.jsonl"
        outside.write_text('{"run_id":"' + RUN_ID + '"}\n')
        path.symlink_to(outside)
        self.assertNotEqual(self.event(path, "submit_attempt", "negative").returncode, 0)
        self.assertEqual(outside.read_text(), '{"run_id":"' + RUN_ID + '"}\n')


if __name__ == "__main__":
    unittest.main()

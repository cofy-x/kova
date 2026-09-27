"""Offline tests for the bounded Service-side OCI oversize fixture."""

from __future__ import annotations

import hashlib
import importlib.util
import json
import tempfile
import threading
import unittest
import zipfile
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

DIRECTORY = Path(__file__).resolve().parent
SPEC = importlib.util.spec_from_file_location(
    "source_oci_oversize", DIRECTORY / "source-oci-oversize.py"
)
assert SPEC and SPEC.loader
source = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(source)
EVIDENCE_SPEC = importlib.util.spec_from_file_location(
    "source_oci_oversize_evidence", DIRECTORY / "source-oci-oversize-evidence.py"
)
assert EVIDENCE_SPEC and EVIDENCE_SPEC.loader
evidence = importlib.util.module_from_spec(EVIDENCE_SPEC)
EVIDENCE_SPEC.loader.exec_module(evidence)


class RegistryFixture(BaseHTTPRequestHandler):
    blobs: dict[str, bytes] = {}
    uploads: dict[str, bytes] = {}
    manifest: bytes | None = None

    def log_message(self, format: str, *args: object) -> None:
        pass

    def respond(self, code: int, *, location: str = "", digest: str = "", size: int = 0) -> None:
        self.send_response(code)
        if location:
            self.send_header("Location", location)
        if digest:
            self.send_header("Docker-Content-Digest", digest)
        self.send_header("Content-Length", str(size))
        self.end_headers()

    def do_HEAD(self) -> None:
        if "/manifests/" in self.path:
            manifest = self.manifest
            self.respond(
                200, digest=source.digest_bytes(manifest), size=len(manifest)
            ) if manifest else self.respond(404)
        elif "/blobs/" in self.path:
            digest = self.path.rsplit("/", 1)[-1]
            blob = self.blobs.get(digest)
            self.respond(200, digest=digest, size=len(blob)) if blob is not None else self.respond(
                404
            )
        else:
            self.respond(404)

    def do_POST(self) -> None:
        if self.path == f"/v2/{source.REPOSITORY}/blobs/uploads/":
            self.respond(202, location=self.path + "fixture")
        else:
            self.respond(404)

    def do_PATCH(self) -> None:
        if self.path == f"/v2/{source.REPOSITORY}/blobs/uploads/fixture":
            self.uploads["fixture"] = self.rfile.read(int(self.headers["Content-Length"]))
            self.respond(202, location=self.path)
        else:
            self.respond(404)

    def do_PUT(self) -> None:
        if "/manifests/" in self.path:
            RegistryFixture.manifest = self.rfile.read(int(self.headers["Content-Length"]))
            self.respond(201, digest=source.digest_bytes(RegistryFixture.manifest))
        elif self.path.startswith(
            f"/v2/{source.REPOSITORY}/blobs/uploads/fixture?digest=sha256%3A"
        ):
            digest = self.path.split("digest=", 1)[1].replace("%3A", ":")
            blob = self.uploads.pop("fixture")
            if source.digest_bytes(blob) != digest:
                self.respond(400)
            else:
                self.blobs[digest] = blob
                self.respond(201, digest=digest)
        else:
            self.respond(404)


class SparseArchiveTests(unittest.TestCase):
    def test_valid_sparse_zip_and_registry_manifest_use_exact_blob_digest(self) -> None:
        target = "kind-registry:5000/kova-examples/source-capacity:fixture"
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "source.zip"
            facts = source.make_sparse_zip(path, 8193, target)
            self.assertEqual(facts["archive_bytes"], 8193)
            self.assertLessEqual(facts["allocated_bytes"], 16 << 20)
            with zipfile.ZipFile(path) as archive:
                self.assertIsNone(archive.testzip())
                self.assertEqual(json.loads(archive.read("image/metadata.json"))["target"], target)
                self.assertEqual(archive.read("image/payload"), bytes(facts["payload_bytes"]))
            digest = source.digest_file(path)
            self.assertEqual(digest, "sha256:" + hashlib.sha256(path.read_bytes()).hexdigest())

            RegistryFixture.blobs = {}
            RegistryFixture.uploads = {}
            RegistryFixture.manifest = None
            server = ThreadingHTTPServer(("127.0.0.1", 0), RegistryFixture)
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            try:
                published = source.Registry(f"127.0.0.1:{server.server_port}").publish(
                    "fixture", path, digest
                )
                self.assertEqual(published["source_digest"], digest)
                manifest = json.loads(RegistryFixture.manifest)
                self.assertEqual(
                    manifest["layers"],
                    [
                        {
                            "mediaType": source.LAYER_MEDIA_TYPE,
                            "digest": digest,
                            "size": 8193,
                        }
                    ],
                )
                self.assertEqual(RegistryFixture.blobs[digest], path.read_bytes())
            finally:
                server.shutdown()
                server.server_close()
                thread.join(timeout=2)

    def test_registry_upload_location_cannot_escape_exact_repository(self) -> None:
        allowed = (
            f"http://kind-registry:5000/v2/{source.REPOSITORY}/blobs/uploads/fixture?_state=abc"
        )
        self.assertEqual(
            source.upload_path(allowed), f"/v2/{source.REPOSITORY}/blobs/uploads/fixture?_state=abc"
        )
        self.assertIn("digest=sha256%3A", source.commit_path(allowed, "sha256:" + "a" * 64))
        for location in (
            "https://example.invalid/v2/kova-sources/source-capacity/blobs/uploads/fixture",
            "http://example.invalid/v2/kova-sources/source-capacity/blobs/uploads/fixture",
            "/v2/other/blobs/uploads/fixture",
            f"/v2/{source.REPOSITORY}/blobs/uploads/../other",
            f"/v2/{source.REPOSITORY}/blobs/uploads/%2e%2e/other",
            f"/v2/{source.REPOSITORY}/blobs/uploads/fixture?digest=sha256:bad",
        ):
            with self.subTest(location=location), self.assertRaises(source.PublicationError):
                source.upload_path(location)


class ShellContractTests(unittest.TestCase):
    def test_live_mode_reuses_exact_kind_and_uid_stop(self) -> None:
        script = (DIRECTORY / "e2e-source-oci-oversize.sh").read_text()
        for phrase in (
            "SOURCE_OCI_OVERSIZE_MODE:-check",
            "SOURCE_CAPACITY_E2E_MODE=check",
            '"${guard}" check "${run_dir}/candidate-images.json"',
            '"${guard}" stop "${run_dir}"',
            "SOURCE_OCI_OVERSIZE_ACK",
            "disposable Kind Secret UID changed during the run",
            '"${run_dir}/runner-pod-image.json"',
            'tag_status "${output_repo}" output-final 404',
            "submission_possible=true",
        ):
            self.assertIn(phrase, script)


class FailureEvidenceTests(unittest.TestCase):
    def test_early_image_identity_can_precede_terminal_init_status(self) -> None:
        self.assertFalse(evidence.check_optional_init_failure({"status": {}}))
        status = {"name": "source-fetch", "restartCount": 0, "state": {"running": {}}}
        pod = {"status": {"initContainerStatuses": [status]}}
        self.assertFalse(evidence.check_optional_init_failure(pod))
        status["state"] = {"terminated": {"exitCode": 20, "reason": "Error"}}
        self.assertTrue(evidence.check_optional_init_failure(pod))
        status["state"]["terminated"]["reason"] = "OOMKilled"
        with self.assertRaises(evidence.EvidenceError):
            evidence.check_optional_init_failure(pod)

    def test_expected_failed_pod_requires_source_fetch_exit_without_oom(self) -> None:
        sample = {
            "name": "kova-job-idem-123",
            "uid": "uid-1",
            "phase": "Failed",
            "reason": "",
            "initContainerStatuses": [
                {
                    "name": "source-fetch",
                    "restartCount": 0,
                    "state": {"terminated": {"exitCode": 20, "reason": "Error"}},
                }
            ],
            "containerStatuses": [],
        }
        self.assertEqual(evidence.check_runner_samples([sample], "idem-123", "uid-1"), 1)
        sample["reason"] = "Evicted"
        with self.assertRaises(evidence.EvidenceError):
            evidence.check_runner_samples([sample], "idem-123", "uid-1")
        sample["reason"] = ""
        sample["initContainerStatuses"][0]["state"]["terminated"]["reason"] = "OOMKilled"
        with self.assertRaises(evidence.EvidenceError):
            evidence.check_runner_samples([sample], "idem-123", "uid-1")

    def test_precise_error_requires_complete_uninterrupted_log_capture(self) -> None:
        logs = "error: " + evidence.ERROR + "\n"
        capture = {
            "capture_complete": True,
            "command_exit_code": 0,
            "forwarded_signal": None,
            "stdout": {"truncated": False, "retained_bytes": len(logs.encode())},
            "stderr": {"truncated": False},
        }
        self.assertEqual(evidence.check_source_log(capture, logs), len(logs.encode()))
        capture["command_exit_code"] = 1
        with self.assertRaises(evidence.EvidenceError):
            evidence.check_source_log(capture, logs)
        capture["command_exit_code"] = 0
        capture["stdout"]["truncated"] = True
        with self.assertRaises(evidence.EvidenceError):
            evidence.check_source_log(capture, logs)
        capture["stdout"]["truncated"] = False
        with self.assertRaises(evidence.EvidenceError):
            evidence.check_source_log(capture, "source fetch failed\n")


if __name__ == "__main__":
    unittest.main()

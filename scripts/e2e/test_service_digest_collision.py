"""Cluster-free safety checks for the disposable real-runner #41 acceptance."""

from __future__ import annotations

import hashlib
import http.client
import importlib.util
import json
import os
import sys
import tempfile
import threading
import unittest
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from unittest.mock import patch

HERE = Path(__file__).parent
sys.path.insert(0, str(HERE))
SPEC = importlib.util.spec_from_file_location(
    "e2e_service_digest_collision", HERE / "e2e-service-digest-collision.py"
)
assert SPEC and SPEC.loader
acceptance = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(acceptance)
RUN = "digest-41-20260927t000000z-deadbeef"
FIRST, SECOND = acceptance.expected_targets(RUN)


def pending_build(*, failed: bool, prefix: str) -> dict:
    started = datetime(2026, 9, 27, tzinfo=timezone.utc)  # noqa: UP017 (Python 3.10)
    results = [
        {
            "image": FIRST,
            "format": "oci",
            "platform": "linux/amd64",
            "state": "pending",
            "pushedDigest": "sha256:" + prefix * 64,
        },
        {
            "image": FIRST.replace(":dev", ":dev_nydus_v3"),
            "format": "nydus",
            "platform": "linux/amd64",
            "state": "pending",
            "pushedDigest": "sha256:" + ("b" if prefix == "a" else "d") * 64,
        },
    ]
    if failed:
        results.extend(
            [
                {"image": SECOND, "format": "oci", "state": "failed"},
                {
                    "image": SECOND.replace(":dev", ":dev_nydus_v3"),
                    "format": "nydus",
                    "state": "failed",
                },
            ]
        )
    return {
        "metadata": {"name": "idem-" + prefix * 20},
        "status": {
            "phase": "FailedVerifying" if failed else "Verifying",
            "reason": "BuildFailed" if failed else "",
            "verificationAttempts": 1,
            "verificationLastError": "registry manifest verification unavailable",
            "verificationDeadlineAt": (started + timedelta(minutes=5)).isoformat(),
            "verificationResults": results,
            "outputs": [],
        },
    }


class ReceiptSafetyTest(unittest.TestCase):
    def test_first_and_second_pending_receipts_are_distinct(self) -> None:
        first = pending_build(failed=True, prefix="a")
        second = pending_build(failed=False, prefix="c")
        pushed_a = acceptance.pending_digests(first, FIRST, SECOND)
        pushed_b = acceptance.pending_digests(second, FIRST, None)
        self.assertEqual(set(pushed_a), set(pushed_b))
        self.assertTrue(all(pushed_a[image] != pushed_b[image] for image in pushed_a))

    def test_pending_receipt_fails_closed_without_own_pushed_digest(self) -> None:
        build = pending_build(failed=True, prefix="a")
        build["status"]["verificationResults"][0]["pushedDigest"] = ""
        with self.assertRaises(acceptance.base.AcceptanceError):
            acceptance.pending_digests(build, FIRST, SECOND)
        build = pending_build(failed=True, prefix="a")
        build["status"]["phase"] = "Failed"
        with self.assertRaises(acceptance.base.AcceptanceError):
            acceptance.pending_digests(build, FIRST, SECOND)

    def test_successful_second_build_keeps_its_own_two_digests(self) -> None:
        build = pending_build(failed=False, prefix="c")
        build["status"].update(
            {"phase": "Succeeded", "finishedAt": "2026-09-27T00:01:00Z", "verificationAttempts": 2}
        )
        for item in build["status"]["verificationResults"]:
            item["state"] = "succeeded"
        build["status"]["outputs"] = [
            {
                "image": item["image"],
                "format": item["format"],
                "platform": item["platform"],
                "manifestDigest": item["pushedDigest"],
            }
            for item in build["status"]["verificationResults"]
        ]
        self.assertEqual(
            acceptance.verify_succeeded_receipt(build, FIRST),
            {
                item["image"]: item["pushedDigest"]
                for item in build["status"]["verificationResults"]
            },
        )
        build["status"]["outputs"][0]["manifestDigest"] = "sha256:" + "e" * 64
        with self.assertRaises(acceptance.base.AcceptanceError):
            acceptance.verify_succeeded_receipt(build, FIRST)

    def test_second_archive_changes_the_same_target_payload(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            archive = acceptance.make_b_archive(Path(temporary), FIRST, RUN)
            import zipfile

            with zipfile.ZipFile(archive) as source:
                self.assertEqual(len(source.namelist()), 3)
                self.assertEqual(json.loads(source.read("a-pass/metadata.json"))["target"], FIRST)
                self.assertIn(b"second overwrite build", source.read("a-pass/payload"))


class RegistryProxyTest(unittest.TestCase):
    def test_only_exact_repository_digest_get_faults_and_writes_pass(self) -> None:
        repository = f"kova-examples/{RUN}-a"
        failed_repository = f"kova-examples/{RUN}-z"
        proxy_path = HERE / "registry-digest-collision-proxy.py"
        with tempfile.TemporaryDirectory() as temporary:
            mode_file = Path(temporary) / "mode"
            mode_file.write_text("503\n")
            manifest = b'{"schemaVersion":2,"config":{"digest":"sha256:example"}}'
            digest = "sha256:" + hashlib.sha256(manifest).hexdigest()

            class Backend(BaseHTTPRequestHandler):
                protocol_version = "HTTP/1.1"

                def reply(self, code: int, body: bytes = b"", **headers: str) -> None:
                    self.send_response(code)
                    for key, value in headers.items():
                        self.send_header(key, value)
                    self.send_header("Content-Length", str(len(body)))
                    self.end_headers()
                    if self.command != "HEAD":
                        self.wfile.write(body)

                def do_POST(self) -> None:  # noqa: N802
                    self.reply(
                        202,
                        Location=f"http://127.0.0.1:{self.server.server_port}/v2/{repository}/blobs/uploads/12345678-1234-1234-1234-123456789abc",
                    )

                def do_PUT(self) -> None:  # noqa: N802
                    length = int(self.headers.get("Content-Length", "0"))
                    body = self.rfile.read(length)
                    if "/blobs/uploads/" in self.path:
                        self.reply(201, **{"Docker-Content-Digest": "sha256:" + "f" * 64})
                        return
                    if body != manifest:
                        self.reply(400)
                        return
                    self.reply(201, **{"Docker-Content-Digest": digest})

                def do_PATCH(self) -> None:  # noqa: N802
                    length = int(self.headers.get("Content-Length", "0"))
                    if self.rfile.read(length) != b"small-layer":
                        self.reply(400)
                        return
                    self.reply(202)

                def do_HEAD(self) -> None:  # noqa: N802
                    self.reply(200, **{"Docker-Content-Digest": digest})

                def do_GET(self) -> None:  # noqa: N802
                    self.reply(200, manifest, **{"Docker-Content-Digest": digest})

                def log_message(self, *_args: object) -> None:
                    pass

            backend = ThreadingHTTPServer(("127.0.0.1", 0), Backend)
            backend_thread = threading.Thread(target=backend.serve_forever, daemon=True)
            backend_thread.start()
            try:
                with patch.dict(
                    os.environ,
                    {
                        "COLLISION_REPOSITORY": repository,
                        "COLLISION_FAILED_REPOSITORY": failed_repository,
                    },
                ):
                    spec = importlib.util.spec_from_file_location(
                        "collision_registry_proxy", proxy_path
                    )
                    assert spec and spec.loader
                    proxy = importlib.util.module_from_spec(spec)
                    spec.loader.exec_module(proxy)
                proxy.BACKEND_HOST = "127.0.0.1"
                proxy.BACKEND_PORT = backend.server_port
                proxy.MODE_FILE = mode_file
                self.assertTrue(proxy.valid_repositories())
                server = ThreadingHTTPServer(("127.0.0.1", 0), proxy.Handler)
                thread = threading.Thread(target=server.serve_forever, daemon=True)
                thread.start()
                try:

                    def request(
                        method: str, path: str, body: bytes = b""
                    ) -> tuple[int, bytes, dict]:
                        conn = http.client.HTTPConnection(
                            "127.0.0.1", server.server_port, timeout=3
                        )
                        try:
                            conn.request(
                                method, path, body=body, headers={"Host": proxy.EXPECTED_HOST}
                            )
                            response = conn.getresponse()
                            return response.status, response.read(), dict(response.getheaders())
                        finally:
                            conn.close()

                    status, _, headers = request("POST", f"/v2/{repository}/blobs/uploads/")
                    self.assertEqual(status, 202)
                    self.assertIn(proxy.EXPECTED_HOST, headers["Location"])
                    upload_path = headers["Location"].split(proxy.EXPECTED_HOST, 1)[1]
                    self.assertEqual(request("PATCH", upload_path, b"small-layer")[0], 202)
                    self.assertEqual(
                        request("PUT", upload_path + "?digest=sha256:" + "f" * 64)[0], 201
                    )
                    self.assertEqual(request("GET", "/v2/")[0], 200)
                    self.assertEqual(
                        request("PUT", f"/v2/{repository}/manifests/dev", manifest)[0], 201
                    )
                    self.assertEqual(request("HEAD", f"/v2/{repository}/manifests/dev")[0], 200)
                    self.assertEqual(request("GET", f"/v2/{repository}/manifests/{digest}")[0], 503)
                    self.assertEqual(request("GET", f"/v2/other/manifests/{digest}")[0], 404)
                    self.assertEqual(
                        request("DELETE", f"/v2/{repository}/manifests/{digest}")[0], 405
                    )
                    mode_file.write_text("only:" + digest + ",sha256:" + "a" * 64 + "\n")
                    self.assertEqual(request("GET", f"/v2/{repository}/manifests/{digest}")[0], 503)
                    self.assertEqual(
                        request("GET", f"/v2/{repository}/manifests/sha256:{'c' * 64}")[0],
                        200,
                    )
                    mode_file.write_text("healthy\n")
                    status, body, headers = request("GET", f"/v2/{repository}/manifests/{digest}")
                    self.assertEqual(
                        (status, body, headers["Docker-Content-Digest"]), (200, manifest, digest)
                    )
                finally:
                    server.shutdown()
                    server.server_close()
                    thread.join(timeout=3)
            finally:
                backend.shutdown()
                backend.server_close()
                backend_thread.join(timeout=3)


if __name__ == "__main__":
    unittest.main()

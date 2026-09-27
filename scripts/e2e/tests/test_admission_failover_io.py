"""Pure-local safety checks for the opt-in admission handoff fixture."""

from __future__ import annotations

import base64
import importlib.util
import io
import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
import subprocess
import sys
import tempfile
import threading
from types import SimpleNamespace
import unittest
from unittest.mock import patch

SCRIPT = Path(__file__).resolve().parents[1] / "admission_failover_io.py"
SHELL = SCRIPT.with_name("e2e-service-admission-failover.sh")
SPEC = importlib.util.spec_from_file_location("admission_failover_io", SCRIPT)
IO = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(IO)
TOKEN = "test-only-secret-78f422e9"
BUILD = "idem-0123456789abcdef0123"
UID = "12345678-1234-1234-1234-123456789abc"


class Handler(BaseHTTPRequestHandler):
    seen = []
    status = 200
    echo_token = False

    def do_POST(self) -> None:
        body = self.rfile.read(int(self.headers["Content-Length"]))
        self.seen.append(("POST", self.path, dict(self.headers), body))
        self.send_response(202)
        self.send_header("X-Kova-Build-ID", TOKEN if self.echo_token else BUILD)
        self.end_headers()
        self.wfile.write(TOKEN.encode() if self.echo_token else b"accepted")

    def do_DELETE(self) -> None:
        body = self.rfile.read(int(self.headers["Content-Length"]))
        self.seen.append(("DELETE", self.path, dict(self.headers), body))
        self.send_response(self.status)
        self.end_headers()
        self.wfile.write(TOKEN.encode())

    def log_message(self, *_args: object) -> None:
        pass


class LocalServer(ThreadingHTTPServer):
    allow_reuse_address = True


class FakeProxy:
    def __init__(self, port: int) -> None:
        self.stdout = io.StringIO(f"Starting to serve on 127.0.0.1:{port}\n")
        self.stopped = False

    def poll(self) -> None:
        return None

    def terminate(self) -> None:
        self.stopped = True

    def wait(self, timeout: int) -> int:
        return 0


class FailoverIOTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.server = LocalServer(("127.0.0.1", 0), Handler)
        cls.thread = threading.Thread(target=cls.server.serve_forever, daemon=True)
        cls.thread.start()

    @classmethod
    def tearDownClass(cls) -> None:
        cls.server.shutdown()
        cls.thread.join(timeout=5)
        cls.server.server_close()

    def setUp(self) -> None:
        Handler.seen = []
        Handler.status = 200
        Handler.echo_token = False
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.receipt = Path(self.temp.name) / "receipt.json"
        self.kubeconfig = Path(self.temp.name) / "kind.kubeconfig"
        self.kubeconfig.write_text("test-only")

    def test_token_is_loaded_from_exact_opaque_secret(self) -> None:
        secret = {
            "metadata": {"name": "kova-e2e-token", "namespace": "kova", "uid": UID},
            "type": "Opaque",
            "data": {"token": base64.b64encode(TOKEN.encode()).decode()},
        }
        with patch.object(IO, "kubectl_json", return_value=secret):
            self.assertEqual(IO.test_token(self.kubeconfig, "kova", UID), TOKEN)
            secret["metadata"]["namespace"] = "other"
            with self.assertRaises(IO.SafetyError):
                IO.test_token(self.kubeconfig, "kova", UID)

    def test_post_never_persists_echoed_token_or_arbitrary_header(self) -> None:
        args = SimpleNamespace(
            expected_id=BUILD,
            kubeconfig=self.kubeconfig,
            namespace="kova",
            port=self.server.server_address[1],
            receipt=self.receipt,
            cluster="kova-admission-pump",
            kubeconfig_sha256="expected",
            secret_uid=UID,
        )
        Handler.echo_token = True
        with (
            patch.object(IO, "test_token", return_value=TOKEN),
            patch.object(IO, "assert_exact_kind"),
            patch.object(sys, "stdin", io.TextIOWrapper(io.BytesIO(b"{}"))),
        ):
            with self.assertRaises(IO.SafetyError):
                IO.post_build(args)
        saved = self.receipt.read_text()
        self.assertNotIn(TOKEN, saved)
        self.assertNotIn(IO.hashlib.sha256(TOKEN.encode()).hexdigest(), saved)
        self.assertFalse(json.loads(saved)["header_matches_expected"])
        self.assertEqual(Handler.seen[0][2]["Authorization"], f"Bearer {TOKEN}")
        self.assertEqual(Handler.seen[0][3], b"{}")

    def test_post_success_saves_only_allowlisted_facts_with_private_mode(self) -> None:
        args = SimpleNamespace(
            expected_id=BUILD,
            kubeconfig=self.kubeconfig,
            namespace="kova",
            port=self.server.server_address[1],
            receipt=self.receipt,
            cluster="kova-admission-pump",
            kubeconfig_sha256="expected",
            secret_uid=UID,
        )
        with (
            patch.object(IO, "test_token", return_value=TOKEN),
            patch.object(IO, "assert_exact_kind"),
            patch.object(sys, "stdin", io.TextIOWrapper(io.BytesIO(b"{}"))),
        ):
            IO.post_build(args)
        receipt = json.loads(self.receipt.read_text())
        self.assertEqual(receipt["outcome"], "accepted")
        self.assertEqual(receipt["expected_id"], BUILD)
        self.assertNotIn(TOKEN, self.receipt.read_text())
        self.assertNotIn(IO.hashlib.sha256(TOKEN.encode()).hexdigest(), self.receipt.read_text())
        self.assertEqual(self.receipt.stat().st_mode & 0o777, 0o600)

    def test_uid_delete_sends_atomic_precondition_and_allowlisted_receipt(self) -> None:
        args = SimpleNamespace(
            kubeconfig=self.kubeconfig,
            kubeconfig_sha256="expected",
            cluster="kova-admission-pump",
            namespace="kova",
            resource="kovabuilds",
            name=BUILD,
            uid=UID,
            lease_name="kova-service.kova.cofy.dev",
            lease_uid=UID,
            lease_holder="kova-service-a_123",
            receipt=self.receipt,
        )
        proxy = FakeProxy(self.server.server_address[1])
        def object_or_lease(_config: Path, _namespace: str, resource: str, _name: str) -> dict:
            if resource == "lease":
                return {"metadata": {"uid": UID}, "spec": {"holderIdentity": args.lease_holder}}
            return {"metadata": {"uid": UID}}
        with (
            patch.object(IO, "assert_exact_kind") as identity,
            patch.object(IO, "kubectl_json", side_effect=object_or_lease),
            patch.object(IO.subprocess, "Popen", return_value=proxy) as start,
            patch.object(IO.select, "select", side_effect=lambda *_: ([proxy.stdout], [], [])),
        ):
            IO.uid_delete(args)
        self.assertTrue(proxy.stopped)
        self.assertEqual(identity.call_count, 2)
        self.assertIn("--address=127.0.0.1", start.call_args.args[0])
        method, path, headers, body = Handler.seen[0]
        self.assertEqual(method, "DELETE")
        self.assertEqual(path, f"/apis/kova.cofy.dev/v1alpha1/namespaces/kova/kovabuilds/{BUILD}")
        self.assertEqual(headers["Content-Type"], "application/json")
        self.assertEqual(json.loads(body)["preconditions"], {"uid": UID})
        self.assertNotIn(TOKEN, self.receipt.read_text())
        self.assertEqual(json.loads(self.receipt.read_text())["outcome"], "accepted")

    def test_uid_conflict_or_changed_identity_refuses_without_guessing(self) -> None:
        args = SimpleNamespace(
            kubeconfig=self.kubeconfig,
            kubeconfig_sha256="expected",
            cluster="kova-admission-pump",
            namespace="kova",
            resource="pods",
            name="kova-service-abcde-12345",
            uid=UID,
            lease_name="kova-service.kova.cofy.dev",
            lease_uid=UID,
            lease_holder="kova-service-a_123",
            receipt=self.receipt,
        )
        with patch.object(IO, "assert_exact_kind"), patch.object(
            IO, "kubectl_json", return_value={"metadata": {"uid": "changed"}}
        ), patch.object(IO.subprocess, "Popen") as start:
            with self.assertRaises(IO.SafetyError):
                IO.uid_delete(args)
            start.assert_not_called()
        wrong_lease_proxy = FakeProxy(self.server.server_address[1])
        def changed_lease(_config: Path, _namespace: str, resource: str, _name: str) -> dict:
            if resource == "lease":
                return {"metadata": {"uid": UID}, "spec": {"holderIdentity": "different"}}
            return {"metadata": {"uid": UID}}
        with (
            patch.object(IO, "assert_exact_kind"),
            patch.object(IO, "kubectl_json", side_effect=changed_lease),
            patch.object(IO.subprocess, "Popen", return_value=wrong_lease_proxy),
            patch.object(IO.select, "select", side_effect=lambda *_: ([wrong_lease_proxy.stdout], [], [])),
        ):
            with self.assertRaises(IO.SafetyError):
                IO.uid_delete(args)
        self.assertEqual(Handler.seen, [])
        Handler.status = 409
        proxy = FakeProxy(self.server.server_address[1])
        def object_or_lease(_config: Path, _namespace: str, resource: str, _name: str) -> dict:
            if resource == "lease":
                return {"metadata": {"uid": UID}, "spec": {"holderIdentity": args.lease_holder}}
            return {"metadata": {"uid": UID}}
        with (
            patch.object(IO, "assert_exact_kind"),
            patch.object(IO, "kubectl_json", side_effect=object_or_lease),
            patch.object(IO.subprocess, "Popen", return_value=proxy),
            patch.object(IO.select, "select", side_effect=lambda *_: ([proxy.stdout], [], [])),
        ):
            with self.assertRaises(IO.SafetyError):
                IO.uid_delete(args)
        self.assertEqual(json.loads(self.receipt.read_text())["http_status"], 409)
        self.assertEqual(json.loads(self.receipt.read_text())["outcome"], "refused")

    def test_kind_identity_rejects_kubeconfig_or_cluster_drift(self) -> None:
        actual = IO.hashlib.sha256(self.kubeconfig.read_bytes()).hexdigest()
        with self.assertRaises(IO.SafetyError):
            IO.assert_exact_kind(self.kubeconfig, "kova-admission-pump", "0" * 64)
        one = subprocess.CompletedProcess([], 0, "kova-admission-pump\n", "")
        other = subprocess.CompletedProcess([], 0, "other\n", "")
        with patch.object(IO.subprocess, "run", side_effect=[one, other]):
            with self.assertRaises(IO.SafetyError):
                IO.assert_exact_kind(self.kubeconfig, "kova-admission-pump", actual)
        two = subprocess.CompletedProcess([], 0, "kova-admission-pump\nother\n", "")
        with patch.object(IO.subprocess, "run", return_value=two):
            with self.assertRaises(IO.SafetyError):
                IO.assert_exact_kind(self.kubeconfig, "kova-admission-pump", actual)

    def test_shell_default_is_check_and_promotion_is_opt_in(self) -> None:
        subprocess.run(["bash", "-n", str(SHELL)], check=True)
        source = SHELL.read_text()
        self.assertIn("mode=${ADMISSION_E2E_MODE:-check}", source)
        self.assertIn("promotion=${ADMISSION_FAILOVER_PROMOTION:-false}", source)
        self.assertIn('uid_delete kovabuilds "${blocker_id}" "${blocker_uid}"', source)
        self.assertIn('uid_delete kovabuilds "${challenger_id}" "${challenger_uid}"', source)
        self.assertNotIn('-H "Authorization: Bearer ${token}"', source)
        self.assertIn("--request-timeout=15s", source)


if __name__ == "__main__":
    unittest.main()

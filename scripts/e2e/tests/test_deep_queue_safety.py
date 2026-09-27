"""Pure-local safety tests for the disposable deep-queue benchmark."""

from __future__ import annotations

import hashlib
import importlib.util
import json
import socketserver
import tempfile
import threading
import unittest
from http.server import BaseHTTPRequestHandler
from pathlib import Path
from unittest.mock import patch

SCRIPT = Path(__file__).resolve().parents[1] / "e2e-service-admission-deep-queue.py"
SPEC = importlib.util.spec_from_file_location("deep_queue_benchmark", SCRIPT)
BENCH = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(BENCH)
TOKEN = "test-only-credential-9f732b1f"
UID = "12345678-1234-1234-1234-123456789abc"


class Handler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:
        self.send_response(200)
        if self.path == "/echo-header":
            self.send_header("X-Echo", TOKEN)
        self.end_headers()
        body = {"echo": TOKEN} if self.path == "/echo-body" else {"ok": True}
        self.wfile.write(json.dumps(body).encode())

    def log_message(self, *_args: object) -> None:
        pass


class LocalServer(socketserver.TCPServer):
    allow_reuse_address = True


def deployment() -> dict:
    return {
        "metadata": {
            "name": "kova-service",
            "namespace": "kova",
            "uid": UID,
            "resourceVersion": "10",
            "labels": {"app.kubernetes.io/instance": "kova"},
        },
        "spec": {
            "replicas": 2,
            "selector": {"matchLabels": {"app.kubernetes.io/component": "service"}},
            "template": {"spec": {"containers": [{"name": "kova-service", "image": "test-image"}]}},
        },
    }


def preflight_fact(value: dict) -> dict:
    return {
        "kubeconfig_fingerprint": "test-fingerprint",
        "node_uids": {"kova-deep-queue-worker": "old-node-uid"},
        "service_deployment": {
            "uid": UID,
            "labels_sha256": BENCH.object_sha256(value["metadata"]["labels"]),
            "spec_except_replicas_sha256": BENCH.object_sha256(
                {key: item for key, item in value["spec"].items() if key != "replicas"}
            ),
            "image": "test-image",
            "image_container_index": 0,
        },
        "service_pods": {"old-service-pod": "old-pod-uid"},
        "service_pod_image_ids": {"old-service-pod": "sha256:" + "a" * 64},
    }


class DeepQueueSafetyTest(unittest.TestCase):
    def test_direct_invocation_rejects_another_host_before_preflight(self) -> None:
        with (
            patch.object(BENCH.sys, "platform", "linux"),
            patch.object(BENCH.socket, "gethostname", return_value="wayne-hk-dev"),
            patch.object(BENCH, "preflight") as preflight,
        ):
            with self.assertRaises(BENCH.BenchError):
                BENCH.main()
            preflight.assert_not_called()

    def test_response_token_echo_is_never_returned(self) -> None:
        with LocalServer(("127.0.0.1", 0), Handler) as server:
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            for path in ("/echo-header", "/echo-body"):
                with self.assertRaises(BENCH.BenchError) as raised:
                    BENCH.request_json(server.server_address[1], "GET", path, TOKEN)
                self.assertNotIn(TOKEN, str(raised.exception))
            status, headers, _, fingerprint = BENCH.request_json(
                server.server_address[1], "GET", "/safe", TOKEN
            )
            self.assertEqual(status, 200)
            self.assertNotIn("echo", headers)
            self.assertIn("body_sha256", fingerprint)
            server.shutdown()

    def test_uid_delete_uses_atomic_precondition_and_fails_on_kubeconfig_drift(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            config = Path(directory) / "kubeconfig"
            config.write_bytes(b"dedicated-test-cluster")
            digest = hashlib.sha256(config.read_bytes()).hexdigest()
            with (
                patch.object(BENCH, "KUBECONFIG", config),
                patch.object(
                    BENCH, "request_json", return_value=(200, {}, 0.1, {"body_length_bytes": 0})
                ) as request,
            ):
                name = "idem-" + "a" * 20
                receipt = BENCH.delete_build_with_uid(name, UID, digest)
                self.assertTrue(receipt["accepted"])
                self.assertEqual(request.call_args.kwargs["payload"]["preconditions"], {"uid": UID})
                self.assertEqual(request.call_args.args[1], "DELETE")
                config.write_bytes(b"another-cluster")
                with self.assertRaises(BENCH.BenchError):
                    BENCH.delete_build_with_uid(name, UID, digest)
                self.assertEqual(request.call_count, 1)

    def test_emergency_stop_survives_pod_and_node_replacement(self) -> None:
        current = deployment()
        expected = preflight_fact(current)
        pod_observation = [
            {
                "name": "new-service-pod",
                "uid": "new-pod-uid",
                "phase": "Running",
                "image_ids": {"kova-service": "sha256:" + "b" * 64},
            }
        ]
        patch_payloads = []

        def fake_kjson(*args: str) -> dict:
            if args == ("get", "nodes", "-o", "json"):
                return {
                    "items": [
                        {"metadata": {"name": "kova-deep-queue-worker", "uid": "new-node-uid"}}
                    ]
                }
            if "deployment" in args:
                return current
            raise AssertionError(args)

        def fake_kctl(*args: str) -> str:
            operations = json.loads(args[-1])
            patch_payloads.append(operations)
            current["spec"]["replicas"] = 0
            current["metadata"]["resourceVersion"] = "11"
            return "patched"

        with (
            tempfile.TemporaryDirectory() as directory,
            patch.object(BENCH, "exact_kind_identity", return_value="test-fingerprint"),
            patch.object(BENCH, "kjson", side_effect=fake_kjson),
            patch.object(BENCH, "kctl", side_effect=fake_kctl),
            patch.object(BENCH, "service_owned_pod_projection", side_effect=[pod_observation, []]),
        ):
            outcome = BENCH.emergency_stop(expected, Path(directory), TOKEN)
        self.assertEqual(outcome["status"], "confirmed")
        self.assertTrue(outcome["original_service_pods_deleted"])
        self.assertTrue(outcome["node_uids_changed"])
        self.assertTrue(outcome["original_service_pods_changed"])
        self.assertEqual(len(patch_payloads), 1)
        self.assertIn({"op": "test", "path": "/metadata/uid", "value": UID}, patch_payloads[0])
        self.assertIn({"op": "replace", "path": "/spec/replicas", "value": 0}, patch_payloads[0])

    def test_emergency_stop_refuses_replaced_deployment(self) -> None:
        current = deployment()
        expected = preflight_fact(current)
        current["metadata"]["uid"] = "different-deployment"
        with (
            tempfile.TemporaryDirectory() as directory,
            patch.object(BENCH, "exact_kind_identity", return_value="test-fingerprint"),
            patch.object(BENCH, "kjson", return_value=current),
            patch.object(BENCH, "service_owned_pod_projection", return_value=[]),
            patch.object(BENCH, "kctl") as patch_command,
        ):
            outcome = BENCH.emergency_stop(expected, Path(directory), TOKEN)
        self.assertEqual(outcome["status"], "unconfirmed")
        patch_command.assert_not_called()

    def test_failure_snapshots_exclude_unknown_specs_and_ledger_values(self) -> None:
        sensitive = "do-not-store-this-secret"

        def fake_kjson(*args: str) -> dict:
            if "kovabuilds" in args:
                return {
                    "items": [
                        {
                            "metadata": {"name": "foreign", "uid": UID},
                            "spec": {"secret": sensitive},
                            "status": {"phase": "Queued"},
                        }
                    ]
                }
            return {
                "metadata": {"name": args[4], "uid": UID, "resourceVersion": "1"},
                "data": {"queue.json": sensitive},
            }

        with (
            tempfile.TemporaryDirectory() as directory,
            patch.object(BENCH, "kjson", side_effect=fake_kjson),
        ):
            outcome = BENCH.capture_failure_objects(Path(directory), TOKEN, set())
            receipts = "\n".join(path.read_text() for path in Path(directory).rglob("*.json"))
        self.assertEqual(outcome["builds"], "projected")
        self.assertNotIn(sensitive, receipts)

    def test_launcher_pid_identity_uses_start_ticks(self) -> None:
        launcher_path = SCRIPT.parent / "deep-queue-launcher.py"
        spec = importlib.util.spec_from_file_location("deep_queue_launcher", launcher_path)
        launcher = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(launcher)
        proc_stat = "123 (python3) " + " ".join(["S", *(["0"] * 18), "456789"])
        with patch.object(Path, "read_text", return_value=proc_stat):
            observed = launcher.process_start(123)
            self.assertEqual(observed, ("S", "456789"))
            self.assertTrue(launcher.process_alive(123, "456789"))
            self.assertFalse(launcher.process_alive(123, "other-start"))


if __name__ == "__main__":
    unittest.main()

from __future__ import annotations

import importlib.util
import contextlib
import copy
import errno
import hashlib
import io
import json
import os
import signal
import socket
import ssl
import subprocess
import tempfile
import unittest
import urllib.error
import zipfile
from pathlib import Path
from unittest import mock

spec = importlib.util.spec_from_file_location(
    "pypi_artifacts", Path(__file__).with_name("verify-pypi-artifacts.py")
)
assert spec is not None and spec.loader is not None
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)


VERSION = "0.1.0rc10"
EXPECTED = {"kova_client-0.1.0rc10.whl": "a" * 64, "kova_client-0.1.0rc10.tar.gz": "b" * 64}


def matching_payload(expected=EXPECTED):
    return {
        "info": {"version": VERSION},
        "urls": [
            {"filename": name, "digests": {"sha256": digest}, "yanked": False}
            for name, digest in expected.items()
        ],
    }


class Clock:
    def __init__(self):
        self.now = 0.0
        self.sleeps = []

    def monotonic(self):
        return self.now

    def sleep(self, seconds):
        self.sleeps.append(seconds)
        self.now += seconds


class PublicArtifactTests(unittest.TestCase):
    def test_preflight_allows_only_matching_subset(self) -> None:
        expected = {"wheel.whl": "a" * 64, "sdist.tar.gz": "b" * 64}
        payload = {
            "info": {"version": "0.1.0rc10"},
            "urls": [{"filename": "wheel.whl", "digests": {"sha256": "a" * 64}, "yanked": False}],
        }
        module.verify_public(payload, "0.1.0rc10", expected, allow_subset=True)
        with self.assertRaises(ValueError):
            module.verify_public(payload, "0.1.0rc10", expected)
        payload["urls"][0]["digests"]["sha256"] = "c" * 64
        with self.assertRaises(ValueError):
            module.verify_public(payload, "0.1.0rc10", expected, allow_subset=True)

    def test_exact_files_required(self) -> None:
        expected = {"kova_client-0.1.0rc10.whl": "a" * 64, "kova_client-0.1.0rc10.tar.gz": "b" * 64}
        payload = {
            "info": {"version": "0.1.0rc10"},
            "urls": [
                {"filename": name, "digests": {"sha256": digest}, "yanked": False}
                for name, digest in expected.items()
            ],
        }
        module.verify_public(payload, "0.1.0rc10", expected)
        for mutation in ("digest", "missing", "extra", "version", "yanked"):
            changed = copy.deepcopy(payload)
            if mutation == "digest":
                changed["urls"][0]["digests"]["sha256"] = "c" * 64
            elif mutation == "missing":
                changed["urls"].pop()
            elif mutation == "extra":
                changed["urls"].append(changed["urls"][0])
            elif mutation == "version":
                changed["info"]["version"] = "0.1.0rc9"
            else:
                changed["urls"][0]["yanked"] = True
            with self.subTest(mutation=mutation), self.assertRaises(ValueError):
                module.verify_public(changed, "0.1.0rc10", expected)

    def test_yank_state_must_be_explicit_boolean_false(self):
        for value in (None, 0, 1, "false", "true", [], {}):
            payload = matching_payload()
            payload["urls"][0]["yanked"] = value
            with self.subTest(value=value), self.assertRaises(ValueError):
                module.verify_public(payload, VERSION, EXPECTED)
        payload = matching_payload()
        del payload["urls"][0]["yanked"]
        with self.assertRaises(ValueError):
            module.verify_public(payload, VERSION, EXPECTED)

    def test_wheel_identity_and_hash_use_same_single_read_snapshot(self):
        def wheel_bytes(version):
            stream = io.BytesIO()
            with zipfile.ZipFile(stream, "w") as archive:
                archive.writestr("kova_client.dist-info/METADATA", f"Name: kova-client\nVersion: {version}\n")
            return stream.getvalue()
        with tempfile.TemporaryDirectory() as directory:
            dist = Path(directory)
            wheel = dist / "kova_client-0.1.0rc10-py3-none-any.whl"
            sdist = dist / "kova_client-0.1.0rc10.tar.gz"
            original = wheel_bytes(VERSION)
            foreign = wheel_bytes("0.1.0rc11")
            wheel.write_bytes(original)
            sdist.write_bytes(b"original sdist")
            read_bytes = Path.read_bytes
            calls = []
            def mutate_after_read(path):
                data = read_bytes(path)
                calls.append(path.name)
                if path == wheel:
                    wheel.write_bytes(foreign)
                return data
            with mock.patch.object(Path, "read_bytes", mutate_after_read):
                version, hashes = module.local_artifacts(dist)
            self.assertEqual(version, VERSION)
            self.assertEqual(calls, [wheel.name, sdist.name])
            self.assertEqual(hashes[wheel.name], hashlib.sha256(original).hexdigest())
            self.assertEqual(hashes[sdist.name], hashlib.sha256(b"original sdist").hexdigest())
            self.assertEqual(wheel.read_bytes(), foreign)


class VisibilityTests(unittest.TestCase):
    def setUp(self):
        self.clock = Clock()
        self.logs = io.StringIO()
        self.stdout = io.StringIO()
        self.stack = contextlib.ExitStack()
        self.addCleanup(self.stack.close)
        self.stack.enter_context(mock.patch.object(module.time, "monotonic", self.clock.monotonic))
        self.stack.enter_context(mock.patch.object(module.time, "sleep", self.clock.sleep))
        self.stack.enter_context(contextlib.redirect_stderr(self.logs))
        self.stack.enter_context(contextlib.redirect_stdout(self.stdout))

    def wait(self, fetch, **kwargs):
        with mock.patch.object(module, "fetch_public", side_effect=fetch) as transport:
            module.wait_for_public(VERSION, EXPECTED, **kwargs)
        return transport

    def test_matching_visibility_beyond_old_55_second_window(self):
        def fetch(version, timeout):
            self.assertEqual(version, VERSION)
            if self.clock.now < 65:
                raise module.FetchFailure("http_404", retryable=True)
            return matching_payload()
        transport = self.wait(fetch)
        self.assertEqual(self.clock.now, 65)
        self.assertEqual(self.clock.sleeps, [5, 10, 20, 30])
        self.assertEqual(transport.call_count, 5)
        self.assertIn("attempt=5 status=verified elapsed=65.0s", self.logs.getvalue())
        self.assertEqual(self.stdout.getvalue(), "")

    def test_permanent_404_expires_exact_deadline_with_attempt_diagnostics(self):
        with self.assertRaisesRegex(module.FetchFailure, "visibility_deadline_expired.*last_status=http_404"):
            self.wait(lambda *_: (_ for _ in ()).throw(module.FetchFailure("http_404", retryable=True)), deadline_seconds=80)
        self.assertEqual(self.clock.now, 80)
        self.assertEqual(self.clock.sleeps, [5, 10, 20, 30, 15])
        self.assertIn("retry_in=15.0s remaining=15.0s", self.logs.getvalue())

    def test_network_recovers_with_same_expected_identity(self):
        expected_before = EXPECTED.copy()
        transport = self.wait([
            module.FetchFailure("network_error", retryable=True),
            module.FetchFailure("request_timeout", retryable=True), matching_payload(),
        ])
        self.assertEqual(transport.call_count, 3)
        self.assertEqual(self.clock.now, 15)
        self.assertEqual(EXPECTED, expected_before)

    def test_permanent_network_failure_does_not_sleep_or_retry(self):
        with mock.patch.object(module, "fetch_public", side_effect=module.FetchFailure("network_error")) as fetch:
            with self.assertRaisesRegex(module.FetchFailure, "network_error"):
                module.wait_for_public(VERSION, EXPECTED)
        self.assertEqual(fetch.call_count, 1)
        self.assertEqual(self.clock.sleeps, [])
        self.assertIn("retry=false", self.logs.getvalue())

    def test_preflight_404_and_matching_subset_retained(self):
        self.wait([module.FetchFailure("http_404", retryable=True)], preflight=True, deadline_seconds=60)
        partial = matching_payload()
        partial["urls"].pop()
        self.wait([partial], preflight=True, deadline_seconds=60)
        self.assertEqual(self.clock.sleeps, [])

    def test_visible_identity_drift_never_retries(self):
        for mutation in ("version", "digest", "missing", "extra", "duplicate", "yanked", "schema"):
            payload = matching_payload()
            if mutation == "version":
                payload["info"]["version"] = "0.1.0rc9"
            elif mutation == "digest":
                payload["urls"][0]["digests"]["sha256"] = "c" * 64
            elif mutation == "missing":
                payload["urls"].pop()
            elif mutation in ("extra", "duplicate"):
                row = copy.deepcopy(payload["urls"][0])
                if mutation == "extra":
                    row["filename"] = "foreign.whl"
                payload["urls"].append(row)
            elif mutation == "schema":
                payload["urls"][0]["filename"] = []
            else:
                payload["urls"][0]["yanked"] = True
            with self.subTest(mutation=mutation), mock.patch.object(module, "fetch_public", return_value=payload) as fetch:
                with self.assertRaises(ValueError):
                    module.wait_for_public(VERSION, EXPECTED)
                self.assertEqual(fetch.call_count, 1)
                self.assertEqual(self.clock.sleeps, [])

    def test_request_time_counts_and_last_request_uses_remaining_budget(self):
        budgets = []
        def fetch(version, timeout):
            budgets.append(timeout)
            self.clock.now += timeout
            raise module.FetchFailure("request_timeout", retryable=True)
        with self.assertRaisesRegex(module.FetchFailure, "visibility_deadline_expired"):
            self.wait(fetch, deadline_seconds=17)
        self.assertEqual(budgets, [10, 2])
        self.assertEqual(self.clock.now, 17)
        self.assertEqual(self.clock.sleeps, [5])

    def test_response_at_or_after_deadline_is_not_success(self):
        def fetch(*_):
            self.clock.now = 10
            return matching_payload()
        with self.assertRaisesRegex(module.FetchFailure, "response_after_deadline"):
            self.wait(fetch, deadline_seconds=10)
        self.assertEqual(self.clock.sleeps, [])

    def test_verification_finishing_after_deadline_is_not_success(self):
        def verify(*args, **kwargs):
            self.clock.now = 10
        with mock.patch.object(module, "verify_public", side_effect=verify):
            with self.assertRaisesRegex(module.FetchFailure, "verification_after_deadline"):
                self.wait([matching_payload()], deadline_seconds=10)
        self.assertNotIn("status=verified", self.logs.getvalue())

    def test_original_artifacts_read_once_and_not_mutated_by_retries(self):
        with tempfile.TemporaryDirectory() as directory:
            dist = Path(directory)
            wheel = dist / "kova_client-0.1.0rc10-py3-none-any.whl"
            with zipfile.ZipFile(wheel, "w") as archive:
                archive.writestr("kova_client-0.1.0rc10.dist-info/METADATA", "Name: kova-client\nVersion: 0.1.0rc10\n")
            (dist / "kova_client-0.1.0rc10.tar.gz").write_bytes(b"original validated sdist")
            original = {path.name: path.read_bytes() for path in dist.iterdir()}
            _, expected = module.local_artifacts(dist)
            with mock.patch.object(module, "local_artifacts", wraps=module.local_artifacts) as local:
                with mock.patch.object(module, "fetch_public", side_effect=[
                    module.FetchFailure("http_404", retryable=True), matching_payload(expected),
                ]):
                    module.main([directory, "--visibility-deadline-seconds", "300"])
                self.assertEqual(local.call_count, 1)
            self.assertEqual({path.name: path.read_bytes() for path in dist.iterdir()}, original)
            self.assertEqual(self.stdout.getvalue(), VERSION + "\n")

    def test_cli_deadline_options_are_bounded_and_post_publication_only(self):
        for args in (["dist", "--visibility-deadline-seconds", value] for value in ("0", "601", "nan", "-1", "1.5")):
            with self.subTest(args=args), self.assertRaises(SystemExit):
                module.main(args)
        with self.assertRaises(SystemExit):
            module.main(["dist", "--preflight", "--visibility-deadline-seconds", "300"])


class FetchTests(unittest.TestCase):
    def test_http_404_retryable_but_auth_and_other_http_permanent(self):
        for status in (404, 401, 403, 429, 500):
            with self.subTest(status=status), mock.patch.object(module.urllib.request, "urlopen", side_effect=urllib.error.HTTPError("ignored", status, "ignored", {}, None)):
                self.assertEqual(module.fetch_worker(VERSION), {"error": f"http_{status}", "retryable": status == 404})

    def test_network_classification_is_explicit_and_no_exception_payload(self):
        for reason, retryable in (
            (TimeoutError("secret"), True), (ConnectionResetError("secret"), True),
            (OSError(errno.ENETUNREACH, "secret"), True),
            (socket.gaierror(socket.EAI_AGAIN, "secret"), True),
            (socket.gaierror(socket.EAI_NONAME, "secret"), False),
            (ssl.SSLCertVerificationError("secret"), False),
            (ValueError("secret"), False), ("secret", False),
        ):
            with self.subTest(reason=type(reason).__name__), mock.patch.object(module.urllib.request, "urlopen", side_effect=urllib.error.URLError(reason)):
                result = module.fetch_worker(VERSION)
                self.assertEqual(result, {"error": "network_error", "retryable": retryable})
                self.assertNotIn("secret", json.dumps(result))

    def test_direct_timeout_is_transient(self):
        with mock.patch.object(module.urllib.request, "urlopen", side_effect=TimeoutError()):
            self.assertTrue(module.fetch_worker(VERSION)["retryable"])

    def test_metadata_size_and_json_errors_permanent(self):
        for body, code in ((b"x" * (module.METADATA_LIMIT + 1), "metadata_size_guard"), (b"invalid", "invalid_metadata")):
            response = mock.MagicMock()
            response.__enter__.return_value.read.return_value = body
            with self.subTest(code=code), mock.patch.object(module.urllib.request, "urlopen", return_value=response):
                self.assertEqual(module.fetch_worker(VERSION), {"error": code, "retryable": False})
                response.__enter__.return_value.read.assert_called_once_with(module.METADATA_LIMIT + 1)

    def test_owned_worker_uses_fixed_version_only_argv_and_remaining_timeout(self):
        result = subprocess.CompletedProcess([], 0, json.dumps({"payload": matching_payload()}).encode(), b"")
        with mock.patch.object(module.subprocess, "run", return_value=result) as run:
            self.assertEqual(module.fetch_public(VERSION, 2), matching_payload())
        argv = run.call_args.args[0]
        self.assertEqual(argv, [module.sys.executable, str(Path(module.__file__).resolve()), "--fetch", VERSION])
        self.assertEqual(run.call_args.kwargs, {"capture_output": True, "timeout": 2, "check": False})

    def test_owned_worker_timeout_is_retryable_and_no_stderr_exposure(self):
        with mock.patch.object(module.subprocess, "run", side_effect=subprocess.TimeoutExpired("ignored", 2, stderr=b"secret")):
            with self.assertRaises(module.FetchFailure) as raised:
                module.fetch_public(VERSION, 2)
        self.assertTrue(raised.exception.retryable)
        self.assertEqual(str(raised.exception), "request_timeout")

    @unittest.skipUnless(hasattr(signal, "SIGSTOP"), "requires POSIX stopped-process fixture")
    def test_real_owned_worker_timeout_kills_and_reaps_without_network(self):
        run = subprocess.run
        exited_pid = []
        def stopped_worker(argv, **kwargs):
            # Stop this owned child after publishing its PID. No timer or network
            # in the fixture: run's deadline must kill and reap the stopped PID.
            fixture = "import os,signal; print(os.getpid(),flush=True); os.kill(os.getpid(),signal.SIGSTOP)"
            try:
                return run([module.sys.executable, "-c", fixture], **kwargs)
            except subprocess.TimeoutExpired as exc:
                exited_pid.append(int(exc.output.strip()))
                raise
        with mock.patch.object(module.subprocess, "run", side_effect=stopped_worker):
            with self.assertRaisesRegex(module.FetchFailure, "request_timeout"):
                module.fetch_public(VERSION, 1)
        self.assertEqual(len(exited_pid), 1)
        with self.assertRaises(ProcessLookupError):
            os.kill(exited_pid[0], 0)

    def test_worker_failure_or_malformed_result_not_retried(self):
        for result in (
            subprocess.CompletedProcess([], 1, b"secret", b"secret"),
            subprocess.CompletedProcess([], 0, b"secret", b"secret"),
            subprocess.CompletedProcess([], 0, b'{"error":"network_error","retryable":"yes"}', b""),
        ):
            with self.subTest(result=result.returncode), mock.patch.object(module.subprocess, "run", return_value=result):
                with self.assertRaises(module.FetchFailure) as raised:
                    module.fetch_public(VERSION, 10)
                self.assertFalse(raised.exception.retryable)
                self.assertNotIn("secret", str(raised.exception))

    def test_worker_retryable_envelope_retained(self):
        result = subprocess.CompletedProcess([], 0, b'{"error":"http_404","retryable":true}', b"")
        with mock.patch.object(module.subprocess, "run", return_value=result):
            with self.assertRaises(module.FetchFailure) as raised:
                module.fetch_public(VERSION, 10)
        self.assertTrue(raised.exception.retryable)
        self.assertEqual(str(raised.exception), "http_404")


if __name__ == "__main__":
    unittest.main()

"""Pure-local identity, denial, review-counter, and cleanup safety checks."""

from __future__ import annotations

import importlib.util
import json
import sys
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

SCRIPT = Path(__file__).resolve().parents[1] / "e2e-service-auth-owner-probe.py"
sys.path.insert(0, str(SCRIPT.parent))
SPEC = importlib.util.spec_from_file_location("auth_owner_probe", SCRIPT)
assert SPEC and SPEC.loader
PROBE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(PROBE)

RUN_ID = "owner-get-20260927t120000z-0123abcd"
SHA = "c" * 40
UID = "11111111-1111-1111-1111-111111111111"


def metric(token: float, sar: float) -> dict:
    return {
        "series": [
            {
                "labels": {
                    "group": "authentication.k8s.io",
                    "resource": "tokenreviews",
                    "code": "201",
                },
                "value": token,
            },
            {
                "labels": {
                    "group": "authorization.k8s.io",
                    "resource": "subjectaccessreviews",
                    "code": "201",
                },
                "value": sar,
            },
        ]
    }


class AuthOwnerProbeTest(unittest.TestCase):
    def test_case_is_run_scoped_and_exact(self) -> None:
        case = PROBE.case_for(RUN_ID, "linux/amd64")
        self.assertEqual(case["requester"], PROBE.fair.principal("a"))
        self.assertEqual(case["id"], PROBE.fair.build_id(case["requester"], case["key"]))
        self.assertIn(RUN_ID, case["target"])
        with self.assertRaisesRegex(PROBE.fair.SafetyError, "run ID"):
            PROBE.case_for("foreign", "linux/amd64")

    def test_owner_get_and_nonowner_denial_never_persist_bearer_or_body(self) -> None:
        case = PROBE.case_for(RUN_ID, "linux/amd64")

        def reply(request, *, timeout):
            self.assertEqual(request.get_method(), "GET")
            self.assertEqual(request.get_header("Authorization"), "Bearer secret-token")
            self.assertEqual(timeout, 5)
            if request.full_url.endswith("/" + case["id"]):
                return (
                    200,
                    {},
                    json.dumps(
                        {
                            "id": case["id"],
                            "requester": case["requester"],
                            "source_digest": case["source_digest"],
                            "idempotency_key": case["key"],
                        }
                    ).encode(),
                )
            raise AssertionError("unexpected URL")

        with patch.object(PROBE.fair.failover_io, "bounded_response", side_effect=reply):
            receipt = PROBE.get_build("a", "secret-token", 18110, case)
        self.assertEqual(receipt["http_status"], 200)
        self.assertNotIn("secret-token", json.dumps(receipt))
        with patch.object(
            PROBE.fair.failover_io,
            "bounded_response",
            return_value=(
                403,
                {},
                b'{"code":"forbidden","message":"access is forbidden","retryable":false}',
            ),
        ):
            denial = PROBE.get_build("b", "secret-token", 18111, case)
        self.assertEqual(denial["http_status"], 403)
        with patch.object(
            PROBE.fair.failover_io,
            "bounded_response",
            return_value=(
                403,
                {},
                json.dumps({"code": "forbidden", "retryable": False, "id": case["id"]}).encode(),
            ),
        ):
            with self.assertRaisesRegex(PROBE.fair.SafetyError, "non-disclosing"):
                PROBE.get_build("b", "secret-token", 18111, case)

    def test_review_deltas_require_zero_owner_sar_and_one_nonowner_sar_each(self) -> None:
        owner = PROBE.review_deltas(metric(10.0, 20.0), metric(40.0, 20.0), 30, 0)
        self.assertEqual(owner["totals"], {"tokenreviews": 30, "subjectaccessreviews": 0})
        denied = PROBE.review_deltas(metric(40.0, 20.0), metric(50.0, 30.0), 10, 10)
        self.assertEqual(denied["totals"], {"tokenreviews": 10, "subjectaccessreviews": 10})
        with self.assertRaisesRegex(PROBE.fair.SafetyError, "noisy"):
            PROBE.review_deltas(metric(10.0, 20.0), metric(40.0, 21.0), 30, 0)
        with self.assertRaisesRegex(PROBE.fair.SafetyError, "reset"):
            PROBE.review_deltas(metric(10.0, 20.0), metric(9.0, 20.0), 30, 0)

    def test_preflight_rejects_used_cursor_or_granted_virtual_get(self) -> None:
        identity = {"candidate_commit": SHA}
        baseline = {"fairness_cursor": ""}
        with (
            patch.object(PROBE.fair, "fixture_identity", return_value=identity),
            patch.object(PROBE.read, "empty_and_healthy", return_value=baseline),
            patch.object(PROBE, "sample_capacity"),
            patch.object(PROBE, "virtual_get_granted", side_effect=[False, False]) as access,
        ):
            self.assertEqual(PROBE.fresh_preflight(SHA), (identity, baseline))
            self.assertEqual(access.call_count, 2)
        baseline["fairness_cursor"] = "already-used"
        with (
            patch.object(PROBE.fair, "fixture_identity", return_value=identity),
            patch.object(PROBE.read, "empty_and_healthy", return_value=baseline),
        ):
            with self.assertRaisesRegex(PROBE.fair.SafetyError, "not fresh"):
                PROBE.fresh_preflight(SHA)
        baseline["fairness_cursor"] = ""
        with (
            patch.object(PROBE.fair, "fixture_identity", return_value=identity),
            patch.object(PROBE.read, "empty_and_healthy", return_value=baseline),
            patch.object(PROBE, "sample_capacity"),
            patch.object(PROBE, "virtual_get_granted", side_effect=[False, True]),
        ):
            with self.assertRaisesRegex(PROBE.fair.SafetyError, "GET permission"):
                PROBE.fresh_preflight(SHA)

    def test_virtual_get_requires_definitive_yes_or_no(self) -> None:
        with patch.object(
            PROBE.subprocess, "run", return_value=SimpleNamespace(returncode=1, stdout="no\n")
        ) as run:
            self.assertFalse(PROBE.virtual_get_granted(PROBE.fair.principal("b")))
            self.assertIn("--as=" + PROBE.fair.principal("b"), run.call_args.args[0])
        with patch.object(
            PROBE.subprocess, "run", return_value=SimpleNamespace(returncode=1, stdout="")
        ):
            with self.assertRaisesRegex(PROBE.fair.SafetyError, "inconclusive"):
                PROBE.virtual_get_granted(PROBE.fair.principal("b"))

    def test_node_cpu_quantity_is_bounded_and_understood(self) -> None:
        self.assertEqual(PROBE.allocatable_cpu_cores("4"), 4.0)
        self.assertEqual(PROBE.allocatable_cpu_cores("750m"), 0.75)
        with self.assertRaisesRegex(PROBE.fair.SafetyError, "invalid"):
            PROBE.allocatable_cpu_cores("0")

    def test_cleanup_refuses_changed_uid_without_deletion(self) -> None:
        state = {"uid": UID, "identity": {"candidate_commit": SHA}}
        view = {"builds": {"a1": {"metadata": {"uid": "different"}}}}
        with (
            patch.object(PROBE, "observe_owned", return_value=(view, {})),
            patch.object(PROBE.fair.failover_io, "uid_delete") as delete,
            patch.object(PROBE, "save"),
        ):
            with self.assertRaisesRegex(PROBE.fair.SafetyError, "UID changed"):
                PROBE.cleanup_owned(state, Path("/tmp/unwritten"), {})
            delete.assert_not_called()


if __name__ == "__main__":
    unittest.main()

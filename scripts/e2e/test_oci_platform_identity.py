"""Pure-local Docker OCI digest-chain checks shared by the #41/#43 guards."""

from __future__ import annotations

import hashlib
import importlib.util
import io
import json
import tarfile
import unittest
from pathlib import Path
from unittest.mock import patch

SCRIPT = Path(__file__).with_name("oci_platform_identity.py")
SPEC = importlib.util.spec_from_file_location("oci_platform_identity_test", SCRIPT)
assert SPEC and SPEC.loader
identity = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(identity)

REFERENCE = "localhost:5002/kova:controller-" + "a" * 12
REVISION = "a" * 12


def archive_fixture(
    *,
    extra_descriptor: bool = False,
    omit_config: bool = False,
    corrupt_config: bool = False,
    wrong_reference: bool = False,
) -> tuple[bytes, str, str]:
    config = json.dumps(
        {
            "architecture": "amd64",
            "os": "linux",
            "config": {"Labels": {"org.opencontainers.image.revision": REVISION}},
        },
        separators=(",", ":"),
    ).encode()
    config_digest = "sha256:" + hashlib.sha256(config).hexdigest()
    manifest = json.dumps(
        {
            "schemaVersion": 2,
            "mediaType": "application/vnd.oci.image.manifest.v1+json",
            "config": {
                "mediaType": "application/vnd.oci.image.config.v1+json",
                "digest": config_digest,
                "size": len(config),
            },
            "layers": [],
        },
        separators=(",", ":"),
    ).encode()
    manifest_digest = "sha256:" + hashlib.sha256(manifest).hexdigest()
    descriptor = {
        "mediaType": "application/vnd.oci.image.manifest.v1+json",
        "digest": manifest_digest,
        "size": len(manifest),
        "annotations": {
            "io.containerd.image.name": "localhost:5002/other:tag" if wrong_reference else REFERENCE
        },
        "platform": {"architecture": "amd64", "os": "linux"},
    }
    index = json.dumps(
        {
            "schemaVersion": 2,
            "mediaType": "application/vnd.oci.image.index.v1+json",
            "manifests": [descriptor, descriptor] if extra_descriptor else [descriptor],
        },
        separators=(",", ":"),
    ).encode()
    stream = io.BytesIO()
    with tarfile.open(fileobj=stream, mode="w") as archive:
        entries = []
        if not omit_config:
            entries.append(
                (
                    "blobs/sha256/" + config_digest[7:],
                    config.replace(b"amd64", b"arm64") if corrupt_config else config,
                )
            )
        entries.extend(
            (
                ("blobs/sha256/" + manifest_digest[7:], manifest),
                # A layer-like blob must not be retained as metadata.
                ("blobs/sha256/" + "f" * 64, b"not-json-layer"),
                ("index.json", index),
            )
        )
        for name, data in entries:
            member = tarfile.TarInfo(name)
            member.size = len(data)
            archive.addfile(member, io.BytesIO(data))
    return stream.getvalue(), manifest_digest, config_digest


class FakeProcess:
    def __init__(self, data: bytes, exit_code: int = 0) -> None:
        self.stdout = io.BytesIO(data)
        self.exit_code = exit_code

    def poll(self) -> int:
        return self.exit_code

    def wait(self, timeout: int) -> int:
        return self.exit_code

    def kill(self) -> None:
        self.exit_code = -9


class OCIPlatformIdentityTests(unittest.TestCase):
    def test_manifest_and_config_are_distinct_and_both_verified(self) -> None:
        raw, manifest, config = archive_fixture()
        self.assertNotEqual(manifest, config)
        fact = identity.parse_saved_image(io.BytesIO(raw), REFERENCE, manifest, REVISION)
        self.assertEqual(fact["platform_manifest_digest"], manifest)
        self.assertEqual(fact["config_digest"], config)
        self.assertRegex(fact["archive_index_sha256"], r"^sha256:[a-f0-9]{64}$")

    def test_local_inspection_manifest_id_resolves_to_config_id(self) -> None:
        raw, manifest, config = archive_fixture()
        inspected = [
            {
                "Id": manifest,
                "Os": "linux",
                "Architecture": "amd64",
                "Config": {"Labels": {"org.opencontainers.image.revision": REVISION}},
            }
        ]
        process = FakeProcess(raw)
        with patch.object(identity.subprocess, "Popen", return_value=process) as started:
            fact = identity.local_platform_image_fact(
                REFERENCE, REVISION, lambda argv: json.dumps(inspected)
            )
        self.assertEqual(fact["config_digest"], config)
        self.assertNotEqual(fact["platform_manifest_digest"], config)
        self.assertEqual(
            started.call_args.args[0],
            [
                "timeout",
                "--kill-after=5s",
                "90s",
                "docker",
                "image",
                "save",
                "--platform",
                "linux/amd64",
                REFERENCE,
            ],
        )

    def test_digest_revision_reference_and_platform_drift_fail_closed(self) -> None:
        for variant in (
            {"extra_descriptor": True},
            {"omit_config": True},
            {"corrupt_config": True},
            {"wrong_reference": True},
        ):
            with self.subTest(variant=variant):
                raw, manifest, _ = archive_fixture(**variant)
                with self.assertRaises(identity.ImageIdentityError):
                    identity.parse_saved_image(io.BytesIO(raw), REFERENCE, manifest, REVISION)
        raw, manifest, config = archive_fixture()
        for wrong_manifest, wrong_revision in ((config, REVISION), (manifest, "b" * 12)):
            with self.subTest(manifest=wrong_manifest, revision=wrong_revision):
                with self.assertRaises(identity.ImageIdentityError):
                    identity.parse_saved_image(
                        io.BytesIO(raw), REFERENCE, wrong_manifest, wrong_revision
                    )

    def test_metadata_limits_fail_closed_before_large_retention(self) -> None:
        raw, manifest, _ = archive_fixture()
        with patch.object(identity, "MAX_METADATA_BLOB_BYTES", 10):
            with self.assertRaises(identity.ImageIdentityError):
                identity.parse_saved_image(io.BytesIO(raw), REFERENCE, manifest, REVISION)
        with patch.object(identity, "MAX_METADATA_TOTAL_BYTES", 10):
            with self.assertRaises(identity.ImageIdentityError):
                identity.parse_saved_image(io.BytesIO(raw), REFERENCE, manifest, REVISION)

    def test_inspection_or_save_failure_is_not_accepted(self) -> None:
        raw, manifest, _ = archive_fixture()
        inspected = [
            {
                "Id": manifest,
                "Os": "linux",
                "Architecture": "amd64",
                "Config": {"Labels": {"org.opencontainers.image.revision": REVISION}},
            }
        ]
        with patch.object(identity.subprocess, "Popen", return_value=FakeProcess(raw, 1)):
            with self.assertRaisesRegex(identity.ImageIdentityError, "returned 1"):
                identity.local_platform_image_fact(
                    REFERENCE, REVISION, lambda _: json.dumps(inspected)
                )
        inspected[0]["Config"]["Labels"]["org.opencontainers.image.revision"] = "old"
        with self.assertRaises(identity.ImageIdentityError):
            identity.local_platform_image_fact(REFERENCE, REVISION, lambda _: json.dumps(inspected))
        inspected[0]["Config"] = None
        with self.assertRaises(identity.ImageIdentityError):
            identity.local_platform_image_fact(REFERENCE, REVISION, lambda _: json.dumps(inspected))


if __name__ == "__main__":
    unittest.main()

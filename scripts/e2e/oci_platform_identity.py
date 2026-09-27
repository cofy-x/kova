"""Prove a local Linux/amd64 Docker image's config digest without loading layers.

With Docker's containerd image store, `docker image inspect --platform` returns
the selected *manifest* digest as Id. Kind's CRI `status.id` is the image
*config* digest. The OCI index -> manifest -> config chain makes those two
different identities comparable without trusting a mutable tag alone.
"""

from __future__ import annotations

import hashlib
import json
import re
import subprocess
import tarfile
from typing import IO, Callable

SHA256 = r"sha256:[0-9a-f]{64}"
MAX_METADATA_BLOB_BYTES = 2 * 1024 * 1024
MAX_METADATA_TOTAL_BYTES = 16 * 1024 * 1024


class ImageIdentityError(RuntimeError):
    pass


def _fail(message: str) -> None:
    raise ImageIdentityError(message)


def parse_saved_image(
    stream: IO[bytes], reference: str, manifest_digest: str, revision: str
) -> dict[str, str]:
    """Verify a platform-filtered Docker OCI export with bounded metadata memory."""
    blobs: dict[str, bytes] = {}
    index_bytes: bytes | None = None
    metadata_bytes = 0
    try:
        with tarfile.open(fileobj=stream, mode="r|*") as archive:
            for member in archive:
                if not member.isfile():
                    continue
                if member.name == "index.json":
                    if index_bytes is not None or member.size > MAX_METADATA_BLOB_BYTES:
                        _fail("Docker OCI archive has duplicate or oversized index metadata")
                    extracted = archive.extractfile(member)
                    if extracted is None:
                        _fail("Docker OCI archive index is unreadable")
                    index_bytes = extracted.read(MAX_METADATA_BLOB_BYTES + 1)
                    if len(index_bytes) != member.size:
                        _fail("Docker OCI archive index is truncated")
                    continue
                match = re.fullmatch(r"blobs/sha256/([0-9a-f]{64})", member.name)
                if match is None or member.size > MAX_METADATA_BLOB_BYTES:
                    continue
                extracted = archive.extractfile(member)
                if extracted is None:
                    _fail("Docker OCI archive blob is unreadable")
                prefix = extracted.read(min(member.size, 64))
                # A small layer can share the blobs/ path. Keep only JSON
                # candidates; metadata remains bounded even for many blobs.
                if prefix.lstrip()[:1] != b"{":
                    continue
                metadata_bytes += member.size
                if metadata_bytes > MAX_METADATA_TOTAL_BYTES:
                    _fail("Docker OCI archive has too much JSON metadata")
                digest = "sha256:" + match.group(1)
                if digest in blobs:
                    _fail("Docker OCI archive has duplicate blob metadata")
                data = prefix + extracted.read(MAX_METADATA_BLOB_BYTES + 1 - len(prefix))
                if len(data) != member.size or "sha256:" + hashlib.sha256(data).hexdigest() != digest:
                    _fail("Docker OCI archive blob is truncated or digest-mismatched")
                blobs[digest] = data
    except (tarfile.TarError, OSError) as error:
        _fail(f"Docker OCI archive could not be read: {type(error).__name__}")

    if index_bytes is None:
        _fail("Docker OCI archive has no index")
    try:
        index = json.loads(index_bytes)
        descriptors = index["manifests"]
        if (
            index.get("schemaVersion") != 2
            or index.get("mediaType") != "application/vnd.oci.image.index.v1+json"
            or not isinstance(descriptors, list)
            or len(descriptors) != 1
        ):
            _fail("Docker OCI archive is not an unambiguous platform-filtered index")
        descriptor = descriptors[0]
        if (
            descriptor.get("mediaType") != "application/vnd.oci.image.manifest.v1+json"
            or descriptor.get("digest") != manifest_digest
            or descriptor.get("platform") != {"architecture": "amd64", "os": "linux"}
            or descriptor.get("annotations", {}).get("io.containerd.image.name") != reference
        ):
            _fail("Docker OCI archive index differs from the inspected image/platform")
        manifest_bytes = blobs[manifest_digest]
        if descriptor.get("size") != len(manifest_bytes):
            _fail("Docker OCI archive manifest size differs from its index descriptor")
        manifest = json.loads(manifest_bytes)
        config_descriptor = manifest["config"]
        config_digest = config_descriptor["digest"]
        if (
            manifest.get("schemaVersion") != 2
            or manifest.get("mediaType") != "application/vnd.oci.image.manifest.v1+json"
            or config_descriptor.get("mediaType") != "application/vnd.oci.image.config.v1+json"
            or not isinstance(config_digest, str)
            or not re.fullmatch(SHA256, config_digest)
        ):
            _fail("Docker OCI archive has malformed manifest/config descriptors")
        config_bytes = blobs[config_digest]
        if config_descriptor.get("size") != len(config_bytes):
            _fail("Docker OCI archive config size differs from its manifest descriptor")
        config = json.loads(config_bytes)
        if (
            config.get("architecture") != "amd64"
            or config.get("os") != "linux"
            or config.get("config", {}).get("Labels", {}).get("org.opencontainers.image.revision")
            != revision
        ):
            _fail("Docker OCI archive config architecture/revision differs from inspected image")
    except (KeyError, TypeError, AttributeError, ValueError) as error:
        _fail(f"Docker OCI archive metadata is malformed: {type(error).__name__}")
    return {
        "archive_index_sha256": "sha256:" + hashlib.sha256(index_bytes).hexdigest(),
        "platform_manifest_digest": manifest_digest,
        "config_digest": config_digest,
    }


def local_platform_image_fact(
    reference: str, revision: str, inspect_command: Callable[[list[str]], str]
) -> dict[str, str]:
    """Bind a reviewed Docker tag and revision to its exact amd64 config digest."""
    try:
        inspected = json.loads(
            inspect_command(["docker", "image", "inspect", "--platform", "linux/amd64", reference])
        )
    except (TypeError, ValueError) as error:
        _fail(f"Docker platform image inspection is malformed: {type(error).__name__}")
    if not isinstance(inspected, list) or len(inspected) != 1:
        _fail(f"local candidate image is unavailable or ambiguous: {reference}")
    selected = inspected[0]
    if not isinstance(selected, dict):
        _fail("Docker platform image inspection is not an object")
    manifest_digest = selected.get("Id", "")
    config = selected.get("Config")
    labels = config.get("Labels") if isinstance(config, dict) else None
    if (
        not isinstance(manifest_digest, str)
        or not re.fullmatch(SHA256, manifest_digest)
        or selected.get("Os") != "linux"
        or selected.get("Architecture") != "amd64"
        or not isinstance(labels, dict)
        or labels.get("org.opencontainers.image.revision") != revision
    ):
        _fail(f"local linux/amd64 candidate manifest/revision differs: {reference}")
    try:
        process = subprocess.Popen(
            [
                "timeout", "--kill-after=5s", "90s", "docker", "image", "save",
                "--platform", "linux/amd64", reference,
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
        )
    except OSError as error:
        _fail(f"Docker OCI image save could not start: {type(error).__name__}")
    try:
        if process.stdout is None:
            _fail("Docker OCI image save has no output stream")
        fact = parse_saved_image(process.stdout, reference, manifest_digest, revision)
    except BaseException:
        if process.poll() is None:
            process.kill()
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            pass
        raise
    finally:
        if process.stdout is not None:
            process.stdout.close()
    try:
        exit_code = process.wait(timeout=5)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait(timeout=5)
        _fail("Docker OCI image save did not exit after streaming")
    if exit_code != 0:
        _fail(f"Docker OCI image save returned {exit_code}")
    return fact

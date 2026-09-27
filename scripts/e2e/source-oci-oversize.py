#!/usr/bin/env python3
"""Publish one exact 512 MiB + 1 byte OCI source to the isolated Kind registry.

This deliberately bypasses the Kova client-side archive limit so the Service
runner's immutable OCI fetch can be tested. The local ZIP is sparse; registry
upload and hashing stream fixed-size chunks. No registry deletion is performed.
"""

from __future__ import annotations

import argparse
import hashlib
import http.client
import json
import os
import re
import struct
import sys
import zlib
from pathlib import Path
from urllib.parse import parse_qs, urlencode, urlsplit, urlunsplit

ARCHIVE_BYTES = (512 << 20) + 1
CHUNK = 1 << 20
REGISTRY = "localhost:5002"
REPOSITORY = "kova-sources/source-capacity"
RUN_PATTERN = re.compile(r"source-capacity-[0-9]{8}t[0-9]{6}z-[0-9a-f]{8}\Z")
SHA_PATTERN = re.compile(r"sha256:[0-9a-f]{64}\Z")
LAYER_MEDIA_TYPE = "application/vnd.cofy.kova.source.v1+zip"
MANIFEST_MEDIA_TYPE = "application/vnd.oci.image.manifest.v1+json"
CONFIG_MEDIA_TYPE = "application/vnd.oci.image.config.v1+json"


class PublicationError(RuntimeError):
    pass


def compact_json(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode("utf-8")


def digest_bytes(data: bytes) -> str:
    return "sha256:" + hashlib.sha256(data).hexdigest()


def digest_file(path: Path) -> str:
    hasher = hashlib.sha256()
    with path.open("rb") as source:
        while chunk := source.read(CHUNK):
            hasher.update(chunk)
    return "sha256:" + hasher.hexdigest()


def zero_crc32(size: int) -> int:
    block = bytes(CHUNK)
    value = 0
    while size:
        amount = min(size, CHUNK)
        value = zlib.crc32(memoryview(block)[:amount], value)
        size -= amount
    return value


def make_sparse_zip(path: Path, total_bytes: int, target: str) -> dict:
    """Write a valid stored ZIP with a sparse zero payload and exact byte size."""
    members = [
        (b"image/Dockerfile", b"FROM scratch\n"),
        (b"image/metadata.json", compact_json({"target": target, "platform": "linux/amd64"})),
        (b"image/payload", None),
    ]
    overhead = 22 + sum(
        30 + 46 + 2 * len(name) + (len(data) if data else 0) for name, data in members
    )
    payload_bytes = total_bytes - overhead
    if payload_bytes <= 0 or payload_bytes > 0xFFFFFFFF or total_bytes > 0xFFFFFFFF:
        raise PublicationError("sparse ZIP total size is outside the bounded ZIP32 fixture range")
    directory = bytearray()
    with path.open("xb") as output:
        for name, data in members:
            offset = output.tell()
            size = payload_bytes if data is None else len(data)
            crc = zero_crc32(size) if data is None else zlib.crc32(data)
            output.write(
                struct.pack(
                    "<IHHHHHIIIHH", 0x04034B50, 20, 0, 0, 0, 0, crc, size, size, len(name), 0
                )
            )
            output.write(name)
            if data is None:
                output.seek(size - 1, os.SEEK_CUR)
                output.write(b"\0")
            else:
                output.write(data)
            directory.extend(
                struct.pack(
                    "<IHHHHHHIIIHHHHHII",
                    0x02014B50,
                    20,
                    20,
                    0,
                    0,
                    0,
                    0,
                    crc,
                    size,
                    size,
                    len(name),
                    0,
                    0,
                    0,
                    0,
                    0,
                    offset,
                )
            )
            directory.extend(name)
        directory_offset = output.tell()
        output.write(directory)
        output.write(
            struct.pack(
                "<IHHHHIIH",
                0x06054B50,
                0,
                0,
                len(members),
                len(members),
                len(directory),
                directory_offset,
                0,
            )
        )
    actual = path.stat()
    if actual.st_size != total_bytes or actual.st_blocks * 512 > 16 << 20:
        raise PublicationError("fixture size or sparse-disk budget differs from the contract")
    return {
        "archive_bytes": actual.st_size,
        "allocated_bytes": actual.st_blocks * 512,
        "payload_bytes": payload_bytes,
    }


def upload_path(location: str) -> str:
    parsed = urlsplit(location)
    prefix = f"/v2/{REPOSITORY}/blobs/uploads/"
    if (
        parsed.scheme not in ("", "http")
        or parsed.username
        or parsed.password
        or parsed.fragment
        or (
            parsed.netloc
            and parsed.netloc not in (REGISTRY, "127.0.0.1:5002", "kind-registry:5000")
        )
    ):
        raise PublicationError("registry returned an unsafe upload URL")
    if (
        not parsed.path.startswith(prefix)
        or parsed.path == prefix
        or ".." in parsed.path
        or "%" in parsed.path
    ):
        raise PublicationError("registry upload URL escaped the exact source repository")
    if "digest" in parse_qs(parsed.query):
        raise PublicationError("registry upload URL already contains a digest")
    return urlunsplit(("", "", parsed.path, parsed.query, ""))


def commit_path(location: str, digest: str) -> str:
    path = upload_path(location)
    parsed = urlsplit(path)
    query = parsed.query + ("&" if parsed.query else "") + urlencode({"digest": digest})
    return urlunsplit(("", "", parsed.path, query, ""))


class Registry:
    def __init__(self, host: str = REGISTRY):
        self.host = host

    def request(
        self,
        method: str,
        path: str,
        body: bytes | Path | None = None,
        content_type: str | None = None,
    ) -> tuple[int, dict[str, str]]:
        connection = http.client.HTTPConnection(self.host, timeout=30)
        try:
            connection.putrequest(method, path)
            if method == "HEAD" and "/manifests/" in path:
                # Distribution registries may return 404 for an OCI manifest
                # unless its media type is explicitly accepted.
                connection.putheader("Accept", MANIFEST_MEDIA_TYPE)
            if content_type:
                connection.putheader("Content-Type", content_type)
            size = body.stat().st_size if isinstance(body, Path) else len(body) if body else 0
            connection.putheader("Content-Length", str(size))
            connection.endheaders()
            if isinstance(body, Path):
                with body.open("rb") as stream:
                    while chunk := stream.read(CHUNK):
                        connection.send(chunk)
            elif body:
                connection.send(body)
            response = connection.getresponse()
            if len(response.read(65537)) > 65536:
                raise PublicationError("registry response exceeds 64 KiB")
            return response.status, {key.lower(): value for key, value in response.getheaders()}
        finally:
            connection.close()

    def upload_blob(self, source: bytes | Path, digest: str) -> None:
        if not SHA_PATTERN.fullmatch(digest):
            raise PublicationError("invalid OCI blob digest")
        base = f"/v2/{REPOSITORY}/blobs/uploads/"
        status, headers = self.request("POST", base)
        if status != 202 or not headers.get("location"):
            raise PublicationError(f"registry blob POST returned HTTP {status}")
        status, headers = self.request(
            "PATCH", upload_path(headers["location"]), source, "application/octet-stream"
        )
        if status != 202 or not headers.get("location"):
            raise PublicationError(f"registry blob PATCH returned HTTP {status}")
        status, headers = self.request("PUT", commit_path(headers["location"], digest))
        if status != 201 or headers.get("docker-content-digest") != digest:
            raise PublicationError(
                f"registry blob commit returned HTTP {status} or unexpected digest"
            )

    def publish(self, tag: str, archive: Path, content_digest: str) -> dict:
        manifest_path = f"/v2/{REPOSITORY}/manifests/{tag}"
        status, _ = self.request("HEAD", manifest_path)
        if status != 404:
            raise PublicationError(f"source tag was not confirmed absent: HTTP {status}")
        config = compact_json(
            {
                "architecture": "amd64",
                "os": "linux",
                "config": {},
                "rootfs": {"type": "layers", "diff_ids": [content_digest]},
            }
        )
        config_digest = digest_bytes(config)
        manifest = compact_json(
            {
                "schemaVersion": 2,
                "mediaType": MANIFEST_MEDIA_TYPE,
                "config": {
                    "mediaType": CONFIG_MEDIA_TYPE,
                    "digest": config_digest,
                    "size": len(config),
                },
                "layers": [
                    {
                        "mediaType": LAYER_MEDIA_TYPE,
                        "digest": content_digest,
                        "size": archive.stat().st_size,
                    }
                ],
                "annotations": {
                    "org.opencontainers.image.title": "kova-source.zip",
                    "dev.cofy.kova.source.digest": content_digest,
                },
            }
        )
        manifest_digest = digest_bytes(manifest)
        self.upload_blob(config, config_digest)
        self.upload_blob(archive, content_digest)
        status, headers = self.request("PUT", manifest_path, manifest, MANIFEST_MEDIA_TYPE)
        if status != 201 or headers.get("docker-content-digest") != manifest_digest:
            raise PublicationError(
                f"registry manifest PUT returned HTTP {status} or unexpected digest"
            )
        status, headers = self.request("HEAD", manifest_path)
        if status != 200 or headers.get("docker-content-digest") != manifest_digest:
            raise PublicationError("source tag HEAD did not return the immutable manifest digest")
        status, headers = self.request("HEAD", f"/v2/{REPOSITORY}/blobs/{content_digest}")
        if status != 200 or int(headers.get("content-length", "-1")) != archive.stat().st_size:
            raise PublicationError("source blob HEAD did not return the exact payload size")
        return {
            "source_manifest_digest": manifest_digest,
            "source_digest": content_digest,
            "config_digest": config_digest,
        }


def save(path: Path, value: object) -> None:
    path.write_text(json.dumps(value, sort_keys=True, indent=2) + "\n", encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--run-dir", type=Path, required=True)
    args = parser.parse_args()
    if not RUN_PATTERN.fullmatch(args.run_id):
        raise PublicationError("run ID does not match the exact source-capacity identity")
    if args.run_dir.is_symlink() or not args.run_dir.is_dir() or args.run_dir.name != args.run_id:
        raise PublicationError("run directory is not the exact existing private run")
    target = f"kind-registry:5000/kova-examples/source-capacity:{args.run_id}"
    archive = args.run_dir / "oversize-source.zip"
    if archive.exists() or archive.is_symlink():
        raise PublicationError("oversize source fixture already exists")
    facts = make_sparse_zip(archive, ARCHIVE_BYTES, target)
    content_digest = digest_file(archive)
    published = Registry().publish(args.run_id, archive, content_digest)
    source_uri = f"oci://{REGISTRY}/{REPOSITORY}@{published['source_manifest_digest']}"
    save(args.run_dir / "source-receipt.json", {"uri": source_uri, "digest": content_digest})
    save(
        args.run_dir / "source-publication.json",
        {**facts, **published, "target": target, "source_uri": source_uri, "tag": args.run_id},
    )
    print(json.dumps({"uri": source_uri, "digest": content_digest}, sort_keys=True))


if __name__ == "__main__":
    try:
        main()
    except (PublicationError, OSError, ValueError, http.client.HTTPException) as error:
        print(f"source-oci-oversize: {type(error).__name__}: {error}", file=sys.stderr)
        raise SystemExit(1) from None

#!/usr/bin/env python3
"""Bounded, test-only registry proxy for the isolated #41 overwrite acceptance.

Only two exact run-scoped output repositories are reachable. The fault blocks
GET-by-digest from the pinned Service Pod IP, not runner-side Nydusify reads,
blob uploads, manifest PUTs, or tag HEADs.
The backend is the dedicated disposable registry, never the shared Kind one.
"""

from __future__ import annotations

import http.client
import ipaddress
import json
import os
import re
import threading
import uuid
from collections import deque
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlsplit, urlunsplit

BACKEND_HOST = "kind-registry-digest-41"
BACKEND_PORT = 5000
MODE_FILE = Path("/fault/mode")
REPOSITORY = os.environ.get("COLLISION_REPOSITORY", "")
FAILED_REPOSITORY = os.environ.get("COLLISION_FAILED_REPOSITORY", "")
EXPECTED_HOST = "kova-digest-fault-proxy.kova.svc.cluster.local:5000"
VERIFIER_POD_IP = os.environ.get("COLLISION_VERIFIER_POD_IP", "")
VERIFIER_POD_UID = os.environ.get("COLLISION_VERIFIER_POD_UID", "")
MAX_BODY = 16 * 1024 * 1024
MAX_OBSERVATIONS = 256
OBSERVATIONS: deque[dict] = deque(maxlen=MAX_OBSERVATIONS)
OBSERVATION_LOCK = threading.Lock()
OBSERVATION_TOTAL = 0
HOP_HEADERS = {
    "connection",
    "content-length",
    "expect",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailer",
    "transfer-encoding",
    "upgrade",
}


def valid_repositories() -> bool:
    pattern = r"kova-examples/digest-41-[0-9]{8}t[0-9]{6}z-[0-9a-f]{8}-(?:a|z)"
    return (
        re.fullmatch(pattern, REPOSITORY) is not None
        and re.fullmatch(pattern, FAILED_REPOSITORY) is not None
        and REPOSITORY[:-1] == FAILED_REPOSITORY[:-1]
        and REPOSITORY.endswith("a")
        and FAILED_REPOSITORY.endswith("z")
    )


def valid_verifier_identity() -> bool:
    try:
        ip = ipaddress.IPv4Address(VERIFIER_POD_IP)
        uid = uuid.UUID(VERIFIER_POD_UID)
    except (ipaddress.AddressValueError, ValueError):
        return False
    return (
        ip.is_private
        and not ip.is_loopback
        and not ip.is_link_local
        and not ip.is_multicast
        and not ip.is_unspecified
        and str(uid) == VERIFIER_POD_UID
    )


def allowed_path(path: str) -> bool:
    if path == "/v2/":
        return True
    parsed = urlsplit(path)
    if parsed.scheme or parsed.netloc or parsed.fragment:
        return False
    query = parse_qs(parsed.query, keep_blank_values=True)
    if not set(query).issubset({"digest", "_state"}) or any(
        len(values) != 1 for values in query.values()
    ):
        return False
    if "digest" in query and re.fullmatch(r"sha256:[0-9a-f]{64}", query["digest"][0]) is None:
        return False
    if "_state" in query and len(query["_state"][0]) > 4096:
        return False
    for repository in (REPOSITORY, FAILED_REPOSITORY):
        prefix = f"/v2/{repository}/"
        if parsed.path.startswith(prefix):
            suffix = parsed.path[len(prefix) :]
            if suffix == "blobs/uploads/" and not query:
                return True
            if re.fullmatch(r"blobs/uploads/[0-9a-f-]{16,128}", suffix) and set(query).issubset(
                {"digest", "_state"}
            ):
                return True
            if re.fullmatch(r"blobs/sha256:[0-9a-f]{64}", suffix) and not query:
                return True
            if (
                re.fullmatch(r"manifests/(?:dev|dev_nydus_v3|sha256:[0-9a-f]{64})", suffix)
                and not query
            ):
                return True
    return False


def digest_get(method: str, path: str) -> str:
    parsed = urlsplit(path)
    match = re.fullmatch(
        rf"/v2/{re.escape(REPOSITORY)}/manifests/(sha256:[0-9a-f]{{64}})",
        parsed.path,
    )
    if method != "GET" or match is None:
        return ""
    return match.group(1)


def blocked_digest_get(method: str, path: str, mode: str, peer_ip: str) -> bool:
    digest = digest_get(method, path)
    if peer_ip != VERIFIER_POD_IP or not digest:
        return False
    if mode == "503":
        return True
    if mode.startswith("only:"):
        return digest in mode.removeprefix("only:").split(",")
    return False


def record_digest_get(peer_ip: str, digest: str, mode: str, action: str, status: int) -> None:
    global OBSERVATION_TOTAL
    with OBSERVATION_LOCK:
        OBSERVATION_TOTAL += 1
        OBSERVATIONS.append(
            {
                "sequence": OBSERVATION_TOTAL,
                "method": "GET",
                "repository": REPOSITORY,
                "path": f"/v2/{REPOSITORY}/manifests/{digest}",
                "peer_ip": peer_ip,
                "digest": digest,
                "mode": mode,
                "action": action,
                "status": status,
            }
        )


def observations() -> bytes:
    with OBSERVATION_LOCK:
        value = {
            "repository": REPOSITORY,
            "failed_repository": FAILED_REPOSITORY,
            "verifier_pod_ip": VERIFIER_POD_IP,
            "verifier_pod_uid": VERIFIER_POD_UID,
            "total": OBSERVATION_TOTAL,
            "overflow": OBSERVATION_TOTAL > MAX_OBSERVATIONS,
            "events": list(OBSERVATIONS),
        }
    return json.dumps(value, separators=(",", ":")).encode()


def valid_mode(mode: str) -> bool:
    if mode in ("healthy", "503"):
        return True
    if not mode.startswith("only:"):
        return False
    digests = mode.removeprefix("only:").split(",")
    return (
        len(digests) == 2
        and len(set(digests)) == 2
        and all(re.fullmatch(r"sha256:[0-9a-f]{64}", digest) is not None for digest in digests)
    )


def safe_location(value: str, incoming_host: str) -> str:
    parsed = urlsplit(value)
    if not allowed_path(parsed.path + ("?" + parsed.query if parsed.query else "")):
        raise ValueError("registry upload Location escaped test repositories")
    if parsed.scheme == parsed.netloc == "":
        if not parsed.path.startswith(
            f"/v2/{REPOSITORY}/blobs/uploads/"
        ) and not parsed.path.startswith(f"/v2/{FAILED_REPOSITORY}/blobs/uploads/"):
            raise ValueError("registry upload Location escaped test repositories")
        return value
    if parsed.scheme != "http" or parsed.username or parsed.password:
        raise ValueError("registry upload Location used an unsafe authority")
    if parsed.netloc not in (incoming_host, f"{BACKEND_HOST}:{BACKEND_PORT}"):
        raise ValueError("registry upload Location escaped test registry")
    return urlunsplit(("http", incoming_host, parsed.path, parsed.query, ""))


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def do_GET(self) -> None:  # noqa: N802
        self.handle_request()

    def do_HEAD(self) -> None:  # noqa: N802
        self.handle_request()

    def do_POST(self) -> None:  # noqa: N802
        self.handle_request()

    def do_PATCH(self) -> None:  # noqa: N802
        self.handle_request()

    def do_PUT(self) -> None:  # noqa: N802
        self.handle_request()

    def do_DELETE(self) -> None:  # noqa: N802
        self.reply(405, b"registry deletion is not supported by this acceptance")

    def read_body(self) -> bytes:
        transfer = self.headers.get("Transfer-Encoding", "").lower()
        if transfer:
            if transfer != "chunked" or self.headers.get("Content-Length"):
                raise ValueError("unsupported registry request framing")
            chunks = bytearray()
            while True:
                line = self.rfile.readline(128)
                if not line.endswith(b"\r\n"):
                    raise ValueError("invalid registry chunk header")
                size = int(line[:-2].split(b";", 1)[0], 16)
                if size > MAX_BODY - len(chunks):
                    raise ValueError("registry upload exceeds 16 MiB")
                if size == 0:
                    if self.rfile.read(2) != b"\r\n":
                        raise ValueError("registry chunk trailer is unsupported")
                    return bytes(chunks)
                chunks.extend(self.rfile.read(size))
                if self.rfile.read(2) != b"\r\n":
                    raise ValueError("truncated registry chunk")
        length = self.headers.get("Content-Length", "0")
        if not length.isdigit() or int(length) > MAX_BODY:
            raise ValueError("registry upload exceeds 16 MiB")
        body = self.rfile.read(int(length))
        if len(body) != int(length):
            raise ValueError("truncated registry request")
        return body

    def handle_request(self) -> None:
        if self.path == "/fault/mode" and self.command == "GET":
            self.reply(200, MODE_FILE.read_bytes(), "text/plain")
            return
        if self.path == "/fault/observations" and self.command == "GET":
            self.reply(200, observations(), "application/json")
            return
        if self.path == "/v2/" and self.command in ("GET", "HEAD"):
            # Kubelet's readiness probe uses the Pod IP as Host. Registry
            # traffic still requires the exact proxy DNS authority below.
            self.reply(200, b"{}", "application/json")
            return
        if not allowed_path(self.path):
            self.reply(404, b"outside exact test repositories")
            return
        try:
            mode = MODE_FILE.read_text(encoding="ascii").strip()
            if not valid_mode(mode):
                raise ValueError("invalid fault mode")
            digest = digest_get(self.command, self.path)
            peer_ip = self.client_address[0]
            if blocked_digest_get(self.command, self.path, mode, peer_ip):
                record_digest_get(peer_ip, digest, mode, "fault", 503)
                self.reply(503, b"test-only digest verification fault")
                return
            body = self.read_body()
            headers = {
                key: value for key, value in self.headers.items() if key.lower() not in HOP_HEADERS
            }
            headers["Content-Length"] = str(len(body))
            host = self.headers.get("Host", "")
            if host != EXPECTED_HOST:
                raise ValueError("registry Host is not the exact isolated proxy")
            headers["Host"] = host
            connection = http.client.HTTPConnection(BACKEND_HOST, BACKEND_PORT, timeout=15)
            try:
                connection.request(self.command, self.path, body=body, headers=headers)
                response = connection.getresponse()
                payload = response.read(MAX_BODY + 1) if self.command != "HEAD" else b""
                if len(payload) > MAX_BODY:
                    raise ValueError("registry response exceeds 16 MiB")
                forwarded = {
                    key: value
                    for key, value in response.getheaders()
                    if key.lower() not in HOP_HEADERS
                }
                content_length = None
                if self.command == "HEAD":
                    declared_length = response.getheader("Content-Length")
                    if declared_length is None or not declared_length.isdecimal():
                        raise ValueError("registry HEAD omitted a valid descriptor size")
                    content_length = int(declared_length)
                for key in list(forwarded):
                    if key.lower() == "location":
                        forwarded[key] = safe_location(forwarded[key], host)
                if digest:
                    record_digest_get(peer_ip, digest, mode, "forwarded", response.status)
                self.reply(
                    response.status, payload, headers=forwarded, content_length=content_length
                )
            finally:
                connection.close()
        except (OSError, ValueError, http.client.HTTPException) as error:
            self.log_error("isolated registry proxy rejected request: %s", type(error).__name__)
            self.reply(502, b"isolated registry proxy cannot prove request outcome")

    def reply(
        self,
        status: int,
        payload: bytes,
        content_type: str = "text/plain",
        *,
        headers: dict | None = None,
        content_length: int | None = None,
    ) -> None:
        self.send_response(status)
        for key, value in (headers or {}).items():
            self.send_header(key, value)
        if not any(key.lower() == "content-type" for key in (headers or {})):
            self.send_header("Content-Type", content_type)
        response_length = len(payload) if content_length is None else content_length
        self.send_header("Content-Length", str(response_length))
        self.end_headers()
        if self.command != "HEAD":
            try:
                self.wfile.write(payload)
            except (BrokenPipeError, ConnectionResetError):
                pass


if __name__ == "__main__":
    if not valid_repositories() or not valid_verifier_identity():
        raise SystemExit("exact run-scoped repository and verifier Pod identities are required")
    ThreadingHTTPServer(("0.0.0.0", 5000), Handler).serve_forever()

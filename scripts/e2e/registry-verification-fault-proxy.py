#!/usr/bin/env python3
"""Test-only, read-only registry proxy for a single Kova E2E repository."""

import http.server
import os
import time
import urllib.error
import urllib.request


REPOSITORY = os.environ["FAULT_REPOSITORY"]
BACKEND = os.environ.get("FAULT_BACKEND", "http://kind-registry:5000")
MODE_FILE = "/fault/mode"
MAX_RESPONSE = 4 << 20
OPENER = urllib.request.build_opener(urllib.request.ProxyHandler({}))


class Handler(http.server.BaseHTTPRequestHandler):
    def do_HEAD(self):
        self.handle_request()

    def do_GET(self):
        self.handle_request()

    def handle_request(self):
        if self.path == "/fault/mode":
            mode = self.mode()
            self.reply(200, mode.encode(), "text/plain")
            return
        if self.path == "/v2/":
            self.reply(200, b"{}", "application/json")
            return
        prefix = f"/v2/{REPOSITORY}/"
        if not self.path.startswith(prefix):
            self.reply(404, b"outside test repository", "text/plain")
            return
        if self.method_is_blob_get() and self.mode() != "healthy":
            if self.mode() == "timeout":
                time.sleep(25)
            self.reply(503, b"test-only injected registry failure", "text/plain")
            return
        request = urllib.request.Request(
            BACKEND + self.path,
            method=self.command,
            headers={"Accept": self.headers.get("Accept", "*/*")},
        )
        try:
            with OPENER.open(request, timeout=8) as response:
                payload = response.read(MAX_RESPONSE + 1) if self.command == "GET" else b""
                if len(payload) > MAX_RESPONSE:
                    self.reply(502, b"test proxy response exceeded 4 MiB", "text/plain")
                    return
                self.reply(response.status, payload, response.headers.get("Content-Type", "application/octet-stream"),
                           response.headers.get("Docker-Content-Digest"))
        except urllib.error.HTTPError as error:
            payload = error.read(MAX_RESPONSE + 1) if self.command == "GET" else b""
            self.reply(error.code, payload[:MAX_RESPONSE], error.headers.get("Content-Type", "text/plain"))
        except Exception as error:  # Test evidence, never forward an unknown outcome as success.
            self.log_error("backend request failed: %s", error)
            self.reply(502, b"test proxy backend unavailable", "text/plain")

    def method_is_blob_get(self):
        return self.command == "GET" and "/blobs/" in self.path

    @staticmethod
    def mode():
        with open(MODE_FILE, encoding="ascii") as mode_file:
            mode = mode_file.read().strip()
        if mode not in ("healthy", "503", "timeout"):
            raise ValueError("invalid test fault mode")
        return mode

    def reply(self, status, payload, content_type, digest=None):
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(payload)))
        if digest:
            self.send_header("Docker-Content-Digest", digest)
        self.end_headers()
        if self.command != "HEAD":
            try:
                self.wfile.write(payload)
            except (BrokenPipeError, ConnectionResetError):
                pass


class Server(http.server.ThreadingHTTPServer):
    daemon_threads = True


Server(("0.0.0.0", 5000), Handler).serve_forever()

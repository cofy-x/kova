from __future__ import annotations

import json
import threading
from collections import defaultdict, deque
from collections.abc import Iterator
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

import pytest


@dataclass(frozen=True)
class FakeResponse:
    status: int
    body: bytes
    headers: dict[str, str]


class FakeKovaService:
    def __init__(self) -> None:
        self._responses: dict[tuple[str, str], deque[FakeResponse]] = defaultdict(deque)
        self.requests: list[tuple[str, str, dict[str, str], bytes]] = []
        self._lock = threading.Lock()
        owner = self

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self) -> None:
                owner._serve(self)

            def do_POST(self) -> None:
                owner._serve(self)

            def log_message(self, _format: str, *_args: object) -> None:
                return

        self._server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self._thread = threading.Thread(target=self._server.serve_forever, daemon=True)
        self._thread.start()
        self.url = f"http://127.0.0.1:{self._server.server_port}"

    def close(self) -> None:
        self._server.shutdown()
        self._server.server_close()
        self._thread.join(timeout=2)

    def json_response(
        self,
        method: str,
        path: str,
        value: Any,
        *,
        status: int = 200,
        headers: dict[str, str] | None = None,
    ) -> None:
        body = json.dumps(value).encode()
        response_headers = {"Content-Type": "application/json", **(headers or {})}
        self._responses[(method, path)].append(
            FakeResponse(status=status, body=body, headers=response_headers)
        )

    def text_response(
        self,
        method: str,
        path: str,
        value: str,
        *,
        status: int = 200,
    ) -> None:
        self._responses[(method, path)].append(
            FakeResponse(
                status=status,
                body=value.encode(),
                headers={"Content-Type": "text/plain"},
            )
        )

    def _serve(self, request: BaseHTTPRequestHandler) -> None:
        length = int(request.headers.get("Content-Length", "0"))
        body = request.rfile.read(length)
        path = request.path.split("?", 1)[0]
        with self._lock:
            self.requests.append(
                (request.command, request.path, dict(request.headers.items()), body)
            )
            queue = self._responses[(request.command, path)]
            response = queue.popleft() if queue else None
        if response is None:
            response = FakeResponse(
                status=500,
                body=b'{"code":"internal","message":"unexpected request","retryable":false}',
                headers={"Content-Type": "application/json"},
            )
        request.send_response(response.status)
        for name, value in response.headers.items():
            request.send_header(name, value)
        request.send_header("Content-Length", str(len(response.body)))
        request.end_headers()
        request.wfile.write(response.body)


@pytest.fixture
def fake_service() -> Iterator[FakeKovaService]:
    service = FakeKovaService()
    try:
        yield service
    finally:
        service.close()

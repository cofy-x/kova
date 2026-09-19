from __future__ import annotations

import asyncio
import re
import threading
import time
from collections.abc import Callable
from typing import Any, TypeVar

import httpx

from ._config import ClientConfig
from ._errors import KovaAPIError, KovaProtocolError, KovaWaitCancelled
from ._models import (
    BuildJob,
    BuildResults,
    CreateBuildRequest,
    JobList,
    JobStatus,
    ReadyStatus,
    VersionInfo,
)
from ._transport import ERROR_BODY_LIMIT, api_error, decode_json, decode_text, read_bounded

Model = TypeVar("Model")
ModelFactory = Callable[[dict[str, Any]], Model]
_JOB_ID = re.compile(r"^[a-z0-9](?:[-a-z0-9]*[a-z0-9])?$")
_TERMINAL_STATUSES = {JobStatus.SUCCEEDED, JobStatus.FAILED, JobStatus.CANCELLED}


class KovaClient:
    """Synchronous client for the Kova Service HTTP API v1."""

    def __init__(self, config: ClientConfig, *, http_client: httpx.Client | None = None):
        self._config = config
        self._owns_client = http_client is None
        self._http = http_client or httpx.Client(
            timeout=config.request_timeout,
            verify=config.ssl_context(),
            follow_redirects=False,
        )

    def __enter__(self) -> KovaClient:
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    def close(self) -> None:
        if self._owns_client:
            self._http.close()

    def version(self) -> VersionInfo:
        return self._request_model("GET", "/version", VersionInfo.from_dict, authenticated=False)

    def ready(self) -> ReadyStatus:
        result = self._request_model("GET", "/readyz", ReadyStatus.from_dict, authenticated=False)
        if result.status != "ready":
            raise KovaProtocolError(f"unexpected readiness status {result.status!r}")
        return result

    def create_build(self, request: CreateBuildRequest) -> BuildJob:
        return self._request_model("POST", "/v1/builds", BuildJob.from_dict, json=request.to_dict())

    def get_build(self, build_id: str) -> BuildJob:
        return self._get_model(_build_path(build_id), BuildJob.from_dict)

    def list_builds(self, *, limit: int = 100, continue_token: str | None = None) -> JobList:
        _validate_limit(limit)
        params: dict[str, str | int] = {"limit": limit}
        if continue_token:
            params["continue"] = continue_token
        return self._request_model("GET", "/v1/builds", JobList.from_dict, params=params)

    def get_results(self, build_id: str) -> BuildResults:
        return self._get_model(_build_path(build_id, "results"), BuildResults.from_dict)

    def get_logs(self, build_id: str, *, tail_lines: int = 100) -> str:
        _validate_tail_lines(tail_lines)
        raw = self._request("GET", _build_path(build_id, "logs"), params={"tail_lines": tail_lines})
        return decode_text(raw)

    def cancel_build(self, build_id: str) -> BuildJob:
        return self._request_model("POST", _build_path(build_id, "cancel"), BuildJob.from_dict)

    def wait_build(
        self,
        build_id: str,
        *,
        timeout: float | None = None,
        poll_interval: float = 2.0,
        cancel_event: threading.Event | None = None,
    ) -> BuildJob:
        deadline = _deadline(timeout, poll_interval)
        while True:
            _check_sync_cancel(cancel_event)
            try:
                job = self.get_build(build_id)
            except KovaAPIError as error:
                if not error.retryable:
                    raise
                delay = _retry_delay(error, poll_interval)
            else:
                if job.status in _TERMINAL_STATUSES:
                    return job
                delay = poll_interval
            _sync_sleep(delay, deadline, cancel_event)

    def _get_model(self, path: str, factory: ModelFactory[Model]) -> Model:
        return self._request_model("GET", path, factory)

    def _request_model(
        self,
        method: str,
        path: str,
        factory: ModelFactory[Model],
        authenticated: bool = True,
        **kwargs: Any,
    ) -> Model:
        raw = self._request(method, path, authenticated=authenticated, **kwargs)
        try:
            return factory(decode_json(raw))
        except KovaProtocolError:
            raise
        except (TypeError, ValueError) as error:
            raise KovaProtocolError("Kova Service response does not satisfy API v1") from error

    def _request(
        self, method: str, path: str, *, authenticated: bool = True, **kwargs: Any
    ) -> bytes:
        with self._http.stream(
            method,
            self._config.base_url + path,
            headers=_headers(self._config, authenticated=authenticated),
            **kwargs,
        ) as response:
            limit = self._config.max_response_bytes if response.is_success else ERROR_BODY_LIMIT
            try:
                raw = read_bounded(response.iter_bytes(), limit)
            except KovaProtocolError:
                if response.is_success:
                    raise
                raw = b""
            if not response.is_success:
                raise api_error(response.status_code, response.headers, raw)
            return raw


class AsyncKovaClient:
    """Asynchronous client with the same contract as KovaClient."""

    def __init__(
        self,
        config: ClientConfig,
        *,
        http_client: httpx.AsyncClient | None = None,
    ):
        self._config = config
        self._owns_client = http_client is None
        self._http = http_client or httpx.AsyncClient(
            timeout=config.request_timeout,
            verify=config.ssl_context(),
            follow_redirects=False,
        )

    async def __aenter__(self) -> AsyncKovaClient:
        return self

    async def __aexit__(self, *_: object) -> None:
        await self.aclose()

    async def aclose(self) -> None:
        if self._owns_client:
            await self._http.aclose()

    async def version(self) -> VersionInfo:
        return await self._request_model(
            "GET", "/version", VersionInfo.from_dict, authenticated=False
        )

    async def ready(self) -> ReadyStatus:
        result = await self._request_model(
            "GET", "/readyz", ReadyStatus.from_dict, authenticated=False
        )
        if result.status != "ready":
            raise KovaProtocolError(f"unexpected readiness status {result.status!r}")
        return result

    async def create_build(self, request: CreateBuildRequest) -> BuildJob:
        return await self._request_model(
            "POST", "/v1/builds", BuildJob.from_dict, json=request.to_dict()
        )

    async def get_build(self, build_id: str) -> BuildJob:
        return await self._get_model(_build_path(build_id), BuildJob.from_dict)

    async def list_builds(
        self,
        *,
        limit: int = 100,
        continue_token: str | None = None,
    ) -> JobList:
        _validate_limit(limit)
        params: dict[str, str | int] = {"limit": limit}
        if continue_token:
            params["continue"] = continue_token
        return await self._request_model("GET", "/v1/builds", JobList.from_dict, params=params)

    async def get_results(self, build_id: str) -> BuildResults:
        return await self._get_model(_build_path(build_id, "results"), BuildResults.from_dict)

    async def get_logs(self, build_id: str, *, tail_lines: int = 100) -> str:
        _validate_tail_lines(tail_lines)
        raw = await self._request(
            "GET", _build_path(build_id, "logs"), params={"tail_lines": tail_lines}
        )
        return decode_text(raw)

    async def cancel_build(self, build_id: str) -> BuildJob:
        return await self._request_model(
            "POST", _build_path(build_id, "cancel"), BuildJob.from_dict
        )

    async def wait_build(
        self,
        build_id: str,
        *,
        timeout: float | None = None,
        poll_interval: float = 2.0,
        cancel_event: asyncio.Event | None = None,
    ) -> BuildJob:
        deadline = _deadline(timeout, poll_interval)
        while True:
            _check_async_cancel(cancel_event)
            try:
                job = await self.get_build(build_id)
            except KovaAPIError as error:
                if not error.retryable:
                    raise
                delay = _retry_delay(error, poll_interval)
            else:
                if job.status in _TERMINAL_STATUSES:
                    return job
                delay = poll_interval
            await _async_sleep(delay, deadline, cancel_event)

    async def _get_model(self, path: str, factory: ModelFactory[Model]) -> Model:
        return await self._request_model("GET", path, factory)

    async def _request_model(
        self,
        method: str,
        path: str,
        factory: ModelFactory[Model],
        authenticated: bool = True,
        **kwargs: Any,
    ) -> Model:
        raw = await self._request(method, path, authenticated=authenticated, **kwargs)
        try:
            return factory(decode_json(raw))
        except KovaProtocolError:
            raise
        except (TypeError, ValueError) as error:
            raise KovaProtocolError("Kova Service response does not satisfy API v1") from error

    async def _request(
        self, method: str, path: str, *, authenticated: bool = True, **kwargs: Any
    ) -> bytes:
        async with self._http.stream(
            method,
            self._config.base_url + path,
            headers=_headers(self._config, authenticated=authenticated),
            **kwargs,
        ) as response:
            limit = self._config.max_response_bytes if response.is_success else ERROR_BODY_LIMIT
            content = bytearray()
            async for chunk in response.aiter_bytes():
                if len(content) + len(chunk) > limit:
                    if response.is_success:
                        raise KovaProtocolError(f"Kova Service response exceeds {limit} bytes")
                    content.clear()
                    break
                content.extend(chunk)
            raw = bytes(content)
            if not response.is_success:
                raise api_error(response.status_code, response.headers, raw)
            return raw


def _headers(config: ClientConfig, *, authenticated: bool) -> dict[str, str]:
    headers = {"Accept": "application/json", "User-Agent": "kova-client-python"}
    if authenticated and config.token:
        headers["Authorization"] = f"Bearer {config.token}"
    return headers


def _build_path(build_id: str, action: str | None = None) -> str:
    normalized = build_id.strip()
    if len(normalized) > 253 or not _JOB_ID.fullmatch(normalized):
        raise ValueError(f"invalid build ID {build_id!r}")
    path = f"/v1/builds/{normalized}"
    return f"{path}/{action}" if action else path


def _validate_limit(limit: int) -> None:
    if not 1 <= limit <= 500:
        raise ValueError("limit must be between 1 and 500")


def _validate_tail_lines(tail_lines: int) -> None:
    if not 0 <= tail_lines <= 10_000:
        raise ValueError("tail_lines must be between 0 and 10000")


def _deadline(timeout: float | None, poll_interval: float) -> float | None:
    if poll_interval <= 0:
        raise ValueError("poll_interval must be greater than zero")
    if timeout is None:
        return None
    if timeout < 0:
        raise ValueError("timeout must be non-negative")
    return time.monotonic() + timeout


def _retry_delay(error: KovaAPIError, poll_interval: float) -> float:
    if error.retry_after is None:
        return poll_interval
    return max(error.retry_after.total_seconds(), 0.0)


def _remaining(deadline: float | None, delay: float) -> float:
    if deadline is None:
        return delay
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise TimeoutError("timed out waiting for Kova build")
    return min(delay, remaining)


def _check_sync_cancel(cancel_event: threading.Event | None) -> None:
    if cancel_event is not None and cancel_event.is_set():
        raise KovaWaitCancelled("wait for Kova build was cancelled")


def _sync_sleep(
    delay: float,
    deadline: float | None,
    cancel_event: threading.Event | None,
) -> None:
    wait_for = _remaining(deadline, delay)
    if cancel_event is not None:
        if cancel_event.wait(wait_for):
            raise KovaWaitCancelled("wait for Kova build was cancelled")
    else:
        time.sleep(wait_for)
    if deadline is not None and time.monotonic() >= deadline:
        raise TimeoutError("timed out waiting for Kova build")


def _check_async_cancel(cancel_event: asyncio.Event | None) -> None:
    if cancel_event is not None and cancel_event.is_set():
        raise KovaWaitCancelled("wait for Kova build was cancelled")


async def _async_sleep(
    delay: float,
    deadline: float | None,
    cancel_event: asyncio.Event | None,
) -> None:
    wait_for = _remaining(deadline, delay)
    if cancel_event is None:
        await asyncio.sleep(wait_for)
    else:
        try:
            await asyncio.wait_for(cancel_event.wait(), timeout=wait_for)
        except asyncio.TimeoutError:
            pass
        else:
            raise KovaWaitCancelled("wait for Kova build was cancelled")
    if deadline is not None and time.monotonic() >= deadline:
        raise TimeoutError("timed out waiting for Kova build")

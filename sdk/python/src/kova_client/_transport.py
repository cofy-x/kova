from __future__ import annotations

import json
from collections.abc import Iterable, Mapping
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from http import HTTPStatus
from typing import Any

from ._errors import KovaAPIError, KovaProtocolError

ERROR_BODY_LIMIT = 1 << 20


def decode_json(raw: bytes) -> dict[str, Any]:
    try:
        value = json.loads(raw)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise KovaProtocolError("Kova Service returned invalid JSON") from error
    if not isinstance(value, dict) or not all(isinstance(key, str) for key in value):
        raise KovaProtocolError("Kova Service response must be a JSON object")
    return value


def decode_text(raw: bytes) -> str:
    try:
        return raw.decode("utf-8")
    except UnicodeDecodeError as error:
        raise KovaProtocolError("Kova Service returned non-UTF-8 logs") from error


def read_bounded(chunks: Iterable[bytes], max_bytes: int) -> bytes:
    content = bytearray()
    for chunk in chunks:
        if len(content) + len(chunk) > max_bytes:
            raise KovaProtocolError(f"Kova Service response exceeds {max_bytes} bytes")
        content.extend(chunk)
    return bytes(content)


def api_error(status_code: int, headers: Mapping[str, str], raw: bytes) -> KovaAPIError:
    code = _default_error_code(status_code)
    message = _status_text(status_code)
    retryable = status_code == 429 or status_code >= 500
    try:
        payload = json.loads(raw)
    except (UnicodeDecodeError, json.JSONDecodeError):
        payload = None
    if isinstance(payload, dict):
        if isinstance(payload.get("code"), str) and payload["code"]:
            code = payload["code"]
        if isinstance(payload.get("message"), str) and payload["message"].strip():
            message = payload["message"].strip()
        if isinstance(payload.get("retryable"), bool):
            retryable = payload["retryable"]
    return KovaAPIError(
        status_code=status_code,
        code=code,
        message=message,
        retryable=retryable,
        retry_after=parse_retry_after(headers.get("Retry-After")),
    )


def parse_retry_after(value: str | None, *, now: datetime | None = None) -> timedelta | None:
    if not value:
        return None
    normalized = value.strip()
    try:
        seconds = int(normalized)
    except ValueError:
        seconds = -1
    if seconds >= 0:
        return timedelta(seconds=seconds) if seconds > 0 else None
    try:
        when = parsedate_to_datetime(normalized)
    except (TypeError, ValueError, OverflowError):
        return None
    if when.tzinfo is None:
        when = when.replace(tzinfo=timezone.utc)
    current = now or datetime.now(timezone.utc)
    delay = when - current
    return delay if delay.total_seconds() > 0 else None


def _default_error_code(status_code: int) -> str:
    return {
        400: "invalid_request",
        405: "invalid_request",
        401: "unauthenticated",
        403: "forbidden",
        404: "not_found",
        409: "conflict",
        429: "queue_capacity_exceeded",
        410: "logs_unavailable",
    }.get(status_code, "internal")


def _status_text(status_code: int) -> str:
    try:
        return HTTPStatus(status_code).phrase
    except ValueError:
        return "request failed"

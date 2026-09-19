from __future__ import annotations

from datetime import timedelta


class KovaAPIError(Exception):
    """A structured error returned by the Kova Service."""

    __slots__ = ("code", "message", "retry_after", "retryable", "status_code")

    def __init__(
        self,
        *,
        status_code: int,
        code: str,
        message: str,
        retryable: bool,
        retry_after: timedelta | None = None,
    ) -> None:
        self.status_code = status_code
        self.code = code
        self.message = message
        self.retryable = retryable
        self.retry_after = retry_after
        super().__init__(message)

    def __str__(self) -> str:
        return f"Kova Service API {self.code} (HTTP {self.status_code}): {self.message}"


class KovaProtocolError(Exception):
    """The Service returned a response that does not satisfy HTTP API v1."""


class KovaWaitCancelled(Exception):
    """A caller-owned cancellation signal stopped wait_build."""

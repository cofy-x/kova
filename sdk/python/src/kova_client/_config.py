from __future__ import annotations

import os
import ssl
from collections.abc import Mapping
from dataclasses import dataclass, field
from urllib.parse import urlsplit, urlunsplit

DEFAULT_MAX_RESPONSE_BYTES = 64 << 20
MAX_MAX_RESPONSE_BYTES = 1 << 30


@dataclass(frozen=True, slots=True)
class ClientConfig:
    """Explicit Kova Service connection settings."""

    base_url: str
    token: str | None = field(default=None, repr=False)
    ca_file: str | None = None
    insecure: bool = False
    request_timeout: float = 30.0
    max_response_bytes: int = DEFAULT_MAX_RESPONSE_BYTES

    def __post_init__(self) -> None:
        parsed = urlsplit(self.base_url.strip())
        if parsed.scheme not in {"http", "https"} or not parsed.netloc:
            raise ValueError("service URL must be an absolute http or https URL")
        if parsed.username is not None or parsed.password is not None:
            raise ValueError("service URL must not contain credentials")
        if parsed.query or parsed.fragment:
            raise ValueError("service URL must not contain a query or fragment")
        if self.ca_file and self.insecure:
            raise ValueError("ca_file and insecure cannot be used together")
        if self.request_timeout <= 0:
            raise ValueError("request_timeout must be greater than zero")
        if not 1 <= self.max_response_bytes <= MAX_MAX_RESPONSE_BYTES:
            raise ValueError(f"max_response_bytes must be between 1 and {MAX_MAX_RESPONSE_BYTES}")
        normalized = urlunsplit((parsed.scheme, parsed.netloc, parsed.path.rstrip("/"), "", ""))
        object.__setattr__(self, "base_url", normalized)
        token = self.token.strip() if self.token else None
        object.__setattr__(self, "token", token or None)
        ca_file = self.ca_file.strip() if self.ca_file else None
        object.__setattr__(self, "ca_file", ca_file or None)

    @classmethod
    def from_env(cls, env: Mapping[str, str] | None = None) -> ClientConfig:
        """Explicitly construct configuration from the documented KOVA_SERVICE_* variables."""

        values = os.environ if env is None else env
        base_url = values.get("KOVA_SERVICE_URL", "").strip()
        if not base_url:
            raise ValueError("KOVA_SERVICE_URL is required")
        return cls(
            base_url=base_url,
            token=values.get("KOVA_SERVICE_TOKEN"),
            ca_file=values.get("KOVA_SERVICE_CA_FILE"),
            insecure=_parse_bool(values.get("KOVA_SERVICE_INSECURE", ""), "KOVA_SERVICE_INSECURE"),
        )

    def ssl_context(self) -> ssl.SSLContext | bool:
        if self.insecure:
            return False
        if self.ca_file:
            return ssl.create_default_context(cafile=self.ca_file)
        return True


def _parse_bool(value: str, name: str) -> bool:
    normalized = value.strip().lower()
    if normalized in {"", "0", "false", "no", "off"}:
        return False
    if normalized in {"1", "true", "yes", "on"}:
        return True
    raise ValueError(f"{name} must be a boolean")

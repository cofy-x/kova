"""Official Python client for the Kova Service HTTP API v1."""

from ._client import AsyncKovaClient, KovaClient
from ._config import ClientConfig
from ._errors import KovaAPIError, KovaProtocolError, KovaWaitCancelled
from ._models import (
    BuildFailureCode,
    BuildFormat,
    BuildJob,
    BuildOutput,
    BuildResults,
    CreateBuildRequest,
    JobList,
    JobStatus,
    OutputFormat,
    Platform,
    ReadyStatus,
    TargetSpec,
    VersionInfo,
)

try:
    from ._version import __version__
except ImportError:  # pragma: no cover - generated for wheels and editable installs
    __version__ = "0.0.0.dev0"

__all__ = [
    "AsyncKovaClient",
    "BuildFailureCode",
    "BuildFormat",
    "BuildJob",
    "BuildOutput",
    "BuildResults",
    "ClientConfig",
    "CreateBuildRequest",
    "JobList",
    "JobStatus",
    "KovaAPIError",
    "KovaClient",
    "KovaProtocolError",
    "KovaWaitCancelled",
    "OutputFormat",
    "Platform",
    "ReadyStatus",
    "TargetSpec",
    "VersionInfo",
    "__version__",
]

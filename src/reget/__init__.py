"""reget: resumable, cursor-based, CDN-safe HTTP downloads."""

from reget._types import (
    ContentRangeError,
    ControlFileError,
    DownloadComplete,
    DownloadPartial,
    DownloadResult,
    DownloadStatus,
    ETag,
    HashBuilder,
    RegetError,
    ServerMeta,
    ServerMisconfiguredError,
    Url,
    parse_etag,
    parse_url,
)
from reget._version import __version__

__all__ = [
    "ContentRangeError",
    "ControlFileError",
    "DownloadComplete",
    "DownloadPartial",
    "DownloadResult",
    "DownloadStatus",
    "ETag",
    "HashBuilder",
    "RegetError",
    "ServerMeta",
    "ServerMisconfiguredError",
    "Url",
    "__version__",
    "parse_etag",
    "parse_url",
]

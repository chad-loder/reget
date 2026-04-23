"""reget: resumable, piece-tracked, CDN-safe HTTP downloads.

See the README for an overview.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from reget._types import (
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
from reget.alloc import (
    AllocationOutcome,
    AllocationPreset,
    AllocationResult,
    Mechanism,
    allocate_file,
    allocation_reserved_fpreallocate,
    allocation_reserved_posix,
    allocation_sparse,
)
from reget.control import ControlFile, ControlFileError, ControlMeta
from reget.downloader import PieceDownloader, fetch
from reget.headers import DEFAULT_HEADERS, is_file_changed
from reget.tracker import PieceTracker

if TYPE_CHECKING:
    from reget.transport.factory import wrap_transport
    from reget.transport.niquests_adapter import NiquestsAdapter, niquests_transport

__all__ = [
    "DEFAULT_HEADERS",
    "AllocationOutcome",
    "AllocationPreset",
    "AllocationResult",
    "ControlFile",
    "ControlFileError",
    "ControlMeta",
    "DownloadComplete",
    "DownloadPartial",
    "DownloadResult",
    "DownloadStatus",
    "ETag",
    "HashBuilder",
    "Mechanism",
    "NiquestsAdapter",
    "PieceDownloader",
    "PieceTracker",
    "RegetError",
    "ServerMeta",
    "ServerMisconfiguredError",
    "Url",
    "__version__",
    "allocate_file",
    "allocation_reserved_fpreallocate",
    "allocation_reserved_posix",
    "allocation_sparse",
    "fetch",
    "is_file_changed",
    "niquests_transport",
    "parse_etag",
    "parse_url",
    "wrap_transport",
]


def __getattr__(name: str) -> Any:
    if name == "NiquestsAdapter":
        from reget.transport import NiquestsAdapter as _NiquestsAdapter

        return _NiquestsAdapter
    if name == "niquests_transport":
        from reget.transport import niquests_transport as _niquests_transport

        return _niquests_transport
    if name == "wrap_transport":
        from reget.transport.factory import wrap_transport as _wrap_transport

        return _wrap_transport
    msg = f"module {__name__!r} has no attribute {name!r}"
    raise AttributeError(msg)

"""Shared types and result containers for reget."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from enum import Enum, auto
from pathlib import Path
from typing import Literal, NewType
from urllib.parse import urlparse

ByteOffset = NewType("ByteOffset", int)
"""Absolute byte offset from the start of the download target."""

ByteLength = NewType("ByteLength", int)
"""Byte count — length of a range, piece, or buffer."""

Url = NewType("Url", str)
"""An http(s) URL that has passed :func:`parse_url`."""

ETag = NewType("ETag", str)
"""An HTTP ETag value (possibly empty) that has passed :func:`parse_etag`.

Stored verbatim as the server sent it — quotes and optional ``W/`` prefix
included — so that it can be echoed back in ``If-Range`` / ``If-Match``
byte-for-byte. An empty string means "no ETag for this resource".
"""

EMPTY_ETAG: ETag = ETag("")
"""Shared "no ETag" singleton. Used as the default for signatures where
inline ``ETag("")`` would trip B008."""

EMPTY_URL: Url = Url("")
"""Shared "no URL yet" sentinel for components constructed before the
downloader has a parsed URL to hand them (e.g. tests, trackers built
for size bookkeeping only)."""

_URL_SCHEMES: frozenset[str] = frozenset({"http", "https"})
_MIN_QUOTED_ETAG_LEN = 2  # two DQUOTEs wrap any valid entity-tag


def parse_url(raw: str) -> Url:
    """Validate *raw* as an http(s) URL and brand it as :data:`Url`.

    Empty / whitespace-only input returns :data:`EMPTY_URL` ("no URL
    yet") — callers that forbid that case (e.g. the CLI) must check
    separately. Non-empty input must have an ``http`` or ``https``
    scheme and a host, or :class:`ValueError` is raised.
    """
    if not raw or not raw.strip():
        return EMPTY_URL
    parsed = urlparse(raw)
    if parsed.scheme.lower() not in _URL_SCHEMES:
        raise ValueError(
            f"unsupported url scheme {parsed.scheme!r}; expected http or https",
        )
    if not parsed.netloc:
        raise ValueError(f"url is missing a host: {raw!r}")
    return Url(raw)


def parse_etag(raw: object) -> ETag:
    """Normalize a raw ETag header value into an :data:`ETag`.

    Strips surrounding whitespace. Empty input becomes ``ETag("")`` to
    represent "no ETag". Values that clearly aren't well-formed
    entity-tags (per RFC 7232: optional ``W/`` then a quoted string) are
    also treated as absent, since echoing them back in ``If-Range`` /
    ``If-Match`` would be unsafe. Non-string input raises
    :class:`TypeError`.
    """
    if not isinstance(raw, str):
        raise TypeError(f"etag must be str, got {type(raw).__name__}")
    stripped = raw.strip()
    if not stripped:
        return EMPTY_ETAG
    candidate = stripped.removeprefix("W/")
    if len(candidate) < _MIN_QUOTED_ETAG_LEN or not candidate.startswith('"') or not candidate.endswith('"'):
        return EMPTY_ETAG
    return ETag(stripped)


@dataclass(frozen=True, slots=True, kw_only=True)
class ServerMeta:
    """Typed view of the response headers reget cares about.

    ``total_length`` is ``None`` when the server omits ``Content-Length``
    or returns a non-integer value.  Every string field is ``""`` when
    the corresponding header is absent; ``is_file_changed`` treats
    missing ``last_modified`` on either side as "cannot prove unchanged".
    """

    etag: ETag = EMPTY_ETAG
    total_length: int | None = None
    last_modified: str = ""
    content_type: str = ""


class DownloadStatus(Enum):
    """Tag value for the :data:`DownloadResult` variants."""

    COMPLETE = auto()
    PARTIAL = auto()


@dataclass(frozen=True, slots=True, kw_only=True)
class DownloadComplete:
    """Download finished; all requested bytes are on disk.

    ``sha256``, ``etag``, and ``content_type`` are only available on
    this variant — callers must narrow (``isinstance`` or ``match``)
    before reading them.
    """

    status: Literal[DownloadStatus.COMPLETE] = DownloadStatus.COMPLETE
    bytes_written: int
    elapsed: float
    sha256: str
    etag: ETag
    content_type: str


@dataclass(frozen=True, slots=True, kw_only=True)
class DownloadPartial:
    """Download stopped before the full range was retrieved.

    The ``.part`` and ``.part.ctrl`` files persist on disk, so a
    subsequent ``fetch()`` call against the same destination resumes
    from where this result left off. ``reason`` is a short
    human-readable message intended for logging.
    """

    status: Literal[DownloadStatus.PARTIAL] = DownloadStatus.PARTIAL
    bytes_written: int
    valid_length: int
    elapsed: float
    reason: str = ""


DownloadResult = DownloadComplete | DownloadPartial
"""Union of the two terminal outcomes returned by ``fetch()``."""


class RegetError(Exception):
    """Base exception for reget downloader errors."""


class ServerMisconfiguredError(RegetError):
    """The server violated protocol in a way that prevents safe download."""


class ContentRangeError(RegetError):
    """Content-Range header doesn't match the expected range boundaries."""


class ControlFileError(RegetError):
    """The control file is corrupt, unreadable, or inconsistent with the part file."""


class HashBuilder:
    """Incremental SHA-256 accumulator fed during sequential writes."""

    def __init__(self) -> None:
        self._h = hashlib.sha256()
        self._pos = 0

    def update(self, offset: int, data: bytes) -> None:
        """Feed data at the given file offset.

        Writes are always sequential in the cursor model (offset always
        == self._pos), producing a correct rolling hash.
        """
        if offset == self._pos:
            self._h.update(data)
            self._pos += len(data)

    @property
    def sequential(self) -> bool:
        """True if every update has been in-order (sequential stream)."""
        return True

    def hexdigest(self) -> str:
        return self._h.hexdigest()

    @staticmethod
    def hash_file(path: str | Path, buf_size: int = 1 << 16) -> str:
        """Compute SHA-256 of a complete file on disk."""
        h = hashlib.sha256()
        with Path(path).open("rb") as f:
            while True:
                chunk = f.read(buf_size)
                if not chunk:
                    break
                h.update(chunk)
        return h.hexdigest()

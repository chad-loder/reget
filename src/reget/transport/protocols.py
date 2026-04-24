"""Structural protocols for pluggable HTTP transports.

Concrete adapters implement the sync pair (:class:`TransportSession` /
:class:`TransportResponse`) or the async pair (:class:`AsyncTransportSession` /
:class:`AsyncTransportResponse`).  The download engine depends only on these
contracts.

All four protocols are :func:`typing.runtime_checkable`.
"""

from collections.abc import AsyncIterator, Iterator, Mapping
from contextlib import AbstractAsyncContextManager, AbstractContextManager
from typing import Protocol, runtime_checkable

from reget._types import Url
from reget.transport.types import TransportHeaders, TransportRequestOptions

# ---------------------------------------------------------------------------
# Sync protocols
# ---------------------------------------------------------------------------


@runtime_checkable
class TransportResponse(Protocol):
    """Read-only HTTP response: status, normalized headers, raw body iterator."""

    @property
    def status_code(self) -> int:
        """HTTP status line code (e.g. 200, 206, 416)."""
        ...

    @property
    def headers(self) -> TransportHeaders:
        """Normalized response headers."""
        ...

    def raise_for_status(self) -> None:
        """Raise an error if the response status is a client or server error."""
        ...

    def iter_raw_bytes(self, *, chunk_size: int) -> Iterator[bytes]:
        """Yield raw entity-body chunks (post-TE, pre-CE).

        ``chunk_size`` is a hint; the last chunk may be shorter.
        """
        ...


@runtime_checkable
class TransportSession(Protocol):
    """Sync session: one method (``stream_get``), no lifecycle, no mutation."""

    def stream_get(
        self,
        url: Url,
        *,
        headers: Mapping[str, str],
        options: TransportRequestOptions | None = None,
    ) -> AbstractContextManager[TransportResponse]:
        """Context manager yielding a response whose body must be streamed."""
        ...


# ---------------------------------------------------------------------------
# Async protocols
# ---------------------------------------------------------------------------


@runtime_checkable
class AsyncTransportResponse(Protocol):
    """Async equivalent of :class:`TransportResponse`."""

    @property
    def status_code(self) -> int:
        """HTTP status line code (e.g. 200, 206, 416)."""
        ...

    @property
    def headers(self) -> TransportHeaders:
        """Normalized response headers."""
        ...

    def raise_for_status(self) -> None:
        """Raise an error if the response status is a client or server error."""
        ...

    def aiter_raw_bytes(self, *, chunk_size: int) -> AsyncIterator[bytes]:
        """Async version of :meth:`TransportResponse.iter_raw_bytes`."""
        ...


@runtime_checkable
class AsyncTransportSession(Protocol):
    """Async equivalent of :class:`TransportSession`."""

    def stream_get(
        self,
        url: Url,
        *,
        headers: Mapping[str, str],
        options: TransportRequestOptions | None = None,
    ) -> AbstractAsyncContextManager[AsyncTransportResponse]:
        """Async context manager yielding a response whose body must be streamed."""
        ...

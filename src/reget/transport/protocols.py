"""Structural protocols for pluggable HTTP transports.

Concrete adapters implement :class:`TransportSession` and
:class:`TransportResponse`; the download engine depends only on these contracts.

:class:`TransportSession` and :class:`TransportResponse` are
:func:`typing.runtime_checkable` so ``isinstance`` checks distinguish adapters
from native clients where needed (attribute presence only; see project docs for
custom implementations).
"""

from collections.abc import Iterator, Mapping
from contextlib import AbstractContextManager
from typing import Protocol, runtime_checkable

from reget._types import Url
from reget.transport.types import TransportHeaders, TransportRequestOptions


@runtime_checkable
class TransportResponse(Protocol):
    """Scoped HTTP response: status, normalized headers, raw body iterator."""

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
        """Yield entity-body chunks without transparent Content-Encoding decoding.

        ``chunk_size`` is keyword-only and is a hint; the last chunk may be
        shorter. Empty chunks are allowed.
        """
        ...


@runtime_checkable
class TransportSession(Protocol):
    """Factory for scoped HEAD / streaming GET responses."""

    def opaque_progress_handle(self) -> object:
        """Opaque value for progress callbacks (e.g. the native client session).

        Callers must not assume a concrete type; treat as an opaque token unless
        they created the transport and know what they passed in.
        """
        ...

    def head(
        self,
        url: Url,
        *,
        headers: Mapping[str, str],
        options: TransportRequestOptions | None = None,
    ) -> AbstractContextManager[TransportResponse]:
        """Context manager yielding a response (body typically unused)."""
        ...

    def stream_get(
        self,
        url: Url,
        *,
        headers: Mapping[str, str],
        options: TransportRequestOptions | None = None,
    ) -> AbstractContextManager[TransportResponse]:
        """Context manager yielding a response whose body must be streamed."""
        ...

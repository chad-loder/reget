"""Niquests-backed transport adapters (sync and async)."""

from __future__ import annotations

from collections.abc import AsyncIterator, Iterator, Mapping
from contextlib import asynccontextmanager, contextmanager
from typing import TypedDict, cast

import niquests
import niquests.exceptions as _niquests_exceptions

from reget._types import Url
from reget.transport._requests_like_error_map import map_requests_like_transport_errors
from reget.transport.protocols import (
    AsyncTransportResponse,
    AsyncTransportSession,
    TransportResponse,
    TransportSession,
)
from reget.transport.types import TransportHeaders, TransportRequestOptions


def _header_value_to_str(value: object, default: str = "") -> str:
    if value is None:
        return default
    if isinstance(value, bytes):
        return value.decode("latin-1")
    return str(value)


def headers_from_niquests_response(resp: niquests.Response) -> TransportHeaders:
    """Build :class:`TransportHeaders` from a niquests response."""
    pairs: list[tuple[str, str]] = []
    hdr = resp.headers
    for key_obj in hdr:
        k = str(key_obj)
        v_raw: object = hdr[key_obj]
        v = _header_value_to_str(v_raw).strip()
        pairs.append((k, v))
    return TransportHeaders.from_pairs(pairs)


class _NiquestsRequestKwargs(TypedDict, total=False):
    timeout: float | tuple[float, float]
    verify: bool
    allow_redirects: bool


def _request_options_to_niquests_kwargs(
    options: TransportRequestOptions | None,
) -> _NiquestsRequestKwargs:
    if options is None:
        return {}
    kw: _NiquestsRequestKwargs = {}
    if options.timeout is not None:
        kw["timeout"] = options.timeout
    if options.verify is not None:
        kw["verify"] = options.verify
    if options.allow_redirects is not None:
        kw["allow_redirects"] = options.allow_redirects
    return kw


# ---------------------------------------------------------------------------
# Sync
# ---------------------------------------------------------------------------


class NiquestsTransportResponse(TransportResponse):
    """Transport view over a niquests :class:`~niquests.Response`."""

    __slots__ = ("_headers", "_resp")

    def __init__(self, resp: niquests.Response) -> None:
        self._resp = resp
        self._headers: TransportHeaders | None = None

    @property
    def status_code(self) -> int:
        return cast(int, self._resp.status_code)

    @property
    def headers(self) -> TransportHeaders:
        if self._headers is None:
            self._headers = headers_from_niquests_response(self._resp)
        return self._headers

    def raise_for_status(self) -> None:
        with map_requests_like_transport_errors(_niquests_exceptions):
            self._resp.raise_for_status()

    def iter_raw_bytes(self, *, chunk_size: int) -> Iterator[bytes]:
        with map_requests_like_transport_errors(_niquests_exceptions):
            yield from self._resp.iter_raw(chunk_size=chunk_size)


class NiquestsAdapter:
    """Wrap a :class:`niquests.Session` as a :class:`TransportSession`."""

    __slots__ = ("_session",)

    def __init__(self, session: niquests.Session) -> None:
        self._session = session

    @contextmanager
    def stream_get(
        self,
        url: Url,
        *,
        headers: Mapping[str, str],
        options: TransportRequestOptions | None = None,
    ) -> Iterator[TransportResponse]:
        kwargs = _request_options_to_niquests_kwargs(options)
        with (
            map_requests_like_transport_errors(_niquests_exceptions),
            self._session.get(url, headers=dict(headers), stream=True, **kwargs) as resp,
        ):
            yield NiquestsTransportResponse(resp)


def niquests_transport(session: niquests.Session) -> TransportSession:
    """Shorthand: ``NiquestsAdapter(session)``."""
    return NiquestsAdapter(session)


# ---------------------------------------------------------------------------
# Async
# ---------------------------------------------------------------------------


class AsyncNiquestsTransportResponse(AsyncTransportResponse):
    """Async transport view over a niquests :class:`~niquests.AsyncSession` response."""

    __slots__ = ("_headers", "_resp")

    def __init__(self, resp: niquests.AsyncResponse) -> None:
        self._resp = resp
        self._headers: TransportHeaders | None = None

    @property
    def status_code(self) -> int:
        return cast(int, self._resp.status_code)

    @property
    def headers(self) -> TransportHeaders:
        if self._headers is None:
            pairs: list[tuple[str, str]] = []
            hdr = self._resp.headers
            for key_obj in hdr:
                k = str(key_obj)
                v_raw: object = hdr[key_obj]
                v = _header_value_to_str(v_raw).strip()
                pairs.append((k, v))
            self._headers = TransportHeaders.from_pairs(pairs)
        return self._headers

    def raise_for_status(self) -> None:
        with map_requests_like_transport_errors(_niquests_exceptions):
            self._resp.raise_for_status()

    async def aiter_raw_bytes(self, *, chunk_size: int) -> AsyncIterator[bytes]:
        with map_requests_like_transport_errors(_niquests_exceptions):
            async for chunk in await self._resp.iter_raw(chunk_size=chunk_size):
                yield chunk


class AsyncNiquestsAdapter:
    """Wrap a :class:`niquests.AsyncSession` as an :class:`AsyncTransportSession`."""

    __slots__ = ("_session",)

    def __init__(self, session: niquests.AsyncSession) -> None:
        self._session = session

    @asynccontextmanager
    async def stream_get(
        self,
        url: Url,
        *,
        headers: Mapping[str, str],
        options: TransportRequestOptions | None = None,
    ) -> AsyncIterator[AsyncTransportResponse]:
        kwargs = _request_options_to_niquests_kwargs(options)
        with map_requests_like_transport_errors(_niquests_exceptions):
            resp = await self._session.get(url, headers=dict(headers), stream=True, **kwargs)
            try:
                yield AsyncNiquestsTransportResponse(resp)
            finally:
                await resp.close()


def async_niquests_transport(session: niquests.AsyncSession) -> AsyncTransportSession:
    """Shorthand: ``AsyncNiquestsAdapter(session)``."""
    return AsyncNiquestsAdapter(session)

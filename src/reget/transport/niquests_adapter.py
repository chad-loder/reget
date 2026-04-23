"""Niquests-backed :class:`TransportSession`.

Uses :meth:`niquests.Session.head` and :meth:`~niquests.Session.get` with
``stream=True``. Response bodies use :meth:`niquests.Response.iter_content`.
Headers are built from
``response.headers.items()``; duplicate field names may collapse depending on
the client.
"""

from __future__ import annotations

from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from typing import TypedDict, cast

import niquests
import niquests.exceptions as _niquests_exceptions

from reget._types import Url
from reget.transport._requests_like_error_map import map_requests_like_transport_errors
from reget.transport.protocols import TransportResponse, TransportSession
from reget.transport.types import TransportHeaders, TransportRequestOptions


def _header_value_to_str(value: object, default: str = "") -> str:
    if value is None:
        return default
    if isinstance(value, bytes):
        return value.decode("latin-1")
    return str(value)


def headers_from_niquests_response(resp: niquests.Response) -> TransportHeaders:
    """Build :class:`TransportHeaders` from a niquests response (``str`` / ``bytes`` values).

    Uses ``response.headers.items()``; duplicate field names may be collapsed
    to a single entry depending on niquests/urllib3 behavior — prefer building
    from ordered pairs at the adapter call site if that becomes necessary.
    """
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
            yield from self._resp.iter_content(chunk_size=chunk_size)


class NiquestsAdapter:
    """Wrap a :class:`niquests.Session` as a :class:`TransportSession`."""

    __slots__ = ("_session",)

    def __init__(self, session: niquests.Session) -> None:
        self._session = session

    def opaque_progress_handle(self) -> niquests.Session:
        return self._session

    @property
    def session(self) -> niquests.Session:
        """Underlying niquests session (escape hatch for proxies, mounts, …)."""
        return self._session

    @contextmanager
    def head(
        self,
        url: Url,
        *,
        headers: Mapping[str, str],
        options: TransportRequestOptions | None = None,
    ) -> Iterator[TransportResponse]:
        kwargs = _request_options_to_niquests_kwargs(options)
        with (
            map_requests_like_transport_errors(_niquests_exceptions),
            self._session.head(url, headers=dict(headers), **kwargs) as resp,
        ):
            yield NiquestsTransportResponse(resp)

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

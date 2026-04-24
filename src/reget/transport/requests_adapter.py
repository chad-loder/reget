"""Requests-backed sync transport adapter."""

from __future__ import annotations

from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from typing import TypedDict

import requests
import requests.exceptions as _requests_exceptions

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


def headers_from_requests_response(resp: requests.Response) -> TransportHeaders:
    """Build :class:`TransportHeaders` from a requests response."""
    pairs: list[tuple[str, str]] = []
    for raw_key, raw_val in resp.headers.items():
        k = str(raw_key)
        v = _header_value_to_str(raw_val).strip()
        pairs.append((k, v))
    return TransportHeaders.from_pairs(pairs)


class _RequestsRequestKwargs(TypedDict, total=False):
    timeout: float | tuple[float, float]
    verify: bool
    allow_redirects: bool


def _request_options_to_requests_kwargs(
    options: TransportRequestOptions | None,
) -> _RequestsRequestKwargs:
    if options is None:
        return {}
    kw: _RequestsRequestKwargs = {}
    if options.timeout is not None:
        kw["timeout"] = options.timeout
    if options.verify is not None:
        kw["verify"] = options.verify
    if options.allow_redirects is not None:
        kw["allow_redirects"] = options.allow_redirects
    return kw


class RequestsTransportResponse(TransportResponse):
    """Transport view over a :class:`requests.Response`."""

    __slots__ = ("_headers", "_resp")

    def __init__(self, resp: requests.Response) -> None:
        self._resp = resp
        self._headers: TransportHeaders | None = None

    @property
    def status_code(self) -> int:
        return int(self._resp.status_code)

    @property
    def headers(self) -> TransportHeaders:
        if self._headers is None:
            self._headers = headers_from_requests_response(self._resp)
        return self._headers

    def raise_for_status(self) -> None:
        with map_requests_like_transport_errors(_requests_exceptions):
            self._resp.raise_for_status()

    def iter_raw_bytes(self, *, chunk_size: int) -> Iterator[bytes]:
        with map_requests_like_transport_errors(_requests_exceptions):
            yield from self._resp.raw.stream(chunk_size, decode_content=False)


class RequestsAdapter:
    """Wrap a :class:`requests.Session` as a :class:`TransportSession`."""

    __slots__ = ("_session",)

    def __init__(self, session: requests.Session) -> None:
        self._session = session

    @contextmanager
    def stream_get(
        self,
        url: Url,
        *,
        headers: Mapping[str, str],
        options: TransportRequestOptions | None = None,
    ) -> Iterator[TransportResponse]:
        kwargs = _request_options_to_requests_kwargs(options)
        with (
            map_requests_like_transport_errors(_requests_exceptions),
            self._session.get(url, headers=dict(headers), stream=True, **kwargs) as resp,
        ):
            yield RequestsTransportResponse(resp)


def requests_transport(session: requests.Session) -> TransportSession:
    """Shorthand: ``RequestsAdapter(session)``."""
    return RequestsAdapter(session)

"""httpx-backed :class:`TransportSession` for a synchronous :class:`httpx.Client`.

Maps :mod:`httpx` request and transport errors to :mod:`reget.transport.errors`,
chaining the library exception as ``__cause__``.

- **HEAD** — :meth:`httpx.Client.head`; always :meth:`~httpx.Response.close` on exit.
- **GET** — :meth:`httpx.Client.stream` with ``GET``.
- **Body** — :meth:`httpx.Response.iter_raw` (wire bytes, not ``iter_bytes``).
- **Headers** — :meth:`httpx.Headers.multi_items` keeps duplicate field names.
- **TLS** — per-request ``TransportRequestOptions.verify`` is ignored; set
  ``verify`` on the ``Client`` constructor.
"""

from __future__ import annotations

from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from typing import TypedDict

import httpx

from reget._types import Url
from reget.transport.errors import (
    TransportConnectionError,
    TransportError,
    TransportHTTPError,
    TransportUnsupportedError,
)
from reget.transport.protocols import TransportResponse, TransportSession
from reget.transport.types import TransportHeaders, TransportRequestOptions


def _header_value_to_str(value: object, default: str = "") -> str:
    if value is None:
        return default
    if isinstance(value, bytes):
        return value.decode("latin-1")
    return str(value)


def headers_from_httpx_response(resp: httpx.Response) -> TransportHeaders:
    """Build :class:`TransportHeaders` from an httpx response (multi-value safe)."""
    pairs: list[tuple[str, str]] = []
    for raw_key, raw_val in resp.headers.multi_items():
        k = str(raw_key)
        v = _header_value_to_str(raw_val).strip()
        pairs.append((k, v))
    return TransportHeaders.from_pairs(pairs)


@contextmanager
def map_httpx_transport_errors() -> Iterator[None]:
    """Map :mod:`httpx` request/transport failures to :mod:`reget.transport.errors`."""
    try:
        yield
    except httpx.HTTPStatusError as e:
        sc = int(e.response.status_code)
        raise TransportHTTPError(str(e), status_code=sc) from e
    except httpx.UnsupportedProtocol as e:
        raise TransportUnsupportedError(str(e)) from e
    except httpx.LocalProtocolError as e:
        raise TransportError(f"local HTTP protocol error: {e}") from e
    except httpx.RemoteProtocolError as e:
        raise TransportConnectionError(str(e)) from e
    except (
        httpx.ConnectError,
        httpx.ReadError,
        httpx.WriteError,
        httpx.CloseError,
        httpx.ProxyError,
    ) as e:
        raise TransportConnectionError(str(e)) from e
    except httpx.TimeoutException as e:
        raise TransportConnectionError(str(e)) from e
    except httpx.ProtocolError as e:
        raise TransportConnectionError(str(e)) from e
    except httpx.RequestError as e:
        raise TransportError(str(e)) from e


class _HttpxRequestKwargs(TypedDict, total=False):
    timeout: float | tuple[float | None, float | None, float | None, float | None] | httpx.Timeout
    follow_redirects: bool


def _request_options_to_httpx_kwargs(
    options: TransportRequestOptions | None,
) -> _HttpxRequestKwargs:
    if options is None:
        return {}
    kw: _HttpxRequestKwargs = {}
    if options.timeout is not None:
        t = options.timeout
        kw["timeout"] = (t[0], t[1], None, None) if isinstance(t, tuple) else t
    if options.allow_redirects is not None:
        kw["follow_redirects"] = options.allow_redirects
    return kw


class HttpxTransportResponse(TransportResponse):
    """Transport view over an :class:`httpx.Response`."""

    __slots__ = ("_headers", "_resp")

    def __init__(self, resp: httpx.Response) -> None:
        self._resp = resp
        self._headers: TransportHeaders | None = None

    @property
    def status_code(self) -> int:
        return int(self._resp.status_code)

    @property
    def headers(self) -> TransportHeaders:
        if self._headers is None:
            self._headers = headers_from_httpx_response(self._resp)
        return self._headers

    def raise_for_status(self) -> None:
        with map_httpx_transport_errors():
            self._resp.raise_for_status()

    def iter_raw_bytes(self, *, chunk_size: int) -> Iterator[bytes]:
        """Yield the response body as raw wire bytes."""
        with map_httpx_transport_errors():
            yield from self._resp.iter_raw(chunk_size=chunk_size)


class HttpxAdapter:
    """Wrap a synchronous :class:`httpx.Client` as a :class:`TransportSession`."""

    __slots__ = ("_client",)

    def __init__(self, client: httpx.Client) -> None:
        self._client = client

    def opaque_progress_handle(self) -> httpx.Client:
        return self._client

    @property
    def client(self) -> httpx.Client:
        """Underlying httpx client (escape hatch for mounts, limits, …)."""
        return self._client

    @contextmanager
    def head(
        self,
        url: Url,
        *,
        headers: Mapping[str, str],
        options: TransportRequestOptions | None = None,
    ) -> Iterator[TransportResponse]:
        kwargs = _request_options_to_httpx_kwargs(options)
        with map_httpx_transport_errors():
            resp = self._client.head(str(url), headers=dict(headers), **kwargs)
        try:
            yield HttpxTransportResponse(resp)
        finally:
            resp.close()

    @contextmanager
    def stream_get(
        self,
        url: Url,
        *,
        headers: Mapping[str, str],
        options: TransportRequestOptions | None = None,
    ) -> Iterator[TransportResponse]:
        kwargs = _request_options_to_httpx_kwargs(options)
        with (
            map_httpx_transport_errors(),
            self._client.stream("GET", str(url), headers=dict(headers), **kwargs) as resp,
        ):
            yield HttpxTransportResponse(resp)


def httpx_transport(client: httpx.Client) -> TransportSession:
    """Shorthand: ``HttpxAdapter(client)``."""
    return HttpxAdapter(client)

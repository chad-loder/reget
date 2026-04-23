"""Unit tests for :mod:`reget.transport.requests_adapter`."""

from __future__ import annotations

from collections.abc import Iterator
from typing import cast
from unittest.mock import MagicMock

import pytest

pytest.importorskip("requests")
import requests
from requests.structures import CaseInsensitiveDict

from reget._types import Url
from reget.transport.errors import TransportHTTPError
from reget.transport.protocols import TransportResponse, TransportSession
from reget.transport.requests_adapter import (
    RequestsAdapter,
    RequestsTransportResponse,
    headers_from_requests_response,
    requests_transport,
)
from reget.transport.types import TransportRequestOptions


class _CMResponse:
    """Minimal object usable as ``with session.head(...) as resp``."""

    __slots__ = ("_iter_chunks", "headers", "status_code")

    def __init__(
        self,
        *,
        status_code: int = 200,
        headers: CaseInsensitiveDict[str] | dict[str, str] | None = None,
        chunks: tuple[bytes, ...] = (b"ab", b"c"),
    ) -> None:
        self.status_code = status_code
        if headers is not None:
            self.headers = headers if isinstance(headers, CaseInsensitiveDict) else CaseInsensitiveDict(headers)
        else:
            self.headers = CaseInsensitiveDict()
        self._iter_chunks = chunks

    def __enter__(self) -> _CMResponse:
        return self

    def __exit__(self, *args: object) -> None:
        return None

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise requests.HTTPError(response=MagicMock(status_code=self.status_code))

    def iter_content(self, chunk_size: int = 1) -> Iterator[bytes]:
        del chunk_size
        yield from self._iter_chunks


def test_headers_from_requests_response_normalizes() -> None:
    r = requests.Response()
    r.status_code = 200
    r.headers = CaseInsensitiveDict([("ETag", ' "x" '), ("Content-Length", "3")])
    h = headers_from_requests_response(r)
    assert h.get("etag") == '"x"'
    assert h.get("content-length") == "3"


def test_requests_transport_response_protocol() -> None:
    inner = _CMResponse(chunks=(b"a", b"bc"))
    tr: TransportResponse = RequestsTransportResponse(cast(requests.Response, inner))
    assert tr.status_code == 200
    assert list(tr.iter_raw_bytes(chunk_size=1024)) == [b"a", b"bc"]
    tr.raise_for_status()


def test_requests_transport_response_http_error_maps() -> None:
    inner = _CMResponse(status_code=503, chunks=())
    tr = RequestsTransportResponse(cast(requests.Response, inner))
    with pytest.raises(TransportHTTPError) as ctx:
        tr.raise_for_status()
    assert ctx.value.status_code == 503
    assert isinstance(ctx.value.__cause__, requests.HTTPError)


def test_requests_adapter_head_and_stream_get() -> None:
    head_resp = _CMResponse(status_code=200, headers=CaseInsensitiveDict([("Content-Length", "99")]))
    get_resp = _CMResponse(status_code=206, chunks=(b"z",))

    session = MagicMock(spec=requests.Session)
    session.head.return_value = head_resp
    session.get.return_value = get_resp

    adapter = RequestsAdapter(session)
    url = Url("https://example.test/file")

    with adapter.head(url, headers={"X-Req": "1"}, options=None) as resp:
        assert resp.status_code == 200
        assert resp.headers.get("content-length") == "99"

    session.head.assert_called_once_with(
        url,
        headers={"X-Req": "1"},
    )

    opts = TransportRequestOptions(timeout=30.0, verify=False, allow_redirects=True)
    with adapter.stream_get(url, headers={"Range": "bytes=0-1"}, options=opts) as resp:
        assert resp.status_code == 206
        assert list(resp.iter_raw_bytes(chunk_size=4096)) == [b"z"]

    session.get.assert_called_once_with(
        url,
        headers={"Range": "bytes=0-1"},
        stream=True,
        timeout=30.0,
        verify=False,
        allow_redirects=True,
    )


def test_requests_adapter_opaque_progress_handle() -> None:
    session = requests.Session()
    adapter = RequestsAdapter(session)
    assert adapter.opaque_progress_handle() is session


def test_requests_transport_factory() -> None:
    session = requests.Session()
    t: TransportSession = requests_transport(session)
    assert isinstance(t, RequestsAdapter)


def test_transport_package_exports_requests_symbols() -> None:
    import reget.transport as tr

    assert "HttpxAdapter" in tr.__all__
    assert "HttpxClient" in tr.__all__
    assert "NiquestsAdapter" in tr.__all__
    assert "httpx_transport" in tr.__all__
    assert "niquests_transport" in tr.__all__
    assert "RequestsAdapter" in tr.__all__
    assert "requests_transport" in tr.__all__
    assert "SupportedNativeHttpSession" in tr.__all__
    assert "AnySession" in tr.__all__
    assert "coerce_transport" in tr.__all__
    assert "wrap_transport" in tr.__all__
    assert hasattr(tr, "__getattr__")

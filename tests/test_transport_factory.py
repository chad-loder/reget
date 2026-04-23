"""Tests for :func:`reget.transport.factory.wrap_transport`."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from reget._types import RegetError
from reget.transport import coerce_transport, wrap_transport


def test_wrap_transport_rejects_unknown_type() -> None:
    with pytest.raises(RegetError, match="unsupported session type"):
        wrap_transport(MagicMock())


def test_wrap_transport_niquests_session() -> None:
    pytest.importorskip("niquests")
    import niquests

    from reget.transport.niquests_adapter import NiquestsAdapter

    s = niquests.Session()
    t = wrap_transport(s)
    assert isinstance(t, NiquestsAdapter)


def test_wrap_transport_httpx_client() -> None:
    pytest.importorskip("httpx")
    import httpx

    from reget.transport.httpx_adapter import HttpxAdapter

    c = httpx.Client()
    try:
        t = wrap_transport(c)
        assert isinstance(t, HttpxAdapter)
    finally:
        c.close()


def test_wrap_transport_requests_session() -> None:
    pytest.importorskip("requests")
    import requests

    from reget.transport.requests_adapter import RequestsAdapter

    s = requests.Session()
    t = wrap_transport(s)
    assert isinstance(t, RequestsAdapter)


def test_wrap_transport_exported_from_root() -> None:
    import reget

    assert reget.wrap_transport is wrap_transport


def test_coerce_transport_returns_adapter_unchanged() -> None:
    pytest.importorskip("niquests")
    import niquests

    from reget.transport.niquests_adapter import NiquestsAdapter

    s = niquests.Session()
    adapter = NiquestsAdapter(s)
    assert coerce_transport(adapter) is adapter


def test_coerce_transport_wraps_native_session() -> None:
    pytest.importorskip("niquests")
    import niquests

    from reget.transport.niquests_adapter import NiquestsAdapter

    s = niquests.Session()
    t = coerce_transport(s)
    assert isinstance(t, NiquestsAdapter)


def test_transport_session_runtime_checkable() -> None:
    pytest.importorskip("niquests")
    import niquests

    from reget.transport.niquests_adapter import NiquestsAdapter
    from reget.transport.protocols import TransportSession

    assert isinstance(NiquestsAdapter(niquests.Session()), TransportSession)
    assert not isinstance(niquests.Session(), TransportSession)

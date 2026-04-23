"""Construct :class:`~reget.transport.protocols.TransportSession` from native HTTP clients."""

from __future__ import annotations

import types

from reget._types import RegetError
from reget.transport.protocols import TransportSession
from reget.transport.session_input import AnySession, SupportedNativeHttpSession


def coerce_transport(transport: AnySession) -> TransportSession:
    """Return ``transport`` if already a :class:`TransportSession`, else wrap a native client."""
    if isinstance(transport, TransportSession):
        return transport
    return wrap_transport(transport)


def wrap_transport(session: SupportedNativeHttpSession) -> TransportSession:
    """Wrap a supported native session or client as a :class:`TransportSession`.

    Accepts ``niquests.Session``, ``httpx.Client``, or ``requests.Session`` when
    the matching optional extra is installed. The original exception is chained
    on :exc:`ImportError` from a missing adapter dependency.

    For a **default** niquests-backed transport (new session owned by the
    caller), use :func:`reget.downloader.fetch` with ``session=None`` instead.
    """
    httpx_mod: types.ModuleType | None
    try:
        import httpx as httpx_mod
    except ImportError:
        httpx_mod = None
    if httpx_mod is not None and isinstance(session, httpx_mod.Client):
        try:
            from reget.transport.httpx_adapter import HttpxAdapter
        except ImportError as e:
            msg = "httpx.Client requires httpx. Install with: pip install reget[httpx]"
            raise RegetError(msg) from e
        return HttpxAdapter(session)

    requests_mod: types.ModuleType | None
    try:
        import requests as requests_mod
    except ImportError:
        requests_mod = None
    if requests_mod is not None and isinstance(session, requests_mod.Session):
        try:
            from reget.transport.requests_adapter import RequestsAdapter
        except ImportError as e:
            msg = "requests.Session requires requests. Install with: pip install reget[requests]"
            raise RegetError(msg) from e
        return RequestsAdapter(session)

    try:
        import niquests
    except ImportError as e:
        msg = "niquests.Session requires niquests. Install with: pip install reget[niquests]"
        raise RegetError(msg) from e
    if isinstance(session, niquests.Session):
        from reget.transport.niquests_adapter import NiquestsAdapter

        return NiquestsAdapter(session)

    raise RegetError(f"unsupported session type {type(session)!r}")

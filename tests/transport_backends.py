"""Live HTTP transport matrix helpers for the test suite.

Each supported optional client (niquests, requests, httpx) gets the same
integration coverage against ``http.server`` via :class:`tests.conftest.HttpTest`.
"""

from __future__ import annotations

from reget.transport.protocols import TransportSession

LIVE_BACKENDS: tuple[str, ...] = ("niquests", "requests", "httpx")


def make_native(backend: str) -> object:
    """Construct a fresh native client for *backend* (caller owns lifecycle)."""
    if backend == "niquests":
        import niquests as nq

        return nq.Session()
    if backend == "requests":
        import requests as rq

        return rq.Session()
    if backend == "httpx":
        import httpx as hx

        return hx.Client()
    msg = f"unknown transport backend {backend!r}"
    raise ValueError(msg)


def make_transport(backend: str, native: object) -> TransportSession:
    """Wrap *native* in the matching :class:`TransportSession` adapter."""
    if backend == "niquests":
        from reget.transport.niquests_adapter import NiquestsAdapter

        return NiquestsAdapter(native)  # type: ignore[arg-type]
    if backend == "requests":
        from reget.transport.requests_adapter import RequestsAdapter

        return RequestsAdapter(native)  # type: ignore[arg-type]
    if backend == "httpx":
        from reget.transport.httpx_adapter import HttpxAdapter

        return HttpxAdapter(native)  # type: ignore[arg-type]
    msg = f"unknown transport backend {backend!r}"
    raise ValueError(msg)


def close_native(native: object) -> None:
    close = getattr(native, "close", None)
    if callable(close):
        close()

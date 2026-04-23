"""Pluggable HTTP transport: protocols, types, errors, and adapters."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from reget.transport.errors import (
    TransportConnectionError,
    TransportError,
    TransportHTTPError,
    TransportTLSError,
    TransportUnsupportedError,
)
from reget.transport.factory import coerce_transport, wrap_transport
from reget.transport.protocols import TransportResponse, TransportSession
from reget.transport.session_input import (
    AnySession,
    HttpxClient,
    NiquestsSession,
    RequestsLibrarySession,
    SupportedNativeHttpSession,
)
from reget.transport.types import TransportHeaders, TransportRequestOptions

if TYPE_CHECKING:
    from reget.transport.httpx_adapter import HttpxAdapter, httpx_transport
    from reget.transport.niquests_adapter import NiquestsAdapter, niquests_transport
    from reget.transport.requests_adapter import RequestsAdapter, requests_transport

__all__ = [
    "AnySession",
    "HttpxAdapter",
    "HttpxClient",
    "NiquestsAdapter",
    "NiquestsSession",
    "RequestsAdapter",
    "RequestsLibrarySession",
    "SupportedNativeHttpSession",
    "TransportConnectionError",
    "TransportError",
    "TransportHTTPError",
    "TransportHeaders",
    "TransportRequestOptions",
    "TransportResponse",
    "TransportSession",
    "TransportTLSError",
    "TransportUnsupportedError",
    "coerce_transport",
    "httpx_transport",
    "niquests_transport",
    "requests_transport",
    "wrap_transport",
]


def __getattr__(name: str) -> Any:
    if name == "HttpxAdapter":
        try:
            from reget.transport.httpx_adapter import HttpxAdapter as _HttpxAdapter
        except ImportError as e:
            msg = "HttpxAdapter requires httpx. Install with: pip install reget[httpx]"
            raise ImportError(msg) from e
        return _HttpxAdapter
    if name == "httpx_transport":
        try:
            from reget.transport.httpx_adapter import httpx_transport as _httpx_transport
        except ImportError as e:
            msg = "httpx_transport requires httpx. Install with: pip install reget[httpx]"
            raise ImportError(msg) from e
        return _httpx_transport
    if name == "NiquestsAdapter":
        try:
            from reget.transport.niquests_adapter import NiquestsAdapter as _NiquestsAdapter
        except ImportError as e:
            msg = "NiquestsAdapter requires niquests. Install with: pip install reget[niquests]"
            raise ImportError(msg) from e
        return _NiquestsAdapter
    if name == "niquests_transport":
        try:
            from reget.transport.niquests_adapter import niquests_transport as _niquests_transport
        except ImportError as e:
            msg = "niquests_transport requires niquests. Install with: pip install reget[niquests]"
            raise ImportError(msg) from e
        return _niquests_transport
    if name == "RequestsAdapter":
        try:
            from reget.transport.requests_adapter import RequestsAdapter as _RequestsAdapter
        except ImportError as e:
            msg = "RequestsAdapter requires requests. Install with: pip install reget[requests]"
            raise ImportError(msg) from e
        return _RequestsAdapter
    if name == "requests_transport":
        try:
            from reget.transport.requests_adapter import requests_transport as _requests_transport
        except ImportError as e:
            msg = "requests_transport requires requests. Install with: pip install reget[requests]"
            raise ImportError(msg) from e
        return _requests_transport
    msg = f"module {__name__!r} has no attribute {name!r}"
    raise AttributeError(msg)

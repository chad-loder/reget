"""Static typing aliases for HTTP client inputs (native stacks + :class:`TransportSession`)."""

# Use ``TypeAlias`` + quoted forward refs for optional third-party stacks (not PEP 695 ``type``).
# ruff: noqa: UP040

from typing import TYPE_CHECKING, TypeAlias, Union

from reget.transport.protocols import TransportSession

if TYPE_CHECKING:
    import httpx
    import niquests
    import requests

HttpxClient: TypeAlias = "httpx.Client"
NiquestsSession: TypeAlias = "niquests.Session"
RequestsLibrarySession: TypeAlias = "requests.Session"
SupportedNativeHttpSession: TypeAlias = Union[
    "niquests.Session",
    "requests.Session",
    "httpx.Client",
]
AnySession: TypeAlias = TransportSession | SupportedNativeHttpSession

__all__ = [
    "AnySession",
    "HttpxClient",
    "NiquestsSession",
    "RequestsLibrarySession",
    "SupportedNativeHttpSession",
]

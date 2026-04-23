"""Re-exports of native client type aliases (see :mod:`reget.transport.session_input`)."""

from reget.transport.session_input import (
    HttpxClient,
    NiquestsSession,
    RequestsLibrarySession,
    SupportedNativeHttpSession,
)

__all__ = [
    "HttpxClient",
    "NiquestsSession",
    "RequestsLibrarySession",
    "SupportedNativeHttpSession",
]

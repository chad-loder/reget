"""SHA parity across the live HTTP transport matrix.

Ported from ``tests.old/test_transport_sha_parity.py`` for the new
single-range engine (no piece_size, no PieceDownloader).
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from reget._types import DownloadComplete
from tests.conftest import deterministic

if TYPE_CHECKING:
    from tests.conftest import HttpTest

_PARITY_PAYLOAD = deterministic(64 * 1024, seed=42)


def test_live_transport_matrix_complete_download_sha(http: HttpTest) -> None:
    """Each installed backend yields the same SHA for the same bytes."""
    http.serve(_PARITY_PAYLOAD)
    result = http.fetch()
    assert isinstance(result, DownloadComplete)
    assert result.sha256 == _PARITY_PAYLOAD.sha256


def test_live_fresh_download_with_200_fallback_sha(http: HttpTest) -> None:
    """Server ignores Range, returns 200 full body. SHA must still match."""
    http.serve(_PARITY_PAYLOAD).force_200()
    result = http.fetch()
    assert isinstance(result, DownloadComplete)
    assert result.sha256 == _PARITY_PAYLOAD.sha256

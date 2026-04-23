"""SHA parity across the live HTTP transport matrix."""

from __future__ import annotations

from reget import DownloadComplete
from tests.conftest import HttpTest, deterministic

_PARITY_PAYLOAD = deterministic(64 * 1024, seed=42)


def test_live_transport_matrix_complete_download_sha(http: HttpTest) -> None:
    """Each installed backend (``http`` fixture) yields the same SHA for the same bytes."""
    http.serve(_PARITY_PAYLOAD)
    result = http.fetch(piece_size=8 * 1024)
    assert isinstance(result, DownloadComplete)
    assert result.sha256 == _PARITY_PAYLOAD.sha256

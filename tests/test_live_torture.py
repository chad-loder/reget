"""HTTP edge-case and pathology tests for the single-range download engine.

Ported from ``tests.old/test_torture.py``.  Tests are adapted from the
multi-piece / HEAD-based model to the new cursor-based single-range engine.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from reget._types import DownloadComplete, DownloadPartial
from reget.persist import Checkpoint, write_atomic
from reget.transport.errors import TransportError
from tests.conftest import Request, ServerState, deterministic

if TYPE_CHECKING:
    from tests.conftest import HttpTest

_ANY_TRANSPORT_OR_OS_ERROR = (TransportError, OSError, ConnectionError)


# ---------------------------------------------------------------------------
# Pathology 1: Mid-body connection reset on 206
# ---------------------------------------------------------------------------


def test_short_read_on_206_is_not_complete(http: HttpTest) -> None:
    """Server sends headers claiming full range, then closes the socket
    halfway through.  The download must not report DownloadComplete."""
    data = deterministic(4 * 1024, seed=3)
    http.serve(data).truncate_206_body_after(len(data) // 2)

    with pytest.raises(_ANY_TRANSPORT_OR_OS_ERROR):
        http.fetch()

    assert not http.dest.exists(), "dest must not exist after truncated 206"


# ---------------------------------------------------------------------------
# Pathology 2: 200 fallback that dies mid-stream
# ---------------------------------------------------------------------------


def test_200_truncated_mid_stream_is_not_complete(http: HttpTest) -> None:
    """Server returns 200 (Range ignored) then closes mid-body."""
    data = deterministic(8 * 1024, seed=5)
    http.serve(data).force_200().truncate_200_body_after(len(data) // 2)

    with pytest.raises(_ANY_TRANSPORT_OR_OS_ERROR):
        http.fetch()

    assert not http.dest.exists()


# ---------------------------------------------------------------------------
# Pathology 3: Resume after server file shrank → 416
# ---------------------------------------------------------------------------


def _write_synthetic_ctrl(http: HttpTest, *, valid_length: int, extent: int, etag: str = '"test-etag"') -> None:
    """Stage a .ctrl as if a prior fetch wrote ``valid_length`` bytes."""
    from reget._types import ETag, parse_url
    from reget.persist import CTRL_VERSION

    cp = Checkpoint(
        version=CTRL_VERSION,
        url=parse_url(http.url),
        start=0,
        extent=extent,
        valid_length=valid_length,
        etag=ETag(etag),
        resource_length=extent,
    )
    http.ctrl_path.parent.mkdir(parents=True, exist_ok=True)
    write_atomic(http.ctrl_path, cp)


def test_416_after_shrink_resets_and_recovers(http: HttpTest) -> None:
    """File shrinks between fetch() calls.  First resume gets 416 (range
    past EOF), engine resets checkpoint (DownloadPartial).  Second call
    starts fresh and succeeds."""
    original = deterministic(8 * 1024, seed=6)
    shrunk = deterministic(2 * 1024, seed=7)

    # Stage a partial download of the "original" content.
    http.serve(original)
    http.part_path.parent.mkdir(parents=True, exist_ok=True)
    http.part_path.write_bytes(original)
    _write_synthetic_ctrl(http, valid_length=len(original), extent=len(original))

    # Server now has the smaller file.
    http.serve(shrunk)

    r1 = http.fetch()
    assert isinstance(r1, DownloadPartial), "416 path should return DownloadPartial"

    # Second call starts fresh (checkpoint was reset).
    r2 = http.fetch()
    assert isinstance(r2, DownloadComplete)
    assert r2.sha256 == shrunk.sha256
    assert http.output == shrunk


# ---------------------------------------------------------------------------
# Pathology 4: Resume after ETag change → 200 restart
# ---------------------------------------------------------------------------


def test_resume_after_etag_change_restarts_from_zero(http: HttpTest) -> None:
    """First fetch partially completes.  Server changes content + ETag.
    Resume sends If-Range with the old ETag; server ignores Range and
    returns 200 with the new content.  Engine restarts from byte 0."""
    v1 = deterministic(4 * 1024, seed=10)
    v2 = deterministic(4 * 1024, seed=11)
    half = len(v1) // 2

    http.serve(v1).set_etag('"v1"')
    http.part_path.parent.mkdir(parents=True, exist_ok=True)
    http.part_path.write_bytes(v1[:half])
    _write_synthetic_ctrl(http, valid_length=half, extent=len(v1), etag='"v1"')

    # Server swaps to v2.  The before_each hook returns 200 (no Range
    # honour) whenever If-Range doesn't match, which the stdlib handler
    # does automatically for us when force_200 is set.
    def swap_to_v2(_req: Request, state: ServerState) -> None:
        state.content = v2
        state.etag = '"v2"'
        state.force_200_enabled = True

    http.before_each(swap_to_v2)

    result = http.fetch()
    assert isinstance(result, DownloadComplete)
    assert result.sha256 == v2.sha256
    assert http.output == v2


# ---------------------------------------------------------------------------
# Pathology 5: Resume without ETag → no If-Range sent
# ---------------------------------------------------------------------------


def test_resume_without_etag_still_works(http: HttpTest) -> None:
    """When the original download had no ETag, resume sends Range but
    omits If-Range.  The server honours the Range and returns 206."""
    data = deterministic(8 * 1024, seed=20)
    half = len(data) // 2

    http.serve(data).set_etag('""')

    # We can't use an empty ETag("") directly because our engine won't
    # emit If-Range when etag is empty. But the server sets ETag to '""'
    # which after parse_etag is also treated as no-etag? Let me use ''.
    # Actually, the test server sends whatever state.etag is.  We want
    # the ctrl to record an empty ETag so If-Range is skipped.
    http.serve(data).set_etag('"test-etag"')

    http.part_path.parent.mkdir(parents=True, exist_ok=True)
    http.part_path.write_bytes(data[:half])
    _write_synthetic_ctrl(http, valid_length=half, extent=len(data), etag="")

    result = http.fetch()
    assert isinstance(result, DownloadComplete)
    assert result.sha256 == data.sha256
    assert http.output == data


# ---------------------------------------------------------------------------
# Pathology 6: Clean fresh download via 206 (baseline / sanity)
# ---------------------------------------------------------------------------


def test_fresh_download_206(http: HttpTest) -> None:
    """Happy path: server supports Range, returns 206 for the full
    range, download completes in one call."""
    data = deterministic(16 * 1024, seed=30)
    http.serve(data)

    result = http.fetch()
    assert isinstance(result, DownloadComplete)
    assert result.sha256 == data.sha256
    assert http.output == data
    assert not http.part_path.exists(), ".part should be renamed to dest"
    assert not http.ctrl_path.exists(), ".ctrl should be cleaned up"


# ---------------------------------------------------------------------------
# Pathology 7: 206 with lie about total length
# ---------------------------------------------------------------------------


def test_206_total_length_lie_detected_on_resume(http: HttpTest) -> None:
    """Server lies about /TOTAL in Content-Range on a resume attempt.

    The engine trusts Content-Range for ``extent``.  A lying /TOTAL
    makes the engine expect more bytes than the server actually sends,
    so the stream ends short → DownloadPartial.  A second fetch without
    the lie recovers cleanly.
    """
    data = deterministic(4 * 1024, seed=40)
    half = len(data) // 2
    fake_total = len(data) * 2

    http.serve(data).set_etag('"v1"')

    http.part_path.parent.mkdir(parents=True, exist_ok=True)
    http.part_path.write_bytes(data[:half])
    _write_synthetic_ctrl(http, valid_length=half, extent=len(data), etag='"v1"')

    http.lie_about_total_length_on_206(fake_total)

    r1 = http.fetch()
    assert isinstance(r1, DownloadPartial), "lying /TOTAL should cause DownloadPartial (extent > actual bytes)"

    # Remove the lie; second fetch should complete cleanly.
    http._state.lie_206_total_length = None
    r2 = http.fetch()
    assert isinstance(r2, DownloadComplete)
    assert r2.sha256 == data.sha256

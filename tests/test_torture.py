"""HTTP edge-case and pathology tests for the download engine."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from reget._types import (
    EMPTY_ETAG,
    ContentRangeError,
    DownloadComplete,
    RegetError,
    ServerMisconfiguredError,
    parse_url,
)
from tests.conftest import Request, ServerState, deterministic

if TYPE_CHECKING:
    from tests.conftest import HttpTest


# ---------------------------------------------------------------------------
# Pathology 1: Naive CGI that honors Range start but not end
# ---------------------------------------------------------------------------
#
# Scenario: a homegrown CGI (or a middleware that rewrites responses)
# implements Range by doing ``f.seek(start); return f.read()`` — it
# ignores the client's end byte entirely and streams from start to
# EOF. ``Content-Range`` reports the actual bytes sent, e.g.
# ``bytes 0-16383/16384`` on a client that asked for ``bytes=0-1023``.
#
# The correct response is to reject the piece: the server did not honor
# our contract. ``_validate_content_range`` does this by strict prefix
# match on the ``bytes start-end/`` portion. We lock in that defense
# here — a regression that loosens the check to "endpoints inside the
# claimed range" would let these naive servers corrupt the file even
# though the write cap caps individual pieces.


def test_naive_cgi_that_oversends_past_range_is_rejected(http: HttpTest) -> None:
    """Server ignores Range end, sends start→EOF. The Content-Range
    doesn't match what we asked for, so reget must reject the piece."""
    data = deterministic(16 * 1024, seed=11)
    http.serve(data).ignore_range_end_on_206()

    with pytest.raises(ContentRangeError):
        http.fetch(piece_size=1024)


# ---------------------------------------------------------------------------
# Pathology 2: Cluster skew — HEAD and 206 served by different replicas
# ---------------------------------------------------------------------------
#
# Scenario: active-active CDN with eventual consistency. The HEAD lands
# on replica A (ETag "v1", content v1). The client pins ETag "v1". The
# subsequent range GETs are routed by a load balancer to replica B,
# which has ETag "v2" and content v2. Replica B doesn't honor If-Range
# (it's not in the cache key) and returns 206 with its *own* ETag in the
# header.
#
# A correct resume layer MUST detect that the 206's ETag differs from
# the one pinned at prepare() and abort the piece. Otherwise the final
# file is a Frankenstein stitch of v1 at offset 0 and v2 everywhere
# else, with a ``result.sha256`` that doesn't match either version.
#
# Today: reget does not validate the 206 ETag. The download "completes"
# with corrupted bytes.


def test_cluster_skew_between_head_and_206_is_detected(http: HttpTest) -> None:
    """HEAD returns etag v1 + content v1; all range GETs return etag v2
    + content v2. reget should detect the mismatch and abort rather
    than silently stitching two versions together."""
    v1 = deterministic(8 * 1024, seed=1)
    v2 = deterministic(8 * 1024, seed=2)

    def cluster_skew(req: Request, state: ServerState) -> None:
        if req.method == "HEAD":
            state.content = v1
            state.etag = '"v1"'
        else:
            state.content = v2
            state.etag = '"v2"'

    http.before_each(cluster_skew)

    with pytest.raises((ServerMisconfiguredError, ContentRangeError, RegetError)):
        http.fetch(piece_size=1024)


# ---------------------------------------------------------------------------
# Pathology 3: Mid-body connection reset
# ---------------------------------------------------------------------------
#
# Scenario: a reverse proxy (nginx upstream timeout, AWS ALB idle
# timeout, random TCP RST from flaky middleware) cuts the connection
# after delivering half of a 206 body. The server's response headers
# claimed Content-Length: piece_size; the client received ~piece_size/2
# bytes then EOF.
#
# A correct resume layer must NOT mark the piece complete on a short
# read — it must detect ``written < expected_length`` and re-queue the
# piece for retry.
#
# The defense here is primarily in the transport (niquests/urllib3 raises
# ``IncompleteRead`` / ``ProtocolError`` when EOF is reached before
# ``Content-Length`` bytes have been read). reget's contribution is the
# ``except OSError`` in ``_handle_206`` that releases the piece claim
# before propagating. If the transport ever stopped raising on short
# reads (e.g. a custom session without strict CL enforcement), reget's
# own code has no ``written == length`` check and would mark the piece
# complete with partial bytes. This test pins the end-to-end behavior;
# any regression either in transport or in reget's exception handling
# would flip it.


def test_short_read_on_206_is_not_marked_complete(http: HttpTest) -> None:
    """Server sends headers claiming the full range's worth of bytes,
    then closes the socket halfway through. The piece must not be
    marked complete with half the data."""
    data = deterministic(4 * 1024, seed=3)
    piece_size = 1024
    http.serve(data).truncate_206_body_after(piece_size // 2)

    with http.downloader(piece_size=piece_size) as pd:
        pd.prepare(http.transport)
        with pytest.raises((RegetError, OSError, ConnectionError)):
            for _ in range(10):
                pd.download_piece(http.transport)
                if pd.is_complete():
                    break

        if pd.is_complete():
            result = pd.finalize()
            assert isinstance(result, DownloadComplete)
            assert result.sha256 == data.sha256, (
                "reget marked a truncated piece as complete; final SHA "
                "matches neither the real content nor the truncated "
                "bytes — silent corruption"
            )


# ---------------------------------------------------------------------------
# Pathology 4: 206 Content-Range reports a fabricated /TOTAL
# ---------------------------------------------------------------------------
#
# Scenario: a caching proxy serves a 206 for byte range 0-1023 but
# reports ``Content-Range: bytes 0-1023/9999999`` — a total that
# disagrees with what the HEAD returned. Could be a stale cache entry,
# a misconfigured rewrite rule, or a replica with an inconsistent view
# of the object.
#
# A correct resume layer should detect the /TOTAL disagreement (the
# resource "changed size" between HEAD and GET) and raise, just like
# reget already does in the 416 path via ``is_file_changed``.
#
# Today: reget's ``_validate_content_range`` only checks the
# ``bytes start-end/`` prefix — the total after the slash is ignored.


def test_206_total_length_disagreement_with_head(http: HttpTest) -> None:
    """Server reports a fake /TOTAL in Content-Range. A resume layer
    that trusts HEAD's Content-Length should reject this — the resource
    appears to have changed size between HEAD and GET."""
    data = deterministic(4 * 1024, seed=4)
    fake_total = len(data) * 2
    http.serve(data).lie_about_total_length_on_206(fake_total)

    with pytest.raises((ContentRangeError, RegetError)):
        http.fetch(piece_size=1024)


# ---------------------------------------------------------------------------
# Pathology 5: NGINX-style Range-to-Full upgrade that dies mid-stream
# ---------------------------------------------------------------------------
#
# Scenario: a reverse proxy (default NGINX config, as documented by
# kevincox.ca/2021/06/04/http-range-caching/) strips the ``Range``
# header before forwarding and fetches the whole object from the
# origin. If the upstream dies midway through that fetch, the proxy
# delivers a 200 response with an accurate ``Content-Length`` claim
# but a truncated body.
#
# A correct resume layer must NOT leave the download stuck and must NOT
# report success. Either it retries cleanly or it raises. What it must
# never do is spin forever with some pieces perpetually un-claimable.


def test_sequential_fallback_truncated_mid_stream(http: HttpTest) -> None:
    """200 fallback delivers half the promised body, then EOFs. The
    download must not hang and must not claim success."""
    data = deterministic(8 * 1024, seed=5)
    http.serve(data).force_200().truncate_200_body_after(len(data) // 2)

    with pytest.raises((RegetError, OSError, ConnectionError)):
        http.fetch(piece_size=1024)


# ---------------------------------------------------------------------------
# Pathology 6: 416 returned for an in-bounds range (file shrank server-side)
# ---------------------------------------------------------------------------
#
# Scenario: HEAD pins the file at 8 KiB. Between HEAD and our first
# ranged GET, the origin truncates or replaces the file with a much
# smaller one. The 416 response carries ``Content-Range: bytes */NEW``.
#
# reget's ``_handle_416`` compares the new total against the pinned
# one and raises on mismatch. We lock that path in here — a regression
# that quietly returns ``False`` from ``_handle_416`` on size change
# would spin the download loop forever.


def test_416_for_range_past_eof_after_file_shrank_midrun(http: HttpTest) -> None:
    """File shrinks *between pieces* (after the first successful GET).
    Subsequent range request falls past EOF, server replies 416, reget
    must detect via _handle_416's total-length comparison and raise."""
    original = deterministic(8 * 1024, seed=6)
    # Shrink to exactly one piece so piece 0 (bytes 0-1023) succeeds, but
    # piece 1 (starts at byte 1024) trips the 416 path.
    shrunk = deterministic(1024, seed=7)

    def shrink_after_first_get(req: Request, state: ServerState) -> None:
        # req.index: 0=HEAD, 1=piece 0 GET, 2=piece 1 GET (shrink here)
        if req.index >= 2:
            state.content = shrunk
            state.etag = '"v2"'

    http.serve(original).before_each(shrink_after_first_get)

    with pytest.raises(RegetError):
        http.fetch(piece_size=1024)


# ---------------------------------------------------------------------------
# Pathology 7: Partial-fill cache — stale bytes served with matching ETag
# ---------------------------------------------------------------------------
#
# Scenario (kevincox.ca): a proxy's cache has a "Partial Fill" for
# ``bytes=1024-2047``. The upstream resource has since changed, but
# the proxy's cache metadata (including ETag) is still ``"v1"``.
# When the client requests ``bytes=0-1023``, the proxy fetches just
# those bytes from the origin and serves them alongside its stale
# ``bytes=1024-2047`` tail, all with ETag ``"v1"``.
#
# There is no HTTP-level defense against this without a server-provided
# content digest: both the ETag and the Content-Range look clean; only
# a cross-range integrity check could expose the corruption.
#
# This test documents the gap explicitly. If we ever add a defense
# (e.g. honoring ``Digest`` / ``Repr-Digest`` headers, or requiring a
# user-supplied expected SHA), this test becomes an xfail we can flip.
# Today it asserts the silent-corruption behavior so regressions in
# _handle_206 don't silently change the failure mode.


# ---------------------------------------------------------------------------
# Pathology 7a: Fallocate-vs-shrink interaction
# ---------------------------------------------------------------------------
#
# ``prepare()`` calls ``posix_fallocate`` to physically reserve
# ``total_length`` bytes on disk (falling back to ``ftruncate`` sparse
# file). This is fine when the server's view of the file is stable, but
# it creates two concerns when the server's file shrinks mid-run:
#
# 1. The ``.part`` is now LARGER than the current server file. If
#    reget ever renamed it to the destination, the caller would get a
#    file zero-padded past the server's real EOF — a silent corruption
#    made worse by the fact that ``len(file)`` would look plausible if
#    the caller was resuming a multi-gigabyte download.
#
# 2. Pieces we already successfully wrote (from the v1 content) now
#    live on disk at offsets that may be beyond the v2 content's EOF.
#    Those bytes must not survive a retry.
#
# These tests pin both invariants.


def test_mid_run_shrink_leaves_part_oversized_with_zero_padded_tail(
    http: HttpTest,
) -> None:
    """When the server shrinks after a piece has been written, reget
    raises and leaves the ``.part`` on disk at its original fallocated
    size. The destination file must not exist (no silent success), and
    the tail past the new EOF must be zeros (fallocate-reserved, not
    v1 leftovers) so a fresh download doesn't pick up stale bytes."""
    original = deterministic(8 * 1024, seed=20)
    shrunk = deterministic(1024, seed=21)

    def shrink_after_first_get(req: Request, state: ServerState) -> None:
        if req.index >= 2:  # HEAD=0, piece 0=1, piece 1=2 (shrink here)
            state.content = shrunk
            state.etag = '"v2"'

    http.serve(original).before_each(shrink_after_first_get)

    with pytest.raises(RegetError):
        http.fetch(piece_size=1024)

    # dest was never created — no one should mistake this for success
    assert not http.dest.exists()
    # .part survives for restart. Note: .ctrl may or may not exist,
    # depending on whether the flush threshold was crossed — reget
    # flushes every N pieces (default 5), not every piece, so a shrink
    # detected before N pieces complete leaves an un-ctrl'd .part.
    assert http.part_path.exists()
    # The fallocated allocation is NOT retroactively resized; the .part
    # is still 8 KiB even though the server thinks the file is 1 KiB.
    assert http.part_path.stat().st_size == len(original)
    # Bytes past the new server EOF are fallocate zeros, not v1 leftovers.
    on_disk = http.part_path.read_bytes()
    assert on_disk[: len(shrunk)] == original[: len(shrunk)]  # piece 0 is v1
    assert on_disk[len(shrunk) :] == b"\x00" * (len(original) - len(shrunk))


def test_fresh_retry_after_shrink_discards_old_allocation(http: HttpTest) -> None:
    """After a mid-run shrink raises, a fresh ``PieceDownloader`` must
    produce a destination file sized to the current server file — not
    the old fallocated size with a zero-padded tail.

    ``allocate_file`` now ftruncates down before ``posix_fallocate``
    when the existing file is already larger than the new
    ``total_length``, so this works correctly on both Linux and macOS.
    See ``tests/test_alloc.py::test_allocate_file_shrinks_oversized_file_under_linux_fallocate``
    for the platform-forced version of this invariant.
    """
    original = deterministic(8 * 1024, seed=22)
    shrunk = deterministic(1024, seed=23)

    def shrink_after_first_get(req: Request, state: ServerState) -> None:
        if req.index >= 2:
            state.content = shrunk
            state.etag = '"v2"'

    http.serve(original).before_each(shrink_after_first_get)

    with pytest.raises(RegetError):
        http.fetch(piece_size=1024)

    http.before_each(lambda _req, _state: None)

    result = http.fetch(piece_size=1024)

    assert http.output == shrunk
    assert isinstance(result, DownloadComplete)
    assert result.sha256 == shrunk.sha256
    assert http.dest.stat().st_size == len(shrunk)


def test_resume_after_shrink_without_etag_recovers(http: HttpTest) -> None:
    """Some origins serve no ``ETag`` at all. Without ETag, the resume
    path can't use ETag equality to detect a stale control file. reget
    must still detect the size change and discard the stale ``.ctrl``
    + ``.part``.

    Scenario: the first attempt completes a few pieces and flushes a
    ``.ctrl``. Between attempts the server's file shrinks (no ETag is
    sent on either the original HEAD or the resume HEAD). A second
    ``PieceDownloader.prepare()`` must cross-check ``total_length``
    and start fresh."""
    original = deterministic(10 * 1024, seed=30)
    shrunk = deterministic(2 * 1024, seed=31)

    # First run: serve ``original`` with no ETag. Tiny piece size so we
    # cross the flush threshold (5 pieces) and persist a .ctrl before
    # the test drives the download to completion.
    http.serve(original).set_etag("")
    result1 = http.fetch(piece_size=1024)
    assert isinstance(result1, DownloadComplete)
    assert result1.sha256 == original.sha256
    # A successful fetch deletes .ctrl on finalize — simulate crash-
    # before-finalize by re-staging both files from the known-good
    # bytes we just got, then shrinking the server.
    http.part_path.write_bytes(http.dest.read_bytes())
    http.dest.unlink()
    # Replay a ctrl file from a tracker that matches what would have
    # been flushed mid-download.
    _write_ctrl_for_completed(http, original_len=len(original), piece_size=1024)
    assert http.ctrl_path.exists() and http.part_path.exists()

    # Second run: server now has a smaller file, still no ETag.
    http.serve(shrunk).set_etag("")

    result2 = http.fetch(piece_size=1024)

    assert http.output == shrunk
    assert isinstance(result2, DownloadComplete)
    assert result2.sha256 == shrunk.sha256
    assert http.dest.stat().st_size == len(shrunk)


def _write_ctrl_for_completed(http: HttpTest, *, original_len: int, piece_size: int) -> None:
    """Write a synthetic ``.part.ctrl`` as if a previous attempt had
    flushed mid-run. Used to simulate the "crashed before finalize"
    state that resume must recover from."""
    from bitarray import bitarray

    from reget.control import ControlMeta, serialize

    num_pieces = (original_len + piece_size - 1) // piece_size
    done = bitarray(num_pieces)
    done.setall(True)
    meta = ControlMeta(
        version=1,
        url=parse_url(http.url),
        piece_size=piece_size,
        total_length=original_len,
        etag=EMPTY_ETAG,
        alloc_outcome="reserved",
        alloc_mechanism="posix_fallocate",
        alloc_previous_size=0,
    )
    http.ctrl_path.write_bytes(serialize(meta, done))


def test_partial_fill_cache_corruption_is_undetectable_without_digest(
    http: HttpTest,
) -> None:
    """Two versions, one stale ETag. reget accepts the mixed response
    because HTTP framing alone cannot distinguish it from a valid one.
    Documented limitation; caller-level SHA verification is the fix."""
    v1 = deterministic(4 * 1024, seed=10)
    v2 = deterministic(4 * 1024, seed=11)

    def frankenstein(req: Request, state: ServerState) -> None:
        # Even-indexed pieces served from v1's cache, odd from v2's
        # (or vice versa) — every response carries the same ETag so
        # our 206 ETag check is satisfied.
        if req.method == "GET" and req.range is not None:
            state.content = v1 if req.range[0] // 1024 % 2 == 0 else v2

    http.serve(v1).before_each(frankenstein)

    result = http.fetch(piece_size=1024)

    # Download "succeeds" — neither reget nor the HTTP framing has
    # signal to reject this. The resulting SHA matches neither v1 nor v2.
    assert isinstance(result, DownloadComplete)
    assert result.sha256 not in {v1.sha256, v2.sha256}

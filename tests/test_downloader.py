"""Tests for the download engine: piece-mode, sequential fallback,
416 handling, Content-Encoding abort, write cap, and flush-on-N.

All tests use a mock HTTP server (``http.server`` on localhost) so there
are no real network dependencies.
"""

from __future__ import annotations

import errno
import threading
import time
from pathlib import Path
from typing import TYPE_CHECKING

import pytest

from reget._types import (
    ContentRangeError,
    DownloadComplete,
    DownloadStatus,
    RegetError,
    ServerMisconfiguredError,
)
from reget.downloader import PieceDownloader, _publish_part
from reget.transport import NiquestsAdapter, TransportSession
from tests.conftest import deterministic, repeated

if TYPE_CHECKING:
    from tests.conftest import HttpTest


def test_piece_mode_small_file(http: HttpTest) -> None:
    data = deterministic(10240)  # 10 pieces at piece_size=1024
    http.serve(data)
    result = http.fetch(piece_size=1024)
    assert result.status == DownloadStatus.COMPLETE
    assert result.pieces_completed == 10
    assert result.pieces_total == 10
    assert http.output == data
    assert result.sha256 == data.sha256
    assert result.bytes_written == len(data)


def test_piece_mode_non_aligned(http: HttpTest) -> None:
    data = repeated(b"A", 2500)
    http.serve(data)
    result = http.fetch(piece_size=1024)
    assert result.status == DownloadStatus.COMPLETE
    assert result.pieces_completed == 3  # ceil(2500/1024)
    assert http.output == data
    assert result.sha256 == data.sha256


def test_piece_mode_creates_ctrl_file_during_download(http: HttpTest) -> None:
    data = repeated(b"X", 1024 * 20)
    http.serve(data)

    with http.downloader(piece_size=1024, flush_every=3) as pd:
        pd.prepare(http.transport)
        pieces_done = 0
        while not pd.is_complete():
            pd.download_piece(http.transport)
            pieces_done += 1
            if pieces_done == 5:
                assert http.ctrl_path.exists(), "ctrl file should be flushed by now"

        result = pd.finalize()

    assert result.status == DownloadStatus.COMPLETE
    assert not http.ctrl_path.exists(), "ctrl should be cleaned up after finalize"
    assert http.output == data
    assert result.sha256 == data.sha256


def test_write_cap_at_piece_boundary(http: HttpTest) -> None:
    data = repeated(b"D", 4096)
    http.serve(data)
    result = http.fetch(piece_size=1024)
    assert result.status == DownloadStatus.COMPLETE
    assert http.output == data
    assert len(http.output) == 4096
    assert result.sha256 == data.sha256


def test_sequential_fallback_on_200(http: HttpTest) -> None:
    data = repeated(b"S", 5000)
    http.serve(data).force_200()
    result = http.fetch(piece_size=1024)
    assert result.status == DownloadStatus.COMPLETE
    assert http.output == data
    assert result.sha256 == data.sha256


def test_content_encoding_on_206_aborts(http: HttpTest) -> None:
    http.serve(repeated(b"G", 2048)).inject_content_encoding_on_206()
    with pytest.raises(ServerMisconfiguredError, match=r"Content-Encoding"):
        http.fetch(piece_size=1024)


def test_416_when_range_overshoots(http: HttpTest) -> None:
    data = repeated(b"T", 512)
    http.serve(data)
    result = http.fetch(piece_size=1024)
    assert result.status == DownloadStatus.COMPLETE
    assert http.output == data
    assert result.sha256 == data.sha256


def test_resume_after_interrupt(http: HttpTest) -> None:
    data = repeated(b"R", 1024 * 10)
    http.serve(data)

    with http.downloader(piece_size=1024, flush_every=1) as pd:
        pd.prepare(http.transport)
        for _ in range(5):
            pd.download_piece(http.transport)

    assert http.part_path.exists()
    assert http.ctrl_path.exists()

    result = http.fetch(piece_size=1024)
    assert result.status == DownloadStatus.COMPLETE
    assert http.output == data
    assert result.sha256 == data.sha256


def test_context_manager_closes_session(http: HttpTest) -> None:
    data = repeated(b"C", 1024)
    http.serve(data)
    with http.downloader(piece_size=1024) as pd:
        pd.prepare(http.transport)
        while not pd.is_complete():
            pd.download_piece(http.transport)
        result = pd.finalize()
    assert result.status == DownloadStatus.COMPLETE
    assert http.output == data
    assert result.sha256 == data.sha256


def test_sha256_matches_content(http: HttpTest) -> None:
    """Streaming sha256 matches an independent hash of the served body."""
    data = repeated(b"H", 3000)
    http.serve(data)
    result = http.fetch(piece_size=1024)
    assert isinstance(result, DownloadComplete)
    assert result.sha256 == data.sha256


# ---------------------------------------------------------------------------
# HTTP validation (negative tests)
# ---------------------------------------------------------------------------


def test_head_missing_content_length_raises(http: HttpTest) -> None:
    """Server must report Content-Length; chunked-only responses are unsupported."""
    http.serve(repeated(b"X", 4096)).omit_content_length_on_head()

    with http.downloader(piece_size=1024) as pd, pytest.raises(RegetError, match="Content-Length"):
        pd.prepare(http.transport)


def test_206_missing_content_range_raises(http: HttpTest) -> None:
    """A 206 without Content-Range is a protocol violation — abort the piece."""
    http.serve(repeated(b"Y", 4096)).omit_content_range_on_206()

    with http.downloader(piece_size=1024) as pd:
        pd.prepare(http.transport)
        with pytest.raises(ContentRangeError):
            pd.download_piece(http.transport)


def test_206_wrong_content_range_raises(http: HttpTest) -> None:
    """Mismatched Content-Range means the server isn't honoring our request."""
    http.serve(repeated(b"Z", 4096)).lie_content_range_on_206()

    with http.downloader(piece_size=1024) as pd:
        pd.prepare(http.transport)
        with pytest.raises(ContentRangeError):
            pd.download_piece(http.transport)


def test_206_missing_range_releases_piece_for_retry(http: HttpTest) -> None:
    """A bad Content-Range must release the in-flight piece (no leaked claim)."""
    http.serve(repeated(b"Q", 4096)).omit_content_range_on_206()

    with http.downloader(piece_size=1024) as pd:
        pd.prepare(http.transport)
        with pytest.raises(ContentRangeError):
            pd.download_piece(http.transport)
        tracker = pd.tracker
        assert tracker is not None
        # No piece should still be in-flight — the failed claim was released.
        done, total = tracker.progress()
        assert done == 0
        assert total == 4


# ---------------------------------------------------------------------------
# Parallel piece download — the core multi-threaded use case
# ---------------------------------------------------------------------------


def _run_workers(pd: PieceDownloader, n_workers: int, transport: TransportSession) -> list[BaseException]:
    """Drive ``download_piece()`` from ``n_workers`` threads until complete.

    Collects any exceptions rather than letting them vanish into thread-
    local state.
    """
    errors: list[BaseException] = []
    err_lock = threading.Lock()
    stop = threading.Event()

    def worker() -> None:
        try:
            while not stop.is_set() and not pd.is_complete():
                progressed = pd.download_piece(transport)
                if not progressed:
                    # Either all claimed by peers, or we hit an abort flag.
                    time.sleep(0.001)
        except BaseException as exc:  # noqa: BLE001 — surface to main
            with err_lock:
                errors.append(exc)
            stop.set()

    threads = [threading.Thread(target=worker, daemon=True) for _ in range(n_workers)]
    for th in threads:
        th.start()
    for th in threads:
        th.join(timeout=30.0)
        if th.is_alive():
            stop.set()
            errors.append(RuntimeError("worker thread did not terminate within 30s"))
    return errors


def test_parallel_pieces_correctness(http: HttpTest) -> None:
    """N threads claim disjoint pieces; final bytes + SHA match exactly."""
    data = deterministic(128 * 1024)
    http.serve(data)

    with http.downloader(piece_size=1024) as pd:
        pd.prepare(http.transport)
        errors = _run_workers(pd, n_workers=8, transport=http.transport)
        assert errors == []
        assert pd.is_complete()
        result = pd.finalize()

    assert result.status == DownloadStatus.COMPLETE
    assert result.pieces_completed == 128
    assert http.output == data
    assert result.sha256 == data.sha256


def test_parallel_actually_concurrent(http: HttpTest) -> None:
    """Confirm workers really run in parallel (peak concurrency > 1).

    Without a threading HTTP server, the handler serializes requests and
    this assertion catches regressions in the fixture.
    """
    data = repeated(b"C", 64 * 1024)
    http.serve(data).delay_gets(0.02)  # 20 ms per request

    with http.downloader(piece_size=1024) as pd:
        pd.prepare(http.transport)
        errors = _run_workers(pd, n_workers=6, transport=http.transport)
        assert errors == []
        result = pd.finalize()

    assert http.peak_concurrent_gets >= 2, f"expected parallel GETs, saw peak={http.peak_concurrent_gets}"
    assert http.output == data
    assert isinstance(result, DownloadComplete)
    assert result.sha256 == data.sha256


def test_parallel_with_frequent_flushes(http: HttpTest) -> None:
    """Concurrent completes + ctrl flushes never produce a corrupt sidecar."""
    data = deterministic(64 * 1024, seed=101)
    http.serve(data)

    with http.downloader(piece_size=1024, flush_every=1) as pd:
        pd.prepare(http.transport)
        errors = _run_workers(pd, n_workers=8, transport=http.transport)
        assert errors == []
        if http.ctrl_path.exists():
            parsed = http.read_ctrl()
            assert parsed.meta.total_length == len(data)
            expected_pieces = -(-parsed.meta.total_length // parsed.meta.piece_size)
            assert 0 <= parsed.done.count() <= expected_pieces

        result = pd.finalize()

    assert result.status == DownloadStatus.COMPLETE
    assert http.output == data
    assert result.sha256 == data.sha256
    assert not http.ctrl_path.exists()


def test_parallel_swarm_collapse(http: HttpTest) -> None:
    """When the server force-200s, one worker streams; others back off cleanly."""
    data = deterministic(64 * 1024, seed=7)
    http.serve(data).force_200()

    with http.downloader(piece_size=1024) as pd:
        pd.prepare(http.transport)
        errors = _run_workers(pd, n_workers=6, transport=http.transport)
        assert errors == [], f"worker errors: {errors}"
        assert pd.is_complete()
        result = pd.finalize()

    assert result.status == DownloadStatus.COMPLETE
    assert http.output == data
    assert result.sha256 == data.sha256


def test_parallel_aborts_on_first_server_misconfig(http: HttpTest) -> None:
    """If the server injects Content-Encoding on 206, the first worker
    raises ServerMisconfiguredError and the others exit (via the tracker
    abort flag) without deadlocking."""
    http.serve(repeated(b"M", 16 * 1024)).inject_content_encoding_on_206()

    with http.downloader(piece_size=1024) as pd:
        pd.prepare(http.transport)

        errors: list[BaseException] = []
        lock = threading.Lock()

        def worker() -> None:
            try:
                while not pd.is_complete():
                    try:
                        pd.download_piece(http.transport)
                    except ServerMisconfiguredError as exc:
                        with lock:
                            errors.append(exc)
                        # Signal peers to stop by flipping the abort flag.
                        tracker = pd.tracker
                        assert tracker is not None
                        tracker.enter_sequential_mode(threading.get_ident())
                        return
            except BaseException as exc:  # noqa: BLE001
                with lock:
                    errors.append(exc)

        threads = [threading.Thread(target=worker, daemon=True) for _ in range(4)]
        for th in threads:
            th.start()
        for th in threads:
            th.join(timeout=10.0)
            assert not th.is_alive(), "worker did not terminate"

    assert any(isinstance(e, ServerMisconfiguredError) for e in errors)


def test_parallel_same_session_is_reused(http_niquests: HttpTest) -> None:
    """All worker threads share one ``niquests.Session`` — this exercises
    the session's internal connection pool under concurrent access."""
    import niquests

    data = repeated(b"S", 32 * 1024)
    http_niquests.serve(data)

    session = niquests.Session()
    transport = NiquestsAdapter(session)
    try:
        with http_niquests.downloader(piece_size=1024) as pd:
            pd.prepare(transport)
            errors = _run_workers(pd, n_workers=8, transport=transport)
            assert errors == []
            result = pd.finalize()
    finally:
        session.close()

    assert result.status == DownloadStatus.COMPLETE
    assert http_niquests.output == data
    assert result.sha256 == data.sha256


def test_parallel_resume_after_partial(http: HttpTest) -> None:
    """Parallel workers, stop halfway, resume with parallel workers again."""
    data = deterministic(96 * 1024, seed=17)
    http.serve(data)

    # First pass: claim ~half the pieces then stop.
    target_pieces = 48
    with http.downloader(piece_size=1024, flush_every=4) as pd:
        pd.prepare(http.transport)
        tracker = pd.tracker
        assert tracker is not None

        done_event = threading.Event()

        def worker() -> None:
            while not done_event.is_set():
                done, _total = tracker.progress()
                if done >= target_pieces:
                    done_event.set()
                    return
                if not pd.download_piece(http.transport):
                    return

        threads = [threading.Thread(target=worker, daemon=True) for _ in range(4)]
        for th in threads:
            th.start()
        for th in threads:
            th.join(timeout=10.0)

        tracker.flush_state(force=True)
        done_pieces, _ = tracker.progress()
        assert done_pieces >= target_pieces

    assert http.part_path.exists()
    assert http.ctrl_path.exists()

    # Second pass: resume and finish in parallel.
    with http.downloader(piece_size=1024) as pd2:
        pd2.prepare(http.transport)
        tracker2 = pd2.tracker
        assert tracker2 is not None
        resumed_done, _ = tracker2.progress()
        assert resumed_done >= target_pieces, f"resume should preserve progress; got {resumed_done}"

        errors = _run_workers(pd2, n_workers=6, transport=http.transport)
        assert errors == []
        result = pd2.finalize()

    assert result.status == DownloadStatus.COMPLETE
    assert http.output == data
    assert result.sha256 == data.sha256


# ---------------------------------------------------------------------------
# File-I/O error handling — finalize() and _publish_part()
# ---------------------------------------------------------------------------


class TestPublishPart:
    """Direct tests for ``_publish_part`` — the atomic part → dest publisher.

    These exercise both the POSIX-atomic rename path and the ``EXDEV``
    fallback without needing a real cross-filesystem setup (which would
    require root / a mounted tmpfs in CI).
    """

    def test_same_filesystem_uses_atomic_rename(self, tmp_path: Path) -> None:
        part = tmp_path / "f.bin.part"
        dest = tmp_path / "f.bin"
        part.write_bytes(b"payload")

        _publish_part(part, dest)

        assert dest.read_bytes() == b"payload"
        assert not part.exists()

    def test_cross_device_falls_back_to_copy_and_rename(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        part = tmp_path / "f.bin.part"
        dest = tmp_path / "f.bin"
        part.write_bytes(b"cross-device payload")

        orig_replace = Path.replace
        calls: list[tuple[Path, Path]] = []

        def fake_replace(self: Path, target: str | Path) -> Path:
            target_path = Path(target)
            calls.append((self, target_path))
            if len(calls) == 1 and self == part and target_path == dest:
                raise OSError(errno.EXDEV, "Invalid cross-device link")
            return orig_replace(self, target)

        monkeypatch.setattr(Path, "replace", fake_replace)

        _publish_part(part, dest)

        assert dest.read_bytes() == b"cross-device payload"
        assert not part.exists(), "source .part must be unlinked after copy fallback"
        assert not (tmp_path / "f.bin.tmp").exists()

    def test_non_exdev_oserror_is_re_raised(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        """EACCES (or any non-EXDEV errno) must propagate — falling back to
        copy there would mask a real permission problem the caller needs."""
        part = tmp_path / "f.bin.part"
        dest = tmp_path / "f.bin"
        part.write_bytes(b"data")

        def fake_replace(self: Path, target: str | Path) -> Path:
            raise PermissionError(errno.EACCES, "nope")

        monkeypatch.setattr(Path, "replace", fake_replace)

        with pytest.raises(PermissionError):
            _publish_part(part, dest)

    def test_copy_failure_in_fallback_cleans_up_staging(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        """If copyfile raises mid-fallback, the staging .tmp must not
        linger on disk."""
        import shutil

        part = tmp_path / "f.bin.part"
        dest = tmp_path / "f.bin"
        part.write_bytes(b"payload")

        def fake_replace(self: Path, target: str | Path) -> Path:
            raise OSError(errno.EXDEV, "cross-device")

        def boom_copyfile(src: str, dst: str, *, follow_symlinks: bool = True) -> str:
            Path(dst).write_bytes(b"partial")
            raise OSError(errno.ENOSPC, "disk full during copy")

        monkeypatch.setattr(Path, "replace", fake_replace)
        monkeypatch.setattr(shutil, "copyfile", boom_copyfile)

        with pytest.raises(OSError, match="disk full"):
            _publish_part(part, dest)

        assert not (tmp_path / "f.bin.tmp").exists(), "staging file must be cleaned up"
        assert part.exists(), "source .part must still exist when publish fails"
        assert not dest.exists(), "dest must not be created on copy failure"


def test_finalize_swallows_post_rename_ctrl_unlink_failure(http: HttpTest, monkeypatch: pytest.MonkeyPatch) -> None:
    """If the ctrl file can't be unlinked *after* a successful rename, the
    download is already done — we must not turn a user-visible success
    into a reported failure.

    Regression for H2 from the file-I/O audit: previously the OSError
    from ``ctrl_path.unlink(missing_ok=True)`` propagated, causing the
    caller to believe the download had failed when the bytes were
    already at their final destination.
    """
    data = deterministic(4096)
    http.serve(data)

    orig_unlink = Path.unlink
    raised_after: dict[str, bool] = {"rename": False}

    def fake_unlink(self: Path, missing_ok: bool = False) -> None:
        # Only fail the finalize-time ctrl unlink (after the rename),
        # not the prepare-time stale cleanup.  We detect "after rename"
        # by checking that the .part file is gone (it was replaced).
        if self.suffix == ".ctrl" and not http.part_path.exists():
            raised_after["rename"] = True
            raise PermissionError(errno.EACCES, "ctrl unlink denied")
        return orig_unlink(self, missing_ok=missing_ok)

    monkeypatch.setattr(Path, "unlink", fake_unlink)

    result = http.fetch(piece_size=1024)

    assert raised_after["rename"], "test precondition: unlink was intercepted post-rename"
    assert result.status == DownloadStatus.COMPLETE
    assert http.output == data
    assert result.sha256 == data.sha256
    assert http.dest.exists()


def test_finalize_cross_device_publish_end_to_end(http: HttpTest, monkeypatch: pytest.MonkeyPatch) -> None:
    """Full download where the final rename hits EXDEV. The download must
    still complete and land at ``dest`` via the copy+rename fallback.

    Regression for H1: previously ``EXDEV`` surfaced as a bare
    ``OSError`` after every byte had already been written successfully.
    """
    data = deterministic(8192, seed=42)
    http.serve(data)

    orig_replace = Path.replace
    exdev_fired = {"fired": False}

    def fake_replace(self: Path, target: str | Path) -> Path:
        if self == http.part_path and Path(target) == http.dest and not exdev_fired["fired"]:
            exdev_fired["fired"] = True
            raise OSError(errno.EXDEV, "Invalid cross-device link")
        return orig_replace(self, target)

    monkeypatch.setattr(Path, "replace", fake_replace)

    result = http.fetch(piece_size=1024)

    assert exdev_fired["fired"], "test precondition: EXDEV was injected"
    assert result.status == DownloadStatus.COMPLETE
    assert http.output == data
    assert result.sha256 == data.sha256
    assert not http.part_path.exists(), "source .part must be cleaned up"

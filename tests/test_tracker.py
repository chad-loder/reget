"""Tests for PieceTracker.

Covers bitfield operations, thread safety, swarm collapse on 200
fallback, control-file persistence round-trips, and the flush
deduplication + durability-pairing invariants.
"""

from __future__ import annotations

import threading
from pathlib import Path

import pytest

from reget._types import EMPTY_ETAG, PieceIndex, parse_etag, parse_url
from reget.alloc import allocation_reserved_posix, allocation_sparse
from reget.control import ControlFileError
from reget.tracker import PieceTracker

# -----------------------------------------------------------------------
# Basic bitfield operations
# -----------------------------------------------------------------------


def test_claim_yields_distinct_pieces() -> None:
    t = PieceTracker(total_length=10 * 1024, piece_size=1024)
    seen: set[int] = set()
    while (c := t.claim()) is not None:
        idx, offset, length = c
        assert idx not in seen
        seen.add(idx)
        assert offset == idx * 1024
        assert length == 1024
    assert seen == set(range(10))


def test_tail_piece_is_truncated() -> None:
    t = PieceTracker(total_length=1024 + 100, piece_size=1024)
    first = t.claim()
    second = t.claim()
    assert first == (0, 0, 1024)
    assert second == (1, 1024, 100)


def test_complete_release_cycle() -> None:
    t = PieceTracker(total_length=3 * 1024, piece_size=1024)
    c0 = t.claim()
    assert c0 is not None
    idx0 = c0[0]
    t.release(idx0)
    again = t.claim()
    assert again is not None
    assert again[0] == idx0


def test_is_complete_after_all_completed() -> None:
    t = PieceTracker(total_length=4 * 1024, piece_size=1024)
    for _ in range(4):
        c = t.claim()
        assert c is not None
        t.complete(c[0])
    assert t.is_complete()
    assert t.claim() is None


def test_rejects_bad_inputs() -> None:
    with pytest.raises(ValueError):
        PieceTracker(total_length=-1, piece_size=1024)
    with pytest.raises(ValueError):
        PieceTracker(total_length=1024, piece_size=0)


def test_rejects_allocation_and_preset_together() -> None:
    with pytest.raises(ValueError, match="only one of allocation and alloc_preset"):
        PieceTracker(
            total_length=1024,
            piece_size=256,
            allocation=allocation_sparse(1024),
            alloc_preset="sparse",
        )


def test_alloc_preset_previous_size_persists_in_control_file(tmp_path: Path) -> None:
    ctrl = tmp_path / "x.part.ctrl"
    t = PieceTracker(
        total_length=1024,
        piece_size=512,
        alloc_preset="reserved_posix",
        alloc_previous_size=8192,
    )
    t.flush_state(ctrl, force=True)
    restored = PieceTracker.from_control_file(ctrl)
    assert restored.allocation is not None
    assert restored.allocation.previous_size == 8192
    assert restored.allocation == allocation_reserved_posix(1024, previous_size=8192)


def test_progress_reports() -> None:
    t = PieceTracker(total_length=5 * 1024, piece_size=1024)
    assert t.progress() == (0, 5)
    c = t.claim()
    assert c is not None
    t.complete(c[0])
    assert t.progress() == (1, 5)


def test_is_done_per_piece() -> None:
    t = PieceTracker(total_length=3 * 1024, piece_size=1024)
    c = t.claim()
    assert c is not None
    assert not t.is_done(c[0])
    t.complete(c[0])
    assert t.is_done(c[0])


def test_zero_length_tracker() -> None:
    t = PieceTracker(total_length=0, piece_size=1024)
    assert t.num_pieces == 0
    assert t.claim() is None
    assert t.is_complete()
    assert t.progress() == (0, 0)


# -----------------------------------------------------------------------
# Thread safety
# -----------------------------------------------------------------------


def test_concurrent_claims_are_unique() -> None:
    n_pieces = 1000
    t = PieceTracker(total_length=n_pieces * 1024, piece_size=1024)
    claimed: list[int] = []
    lock = threading.Lock()

    def worker() -> None:
        while (c := t.claim()) is not None:
            with lock:
                claimed.append(c[0])
            t.complete(c[0])

    threads = [threading.Thread(target=worker) for _ in range(8)]
    for th in threads:
        th.start()
    for th in threads:
        th.join()

    assert sorted(claimed) == list(range(n_pieces))
    assert t.is_complete()


def test_concurrent_release_and_reclaim() -> None:
    t = PieceTracker(total_length=10 * 1024, piece_size=1024)
    completed: list[int] = []
    lock = threading.Lock()

    def worker(fail_first: bool) -> None:
        while True:
            c = t.claim()
            if c is None:
                break
            idx, _, _ = c
            if fail_first:
                t.release(idx)
                fail_first = False
                continue
            with lock:
                completed.append(idx)
            t.complete(idx)

    threads = [threading.Thread(target=worker, args=(i % 2 == 0,)) for i in range(4)]
    for th in threads:
        th.start()
    for th in threads:
        th.join()

    assert sorted(completed) == list(range(10))
    assert t.is_complete()


# -----------------------------------------------------------------------
# Swarm collapse (200 fallback)
# -----------------------------------------------------------------------


def test_enter_sequential_mode_winner() -> None:
    t = PieceTracker(total_length=5 * 1024, piece_size=1024)
    t.claim()
    t.claim()
    assert t.enter_sequential_mode(thread_id=42)
    assert t.should_abort()


def test_enter_sequential_mode_loser() -> None:
    t = PieceTracker(total_length=5 * 1024, piece_size=1024)
    assert t.enter_sequential_mode(thread_id=1)
    assert not t.enter_sequential_mode(thread_id=2)


def test_clear_sequential_mode_resets() -> None:
    t = PieceTracker(total_length=5 * 1024, piece_size=1024)
    t.enter_sequential_mode(thread_id=1)
    assert t.should_abort()
    t.clear_sequential_mode(thread_id=1)
    assert not t.should_abort()


def test_clear_sequential_mode_wrong_owner_ignored() -> None:
    t = PieceTracker(total_length=5 * 1024, piece_size=1024)
    t.enter_sequential_mode(thread_id=1)
    t.clear_sequential_mode(thread_id=999)
    assert t.should_abort()  # still set — wrong thread_id


def test_sequential_mode_releases_inflight() -> None:
    t = PieceTracker(total_length=5 * 1024, piece_size=1024)
    t.claim()
    t.claim()
    t.claim()
    # Three pieces are in-flight. Collapse should release them all.
    t.enter_sequential_mode(thread_id=1)
    # After collapse, all non-done pieces should be claimable again.
    t.clear_sequential_mode(thread_id=1)
    claimed: list[int] = []
    while (c := t.claim()) is not None:
        claimed.append(c[0])
    assert sorted(claimed) == list(range(5))


def test_flush_and_resume(tmp_path: Path) -> None:
    part = tmp_path / "file.zip.part"
    ctrl = tmp_path / "file.zip.part.ctrl"
    t = PieceTracker(
        total_length=10 * 1024,
        piece_size=1024,
        url=parse_url("https://example.com/file.zip"),
        etag=parse_etag('"abc"'),
        alloc_preset="reserved_posix",
        part_path=part,
    )
    for _ in range(4):
        c = t.claim()
        assert c is not None
        t.complete(c[0])

    t.flush_state(ctrl)

    restored = PieceTracker.from_control_file(ctrl, server_etag=parse_etag('"abc"'))
    assert restored.progress() == (4, 10)
    assert restored.url == "https://example.com/file.zip"
    assert restored.etag == '"abc"'
    assert restored.allocation == t.allocation
    for i in range(4):
        assert restored.is_done(PieceIndex(i))
    for i in range(4, 10):
        assert not restored.is_done(PieceIndex(i))


def test_flush_skips_when_no_completions(tmp_path: Path) -> None:
    ctrl = tmp_path / "file.zip.part.ctrl"
    t = PieceTracker(total_length=5 * 1024, piece_size=1024)
    assert not t.flush_state(ctrl)
    assert not ctrl.exists()


def test_flush_force(tmp_path: Path) -> None:
    ctrl = tmp_path / "file.zip.part.ctrl"
    t = PieceTracker(total_length=5 * 1024, piece_size=1024, alloc_preset="sparse")
    assert t.flush_state(ctrl, force=True)
    assert ctrl.exists()


def test_should_flush_threshold() -> None:
    t = PieceTracker(total_length=10 * 1024, piece_size=1024)
    assert not t.should_flush(every_n=3)
    for _ in range(3):
        c = t.claim()
        assert c is not None
        t.complete(c[0])
    assert t.should_flush(every_n=3)


def test_resume_etag_mismatch(tmp_path: Path) -> None:
    ctrl = tmp_path / "file.zip.part.ctrl"
    t = PieceTracker(
        total_length=5 * 1024,
        piece_size=1024,
        etag=parse_etag('"old"'),
        alloc_preset="sparse",
    )
    c = t.claim()
    assert c is not None
    t.complete(c[0])
    t.flush_state(ctrl, force=True)

    with pytest.raises(ControlFileError, match="ETag mismatch"):
        PieceTracker.from_control_file(ctrl, server_etag=parse_etag('"new"'))


def test_resume_etag_empty_server_etag_accepted(tmp_path: Path) -> None:
    ctrl = tmp_path / "file.zip.part.ctrl"
    t = PieceTracker(
        total_length=5 * 1024,
        piece_size=1024,
        etag=parse_etag('"stored"'),
        alloc_preset="sparse",
    )
    c = t.claim()
    assert c is not None
    t.complete(c[0])
    t.flush_state(ctrl, force=True)

    restored = PieceTracker.from_control_file(ctrl, server_etag=EMPTY_ETAG)
    assert restored.progress() == (1, 5)


def test_flush_clears_completion_counter(tmp_path: Path) -> None:
    ctrl = tmp_path / "file.zip.part.ctrl"
    t = PieceTracker(total_length=5 * 1024, piece_size=1024, alloc_preset="sparse")
    c = t.claim()
    assert c is not None
    t.complete(c[0])
    assert t.should_flush(every_n=1)
    t.flush_state(ctrl)
    assert not t.should_flush(every_n=1)


def test_flush_state_is_noop_without_progress(tmp_path: Path) -> None:
    """``flush_state(force=False)`` must bail out cheaply when no pieces
    have completed since the last flush — no temp file, no fsync, no
    ctrl written. Burns IO and bumps the digest counter otherwise."""
    ctrl = tmp_path / "file.zip.part.ctrl"
    t = PieceTracker(total_length=5 * 1024, piece_size=1024)
    assert t.flush_state(ctrl) is False
    assert not ctrl.exists()


def test_resumed_tracker_can_claim_remaining(tmp_path: Path) -> None:
    ctrl = tmp_path / "f.part.ctrl"
    t = PieceTracker(
        total_length=5 * 1024,
        piece_size=1024,
        url=parse_url("http://example.com/u"),
        etag=parse_etag('"e"'),
        alloc_preset="sparse",
    )
    for _ in range(3):
        c = t.claim()
        assert c is not None
        t.complete(c[0])
    t.flush_state(ctrl, force=True)

    restored = PieceTracker.from_control_file(ctrl)
    remaining: list[int] = []
    while (c2 := restored.claim()) is not None:
        remaining.append(c2[0])
    assert sorted(remaining) == [3, 4]


def test_flush_state_no_path_raises() -> None:
    t = PieceTracker(total_length=1024, piece_size=1024, alloc_preset="sparse")
    c = t.claim()
    assert c is not None
    t.complete(c[0])
    with pytest.raises(ValueError, match="no path"):
        t.flush_state()


# -----------------------------------------------------------------------
# Flush dedup + durability pairing
# -----------------------------------------------------------------------


def test_flush_hash_dedup_skips_identical_state(tmp_path: Path) -> None:
    """A force flush with byte-identical state returns False and doesn't rewrite."""
    ctrl = tmp_path / "f.part.ctrl"
    t = PieceTracker(total_length=5 * 1024, piece_size=1024, alloc_preset="sparse")
    c = t.claim()
    assert c is not None
    t.complete(c[0])

    assert t.flush_state(ctrl, force=True) is True
    first_mtime_ns = ctrl.stat().st_mtime_ns
    first_bytes = ctrl.read_bytes()

    # Second force flush with unchanged state: hash gate short-circuits.
    assert t.flush_state(ctrl, force=True) is False
    assert ctrl.read_bytes() == first_bytes
    # On filesystems where mtime granularity is 1ns, we can also assert
    # the file wasn't touched — mtime is a stronger signal than bytes
    # (which would match even if we rewrote them).
    assert ctrl.stat().st_mtime_ns == first_mtime_ns


def test_flush_rewrites_when_state_changes(tmp_path: Path) -> None:
    """Completing another piece after flush must produce a different ctrl file."""
    ctrl = tmp_path / "f.part.ctrl"
    t = PieceTracker(total_length=5 * 1024, piece_size=1024, alloc_preset="sparse")
    c = t.claim()
    assert c is not None
    t.complete(c[0])

    assert t.flush_state(ctrl, force=True) is True
    first_bytes = ctrl.read_bytes()

    c2 = t.claim()
    assert c2 is not None
    t.complete(c2[0])

    assert t.flush_state(ctrl, force=True) is True
    assert ctrl.read_bytes() != first_bytes


def test_flush_hash_dedup_after_resume(tmp_path: Path) -> None:
    """Resumed tracker with no new progress is a no-op flush (digest seeded)."""
    ctrl = tmp_path / "f.part.ctrl"
    t = PieceTracker(
        total_length=5 * 1024,
        piece_size=1024,
        url=parse_url("http://example.com/u"),
        etag=parse_etag('"e"'),
        alloc_preset="reserved_posix",
    )
    c = t.claim()
    assert c is not None
    t.complete(c[0])
    t.flush_state(ctrl, force=True)

    restored = PieceTracker.from_control_file(ctrl)
    # No new progress — forced flush should detect identical state.
    assert restored.flush_state(ctrl, force=True) is False


def test_flush_syncs_part_fd_before_rename(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """When a .part fd is registered, data sync runs before the ctrl rename.

    This is the durability invariant: a bit set in .part.ctrl on disk
    must imply the corresponding .part bytes are durable.
    """
    import os as _os

    from reget import control as _control, tracker as _tracker

    part = tmp_path / "f.part"
    ctrl = tmp_path / "f.part.ctrl"
    part.write_bytes(b"\x00" * (5 * 1024))
    fd = _os.open(str(part), _os.O_RDWR)
    try:
        t = PieceTracker(total_length=5 * 1024, piece_size=1024, alloc_preset="sparse")
        t.set_part_fd(fd)
        c = t.claim()
        assert c is not None
        t.complete(c[0])

        order: list[str] = []

        real_sync = _tracker._sync_data_pages
        real_write_atomic_raw = _control.write_atomic_raw

        def tracking_sync(arg: int) -> None:
            order.append("sync_data")
            real_sync(arg)

        def tracking_write_atomic_raw(*args: object, **kwargs: object) -> None:
            order.append("write_atomic_raw")
            real_write_atomic_raw(*args, **kwargs)  # type: ignore[arg-type]

        monkeypatch.setattr(_tracker, "_sync_data_pages", tracking_sync)
        monkeypatch.setattr(_tracker, "write_atomic_raw", tracking_write_atomic_raw)

        assert t.flush_state(ctrl, force=True) is True
        assert order == ["sync_data", "write_atomic_raw"], order
    finally:
        _os.close(fd)


def test_flush_without_part_fd_is_still_atomic(tmp_path: Path) -> None:
    """No part_fd registered → no fdatasync, but the ctrl write still lands."""
    ctrl = tmp_path / "f.part.ctrl"
    t = PieceTracker(total_length=3 * 1024, piece_size=1024, alloc_preset="sparse")
    for _ in range(3):
        c = t.claim()
        assert c is not None
        t.complete(c[0])
    assert t.flush_state(ctrl, force=True) is True
    assert ctrl.exists()


def test_concurrent_flushes_do_not_corrupt_ctrl(tmp_path: Path) -> None:
    """Many threads calling flush_state in a tight loop produce only valid
    snapshots, never a FileNotFoundError or truncated file.

    This was the original parallel-download bug: a deterministic
    ``<ctrl>.tmp`` filename let two flushers race on rename+unlink.
    """
    from reget.control import read_control

    ctrl = tmp_path / "f.part.ctrl"
    t = PieceTracker(total_length=100 * 1024, piece_size=1024, alloc_preset="sparse")

    errors: list[BaseException] = []
    err_lock = threading.Lock()
    done = threading.Event()

    def completer() -> None:
        while not done.is_set():
            c = t.claim()
            if c is None:
                return
            t.complete(c[0])

    def flusher() -> None:
        try:
            while not done.is_set():
                t.flush_state(ctrl)
                if ctrl.exists():
                    # If readable, it must be a valid snapshot.
                    read_control(ctrl)
        except BaseException as exc:  # noqa: BLE001
            with err_lock:
                errors.append(exc)
            done.set()

    threads = [
        *(threading.Thread(target=completer, daemon=True) for _ in range(4)),
        *(threading.Thread(target=flusher, daemon=True) for _ in range(4)),
    ]
    for th in threads:
        th.start()
    # Let them hammer for a bit, then stop.
    import time as _time

    _time.sleep(0.5)
    done.set()
    for th in threads:
        th.join(timeout=5.0)
        assert not th.is_alive()

    assert errors == [], f"concurrent flushes raised: {errors!r}"
    # Final force-flush to catch any lingering state.
    t.flush_state(ctrl, force=True)
    cf = read_control(ctrl)
    assert 0 <= cf.done.count() <= 100

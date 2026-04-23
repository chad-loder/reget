"""Piece tracker: bitfield-backed progress management.

Two parallel bitfields (``done`` and ``inflight``) back a thread-safe
claim / complete / release API.  The selection query is
``~(done | inflight)``; claiming and completing are both
``O(num_pieces / 64)`` bitwise ops in C thanks to the ``bitarray``
extension.
"""

from __future__ import annotations

import hashlib
import os
import threading
from pathlib import Path

from bitarray import bitarray

from reget._types import (
    EMPTY_ETAG,
    EMPTY_URL,
    ByteLength,
    ByteOffset,
    ETag,
    PieceIndex,
    Url,
)
from reget.alloc import (
    AllocationOutcome,
    AllocationPreset,
    AllocationResult,
    Mechanism,
    allocation_reserved_fpreallocate,
    allocation_reserved_posix,
    allocation_sparse,
)
from reget.control import (
    CTRL_VERSION,
    ControlFileError,
    ControlMeta,
    ctrl_path_for,
    read_control,
    serialize,
    write_atomic_raw,
)

_Claim = tuple[PieceIndex, ByteOffset, ByteLength]
"""(piece_index, byte_offset, byte_length) — branded so call sites can't
transpose the three ``int``s at the API boundary."""

_DIGEST_SIZE = 16
"""blake2b digest bytes for change detection (128 bits — collision-free
for our use case at any realistic flush count).  Not a cryptographic
primitive; picked because it is the stdlib's fastest hash for small
inputs on modern CPUs."""


def _digest(raw: bytes) -> bytes:
    return hashlib.blake2b(raw, digest_size=_DIGEST_SIZE).digest()


_sync_data_pages = getattr(os, "fdatasync", os.fsync)


class PieceTracker:
    """Thread-safe tracker for per-piece download state.

    Two orthogonal bitfields:

    * ``_done`` — pieces whose bytes are on disk and verified.
    * ``_inflight`` — pieces currently being fetched by some worker.

    The selection query is ``~(done | inflight)``; both claiming and
    completing are ``O(num_pieces / 64)`` bitwise ops in C.

    Supply ``allocation`` (a full :class:`~reget.alloc.AllocationResult`) or
    ``alloc_preset`` + optional ``alloc_previous_size`` for a shorthand; not
    both. :class:`PieceDownloader` uses ``allocate_file`` + ``set_allocation``.
    """

    def __init__(
        self,
        total_length: int,
        piece_size: int,
        *,
        url: Url = EMPTY_URL,
        etag: ETag = EMPTY_ETAG,
        allocation: AllocationResult | None = None,
        alloc_preset: AllocationPreset | None = None,
        alloc_previous_size: int = 0,
        part_path: Path | None = None,
    ) -> None:
        if total_length < 0:
            raise ValueError("total_length must be non-negative")
        if piece_size <= 0:
            raise ValueError("piece_size must be positive")
        if allocation is not None and alloc_preset is not None:
            msg = "pass only one of allocation and alloc_preset"
            raise ValueError(msg)
        resolved = allocation
        if alloc_preset is not None:
            if alloc_preset == "sparse":
                resolved = allocation_sparse(total_length, previous_size=alloc_previous_size)
            elif alloc_preset == "reserved_posix":
                resolved = allocation_reserved_posix(total_length, previous_size=alloc_previous_size)
            elif alloc_preset == "reserved_fpreallocate":
                resolved = allocation_reserved_fpreallocate(total_length, previous_size=alloc_previous_size)
            else:
                msg = f"unknown alloc_preset: {alloc_preset!r}"
                raise ValueError(msg)
        self._total_length = total_length
        self._piece_size = piece_size
        self._num_pieces = -(-total_length // piece_size) if total_length else 0
        self._url = url
        self._etag = etag
        self._allocation = resolved
        self._part_path = part_path
        self._lock = threading.Lock()
        self._writer_lock = threading.Lock()
        self._done = bitarray(self._num_pieces)
        self._inflight = bitarray(self._num_pieces)
        self._done.setall(False)
        self._inflight.setall(False)
        self._done_count = 0
        self._sequential_owner: int | None = None
        self._collapse_event = threading.Event()
        self._completions_since_flush = 0
        self._part_fd: int | None = None
        self._last_flushed_digest: bytes | None = None

    # ------------------------------------------------------------------
    # Construction from a .part.ctrl file
    # ------------------------------------------------------------------

    @classmethod
    def from_control_file(
        cls,
        ctrl_path: Path,
        *,
        server_etag: ETag = EMPTY_ETAG,
    ) -> PieceTracker:
        """Reconstruct a tracker from a ``.part.ctrl`` file.

        If *server_etag* is provided and differs from the stored ETag,
        raises ``ControlFileError`` — the caller should discard the
        ``.part`` and ``.ctrl`` and start fresh.
        """
        cf = read_control(ctrl_path)
        m = cf.meta

        if server_etag and m.etag and m.etag != server_etag:
            raise ControlFileError(f"ETag mismatch: control file has {m.etag!r}, server has {server_etag!r}")

        allocation = AllocationResult(
            outcome=AllocationOutcome(m.alloc_outcome),
            previous_size=m.alloc_previous_size,
            final_size=m.total_length,
            mechanism=Mechanism(m.alloc_mechanism),
        )

        tracker = cls(
            total_length=m.total_length,
            piece_size=m.piece_size,
            url=m.url,
            etag=m.etag,
            allocation=allocation,
            part_path=ctrl_path.parent / ctrl_path.stem.removesuffix(".ctrl"),
        )
        with tracker._lock:
            tracker._done = cf.done
            tracker._done_count = cf.done.count()
            tracker._last_flushed_digest = _digest(serialize(m, cf.done))
        return tracker

    # ------------------------------------------------------------------
    # Properties
    # ------------------------------------------------------------------

    @property
    def total_length(self) -> int:
        return self._total_length

    @property
    def piece_size(self) -> int:
        return self._piece_size

    @property
    def num_pieces(self) -> int:
        return self._num_pieces

    @property
    def url(self) -> str:
        return self._url

    @property
    def etag(self) -> ETag:
        return self._etag

    @property
    def allocation(self) -> AllocationResult | None:
        return self._allocation

    def set_allocation(self, result: AllocationResult) -> None:
        if result.final_size != self._total_length:
            msg = f"allocation final_size {result.final_size} != tracker total_length {self._total_length}"
            raise ValueError(msg)
        self._allocation = result

    # ------------------------------------------------------------------
    # Piece lifecycle
    # ------------------------------------------------------------------

    def claim(self) -> _Claim | None:
        """Reserve the next available piece for this worker."""
        with self._lock:
            candidates = ~(self._done | self._inflight)
            try:
                idx = PieceIndex(candidates.index(True))
            except ValueError:
                return None
            self._inflight[idx] = True
        offset = ByteOffset(idx * self._piece_size)
        length = ByteLength(min(self._piece_size, self._total_length - offset))
        return idx, offset, length

    def complete(self, idx: PieceIndex) -> int:
        """Mark a piece as successfully written and verified. Returns
        the new total number of completed pieces."""
        with self._lock:
            if not self._done[idx]:
                self._done[idx] = True
                self._done_count += 1
            self._inflight[idx] = False
            self._completions_since_flush += 1
            return self._done_count

    def release(self, idx: PieceIndex) -> None:
        """Return a piece to the pool (fetch failed)."""
        with self._lock:
            self._inflight[idx] = False

    def is_done(self, idx: PieceIndex) -> bool:
        with self._lock:
            return bool(self._done[idx])

    def is_complete(self) -> bool:
        with self._lock:
            return self._done_count >= self._num_pieces

    def progress(self) -> tuple[int, int]:
        """Return ``(completed_pieces, total_pieces)``."""
        with self._lock:
            return self._done_count, self._num_pieces

    # ------------------------------------------------------------------
    # Swarm collapse (200 fallback)
    # ------------------------------------------------------------------

    def enter_sequential_mode(self, thread_id: int) -> bool:
        """Attempt to become the sole sequential streamer.

        Returns ``True`` if this thread won; ``False`` if another thread
        already owns the sequential stream.
        """
        with self._lock:
            if self._sequential_owner is not None:
                return False
            self._sequential_owner = thread_id
            self._inflight.setall(False)
            self._collapse_event.set()
            return True

    def clear_sequential_mode(self, thread_id: int) -> None:
        """Release sequential-stream ownership (stream finished or dropped)."""
        with self._lock:
            if self._sequential_owner == thread_id:
                self._sequential_owner = None
                self._collapse_event.clear()

    def should_abort(self) -> bool:
        """Poll this between chunk reads; ``True`` means another thread
        triggered swarm collapse and this worker should back off."""
        return self._collapse_event.is_set()

    # ------------------------------------------------------------------
    # Persistence
    # ------------------------------------------------------------------

    def _make_meta(self) -> ControlMeta:
        if self._allocation is None:
            msg = "allocation not set; call set_allocation after allocate_file"
            raise RuntimeError(msg)
        a = self._allocation
        return ControlMeta(
            version=CTRL_VERSION,
            url=self._url,
            piece_size=self._piece_size,
            total_length=self._total_length,
            etag=self._etag,
            alloc_outcome=a.outcome.value,
            alloc_mechanism=a.mechanism.value,
            alloc_previous_size=a.previous_size,
        )

    def set_part_fd(self, fd: int | None) -> None:
        """Register a persistent fd on the ``.part`` file for durability pairing.

        When set, :meth:`flush_state` will ``fdatasync`` this fd *before*
        writing the control file, upholding the invariant that a bit set
        in ``.part.ctrl`` on disk implies the corresponding ``.part``
        bytes are durable on disk.  Without this pairing, a crash between
        the two writes can leave the control file claiming pieces are
        done while their bytes are still in page cache — silent
        corruption on recovery (the original ``tor-scraper`` bug reget
        exists to prevent).

        Passing ``None`` disables pairing (only appropriate for tests or
        in-memory use).  The fd is not owned by the tracker; the caller
        is responsible for closing it.
        """
        self._part_fd = fd

    def flush_state(self, path: Path | None = None, *, force: bool = False) -> bool:
        """Persist the done-bitfield to a ``.part.ctrl`` file, durably.

        The write is gated by three cheap checks in order:

        1. **Event counter.**  If no piece has completed since the last
           flush and *force* is ``False``, return immediately.
        2. **Writer-lock try-acquire** (non-force path).  Losers return
           without blocking.  Their completions sit in the bitfield and
           are captured by the winner's snapshot, or by the next flush.
        3. **Content hash.**  If the serialized control bytes are
           byte-identical to what was last written, skip both the
           fdatasync and the ctrl-file rewrite.

        When all three gates pass, the flush is durability-paired:
        ``fdatasync`` on the registered ``.part`` fd (if any) runs
        *before* the control file rename, so the bitfield can never
        claim bytes that haven't landed.  Returns ``True`` iff bytes
        were written to disk.
        """
        ctrl = path or (ctrl_path_for(self._part_path) if self._part_path else None)
        if ctrl is None:
            raise ValueError("no path: pass path= or set part_path on construction")

        if not force and self._completions_since_flush == 0:
            return False

        if force:
            self._writer_lock.acquire()
        elif not self._writer_lock.acquire(blocking=False):
            return False
        try:
            with self._lock:
                if not force and self._completions_since_flush == 0:
                    return False
                meta = self._make_meta()
                done_snapshot = self._done.copy()
                self._completions_since_flush = 0

            raw = serialize(meta, done_snapshot)
            digest = _digest(raw)

            if digest == self._last_flushed_digest:
                return False

            if self._part_fd is not None:
                _sync_data_pages(self._part_fd)

            write_atomic_raw(ctrl, raw)
            self._last_flushed_digest = digest
            return True
        finally:
            self._writer_lock.release()

    def should_flush(self, every_n: int = 5) -> bool:
        """Return ``True`` when enough completions have accumulated."""
        with self._lock:
            return self._completions_since_flush >= every_n

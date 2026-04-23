"""Piece-based HTTP download engine (200 / 206 / 416) with resume.

Orchestrates :class:`PieceTracker`, file allocation, and control-file
persistence. Safe for concurrent :meth:`~PieceDownloader.download_piece`
calls on one instance. :func:`fetch` is a single-threaded convenience wrapper.
"""

from __future__ import annotations

import errno
import logging
import os
import shutil
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass
from http import HTTPStatus
from pathlib import Path
from typing import overload

from reget._types import (
    ByteLength,
    ByteOffset,
    ContentRangeError,
    DownloadComplete,
    DownloadPartial,
    DownloadResult,
    HashBuilder,
    PieceIndex,
    RegetError,
    ServerMeta,
    ServerMisconfiguredError,
    Url,
    parse_etag,
    parse_url,
)
from reget.alloc import allocate_file
from reget.control import ControlFileError, ctrl_path_for
from reget.headers import DEFAULT_HEADERS, is_file_changed, merge_headers
from reget.tracker import PieceTracker
from reget.transport.errors import TransportError
from reget.transport.factory import wrap_transport
from reget.transport.protocols import TransportResponse, TransportSession
from reget.transport.session_input import AnySession
from reget.transport.types import TransportHeaders, TransportRequestOptions

log = logging.getLogger("reget")

_CHUNK_SIZE = 65536
_DEFAULT_PIECE_SIZE = 1 << 20  # 1 MiB
_FLUSH_EVERY_N = 5


def _parse_content_range_total(headers: TransportHeaders) -> int | None:
    """Extract total length from ``Content-Range: bytes */TOTAL`` (416 responses)."""
    cr = headers.get("Content-Range")
    if not cr.startswith("bytes */"):
        return None
    total_str = cr.split("/", 1)[-1]
    if total_str == "*":
        return None
    try:
        return int(total_str)
    except ValueError:
        return None


def _has_content_encoding(headers: TransportHeaders) -> bool:
    ce = headers.get("Content-Encoding").strip().lower()
    return ce not in ("", "identity")


def _validate_content_range(
    headers: TransportHeaders,
    expected_offset: ByteOffset,
    expected_length: ByteLength,
    expected_total: int,
) -> None:
    """Validate a 206 ``Content-Range`` for the **legacy HEAD-first** pipeline.

    Compares the full header string to ``bytes {start}-{end}/{expected_total}``.
    ``expected_total`` is the tracker size pinned from **HEAD** ``Content-Length``
    during :meth:`PieceDownloader.prepare`.

    **Not** used for optimistic GET + growth (plan §1.1): there the instance
    length may be ``*`` until the server publishes ``/N``; validation must be
    parse-driven and **mode-aware** (``GROWING_UNKNOWN_TOTAL`` vs
    ``FIXED_KNOWN_TOTAL``) instead of rejecting ``/*`` against a HEAD pin.
    """
    cr = headers.get("Content-Range")
    if not cr:
        raise ContentRangeError("206 response missing Content-Range header")
    end = expected_offset + expected_length - 1
    expected = f"bytes {expected_offset}-{end}/{expected_total}"
    # Legacy behavior: HEAD gave a definite total, so ``bytes a-b/*`` is treated
    # as a mismatch. Optimistic GET (plan §1) accepts ``/*`` in growth mode.
    if cr != expected:
        raise ContentRangeError(f"Content-Range mismatch: got {cr!r}, expected {expected!r}")


def _publish_part(part_path: Path, dest: Path) -> None:
    """Atomically publish ``part_path`` as ``dest``, falling back across mounts.

    The fast path is ``Path.replace`` — POSIX-atomic on the same filesystem
    and what every durable-write recipe recommends.  It raises ``OSError``
    with ``errno.EXDEV`` when ``part_path`` and ``dest`` live on different
    filesystems (e.g. user staged on tmpfs and wants the final file on a
    spinning disk).  In that case we copy+fsync+rename on the destination
    side, then unlink the source — not atomic across the two mounts (no
    POSIX primitive offers that), but the window is bounded to the final
    rename on ``dest``'s filesystem and still crash-consistent: either
    ``dest`` exists with the right bytes or it doesn't.
    """
    try:
        part_path.replace(dest)
        return
    except OSError as exc:
        if exc.errno != errno.EXDEV:
            raise

    log.info(
        "cross-device publish %s -> %s (EXDEV); falling back to copy+rename",
        part_path,
        dest,
    )
    # Stage next to dest so the final rename is same-filesystem.  The .tmp
    # suffix matches the control-file writer's convention.
    staging = dest.with_suffix(dest.suffix + ".tmp")
    try:
        shutil.copyfile(part_path, staging)
        # fsync the copy before the rename so a crash between the rename
        # and the source unlink doesn't leave ``dest`` with torn bytes.
        with staging.open("rb") as f:
            os.fsync(f.fileno())
        staging.replace(dest)
    except OSError:
        # Clean up the staging file on failure so we don't leave debris.
        try:
            staging.unlink(missing_ok=True)
        except OSError:
            log.warning("failed to clean up staging file %s", staging, exc_info=True)
        raise

    try:
        part_path.unlink()
    except OSError:
        # Source unlink failure after a successful publish is cosmetic:
        # ``dest`` is correct, and the orphaned .part will be discarded by
        # the next prepare() (its control file, if any, was already
        # removed by the caller-side cleanup).
        log.warning(
            "failed to remove source .part %s after cross-device publish",
            part_path,
            exc_info=True,
        )


def _extract_server_meta(resp: TransportResponse) -> ServerMeta:
    """Pull ETag, Content-Length, Last-Modified, Content-Type from response headers.

    This is the single HTTP-layer boundary where we turn normalized headers
    into a typed ``ServerMeta``.  After this point the rest of the downloader
    passes ``ServerMeta`` around as a value object.
    """
    h = resp.headers
    cl = h.get("Content-Length")
    return ServerMeta(
        etag=parse_etag(h.get("ETag")),
        total_length=int(cl) if cl and cl.isdigit() else None,
        last_modified=h.get("Last-Modified"),
        content_type=h.get("Content-Type"),
    )


@dataclass(frozen=True, slots=True)
class ProgressUpdate:
    """Snapshot of download progress emitted via :data:`ProgressCallback`.

    The callback runs **synchronously** on the **same thread** that called
    :meth:`PieceDownloader.download_piece` for the work unit that just
    completed (piece mode or 200 sequential fallback). There is no internal
    thread pool; if several threads call :meth:`~PieceDownloader.download_piece`,
    each may receive callbacks on its own thread. :meth:`~PieceDownloader.prepare`
    and :meth:`~PieceDownloader.finalize` do **not** invoke this callback.
    """

    piece_index: PieceIndex
    """Index of the piece that was just successfully completed."""
    pieces_completed: int
    """Total number of completed pieces (including this one)."""
    pieces_total: int
    """Total number of pieces in the download."""
    bytes_written: int
    """Monotonic session counter: payload bytes appended to ``.part`` in this process.

    Includes work from all threads calling :meth:`PieceDownloader.download_piece`.
    Under :meth:`PieceDownloader._lock` whenever it changes; each callback sees the
    value *after* the bytes for the completed piece (or sequential chunk) are
    accounted for, so it always matches real I/O progress — including during
    200 sequential fallback, where it increments per ``write``, not only at
    stream end.
    """
    is_sequential: bool
    """True if currently in the '200 OK' sequential fallback mode."""
    progress_handle: object
    """Value from :meth:`TransportSession.opaque_progress_handle` (opaque to the engine)."""


# Invoked synchronously on the thread that ran download_piece (see ProgressUpdate).
ProgressCallback = Callable[[ProgressUpdate], None]


class PieceDownloader:
    """Manages a single file download with piece-level tracking.

    Lifecycle::

        pd = PieceDownloader(url, dest, on_progress=my_cb)
        transport = NiquestsAdapter(session)
        pd.prepare(transport)              # HEAD, allocate, resume from ctrl
        while not pd.is_complete():
            pd.download_piece(transport)   # or call from N threads
        result = pd.finalize()

    If ``on_progress`` is set, it is called from each
    :meth:`~PieceDownloader.download_piece` caller thread as pieces complete
    (never from :meth:`~PieceDownloader.prepare` / :meth:`~PieceDownloader.finalize`).
    Callbacks must be cheap or offload work themselves (e.g. queue to a UI thread).
    """

    def __init__(
        self,
        url: str | Url,
        dest: Path,
        *,
        piece_size: int = _DEFAULT_PIECE_SIZE,
        chunk_size: int = _CHUNK_SIZE,
        flush_every: int = _FLUSH_EVERY_N,
        extra_headers: dict[str, str] | None = None,
        on_progress: ProgressCallback | None = None,
        part_path: Path | None = None,
        timeout: float | tuple[float, float] | None = None,
        verify: bool | None = None,
    ) -> None:
        self._url: Url = parse_url(url)
        self._dest = Path(dest)
        if part_path is not None:
            self._part_path = Path(part_path)
            self._ctrl_path = ctrl_path_for(self._part_path)
        else:
            self._part_path = self._dest.with_suffix(self._dest.suffix + ".part")
            self._ctrl_path = ctrl_path_for(self._part_path)
        self._piece_size = piece_size
        self._chunk_size = chunk_size
        self._flush_every = flush_every
        self._extra_headers = extra_headers or {}
        self._on_progress = on_progress
        self._timeout = timeout
        self._verify = verify
        self._tracker: PieceTracker | None = None
        self._server_meta: ServerMeta = ServerMeta()
        self._t0 = 0.0
        self._bytes_written = 0
        self._lock = threading.Lock()
        # Protects the prepare() critical section from concurrent thundering herds.
        self._prepare_lock = threading.Lock()
        # Persistent .part fd: used only for pairing fdatasync() with ctrl
        # flushes.
        self._part_fd: int | None = None

    def _transport_options_for_head(self) -> TransportRequestOptions:
        return TransportRequestOptions(
            timeout=self._timeout,
            verify=self._verify,
            allow_redirects=True,
        )

    def _transport_options_for_get(self) -> TransportRequestOptions:
        return TransportRequestOptions(
            timeout=self._timeout,
            verify=self._verify,
        )

    @property
    def tracker(self) -> PieceTracker | None:
        return self._tracker

    @property
    def bytes_written(self) -> int:
        """Bytes written in this session (piece mode + sequential fallback)."""
        return self._bytes_written

    def prepare(self, transport: TransportSession) -> None:
        """Probe the server (HEAD) and set up or resume the tracker.

        Idempotent and thread-safe. Multiple concurrent calls will block
        until the first one completes the preparation, then return immediately.

        Pass a :class:`~reget.transport.NiquestsAdapter` built from a
        :class:`niquests.Session`, or any other :class:`TransportSession`
        implementation.
        """
        # Double-checked locking: avoid the heavy lock if already prepared.
        if self._tracker is not None:
            return

        with self._prepare_lock:
            # Check again now that we hold the lock.
            if self._tracker is not None:
                return

            self._t0 = time.monotonic()

            headers = merge_headers(self._extra_headers, DEFAULT_HEADERS)
            with transport.head(
                self._url,
                headers=headers,
                options=self._transport_options_for_head(),
            ) as resp:
                resp.raise_for_status()
                self._server_meta = _extract_server_meta(resp)
            total_length = self._server_meta.total_length
            if total_length is None or total_length <= 0:
                raise RegetError(f"server did not report Content-Length for {self._url}")
            server_etag = self._server_meta.etag

            if self._ctrl_path.exists() and self._part_path.exists():
                try:
                    self._tracker = PieceTracker.from_control_file(
                        self._ctrl_path,
                        server_etag=server_etag,
                    )
                    # Cross-check total_length in addition to ETag. The ETag
                    # check alone misses two real scenarios: (a) origins that
                    # serve no ETag at all and (b) origins whose HEAD returned
                    # a blank ETag on the original attempt but provide one on
                    # resume. A size change is unambiguous evidence the
                    # resource has changed, regardless of metadata.
                    if self._tracker.total_length != total_length:
                        raise ControlFileError(
                            f"total_length changed: control file has "
                            f"{self._tracker.total_length}, server reports {total_length}"
                        )
                    log.info(
                        "resuming %s: %d/%d pieces done",
                        self._url,
                        *self._tracker.progress(),
                    )
                    self._open_part_fd()
                    self._tracker.set_part_fd(self._part_fd)
                    return
                except ControlFileError as exc:
                    log.warning("discarding stale control file: %s", exc)
                    self._ctrl_path.unlink(missing_ok=True)
                    self._part_path.unlink(missing_ok=True)

            self._tracker = PieceTracker(
                total_length=total_length,
                piece_size=self._piece_size,
                url=self._url,
                etag=server_etag,
                part_path=self._part_path,
            )

            fd = os.open(str(self._part_path), os.O_CREAT | os.O_RDWR, 0o644)
            try:
                allocation = allocate_file(fd, total_length=total_length)
            finally:
                os.close(fd)
            self._tracker.set_allocation(allocation)
            log.info(
                "allocated %s (%s/%s, prev=%d, %d bytes)",
                self._part_path.name,
                allocation.outcome,
                allocation.mechanism,
                allocation.previous_size,
                total_length,
            )

            self._open_part_fd()
            self._tracker.set_part_fd(self._part_fd)

    def _open_part_fd(self) -> None:
        """Open the persistent ``.part`` fd used for durability pairing.

        Idempotent; safe to call repeatedly (only opens once per
        downloader instance).  The fd is closed in :meth:`close`.
        """
        if self._part_fd is not None:
            return
        self._part_fd = os.open(str(self._part_path), os.O_RDWR)

    def is_complete(self) -> bool:
        return self._tracker is not None and self._tracker.is_complete()

    def download_piece(self, transport: TransportSession) -> bool:
        """Fetch one piece (or run the sequential fallback).

        Returns ``True`` if progress was made, ``False`` if no work is
        available (all pieces claimed or done).
        """
        tracker = self._tracker
        if tracker is None:
            raise RegetError("call prepare() first")

        if tracker.should_abort():
            return False

        claim = tracker.claim()
        if claim is None:
            return False

        idx, offset, length = claim
        reget_headers = {
            "Range": f"bytes={offset}-{offset + length - 1}",
        }
        if tracker.etag:
            reget_headers["If-Range"] = tracker.etag

        req_headers = merge_headers(self._extra_headers, {**DEFAULT_HEADERS, **reget_headers})

        # Streaming response body is scoped to this ``with`` (transport closes it).
        try:
            with transport.stream_get(
                self._url,
                headers=req_headers,
                options=self._transport_options_for_get(),
            ) as resp:
                status = resp.status_code

                if status == HTTPStatus.OK:
                    return self._handle_200(transport, resp, tracker, idx)

                if status == HTTPStatus.PARTIAL_CONTENT:
                    if _has_content_encoding(resp.headers):
                        tracker.release(idx)
                        raise ServerMisconfiguredError(
                            "206 with Content-Encoding after requesting identity — aborting piece mode"
                        )
                    return self._handle_206(transport, resp, tracker, idx, offset, length)

                if status == HTTPStatus.REQUESTED_RANGE_NOT_SATISFIABLE:
                    tracker.release(idx)
                    return self._handle_416(resp, tracker)

                tracker.release(idx)
                raise RegetError(f"unexpected HTTP {status} for {self._url}")

        except OSError:
            tracker.release(idx)
            raise
        except TransportError:
            tracker.release(idx)
            raise
        except (RegetError, ServerMisconfiguredError):
            raise

    def _handle_206(
        self,
        transport: TransportSession,
        resp: TransportResponse,
        tracker: PieceTracker,
        idx: PieceIndex,
        offset: ByteOffset,
        length: ByteLength,
    ) -> bool:
        """Normal piece mode: validate Content-Range, seek+write, cap at piece boundary.

        *resp* must stay open until this returns; :meth:`download_piece` wraps
        ``transport.stream_get(...)`` so the body is always closed on exit.
        """
        try:
            _validate_content_range(resp.headers, offset, length, tracker.total_length)
        except ContentRangeError:
            tracker.release(idx)
            raise

        # If we pinned an ETag at HEAD time, require the 206 to echo the same
        # one. Servers that ignore ``If-Range`` but still return 206 (e.g. a
        # cache layer without ``If-Range`` in the key, or an active-active
        # cluster with replica-level ETag skew) would otherwise let us stitch
        # bytes from a different version into the piece.
        if tracker.etag:
            resp_etag = parse_etag(resp.headers.get("ETag"))
            if resp_etag and resp_etag != tracker.etag:
                tracker.release(idx)
                raise ServerMisconfiguredError(
                    f"206 ETag mismatch: pinned {tracker.etag!r}, server returned {resp_etag!r}"
                )

        written = 0
        try:
            with self._part_path.open("r+b") as f:
                f.seek(offset)
                for chunk in resp.iter_raw_bytes(chunk_size=self._chunk_size):
                    if tracker.should_abort():
                        tracker.release(idx)
                        return False
                    remaining = length - written
                    if remaining <= 0:
                        break
                    to_write = chunk[:remaining]
                    f.write(to_write)
                    written += len(to_write)
        except OSError:
            tracker.release(idx)
            raise

        done_count = tracker.complete(idx)
        with self._lock:
            self._bytes_written += written

        if self._on_progress:
            try:
                self._on_progress(
                    ProgressUpdate(
                        piece_index=idx,
                        pieces_completed=done_count,
                        pieces_total=tracker.num_pieces,
                        bytes_written=self._bytes_written,
                        is_sequential=False,
                        progress_handle=transport.opaque_progress_handle(),
                    )
                )
            except Exception:  # noqa: BLE001
                # Don't let a user-provided callback crash the downloader.
                log.warning("progress callback failed", exc_info=True)

        self._maybe_flush(tracker)
        return True

    def _handle_200(
        self,
        transport: TransportSession,
        resp: TransportResponse,
        tracker: PieceTracker,
        original_idx: PieceIndex,
    ) -> bool:
        """Server ignored Range — swarm collapse, sequential stream.

        *resp* must stay open through :meth:`_sequential_stream`; the outer
        ``with transport.stream_get(...)`` in :meth:`download_piece` owns close().
        """
        tid = threading.get_ident()
        tracker.release(original_idx)

        if not tracker.enter_sequential_mode(tid):
            return False

        log.info("200 fallback: sequential stream for %s (tid=%d)", self._url, tid)
        try:
            self._sequential_stream(transport, resp, tracker)
        finally:
            tracker.clear_sequential_mode(tid)
        return True

    def _sequential_stream(
        self,
        transport: TransportSession,
        resp: TransportResponse,
        tracker: PieceTracker,
    ) -> None:
        """Stream the full response from byte 0, writing only missing pieces.

        Caller keeps *resp* open for the duration of the stream.

        Session byte accounting matches piece mode: each ``f.write`` bumps
        :attr:`PieceDownloader._bytes_written` under :attr:`PieceDownloader._lock`
        before any ``on_progress`` snapshot for pieces completed in that chunk.
        """
        piece_size = tracker.piece_size
        num_pieces = tracker.num_pieces
        stream_pos = 0

        with self._part_path.open("r+b") as f:
            for chunk in resp.iter_raw_bytes(chunk_size=self._chunk_size):
                chunk_len = len(chunk)
                start_piece = PieceIndex(stream_pos // piece_size)
                end_piece = PieceIndex(min((stream_pos + chunk_len - 1) // piece_size, num_pieces - 1))

                if not (tracker.is_done(start_piece) and tracker.is_done(end_piece)):
                    f.seek(stream_pos)
                    f.write(chunk)
                    # Keep ``_bytes_written`` in lockstep with disk writes (same contract as
                    # piece-mode) so ``on_progress`` always sees a cumulative session total,
                    # including under 200 sequential fallback and with threaded piece workers.
                    with self._lock:
                        self._bytes_written += chunk_len

                cur = start_piece
                while cur <= end_piece and cur < num_pieces:
                    piece_end = (cur + 1) * piece_size
                    if piece_end <= stream_pos + chunk_len and not tracker.is_done(cur):
                        done_count = tracker.complete(cur)
                        if self._on_progress:
                            try:
                                self._on_progress(
                                    ProgressUpdate(
                                        piece_index=cur,
                                        pieces_completed=done_count,
                                        pieces_total=tracker.num_pieces,
                                        bytes_written=self._bytes_written,
                                        is_sequential=True,
                                        progress_handle=transport.opaque_progress_handle(),
                                    )
                                )
                            except Exception:  # noqa: BLE001
                                # Don't let a user-provided callback crash the downloader.
                                log.warning("progress callback failed", exc_info=True)
                    cur = PieceIndex(cur + 1)

                stream_pos += chunk_len

            last = PieceIndex(num_pieces - 1)
            if num_pieces > 0 and stream_pos >= last * piece_size and not tracker.is_done(last):
                done_count = tracker.complete(last)
                if self._on_progress:
                    try:
                        self._on_progress(
                            ProgressUpdate(
                                piece_index=last,
                                pieces_completed=done_count,
                                pieces_total=tracker.num_pieces,
                                bytes_written=self._bytes_written,
                                is_sequential=True,
                                progress_handle=transport.opaque_progress_handle(),
                            )
                        )
                    except Exception:  # noqa: BLE001
                        # Don't let a user-provided callback crash the downloader.
                        log.warning("progress callback failed", exc_info=True)

        self._maybe_flush(tracker)

    def _handle_416(
        self,
        resp: TransportResponse,
        tracker: PieceTracker,
    ) -> bool:
        """Range not satisfiable — parse actual length, validate ETag."""
        h = resp.headers
        actual = _parse_content_range_total(h)
        if actual is not None and actual != tracker.total_length:
            new_meta = ServerMeta(
                etag=parse_etag(h.get("ETag")),
                total_length=actual,
                last_modified=h.get("Last-Modified"),
            )
            old_meta = ServerMeta(
                etag=tracker.etag,
                total_length=tracker.total_length,
            )
            if is_file_changed(old_meta, new_meta):
                raise RegetError(
                    f"file changed on server: was {tracker.total_length} bytes, now {actual} — restart required"
                )
        return False

    def _maybe_flush(self, tracker: PieceTracker) -> None:
        if tracker.should_flush(every_n=self._flush_every):
            try:
                tracker.flush_state(self._ctrl_path)
            except OSError:
                log.warning("failed to flush control file", exc_info=True)

    def finalize(self) -> DownloadResult:
        """Rename .part → final, delete .ctrl, compute SHA-256."""
        tracker = self._tracker
        if tracker is None or not tracker.is_complete():
            completed, total = tracker.progress() if tracker else (0, 0)
            return DownloadPartial(
                bytes_written=self._bytes_written,
                pieces_completed=completed,
                pieces_total=total,
                reason="download incomplete",
                elapsed=time.monotonic() - self._t0,
            )

        # Force-flush ctrl while the persistent .part fd is still open —
        # the fdatasync pairing inside flush_state relies on it.
        tracker.flush_state(self._ctrl_path, force=True)

        # Release the persistent fd before rename + unlink so nothing
        # holds the .part inode open past the atomic swap.
        if self._part_fd is not None:
            try:
                os.close(self._part_fd)
            except OSError:
                log.warning("failed to close .part fd", exc_info=True)
            self._part_fd = None
            tracker.set_part_fd(None)

        sha = HashBuilder.hash_file(str(self._part_path))

        _publish_part(self._part_path, self._dest)

        # Post-publish cleanup: the download has already succeeded (bytes
        # are at `dest`).  A failure to unlink the stale ctrl file is
        # cosmetic — the next prepare() discards orphaned ctrls when the
        # matching .part is absent.  Never let it turn a successful
        # download into a reported failure.
        try:
            self._ctrl_path.unlink(missing_ok=True)
        except OSError:
            log.warning(
                "failed to remove control file %s after publish (download still succeeded)",
                self._ctrl_path,
                exc_info=True,
            )

        completed, total = tracker.progress()
        return DownloadComplete(
            bytes_written=self._bytes_written,
            pieces_completed=completed,
            pieces_total=total,
            sha256=sha,
            etag=tracker.etag,
            content_type=self._server_meta.content_type,
            elapsed=time.monotonic() - self._t0,
        )

    def close(self) -> None:
        if self._part_fd is not None:
            try:
                os.close(self._part_fd)
            except OSError:
                log.warning("failed to close .part fd", exc_info=True)
            self._part_fd = None
            if self._tracker is not None:
                self._tracker.set_part_fd(None)

    def __enter__(self) -> PieceDownloader:
        return self

    def __exit__(self, *_: object) -> None:
        self.close()


def _fetch_transport_and_session(
    session: AnySession | None,
) -> tuple[TransportSession, object | None, bool]:
    """Return ``(transport, native_session, owned_session)`` for :func:`fetch`.

    When *owned_session* is True, *native_session* was created here and must be
    closed by the caller.
    """
    if session is None:
        try:
            import niquests
        except ImportError as e:
            msg = "fetch() needs niquests for a default session. Install with: pip install reget[niquests]"
            raise RegetError(msg) from e
        from reget.transport.niquests_adapter import NiquestsAdapter

        native = niquests.Session()
        return NiquestsAdapter(native), native, True

    if isinstance(session, TransportSession):
        return session, None, False

    return wrap_transport(session), session, False


@overload
def fetch(
    url: str,
    dest: str | Path,
    *,
    session: None = None,
    piece_size: int = _DEFAULT_PIECE_SIZE,
    extra_headers: dict[str, str] | None = None,
) -> DownloadResult: ...


@overload
def fetch(
    url: str,
    dest: str | Path,
    *,
    session: AnySession,
    piece_size: int = _DEFAULT_PIECE_SIZE,
    extra_headers: dict[str, str] | None = None,
) -> DownloadResult: ...


def fetch(
    url: str,
    dest: str | Path,
    *,
    session: AnySession | None = None,
    piece_size: int = _DEFAULT_PIECE_SIZE,
    extra_headers: dict[str, str] | None = None,
) -> DownloadResult:
    """Download a file with piece-tracked resume (single-threaded convenience).

    Requires ``niquests`` for a default session (``session=None``). For a
    caller-supplied client, pass a supported native session (``httpx.Client``,
    ``requests.Session``, or ``niquests.Session`` when the matching extra is
    installed) or any :class:`~reget.transport.protocols.TransportSession`
    implementation (e.g. :class:`~reget.transport.NiquestsAdapter`).

    Example::

        from reget import fetch
        result = fetch("https://example.com/big.zip", "/tmp/big.zip")
        print(result.sha256, result.bytes_written)
    """
    transport, native, owned_session = _fetch_transport_and_session(session)

    try:
        with PieceDownloader(
            url,
            Path(dest),
            piece_size=piece_size,
            extra_headers=extra_headers,
        ) as pd:
            pd.prepare(transport)
            while not pd.is_complete():
                if not pd.download_piece(transport):
                    break
            return pd.finalize()
    finally:
        if owned_session:
            close = getattr(native, "close", None)
            if callable(close):
                close()

"""Shared non-I/O logic for the sync and async download engines.

Both ``engine.fetch`` and ``async_engine.async_fetch`` delegate all
pure-logic work here: checkpoint reading, header building, response
interpretation, file allocation, chunk writing, flush, and
finalization.  The only thing each engine owns is the I/O boundary
(``with`` vs ``async with``, ``for`` vs ``async for``).
"""

from __future__ import annotations

import os
import time
from dataclasses import dataclass, field
from pathlib import Path

from reget._types import (
    ControlFileError,
    DownloadComplete,
    DownloadPartial,
    DownloadResult,
    ETag,
    HashBuilder,
    ServerMisconfiguredError,
    Url,
    parse_etag,
    parse_url,
)
from reget.alloc import allocate_file
from reget.content_range import parse_content_range
from reget.headers import DEFAULT_HEADERS, merge_headers
from reget.persist import (
    CTRL_VERSION,
    Checkpoint,
    ctrl_path_for,
    read_checkpoint,
    write_atomic,
)
from reget.transport.types import TransportHeaders

DEFAULT_CHUNK = 1 << 16  # 64 KiB
DEFAULT_FLUSH = 1 << 20  # 1 MiB
datasync = getattr(os, "fdatasync", os.fsync)

_HTTP_200 = 200
_HTTP_206 = 206
_HTTP_416 = 416


# ─── Dataclasses ──────────────────────────────────────────────────


@dataclass(frozen=True, slots=True)
class PreparedFetch:
    """Immutable context computed once before the HTTP request."""

    dest_path: Path
    parsed_url: Url
    part_path: Path
    ctrl_path: Path
    start: int
    cursor: int
    stored_etag: ETag
    request_byte: int
    merged_headers: dict[str, str]
    t0: float


@dataclass(slots=True)
class StreamPlan:
    """Mutable write-loop state initialized from the HTTP response."""

    start: int
    cursor: int
    extent: int | None
    etag: ETag
    resource_length: int | None
    content_type: str
    bytes_written: int = field(default=0, init=False)
    bytes_since_flush: int = field(default=0, init=False)


# ─── Preparation ──────────────────────────────────────────────────


def prepare_fetch(dest: str | Path, url: str) -> PreparedFetch:
    """Read checkpoint, build request headers, return immutable context."""
    t0 = time.monotonic()
    dest_path = Path(dest)
    parsed_url = parse_url(url)
    part_path = dest_path.with_suffix(dest_path.suffix + ".part")
    ctrl_path = ctrl_path_for(part_path)

    cp: Checkpoint | None = None
    if ctrl_path.exists():
        try:
            cp = read_checkpoint(ctrl_path)
        except ControlFileError:
            cp = None

    start = cp.start if cp else 0
    cursor = cp.valid_length if cp else 0
    stored_etag = cp.etag if cp else ETag("")

    request_byte = start + cursor
    req_hdrs: dict[str, str] = {"Range": f"bytes={request_byte}-"}
    if stored_etag:
        req_hdrs["If-Range"] = str(stored_etag)
    merged = merge_headers({}, {**DEFAULT_HEADERS, **req_hdrs})

    return PreparedFetch(
        dest_path=dest_path,
        parsed_url=parsed_url,
        part_path=part_path,
        ctrl_path=ctrl_path,
        start=start,
        cursor=cursor,
        stored_etag=stored_etag,
        request_byte=request_byte,
        merged_headers=merged,
        t0=t0,
    )


# ─── Response interpretation ──────────────────────────────────────


def handle_response(
    status: int,
    headers: TransportHeaders,
    prep: PreparedFetch,
) -> DownloadResult | StreamPlan:
    """Branch on status code; return a finished result or a streaming plan."""
    resp_etag = parse_etag(headers.get("ETag"))
    resp_ct = headers.get("Content-Type")

    if status == _HTTP_416:
        return _on_416(headers, prep, resp_etag=resp_etag, content_type=resp_ct)

    if status == _HTTP_206:
        return _plan_206(headers, prep, resp_etag=resp_etag, content_type=resp_ct)

    if status == _HTTP_200:
        return _plan_200(headers, resp_etag=resp_etag, content_type=resp_ct)

    raise ServerMisconfiguredError(f"unexpected HTTP {status}")


def _on_416(
    headers: TransportHeaders,
    prep: PreparedFetch,
    *,
    resp_etag: ETag,
    content_type: str,
) -> DownloadResult:
    cr_raw = headers.get("Content-Range")
    if cr_raw:
        cr = parse_content_range(cr_raw)
        if cr.is_unsatisfied and cr.instance_length is not None and cr.instance_length == prep.cursor:
            if prep.part_path.exists() and prep.part_path.stat().st_size > prep.cursor:
                with prep.part_path.open("r+b") as f:
                    f.truncate(prep.cursor)
            return finalize(
                prep.dest_path,
                prep.part_path,
                prep.ctrl_path,
                bytes_written=0,
                etag=resp_etag or prep.stored_etag,
                content_type=content_type,
                t0=prep.t0,
            )

    _reset_checkpoint(prep.ctrl_path, prep.parsed_url, prep.start)
    return DownloadPartial(
        bytes_written=0,
        valid_length=0,
        elapsed=time.monotonic() - prep.t0,
        reason="416 Range Not Satisfiable — checkpoint reset",
    )


def _plan_206(
    headers: TransportHeaders,
    prep: PreparedFetch,
    *,
    resp_etag: ETag,
    content_type: str,
) -> StreamPlan:
    cr_raw = headers.get("Content-Range")
    if not cr_raw:
        raise ServerMisconfiguredError("206 without Content-Range")
    cr = parse_content_range(cr_raw)
    if cr.is_unsatisfied:
        raise ServerMisconfiguredError("206 with unsatisfied Content-Range")
    assert cr.start is not None
    if cr.start != prep.request_byte:
        raise ServerMisconfiguredError(f"Content-Range start {cr.start} != requested {prep.request_byte}")

    new_etag = resp_etag or prep.stored_etag
    new_rl = cr.instance_length
    new_extent = (cr.instance_length - prep.start) if cr.instance_length is not None else None

    return StreamPlan(
        start=prep.start,
        cursor=prep.cursor,
        extent=new_extent,
        etag=new_etag,
        resource_length=new_rl,
        content_type=content_type,
    )


def _plan_200(
    headers: TransportHeaders,
    *,
    resp_etag: ETag,
    content_type: str,
) -> StreamPlan:
    cl_str = headers.get("Content-Length")
    resp_cl = int(cl_str) if cl_str.isdigit() else None

    return StreamPlan(
        start=0,
        cursor=0,
        extent=resp_cl,
        etag=resp_etag,
        resource_length=resp_cl,
        content_type=content_type,
    )


# ─── File + write helpers ─────────────────────────────────────────


def open_part_file(plan: StreamPlan, part_path: Path) -> int:
    """Open (or create) the ``.part`` file, allocate, and seek to cursor.

    Returns an ``os``-level file descriptor.  The caller MUST close it.
    """
    part_path.parent.mkdir(parents=True, exist_ok=True)
    fd = os.open(str(part_path), os.O_RDWR | os.O_CREAT, 0o644)
    try:
        if plan.extent is not None and plan.extent > 0:
            allocate_file(fd, total_length=plan.extent)
        os.lseek(fd, plan.cursor, os.SEEK_SET)
    except BaseException:
        os.close(fd)
        raise
    return fd


def write_chunk(
    fd: int,
    chunk: bytes,
    plan: StreamPlan,
    prep: PreparedFetch,
    flush_every: int,
) -> None:
    """Write *chunk* to *fd*, advance counters, and flush ctrl if threshold hit."""
    os.write(fd, chunk)
    n = len(chunk)
    plan.cursor += n
    plan.bytes_since_flush += n
    plan.bytes_written += n

    if plan.bytes_since_flush >= flush_every:
        datasync(fd)
        write_atomic(prep.ctrl_path, _make_checkpoint(plan, prep))
        plan.bytes_since_flush = 0


def after_stream(plan: StreamPlan, prep: PreparedFetch) -> DownloadResult:
    """Post-loop: check completeness, trim junk tail, finalize or return partial."""
    if plan.extent is not None and plan.cursor < plan.extent:
        write_atomic(prep.ctrl_path, _make_checkpoint(plan, prep))
        return DownloadPartial(
            bytes_written=plan.bytes_written,
            valid_length=plan.cursor,
            elapsed=time.monotonic() - prep.t0,
            reason="stream ended before extent reached",
        )

    actual = prep.part_path.stat().st_size
    if actual > plan.cursor:
        with prep.part_path.open("r+b") as f:
            f.truncate(plan.cursor)

    return finalize(
        prep.dest_path,
        prep.part_path,
        prep.ctrl_path,
        bytes_written=plan.bytes_written,
        etag=plan.etag,
        content_type=plan.content_type,
        t0=prep.t0,
    )


# ─── Finalization ─────────────────────────────────────────────────


def finalize(
    dest_path: Path,
    part_path: Path,
    ctrl_path: Path,
    *,
    bytes_written: int,
    etag: ETag,
    content_type: str,
    t0: float,
) -> DownloadComplete:
    """Rename ``.part`` → dest, delete ``.ctrl``, hash the result."""
    part_path.rename(dest_path)
    ctrl_path.unlink(missing_ok=True)
    sha = HashBuilder.hash_file(dest_path)
    return DownloadComplete(
        bytes_written=bytes_written,
        elapsed=time.monotonic() - t0,
        sha256=sha,
        etag=etag,
        content_type=content_type,
    )


def _reset_checkpoint(ctrl_path: Path, url: Url, start: int) -> None:
    ctrl_path.parent.mkdir(parents=True, exist_ok=True)
    write_atomic(
        ctrl_path,
        Checkpoint(
            version=CTRL_VERSION,
            url=url,
            start=start,
            extent=None,
            valid_length=0,
            etag=ETag(""),
            resource_length=None,
        ),
    )


def _make_checkpoint(plan: StreamPlan, prep: PreparedFetch) -> Checkpoint:
    return Checkpoint(
        version=CTRL_VERSION,
        url=prep.parsed_url,
        start=plan.start,
        extent=plan.extent,
        valid_length=plan.cursor,
        etag=plan.etag,
        resource_length=plan.resource_length,
    )

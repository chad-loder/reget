"""Async download engine: single-range, cursor-based, resumable.

Async mirror of :mod:`reget.engine`.  All non-I/O logic is shared via
:mod:`reget._engine_common`; only the session context manager and the
chunk iterator differ (``async with`` / ``async for``).
"""

from __future__ import annotations

import os
from pathlib import Path

from reget._engine_common import (
    DEFAULT_CHUNK,
    DEFAULT_FLUSH,
    StreamPlan,
    after_stream,
    datasync,
    handle_response,
    open_part_file,
    prepare_fetch,
    write_chunk,
)
from reget._types import DownloadResult, Url
from reget.transport.protocols import AsyncTransportSession


async def async_fetch(
    dest: str | Path,
    *,
    session: AsyncTransportSession,
    url: Url,
    chunk_size: int = DEFAULT_CHUNK,
    flush_every: int = DEFAULT_FLUSH,
) -> DownloadResult:
    """Async equivalent of :func:`reget.engine.fetch`.

    Returns :class:`DownloadComplete` on success or
    :class:`DownloadPartial` when the stream ends before all bytes
    arrive (the ``.part`` and ``.part.ctrl`` files remain on disk for
    the next call to resume from).
    """
    prep = prepare_fetch(dest, url)

    async with session.stream_get(prep.parsed_url, headers=prep.merged_headers) as resp:
        action = handle_response(resp.status_code, resp.headers, prep)

        if not isinstance(action, StreamPlan):
            return action

        plan = action
        fd = open_part_file(plan, prep.part_path)
        try:
            async for chunk in resp.aiter_raw_bytes(chunk_size=chunk_size):
                write_chunk(fd, chunk, plan, prep, flush_every)
            datasync(fd)
        finally:
            os.close(fd)

        return after_stream(plan, prep)

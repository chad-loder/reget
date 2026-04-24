"""Sync download engine: single-range, cursor-based, resumable.

One range, one session, one part file.  The caller borrows a
``TransportSession`` to the engine; the engine never closes it.
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
from reget.transport.protocols import TransportSession


def fetch(
    dest: str | Path,
    *,
    session: TransportSession,
    url: Url,
    chunk_size: int = DEFAULT_CHUNK,
    flush_every: int = DEFAULT_FLUSH,
) -> DownloadResult:
    """Download a single byte range to *dest*, resumably.

    Returns :class:`DownloadComplete` on success or
    :class:`DownloadPartial` when the stream ends before all bytes
    arrive (the ``.part`` and ``.part.ctrl`` files remain on disk for
    the next call to resume from).
    """
    prep = prepare_fetch(dest, url)

    with session.stream_get(prep.parsed_url, headers=prep.merged_headers) as resp:
        action = handle_response(resp.status_code, resp.headers, prep)

        if not isinstance(action, StreamPlan):
            return action

        plan = action
        fd = open_part_file(plan, prep.part_path)
        try:
            for chunk in resp.iter_raw_bytes(chunk_size=chunk_size):
                write_chunk(fd, chunk, plan, prep, flush_every)
            datasync(fd)
        finally:
            os.close(fd)

        return after_stream(plan, prep)

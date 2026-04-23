"""Tests for the ``HttpTest`` harness (``http_niquests`` + direct niquests calls)."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

pytest.importorskip("niquests")
import niquests

from tests.conftest import Request, ServerState, deterministic, repeated

if TYPE_CHECKING:
    from tests.conftest import HttpTest


def test_before_each_sees_request_index_and_method(http_niquests: HttpTest) -> None:
    """The hook observes HEAD and GET in order with monotonically growing indices."""
    http_niquests.serve(repeated(b"A", 256))

    seen: list[tuple[int, str, tuple[int, int] | None]] = []

    def record(req: Request, _state: ServerState) -> None:
        seen.append((req.index, req.method, req.range))

    http_niquests.before_each(record)

    niquests.head(http_niquests.url)
    niquests.get(http_niquests.url)
    niquests.get(http_niquests.url, headers={"Range": "bytes=0-31"})

    assert seen == [
        (0, "HEAD", None),
        (1, "GET", None),
        (2, "GET", (0, 31)),
    ]


def test_before_each_rotates_etag_and_content_per_request(http_niquests: HttpTest) -> None:
    """Simulate a cluster where each request hits a different backend.

    Each backend has its own content + ETag. The harness must serve a
    consistent (content, etag) pair within a single request even though
    the callback rotates them for every call.
    """
    size = 2048
    backends = [
        (deterministic(size, seed=1), '"v1"'),
        (deterministic(size, seed=2), '"v2"'),
    ]

    schedule = iter([0, 1, 0, 1])

    def rotate(_req: Request, state: ServerState) -> None:
        state.content, state.etag = backends[next(schedule)]

    http_niquests.before_each(rotate)

    # Four sequential range requests hit the four scheduled backends.
    observed: list[tuple[bytes, str]] = []
    for _ in range(4):
        resp = niquests.get(http_niquests.url, headers={"Range": f"bytes=0-{size - 1}"})
        assert resp.status_code == 206
        etag = resp.headers["ETag"]
        body = resp.content
        assert isinstance(etag, str)
        assert isinstance(body, bytes)
        observed.append((body, etag))

    expected = [backends[0], backends[1], backends[0], backends[1]]
    assert observed == expected


def test_before_each_can_condition_content_length_on_request_type(
    http_niquests: HttpTest,
) -> None:
    """``Content-Length`` sent on HEAD but omitted on 206 responses.

    The callback inspects ``req.range`` to decide per-request: no Range
    means an initial probe (send CL), Range present means a resume
    request (omit CL — relying on Content-Range alone).
    """
    data = repeated(b"Z", 4096)
    http_niquests.serve(data)

    def conditional_cl(req: Request, state: ServerState) -> None:
        state.omit_206_content_length = req.range is not None

    http_niquests.before_each(conditional_cl)

    head = niquests.head(http_niquests.url)
    assert head.headers.get("Content-Length") == str(len(data))

    full_get = niquests.get(http_niquests.url)
    assert full_get.status_code == 200
    assert full_get.headers.get("Content-Length") == str(len(data))

    range_get = niquests.get(http_niquests.url, headers={"Range": "bytes=0-1023"})
    assert range_get.status_code == 206
    assert "Content-Length" not in range_get.headers
    assert range_get.headers.get("Content-Range") == f"bytes 0-1023/{len(data)}"

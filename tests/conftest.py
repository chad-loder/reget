"""Pytest fixtures and ``HttpTest`` harness for integration tests.

The ``http`` fixture runs each test once per installed HTTP client (niquests,
requests, httpx). Use ``http_niquests`` for niquests-only cases.

``HttpTest`` wraps a threaded ``http.server`` on localhost, a temp destination
path, fault injection helpers, and shortcuts for ``fetch`` / ``PieceDownloader``.

``ThreadingHTTPServer`` is used so tests can control raw response bytes (e.g.
malformed framing) that higher-level HTTP test servers do not expose.
"""

from __future__ import annotations

import hashlib

# Aliased so the ``http`` pytest fixture below doesn't shadow the stdlib module.
import http.server as _http_server
import threading
import time
from collections.abc import Callable, Generator
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Literal, cast

import pytest

from reget import DownloadResult, PieceDownloader, fetch
from reget.control import ControlFile, ctrl_path_for, read_control
from reget.transport.native_session_types import SupportedNativeHttpSession
from reget.transport.protocols import TransportSession
from tests.transport_backends import LIVE_BACKENDS, close_native, make_native, make_transport


class Content(bytes):
    """``bytes`` subclass that knows its own SHA-256.

    Produced by ``repeated`` / ``deterministic``. Use anywhere tests
    construct a deterministic download body; pass straight to
    ``http.serve(...)`` (still a ``bytes``) and assert
    ``result.sha256 == data.sha256`` without re-hashing in the test.
    """

    @property
    def sha256(self) -> str:
        """Hex SHA-256 of this content."""
        return hashlib.sha256(self).hexdigest()


def repeated(byte: bytes, count: int) -> Content:
    """``Content`` of ``byte`` repeated ``count`` times.

    Example::

        data = repeated(b"X", 4096)
        http.serve(data)
        assert result.sha256 == data.sha256
    """
    if len(byte) != 1:
        msg = f"repeated() expects a single-byte bytes, got {byte!r}"
        raise ValueError(msg)
    return Content(byte * count)


def deterministic(size: int, *, seed: int = 0) -> Content:
    """Reproducible pseudo-random ``Content`` of length ``size``.

    Each byte is ``(i * 37 + seed + 11) & 0xFF`` — every index maps to
    a different byte, so off-by-one or piece-boundary bugs are visible
    in the bytes-equality assertion rather than only in the hash.
    """
    return Content(bytes(((i * 37 + seed + 11) & 0xFF) for i in range(size)))


@dataclass(frozen=True, slots=True)
class Request:
    """An incoming request as seen by the test server.

    Passed to ``before_each`` callbacks so they can set up the next
    response based on request index / method / Range without the test
    having to declare a closure counter.
    """

    index: int
    """0-based counter of requests received by this fixture."""

    method: Literal["HEAD", "GET"]

    range: tuple[int, int] | None
    """Parsed ``Range`` header as ``(start, end)``, or ``None``.

    ``end`` is the ``Range``'s upper bound as sent by the client, clamped
    to ``len(content) - 1`` if the client sent an open range. HEAD
    requests always have ``range=None``.
    """


BeforeEach = Callable[[Request, "ServerState"], None]
"""Callback signature for ``HttpTest.before_each``."""


@dataclass
class ServerState:
    """Per-test state for the mock HTTP handler.

    One instance lives inside the ``http`` fixture's closure. The handler
    reads it on every request, so ``before_each`` callbacks can mutate
    fields here to stage the next response.
    """

    content: bytes = b""
    etag: str = '"test-etag"'

    # Fault-injection knobs.
    force_200_enabled: bool = False
    inject_content_encoding: bool = False
    wrong_head_length: bool = False
    omit_head_content_length: bool = False
    omit_206_content_length: bool = False
    omit_206_content_range: bool = False
    lie_206_content_range: bool = False
    ignore_range_end: bool = False
    """Naive server: 206 responses return ``content[start:]`` regardless of
    the client's end byte, with ``Content-Length`` matching the oversend."""

    truncate_206_body_at: int | None = None
    """If set, the 206 body is cut off after this many bytes and the
    connection is closed — without adjusting ``Content-Length`` — to
    simulate a proxy timeout or upstream TCP reset mid-body."""

    truncate_200_body_at: int | None = None
    """Same as ``truncate_206_body_at`` but applied to the 200 path.
    Models an NGINX-style ``Range``-to-full upgrade that dies mid-stream."""

    lie_206_total_length: int | None = None
    """If set, the ``/TOTAL`` field of the 206 ``Content-Range`` reports
    this value instead of ``len(content)`` — simulates a server whose
    cache has a stale view of the resource size."""

    get_delay_seconds: float = 0.0

    # Per-request hook + request counter (both read/written under ``_lock``).
    before_each: BeforeEach | None = None
    request_counter: int = 0

    # Concurrency sensor (read by parallel tests to verify real parallelism).
    concurrent_gets: int = 0
    peak_concurrent_gets: int = 0

    _lock: threading.Lock = field(default_factory=threading.Lock)


class HttpTest:
    """Fluent test harness.

    Constructed by the ``http`` fixture; not intended to be instantiated
    directly by tests.

    The ``http`` fixture is parametrized over every installed HTTP stack
    (niquests / requests / httpx); ``transport`` and ``fetch()`` use that
    backend. Use ``http_niquests`` when a test is specific to niquests (e.g.
    connection-pool sharing on a single ``Session``).

    All ``serve``/fault-injection methods return ``self`` so they can be
    chained::

        http.serve(b"...").force_200().delay_gets(0.02)
    """

    def __init__(self, url: str, dest: Path, state: ServerState, *, backend: str) -> None:
        self.url = url
        self.dest = dest
        self._state = state
        self.backend = backend
        self._native: object | None = None

    def _ensure_native(self) -> object:
        if self._native is None:
            self._native = make_native(self.backend)
        return self._native

    @property
    def session(self) -> object:
        """Native client (``niquests.Session``, ``requests.Session``, or ``httpx.Client``)."""
        return self._ensure_native()

    @property
    def transport(self) -> TransportSession:
        """Transport adapter for :meth:`PieceDownloader.prepare` / ``download_piece``."""
        return make_transport(self.backend, self._ensure_native())

    def close_session(self) -> None:
        if self._native is not None:
            close_native(self._native)
            self._native = None

    # ---- Fluent server configuration (chainable) --------------------------

    def serve(self, content: bytes) -> HttpTest:
        """Set the body the server will return for GET requests."""
        self._state.content = content
        return self

    def set_etag(self, value: str) -> HttpTest:
        """Set the ETag the server returns on every response. Pass ``""``
        to model an origin that serves no ETag header at all."""
        self._state.etag = value
        return self

    def force_200(self) -> HttpTest:
        """Server ignores ``Range`` and always returns 200 + full body."""
        self._state.force_200_enabled = True
        return self

    def inject_content_encoding_on_206(self) -> HttpTest:
        """Server adds ``Content-Encoding: gzip`` to 206 responses."""
        self._state.inject_content_encoding = True
        return self

    def wrong_content_length_on_head(self) -> HttpTest:
        """HEAD reports ``Content-Length`` of ``len(content) + 999``."""
        self._state.wrong_head_length = True
        return self

    def omit_content_length_on_head(self) -> HttpTest:
        """HEAD response omits ``Content-Length`` entirely."""
        self._state.omit_head_content_length = True
        return self

    def omit_content_length_on_206(self) -> HttpTest:
        """206 responses omit ``Content-Length``."""
        self._state.omit_206_content_length = True
        return self

    def omit_content_range_on_206(self) -> HttpTest:
        """206 responses omit ``Content-Range``."""
        self._state.omit_206_content_range = True
        return self

    def lie_content_range_on_206(self) -> HttpTest:
        """206 responses report ``Content-Range`` shifted by one byte."""
        self._state.lie_206_content_range = True
        return self

    def ignore_range_end_on_206(self) -> HttpTest:
        """Naive server: 206 responses return ``content[start:]`` regardless
        of the client's requested end byte. ``Content-Length`` matches the
        oversend. Models CGI scripts and caching proxies that honor the
        ``start`` of a ``Range`` header but not the ``end``.
        """
        self._state.ignore_range_end = True
        return self

    def truncate_206_body_after(self, n: int) -> HttpTest:
        """Write ``n`` bytes of the 206 body, then close the socket. The
        ``Content-Length`` header still reports the full range length, so
        the client sees a premature EOF. Models proxy timeouts and
        upstream TCP resets mid-body.
        """
        self._state.truncate_206_body_at = n
        return self

    def truncate_200_body_after(self, n: int) -> HttpTest:
        """Write ``n`` bytes of a 200 response body, then close the socket.
        Combine with :meth:`force_200` to model an NGINX-style
        ``Range``-to-full upgrade whose upstream fetch dies mid-stream.
        """
        self._state.truncate_200_body_at = n
        return self

    def lie_about_total_length_on_206(self, total: int) -> HttpTest:
        """Report ``/{total}`` in the 206 ``Content-Range`` instead of the
        actual served content length. Models servers whose cache has a
        stale or inconsistent view of the resource size.
        """
        self._state.lie_206_total_length = total
        return self

    def delay_gets(self, seconds: float) -> HttpTest:
        """Each GET sleeps ``seconds`` before responding."""
        self._state.get_delay_seconds = seconds
        return self

    def before_each(self, fn: BeforeEach) -> HttpTest:
        """Register a callback invoked before each incoming request.

        ``fn`` is called as ``fn(request, state)`` under the fixture's
        state lock, so it can mutate ``state`` (content, etag, any of
        the fault-injection fields) to stage the response for this
        specific request. After ``fn`` returns, the handler snapshots
        the fields it needs and releases the lock before writing the
        response — so concurrent requests each see a consistent view
        even when ``fn`` rotates state on every call.

        The ``request`` argument carries ``request.index`` (0-based),
        ``request.method``, and ``request.range``, so tests don't need
        to declare their own counters for "flip on request N" or
        "behave differently on HEAD vs a range GET" scenarios.
        """
        self._state.before_each = fn
        return self

    # ---- Download conveniences -------------------------------------------

    def fetch(self, **kwargs: Any) -> DownloadResult:
        """``reget.fetch`` pre-filled with the fixture's ``url`` + ``dest`` and backend session."""
        return fetch(
            self.url,
            self.dest,
            session=cast(SupportedNativeHttpSession, self._ensure_native()),
            **kwargs,
        )

    def downloader(self, **kwargs: Any) -> PieceDownloader:
        """``PieceDownloader`` pre-filled with the fixture's ``url`` + ``dest``."""
        return PieceDownloader(self.url, self.dest, **kwargs)

    # ---- Output inspection -----------------------------------------------

    @property
    def output(self) -> bytes:
        """Final on-disk contents of ``dest`` after a completed download."""
        return self.dest.read_bytes()

    @property
    def part_path(self) -> Path:
        """Path to the ``.part`` file (in-progress download data)."""
        return self.dest.with_suffix(self.dest.suffix + ".part")

    @property
    def ctrl_path(self) -> Path:
        """Path to the ``.part.ctrl`` file (piece-bitmap sidecar)."""
        return ctrl_path_for(self.part_path)

    def read_ctrl(self) -> ControlFile:
        """Parse the current ``.part.ctrl`` file."""
        return read_control(self.ctrl_path)

    @property
    def peak_concurrent_gets(self) -> int:
        """Maximum number of in-flight GETs ever observed concurrently."""
        return self._state.peak_concurrent_gets


@dataclass(frozen=True, slots=True)
class _Snapshot:
    """Per-request snapshot of the fields the handler needs to serve one response.

    Taken under the state lock immediately after ``before_each`` runs,
    so even concurrent requests that rotate ``state.content`` /
    ``state.etag`` get a consistent view for the duration of their own
    response.
    """

    content: bytes
    etag: str
    force_200: bool
    inject_content_encoding: bool
    wrong_head_length: bool
    omit_head_content_length: bool
    omit_206_content_length: bool
    omit_206_content_range: bool
    lie_206_content_range: bool
    ignore_range_end: bool
    truncate_206_body_at: int | None
    truncate_200_body_at: int | None
    lie_206_total_length: int | None
    get_delay_seconds: float


def _parse_range(header: str, content_len: int) -> tuple[int, int] | None:
    """Return ``(start, end)`` parsed from a ``Range`` header, or ``None``.

    ``end`` is clamped to ``content_len - 1`` for open-ended ranges.
    Returns ``None`` for absent or malformed headers.
    """
    if not header:
        return None
    try:
        start_s, end_s = header.replace("bytes=", "").split("-")
        start = int(start_s)
        end = int(end_s) if end_s else content_len - 1
    except (ValueError, IndexError):
        return None
    return start, end


def _make_handler(state: ServerState) -> type[_http_server.BaseHTTPRequestHandler]:
    """Build a request-handler class that reads from a per-test state object."""

    def _begin(method: Literal["HEAD", "GET"], range_hdr: str) -> _Snapshot:
        """Run the per-request hook and snapshot the state fields we'll serve from.

        Held under ``state._lock`` so a ``before_each`` that rotates
        state can't race another thread that's mid-snapshot.
        """
        with state._lock:
            parsed_range = _parse_range(range_hdr, len(state.content))
            request = Request(
                index=state.request_counter,
                method=method,
                range=parsed_range,
            )
            state.request_counter += 1
            if state.before_each is not None:
                state.before_each(request, state)
            return _Snapshot(
                content=state.content,
                etag=state.etag,
                force_200=state.force_200_enabled,
                inject_content_encoding=state.inject_content_encoding,
                wrong_head_length=state.wrong_head_length,
                omit_head_content_length=state.omit_head_content_length,
                omit_206_content_length=state.omit_206_content_length,
                omit_206_content_range=state.omit_206_content_range,
                lie_206_content_range=state.lie_206_content_range,
                ignore_range_end=state.ignore_range_end,
                truncate_206_body_at=state.truncate_206_body_at,
                truncate_200_body_at=state.truncate_200_body_at,
                lie_206_total_length=state.lie_206_total_length,
                get_delay_seconds=state.get_delay_seconds,
            )

    class _TestHandler(_http_server.BaseHTTPRequestHandler):
        def do_HEAD(self) -> None:
            snap = _begin("HEAD", "")
            self.send_response(200)
            if not snap.omit_head_content_length:
                length = len(snap.content) + (999 if snap.wrong_head_length else 0)
                self.send_header("Content-Length", str(length))
            self.send_header("ETag", snap.etag)
            self.send_header("Content-Type", "application/octet-stream")
            self.end_headers()

        def do_GET(self) -> None:
            range_hdr = self.headers.get("Range", "")
            snap = _begin("GET", range_hdr)
            with state._lock:
                state.concurrent_gets += 1
                state.peak_concurrent_gets = max(
                    state.peak_concurrent_gets,
                    state.concurrent_gets,
                )
            try:
                self._serve(snap, range_hdr)
            finally:
                with state._lock:
                    state.concurrent_gets -= 1

        def _serve(self, snap: _Snapshot, range_hdr: str) -> None:
            if snap.get_delay_seconds:
                time.sleep(snap.get_delay_seconds)

            parsed = _parse_range(range_hdr, len(snap.content))
            if snap.force_200 or parsed is None:
                self._send_full(snap)
                return

            start, end = parsed
            if start >= len(snap.content):
                self._send_416(snap)
                return

            end = min(end, len(snap.content) - 1)
            # Naive server mode: return everything from start to EOF,
            # regardless of the client's requested end byte.
            actual_end = len(snap.content) - 1 if snap.ignore_range_end else end
            chunk = snap.content[start : actual_end + 1]

            total = snap.lie_206_total_length if snap.lie_206_total_length is not None else len(snap.content)

            self.send_response(206)
            if snap.lie_206_content_range:
                self.send_header(
                    "Content-Range",
                    f"bytes {start + 1}-{actual_end + 1}/{total}",
                )
            elif not snap.omit_206_content_range:
                self.send_header(
                    "Content-Range",
                    f"bytes {start}-{actual_end}/{total}",
                )
            if not snap.omit_206_content_length:
                self.send_header("Content-Length", str(len(chunk)))
            self.send_header("ETag", snap.etag)
            if snap.inject_content_encoding:
                self.send_header("Content-Encoding", "gzip")
            self.end_headers()
            if snap.truncate_206_body_at is not None:
                # Write only the requested prefix, then slam the socket
                # shut to simulate a mid-body connection reset / proxy
                # timeout. Content-Length still reports the full range
                # so the client expects more bytes than arrive.
                self.wfile.write(chunk[: snap.truncate_206_body_at])
                self.wfile.flush()
                self.connection.close()
                # Tell BaseHTTPRequestHandler not to try to keep-alive.
                self.close_connection = True
            else:
                self.wfile.write(chunk)

        def _send_full(self, snap: _Snapshot) -> None:
            self.send_response(200)
            self.send_header("Content-Length", str(len(snap.content)))
            self.send_header("ETag", snap.etag)
            self.end_headers()
            if snap.truncate_200_body_at is not None and self.command == "GET":
                self.wfile.write(snap.content[: snap.truncate_200_body_at])
                self.wfile.flush()
                self.connection.close()
                self.close_connection = True
            else:
                self.wfile.write(snap.content)

        def _send_416(self, snap: _Snapshot) -> None:
            self.send_response(416)
            self.send_header("Content-Range", f"bytes */{len(snap.content)}")
            self.send_header("ETag", snap.etag)
            self.end_headers()

        def log_message(self, format: str, *args: object) -> None:
            # Silence the default stderr access log.
            pass

    return _TestHandler


class _ThreadingHTTPServer(_http_server.ThreadingHTTPServer):
    """Threaded HTTP server used by the ``http`` fixture.

    ``daemon_threads`` ensures dangling request handlers don't block test
    teardown; ``allow_reuse_address`` guards against TIME_WAIT flakes when
    many tests run in rapid succession.
    """

    daemon_threads = True
    allow_reuse_address = True


def _yield_live_http_harness(tmp_path: Path, backend: str) -> Generator[HttpTest]:
    state = ServerState()
    handler_cls = _make_handler(state)

    srv = _ThreadingHTTPServer(("127.0.0.1", 0), handler_cls)
    port = srv.server_address[1]
    thread = threading.Thread(target=srv.serve_forever, daemon=True)
    thread.start()

    ht: HttpTest | None = None
    try:
        ht = HttpTest(
            url=f"http://127.0.0.1:{port}/file.bin",
            dest=tmp_path / "out.bin",
            state=state,
            backend=backend,
        )
        yield ht
    finally:
        if ht is not None:
            ht.close_session()
        srv.shutdown()
        srv.server_close()


@pytest.fixture(params=LIVE_BACKENDS)
def http(tmp_path: Path, request: pytest.FixtureRequest) -> Generator[HttpTest]:
    """Per-test HTTP harness, once per live transport backend (N-matrix)."""
    pytest.importorskip(request.param)
    yield from _yield_live_http_harness(tmp_path, request.param)


@pytest.fixture()
def http_niquests(tmp_path: Path) -> Generator[HttpTest]:
    """Like ``http`` but always niquests — for tests that assert on ``Session`` pooling."""
    pytest.importorskip("niquests")
    yield from _yield_live_http_harness(tmp_path, "niquests")

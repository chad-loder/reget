"""Tests for the ``reget`` command-line interface.

Uses the same mock HTTP server pattern as ``test_downloader.py``.
"""

from __future__ import annotations

import hashlib
import http.server
import importlib
import importlib.util
import threading
from collections.abc import Generator
from pathlib import Path
from typing import ClassVar

import pytest

from reget.cli import (
    build_native_http_client,
    build_parser,
    default_output,
    format_bytes,
    format_duration,
    main,
    parse_header,
    parse_size,
)

# ---------------------------------------------------------------------------
# Unit tests for helpers
# ---------------------------------------------------------------------------


class TestParseSize:
    def test_plain_integer(self) -> None:
        assert parse_size("1024") == 1024

    def test_kilobyte(self) -> None:
        assert parse_size("4K") == 4096
        assert parse_size("4KB") == 4096
        assert parse_size("4KiB") == 4096

    def test_megabyte(self) -> None:
        assert parse_size("1M") == 1 << 20
        assert parse_size("2MiB") == 2 << 20

    def test_gigabyte(self) -> None:
        assert parse_size("1G") == 1 << 30

    def test_fractional(self) -> None:
        assert parse_size("1.5M") == int(1.5 * (1 << 20))

    def test_case_insensitive(self) -> None:
        assert parse_size("1m") == 1 << 20

    def test_rejects_empty(self) -> None:
        with pytest.raises(Exception, match="empty"):
            parse_size("")

    def test_rejects_bad_suffix(self) -> None:
        with pytest.raises(Exception, match="unknown size suffix"):
            parse_size("4X")

    def test_rejects_zero(self) -> None:
        with pytest.raises(Exception, match="must be positive"):
            parse_size("0")


class TestParseHeader:
    def test_basic(self) -> None:
        assert parse_header("Cookie: x=1") == ("Cookie", "x=1")

    def test_trims_whitespace(self) -> None:
        assert parse_header("  X-Foo  :   bar  ") == ("X-Foo", "bar")

    def test_value_with_colon(self) -> None:
        assert parse_header("X-Host: host:8080") == ("X-Host", "host:8080")

    def test_rejects_no_colon(self) -> None:
        with pytest.raises(Exception, match="Name: Value"):
            parse_header("not-a-header")

    def test_rejects_empty_name(self) -> None:
        with pytest.raises(Exception, match="empty header name"):
            parse_header(": value")


class TestDefaultOutput:
    def test_basic(self) -> None:
        assert default_output("https://example.com/file.zip") == "file.zip"

    def test_with_query(self) -> None:
        assert default_output("https://x.test/a/b/c.iso?v=1") == "c.iso"

    def test_no_path(self) -> None:
        assert default_output("https://example.com") == "index.html"

    def test_trailing_slash(self) -> None:
        assert default_output("https://example.com/dir/") == "dir"


class TestFormatters:
    def test_bytes_small(self) -> None:
        assert format_bytes(512) == "512 B"

    def test_bytes_kib(self) -> None:
        assert format_bytes(2048).endswith("KiB")

    def test_bytes_mib(self) -> None:
        assert "MiB" in format_bytes(5 * 1024 * 1024)

    def test_duration_seconds(self) -> None:
        assert format_duration(12.5) == "12.5s"

    def test_duration_minutes(self) -> None:
        assert format_duration(90) == "1m30s"

    def test_duration_hours(self) -> None:
        assert format_duration(3725) == "1h02m05s"


class TestArgparse:
    def test_help_exits_zero(self, capsys: pytest.CaptureFixture[str]) -> None:
        parser = build_parser()
        with pytest.raises(SystemExit) as exc:
            parser.parse_args(["-h"])
        assert exc.value.code == 0
        out = capsys.readouterr().out
        assert "reget" in out
        assert "--proxy" in out

    def test_version(self, capsys: pytest.CaptureFixture[str]) -> None:
        parser = build_parser()
        with pytest.raises(SystemExit):
            parser.parse_args(["-V"])
        out = capsys.readouterr().out
        assert out.startswith("reget ")

    def test_headers_repeatable(self) -> None:
        parser = build_parser()
        args = parser.parse_args(["-H", "A: 1", "-H", "B: 2", "http://x"])
        assert args.header == [("A", "1"), ("B", "2")]

    def test_aliases(self) -> None:
        parser = build_parser()
        args = parser.parse_args(["-s", "http://x"])
        assert args.quiet is True
        args = parser.parse_args(["--silent", "http://x"])
        assert args.quiet is True
        args = parser.parse_args(["-q", "http://x"])
        assert args.quiet is True


# ---------------------------------------------------------------------------
# Integration tests against a mock HTTP server
# ---------------------------------------------------------------------------


class _FileHandler(http.server.BaseHTTPRequestHandler):
    content: ClassVar[bytes] = b""
    require_header: ClassVar[tuple[str, str] | None] = None
    seen_headers: ClassVar[list[dict[str, str]]] = []

    def do_HEAD(self) -> None:
        self._record_headers()
        self.send_response(200)
        self.send_header("Content-Length", str(len(self.content)))
        self.send_header("ETag", '"cli-test"')
        self.send_header("Content-Type", "application/octet-stream")
        self.end_headers()

    def do_GET(self) -> None:
        self._record_headers()
        if self.require_header:
            name, value = self.require_header
            if self.headers.get(name) != value:
                self.send_response(403)
                self.end_headers()
                return

        range_hdr = self.headers.get("Range", "")
        if not range_hdr:
            self.send_response(200)
            self.send_header("Content-Length", str(len(self.content)))
            self.send_header("ETag", '"cli-test"')
            self.end_headers()
            self.wfile.write(self.content)
            return

        spec = range_hdr.replace("bytes=", "")
        start_s, end_s = spec.split("-")
        start = int(start_s)
        end = int(end_s) if end_s else len(self.content) - 1
        end = min(end, len(self.content) - 1)
        slice_data = self.content[start : end + 1]
        self.send_response(206)
        self.send_header("Content-Range", f"bytes {start}-{end}/{len(self.content)}")
        self.send_header("Content-Length", str(len(slice_data)))
        self.send_header("ETag", '"cli-test"')
        self.end_headers()
        self.wfile.write(slice_data)

    def _record_headers(self) -> None:
        self.seen_headers.append(dict(self.headers.items()))

    def log_message(self, format: str, *args: object) -> None:
        pass


@pytest.fixture()
def server() -> Generator[str]:
    _FileHandler.require_header = None
    _FileHandler.seen_headers = []
    srv = http.server.HTTPServer(("127.0.0.1", 0), _FileHandler)
    port = srv.server_address[1]
    t = threading.Thread(target=srv.serve_forever, daemon=True)
    t.start()
    yield f"http://127.0.0.1:{port}/payload.bin"
    srv.shutdown()


def test_cli_basic_download(tmp_path: Path, server: str) -> None:
    data = b"hello, reget" * 1000
    _FileHandler.content = data
    dest = tmp_path / "out.bin"

    rc = main(
        [
            "-o",
            str(dest),
            "--piece-size",
            "1K",
            "-q",
            server,
        ]
    )
    assert rc == 0
    assert dest.read_bytes() == data


def test_cli_default_output_from_url(
    tmp_path: Path,
    server: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _FileHandler.content = b"x" * 4096
    monkeypatch.chdir(tmp_path)

    rc = main(["--piece-size", "1K", "-q", server])
    assert rc == 0
    assert (tmp_path / "payload.bin").exists()


def test_cli_output_dir(tmp_path: Path, server: str) -> None:
    _FileHandler.content = b"y" * 2048
    out_dir = tmp_path / "nested" / "downloads"

    rc = main(
        [
            "--output-dir",
            str(out_dir),
            "--piece-size",
            "1K",
            "-q",
            server,
        ]
    )
    assert rc == 0
    assert (out_dir / "payload.bin").exists()


def test_cli_custom_header_sent(tmp_path: Path, server: str) -> None:
    data = b"guarded content" * 100
    _FileHandler.content = data
    _FileHandler.require_header = ("X-Api-Key", "secret-123")
    dest = tmp_path / "out.bin"

    rc = main(
        [
            "-o",
            str(dest),
            "-H",
            "X-Api-Key: secret-123",
            "--piece-size",
            "1K",
            "-q",
            server,
        ]
    )
    assert rc == 0
    assert dest.read_bytes() == data
    sent = [h.get("X-Api-Key") for h in _FileHandler.seen_headers]
    assert "secret-123" in sent


def test_cli_missing_header_rejects(tmp_path: Path, server: str) -> None:
    _FileHandler.content = b"data"
    _FileHandler.require_header = ("X-Api-Key", "secret")
    dest = tmp_path / "out.bin"

    rc = main(
        [
            "-o",
            str(dest),
            "--piece-size",
            "1K",
            "-q",
            server,
        ]
    )
    assert rc == 1  # EXIT_ERROR from HEAD 403


def test_cli_user_agent_sent(tmp_path: Path, server: str) -> None:
    _FileHandler.content = b"z" * 1024
    dest = tmp_path / "out.bin"

    rc = main(
        [
            "-o",
            str(dest),
            "-A",
            "my-bot/2.0",
            "--piece-size",
            "1K",
            "-q",
            server,
        ]
    )
    assert rc == 0
    uas = [h.get("User-Agent") for h in _FileHandler.seen_headers]
    assert "my-bot/2.0" in uas


def test_cli_no_url_is_usage_error(capsys: pytest.CaptureFixture[str]) -> None:
    rc = main([])
    assert rc == 2
    err = capsys.readouterr().err
    assert "URL is required" in err


@pytest.mark.parametrize(
    "bad_url",
    ["ftp://example.com/", "not-a-url", "file:///etc/passwd", "http://"],
)
def test_cli_invalid_url_is_usage_error(
    bad_url: str,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    """URLs that fail :func:`parse_url` should exit 2 with a clean message,
    not bubble a ValueError traceback out of ``PieceDownloader.__init__``."""
    rc = main(["-o", str(tmp_path / "out.bin"), bad_url])
    assert rc == 2
    err = capsys.readouterr().err
    assert "invalid URL" in err


def test_cli_oserror_on_mkdir_is_clean_error(
    tmp_path: Path,
    server: str,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Regression for M3 from the file-I/O audit: a ``PermissionError``
    raised by ``Path.mkdir`` inside ``resolve_destination`` must surface
    as a clean CLI error (``EXIT_ERROR`` + stderr message), not a raw
    Python traceback."""
    _FileHandler.content = b"x" * 256
    out_dir = tmp_path / "forbidden"

    orig_mkdir = Path.mkdir

    def fake_mkdir(self: Path, *args: object, **kwargs: object) -> None:
        if self == out_dir:
            raise PermissionError("access denied")
        return orig_mkdir(self, *args, **kwargs)  # type: ignore[arg-type]

    monkeypatch.setattr(Path, "mkdir", fake_mkdir)

    rc = main(["--output-dir", str(out_dir), "-q", server])

    assert rc == 1
    err = capsys.readouterr().err
    assert "could not prepare output path" in err
    assert "access denied" in err


def test_cli_oserror_on_prepare_is_clean_error(
    tmp_path: Path,
    server: str,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """OSError raised while allocating the .part file (e.g. ENOSPC, bad
    path) must be converted to an ``EXIT_ERROR`` with a readable message
    rather than a bare traceback."""
    _FileHandler.content = b"x" * 256
    dest = tmp_path / "out.bin"

    import reget.downloader as dl

    def boom_allocate(_fd: int, *, total_length: int) -> str:
        raise OSError(28, f"No space left on device (wanted {total_length} bytes)")

    monkeypatch.setattr(dl, "allocate_file", boom_allocate)

    rc = main(["-o", str(dest), "-q", server])

    assert rc == 1
    err = capsys.readouterr().err
    assert "i/o error during preparation" in err
    assert "No space left on device" in err


def test_cli_sha256_matches_content(tmp_path: Path, server: str) -> None:
    data = b"checksum me" * 5000
    _FileHandler.content = data
    dest = tmp_path / "out.bin"

    rc = main(
        [
            "-o",
            str(dest),
            "--piece-size",
            "1K",
            "-q",
            server,
        ]
    )
    assert rc == 0
    assert hashlib.sha256(dest.read_bytes()).hexdigest() == hashlib.sha256(data).hexdigest()


def test_build_parser_http_backend_defaults_to_niquests() -> None:
    p = build_parser()
    args = p.parse_args(["-o", "out.bin", "https://example.com/f"])
    assert args.http_backend == "niquests"


def test_cli_http_backend_missing_package_exits_cleanly(
    tmp_path: Path,
    server: str,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _FileHandler.content = b"x" * 256
    dest = tmp_path / "out.bin"

    real_find_spec = importlib.util.find_spec

    def fake_find_spec(name: str) -> object:
        if name == "httpx":
            return None
        return real_find_spec(name)

    monkeypatch.setattr(importlib.util, "find_spec", fake_find_spec)

    rc = main(["--http-backend", "httpx", "-o", str(dest), "-q", server])
    assert rc == 1
    err = capsys.readouterr().err
    assert "httpx" in err.lower()
    assert "pip install" in err.lower()


@pytest.mark.parametrize("backend", ["httpx", "requests"])
def test_cli_download_succeeds_with_optional_http_backend(
    backend: str,
    tmp_path: Path,
    server: str,
) -> None:
    pytest.importorskip(backend)
    _FileHandler.content = b"y" * 512
    dest = tmp_path / f"via-{backend}.bin"

    rc = main(
        [
            "--http-backend",
            backend,
            "-o",
            str(dest),
            "--piece-size",
            "256",
            "-q",
            server,
        ]
    )
    assert rc == 0
    assert dest.read_bytes() == _FileHandler.content


def test_build_native_http_client_accepts_each_installed_backend(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Smoke-test dynamic construction; skip backends not installed."""
    import argparse

    for backend in ("niquests", "httpx", "requests"):
        try:
            importlib.import_module(backend)
        except ModuleNotFoundError:
            continue
        args = argparse.Namespace(
            http_backend=backend,
            proxy=None,
            insecure=False,
        )
        native = build_native_http_client(args)
        try:
            assert native is not None
        finally:
            close = getattr(native, "close", None)
            if callable(close):
                close()

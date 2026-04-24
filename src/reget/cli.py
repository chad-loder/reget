"""Minimal curl-compatible CLI for one-off reget downloads.

Uses one optional HTTP stack at runtime (``--http-backend``: ``niquests``,
``requests``, ``httpx``, or ``urllib3``; default ``niquests``).  Install a
matching extra (e.g. ``reget[niquests]``).

Exit codes follow UNIX convention::

    0   success
    1   generic download / HTTP error
    2   usage error
    130 interrupted (SIGINT / SIGTERM)
"""

from __future__ import annotations

import argparse
import importlib.util
import logging
import os
import signal
import sys
import threading
import time
from enum import StrEnum
from pathlib import Path
from types import FrameType
from typing import TYPE_CHECKING, Final, assert_never
from urllib.parse import urlparse

from reget._types import (
    DownloadComplete,
    DownloadPartial,
    RegetError,
    ServerMisconfiguredError,
    Url,
    parse_url,
)
from reget._version import __version__
from reget.transport.errors import TransportError, TransportHTTPError, TransportTLSError
from reget.transport.protocols import TransportSession

if TYPE_CHECKING:
    from collections.abc import Sequence

log = logging.getLogger("reget.cli")

EXIT_OK = 0
EXIT_ERROR = 1
EXIT_USAGE = 2
EXIT_INTERRUPTED = 130

_SEC_PER_MIN = 60
_MIN_PER_HOUR = 60
_MAX_RETRIES = 20

_SIZE_SUFFIXES: dict[str, int] = {
    "": 1,
    "B": 1,
    "K": 1024,
    "KB": 1024,
    "KIB": 1024,
    "M": 1024**2,
    "MB": 1024**2,
    "MIB": 1024**2,
    "G": 1024**3,
    "GB": 1024**3,
    "GIB": 1024**3,
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def parse_size(value: str) -> int:
    """Parse a human size like ``1M``, ``512K``, ``4GiB`` into bytes."""
    raw = value.strip().upper()
    if not raw:
        raise argparse.ArgumentTypeError("empty size")
    digits = raw
    suffix = ""
    for i, ch in enumerate(raw):
        if not (ch.isdigit() or ch == "."):
            digits = raw[:i]
            suffix = raw[i:]
            break
    if suffix not in _SIZE_SUFFIXES:
        raise argparse.ArgumentTypeError(f"unknown size suffix: {suffix!r}")
    try:
        n = float(digits) if digits else 0.0
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"invalid size: {value!r}") from exc
    out = int(n * _SIZE_SUFFIXES[suffix])
    if out <= 0:
        raise argparse.ArgumentTypeError(f"size must be positive: {value!r}")
    return out


def parse_header(raw: str) -> tuple[str, str]:
    """Parse a ``Name: Value`` header string (curl-compatible)."""
    if ":" not in raw:
        raise argparse.ArgumentTypeError(f"header must be 'Name: Value', got {raw!r}")
    name, _, value = raw.partition(":")
    name = name.strip()
    value = value.strip()
    if not name:
        raise argparse.ArgumentTypeError(f"empty header name in {raw!r}")
    return name, value


def default_output(url: str) -> str:
    """Derive a local filename from a URL, curl ``-O`` style."""
    path = urlparse(url).path
    name = path.rstrip("/").rsplit("/", 1)[-1] if path else ""
    return name or "index.html"


class ByteStandard(StrEnum):
    IEC = "IEC"
    SI = "SI"


_BYTE_CONFIG: Final = {
    ByteStandard.IEC: (1024.0, ("B", "KiB", "MiB", "GiB", "TiB", "PiB", "EiB", "ZiB", "YiB")),
    ByteStandard.SI: (1000.0, ("B", "KB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB")),
}


def format_bytes(size: float, std: ByteStandard = ByteStandard.IEC, prec: int = 1) -> str:
    """Short human-readable byte count (``1.5 MiB``)."""
    if size < 0:
        raise ValueError("Negative size")
    base, units = _BYTE_CONFIG[std]
    mag = 0
    while size >= base and mag < len(units) - 1:
        size /= base
        mag += 1
    return f"{size:.{prec}f} {units[mag]}"


def format_duration(seconds: float) -> str:
    if seconds < _SEC_PER_MIN:
        return f"{seconds:.1f}s"
    m, s = divmod(int(seconds), _SEC_PER_MIN)
    if m < _MIN_PER_HOUR:
        return f"{m}m{s:02d}s"
    h, m = divmod(m, _MIN_PER_HOUR)
    return f"{h}h{m:02d}m{s:02d}s"


# ---------------------------------------------------------------------------
# Output / progress
# ---------------------------------------------------------------------------


class Printer:
    """Stderr-only info / error printer.

    curl writes content to stdout and status to stderr; we do the same so
    pipelines stay grep-friendly.
    """

    def __init__(self, *, quiet: bool) -> None:
        self.quiet = quiet

    def info(self, msg: str) -> None:
        if self.quiet:
            return
        sys.stderr.write(msg + "\n")
        sys.stderr.flush()

    def err(self, msg: str) -> None:
        sys.stderr.write(f"reget: {msg}\n")
        sys.stderr.flush()


# ---------------------------------------------------------------------------
# Argparse
# ---------------------------------------------------------------------------


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="reget",
        description=(
            "Resumable, cursor-based HTTP downloader. "
            "Writes <file>.part + <file>.part.ctrl during transfer; "
            "re-running the same command resumes where it stopped."
        ),
        epilog=(
            "Examples:\n"
            "  reget -o file.iso https://example.com/file.iso\n"
            "  reget --http-backend httpx -o out.bin https://example.com/file.bin\n"
            "  reget -x socks5h://127.0.0.1:9050 http://abc.onion/blob.bin\n"
            "  reget -H 'Cookie: x=1' -A 'my-bot/1.0' https://host/f.zip\n"
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("url", nargs="?", help="URL to download")

    out = parser.add_argument_group("output")
    out.add_argument(
        "-o",
        "--output",
        metavar="FILE",
        help="write output to FILE (default: derived from URL)",
    )
    out.add_argument(
        "-O",
        "--remote-name",
        action="store_true",
        help="use URL basename as output filename (default when -o is absent)",
    )
    out.add_argument(
        "--output-dir",
        metavar="DIR",
        help="directory to save file in (created if missing)",
    )

    net = parser.add_argument_group("network")
    net.add_argument(
        "-x",
        "--proxy",
        metavar="URL",
        help="proxy URL (e.g. socks5h://127.0.0.1:9050, http://host:3128)",
    )
    net.add_argument(
        "-H",
        "--header",
        action="append",
        default=[],
        metavar="HEADER",
        type=parse_header,
        help="add custom header 'Name: Value' (repeatable)",
    )
    net.add_argument(
        "-A",
        "--user-agent",
        metavar="NAME",
        help="User-Agent string",
    )
    net.add_argument(
        "--http-backend",
        choices=("niquests", "requests", "httpx", "urllib3"),
        default="niquests",
        metavar="NAME",
        help="HTTP client library (default: niquests)",
    )
    net.add_argument(
        "-k",
        "--insecure",
        action="store_true",
        help="skip TLS certificate verification",
    )
    net.add_argument(
        "--connect-timeout",
        type=float,
        metavar="SECS",
        help="maximum seconds to wait for connect",
    )
    net.add_argument(
        "--read-timeout",
        type=float,
        metavar="SECS",
        help="maximum seconds between response chunks (default: 4x connect-timeout)",
    )

    log_g = parser.add_argument_group("logging")
    log_g.add_argument(
        "-q",
        "--quiet",
        "-s",
        "--silent",
        action="store_true",
        help="suppress progress output",
    )
    log_g.add_argument(
        "-v",
        "--verbose",
        action="count",
        default=0,
        help="verbose logging (repeat for debug)",
    )

    parser.add_argument(
        "-V",
        "--version",
        action="version",
        version=f"reget {__version__}",
    )
    return parser


# ---------------------------------------------------------------------------
# Session setup
# ---------------------------------------------------------------------------


def resolve_timeout(args: argparse.Namespace) -> float | tuple[float, float] | None:
    ct = args.connect_timeout
    rt = args.read_timeout
    if ct is None and rt is None:
        return None
    if ct is None:
        ct = 30.0
    if rt is None:
        rt = ct * 4
    return (ct, rt)


def resolve_destination(args: argparse.Namespace) -> Path:
    name = args.output or default_output(args.url)
    dest = Path(name)
    if args.output_dir:
        out_dir = Path(args.output_dir).expanduser()
        out_dir.mkdir(parents=True, exist_ok=True)
        dest = out_dir / (dest.name if dest.is_absolute() else dest)
    return dest.expanduser().resolve()


def _build_session(args: argparse.Namespace) -> tuple[TransportSession, object]:
    """Create a TransportSession for the selected backend.

    Returns ``(wrapped_session, native_client)`` so the caller can close
    the native client after use.
    """
    backend = args.http_backend
    builders = {
        "niquests": _build_niquests,
        "requests": _build_requests,
        "httpx": _build_httpx,
        "urllib3": _build_urllib3,
    }
    build = builders.get(backend)
    if build is None:
        msg = f"unknown http backend: {backend!r}"
        raise ValueError(msg)
    return build(args)


def _build_niquests(args: argparse.Namespace) -> tuple[TransportSession, object]:
    import niquests

    from reget.transport.niquests_adapter import niquests_transport

    sess = niquests.Session()
    if args.proxy:
        sess.proxies = {"http": args.proxy, "https": args.proxy}
    if args.insecure:
        sess.verify = False
    return niquests_transport(sess), sess


def _build_requests(args: argparse.Namespace) -> tuple[TransportSession, object]:
    import requests

    from reget.transport.requests_adapter import requests_transport

    sess = requests.Session()
    if args.proxy:
        sess.proxies = {"http": args.proxy, "https": args.proxy}
    if args.insecure:
        sess.verify = False
    return requests_transport(sess), sess


def _build_httpx(args: argparse.Namespace) -> tuple[TransportSession, object]:
    import httpx

    from reget.transport.httpx_adapter import httpx_transport

    kw: dict[str, object] = {}
    if args.proxy:
        kw["proxy"] = args.proxy
    if args.insecure:
        kw["verify"] = False
    timeout = resolve_timeout(args)
    if timeout is not None:
        kw["timeout"] = timeout
    client = httpx.Client(**kw)  # type: ignore[arg-type]
    return httpx_transport(client), client


def _build_urllib3(args: argparse.Namespace) -> tuple[TransportSession, object]:
    import urllib3

    from reget.transport.urllib3_adapter import urllib3_transport

    kw: dict[str, object] = {}
    timeout = resolve_timeout(args)
    if timeout is not None:
        kw["timeout"] = urllib3.Timeout(connect=timeout[0], read=timeout[1]) if isinstance(timeout, tuple) else timeout
    if args.insecure:
        kw["cert_reqs"] = "CERT_NONE"
    pool = urllib3.ProxyManager(args.proxy, **kw) if args.proxy else urllib3.PoolManager(**kw)  # type: ignore[arg-type]
    return urllib3_transport(pool), pool


def _close_native(native: object) -> None:
    from typing import Any

    obj: Any = native
    if hasattr(obj, "close") and callable(obj.close):
        obj.close()
    elif hasattr(obj, "clear") and callable(obj.clear):
        obj.clear()


# ---------------------------------------------------------------------------
# Signal handling
# ---------------------------------------------------------------------------


class _InterruptState:
    """Async-signal-safe interrupt state.

    Signal handlers must only perform async-signal-safe operations.
    We restrict ourselves to: setting a ``threading.Event``, assigning an
    attribute, and calling ``os._exit`` (a raw syscall wrapper that is
    documented as safe from a signal handler). No stdio, no logging, no
    lock acquisition.
    """

    def __init__(self) -> None:
        self.event = threading.Event()
        self.signum: int = 0

    def set(self, signum: int) -> None:
        self.signum = signum
        self.event.set()

    @property
    def is_set(self) -> bool:
        return self.event.is_set()


def _install_signal_handlers(state: _InterruptState) -> None:
    """First signal sets flag; second signal hard-exits."""

    def handle(signum: int, _frame: FrameType | None) -> None:
        if state.is_set:
            os._exit(EXIT_INTERRUPTED)
        state.set(signum)

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            signal.signal(sig, handle)
        except (OSError, ValueError):
            log.debug("could not install handler for %s", sig)


def _configure_logging(verbosity: int) -> None:
    if verbosity >= 2:  # noqa: PLR2004
        level = logging.DEBUG
    elif verbosity == 1:
        level = logging.INFO
    else:
        level = logging.WARNING
    logging.basicConfig(
        level=level,
        format="%(levelname)s %(name)s: %(message)s",
        stream=sys.stderr,
    )


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    if not args.url:
        parser.print_usage(sys.stderr)
        sys.stderr.write("reget: URL is required\n")
        return EXIT_USAGE

    _configure_logging(args.verbose)

    printer = Printer(quiet=args.quiet)

    try:
        url = parse_url(args.url)
    except ValueError as exc:
        printer.err(f"invalid URL: {exc}")
        return EXIT_USAGE

    try:
        dest = resolve_destination(args)
    except OSError as exc:
        printer.err(f"could not prepare output path: {exc}")
        return EXIT_ERROR

    if importlib.util.find_spec(args.http_backend) is None:
        printer.err(
            f"HTTP backend {args.http_backend!r} is not installed. Try: pip install reget[{args.http_backend}]",
        )
        return EXIT_ERROR

    transport, native = _build_session(args)
    try:
        return _download_loop(url, transport, dest, printer)
    finally:
        _close_native(native)


def _download_loop(
    url: Url,
    transport: TransportSession,
    dest: Path,
    printer: Printer,
) -> int:
    interrupt = _InterruptState()
    _install_signal_handlers(interrupt)

    printer.info(f"{url} → {dest}")
    t_start = time.monotonic()

    for attempt in range(_MAX_RETRIES):
        if interrupt.is_set:
            name = signal.Signals(interrupt.signum).name
            printer.info(f"{name} received; resume state saved")
            return EXIT_INTERRUPTED

        try:
            result = _run_fetch(dest, transport, url)
        except KeyboardInterrupt:
            return EXIT_INTERRUPTED

        if isinstance(result, int):
            return result

        match result:
            case DownloadComplete(bytes_written=n, sha256=sha):
                elapsed = time.monotonic() - t_start
                avg = n / elapsed if elapsed > 0 else 0.0
                printer.info(
                    f"done: {format_bytes(n)} in "
                    f"{format_duration(elapsed)} ({format_bytes(int(avg))}/s)  "
                    f"sha256={sha[:16]}…"
                )
                return EXIT_OK
            case DownloadPartial(bytes_written=bw, reason=reason):
                printer.info(f"partial (attempt {attempt + 1}/{_MAX_RETRIES}): {reason}; resuming…")
                if bw == 0:
                    time.sleep(min(2**attempt, 30))
            case _ as unreachable:
                assert_never(unreachable)

    printer.err(f"gave up after {_MAX_RETRIES} resume attempts")
    return EXIT_ERROR


def _run_fetch(
    dest: Path,
    transport: TransportSession,
    url: Url,
) -> DownloadComplete | DownloadPartial | int:
    """Call engine.fetch, mapping exceptions to exit codes."""
    from reget.engine import fetch

    try:
        return fetch(dest, session=transport, url=url)
    except ServerMisconfiguredError as exc:
        _print_err(f"server misconfigured: {exc}")
    except TransportTLSError as exc:
        _print_err(f"TLS error: {exc}")
    except TransportHTTPError as exc:
        _print_err(f"HTTP error: {exc}")
    except TransportError as exc:
        _print_err(f"network error: {exc}")
    except RegetError as exc:
        _print_err(str(exc))
    except OSError as exc:
        _print_err(f"i/o error: {exc}")
    return EXIT_ERROR


def _print_err(msg: str) -> None:
    sys.stderr.write(f"reget: {msg}\n")
    sys.stderr.flush()

"""Minimal curl-compatible CLI for one-off reget downloads.

Minimal CLI for ad-hoc downloads and scripting (including Tor ``.onion`` URLs);
not a full ``curl`` replacement. Uses one optional HTTP stack at runtime
(``--http-backend``: ``niquests``, ``requests``, or ``httpx``; default
``niquests``). Install a matching extra (e.g. ``reget[niquests]``).

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
from pathlib import Path
from types import FrameType
from typing import TYPE_CHECKING
from urllib.parse import urlparse

from reget._types import (
    DownloadComplete,
    DownloadPartial,
    RegetError,
    ServerMisconfiguredError,
    parse_url,
)
from reget._version import __version__
from reget.downloader import PieceDownloader
from reget.headers import DEFAULT_HEADERS
from reget.transport.errors import TransportError, TransportHTTPError, TransportTLSError
from reget.transport.factory import wrap_transport
from reget.transport.session_input import SupportedNativeHttpSession

if TYPE_CHECKING:
    from collections.abc import Sequence

log = logging.getLogger("reget.cli")

EXIT_OK = 0
EXIT_ERROR = 1
EXIT_USAGE = 2
EXIT_INTERRUPTED = 130

_KIB = 1024
_SEC_PER_MIN = 60
_MIN_PER_HOUR = 60
_BAR_TICK_SECS = 0.2
_IDLE_SLEEP_SECS = 0.05

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


def format_bytes(n: float) -> str:
    """Short human-readable byte count (``1.5 MB``)."""
    if n < _KIB:
        return f"{int(n)} B"
    x = float(n)
    for unit in ("KiB", "MiB", "GiB", "TiB"):
        x /= _KIB
        if x < _KIB or unit == "TiB":
            return f"{x:.1f} {unit}"
    return f"{x:.1f} PiB"


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
    """Stderr-only progress / info / error printer.

    curl writes content to stdout and status to stderr; we do the same so
    pipelines like ``reget -o - URL`` stay grep-friendly (note: reget does
    not currently support ``-``; all output goes to a real file).
    """

    def __init__(self, *, quiet: bool) -> None:
        self.quiet = quiet
        self._last_bar_len = 0
        self._tty = sys.stderr.isatty()

    def info(self, msg: str) -> None:
        if self.quiet:
            return
        self._clear_bar()
        sys.stderr.write(msg + "\n")
        sys.stderr.flush()

    def err(self, msg: str) -> None:
        self._clear_bar()
        sys.stderr.write(f"reget: {msg}\n")
        sys.stderr.flush()

    def bar(self, done: int, total: int, speed: float) -> None:
        if self.quiet or not self._tty:
            return
        if total > 0:
            pct = min(100, int(100 * done / total))
            width = 30
            filled = min(width, width * done // total if total else 0)
            bar = "#" * filled + "-" * (width - filled)
            line = f"  [{bar}] {pct:3d}%  {format_bytes(done)}/{format_bytes(total)}  {format_bytes(int(speed))}/s"
        else:
            line = f"  {format_bytes(done)}  {format_bytes(int(speed))}/s"
        pad = " " * max(0, self._last_bar_len - len(line))
        sys.stderr.write(f"\r{line}{pad}")
        sys.stderr.flush()
        self._last_bar_len = len(line)

    def end_bar(self) -> None:
        if self.quiet or not self._tty:
            return
        if self._last_bar_len:
            sys.stderr.write("\n")
            sys.stderr.flush()
            self._last_bar_len = 0

    def _clear_bar(self) -> None:
        if self._last_bar_len:
            pad = " " * self._last_bar_len
            sys.stderr.write(f"\r{pad}\r")
            sys.stderr.flush()
            self._last_bar_len = 0


# ---------------------------------------------------------------------------
# Argparse
# ---------------------------------------------------------------------------


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="reget",
        description=(
            "Resumable, piece-tracked HTTP downloader. "
            "Writes ``<file>.part`` + ``<file>.part.ctrl`` during transfer; "
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
        choices=("niquests", "requests", "httpx"),
        default="niquests",
        metavar="NAME",
        help="HTTP client library (install matching extra: reget[niquests|requests|httpx]; default: niquests)",
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

    piece = parser.add_argument_group("transfer")
    piece.add_argument(
        "--piece-size",
        type=parse_size,
        default=1 << 20,
        metavar="SIZE",
        help="piece size, e.g. 512K, 2M, 1G (default: 1M)",
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


def build_native_http_client(args: argparse.Namespace) -> SupportedNativeHttpSession:
    """Construct a fresh native client for ``args.http_backend`` (dynamic import)."""
    backend = args.http_backend
    if backend == "niquests":
        import niquests

        ni_sess = niquests.Session()
        if args.proxy:
            ni_sess.proxies = {"http": args.proxy, "https": args.proxy}
        if args.insecure:
            ni_sess.verify = False
        return ni_sess
    if backend == "requests":
        import requests

        rq_sess = requests.Session()
        if args.proxy:
            rq_sess.proxies = {"http": args.proxy, "https": args.proxy}
        if args.insecure:
            rq_sess.verify = False
        return rq_sess
    if backend == "httpx":
        import httpx

        proxy = args.proxy
        if proxy is not None and args.insecure:
            return httpx.Client(proxy=proxy, verify=False)
        if proxy is not None:
            return httpx.Client(proxy=proxy)
        if args.insecure:
            return httpx.Client(verify=False)
        return httpx.Client()
    msg = f"unknown http backend: {backend!r}"
    raise ValueError(msg)


def _close_native_http_client(native: SupportedNativeHttpSession) -> None:
    closer = getattr(native, "close", None)
    if callable(closer):
        closer()


def build_headers(args: argparse.Namespace) -> dict[str, str]:
    headers: dict[str, str] = dict(DEFAULT_HEADERS)
    if args.user_agent:
        headers["User-Agent"] = args.user_agent
    headers.update(dict(args.header))
    return headers


def resolve_timeout(
    args: argparse.Namespace,
) -> float | tuple[float, float] | None:
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


# ---------------------------------------------------------------------------
# Main
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
    """First signal → set flag; second signal → ``os._exit`` (safe syscall).

    The main loop is responsible for observing ``state.event`` and emitting
    any user-visible output; signal handlers do not touch stdio.
    """

    def handle(signum: int, _frame: FrameType | None) -> None:
        if state.is_set:
            os._exit(EXIT_INTERRUPTED)
        state.set(signum)

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            signal.signal(sig, handle)
        except (OSError, ValueError):
            # Non-main thread or unsupported signal (e.g. Windows SIGTERM).
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


def main(argv: Sequence[str] | None = None) -> int:  # noqa: PLR0911, PLR0912, PLR0915
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

    native = build_native_http_client(args)
    try:
        transport = wrap_transport(native)
        headers = build_headers(args)
        timeout = resolve_timeout(args)

        interrupt = _InterruptState()
        _install_signal_handlers(interrupt)
        announced_interrupt = False

        t_start = time.monotonic()
        last_tick = 0.0

        try:
            with PieceDownloader(
                url,
                dest,
                piece_size=args.piece_size,
                extra_headers=headers,
                timeout=timeout,
                verify=False if args.insecure else None,
            ) as pd:
                try:
                    pd.prepare(transport)
                except TransportTLSError as exc:
                    printer.err(f"TLS error: {exc}")
                    return EXIT_ERROR
                except TransportHTTPError as exc:
                    printer.err(f"HTTP error: {exc}")
                    return EXIT_ERROR
                except TransportError as exc:
                    printer.err(f"network error: {exc}")
                    return EXIT_ERROR
                except RegetError as exc:
                    printer.err(str(exc))
                    return EXIT_ERROR
                except OSError as exc:
                    printer.err(f"i/o error during preparation: {exc}")
                    return EXIT_ERROR

                tracker = pd.tracker
                if tracker is None:
                    printer.err("preparation failed (no tracker)")
                    return EXIT_ERROR

                done_pieces, total_pieces = tracker.progress()
                alloc_suffix = (
                    f", alloc={tracker.allocation.outcome.value}/{tracker.allocation.mechanism.value}"
                    if tracker.allocation
                    else ""
                )
                printer.info(
                    f"{args.url} → {dest}  "
                    f"({format_bytes(tracker.total_length)}, "
                    f"{total_pieces} x {format_bytes(tracker.piece_size)} pieces{alloc_suffix})"
                )
                if done_pieces > 0:
                    printer.info(f"resuming: {done_pieces}/{total_pieces} pieces already complete")

                while not pd.is_complete():
                    if interrupt.is_set:
                        if not announced_interrupt:
                            name = signal.Signals(interrupt.signum).name
                            printer.info(f"{name} received; finishing current piece and flushing state…")
                            announced_interrupt = True
                        break
                    try:
                        progressed = pd.download_piece(transport)
                    except ServerMisconfiguredError as exc:
                        printer.err(f"server misconfigured: {exc}")
                        return EXIT_ERROR
                    except TransportTLSError as exc:
                        printer.err(f"TLS error: {exc}")
                        return EXIT_ERROR
                    except TransportHTTPError as exc:
                        printer.err(f"HTTP error: {exc}")
                        return EXIT_ERROR
                    except TransportError as exc:
                        printer.err(f"network error: {exc}")
                        return EXIT_ERROR
                    except RegetError as exc:
                        printer.err(str(exc))
                        return EXIT_ERROR
                    except OSError as exc:
                        printer.err(f"i/o error during download: {exc}")
                        return EXIT_ERROR

                    if not progressed:
                        time.sleep(_IDLE_SLEEP_SECS)
                        continue

                    now = time.monotonic()
                    if now - last_tick >= _BAR_TICK_SECS:
                        elapsed = now - t_start
                        d, t = tracker.progress()
                        approx_bytes = tracker.total_length * d // t if t else 0
                        speed = pd.bytes_written / elapsed if elapsed > 0 else 0.0
                        printer.bar(approx_bytes, tracker.total_length, speed)
                        last_tick = now

                printer.end_bar()

                if interrupt.is_set and not pd.is_complete():
                    d, t = tracker.progress()
                    printer.info(f"interrupted at {d}/{t} pieces; resume state saved to {dest.name}.part.ctrl")
                    return EXIT_INTERRUPTED

                try:
                    result = pd.finalize()
                except OSError as exc:
                    printer.err(f"i/o error during finalize: {exc}")
                    return EXIT_ERROR

        except KeyboardInterrupt:
            printer.end_bar()
            return EXIT_INTERRUPTED

        match result:
            case DownloadPartial(reason=reason, pieces_completed=done, pieces_total=total):
                msg = reason or f"only {done}/{total} pieces complete"
                printer.err(f"download incomplete: {msg}")
                return EXIT_ERROR
            case DownloadComplete(bytes_written=n, sha256=sha):
                elapsed = time.monotonic() - t_start
                avg = n / elapsed if elapsed > 0 else 0.0
                printer.info(
                    f"done: {format_bytes(n)} in "
                    f"{format_duration(elapsed)} ({format_bytes(int(avg))}/s)  "
                    f"sha256={sha[:16]}…"
                )
                return EXIT_OK
    finally:
        _close_native_http_client(native)


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())

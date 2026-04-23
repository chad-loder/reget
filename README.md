# reget

[![CI](https://github.com/reget/reget/actions/workflows/ci.yml/badge.svg)](https://github.com/reget/reget/actions/workflows/ci.yml)
[![PyPI](https://img.shields.io/pypi/v/reget.svg)](https://pypi.org/project/reget/)
[![Python](https://img.shields.io/pypi/pyversions/reget.svg)](https://pypi.org/project/reget/)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
[![Conventional Commits](https://img.shields.io/badge/conventional%20commits-1.0.0-yellow.svg)](https://www.conventionalcommits.org/en/v1.0.0/)
[![uv](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/uv/main/assets/badge/v0.json)](https://github.com/astral-sh/uv)

A lightweight Python library for crash-safe, resumeable Python http downloads.

**`reget` takes a web URL, your HTTP client session, and a destination path, and gives you a download that can be cleanly restarted after a
dropped connection, a crash, or a hard container shutdown, without corruption.**

You bring the connection transport; `reget` brings the reliable disk persistence and HTTP byte-ranged resume download logic. If the server does not support byte ranged requests (or if it misbehaves), the worst-case is your file download starts over again at the first byte. Most servers *do* support range requests however, so `reget` really helps when you're downloading a multi-gigabyte file over a spotty connection.

> The name is short for **re-GET** — resumable HTTP GETs that actually
> resume correctly.

## Status

**Early alpha.** Public APIs may still shift before `1.0`. See
[`CHANGELOG.md`](CHANGELOG.md).

## What you bring

- An HTTP client library. Install ``reget[niquests]`` so the default integration
  (library + ``reget`` CLI) is available:
  [`niquests`](https://niquests.readthedocs.io/) — a near-drop-in
  `requests` fork with HTTP/2 and HTTP/3 support. If you must stay on
  classic [`requests`](https://requests.readthedocs.io/), install
  ``reget[requests]`` (and ``reget[niquests]`` if you also use ``fetch()`` /
  ``NiquestsAdapter``) and pass a
  [`RequestsAdapter`](https://github.com/reget/reget/blob/main/src/reget/transport/requests_adapter.py)
  wrapping your `requests.Session` (same `TransportSession` protocol as
  `NiquestsAdapter`; see the adapter module docstring for SOCKS / Tor /
  header-multiplicity caveats vs niquests).
  [`httpx`](https://www.python-httpx.org/) via ``pip install reget[httpx]`` and
  [`HttpxAdapter`](https://github.com/reget/reget/blob/main/src/reget/transport/httpx_adapter.py)
  (sync ``httpx.Client``; see that module for TLS ``verify`` on the client vs
  per-request options, SOCKS, and ``iter_raw`` semantics vs niquests).
  Use ``reget.transport.wrap_transport`` (also ``reget.wrap_transport``) to turn
  a supported native session into a ``TransportSession`` without going through
  ``fetch()``.
- Your session: connections, TLS, DNS, proxies (SOCKS5, HTTP),
  authentication, cookies, and transport-level timeouts are all
  configured on the session you hand to `reget`.
- Your retry loop and backoff strategy — because `reget` guarantees
  resume is cheap (see below), retry is just "call it again until it's done"
- Your progress UI, logging sinks, and cancellation signals.
- This library also allows parallel, multi-circuit downloads of a single
  file (if you know the file's length, you can split up the ranges across
  parallel proxy circuits and download it faster without a WAF blocking you).

## What `reget` brings

- The `.part` + `.part.ctrl` multipart download format, durable across
  connection interruptions, process crashes, kernel panics, and power
  loss: uses a bitmask file parts map similar to what bittorrent and
  `aria2c` employ under the hood.
- Piece-level progress tracking so any download resumes from the last
  known-good byte, never from a `posix_fallocate`-reserved hole.
- Tested handling of the HTTP corner cases where
  `Transfer-Encoding: chunked`, `Content-Encoding` (gzip, brotli,
  identity), and byte-range resume combine into states the
  HTTP spec itself leaves ambiguous (see [Why it exists](#why-it-exists)).
- Thread-safe piece claim / complete primitives, so advanced callers
  can download disjoint parts of the same file in parallel —
  optionally with each piece on a different session (i.e. a different
  VPN or Tor circuit).
- A minimal `curl`-compatible CLI for one-off testing.

## Quick start

Install the default HTTP stack (niquests) alongside the core library::

    pip install reget[niquests]

The same extra enables the ``reget`` command and ``python -m reget``.

```python
from reget import DownloadComplete, DownloadPartial, fetch

match fetch("https://example.com/big.zip", "/tmp/big.zip"):
    case DownloadComplete(sha256=sha, bytes_written=n):
        print(f"ok: {n} bytes, sha256={sha}")
    case DownloadPartial(pieces_completed=done, pieces_total=total):
        print(f"incomplete: {done}/{total} pieces — re-run fetch() to resume")
```

`fetch()` is a single-threaded convenience for trying the library. It
creates a default `niquests.Session`, runs the full download, and
returns a `DownloadResult` — a discriminated union of `DownloadComplete`
(carrying `sha256`, `etag`, `content_type`) and `DownloadPartial`
(carrying `reason` and progress counters). Narrowing via `match` or
`isinstance` prevents accidentally reading success-only fields from a
partial result. For production services, construct your own
`PieceDownloader` (below) so you control the session, the retry loop,
and progress reporting.

## Why it exists

The HTTP specification lets servers combine `Transfer-Encoding: chunked`,
`Content-Encoding` (gzip, brotli, identity), `Content-Range`, and
conditional requests (`If-Range`, `If-Match`) in ways it doesn't fully
disambiguate — and real-world CDNs and origin servers serve those
combinations in the wild. General-purpose HTTP libraries like
`requests`, `httpx`, and `aiohttp` do a thorough job of per-request
integrity — decoding, chunked framing, bounding the body at its
declared `Content-Length` — and then stop, because they're not trying
to be a resume layer. Resume-capable fetch tools like `curl -C -` and
`wget -c` *do* resume, but the naive way: trust the on-disk file
size, send a `Range` request starting at that offset, and hope the
bytes already on disk are actually what the server sent.

That leaves the caller of a 1 GB download that keeps dropping at
900 MB with two unhappy choices: restart from byte 0 every time, or
roll their own resume logic and silently produce a corrupt file the first
time a partial write doesn't survive a crash. `reget` exists to make
the resume option safe.

Most clients treat a resume operation as *"seek to the end of the file
on disk, send a `Range` header, and start appending."* That works
until any of these happen — and in the wild, all of them happen:

- The server returns `200 OK` instead of `206 Partial Content` (ignores
  `Range`), silently overwriting your partial from byte 0.
- The server returns `206 Partial Content` **with** a `Content-Encoding`
  header — so your `Range` boundary now points into the middle of a
  decompression window.
- The server returns `416 Range Not Satisfiable` because the file was
  replaced on the origin.
- The server sends *more* bytes than its `Content-Length` (or 206
  `Content-Range`) declares — a real-world glitch from buggy CGIs,
  misbehaving reverse proxies, and HTTP/1.0 keepalive edge cases —
  and the overflow writes past the requested boundary into bytes the
  next piece is supposed to own.
- A transparent proxy strips `Range` or injects `Content-Encoding`.
- A CDN edge serves a different `ETag` than a sibling edge.
- The file was preallocated with `posix_fallocate` and the download
  was killed mid-stream, leaving a plausibly-sized but zero-padded
  file on disk — which the next naive resume happily skips past,
  leaving a corrupted file with holes in it.
- Power loss after the client closed the file but before the kernel
  flushed the page cache — the on-disk offset lies, and the next
  resume silently writes past a hole of zeros.

`reget` handles each of these explicitly.

## Typical usage in a real service

Assume your service has a `niquests.Session` configured with your
proxy, TLS policy, and timeouts, and a `tenacity`-style retry
decorator you already use for other HTTP calls:

```python
import logging
import niquests
from tenacity import retry, wait_exponential, stop_after_attempt
from reget import DownloadComplete, DownloadPartial, NiquestsAdapter, PieceDownloader, RegetError

log = logging.getLogger(__name__)

@retry(wait=wait_exponential(min=2, max=60), stop=stop_after_attempt(6))
def download_artifact(url: str, dest: str, session: niquests.Session) -> None:
    transport = NiquestsAdapter(session)
    with PieceDownloader(url, dest) as pd:
        pd.prepare(transport)
        while not pd.is_complete():
            pd.download_piece(transport)
        result = pd.finalize()

    match result:
        case DownloadPartial(pieces_completed=done, pieces_total=total):
            raise RegetError(f"incomplete: {done}/{total}")
        case DownloadComplete(bytes_written=n, sha256=sha):
            log.info("%s: %d bytes, sha256=%s", dest, n, sha)
```

Every `raise` re-enters the `@retry` loop. Because `.part.ctrl` is
durable, the next attempt resumes from the last completed piece with
zero re-downloaded bytes and zero coordination on your side. This is
the core value proposition: **retry is just calling us again.**

## Parallel downloads

For large files on per-client rate-limited servers, drive a single
`PieceDownloader` from N worker threads. Pieces are claimed atomically,
writes are disjoint, and the shared `.part.ctrl` stays consistent:

```python
import threading
import niquests
from reget import NiquestsAdapter, PieceDownloader

session = niquests.Session()  # configure proxies, TLS, pool, … as needed

def worker(pd: PieceDownloader, transport: NiquestsAdapter) -> None:
    while not pd.is_complete():
        pd.download_piece(transport)

transport = NiquestsAdapter(session)
with PieceDownloader(url, dest) as pd:
    pd.prepare(transport)
    threads = [threading.Thread(target=worker, args=(pd, transport)) for _ in range(4)]
    for t in threads: t.start()
    for t in threads: t.join()
    result = pd.finalize()
```

This example shares one session (and thus one transport) across all
workers — appropriate for most rate-limiting scenarios. For the
advanced case where each piece should ride a separate session (e.g. a
different VPN or Tor circuit per thread), the architecture supports
it but a small coordination helper is still on the roadmap. Open an
issue if you need this today.

## Progress reporting

`PieceDownloader` exposes enough state to drive any progress UI. Poll
after each piece fetch, or from a monitor thread on a timer:

```python
transport = NiquestsAdapter(session)
pd.prepare(transport)
while not pd.is_complete():
    pd.download_piece(transport)
    done, total = pd.tracker.progress()  # pieces completed / total
    bytes_done = pd.bytes_written        # bytes transferred this session
    update_ui(done, total, bytes_done)
```

You can also pass ``on_progress=...`` to ``PieceDownloader`` for
per-piece callbacks; polling remains useful for coarse UI updates.

## Lifecycle and cleanup

- **In-flight.** Two files live next to `dest`: `<dest>.part` (the
  data, pre-allocated to the final size) and `<dest>.part.ctrl` (the
  durable piece bitmap).
- **Interrupted.** Both sidecar files remain on disk. Re-running the
  same `fetch()` or constructing a new `PieceDownloader(url, dest)`
  resumes automatically from the last flushed piece.
- **Complete.** `.part` is atomically renamed to `dest`; `.part.ctrl`
  is unlinked.
- **To discard a partial download** and restart from scratch: delete
  both `<dest>.part` and `<dest>.part.ctrl` before starting. Deleting
  only one trips the "stale sidecar" path and restarts the download
  anyway.
- **To cancel mid-download:** break out of your `download_piece()`
  loop and let the `with` block exit. The context manager flushes
  state and closes the session; the sidecar files remain, durable,
  ready for the next attempt.

The durability guarantee is enforced by pairing `fdatasync` on the
`.part` file with the atomic *tmp + fsync + rename* of the control
file, under a single writer lock. `reget` never sets a piece-done bit
on disk until the corresponding data bytes are on disk.

## Transport response headers

Adapters expose response metadata as `reget.transport.TransportHeaders`.
`get(name)` returns the first value for a field (case-insensitive); most
engine logic (`ETag`, `Content-Length`, `Content-Range`, …) uses that. Use
`get_all(name)` when you need every value for a repeated header (e.g. multiple
`Set-Cookie` lines); **httpx** preserves duplicates via `Headers.multi_items`,
while **niquests** / **requests** may collapse duplicates when building the
adapter view.

## Exceptions

Most library-defined failures use `RegetError` (or a subclass). Transport
adapters map niquests / requests / httpx failures into
`reget.transport.TransportError` subclasses, with the native exception chained
as `__cause__` for debugging.

| Exception | When | Retryable? |
|---|---|---|
| `RegetError` | Base class; protocol anomalies the engine cannot fix alone (missing `Content-Length`, unexpected HTTP status, …). | Case-by-case. |
| `ServerMisconfiguredError` | The server violated HTTP in a way that prevents safe resume: `206` + `Content-Encoding`, etc. | No — the server is broken; retrying does not help. |
| `ContentRangeError` | A `206` response's `Content-Range` does not match the requested piece boundary. | Often yes — the offending piece is released before the exception propagates. |
| `ControlFileError` | A `.part.ctrl` file on disk is truncated, corrupt, or version-mismatched. | No — `reget` discards the stale sidecar and restarts on the next attempt. |
| `TransportConnectionError` | Network / timeout / truncated-body / proxy failures at the transport boundary (also subclasses builtin `ConnectionError`). | Often yes — treat like connection drops. |
| `TransportHTTPError` | HTTP error status from the server (e.g. failed `raise_for_status` / HEAD). | Depends on status. |
| `TransportTLSError` | TLS / certificate verification failure. | Usually no until TLS is fixed. |
| `TransportUnsupportedError` | Unsupported protocol or capability at the transport layer. | No. |
| `TransportError` | Other mapped transport failures not covered above. | Case-by-case. |

## CLI

`reget` ships a minimal, `curl`-compatible CLI for scripts and one-off
testing against arbitrary servers. Runtime deps are stdlib-only
(`argparse`, `signal`, `logging`) plus the HTTP client (`niquests`).

```bash
reget -o file.iso https://example.com/file.iso
reget -x socks5h://127.0.0.1:9050 http://abc.onion/blob.bin
reget -H 'Cookie: x=1' -A 'my-bot/1.0' --piece-size 2M https://host/f.zip
```

Key flags mirror `curl` where practical (`-o`, `-O`, `-x`, `-H`, `-A`,
`-k`, `-s`/`-q`, `-v`, `--connect-timeout`). Re-running the same
command resumes from the sidecar; `SIGINT` / `SIGTERM` flush state and
exit 130 (a second signal forces hard exit). Run `reget --help` for
the full option list.

## Logging

`reget` logs to `logging.getLogger("reget")` (the CLI uses
`logging.getLogger("reget.cli")`). No root-logger configuration, no
side effects on import — attach a handler, route to JSON, or raise the
level to silence.

## Install

```bash
uv add reget
# or
pip install reget
```

Nightly dev builds on TestPyPI:

```bash
uv pip install --index-url https://test.pypi.org/simple/ \
               --extra-index-url https://pypi.org/simple/ \
               reget
```

## Development

```bash
git clone https://github.com/reget/reget.git
cd reget
uv sync --all-groups
uv run pytest
uv run ruff check
uv run mypy src tests
uv build
```

See [`CONTRIBUTING.md`](CONTRIBUTING.md) for the full contributor
guide, including our commit-message convention and release workflow.

## Release model

- **Commit style:** [Conventional Commits](https://www.conventionalcommits.org/).
  PR titles are validated on every pull request.
- **Versioning:** Semantic Versioning. Bumps are computed automatically
  by [release-please](https://github.com/googleapis/release-please)
  from the commit history.
- **Publishing:** PyPI via **Trusted Publishing** (OIDC) — no API
  tokens stored in the repository.
- **Nightlies:** `YYYYMMDD`-stamped `.devN` wheels published to
  TestPyPI from a scheduled workflow.

## License

MIT. See [`LICENSE`](LICENSE).

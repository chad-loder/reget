# reget

[![CI](https://github.com/chad-loder/reget/actions/workflows/ci.yml/badge.svg)](https://github.com/chad-loder/reget/actions/workflows/ci.yml)
[![PyPI](https://img.shields.io/pypi/v/reget.svg)](https://pypi.org/project/reget/)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

Resumable HTTP downloads for Python. Crash-safe, transport-agnostic,
cursor-based.

`reget` takes a URL, an HTTP session you already own, and a destination path.
It gives you a download that survives dropped connections, process crashes,
and hard shutdowns without corruption. Call `fetch()` again with the same
destination and it picks up where it left off.

> **re-GET** — resumable HTTP GETs that actually resume correctly.

## Status

Early alpha (`0.x`). APIs may shift before `1.0`.

## Install

```bash
pip install reget[niquests]   # recommended: HTTP/2+3, async
pip install reget[httpx]      # or httpx
pip install reget[requests]   # or classic requests
pip install reget[urllib3]    # or raw urllib3
```

No hard dependency on any HTTP library. Pick one (or several) as extras.

## Quick start

```python
from reget.engine import fetch
from reget.transport.niquests_adapter import NiquestsAdapter
import niquests

session = niquests.Session()
adapter = NiquestsAdapter(session)

result = fetch("/tmp/big.zip", session=adapter, url="https://example.com/big.zip")

match result:
    case DownloadComplete(sha256=sha, bytes_written=n):
        print(f"done: {n} bytes, sha256={sha}")
    case DownloadPartial(reason=reason):
        print(f"incomplete: {reason} — call fetch() again to resume")
```

`fetch()` returns a discriminated union: `DownloadComplete` (carrying
`sha256`, `etag`, `content_type`) or `DownloadPartial` (carrying `reason`
and progress counters). Narrow with `match` or `isinstance`.

### Async

```python
from reget.async_engine import async_fetch
from reget.transport.httpx_adapter import AsyncHttpxAdapter
import httpx

async with httpx.AsyncClient() as client:
    adapter = AsyncHttpxAdapter(client)
    result = await async_fetch("/tmp/big.zip", session=adapter, url="https://example.com/big.zip")
```

Same return type, same resume semantics. The async engine shares all
non-I/O logic with the sync engine; only the stream iteration differs.

## The guarantee

Every download attempt is safe to abandon and resume. State lives on disk
(`.part` data file + `.part.ctrl` JSON checkpoint), not in process memory.
Kill the process, lose the network, get a 503 — the next `fetch()` call
for the same destination resumes from the last flushed byte offset. If the
server supports standard HTTP range requests and the resource hasn't changed,
zero bytes are re-downloaded.

Durability is enforced by pairing `fdatasync` on the `.part` file with
atomic tmp-fsync-rename of the `.part.ctrl` checkpoint. Data bytes are
never marked committed until they are on disk.

## What reget is not

**Not a retry policy.** `reget` makes individual attempts safe. You decide
how many, on what schedule, and when to give up. Use `tenacity`, a for-loop,
or nothing. What `reget` guarantees is that any strategy works on top of it,
because the disk state is the contract.

**Not a session manager.** `reget` borrows your session, sets per-request
headers (`Range`, `If-Range`), and returns it in the state it received it.
It never creates, configures, or closes sessions.

**Not a content verifier.** `reget` verifies HTTP framing (Content-Range
boundaries, ETag continuity, stream completeness). It does not verify
content correctness — checksums and signatures are the caller's job after
`DownloadComplete`.

## Transport adapters

reget talks to HTTP libraries through a two-method `TransportSession`
protocol. Four adapters ship:

| Extra | Adapter | Async | Notes |
| --- | --- | --- | --- |
| `niquests` | `NiquestsAdapter` / `AsyncNiquestsAdapter` | Yes | HTTP/2+3, recommended |
| `httpx` | `HttpxAdapter` / `AsyncHttpxAdapter` | Yes | Full async support |
| `requests` | `RequestsAdapter` | No | Sync only; no async and never will |
| `urllib3` | `Urllib3Adapter` | No | Minimal dep; sync only |

Wrap any supported client:

```python
from reget.transport.httpx_adapter import HttpxAdapter
adapter = HttpxAdapter(httpx.Client())

from reget.transport.urllib3_adapter import Urllib3Adapter
adapter = Urllib3Adapter(urllib3.PoolManager())
```

Writing a custom adapter means implementing `stream_get()` — a context
manager that yields a response with `.status_code`, `.headers`, and
`.iter_raw_bytes()`. See `protocols.py`.

## Retry pattern

```python
from tenacity import retry, wait_exponential, stop_after_attempt
from reget.engine import fetch
from reget._types import DownloadComplete, DownloadPartial

@retry(wait=wait_exponential(min=2, max=60), stop=stop_after_attempt(6))
def download(url: str, dest: str, session) -> None:
    result = fetch(dest, session=session, url=url)
    if isinstance(result, DownloadPartial):
        raise RuntimeError(f"incomplete: {result.reason}")
```

Every raise re-enters the retry loop. Because `.part.ctrl` is durable,
the next attempt resumes with zero re-downloaded bytes and zero
coordination on your side.

## Lifecycle

- **In-flight.** Two sidecar files: `<dest>.part` (data) and
  `<dest>.part.ctrl` (JSON checkpoint with cursor position, ETag,
  resource length).
- **Interrupted.** Both files remain. Next `fetch()` resumes
  automatically.
- **Complete.** `.part` is atomically renamed to `dest`; `.ctrl` is
  deleted. SHA-256 is computed and returned.
- **Discard.** Delete both `.part` and `.part.ctrl` to force a
  restart.

## How resume works

1. `fetch()` reads `.part.ctrl` (if it exists) to recover the cursor
   position and stored ETag.
2. Sends `Range: bytes=<cursor>-` with `If-Range: <etag>` (omitted
   when no ETag is stored).
3. **206 Partial Content** — server honours the range. Stream appends
   from the cursor.
4. **200 OK** — server ignores the range (resource changed, or server
   doesn't support ranges). Cursor resets to 0; stream overwrites from
   the beginning.
5. **416 Range Not Satisfiable** — cursor equals resource length
   (already complete) or resource shrank (checkpoint reset, next call
   restarts).

The engine handles each case without caller intervention.

## Exceptions

| Exception | When | Retryable? |
| --- | --- | --- |
| `RegetError` | Base; protocol anomalies the engine can't fix alone. | Case-by-case |
| `ServerMisconfiguredError` | Server violated HTTP in a way that prevents safe resume. | No |
| `ContentRangeError` | 206 Content-Range doesn't match the requested range. | Often yes |
| `ControlFileError` | `.part.ctrl` is corrupt or version-mismatched. Discarded on next attempt. | Auto-recovers |
| `TransportConnectionError` | Network/timeout/truncated-body failures. Also subclasses builtin `ConnectionError`. | Usually yes |
| `TransportHTTPError` | HTTP error status from the server. | Depends on status |
| `TransportTLSError` | TLS / certificate verification failure. | No, until TLS is fixed |

## Development

```bash
git clone https://github.com/reget/reget.git && cd reget
uv sync --all-groups
uv run pytest              # 210 tests, including live HTTP integration
uv run ruff check          # lint
uv run mypy src tests      # type check
uv run pyright src tests   # strict type check
```

## License

MIT. See [`LICENSE`](LICENSE).

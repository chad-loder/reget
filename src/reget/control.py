"""Control file (.part.ctrl) serialization for crash-safe resume.

Binary format::

    ┌─────────────────┬──────────────────────────┬────────────────────┐
    │ 4 bytes (big-E) │ JSON metadata (UTF-8)    │ raw bitfield bytes │
    │ = JSON length N │ N bytes                  │ ceil(pieces/8) B   │
    └─────────────────┴──────────────────────────┴────────────────────┘

The ``inflight`` bitfield is never persisted — on restart nothing is
in-flight.  Write is crash-safe via tmp + fsync + ``os.replace``.
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import cast

from bitarray import bitarray

from reget._types import ETag, Url, parse_etag, parse_url
from reget.alloc import AllocationOutcome, Mechanism

CTRL_VERSION = 1
_HEADER_LEN_BYTES = 4
_JSON_ENCODING = "utf-8"  # explicit JSON text encoding for the length-prefixed metadata blob

_KNOWN_ALLOC_OUTCOMES: frozenset[str] = frozenset(m.value for m in AllocationOutcome)
_KNOWN_ALLOC_MECHANISMS: frozenset[str] = frozenset(m.value for m in Mechanism)


@dataclass(frozen=True, slots=True)
class ControlMeta:
    """Metadata stored in the JSON header of a ``.part.ctrl`` file."""

    version: int
    url: Url
    piece_size: int
    total_length: int
    etag: ETag
    alloc_outcome: str
    alloc_mechanism: str
    alloc_previous_size: int

    def to_dict(self) -> dict[str, object]:
        return {
            "version": self.version,
            "url": self.url,
            "piece_size": self.piece_size,
            "total_length": self.total_length,
            "etag": self.etag,
            "alloc_outcome": self.alloc_outcome,
            "alloc_mechanism": self.alloc_mechanism,
            "alloc_previous_size": self.alloc_previous_size,
        }


@dataclass(frozen=True, slots=True)
class ControlFile:
    """Parsed contents of a ``.part.ctrl`` file."""

    meta: ControlMeta
    done: bitarray


class ControlFileError(Exception):
    """Raised when a control file is corrupt or incompatible."""


def ctrl_path_for(part_path: Path) -> Path:
    """Return the conventional ``.part.ctrl`` path for a ``.part`` file."""
    return part_path.with_suffix(part_path.suffix + ".ctrl")


def serialize(meta: ControlMeta, done: bitarray) -> bytes:
    """Encode a control file as raw bytes (header-length + JSON + bitfield).

    JSON metadata is :func:`json.dumps` then :meth:`str.encode` with
    :data:`_JSON_ENCODING`; :func:`deserialize` decodes with the same encoding
    before :func:`json.loads`.
    """
    meta_text = json.dumps(meta.to_dict(), separators=(",", ":"), ensure_ascii=False)
    meta_bytes = meta_text.encode(_JSON_ENCODING)
    bf_bytes = done.tobytes()
    return len(meta_bytes).to_bytes(_HEADER_LEN_BYTES, "big") + meta_bytes + bf_bytes


def deserialize(data: bytes) -> ControlFile:  # noqa: PLR0912, PLR0915 - structural validator
    """Decode raw bytes into a ``ControlFile``.

    Raises ``ControlFileError`` on truncated, corrupt, or version-incompatible
    input.
    """
    if len(data) < _HEADER_LEN_BYTES:
        raise ControlFileError("control file too short for header length prefix")

    json_len = int.from_bytes(data[:_HEADER_LEN_BYTES], "big")
    if json_len <= 0 or _HEADER_LEN_BYTES + json_len > len(data):
        raise ControlFileError(f"invalid JSON length prefix: {json_len}")

    json_bytes = data[_HEADER_LEN_BYTES : _HEADER_LEN_BYTES + json_len]
    try:
        meta_text = json_bytes.decode(_JSON_ENCODING)
        loaded = json.loads(meta_text)
    except UnicodeDecodeError as exc:
        raise ControlFileError(f"corrupt JSON header (not {_JSON_ENCODING}): {exc}") from exc
    except json.JSONDecodeError as exc:
        raise ControlFileError(f"corrupt JSON header: {exc}") from exc

    if not isinstance(loaded, dict):
        raise ControlFileError("JSON header is not an object")

    header = cast(dict[str, object], loaded)

    version_obj = header.get("version")
    if not isinstance(version_obj, int) or isinstance(version_obj, bool):
        raise ControlFileError(f"unsupported control file version: {version_obj!r}")
    if version_obj != CTRL_VERSION:
        raise ControlFileError(f"unsupported control file version: {version_obj}")

    try:
        raw_url = header["url"]
        if not isinstance(raw_url, str):
            raise TypeError(f"url must be str, got {type(raw_url).__name__}")
        url = parse_url(raw_url)

        etag_raw = header.get("etag", "")
        if etag_raw is None:
            etag_raw = ""
        if not isinstance(etag_raw, str):
            raise TypeError(f"etag must be str, got {type(etag_raw).__name__}")
        etag = parse_etag(etag_raw)

        raw_outcome = header["alloc_outcome"]
        raw_mechanism = header["alloc_mechanism"]
        if not isinstance(raw_outcome, str) or raw_outcome not in _KNOWN_ALLOC_OUTCOMES:
            raise TypeError(f"invalid alloc_outcome: {raw_outcome!r}")
        if not isinstance(raw_mechanism, str) or raw_mechanism not in _KNOWN_ALLOC_MECHANISMS:
            raise TypeError(f"invalid alloc_mechanism: {raw_mechanism!r}")

        raw_prev = header["alloc_previous_size"]
        if not isinstance(raw_prev, int) or isinstance(raw_prev, bool):
            raise TypeError("alloc_previous_size must be int")

        piece_size = header["piece_size"]
        if not isinstance(piece_size, int) or isinstance(piece_size, bool):
            raise TypeError("piece_size must be int")

        total_length = header["total_length"]
        if not isinstance(total_length, int) or isinstance(total_length, bool):
            raise TypeError("total_length must be int")

        meta = ControlMeta(
            version=version_obj,
            url=url,
            piece_size=piece_size,
            total_length=total_length,
            etag=etag,
            alloc_outcome=raw_outcome,
            alloc_mechanism=raw_mechanism,
            alloc_previous_size=raw_prev,
        )
    except (KeyError, TypeError, ValueError) as exc:
        raise ControlFileError(f"missing or invalid metadata field: {exc}") from exc

    bf_bytes = data[_HEADER_LEN_BYTES + json_len :]
    num_pieces = -(-meta.total_length // meta.piece_size) if meta.total_length else 0
    expected_bf_len = -(-num_pieces // 8) if num_pieces else 0

    if len(bf_bytes) != expected_bf_len:
        raise ControlFileError(
            f"bitfield length mismatch: got {len(bf_bytes)} bytes, expected {expected_bf_len} for {num_pieces} pieces"
        )

    done = bitarray(endian="big")
    done.frombytes(bf_bytes)
    if len(done) > num_pieces:
        del done[num_pieces:]

    return ControlFile(meta=meta, done=done)


def write_atomic_raw(path: Path, raw: bytes) -> None:
    """Write pre-serialized control bytes atomically (tmp + fsync + rename).

    The destination ``path`` is never left in a partial state, and the
    bytes are durable on disk before the rename returns.

    **Durability invariant.**  Callers that *also* need the associated
    ``.part`` data bytes to be durable (because the bitfield being written
    here will claim them as done) MUST ``fdatasync`` the ``.part`` file
    *before* calling this function.  Otherwise a crash between the two
    writes could leave the control file claiming pieces are done while the
    ``.part`` bytes still live in page cache — silent corruption on
    recovery.  See :meth:`reget.tracker.PieceTracker.flush_state`, which
    upholds this invariant for the canonical call site.
    """
    tmp = path.with_suffix(path.suffix + ".tmp")
    with tmp.open("wb") as f:
        f.write(raw)
        f.flush()
        os.fsync(f.fileno())
    tmp.replace(path)


def write_atomic(path: Path, meta: ControlMeta, done: bitarray) -> None:
    """Serialize ``meta`` + ``done`` and write atomically.

    Thin wrapper around :func:`write_atomic_raw`; see its docstring for the
    durability invariant this call site participates in.
    """
    write_atomic_raw(path, serialize(meta, done))


def read_control(path: Path) -> ControlFile:
    """Read and parse a control file from disk.

    Raises ``ControlFileError`` for any structural issue and
    ``FileNotFoundError`` if the path doesn't exist.
    """
    data = path.read_bytes()
    return deserialize(data)

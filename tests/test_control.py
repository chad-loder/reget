"""Tests for the .part.ctrl control file format.

Covers serialization / deserialization round-trips, atomic writes,
corruption handling, and ETag mismatch on resume.
"""

from __future__ import annotations

from pathlib import Path

import pytest
from bitarray import bitarray

from reget.control import (
    CTRL_VERSION,
    ControlFileError,
    ControlMeta,
    ctrl_path_for,
    deserialize,
    read_control,
    serialize,
    write_atomic,
)


def _make_meta(**overrides: object) -> ControlMeta:
    defaults: dict[str, object] = {
        "version": CTRL_VERSION,
        "url": "https://example.onion/file.zip",
        "piece_size": 1024,
        "total_length": 10 * 1024,
        "etag": '"abc123"',
        "alloc_outcome": "reserved",
        "alloc_mechanism": "posix_fallocate",
        "alloc_previous_size": 0,
    }
    defaults.update(overrides)
    return ControlMeta(**defaults)  # type: ignore[arg-type]


def _make_done(pattern: str) -> bitarray:
    """Create a bitarray from a '0'/'1' string like '1100110000'."""
    ba = bitarray(pattern)
    return ba


# -----------------------------------------------------------------------
# ctrl_path_for
# -----------------------------------------------------------------------


def test_ctrl_path_for() -> None:
    assert ctrl_path_for(Path("/a/b/file.zip.part")) == Path("/a/b/file.zip.part.ctrl")


# -----------------------------------------------------------------------
# Serialization round-trips
# -----------------------------------------------------------------------


def test_roundtrip_all_zeros() -> None:
    meta = _make_meta()
    done = bitarray(10)
    done.setall(False)
    cf = deserialize(serialize(meta, done))
    assert cf.meta == meta
    assert cf.done == done
    assert len(cf.done) == 10


def test_roundtrip_all_ones() -> None:
    meta = _make_meta()
    done = bitarray(10)
    done.setall(True)
    cf = deserialize(serialize(meta, done))
    assert cf.done.all()
    assert len(cf.done) == 10


def test_roundtrip_mixed_pattern() -> None:
    meta = _make_meta()
    done = _make_done("1010101010")
    cf = deserialize(serialize(meta, done))
    assert cf.done == done


def test_roundtrip_single_piece() -> None:
    meta = _make_meta(total_length=512, piece_size=1024)
    done = _make_done("1")
    cf = deserialize(serialize(meta, done))
    assert cf.done == done
    assert cf.meta.total_length == 512


def test_roundtrip_large_bitfield() -> None:
    n_pieces = 5000
    meta = _make_meta(total_length=n_pieces * 1024, piece_size=1024)
    done = bitarray(n_pieces)
    done.setall(False)
    for i in range(0, n_pieces, 3):
        done[i] = True
    cf = deserialize(serialize(meta, done))
    assert cf.done == done
    assert cf.done.count() == len(range(0, n_pieces, 3))


def test_roundtrip_preserves_empty_etag() -> None:
    meta = _make_meta(etag="")
    done = bitarray(10)
    done.setall(False)
    cf = deserialize(serialize(meta, done))
    assert cf.meta.etag == ""


def test_roundtrip_json_metadata_fields() -> None:
    meta = _make_meta(
        url="https://example.com/very/long/path/to/file.zip?token=abc",
        alloc_outcome="sparse",
        alloc_mechanism="ftruncate",
        etag='"W/xyz"',
    )
    done = bitarray(10)
    done.setall(False)
    cf = deserialize(serialize(meta, done))
    assert cf.meta.url == meta.url
    assert cf.meta.alloc_outcome == "sparse"
    assert cf.meta.alloc_mechanism == "ftruncate"
    assert cf.meta.etag == '"W/xyz"'


# -----------------------------------------------------------------------
# Atomic write + read from disk
# -----------------------------------------------------------------------


def test_write_and_read_atomic(tmp_path: Path) -> None:
    ctrl = tmp_path / "file.zip.part.ctrl"
    meta = _make_meta()
    done = _make_done("1100110000")
    write_atomic(ctrl, meta, done)
    assert ctrl.exists()
    cf = read_control(ctrl)
    assert cf.meta == meta
    assert cf.done == done


def test_write_atomic_no_tmp_residue(tmp_path: Path) -> None:
    ctrl = tmp_path / "file.zip.part.ctrl"
    meta = _make_meta()
    done = bitarray(10)
    done.setall(False)
    write_atomic(ctrl, meta, done)
    tmp = ctrl.with_suffix(".ctrl.tmp")
    assert not tmp.exists()


def test_write_atomic_overwrites(tmp_path: Path) -> None:
    ctrl = tmp_path / "file.zip.part.ctrl"
    meta = _make_meta()
    done1 = _make_done("0000000000")
    done2 = _make_done("1111111111")
    write_atomic(ctrl, meta, done1)
    write_atomic(ctrl, meta, done2)
    cf = read_control(ctrl)
    assert cf.done == done2


def test_read_nonexistent_raises(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError):
        read_control(tmp_path / "nope.ctrl")


# -----------------------------------------------------------------------
# Corruption / invalid data
# -----------------------------------------------------------------------


def test_deserialize_too_short() -> None:
    with pytest.raises(ControlFileError, match="too short"):
        deserialize(b"\x00\x00")


def test_deserialize_zero_json_length() -> None:
    with pytest.raises(ControlFileError, match="invalid JSON length"):
        deserialize(b"\x00\x00\x00\x00")


def test_deserialize_json_length_exceeds_data() -> None:
    with pytest.raises(ControlFileError, match="invalid JSON length"):
        deserialize(b"\x00\x00\x00\xff" + b"x" * 10)


def test_deserialize_invalid_json() -> None:
    bad_json = b"not json at all"
    header = len(bad_json).to_bytes(4, "big") + bad_json
    with pytest.raises(ControlFileError, match="corrupt JSON"):
        deserialize(header)


def test_deserialize_json_array_not_object() -> None:
    arr = b"[1,2,3]"
    header = len(arr).to_bytes(4, "big") + arr
    with pytest.raises(ControlFileError, match="not an object"):
        deserialize(header)


def test_deserialize_wrong_version() -> None:
    import json as _json

    obj = {"version": 99, "url": "x", "piece_size": 1, "total_length": 0}
    raw = _json.dumps(obj).encode()
    data = len(raw).to_bytes(4, "big") + raw
    with pytest.raises(ControlFileError, match=r"unsupported.*version"):
        deserialize(data)


def test_deserialize_missing_required_field() -> None:
    import json as _json

    obj = {"version": 1, "url": "x"}
    raw = _json.dumps(obj).encode()
    data = len(raw).to_bytes(4, "big") + raw
    with pytest.raises(ControlFileError, match="missing or invalid"):
        deserialize(data)


def test_deserialize_bitfield_length_mismatch() -> None:
    meta = _make_meta()
    done = bitarray(10)
    done.setall(False)
    raw = serialize(meta, done)
    truncated = raw[:-1]
    with pytest.raises(ControlFileError, match="bitfield length mismatch"):
        deserialize(truncated)


def test_deserialize_bitfield_too_long() -> None:
    meta = _make_meta()
    done = bitarray(10)
    done.setall(False)
    raw = serialize(meta, done)
    with pytest.raises(ControlFileError, match="bitfield length mismatch"):
        deserialize(raw + b"\xff")


# -----------------------------------------------------------------------
# Edge cases
# -----------------------------------------------------------------------


def test_zero_length_file() -> None:
    meta = _make_meta(total_length=0, piece_size=1024)
    done = bitarray(0)
    cf = deserialize(serialize(meta, done))
    assert len(cf.done) == 0
    assert cf.meta.total_length == 0


def test_non_aligned_tail_piece() -> None:
    meta = _make_meta(total_length=1024 + 500, piece_size=1024)
    done = _make_done("10")
    cf = deserialize(serialize(meta, done))
    assert len(cf.done) == 2
    assert cf.done[0]
    assert not cf.done[1]

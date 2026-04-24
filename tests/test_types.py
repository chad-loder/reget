"""Tests for parse_url / parse_etag branding factories and the
DownloadComplete / DownloadPartial discriminated union."""

from __future__ import annotations

import pytest

from reget import (
    DownloadComplete,
    DownloadPartial,
    DownloadStatus,
    ETag,
    Url,
    parse_etag,
    parse_url,
)
from reget._types import EMPTY_ETAG, EMPTY_URL


@pytest.mark.parametrize(
    "raw",
    [
        "http://example.com/",
        "https://example.com/path/to/file.zip?token=abc",
        "http://user:pass@host:8080/",
        "https://[::1]:8443/ipv6",
    ],
)
def test_parse_url_accepts_valid_http_urls(raw: str) -> None:
    parsed = parse_url(raw)
    assert parsed == raw
    assert isinstance(parsed, str)


@pytest.mark.parametrize("raw", ["", "   ", "\t\n"])
def test_parse_url_empty_returns_sentinel(raw: str) -> None:
    assert parse_url(raw) == EMPTY_URL
    assert parse_url(raw) == ""


@pytest.mark.parametrize(
    "raw",
    [
        "ftp://example.com/",
        "file:///etc/passwd",
        "javascript:alert(1)",
        "gopher://example.com",
    ],
)
def test_parse_url_rejects_unsupported_scheme(raw: str) -> None:
    with pytest.raises(ValueError, match="scheme"):
        parse_url(raw)


@pytest.mark.parametrize("raw", ["http://", "https:///path", "http:/missing-host"])
def test_parse_url_rejects_missing_host(raw: str) -> None:
    with pytest.raises(ValueError, match=r"host|scheme"):
        parse_url(raw)


def test_parse_etag_strong_and_weak_are_preserved_verbatim() -> None:
    assert parse_etag('"abc123"') == '"abc123"'
    assert parse_etag('W/"weak-hash"') == 'W/"weak-hash"'


def test_parse_etag_strips_whitespace() -> None:
    assert parse_etag('  "abc"  ') == '"abc"'


@pytest.mark.parametrize("raw", ["", "   ", "\t"])
def test_parse_etag_empty_returns_empty_sentinel(raw: str) -> None:
    result = parse_etag(raw)
    assert result == EMPTY_ETAG
    assert result == ""


@pytest.mark.parametrize(
    "raw",
    [
        "not-quoted",
        '"unterminated',
        'unopened"',
        '"',
        "W/unquoted",
    ],
)
def test_parse_etag_rejects_malformed_as_empty(raw: str) -> None:
    """Malformed ETags become EMPTY_ETAG rather than raise — we refuse to
    echo garbage back in If-Range, but don't want to blow up the download."""
    assert parse_etag(raw) == EMPTY_ETAG


def test_parse_etag_type_error_on_non_string() -> None:
    with pytest.raises(TypeError):
        parse_etag(None)
    with pytest.raises(TypeError):
        parse_etag(b'"bytes"')


def test_newtype_identity_at_runtime() -> None:
    """NewType is a no-op at runtime — the brand only lives in mypy's view."""
    url = parse_url("http://example.com/")
    etag = parse_etag('"abc"')
    assert type(url) is str  # not a subclass
    assert type(etag) is str
    assert Url("x") == "x"
    assert ETag("y") == "y"


def test_download_complete_carries_success_only_fields() -> None:
    result = DownloadComplete(
        bytes_written=100,
        elapsed=1.0,
        sha256="abc",
        etag=ETag('"v1"'),
        content_type="application/octet-stream",
    )
    assert result.status is DownloadStatus.COMPLETE
    assert result.sha256 == "abc"
    assert result.etag == '"v1"'


def test_download_partial_has_no_success_only_fields() -> None:
    """``sha256`` / ``etag`` / ``content_type`` must NOT be reachable on
    a Partial — that's the whole point of the discriminated union."""
    result = DownloadPartial(
        bytes_written=50,
        valid_length=50,
        elapsed=0.5,
        reason="network error",
    )
    assert result.status is DownloadStatus.PARTIAL
    assert result.reason == "network error"
    assert result.valid_length == 50
    for missing in ("sha256", "etag", "content_type"):
        assert not hasattr(result, missing), (
            f"DownloadPartial should not expose {missing}; that defeats the discriminated-union contract"
        )


def test_download_results_are_immutable() -> None:
    """Frozen dataclasses prevent accidental mutation of terminal state."""
    complete = DownloadComplete(
        bytes_written=1,
        elapsed=0.0,
        sha256="",
        etag=EMPTY_ETAG,
        content_type="",
    )
    with pytest.raises(Exception, match=r"frozen|cannot assign"):
        complete.bytes_written = 999  # type: ignore[misc]


def test_match_statement_narrows_result() -> None:
    """Smoke test for the pattern-match ergonomics callers are expected
    to use — the CLI does this in production."""

    def describe(r: DownloadComplete | DownloadPartial) -> str:
        match r:
            case DownloadComplete(sha256=sha):
                return f"ok:{sha}"
            case DownloadPartial(valid_length=vl):
                return f"partial:valid={vl}"

    complete = DownloadComplete(
        bytes_written=0,
        elapsed=0.0,
        sha256="deadbeef",
        etag=EMPTY_ETAG,
        content_type="",
    )
    partial = DownloadPartial(
        bytes_written=0,
        valid_length=1024,
        elapsed=0.0,
    )
    assert describe(complete) == "ok:deadbeef"
    assert describe(partial) == "partial:valid=1024"

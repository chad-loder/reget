# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.2.0](https://github.com/chad-loder/reget/compare/v0.1.0...v0.2.0) (2026-04-25)


### Features

* **cli:** IEC size table, regex parse_size, shared proxy and insecure ([3243710](https://github.com/chad-loder/reget/commit/3243710e0a08eb05622ee8cfbbbc10af345277fe))
* initial commit ([0843fe2](https://github.com/chad-loder/reget/commit/0843fe21e496652554951eff5bc93288f75b0d58))
* preflight destination paths with path_fits and DestinationError ([c3d27f8](https://github.com/chad-loder/reget/commit/c3d27f8213fe40422728b9dea15b1b53817b67ec))


### Bug Fixes

* open part file in binary mode on Windows; skip path_fits long-path test matrix on win32 ([82bbfbe](https://github.com/chad-loder/reget/commit/82bbfbe7ce192a51c442e6516a9c702b33e173b9))


### Documentation

* refresh README, quick start, and dev clone URL ([8fa89c6](https://github.com/chad-loder/reget/commit/8fa89c6c9b8ba9453668a69adc4cc7fb58b3d1bc))


### Code Refactoring

* centralize HTTP header and request helpers in _http_common ([e481ff0](https://github.com/chad-loder/reget/commit/e481ff096d7682f3b3a8c01af2bc06e5ee122a32))

## [Unreleased]

### Added

- Cursor-based single-range resume engine (sync and async).
- Crash-safe persistence via `.part` + `.part.ctrl` (JSON checkpoint) files.
- Transport adapters for httpx, niquests, requests, and urllib3.
- Async engine with niquests and httpx async adapters.
- `Content-Range` parser with full RFC 9110 coverage.
- CLI (`reget` command) with proxy, TLS, timeout, and backend selection.
- `posix_fallocate` / `ftruncate` pre-allocation for reduced fragmentation.

### Changed

- Build system switched from hatchling to uv-build.
- Architecture rewritten from multi-piece parallel model to single-range
  cursor model.

## [0.1.0] - 2026-04-18

- Project bootstrapped.

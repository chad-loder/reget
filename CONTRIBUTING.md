# Contributing to reget

Thanks for your interest in contributing. This document covers the dev
environment, the commit/PR conventions, and the release workflow.

## Prerequisites

- [uv](https://docs.astral.sh/uv/) (package manager)
- [just](https://just.systems/) (command runner) --
  `brew install just` or `cargo install just`

## Development setup

```bash
git clone https://github.com/reget/reget.git
cd reget
just dev
```

`just dev` runs `just setup` (install deps, git hooks) then `just test`.
That single command is all you need to go from clone to green test suite.

## Common commands

| Command | What it does |
|---|---|
| `just dev` | Full setup + test (first-time or after pulling) |
| `just test` | Run pytest |
| `just lint` | All linters: ruff, mypy, pyright, shellcheck, markdownlint |
| `just lint-fix` | Auto-fix everything fixable, then run checks |
| `just ci` | Full CI locally (pre-commit + pytest with coverage) |
| `just install-cli` | Install `reget` CLI on PATH (editable, niquests backend) |
| `just clean` | Remove caches, build artifacts |

<details>
<summary>Without just (raw uv commands)</summary>

```bash
uv sync --all-groups              # install deps
uv run pytest                     # tests
uv run pytest --cov=reget         # tests + coverage
uv run ruff check .               # lint
uv run ruff format .              # format
uv run mypy src tests             # type-check (mypy)
uv run pyright                    # type-check (pyright, strict)
uv build                          # build sdist + wheel
```

</details>

## Pre-commit hooks

Hooks are installed automatically by `just dev`. To re-install manually:

```bash
uv run pre-commit install
```

The hook runs ruff (lint + format), markdownlint, and structural checks
(trailing whitespace, merge conflicts, large files) on every commit.

## Commit messages

This repository uses [Conventional Commits](https://www.conventionalcommits.org/).
The PR title is what matters most -- it's validated on every pull request
and is what `release-please` reads when deciding the next version.

| Type | Meaning | Pre-1.0 bump | Post-1.0 bump |
|---|---|---|---|
| `feat:` | new feature | patch | minor |
| `fix:` | bug fix | patch | patch |
| `perf:` | performance improvement | patch | patch |
| `refactor:` | internal change, no behavior diff | none | none |
| `docs:` | documentation | none | none |
| `test:` | tests only | none | none |
| `build:` | build system changes | none | none |
| `ci:` | CI changes | none | none |
| `chore:` | maintenance | none | none |
| `style:` | formatting, whitespace, etc. | none | none |

A breaking change is either a `!` after the type (`feat!: ...`) or a
`BREAKING CHANGE:` footer in the commit body. Either triggers a **major**
bump (or `0.x.0` bump pre-1.0).

Subject lines should:

- start with a **lowercase** verb (`add ...`, not `Add ...`)
- not end with a period
- be in the imperative mood (`add`, not `adds` / `added`)

## Release workflow

We don't cut releases manually. The flow is:

1. Merge PRs to `main`. Each PR title is a conventional commit.
2. On every push to `main`, the **Release** workflow runs
   [`release-please`](https://github.com/googleapis/release-please),
   which opens or updates a **"Release PR"** with the next version,
   updated `CHANGELOG.md`, and bumped `pyproject.toml`.
3. When ready to release, merge the Release PR. That triggers:
   - A git tag (e.g. `v0.2.0`) and a GitHub Release.
   - The `publish` job, which builds via `uv build` and publishes to
     PyPI using **Trusted Publishing** (OIDC; no API tokens).

### Nightly builds

A scheduled workflow (`nightly.yml`) runs once a day. It stamps a
`.devYYYYMMDD` suffix on the current `pyproject.toml` version, builds
sdist + wheel, and publishes to **TestPyPI**. To install a nightly:

```bash
uv pip install --index-url https://test.pypi.org/simple/ \
               --extra-index-url https://pypi.org/simple/ \
               reget
```

## Reporting security issues

See [`SECURITY.md`](SECURITY.md). Please do **not** file public issues
for vulnerabilities.

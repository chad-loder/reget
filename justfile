set shell := ["bash", "-euo", "pipefail", "-c"]
set export := true

# Global Variables (Automatically exported to Bash)
PYTHON_FILES := "."

# --- Main Entry Points ---

# Install all default dependency groups (test + lint), git hooks, then run tests.
dev: setup test

setup:
    uv sync --all-groups
    git config commit.gpgsign true
    uv run pre-commit install --install-hooks
    @{{ just_executable() }} setup-hooks

# --- Linting & Quality ---

# Run all linters
lint: lint-py lint-sh lint-docs

# Auto-fix everything fixable, run all checks
lint-fix: lint-py-fix lint-sh-fix lint-docs-fix

lint-py:
    uv run ruff check .
    uv run ruff format --check .
    uv run mypy src tests
    uv run pyright

lint-py-fix:
    uv run ruff check --fix .
    uv run ruff format .

lint-sh:
    #!/usr/bin/env bash
    set -euo pipefail
    targets=()
    while IFS= read -r _f; do
        targets+=("$_f")
    done < <(git ls-files '*.sh')
    for hook in .githooks/commit-msg .githooks/pre-commit; do
        if [[ -f "$hook" ]]; then
            targets+=("$hook")
        fi
    done
    if (( ${#targets[@]} == 0 )); then
        echo "No tracked scripts found."
        exit 0
    fi
    echo "Linting ${#targets[@]} scripts..."
    command -v shellcheck >/dev/null || { echo "error: install shellcheck (e.g. brew install shellcheck)" >&2; exit 1; }
    shellcheck -x "${targets[@]}"

lint-sh-fix:
    #!/usr/bin/env bash
    set +e
    {{ just_executable() }} lint-sh
    _exit=$?
    set -e
    printf '\033[33mNote: shellcheck has no auto-fix mode — review the output above and fix manually.\033[0m\n' >&2
    exit $_exit

lint-docs:
    uv run pre-commit run markdownlint-cli2 --all-files

lint-docs-fix:
    uv run pre-commit run markdownlint-cli2-fix --hook-stage manual --all-files

# --- Testing ---

test:
    uv run pytest

# --- CI Helpers ---

ci: setup
    uv run pre-commit run --all-files --show-diff-on-failure
    uv run pytest --cov=./ --cov-report=xml

# --- Maintainer-local git hooks (.githooks → .git/hooks) ---

# Copies tracked hook scripts into .git/hooks. If dest differs from src but dest's
# blob is not in git object DB, treat as local edits and abort (show diff).
[private]
_safe_install src dest:
    #!/usr/bin/env bash
    set -euo pipefail
    _src={{ quote(src) }}
    _dest={{ quote(dest) }}
    SOURCE_HASH=$(git hash-object "$_src")
    DEST_HASH=$(git hash-object "$_dest" 2>/dev/null || echo "none")
    if [[ "$DEST_HASH" != "none" && "$SOURCE_HASH" != "$DEST_HASH" ]]; then
      if ! git rev-list --all --objects | grep -q "$DEST_HASH"; then
        echo "ERROR: Local changes in $_dest"
        diff -u --label "REPO" --label "LOCAL" "$_src" "$_dest" || true
        exit 1
      fi
    fi
    install -m 755 "$_src" "$_dest"

setup-hooks:
    @{{ just_executable() }} _safe_install .githooks/commit-msg .git/hooks/commit-msg

# --- CLI ---

# Install reget as a standalone CLI tool on PATH (editable, changes take effect immediately)
install-cli:
    uv tool install --editable ".[niquests]"

uninstall-cli:
    uv tool uninstall reget

# --- Maintenance ---

clean:
    uvx pyclean . --debris all

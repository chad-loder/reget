"""Console entrypoint and ``python -m reget`` shim.

The CLI needs at least one HTTP client extra: ``niquests``, ``requests``,
``httpx``, or ``urllib3`` (install ``reget[niquests]``, ``reget[requests]``,
``reget[httpx]``, or ``reget[urllib3]`` respectively).
"""

from __future__ import annotations

import importlib.util
import sys

_HTTP_CLIENT_MODULES = ("niquests", "requests", "httpx", "urllib3")


def main() -> int:
    if not any(importlib.util.find_spec(name) for name in _HTTP_CLIENT_MODULES):
        sys.stderr.write(
            "Error: no HTTP client is installed. Install at least one of:\n"
            "  pip install reget[niquests]   # default CLI backend\n"
            "  pip install reget[requests]\n"
            "  pip install reget[httpx]\n"
            "  pip install reget[urllib3]\n",
        )
        return 1
    from reget.cli import main as cli_main

    return cli_main()


if __name__ == "__main__":
    raise SystemExit(main())

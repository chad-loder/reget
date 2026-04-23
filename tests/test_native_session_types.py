"""Tests for :mod:`reget.transport.native_session_types`."""

from __future__ import annotations

import importlib


def test_module_imports_without_pulling_clients() -> None:
    """Aliases resolve via ``session_input`` + ``TYPE_CHECKING`` without optional stacks."""
    m = importlib.import_module("reget.transport.native_session_types")
    assert m.HttpxClient is not None
    assert m.NiquestsSession is not None
    assert m.RequestsLibrarySession is not None
    assert m.SupportedNativeHttpSession is not None


def test_session_input_defines_any_session() -> None:
    si = importlib.import_module("reget.transport.session_input")
    assert si.AnySession is not None

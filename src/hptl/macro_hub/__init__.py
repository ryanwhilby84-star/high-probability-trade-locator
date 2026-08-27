"""Macro Hub — shared USD / yields / crypto / cross-asset data pool."""

from __future__ import annotations

__all__ = ["build_macro_hub_payload", "run", "write_macro_hub_exports"]


def build_macro_hub_payload(*args, **kwargs):
    from hptl.macro_hub.pool_builder import build_macro_hub_payload as _fn

    return _fn(*args, **kwargs)


def run(*args, **kwargs):
    from hptl.macro_hub.macro_hub_export import run as _fn

    return _fn(*args, **kwargs)


def write_macro_hub_exports(*args, **kwargs):
    from hptl.macro_hub.macro_hub_export import write_macro_hub_exports as _fn

    return _fn(*args, **kwargs)

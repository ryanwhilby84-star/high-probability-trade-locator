"""ASCII-safe console output for Windows cp1252 terminals."""

from __future__ import annotations

import sys
from typing import Any

# Order matters for composed symbols (e.g. beta before generic strip).
_ASCII_REPLACEMENTS: tuple[tuple[str, str], ...] = (
    ("\u03b2", "beta"),
    ("\u0392", "Beta"),
    ("\u0394", "delta"),
    ("\u00b1", "+/-"),
    ("\u00d7", "x"),
    ("\u2192", "->"),
    ("\u2190", "<-"),
    ("\u2014", "-"),
    ("\u2013", "-"),
    ("\u00b0", " deg"),
    ("\u2264", "<="),
    ("\u2265", ">="),
    ("\u2248", "~"),
    ("\u00a0", " "),
    ("\u2019", "'"),
    ("\u2018", "'"),
    ("\u201c", '"'),
    ("\u201d", '"'),
)


def configure_stdout_utf8() -> None:
    """Best-effort UTF-8 stdout on Windows; no-op if unsupported."""
    for stream in (sys.stdout, sys.stderr):
        reconfigure = getattr(stream, "reconfigure", None)
        if callable(reconfigure):
            try:
                reconfigure(encoding="utf-8", errors="replace")
            except Exception:
                pass


def ascii_safe(value: Any) -> str:
    """Return ASCII-only text safe for cp1252 consoles."""
    text = str(value)
    for old, new in _ASCII_REPLACEMENTS:
        text = text.replace(old, new)
    return text.encode("ascii", errors="replace").decode("ascii")


def safe_print(*args: Any, **kwargs: Any) -> None:
    """Print with Unicode normalized to ASCII equivalents."""
    sep = kwargs.pop("sep", " ")
    end = kwargs.pop("end", "\n")
    file = kwargs.pop("file", sys.stdout)
    message = sep.join(ascii_safe(a) for a in args)
    file.write(message + end)
    if kwargs.get("flush") or True:
        file.flush()

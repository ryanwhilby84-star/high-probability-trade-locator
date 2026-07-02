"""Shared plumbing for currency-rate ingestion adapters (FX Valuation V1).

This module defines the *normalized contract* that every central-bank /
sovereign-yield adapter must satisfy, plus the HTTP + cache + offline
machinery they share. Source-specific parsing lives in the individual
``*_adapter.py`` modules; nothing here knows about any particular bank.

Design rules
------------
* Each adapter returns a :class:`NormalizedRate`. The valuation engine and the
  master ``fx_currency_rates.json`` only ever see normalized data — never raw
  source payloads.
* Every successful fetch is cached to ``data/cache/fx_rates/``. When
  ``HPTL_SKIP_LIVE_FEEDS=1`` (the offline build mode used elsewhere in HPTL),
  adapters read cache only and never touch the network.
* Failures are non-fatal at the field level: an adapter records a per-field
  ``error`` and leaves the value ``None`` so the audit can surface it instead
  of the pipeline crashing.
* Freshness is field-type aware. Policy rates are *step* series (the published
  ``as_of`` is the date of the last change and can be months old while still
  being current); daily sovereign yields must be recent. The two therefore use
  different staleness windows — see :data:`POLICY_MAX_STALENESS_DAYS` /
  :data:`YIELD_MAX_STALENESS_DAYS`.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

import requests

from hptl.config import DATA_DIR

CACHE_DIR = DATA_DIR / "cache" / "fx_rates"

# Staleness windows (days). Policy rates change on scheduled decisions and can
# legitimately sit unchanged for many months, so they get a wide window; daily
# government yields should refresh every business day.
POLICY_MAX_STALENESS_DAYS = 400
YIELD_MAX_STALENESS_DAYS = 10
CPI_MAX_STALENESS_DAYS = 400

DEFAULT_TIMEOUT = 40
DEFAULT_RETRIES = 3
USER_AGENT = "Mozilla/5.0 (HPTL FX rate ingestion; +https://hptl.local)"

# Full browser-like headers for sources that filter non-browser clients.
BROWSER_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-GB,en;q=0.9",
}

PASS, WARN, FAIL = "PASS", "WARN", "FAIL"


def offline_mode() -> bool:
    """True when live feeds are disabled (cache-only), matching the build flag."""
    return os.getenv("HPTL_SKIP_LIVE_FEEDS", "").strip() in {"1", "true", "True", "yes"}


def now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def today_iso() -> str:
    return date.today().isoformat()


def _parse_date(value: Any) -> date | None:
    if not value:
        return None
    s = str(value)[:10]
    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%m/%d/%Y"):
        try:
            return datetime.strptime(s, fmt).date()
        except ValueError:
            continue
    return None


def field_status(
    value: float | None,
    as_of: str | None,
    *,
    kind: str = "yield",
    reference: date | None = None,
) -> str:
    """PASS / WARN / FAIL for a single field given its value + observation date.

    ``kind`` is ``"policy"``, ``"yield"``, or ``"cpi"`` and selects the staleness window.
    Missing value -> FAIL. Present but stale (or undated) -> WARN. Fresh -> PASS.
    """
    if value is None:
        return FAIL
    ref = reference or date.today()
    d = _parse_date(as_of)
    if d is None:
        return WARN
    max_days = {
        "policy": POLICY_MAX_STALENESS_DAYS,
        "yield": YIELD_MAX_STALENESS_DAYS,
        "cpi": CPI_MAX_STALENESS_DAYS,
    }.get(kind, YIELD_MAX_STALENESS_DAYS)
    return PASS if (ref - d).days <= max_days else WARN


@dataclass
class FieldValue:
    """One normalized metric (policy rate / 2Y / 10Y) with provenance."""

    value: float | None = None
    as_of: str | None = None
    source: str | None = None
    error: str | None = None

    @property
    def ok(self) -> bool:
        return self.value is not None


@dataclass
class NormalizedRate:
    """Identical normalized output every adapter must return.

    The valuation engine and master JSON consume this shape only; they never
    see where the numbers came from beyond the ``source`` label.
    """

    currency: str
    central_bank: str
    policy: FieldValue = field(default_factory=FieldValue)
    y2: FieldValue = field(default_factory=FieldValue)
    y10: FieldValue = field(default_factory=FieldValue)
    fetched_at: str = field(default_factory=now_iso)
    notes: list[str] = field(default_factory=list)

    # -- derived ----------------------------------------------------------
    @property
    def source_label(self) -> str:
        srcs: list[str] = []
        for fv in (self.policy, self.y2, self.y10):
            if fv.source and fv.source not in srcs:
                srcs.append(fv.source)
        return "/".join(srcs) if srcs else "missing"

    @property
    def errors(self) -> list[str]:
        out: list[str] = []
        for name, fv in (("policy_rate", self.policy), ("y2", self.y2), ("y10", self.y10)):
            if fv.error:
                out.append(f"{name}: {fv.error}")
        return out

    def status(self, *, reference: date | None = None) -> str:
        """Roll up the three field statuses into one currency status."""
        statuses = [
            field_status(self.policy.value, self.policy.as_of, kind="policy", reference=reference),
            field_status(self.y2.value, self.y2.as_of, kind="yield", reference=reference),
            field_status(self.y10.value, self.y10.as_of, kind="yield", reference=reference),
        ]
        if FAIL in statuses:
            return FAIL
        if WARN in statuses:
            return WARN
        return PASS

    def as_currency_block(self, *, reference: date | None = None) -> dict[str, Any]:
        """Serialize to the ``fx_currency_rates.json`` ``currencies[CODE]`` schema.

        Backwards compatible with :mod:`hptl.fx.currency_rates` (keeps
        ``policy_rate`` / ``y2`` / ``y10`` / ``*_as_of`` / ``source`` /
        ``data_quality``) and adds richer per-field provenance + a status.
        """
        live = any(fv.source and fv.source != "missing" for fv in (self.policy, self.y2, self.y10))
        as_of_dates = [fv.as_of for fv in (self.policy, self.y2, self.y10) if fv.as_of]
        return {
            "central_bank": self.central_bank,
            "policy_rate": self.policy.value,
            "y2": self.y2.value,
            "y10": self.y10.value,
            "policy_rate_as_of": self.policy.as_of,
            "y2_as_of": self.y2.as_of,
            "y10_as_of": self.y10.as_of,
            "source": self.source_label,
            "data_quality": "live" if live else "missing",
            "status": self.status(reference=reference),
            "as_of": max(as_of_dates) if as_of_dates else None,
            "fetched_at": self.fetched_at,
            "field_sources": {
                "policy_rate": self.policy.source,
                "y2": self.y2.source,
                "y10": self.y10.source,
            },
            "field_status": {
                "policy_rate": field_status(self.policy.value, self.policy.as_of, kind="policy", reference=reference),
                "y2": field_status(self.y2.value, self.y2.as_of, kind="yield", reference=reference),
                "y10": field_status(self.y10.value, self.y10.as_of, kind="yield", reference=reference),
            },
            "errors": self.errors,
            "notes": list(self.notes),
        }


class FeedError(RuntimeError):
    """Raised when a source cannot be fetched and no cache is available."""


def _cache_path(cache_key: str, *, binary: bool) -> Path:
    suffix = ".bin" if binary else ".txt"
    return CACHE_DIR / f"{cache_key}{suffix}"


def _read_cache(cache_key: str, *, binary: bool) -> bytes | str | None:
    path = _cache_path(cache_key, binary=binary)
    if not path.exists():
        return None
    return path.read_bytes() if binary else path.read_text(encoding="utf-8")


def _write_cache(cache_key: str, payload: bytes | str, *, binary: bool) -> None:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    path = _cache_path(cache_key, binary=binary)
    if binary:
        path.write_bytes(payload)  # type: ignore[arg-type]
    else:
        path.write_text(payload, encoding="utf-8")  # type: ignore[arg-type]


def _fetch(
    url: str,
    *,
    cache_key: str,
    binary: bool,
    timeout: int,
    retries: int,
    headers: dict[str, str] | None,
) -> bytes | str:
    """Fetch ``url`` with retries + caching, honouring offline mode."""
    if offline_mode():
        cached = _read_cache(cache_key, binary=binary)
        if cached is None:
            raise FeedError(f"offline mode and no cache for {cache_key!r} ({url})")
        return cached

    hdrs = {"User-Agent": USER_AGENT}
    if headers:
        hdrs.update(headers)

    last_exc: Exception | None = None
    for attempt in range(1, retries + 1):
        try:
            resp = requests.get(url, timeout=timeout, headers=hdrs)
            resp.raise_for_status()
            payload: bytes | str = resp.content if binary else resp.text
            _write_cache(cache_key, payload, binary=binary)
            return payload
        except Exception as exc:  # noqa: BLE001 - we want to retry/fall back on anything
            last_exc = exc

    # Network failed — fall back to last good cache if we have one.
    cached = _read_cache(cache_key, binary=binary)
    if cached is not None:
        return cached
    raise FeedError(f"failed to fetch {url}: {type(last_exc).__name__}: {last_exc}")


def fetch_text(
    url: str,
    *,
    cache_key: str,
    timeout: int = DEFAULT_TIMEOUT,
    retries: int = DEFAULT_RETRIES,
    headers: dict[str, str] | None = None,
) -> str:
    return _fetch(url, cache_key=cache_key, binary=False, timeout=timeout, retries=retries, headers=headers)  # type: ignore[return-value]


def fetch_bytes(
    url: str,
    *,
    cache_key: str,
    timeout: int = DEFAULT_TIMEOUT,
    retries: int = DEFAULT_RETRIES,
    headers: dict[str, str] | None = None,
) -> bytes:
    return _fetch(url, cache_key=cache_key, binary=True, timeout=timeout, retries=retries, headers=headers)  # type: ignore[return-value]


def to_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        f = float(str(value).strip().replace(",", ""))
    except (TypeError, ValueError):
        return None
    return f if f == f else None

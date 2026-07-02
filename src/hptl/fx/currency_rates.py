"""Currency rate inputs (policy rate + 2Y/10Y government yields) for FX valuation.

This is the *data provider* layer for FX Valuation V1. It loads curated currency
rate records from ``data/config/fx_currency_rates.json`` and exposes them with
freshness metadata so downstream valuation can degrade confidence safely when
data is missing or stale.

The repo currently has only a US FRED rates pipeline (``rates_clean.csv``); there
is no live foreign-yield / foreign-policy-rate feed. Until one is wired, the
config file is the single, auditable source of truth — edited by hand and clearly
labelled (``source="manual_seed"``). See ``fx_fair_value`` for the regression prep.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import date, datetime
from functools import lru_cache
from pathlib import Path
from typing import Any

from hptl.config import DATA_DIR

CONFIG_PATH = DATA_DIR / "config" / "fx_currency_rates.json"

# Central bank mapping (authoritative — used even if config omits it).
CENTRAL_BANKS: dict[str, str] = {
    "USD": "Federal Reserve",
    "EUR": "ECB",
    "GBP": "Bank of England",
    "JPY": "Bank of Japan",
    "AUD": "Reserve Bank of Australia",
    "NZD": "Reserve Bank of New Zealand",
    "CAD": "Bank of Canada",
    "CHF": "Swiss National Bank",
}

SUPPORTED_CURRENCIES: tuple[str, ...] = tuple(CENTRAL_BANKS.keys())

DEFAULT_MAX_STALENESS_DAYS = 45
# CPI is annual / low-frequency; wide window.
CPI_MAX_STALENESS_DAYS = 400
# Policy rates are step series (the as_of is the last change date), so they get a
# much wider staleness window than daily government yields.
POLICY_MAX_STALENESS_DAYS = 400


def _num(v: Any) -> float | None:
    if v is None or isinstance(v, bool):
        return None
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    return f if f == f else None


def _parse_date(v: Any) -> date | None:
    if not v:
        return None
    try:
        return datetime.strptime(str(v)[:10], "%Y-%m-%d").date()
    except (TypeError, ValueError):
        return None


@dataclass(frozen=True)
class CurrencyRate:
    """Rate inputs for a single currency with freshness/quality metadata."""

    code: str
    central_bank: str
    policy_rate: float | None
    y2: float | None
    y10: float | None
    policy_rate_as_of: str | None
    y2_as_of: str | None
    y10_as_of: str | None
    source: str
    data_quality: str
    yield_label: str | None = None
    cpi_yoy: float | None = None
    cpi_yoy_as_of: str | None = None
    missing_fields: list[str] = field(default_factory=list)
    stale_fields: list[str] = field(default_factory=list)

    @property
    def has_policy(self) -> bool:
        return self.policy_rate is not None

    @property
    def has_2y(self) -> bool:
        return self.y2 is not None

    @property
    def has_10y(self) -> bool:
        return self.y10 is not None

    @property
    def has_cpi(self) -> bool:
        return self.cpi_yoy is not None

    @property
    def is_stale(self) -> bool:
        return bool(self.stale_fields)

    @property
    def latest_as_of(self) -> str | None:
        dates = [d for d in (self.policy_rate_as_of, self.y2_as_of, self.y10_as_of, self.cpi_yoy_as_of) if d]
        return max(dates) if dates else None

    def as_dict(self) -> dict[str, Any]:
        return {
            "code": self.code,
            "central_bank": self.central_bank,
            "policy_rate": self.policy_rate,
            "y2": self.y2,
            "y10": self.y10,
            "cpi_yoy": self.cpi_yoy,
            "policy_rate_as_of": self.policy_rate_as_of,
            "y2_as_of": self.y2_as_of,
            "y10_as_of": self.y10_as_of,
            "cpi_yoy_as_of": self.cpi_yoy_as_of,
            "real_yield": round(self.y2 - self.cpi_yoy, 3) if self.y2 is not None and self.cpi_yoy is not None else None,
            "source": self.source,
            "data_quality": self.data_quality,
            "yield_label": self.yield_label,
            "missing_fields": list(self.missing_fields),
            "stale_fields": list(self.stale_fields),
            "is_stale": self.is_stale,
        }


@lru_cache(maxsize=4)
def _load_config(path_str: str | None = None) -> dict[str, Any]:
    path = Path(path_str) if path_str else CONFIG_PATH
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}


def _reference_date(cfg: dict[str, Any]) -> date:
    # Freshness is measured against the config's generated_at (auditable, stable),
    # falling back to today so a forgotten config still flags as stale over time.
    return _parse_date(cfg.get("generated_at")) or date.today()


def get_currency_rate(code: str, *, config_path: str | None = None) -> CurrencyRate:
    """Return the rate record for ``code`` with missing/stale flags computed."""
    code = code.upper()
    cfg = _load_config(config_path)
    central_bank = (cfg.get("central_banks") or {}).get(code) or CENTRAL_BANKS.get(code, "Unknown")
    raw = (cfg.get("currencies") or {}).get(code) or {}

    policy = _num(raw.get("policy_rate"))
    y2 = _num(raw.get("y2"))
    y10 = _num(raw.get("y10"))
    cpi = _num(raw.get("cpi_yoy"))

    missing: list[str] = []
    if policy is None:
        missing.append("policy_rate")
    if y2 is None:
        missing.append("y2")
    if y10 is None:
        missing.append("y10")
    if cpi is None:
        missing.append("cpi_yoy")

    # Yields are daily series; policy rates are *step* series whose as_of is the
    # last change date and can sit unchanged for months while remaining current.
    # Use the config window for yields and a generous window for policy.
    yield_stale = int(cfg.get("max_staleness_days") or DEFAULT_MAX_STALENESS_DAYS)
    policy_stale = int(cfg.get("max_staleness_days_policy") or POLICY_MAX_STALENESS_DAYS)
    cpi_stale = int(cfg.get("max_staleness_days_cpi") or CPI_MAX_STALENESS_DAYS)
    ref = _reference_date(cfg)
    stale: list[str] = []
    field_live = raw.get("field_live") or {}
    for label, value, as_of, max_stale in (
        ("policy_rate", policy, raw.get("policy_rate_as_of"), policy_stale),
        ("y2", y2, raw.get("y2_as_of"), yield_stale),
        ("y10", y10, raw.get("y10_as_of"), yield_stale),
        ("cpi_yoy", cpi, raw.get("cpi_yoy_as_of"), cpi_stale),
    ):
        if value is None:
            continue
        # A present-but-not-live (carried/seed) value is treated as stale so the
        # valuation engine never reports trusted confidence on un-refreshed data.
        if label in field_live and not field_live[label]:
            stale.append(label)
            continue
        d = _parse_date(as_of)
        if d is None or (ref - d).days > max_stale:
            stale.append(label)

    return CurrencyRate(
        code=code,
        central_bank=str(raw.get("central_bank") or central_bank),
        policy_rate=policy,
        y2=y2,
        y10=y10,
        cpi_yoy=cpi,
        policy_rate_as_of=raw.get("policy_rate_as_of"),
        y2_as_of=raw.get("y2_as_of"),
        y10_as_of=raw.get("y10_as_of"),
        cpi_yoy_as_of=raw.get("cpi_yoy_as_of"),
        source=str(raw.get("source") or ("missing" if not raw else "unknown")),
        data_quality=str(raw.get("data_quality") or ("missing" if not raw else "unknown")),
        yield_label=raw.get("yield_label"),
        missing_fields=missing,
        stale_fields=stale,
    )


def all_currency_rates(*, config_path: str | None = None) -> dict[str, CurrencyRate]:
    return {code: get_currency_rate(code, config_path=config_path) for code in SUPPORTED_CURRENCIES}


def config_meta(*, config_path: str | None = None) -> dict[str, Any]:
    cfg = _load_config(config_path)
    return {
        "schema_version": cfg.get("schema_version"),
        "generated_at": cfg.get("generated_at"),
        "max_staleness_days": int(cfg.get("max_staleness_days") or DEFAULT_MAX_STALENESS_DAYS),
        "note": cfg.get("note"),
        "config_path": str(CONFIG_PATH),
        "available": bool(cfg),
    }


def clear_cache() -> None:
    _load_config.cache_clear()

"""Index Valuation V2 audit — CAPE, ERP, dividend yield feasibility (audit-only).

Usage:
    python -m hptl.valuation.index_valuation_v2_audit

Writes:
    data/audits/index_valuation_v2_audit.json
    data/audits/index_valuation_v2_audit.md

Does not modify live valuation scores, scanner outputs, or trade signals.
"""

from __future__ import annotations

import io
import json
import re
import sys
import time
from datetime import date, datetime, timezone
from typing import Any, Sequence

import pandas as pd
import requests

from hptl.config import DATA_DIR, get_fmp_api_key, get_fred_api_key, get_settings
from hptl.data_sources.fmp_client import FmpClient, redact_secrets as redact_fmp
from hptl.data_sources.fred_client import FredAuditClient, redact_secrets as redact_fred

CONFIG_PATH = DATA_DIR / "config" / "index_valuation_v2_audit.json"
AUDIT_JSON = DATA_DIR / "audits" / "index_valuation_v2_audit.json"
AUDIT_MD = DATA_DIR / "audits" / "index_valuation_v2_audit.md"

MISSING_VALUE = "."
_SECRET_RE = re.compile(r"(api_key|apikey|apiKey)[\"'=:\s]+[^\s&\"']+", re.I)


# ---------------------------------------------------------------------------
# Pure valuation math (testable, audit-only — not wired to live engine)
# ---------------------------------------------------------------------------


def percentile_rank(current: float, history: Sequence[float]) -> float | None:
    """Percentile rank of *current* within *history* (0–100)."""
    values = [float(v) for v in history if v is not None and v == v]
    if not values:
        return None
    return sum(1 for x in values if x <= current) / len(values) * 100.0


def earnings_yield_from_cape(cape: float | None) -> float | None:
    if cape is None or cape <= 0:
        return None
    return (1.0 / float(cape)) * 100.0


def equity_risk_premium_pct(
    earnings_yield_pct: float | None,
    ten_year_yield_pct: float | None,
) -> float | None:
    if earnings_yield_pct is None or ten_year_yield_pct is None:
        return None
    return float(earnings_yield_pct) - float(ten_year_yield_pct)


def composite_valuation_score(
    cape_percentile: float | None,
    erp_percentile: float | None,
) -> float | None:
    if cape_percentile is None or erp_percentile is None:
        return None
    return round(((100.0 - cape_percentile) + erp_percentile) / 2.0, 2)


def valuation_state_from_score(score: float | None) -> str | None:
    if score is None:
        return None
    if score < 20:
        return "Premium Overvalued"
    if score < 40:
        return "Overvalued"
    if score < 60:
        return "Neutral valuation"
    if score < 80:
        return "Undervalued"
    return "Discount Undervalued"


def assess_confidence(
    *,
    cape_available: bool,
    ten_year_available: bool,
    monthly_observations: int,
    cape_age_days: int | None,
    ten_year_age_days: int | None,
    cape_source_verified: bool,
    rules: dict[str, Any],
) -> tuple[str, list[str]]:
    warnings: list[str] = []
    min_high = int(rules.get("min_monthly_observations_high", 36))
    min_low = int(rules.get("min_monthly_observations_low", 24))
    cape_max = int(rules.get("cape_max_age_days", 60))
    ty_max = int(rules.get("ten_year_max_age_days", 10))

    if not cape_available:
        warnings.append("missing CAPE")
    if not ten_year_available:
        warnings.append("missing 10Y yield")
    if monthly_observations < min_low:
        warnings.append(f"fewer than {min_low} monthly observations")
    if cape_age_days is not None and cape_age_days > cape_max:
        warnings.append(f"CAPE older than {cape_max} days")
    if ten_year_age_days is not None and ten_year_age_days > ty_max:
        warnings.append(f"10Y yield older than {ty_max} days")
    if not cape_source_verified:
        warnings.append("CAPE source unverified")

    if not cape_available or not ten_year_available:
        return "Low", warnings
    if monthly_observations < min_low:
        return "Low", warnings
    if warnings:
        return "Medium", warnings
    if (
        monthly_observations >= min_high
        and (cape_age_days is None or cape_age_days <= cape_max)
        and (ten_year_age_days is None or ten_year_age_days <= ty_max)
        and cape_source_verified
    ):
        return "High", warnings
    return "Medium", warnings


def redact_all_secrets(text: str) -> str:
    if not text:
        return text
    out = redact_fred(text)
    out = redact_fmp(out)
    return _SECRET_RE.sub("api_key=***", out)


# ---------------------------------------------------------------------------
# Config / date helpers
# ---------------------------------------------------------------------------


def _load_config() -> dict[str, Any]:
    if not CONFIG_PATH.exists():
        return {}
    return json.loads(CONFIG_PATH.read_text(encoding="utf-8"))


def _parse_date(s: str | None) -> date | None:
    if not s:
        return None
    try:
        return datetime.strptime(str(s)[:10], "%Y-%m-%d").date()
    except ValueError:
        pass
    try:
        frac = float(str(s).strip())
        year = int(frac)
        month = int(round((frac - year) * 12)) or 1
        month = min(max(month, 1), 12)
        return date(year, month, 1)
    except (TypeError, ValueError):
        return None


def _numeric_observations(obs: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for o in obs:
        d = str(o.get("date") or "")[:10]
        raw = o.get("value")
        if not d or raw is None or str(raw).strip() in ("", MISSING_VALUE):
            continue
        try:
            val = float(raw)
        except (TypeError, ValueError):
            continue
        out.append({"date": d, "value": val})
    return out


def _staleness_threshold_days(frequency: str | None, cfg: dict[str, Any]) -> int:
    bands = cfg.get("staleness_days") or {}
    f = str(frequency or "").strip()
    if f in bands:
        return int(bands[f])
    if "Daily" in f:
        return int(bands.get("Daily", 10))
    if "Weekly" in f:
        return int(bands.get("Weekly", 21))
    if "Monthly" in f:
        return int(bands.get("Monthly", 60))
    if "Quarterly" in f:
        return int(bands.get("Quarterly", 120))
    return int(bands.get("default", 45))


def _staleness_status(latest: date | None, frequency: str | None, cfg: dict[str, Any]) -> str:
    if latest is None:
        return "unknown"
    age = (date.today() - latest).days
    threshold = _staleness_threshold_days(frequency, cfg)
    if age <= threshold:
        return "fresh"
    if age <= threshold * 2:
        return "stale"
    return "discontinued_or_severely_stale"


def _usefulness(
    *,
    available: bool,
    staleness: str,
    numeric_count: int,
    role: str,
) -> str:
    if not available:
        return "unusable — fetch failed"
    if staleness == "discontinued_or_severely_stale":
        return f"low — stale/discontinued for {role}"
    if numeric_count < 24:
        return f"low — insufficient history for {role}"
    if staleness == "stale":
        return f"medium — usable {role} with freshness caveat"
    return f"high — suitable for index valuation ({role})"


# ---------------------------------------------------------------------------
# Source probes
# ---------------------------------------------------------------------------


def _audit_fred_series(
    client: FredAuditClient,
    spec: dict[str, Any],
    cfg: dict[str, Any],
) -> dict[str, Any]:
    series_id = str(spec.get("series_id") or "")
    friendly = spec.get("friendly_name") or series_id
    probe = client.probe_series(series_id, tail_limit=12)

    if not probe.get("ok"):
        return {
            "friendly_name": friendly,
            "source": "fred",
            "series_id": series_id,
            "endpoint": f"fred/series/observations?series_id={series_id}",
            "role": spec.get("role"),
            "index": spec.get("index"),
            "available": False,
            "earliest_date": None,
            "latest_date": None,
            "frequency": None,
            "observation_count": 0,
            "numeric_observation_count": 0,
            "missing_value_count": 0,
            "staleness_status": "unknown",
            "staleness_age_days": None,
            "usefulness": _usefulness(
                available=False, staleness="unknown", numeric_count=0, role=str(spec.get("role") or "")
            ),
            "last_12_values": [],
            "notes": redact_all_secrets(probe.get("error") or "fetch failed"),
            "elapsed_ms": probe.get("elapsed_ms"),
        }

    meta = probe.get("metadata") or {}
    all_obs = probe.get("all_observations") or []
    numeric = _numeric_observations(all_obs)
    missing_count = sum(
        1 for o in all_obs if str(o.get("value") or "").strip() in ("", MISSING_VALUE)
    )
    earliest = numeric[0]["date"] if numeric else None
    latest = numeric[-1]["date"] if numeric else None
    frequency = meta.get("frequency") or meta.get("frequency_short")
    latest_d = _parse_date(latest)
    staleness = _staleness_status(latest_d, str(frequency), cfg)

    tail = _numeric_observations(probe.get("tail_observations") or [])[:12]

    notes: list[str] = []
    if spec.get("notes"):
        notes.append(str(spec["notes"]))
    if meta.get("title"):
        notes.append(str(meta.get("title"))[:120])
    if series_id == "WILL5000PRFC" and staleness != "fresh":
        notes.append("Wilshire series may have been removed or discontinued on FRED (2024)")

    return {
        "friendly_name": friendly,
        "source": "fred",
        "series_id": series_id,
        "endpoint": f"fred/series/observations?series_id={series_id}",
        "role": spec.get("role"),
        "index": spec.get("index"),
        "available": True,
        "earliest_date": earliest,
        "latest_date": latest,
        "frequency": frequency,
        "observation_count": len(all_obs),
        "numeric_observation_count": len(numeric),
        "missing_value_count": missing_count,
        "staleness_status": staleness,
        "staleness_age_days": (date.today() - latest_d).days if latest_d else None,
        "usefulness": _usefulness(
            available=True,
            staleness=staleness,
            numeric_count=len(numeric),
            role=str(spec.get("role") or ""),
        ),
        "last_12_values": tail,
        "notes": "; ".join(notes) if notes else None,
        "elapsed_ms": probe.get("elapsed_ms"),
        "_numeric_series": numeric,
    }


def _fetch_public_url(url: str, *, timeout: int | None = None) -> dict[str, Any]:
    settings = get_settings()
    t = timeout if timeout is not None else settings.request_timeout_seconds
    started = time.monotonic()
    try:
        resp = requests.get(url, timeout=t, headers={"User-Agent": "HPTL-audit/1.0"})
        elapsed_ms = round((time.monotonic() - started) * 1000, 1)
        if resp.status_code >= 400:
            return {
                "ok": False,
                "status_code": resp.status_code,
                "text": None,
                "elapsed_ms": elapsed_ms,
                "error": f"HTTP {resp.status_code}",
            }
        return {
            "ok": True,
            "status_code": resp.status_code,
            "text": resp.text,
            "elapsed_ms": elapsed_ms,
            "error": None,
        }
    except requests.RequestException as exc:
        elapsed_ms = round((time.monotonic() - started) * 1000, 1)
        return {
            "ok": False,
            "status_code": None,
            "text": None,
            "elapsed_ms": elapsed_ms,
            "error": f"{type(exc).__name__}: {exc}",
        }


def parse_yale_shiller_csv(text: str) -> list[dict[str, Any]]:
    """Parse Yale Shiller ie_data.csv into monthly records."""
    df = pd.read_csv(io.StringIO(text), skiprows=7)
    df.columns = [str(c).strip() for c in df.columns]
    date_col = df.columns[0]
    cape_col = next((c for c in df.columns if c.upper() in ("CAPE", "P/E10", "P/E 10")), None)
    price_col = next((c for c in df.columns if c.strip().upper() in ("P", "S&P COMPOSITE")), None)
    div_col = next((c for c in df.columns if c.strip().upper() == "D"), None)
    gs10_col = next((c for c in df.columns if "GS10" in c.upper() or "RATE" in c.upper()), None)

    rows: list[dict[str, Any]] = []
    for _, row in df.iterrows():
        d = _parse_date(str(row.get(date_col)))
        if d is None:
            continue
        cape = None
        if cape_col is not None:
            try:
                v = float(row[cape_col])
                cape = v if v == v and v > 0 else None
            except (TypeError, ValueError):
                pass
        div_yield = None
        if price_col is not None and div_col is not None:
            try:
                p, div = float(row[price_col]), float(row[div_col])
                if p > 0 and div == div:
                    div_yield = div / p * 100.0
            except (TypeError, ValueError):
                pass
        gs10 = None
        if gs10_col is not None:
            try:
                v = float(row[gs10_col])
                gs10 = v if v == v else None
            except (TypeError, ValueError):
                pass
        if cape is None and div_yield is None and gs10 is None:
            continue
        rows.append(
            {
                "date": d.isoformat()[:10],
                "cape": cape,
                "dividend_yield_pct": div_yield,
                "ten_year_yield_pct": gs10,
            }
        )
    rows.sort(key=lambda r: r["date"])
    return rows


def _audit_yale_shiller(spec: dict[str, Any], cfg: dict[str, Any]) -> dict[str, Any]:
    url = str(spec.get("url") or "")
    fetch = _fetch_public_url(url)
    if not fetch.get("ok") or not fetch.get("text"):
        return {
            "friendly_name": spec.get("friendly_name"),
            "source": "yale_shiller",
            "series_id": "ie_data.csv",
            "endpoint": url,
            "role": "cape,dividend_yield,ten_year_yield",
            "index": spec.get("index"),
            "available": False,
            "earliest_date": None,
            "latest_date": None,
            "frequency": "Monthly",
            "observation_count": 0,
            "numeric_observation_count": 0,
            "missing_value_count": 0,
            "staleness_status": "unknown",
            "staleness_age_days": None,
            "usefulness": _usefulness(
                available=False, staleness="unknown", numeric_count=0, role="cape"
            ),
            "last_12_values": [],
            "notes": redact_all_secrets(fetch.get("error") or "fetch failed"),
            "elapsed_ms": fetch.get("elapsed_ms"),
        }

    try:
        parsed = parse_yale_shiller_csv(fetch["text"])
    except Exception as exc:
        return {
            "friendly_name": spec.get("friendly_name"),
            "source": "yale_shiller",
            "series_id": "ie_data.csv",
            "endpoint": url,
            "role": "cape,dividend_yield,ten_year_yield",
            "index": spec.get("index"),
            "available": False,
            "earliest_date": None,
            "latest_date": None,
            "frequency": "Monthly",
            "observation_count": 0,
            "numeric_observation_count": 0,
            "missing_value_count": 0,
            "staleness_status": "unknown",
            "staleness_age_days": None,
            "usefulness": "unusable — parse failed",
            "last_12_values": [],
            "notes": redact_all_secrets(str(exc)),
            "elapsed_ms": fetch.get("elapsed_ms"),
        }

    cape_rows = [r for r in parsed if r.get("cape") is not None]
    earliest = cape_rows[0]["date"] if cape_rows else None
    latest = cape_rows[-1]["date"] if cape_rows else None
    latest_d = _parse_date(latest)
    staleness = _staleness_status(latest_d, "Monthly", cfg)
    tail = [
        {"date": r["date"], "cape": r["cape"], "dividend_yield_pct": r.get("dividend_yield_pct")}
        for r in cape_rows[-12:]
    ]

    return {
        "friendly_name": spec.get("friendly_name"),
        "source": "yale_shiller",
        "series_id": "ie_data.csv",
        "endpoint": url,
        "role": "cape,dividend_yield,ten_year_yield",
        "index": spec.get("index"),
        "available": True,
        "earliest_date": earliest,
        "latest_date": latest,
        "frequency": "Monthly",
        "observation_count": len(parsed),
        "numeric_observation_count": len(cape_rows),
        "missing_value_count": len(parsed) - len(cape_rows),
        "staleness_status": staleness,
        "staleness_age_days": (date.today() - latest_d).days if latest_d else None,
        "usefulness": _usefulness(
            available=True,
            staleness=staleness,
            numeric_count=len(cape_rows),
            role="cape",
        ),
        "last_12_values": tail,
        "notes": "Robert Shiller official dataset — primary free CAPE source for S&P 500",
        "elapsed_ms": fetch.get("elapsed_ms"),
        "_yale_monthly": parsed,
    }


def _audit_multpl_page(spec: dict[str, Any]) -> dict[str, Any]:
    url = str(spec.get("url") or "")
    fetch = _fetch_public_url(url)
    available = bool(fetch.get("ok") and fetch.get("text") and "Shiller" in (fetch.get("text") or ""))
    return {
        "friendly_name": spec.get("friendly_name"),
        "source": "multpl",
        "series_id": "shiller-pe-page",
        "endpoint": url,
        "role": "cape_reference",
        "index": spec.get("index"),
        "available": available,
        "earliest_date": None,
        "latest_date": None,
        "frequency": None,
        "observation_count": 0,
        "numeric_observation_count": 0,
        "missing_value_count": 0,
        "staleness_status": "unknown",
        "staleness_age_days": None,
        "usefulness": "reference only — HTML page, not machine series" if available else "unusable — page unreachable",
        "last_12_values": [],
        "notes": "Use Yale CSV or FRED/Nasdaq Multpl API for programmatic CAPE",
        "elapsed_ms": fetch.get("elapsed_ms"),
    }


def _audit_nasdaq_dataset(spec: dict[str, Any]) -> dict[str, Any]:
    import os

    key = os.getenv("NASDAQ_DATA_LINK_API_KEY", "").strip()
    code = str(spec.get("dataset_code") or "")
    if not key:
        return {
            "friendly_name": spec.get("friendly_name"),
            "source": "nasdaq_data_link",
            "series_id": code,
            "endpoint": f"https://data.nasdaq.com/api/v3/datasets/{code}.json",
            "role": spec.get("role"),
            "index": spec.get("index"),
            "available": False,
            "earliest_date": None,
            "latest_date": None,
            "frequency": None,
            "observation_count": 0,
            "numeric_observation_count": 0,
            "missing_value_count": 0,
            "staleness_status": "unknown",
            "staleness_age_days": None,
            "usefulness": "unusable — NASDAQ_DATA_LINK_API_KEY not set",
            "last_12_values": [],
            "notes": "Optional; Yale/FRED preferred for audit",
            "elapsed_ms": None,
        }

    url = f"https://data.nasdaq.com/api/v3/datasets/{code}.json"
    fetch = _fetch_public_url(f"{url}?api_key={key}")
    # Redact key from any stored endpoint reference
    safe_endpoint = f"{url}?api_key=***"
    available = bool(fetch.get("ok"))
    obs_count = 0
    notes = fetch.get("error")
    if available and fetch.get("text"):
        try:
            payload = json.loads(fetch["text"])
            data = (payload.get("dataset") or {}).get("data") or []
            obs_count = len(data)
            notes = "Nasdaq Data Link dataset reachable"
        except json.JSONDecodeError:
            available = False
            notes = "non-JSON response"

    return {
        "friendly_name": spec.get("friendly_name"),
        "source": "nasdaq_data_link",
        "series_id": code,
        "endpoint": safe_endpoint,
        "role": spec.get("role"),
        "index": spec.get("index"),
        "available": available,
        "earliest_date": None,
        "latest_date": None,
        "frequency": None,
        "observation_count": obs_count,
        "numeric_observation_count": obs_count,
        "missing_value_count": 0,
        "staleness_status": "unknown",
        "staleness_age_days": None,
        "usefulness": "high — programmatic Multpl mirror" if available else "unusable",
        "last_12_values": [],
        "notes": redact_all_secrets(str(notes or "")),
        "elapsed_ms": fetch.get("elapsed_ms"),
    }


def _audit_fmp_probe(client: FmpClient, spec: dict[str, Any]) -> dict[str, Any]:
    path = str(spec.get("path") or "")
    symbol = spec.get("symbol")
    params: dict[str, str] = {}
    if symbol:
        params["symbol"] = str(symbol)
    probe = client.probe_get(path, **params)

    if not probe.get("ok"):
        return {
            "friendly_name": spec.get("friendly_name"),
            "source": "fmp",
            "series_id": symbol or path,
            "endpoint": f"fmp/{path}",
            "role": spec.get("role"),
            "index": spec.get("index"),
            "available": False,
            "earliest_date": None,
            "latest_date": None,
            "frequency": None,
            "observation_count": 0,
            "numeric_observation_count": 0,
            "missing_value_count": 0,
            "staleness_status": "unknown",
            "staleness_age_days": None,
            "usefulness": _usefulness(
                available=False, staleness="unknown", numeric_count=0, role=str(spec.get("role") or "")
            ),
            "last_12_values": [],
            "notes": redact_all_secrets(probe.get("error") or "fetch failed"),
            "elapsed_ms": probe.get("elapsed_ms"),
        }

    payload = probe.get("payload")
    records: list[dict[str, Any]] = []
    if isinstance(payload, list):
        records = [r for r in payload if isinstance(r, dict)]
    elif isinstance(payload, dict):
        for key in ("historical", "data", "rates"):
            if isinstance(payload.get(key), list):
                records = [r for r in payload[key] if isinstance(r, dict)]
                break

    dates = sorted(str(r.get("date") or "")[:10] for r in records if r.get("date"))
    earliest, latest = (dates[0], dates[-1]) if dates else (None, None)
    latest_d = _parse_date(latest)
    role = str(spec.get("role") or "")
    staleness = _staleness_status(latest_d, "Daily" if role == "price_history" else "Daily", {"staleness_days": {"Daily": 10}})

    usefulness = _usefulness(
        available=True,
        staleness=staleness,
        numeric_count=len(records),
        role=role,
    )
    if role == "ten_year_yield":
        usefulness = "medium — treasury rates available but not CAPE/ERP inputs alone"
    elif role == "price_history":
        usefulness = "medium — price EOD only; cannot compute CAPE/ERP without earnings"

    tail = records[:12] if records else []

    return {
        "friendly_name": spec.get("friendly_name"),
        "source": "fmp",
        "series_id": symbol or path,
        "endpoint": f"fmp/{path}",
        "role": spec.get("role"),
        "index": spec.get("index"),
        "available": True,
        "earliest_date": earliest,
        "latest_date": latest,
        "frequency": "Daily",
        "observation_count": len(records),
        "numeric_observation_count": len(records),
        "missing_value_count": 0,
        "staleness_status": staleness,
        "staleness_age_days": (date.today() - latest_d).days if latest_d else None,
        "usefulness": usefulness,
        "last_12_values": tail,
        "notes": None,
        "elapsed_ms": probe.get("elapsed_ms"),
    }


# ---------------------------------------------------------------------------
# S&P 500 example + feasibility
# ---------------------------------------------------------------------------


def _series_row_by_role(series_audit: list[dict[str, Any]], role: str) -> dict[str, Any] | None:
    for row in series_audit:
        if row.get("available") and row.get("role") == role:
            return row
        if row.get("available") and role in str(row.get("role") or ""):
            return row
    return None


def _fmp_ten_year_latest(series_audit: list[dict[str, Any]]) -> tuple[float | None, int | None, str | None]:
    row = next(
        (
            r
            for r in series_audit
            if r.get("source") == "fmp" and r.get("role") == "ten_year_yield" and r.get("available")
        ),
        None,
    )
    if not row:
        return None, None, None
    records = row.get("last_12_values") or []
    if not records:
        return None, None, None
    # FMP treasury probe stores newest first
    latest_row = records[0]
    ty = latest_row.get("year10")
    if ty is None:
        return None, None, None
    try:
        val = float(ty)
    except (TypeError, ValueError):
        return None, None, None
    ld = _parse_date(str(latest_row.get("date") or ""))
    age = (date.today() - ld).days if ld else None
    return val, age, "fmp_treasury_rates"


def _fred_numeric_by_role(series_audit: list[dict[str, Any]], role: str) -> list[dict[str, Any]]:
    row = _series_row_by_role(series_audit, role)
    if not row or row.get("source") != "fred":
        return []
    return list(row.get("_numeric_series") or [])


def build_sp500_example(
    series_audit: list[dict[str, Any]],
    cfg: dict[str, Any],
) -> dict[str, Any]:
    """Compute audit-only S&P 500 CAPE / ERP / composite from probed sources."""
    window = int(cfg.get("percentile_window_months", 36))
    rules = cfg.get("confidence_rules") or {}

    yale = next((r for r in series_audit if r.get("source") == "yale_shiller" and r.get("available")), None)
    fred_cape = _series_row_by_role(series_audit, "cape")
    fred_div = _series_row_by_role(series_audit, "dividend_yield")
    fred_10y = _series_row_by_role(series_audit, "ten_year_yield")

    monthly: list[dict[str, Any]] = list((yale or {}).get("_yale_monthly") or [])

    # FRED Multpl CAPE fallback if Yale missing
    if not monthly and fred_cape and fred_cape.get("_numeric_series"):
        monthly = [
            {"date": o["date"], "cape": o["value"], "dividend_yield_pct": None, "ten_year_yield_pct": None}
            for o in fred_cape["_numeric_series"]
        ]

    cape_history = [float(r["cape"]) for r in monthly if r.get("cape") is not None]
    if not cape_history:
        conf, warnings = assess_confidence(
            cape_available=False,
            ten_year_available=False,
            monthly_observations=0,
            cape_age_days=None,
            ten_year_age_days=None,
            cape_source_verified=False,
            rules=rules,
        )
        return {
            "metrics": {
                "cape": None,
                "earnings_yield_pct": None,
                "erp_pct": None,
                "dividend_yield_pct": None,
                "ten_year_yield_pct": None,
            },
            "percentiles": {
                "cape_percentile": None,
                "erp_percentile": None,
                "dividend_yield_percentile": None,
            },
            "composite_score": None,
            "state": None,
            "confidence": conf,
            "warnings": warnings,
            "data_sources_used": [],
        }

    current = monthly[-1]
    cape = float(current["cape"])
    earnings_yield = earnings_yield_from_cape(cape)

    # 10Y: prefer fresh FRED daily, else Yale monthly
    ten_year: float | None = None
    ten_year_age: int | None = None
    ten_year_source = None
    fred_10y_num = _fred_numeric_by_role(series_audit, "ten_year_yield")
    if fred_10y and fred_10y.get("available") and fred_10y_num:
        latest = fred_10y_num[-1]
        ten_year = float(latest["value"])
        ten_year_source = "fred_dgs10"
        ld = _parse_date(latest["date"])
        ten_year_age = (date.today() - ld).days if ld else None
    elif current.get("ten_year_yield_pct") is not None:
        ten_year = float(current["ten_year_yield_pct"])
        ten_year_source = "yale_gs10"
        ld = _parse_date(current["date"])
        ten_year_age = (date.today() - ld).days if ld else None
    else:
        fmp_ty, fmp_age, fmp_src = _fmp_ten_year_latest(series_audit)
        if fmp_ty is not None:
            ten_year = fmp_ty
            ten_year_age = fmp_age
            ten_year_source = fmp_src

    erp = equity_risk_premium_pct(earnings_yield, ten_year)

    # Dividend yield
    div_yield: float | None = None
    if current.get("dividend_yield_pct") is not None:
        div_yield = float(current["dividend_yield_pct"])
    elif fred_div and fred_div.get("_numeric_series"):
        div_yield = float(fred_div["_numeric_series"][-1]["value"])

    # Monthly ERP history for percentile
    erp_history: list[float] = []
    div_history: list[float] = []
    for r in monthly:
        c = r.get("cape")
        ty = r.get("ten_year_yield_pct")
        if c is None or ty is None:
            continue
        ey = earnings_yield_from_cape(float(c))
        e = equity_risk_premium_pct(ey, float(ty))
        if e is not None:
            erp_history.append(e)
        if r.get("dividend_yield_pct") is not None:
            div_history.append(float(r["dividend_yield_pct"]))

    cape_window = cape_history[-window:]
    erp_window = erp_history[-window:]
    div_window = div_history[-window:]

    cape_pct = percentile_rank(cape, cape_window)
    erp_pct = percentile_rank(erp, erp_window) if erp is not None else None
    div_pct = percentile_rank(div_yield, div_window) if div_yield is not None and div_window else None

    composite = composite_valuation_score(cape_pct, erp_pct)
    state = valuation_state_from_score(composite)

    cape_ld = _parse_date(current["date"])
    cape_age = (date.today() - cape_ld).days if cape_ld else None
    cape_verified = yale is not None or (fred_cape is not None and fred_cape.get("available"))

    conf, warnings = assess_confidence(
        cape_available=True,
        ten_year_available=ten_year is not None,
        monthly_observations=len(cape_history),
        cape_age_days=cape_age,
        ten_year_age_days=ten_year_age,
        cape_source_verified=cape_verified,
        rules=rules,
    )

    sources_used = []
    if yale:
        sources_used.append("yale_shiller")
    if fred_cape and fred_cape.get("available"):
        sources_used.append("fred_multpl_cape")
    if ten_year_source:
        sources_used.append(ten_year_source)
    if fred_div and fred_div.get("available"):
        sources_used.append("fred_multpl_div_yield")

    return {
        "metrics": {
            "cape": round(cape, 4),
            "earnings_yield_pct": round(earnings_yield, 4) if earnings_yield is not None else None,
            "erp_pct": round(erp, 4) if erp is not None else None,
            "dividend_yield_pct": round(div_yield, 4) if div_yield is not None else None,
            "ten_year_yield_pct": round(ten_year, 4) if ten_year is not None else None,
        },
        "percentiles": {
            "cape_percentile": round(cape_pct, 2) if cape_pct is not None else None,
            "erp_percentile": round(erp_pct, 2) if erp_pct is not None else None,
            "dividend_yield_percentile": round(div_pct, 2) if div_pct is not None else None,
        },
        "composite_score": composite,
        "state": state,
        "confidence": conf,
        "warnings": warnings,
        "data_sources_used": sources_used,
        "as_of_cape_date": current.get("date"),
    }


def build_feasibility(
    series_audit: list[dict[str, Any]],
    sp500_example: dict[str, Any],
    sources_tested: list[dict[str, Any]],
) -> dict[str, Any]:
    yale_ok = any(r.get("source") == "yale_shiller" and r.get("available") for r in series_audit)
    fred_cape_ok = any(
        r.get("source") == "fred" and r.get("role") == "cape" and r.get("available") for r in series_audit
    )
    fred_10y_ok = any(
        r.get("source") == "fred" and r.get("role") == "ten_year_yield" and r.get("available") for r in series_audit
    )
    wilshire = next((r for r in series_audit if r.get("series_id") == "WILL5000PRFC"), None)
    fmp_price_ok = any(
        r.get("source") == "fmp" and r.get("role") == "price_history" and r.get("available") for r in series_audit
    )

    cape_available = yale_ok or fred_cape_ok
    erp_computable = sp500_example.get("metrics", {}).get("erp_pct") is not None
    div_available = sp500_example.get("metrics", {}).get("dividend_yield_pct") is not None

    strong: list[str] = []
    weak: list[str] = []
    missing: list[str] = []

    if yale_ok:
        strong.append("Yale Shiller CAPE (S&P 500)")
    elif fred_cape_ok:
        strong.append("FRED Multpl Shiller PE")
    else:
        missing.append("S&P 500 CAPE source")

    if fred_10y_ok:
        strong.append("FRED DGS10 (daily 10Y yield for ERP)")
    else:
        weak.append("FRED DGS10 — needs FRED_API_KEY or use Yale monthly GS10")

    if div_available:
        strong.append("Dividend yield (Yale or FRED Multpl)")
    else:
        missing.append("S&P 500 dividend yield")

    if fmp_price_ok:
        weak.append("FMP index EOD prices (price layer only — no CAPE)")
    else:
        missing.append("FMP index price history (optional)")

    if wilshire and not wilshire.get("available"):
        weak.append("Wilshire 5000 (WILL5000PRFC) unavailable — expected post-2024 FRED removal")
    elif wilshire and wilshire.get("available"):
        weak.append("Wilshire 5000 available but not required for V2 core")

    for idx in ("NASDAQ", "FTSE", "DAX", "NIKKEI"):
        price_row = next(
            (r for r in series_audit if r.get("index") == idx and r.get("role") == "price_history"),
            None,
        )
        if price_row and price_row.get("available"):
            weak.append(f"{idx} price history via FMP")
        missing.append(f"{idx} CAPE / earnings yield (no free series in audit)")

    sp500_buildable = cape_available and (fred_10y_ok or yale_ok) and erp_computable
    nasdaq_buildable = False  # no free CAPE in blueprint
    intl_buildable = False

    production_ready = (
        sp500_buildable
        and sp500_example.get("confidence") == "High"
        and sp500_example.get("composite_score") is not None
    )

    return {
        "can_build_sp500_model": sp500_buildable,
        "can_build_nasdaq_model": nasdaq_buildable,
        "can_build_ftse_dax_nikkei_models": intl_buildable,
        "cape_available": cape_available,
        "erp_calculable": erp_computable,
        "dividend_yield_available": div_available,
        "production_ready_after_audit": production_ready,
        "keep_current_index_valuation_low_confidence": True,
        "strong_inputs": strong,
        "weak_inputs": weak,
        "missing_inputs": missing,
        "fred_vs_fmp_summary": (
            "FRED (+ Yale Shiller) supplies CAPE, dividend yield, and bond yields for ERP. "
            "FMP supplies index price/seasonality only — insufficient alone for Index Valuation V2."
        ),
        "recommended_next_step": (
            "Run this audit with FRED_API_KEY set and network access; if Yale + DGS10 validate, "
            "build offline shadow Index Valuation V2 for ^GSPC only. Extend international indices "
            "only after sourcing CAPE/earnings (e.g. OECD, national stats, or paid datasets)."
        ),
    }


def _strip_internal_fields(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out = []
    for r in rows:
        clean = {k: v for k, v in r.items() if not str(k).startswith("_")}
        out.append(clean)
    return out


def run_audit(*, write_files: bool = True) -> dict[str, Any]:
    cfg = _load_config()
    now = datetime.now(timezone.utc).isoformat()

    report: dict[str, Any] = {
        "generated_at": now,
        "mode": "audit_only",
        "integration_status": "not wired to live scanner or valuation pillar",
        "api_keys": {
            "fred_configured": bool(get_fred_api_key()),
            "fred_key_length": len(get_fred_api_key()) if get_fred_api_key() else 0,
            "fmp_configured": bool(get_fmp_api_key()),
            "fmp_key_length": len(get_fmp_api_key()) if get_fmp_api_key() else 0,
        },
        "sources_tested": [],
        "series_audit": [],
        "sp500_example": {},
        "feasibility": {},
    }

    sources_tested: list[dict[str, Any]] = []
    series_audit: list[dict[str, Any]] = []

    # FRED
    fred_key = get_fred_api_key()
    if fred_key:
        client = FredAuditClient()
        for spec in cfg.get("fred_series") or []:
            row = _audit_fred_series(client, spec, cfg)
            series_audit.append(row)
            sources_tested.append(
                {
                    "source": "fred",
                    "id": spec.get("series_id"),
                    "available": row.get("available"),
                    "error": row.get("notes") if not row.get("available") else None,
                }
            )
    else:
        for spec in cfg.get("fred_series") or []:
            row = {
                "friendly_name": spec.get("friendly_name"),
                "source": "fred",
                "series_id": spec.get("series_id"),
                "endpoint": f"fred/series/observations?series_id={spec.get('series_id')}",
                "role": spec.get("role"),
                "index": spec.get("index"),
                "available": False,
                "earliest_date": None,
                "latest_date": None,
                "frequency": None,
                "observation_count": 0,
                "numeric_observation_count": 0,
                "missing_value_count": 0,
                "staleness_status": "unknown",
                "staleness_age_days": None,
                "usefulness": "unusable — FRED_API_KEY not set",
                "last_12_values": [],
                "notes": "Set FRED_API_KEY in environment to probe FRED series",
                "elapsed_ms": None,
            }
            series_audit.append(row)
            sources_tested.append(
                {"source": "fred", "id": spec.get("series_id"), "available": False, "error": "FRED_API_KEY not set"}
            )

    # Public sources
    for spec in cfg.get("public_sources") or []:
        fmt = str(spec.get("format") or "")
        if fmt == "csv":
            row = _audit_yale_shiller(spec, cfg)
        else:
            row = _audit_multpl_page(spec)
        series_audit.append(row)
        sources_tested.append(
            {
                "source": row.get("source"),
                "id": row.get("series_id"),
                "available": row.get("available"),
                "error": row.get("notes") if not row.get("available") else None,
            }
        )

    # Nasdaq Data Link (optional key)
    for spec in cfg.get("nasdaq_data_link") or []:
        row = _audit_nasdaq_dataset(spec)
        series_audit.append(row)
        sources_tested.append(
            {
                "source": "nasdaq_data_link",
                "id": spec.get("dataset_code"),
                "available": row.get("available"),
                "error": row.get("notes") if not row.get("available") else None,
            }
        )

    # FMP
    if get_fmp_api_key():
        fmp = FmpClient()
        for spec in cfg.get("fmp_probes") or []:
            row = _audit_fmp_probe(fmp, spec)
            series_audit.append(row)
            sources_tested.append(
                {
                    "source": "fmp",
                    "id": spec.get("symbol") or spec.get("path"),
                    "available": row.get("available"),
                    "error": row.get("notes") if not row.get("available") else None,
                }
            )
    else:
        for spec in cfg.get("fmp_probes") or []:
            row = {
                "friendly_name": spec.get("friendly_name"),
                "source": "fmp",
                "series_id": spec.get("symbol") or spec.get("path"),
                "endpoint": f"fmp/{spec.get('path')}",
                "role": spec.get("role"),
                "index": spec.get("index"),
                "available": False,
                "usefulness": "unusable — FMP_API_KEY not set",
                "notes": "Set FMP_API_KEY to probe FMP index endpoints",
            }
            series_audit.append(row)
            sources_tested.append(
                {
                    "source": "fmp",
                    "id": spec.get("symbol") or spec.get("path"),
                    "available": False,
                    "error": "FMP_API_KEY not set",
                }
            )

    sp500 = build_sp500_example(series_audit, cfg)
    feasibility = build_feasibility(series_audit, sp500, sources_tested)

    report["sources_tested"] = sources_tested
    report["series_audit"] = _strip_internal_fields(series_audit)
    report["sp500_example"] = sp500
    report["feasibility"] = feasibility
    report["summary"] = {
        "series_probed": len(series_audit),
        "series_available": sum(1 for r in series_audit if r.get("available")),
        "series_failed": sum(1 for r in series_audit if not r.get("available")),
    }

    if write_files:
        AUDIT_JSON.parent.mkdir(parents=True, exist_ok=True)
        AUDIT_JSON.write_text(json.dumps(report, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
        AUDIT_MD.write_text(_render_md(report), encoding="utf-8")

    return report


def _render_md(report: dict[str, Any]) -> str:
    lines = [
        "# Index Valuation V2 Audit",
        "",
        f"- Generated (UTC): {report.get('generated_at')}",
        f"- Mode: {report.get('mode')} — {report.get('integration_status')}",
        "",
    ]
    keys = report.get("api_keys") or {}
    lines.append(
        f"- FRED key configured: {keys.get('fred_configured')} | "
        f"FMP key configured: {keys.get('fmp_configured')}"
    )
    lines.append("")

    sm = report.get("summary") or {}
    lines.extend(
        [
            "## Summary",
            "",
            f"- Series probed: {sm.get('series_probed', 0)}",
            f"- Available: {sm.get('series_available', 0)} | Failed: {sm.get('series_failed', 0)}",
            "",
            "## Series audit",
            "",
            "| Name | Source | ID | OK | Latest | Freq | Staleness | Usefulness |",
            "|---|---|---|:---:|---|---|---|---|",
        ]
    )
    for r in report.get("series_audit") or []:
        lines.append(
            f"| {r.get('friendly_name')} | {r.get('source')} | {r.get('series_id')} | "
            f"{r.get('available')} | {r.get('latest_date') or '—'} | {r.get('frequency') or '—'} | "
            f"{r.get('staleness_status')} | {r.get('usefulness')} |"
        )

    ex = report.get("sp500_example") or {}
    m = ex.get("metrics") or {}
    p = ex.get("percentiles") or {}
    lines.extend(
        [
            "",
            "## S&P 500 example (audit-only)",
            "",
            f"- CAPE: {m.get('cape')} | Earnings yield: {m.get('earnings_yield_pct')}%",
            f"- 10Y yield: {m.get('ten_year_yield_pct')}% | ERP: {m.get('erp_pct')}%",
            f"- Dividend yield: {m.get('dividend_yield_pct')}%",
            f"- CAPE percentile (36m): {p.get('cape_percentile')} | ERP percentile: {p.get('erp_percentile')}",
            f"- Composite score: {ex.get('composite_score')} | State: {ex.get('state')}",
            f"- Confidence: {ex.get('confidence')} | Warnings: {', '.join(ex.get('warnings') or []) or '—'}",
            "",
        ]
    )

    f = report.get("feasibility") or {}
    lines.extend(
        [
            "## Feasibility",
            "",
            f"1. **S&P 500 model buildable (free)?** {f.get('can_build_sp500_model')}",
            f"2. **NASDAQ model buildable (free)?** {f.get('can_build_nasdaq_model')}",
            f"3. **FTSE/DAX/Nikkei buildable (free)?** {f.get('can_build_ftse_dax_nikkei_models')}",
            f"4. **CAPE available?** {f.get('cape_available')}",
            f"5. **ERP calculable?** {f.get('erp_calculable')}",
            f"6. **Dividend yield available?** {f.get('dividend_yield_available')}",
            f"7. **Production-ready after audit?** {f.get('production_ready_after_audit')}",
            f"8. **Keep current index valuation low-confidence?** {f.get('keep_current_index_valuation_low_confidence')}",
            "",
            f"**Strong inputs:** {', '.join(f.get('strong_inputs') or []) or '—'}",
            f"**Weak inputs:** {', '.join(f.get('weak_inputs') or []) or '—'}",
            f"**Missing inputs:** {', '.join(f.get('missing_inputs') or []) or '—'}",
            "",
            f"**Next step:** {f.get('recommended_next_step')}",
            "",
        ]
    )
    return "\n".join(lines)


def print_console_summary(report: dict[str, Any]) -> None:
    print("=" * 88)
    print("INDEX VALUATION V2 AUDIT (audit-only)")
    print("=" * 88)
    keys = report.get("api_keys") or {}
    print(f"FRED key configured : {'yes' if keys.get('fred_configured') else 'NO'}")
    print(f"FMP key configured  : {'yes' if keys.get('fmp_configured') else 'NO'}")
    sm = report.get("summary") or {}
    print(f"Series available    : {sm.get('series_available', 0)}/{sm.get('series_probed', 0)}")
    print("-" * 88)
    ex = report.get("sp500_example") or {}
    m = ex.get("metrics") or {}
    print(f"SP500 CAPE          : {m.get('cape')}")
    print(f"SP500 ERP           : {m.get('erp_pct')}%")
    print(f"Composite / state   : {ex.get('composite_score')} / {ex.get('state')}")
    print(f"Confidence          : {ex.get('confidence')}")
    f = report.get("feasibility") or {}
    print(f"SP500 buildable     : {f.get('can_build_sp500_model')}")
    print(f"Keep index low-conf : {f.get('keep_current_index_valuation_low_confidence')}")
    print(f"JSON                : {AUDIT_JSON}")
    print(f"Markdown            : {AUDIT_MD}")
    print("=" * 88)


def main(argv: list[str] | None = None) -> int:
    _ = argv
    report = run_audit(write_files=True)
    print_console_summary(report)
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))

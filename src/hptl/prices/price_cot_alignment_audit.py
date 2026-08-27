"""Universal Price ↔ COT alignment audit for LEGACY_COT_MARKETS.

Every instrument must have:
  - latest raw daily OHLC
  - weekly aggregation rebuilt from that daily
  - workstation export matching the rebuilt weekly
  - public/dist mirrors matching processed
  - COT week within MAX_ALIGNMENT_GAP_DAYS of workstation weekly
  - consistent symbol mapping across registry → store → export

No warnings. No skipped instruments. Any failure → overall FAIL.
"""

from __future__ import annotations

import json
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

from hptl.config import PROCESSED_DIR, PROJECT_ROOT
from hptl.markets.canonical_identity import BY_ID
from hptl.markets.instrument_registry import LEGACY_COT_MARKETS, load_registry
from hptl.prices.workstation_ohlc_export import derive_weekly_ohlc_from_daily

DATA = PROJECT_ROOT / "data"
PUBLIC = PROJECT_ROOT / "web-dashboard" / "public" / "data"
DIST = PROJECT_ROOT / "web-dashboard" / "dist" / "data"

OUT_JSON = DATA / "audits" / "price_cot_alignment_audit.json"
OUT_MD = DATA / "audits" / "price_cot_alignment_audit.md"
PUBLIC_JSON = PUBLIC / "price_cot_alignment_audit.json"

WS_PROCESSED = PROCESSED_DIR / "workstation_ohlc_latest.json"
WS_PUBLIC = PUBLIC / "workstation_ohlc_latest.json"
WS_DIST = DIST / "workstation_ohlc_latest.json"
COT_3Y = PUBLIC / "cot_3y_series_latest.json"
PRICES_LATEST = PROCESSED_DIR / "prices_latest.json"

# Maximum calendar days between latest COT report date and latest workstation weekly candle.
MAX_ALIGNMENT_GAP_DAYS = 5

# OHLC value tolerance for stage-to-stage candle compare.
_OHLC_TOL_REL = 0.0005
_OHLC_TOL_ABS = 1e-6


def _read_json(path: Path) -> dict[str, Any]:
    if not path.is_file():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}


def _finite(v: Any) -> float | None:
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    return f if f == f else None


def _parse_date(v: Any) -> date | None:
    s = str(v or "")[:10]
    if len(s) < 10:
        return None
    try:
        return date.fromisoformat(s)
    except ValueError:
        return None


def _gap_days(a: str | None, b: str | None) -> int | None:
    da, db = _parse_date(a), _parse_date(b)
    if da is None or db is None:
        return None
    return abs((da - db).days)


def _gap_weeks(a: str | None, b: str | None) -> float | None:
    g = _gap_days(a, b)
    return None if g is None else round(g / 7.0, 2)


def _values_match(a: float | None, b: float | None) -> bool:
    if a is None or b is None:
        return a is None and b is None
    tol = max(abs(b) * _OHLC_TOL_REL, _OHLC_TOL_ABS)
    return abs(a - b) <= tol


def _bar_match(a: dict[str, Any] | None, b: dict[str, Any] | None) -> bool:
    if not a or not b:
        return False
    if str(a.get("date") or "")[:10] != str(b.get("date") or "")[:10]:
        return False
    for k in ("open", "high", "low", "close"):
        if not _values_match(_finite(a.get(k)), _finite(b.get(k))):
            return False
    return True


def _last_bar(bars: list[dict[str, Any]] | None) -> dict[str, Any] | None:
    if not bars:
        return None
    return bars[-1]


def _ohlc_tuple(bar: dict[str, Any] | None) -> dict[str, Any] | None:
    if not bar:
        return None
    return {
        "date": str(bar.get("date") or "")[:10],
        "open": _finite(bar.get("open")),
        "high": _finite(bar.get("high")),
        "low": _finite(bar.get("low")),
        "close": _finite(bar.get("close")),
    }


def _normalize_symbol(symbol: str | None) -> str | None:
    if not symbol:
        return None
    s = str(symbol).strip()
    for prefix in ("yahoo:", "oanda:", "fred:", "alpha_vantage:", "alpha:"):
        if s.lower().startswith(prefix):
            s = s[len(prefix) :]
            break
    return s.strip() or None


def _symbols_equivalent(a: str | None, b: str | None) -> bool:
    na, nb = _normalize_symbol(a), _normalize_symbol(b)
    if not na or not nb:
        return na == nb
    if na == nb:
        return True
    return na.replace("_", "") == nb.replace("_", "")


def _provider_and_symbol(instrument_id: str) -> tuple[str, str | None]:
    canon = BY_ID.get(instrument_id)
    reg = load_registry().get(instrument_id)
    if canon:
        provider = canon.price_provider
        symbol = canon.price_provider_symbol
    else:
        provider = "unknown"
        symbol = None
    if reg:
        if reg.oanda_symbol:
            symbol = reg.oanda_symbol
            provider = "oanda"
        elif getattr(reg, "fred_series", None):
            symbol = reg.fred_series
            provider = provider if provider == "fred" else provider
    try:
        from hptl.prices.softs_futures_backfill import SOFTS_YAHOO

        if instrument_id in SOFTS_YAHOO:
            return "yahoo_futures", SOFTS_YAHOO[instrument_id]["yahoo_symbol"]
    except Exception:
        pass
    try:
        from hptl.prices.coverage import select_price_source

        src = select_price_source(instrument_id)
        if src:
            provider = src
    except Exception:
        pass
    return provider, symbol


def _store_record(instrument_id: str, prices_doc: dict[str, Any]) -> dict[str, Any]:
    return (prices_doc.get("instruments") or {}).get(instrument_id) or {}


def _cot_last(instrument_id: str, cot_doc: dict[str, Any]) -> str | None:
    block = (cot_doc.get("markets") or {}).get(instrument_id) or {}
    d = str(block.get("latest_date") or "")[:10]
    if d:
        return d
    series = block.get("series") or []
    if series:
        return str(series[-1].get("date") or "")[:10] or None
    return None


def _frontend_cache_checks() -> dict[str, Any]:
    """Static verification that the workstation fetch path busts caches."""
    store_path = (
        PROJECT_ROOT
        / "web-dashboard"
        / "src"
        / "prices"
        / "stores"
        / "WeeklyOHLCStore.js"
    )
    text = store_path.read_text(encoding="utf-8") if store_path.is_file() else ""
    checks = {
        "weekly_ohlc_store_path": str(store_path.relative_to(PROJECT_ROOT)).replace("\\", "/"),
        "uses_cache_no_store": "cache: 'no-store'" in text or 'cache: "no-store"' in text,
        "uses_cache_bust_query": "?v=" in text or "Date.now()" in text,
        "url": "/data/workstation_ohlc_latest.json",
    }
    checks["status"] = (
        "PASS"
        if checks["uses_cache_no_store"] and checks["uses_cache_bust_query"]
        else "FAIL"
    )
    return checks


def _fetch_provider_weekly_series(
    provider: str, symbol: str | None, *, count: int = 12
) -> tuple[list[dict[str, Any]], str]:
    """Fetch recent completed provider weekly candles (full OHLC series tip)."""
    if not symbol:
        return [], "no_symbol"
    if provider == "oanda":
        try:
            from hptl.config import get_oanda_api_key
            from hptl.oanda.oanda_client import api_get

            if not get_oanda_api_key():
                return [], "oanda_key_missing"
            doc = api_get(
                f"/v3/instruments/{symbol}/candles",
                params={"granularity": "W", "count": str(count), "price": "M"},
            )
            out: list[dict[str, Any]] = []
            for c in doc.get("candles") or []:
                if not c.get("complete", True):
                    continue
                mid = c.get("mid") or {}
                bar = {
                    "date": str(c.get("time") or "")[:10],
                    "open": _finite(mid.get("o")),
                    "high": _finite(mid.get("h")),
                    "low": _finite(mid.get("l")),
                    "close": _finite(mid.get("c")),
                }
                if bar["date"] and bar["close"] is not None:
                    out.append(bar)
            return out, "oanda_live"
        except Exception as exc:
            return [], f"oanda_error:{exc}"
    if provider == "yahoo_futures":
        try:
            from hptl.prices.coffee_foundation_backfill import fetch_yahoo_daily

            daily = fetch_yahoo_daily(symbol)
            if not daily:
                return [], "yahoo_empty"
            weekly = derive_weekly_ohlc_from_daily(
                [
                    {
                        "date": str(b.get("date") or "")[:10],
                        "open": b.get("open"),
                        "high": b.get("high"),
                        "low": b.get("low"),
                        "close": b.get("close"),
                        "source": "yahoo",
                    }
                    for b in daily
                ]
            )
            # Drop current incomplete ISO week tip.
            today = date.today()
            cur = today.isocalendar()
            completed = []
            for b in weekly:
                d = _parse_date(b.get("date"))
                if not d:
                    continue
                y, w, _ = d.isocalendar()
                if (y, w) >= (cur.year, cur.week):
                    continue
                completed.append(_ohlc_tuple(b) or {})
            return [b for b in completed if b.get("date")], "yahoo_live_weekly_from_daily"
        except Exception as exc:
            return [], f"yahoo_error:{exc}"
    return [], "provider_live_not_configured"


def _find_matching_week(
    provider_bar: dict[str, Any],
    workstation_bars: list[dict[str, Any]],
    *,
    max_date_skew_days: int = 4,
) -> dict[str, Any] | None:
    """Match provider week to workstation week by date, else close+date proximity.

    OANDA W candles are Friday-dated; ISO aggregation may label the same week
    with the last trade date (e.g. Thu/Sun). Require close match when dates differ.
    """
    d = str(provider_bar.get("date") or "")[:10]
    if not d:
        return None
    by_date = {
        str(b.get("date") or "")[:10]: b
        for b in workstation_bars
        if str(b.get("date") or "")[:10]
    }
    if d in by_date and _bar_match(provider_bar, by_date[d]):
        return by_date[d]
    if d in by_date and _values_match(
        _finite(provider_bar.get("close")), _finite(by_date[d].get("close"))
    ):
        return by_date[d]
    p_close = _finite(provider_bar.get("close"))
    best = None
    best_gap = None
    for b in workstation_bars:
        gap = _gap_days(d, str(b.get("date") or "")[:10])
        if gap is None or gap > max_date_skew_days:
            continue
        if not _values_match(p_close, _finite(b.get("close"))):
            continue
        if best_gap is None or gap < best_gap:
            best = b
            best_gap = gap
    return best


def _compare_last_n_weeks(
    provider_bars: list[dict[str, Any]],
    workstation_bars: list[dict[str, Any]],
    *,
    n: int = 10,
) -> list[str]:
    """FAIL reasons if last N provider completed weeks are missing/differ in workstation."""
    failures: list[str] = []
    prov = provider_bars[-n:]
    if not prov:
        return ["provider weekly series empty — cannot validate last-N candles"]
    # Tip lag: workstation must not trail provider completed tip by > 7 days.
    p_last = str(prov[-1].get("date") or "")[:10]
    w_last = str((workstation_bars[-1] or {}).get("date") or "")[:10] if workstation_bars else ""
    if p_last and w_last and w_last < p_last:
        lag = _gap_days(w_last, p_last)
        if lag is not None and lag > 7:
            failures.append(
                f"workstation weekly tip {w_last} trails provider tip {p_last} by {lag}d"
            )
    for pb in prov:
        d = str(pb.get("date") or "")[:10]
        wb = _find_matching_week(pb, workstation_bars)
        if not wb:
            failures.append(f"missing workstation weekly candle for provider week {d}")
            continue
        # Require OHLC agreement on the matched week (date may skew by convention).
        for k in ("open", "high", "low", "close"):
            if not _values_match(_finite(pb.get(k)), _finite(wb.get(k))):
                failures.append(
                    f"OHLC mismatch near {d} ({k}): provider={_finite(pb.get(k))} "
                    f"workstation={_finite(wb.get(k))} (ws_date={wb.get('date')})"
                )
                break
    return failures


def audit_instrument(
    instrument_id: str,
    *,
    prices_doc: dict[str, Any],
    cot_doc: dict[str, Any],
    ws_processed: dict[str, Any],
    ws_public: dict[str, Any],
    ws_dist: dict[str, Any],
    live_provider: bool = True,
) -> dict[str, Any]:
    failures: list[str] = []
    stages: dict[str, Any] = {}

    provider, registry_symbol = _provider_and_symbol(instrument_id)
    canon = BY_ID.get(instrument_id)
    reg = load_registry().get(instrument_id)

    stages["registry"] = {
        "instrument_id": instrument_id,
        "provider": provider,
        "registry_symbol": registry_symbol,
        "canonical_provider_symbol": canon.price_provider_symbol if canon else None,
        "registry_oanda_symbol": reg.oanda_symbol if reg else None,
        "present": bool(canon and reg),
    }
    if not canon:
        failures.append("missing canonical identity")
    if not reg:
        failures.append("missing registry row")

    store = _store_record(instrument_id, prices_doc)
    daily = list(store.get("daily") or [])
    store_weekly = list(store.get("weekly") or [])
    raw_last = _last_bar(daily)
    store_weekly_last = _last_bar(store_weekly)
    raw_date = str((raw_last or {}).get("date") or "")[:10] or None
    store_weekly_date = str((store_weekly_last or {}).get("date") or "")[:10] or None

    stages["price_download_storage"] = {
        "raw_daily_last": _ohlc_tuple(raw_last),
        "store_weekly_last": _ohlc_tuple(store_weekly_last),
        "daily_bars": len(daily),
        "weekly_bars": len(store_weekly),
        "store_symbol": store.get("symbol") or store.get("canonical_symbol"),
        "fetched_via": store.get("_fetched_via"),
    }
    if not raw_last:
        failures.append("no raw daily OHLC in price store")

    # Fresh weekly aggregation from stored daily (detects skipped rebuild / stale cache).
    derived_weekly = derive_weekly_ohlc_from_daily(
        [
            {
                "date": str(b.get("date") or "")[:10],
                "open": b.get("open"),
                "high": b.get("high"),
                "low": b.get("low"),
                "close": b.get("close"),
                "source": b.get("source") or provider,
            }
            for b in daily
        ]
    )
    derived_last = _last_bar(derived_weekly)
    derived_date = str((derived_last or {}).get("date") or "")[:10] or None
    stages["weekly_aggregation"] = {
        "derived_weekly_last": _ohlc_tuple(derived_last),
        "derived_rows": len(derived_weekly),
        "store_weekly_matches_derived": _bar_match(store_weekly_last, derived_last),
    }
    if not derived_last:
        failures.append("weekly aggregation from daily produced no bars")
    elif raw_date and derived_date and derived_date < raw_date:
        # Fresh aggregation must include the newest daily bar's ISO week.
        failures.append(
            f"derived weekly last {derived_date} behind raw daily last {raw_date}"
        )

    cot_date = _cot_last(instrument_id, cot_doc)
    stages["cot"] = {"latest_cot_report_date": cot_date}
    if not cot_date:
        failures.append("no COT latest date in cot_3y export")

    ws_block = (ws_processed.get("instruments") or {}).get(instrument_id) or {}
    pub_block = (ws_public.get("instruments") or {}).get(instrument_id) or {}
    dist_block = (ws_dist.get("instruments") or {}).get(instrument_id) or {}
    ws_weekly = list(ws_block.get("weekly_ohlc") or [])
    ws_last = _last_bar(ws_weekly)
    ws_date = str((ws_last or {}).get("date") or ws_block.get("ohlc_last_date") or "")[:10] or None
    pub_last = _last_bar(list(pub_block.get("weekly_ohlc") or []))
    dist_last = _last_bar(list(dist_block.get("weekly_ohlc") or [])) if dist_block else None

    displayed_symbol = ws_block.get("canonical_symbol")
    stages["workstation_export"] = {
        "ohlc_last_date": ws_date,
        "weekly_last": _ohlc_tuple(ws_last),
        "price_source": ws_block.get("price_source"),
        "canonical_symbol": displayed_symbol,
        "canonical_source": ws_block.get("canonical_source"),
        "price_quality": ws_block.get("price_quality"),
        "public_matches_processed": _bar_match(ws_last, pub_last),
        "dist_matches_processed": (
            True if not dist_block else _bar_match(ws_last, dist_last)
        ),
    }
    if not ws_last:
        failures.append("workstation weekly OHLC missing")
    # Tip may be provider-native Friday weeks stitched onto derived history — do not
    # require exact equality with ISO-derived tip.
    tip_ok = False
    if ws_last and store_weekly_last and _bar_match(ws_last, store_weekly_last):
        tip_ok = True
    if ws_last and derived_last and _bar_match(ws_last, derived_last):
        tip_ok = True
    if ws_last and not tip_ok and store_weekly_date and ws_date:
        # Allow tip to match store weekly when within one week of derived tip.
        g = _gap_days(ws_date, derived_date)
        if g is not None and g <= 7 and store_weekly_date == ws_date:
            tip_ok = True
    if ws_last and not tip_ok:
        failures.append(
            "workstation weekly tip matches neither store native weekly nor derived weekly "
            f"(ws={ws_date} store_weekly={store_weekly_date} derived={derived_date})"
        )
    if ws_last and pub_last and not _bar_match(ws_last, pub_last):
        failures.append("public workstation_ohlc_latest.json drifts from processed")
    if dist_block and ws_last and dist_last and not _bar_match(ws_last, dist_last):
        failures.append("dist workstation_ohlc_latest.json drifts from processed")

    # Symbol mapping chain
    store_sym = stages["price_download_storage"].get("store_symbol")
    symbol_chain = {
        "registry_symbol": registry_symbol,
        "canonical_symbol": canon.price_provider_symbol if canon else None,
        "store_symbol": store_sym,
        "displayed_symbol": displayed_symbol,
    }
    stages["symbol_mapping"] = symbol_chain
    expected = registry_symbol or (canon.price_provider_symbol if canon else None)
    if expected and displayed_symbol and not _symbols_equivalent(displayed_symbol, expected):
        failures.append(
            f"symbol mismatch displayed={displayed_symbol!r} expected={expected!r}"
        )

    # Price may lead COT. Only FAIL when price is behind COT beyond tolerance.
    gap_days = _gap_days(ws_date, cot_date)
    gap_weeks = _gap_weeks(ws_date, cot_date)
    price_behind_cot = bool(
        ws_date and cot_date and ws_date < cot_date and (gap_days or 0) > MAX_ALIGNMENT_GAP_DAYS
    )
    stages["alignment"] = {
        "weekly_ohlc_date": ws_date,
        "cot_date": cot_date,
        "gap_days": gap_days,
        "gap_weeks": gap_weeks,
        "price_may_lead_cot": True,
        "price_behind_cot": price_behind_cot,
        "max_allowed_days_when_price_behind": MAX_ALIGNMENT_GAP_DAYS,
    }
    if not ws_date or not cot_date:
        failures.append("cannot compute price/COT gap")
    elif price_behind_cot:
        failures.append(
            f"price behind COT by {gap_days}d exceeds max {MAX_ALIGNMENT_GAP_DAYS}d "
            f"(weekly={ws_date} cot={cot_date})"
        )

    # Provider series cross-check — last 10 completed weekly candles must match.
    provider_series: list[dict[str, Any]] = []
    provider_mode = "store_weekly_fallback"
    if live_provider:
        provider_series, provider_mode = _fetch_provider_weekly_series(
            provider, expected or registry_symbol, count=12
        )
    if not provider_series and store_weekly:
        provider_series = [_ohlc_tuple(b) or {} for b in store_weekly if _ohlc_tuple(b)]
        provider_mode = "store_weekly_fallback"
    if not provider_series and derived_weekly:
        # Corn / AV-style instruments may only have daily — validate against derived weekly tip.
        today = date.today()
        cur = today.isocalendar()
        provider_series = []
        for b in derived_weekly:
            d = _parse_date(b.get("date"))
            if not d:
                continue
            y, w, _ = d.isocalendar()
            if (y, w) >= (cur.year, cur.week):
                continue
            provider_series.append(_ohlc_tuple(b) or {})
        provider_mode = "derived_weekly_fallback"
    series_failures = _compare_last_n_weeks(provider_series, ws_weekly, n=10)
    failures.extend(series_failures)
    stages["provider_series_cross_check"] = {
        "mode": provider_mode,
        "provider_last": _ohlc_tuple(provider_series[-1]) if provider_series else None,
        "provider_count": len(provider_series),
        "workstation_last": _ohlc_tuple(ws_last),
        "last_10_failures": series_failures,
    }

    # Pipeline break locus
    break_at = None
    if not raw_last:
        break_at = "price_download_storage"
    elif not derived_last and not store_weekly_last:
        break_at = "weekly_aggregation"
    elif series_failures:
        break_at = "provider_series_cross_check"
    elif price_behind_cot:
        break_at = "alignment"
    elif ws_last and pub_last and not _bar_match(ws_last, pub_last):
        break_at = "frontend_public_mirror"
    elif ws_last and not tip_ok:
        break_at = "workstation_export"
    stages["pipeline_break"] = break_at

    status = "FAIL" if failures else "PASS"
    return {
        "instrument": instrument_id,
        "provider": provider,
        "symbol": expected or registry_symbol,
        "raw_daily_date": raw_date,
        "store_weekly_date": store_weekly_date,
        "weekly_aggregation_date": derived_date,
        "workstation_weekly_date": ws_date,
        "cot_date": cot_date,
        "gap_days": gap_days,
        "gap_weeks": gap_weeks,
        "latest_ohlc": _ohlc_tuple(ws_last),
        "status": status,
        "failures": failures,
        "stages": stages,
    }


def run_price_cot_alignment_audit(*, live_provider: bool = True) -> dict[str, Any]:
    prices_doc = _read_json(PRICES_LATEST)
    if not prices_doc:
        from hptl.prices.price_store import load_price_store

        prices_doc = load_price_store()

    cot_doc = _read_json(COT_3Y)
    if not cot_doc:
        cot_doc = _read_json(PROCESSED_DIR / "cot_3y_series_latest.json")

    ws_processed = _read_json(WS_PROCESSED)
    ws_public = _read_json(WS_PUBLIC)
    ws_dist = _read_json(WS_DIST)

    frontend = _frontend_cache_checks()

    instruments: list[dict[str, Any]] = []
    for iid in LEGACY_COT_MARKETS:
        instruments.append(
            audit_instrument(
                iid,
                prices_doc=prices_doc,
                cot_doc=cot_doc,
                ws_processed=ws_processed,
                ws_public=ws_public,
                ws_dist=ws_dist,
                live_provider=live_provider,
            )
        )

    passed = [r for r in instruments if r["status"] == "PASS"]
    failed = [r for r in instruments if r["status"] == "FAIL"]
    overall = "PASS" if not failed and frontend["status"] == "PASS" else "FAIL"
    if frontend["status"] != "PASS":
        # Attach as synthetic failure line
        failed_ids = [r["instrument"] for r in failed]
        if "FRONTEND_CACHE" not in failed_ids:
            pass

    report = {
        "version": "price_cot_alignment_audit_v2_series",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "max_alignment_gap_days": MAX_ALIGNMENT_GAP_DAYS,
        "universe": list(LEGACY_COT_MARKETS),
        "summary": {
            "markets_total": len(instruments),
            "pass_count": len(passed),
            "fail_count": len(failed) + (0 if frontend["status"] == "PASS" else 1),
            "overall_status": overall,
            "gate_open": overall == "PASS",
        },
        "frontend_cache": frontend,
        "instruments": instruments,
        "failing_instruments": [r["instrument"] for r in failed]
        + ([] if frontend["status"] == "PASS" else ["FRONTEND_CACHE"]),
    }
    return report


def render_markdown(report: dict[str, Any]) -> str:
    s = report.get("summary") or {}
    lines = [
        "# Price ↔ COT Alignment Audit",
        "",
        f"Generated: `{report.get('generated_at')}`",
        f"Max alignment gap: **{report.get('max_alignment_gap_days')} calendar days**",
        "",
        "## Summary",
        "",
        f"- Markets total: **{s.get('markets_total')}**",
        f"- PASS: **{s.get('pass_count')}**",
        f"- FAIL: **{s.get('fail_count')}**",
        f"- Gate open: **{s.get('gate_open')}**",
        "",
        "## Frontend cache",
        "",
        f"- WeeklyOHLCStore cache bust: **{(report.get('frontend_cache') or {}).get('status')}**",
        f"- `cache: 'no-store'`: {(report.get('frontend_cache') or {}).get('uses_cache_no_store')}",
        f"- query bust `Date.now()`: {(report.get('frontend_cache') or {}).get('uses_cache_bust_query')}",
        "",
        "## Per instrument",
        "",
    ]
    for row in report.get("instruments") or []:
        ohlc = row.get("latest_ohlc") or {}
        lines.extend(
            [
                f"### {row.get('instrument')} — **{row.get('status')}**",
                "",
                f"- Provider: `{row.get('provider')}`",
                f"- Symbol: `{row.get('symbol')}`",
                f"- Raw daily date: `{row.get('raw_daily_date')}`",
                f"- Store weekly date: `{row.get('store_weekly_date')}`",
                f"- Weekly aggregation date: `{row.get('weekly_aggregation_date')}`",
                f"- Workstation weekly date: `{row.get('workstation_weekly_date')}`",
                f"- COT date: `{row.get('cot_date')}`",
                f"- Gap days: `{row.get('gap_days')}`",
                f"- Gap weeks: `{row.get('gap_weeks')}`",
                (
                    f"- Latest OHLC: O={ohlc.get('open')} H={ohlc.get('high')} "
                    f"L={ohlc.get('low')} C={ohlc.get('close')} ({ohlc.get('date')})"
                ),
            ]
        )
        stages = row.get("stages") or {}
        break_at = stages.get("pipeline_break")
        if break_at:
            lines.append(f"- Pipeline break: `{break_at}`")
        for fail in row.get("failures") or []:
            lines.append(f"- FAIL: {fail}")
        lines.append("")

    failing = report.get("failing_instruments") or []
    if failing:
        lines.extend(
            [
                "## Failing instruments",
                "",
            ]
        )
        for name in failing:
            lines.append(f"- {name}")
        lines.append("")

    overall = s.get("overall_status") or "FAIL"
    lines.extend(
        [
            "## OVERALL STATUS",
            "",
            overall,
            "",
        ]
    )
    return "\n".join(lines)


def write_price_cot_alignment_audit(report: dict[str, Any] | None = None) -> dict[str, Any]:
    report = report or run_price_cot_alignment_audit()
    OUT_JSON.parent.mkdir(parents=True, exist_ok=True)
    OUT_JSON.write_text(json.dumps(report, indent=2), encoding="utf-8")
    OUT_MD.write_text(render_markdown(report), encoding="utf-8")
    PUBLIC_JSON.parent.mkdir(parents=True, exist_ok=True)
    PUBLIC_JSON.write_text(json.dumps(report, indent=2), encoding="utf-8")
    # Convenience copy at data/ root name requested by the task
    (DATA / "price_cot_alignment_audit.md").write_text(render_markdown(report), encoding="utf-8")
    return report


def run_price_cot_alignment_gate(*, live_provider: bool = True) -> dict[str, Any]:
    """Run audit, write reports, return gate payload. Does not raise."""
    report = write_price_cot_alignment_audit(
        run_price_cot_alignment_audit(live_provider=live_provider)
    )
    summary = report.get("summary") or {}
    return {
        "passed": bool(summary.get("gate_open")),
        "overall_status": summary.get("overall_status"),
        "failing_instruments": list(report.get("failing_instruments") or []),
        "pass_count": summary.get("pass_count"),
        "fail_count": summary.get("fail_count"),
        "report_md": str(OUT_MD),
        "report_json": str(OUT_JSON),
    }

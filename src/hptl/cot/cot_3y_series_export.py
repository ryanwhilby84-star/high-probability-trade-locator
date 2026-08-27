"""Full-history COT-vs-price series export (COT workstation data layer).

Assembles, per tracked instrument, every available weekly series of:
  - institutional net  (non-commercial, ``nc_net`` — same series the dashboard scores)
  - retail net         (non-reportable, ``nrept_net``)
  - commercial net     (``comm_net``)
  - open interest
  - positioning_state  (reused from build_decision_table, not recomputed differently)
  - matching weekly/daily price close aligned to each COT report date

Price alignment rules (visual layer only — does not change COT or scoring):
  1. Prefer daily closes from ``prices_latest.json``; merge weekly bars; resample weekly from daily when needed.
  2. When store history starts after the COT window, extend with FRED index series (NASDAQCOM, SP500, DJIA).
  3. Exact match when a bar date equals the COT report date.
  4. Otherwise use the **nearest prior close** (latest bar with date <= COT date).
  5. Emit ``price_match`` = ``exact`` | ``prior_close`` | ``null`` plus ``price_date``.

This reads only already-computed artifacts (``cot_tracked_master_normalized.csv``
and ``prices_latest.json``). It does NOT change scoring logic or COT calculations.
"""

from __future__ import annotations

import json
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from hptl.config import PROCESSED_DIR, PROJECT_ROOT
from hptl.prices.canonical_timeline import (
    COT_MATCH_METHOD,
    FRED_PRICE_FALLBACK as _FRED_PRICE_FALLBACK,
    FRED_OBS_START as _FRED_OBS_START,
    OANDA_PRICE_FALLBACK as _OANDA_PRICE_FALLBACK,
    PRICE_ALIASES as _PRICE_ALIASES,
    build_canonical_timeline,
    match_close_as_of as _match_price_as_of,
    merge_price_series as _merge_price_series,
    resample_weekly_closes as _resample_weekly_closes,
)

# Reuse the exact positioning-state classifier the pipeline uses (no duplication).
from hptl.confluence.build_decision_table import _compute_positioning_state

TARGET_HISTORY_WEEKS = 520
MIN_HISTORY_WEEKS = 260

MASTER_PATH = PROCESSED_DIR / "cot_tracked_master_normalized.csv"
PRICES_PATH = PROJECT_ROOT / "web-dashboard" / "public" / "data" / "prices_latest.json"
CANONICAL_PATH = PROCESSED_DIR / "cot_3y_series_latest.json"
PUBLIC_PATH = PROJECT_ROOT / "web-dashboard" / "public" / "data" / "cot_3y_series_latest.json"


def _num(v: Any) -> float | None:
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    return f if f == f else None  # drop NaN


def _resample_weekly_closes(daily_bars: list[dict[str, Any]]) -> list[tuple[str, float]]:
    """Last close per ISO week from daily OHLC bars."""
    buckets: dict[str, tuple[str, float]] = {}
    for b in daily_bars:
        d = str(b.get("date") or "")[:10]
        c = _num(b.get("close"))
        if not d or c is None:
            continue
        try:
            wk = pd.Timestamp(d).strftime("%G-W%V")
        except (TypeError, ValueError):
            wk = d[:7]
        prev = buckets.get(wk)
        if prev is None or d >= prev[0]:
            buckets[wk] = (d, c)
    return sorted(buckets.values(), key=lambda t: t[0])


def _bars_from_block(blk: dict[str, Any] | None) -> list[tuple[str, float]]:
    """Chronological (date, close) bars — daily preferred, weekly fill + resample."""
    if not blk:
        return []
    daily = blk.get("daily") or []
    weekly = blk.get("weekly") or []
    out: list[tuple[str, float]] = []
    seen: set[str] = set()

    for b in daily:
        d = str(b.get("date") or "")[:10]
        c = _num(b.get("close"))
        if d and c is not None and d not in seen:
            seen.add(d)
            out.append((d, c))

    if weekly:
        for b in weekly:
            d = str(b.get("date") or "")[:10]
            c = _num(b.get("close"))
            if d and c is not None and d not in seen:
                seen.add(d)
                out.append((d, c))
    elif daily:
        for d, c in _resample_weekly_closes(daily):
            if d not in seen:
                seen.add(d)
                out.append((d, c))

    out.sort(key=lambda t: t[0])
    return out


def _load_fred_price_bars(series_id: str, *, observation_start: str = _FRED_OBS_START) -> list[tuple[str, float]]:
    """Daily FRED index level as (date, close) — cache-first via macro fred_client."""
    try:
        from hptl.macro import fred_client

        df = fred_client.get_series_df(series_id, observation_start)
    except Exception:
        return []
    if df is None or df.empty:
        return []
    out: list[tuple[str, float]] = []
    for _, row in df.iterrows():
        d = pd.Timestamp(row["date"]).strftime("%Y-%m-%d")
        c = _num(row["value"])
        if c is not None:
            out.append((d, c))
    return out


def _load_oanda_price_bars(
    oanda_symbol: str,
    *,
    observation_start: str = _FRED_OBS_START,
) -> list[tuple[str, float]]:
    """Paginated OANDA daily closes — used when the price store is shorter than COT history."""
    try:
        from hptl.prices.fx_daily_backfill import fetch_chunked_daily

        start = pd.Timestamp(str(observation_start)[:10]).date()
        end = date.today()
        bars, _warnings = fetch_chunked_daily(oanda_symbol, start=start, end=end, chunk_size=500)
    except Exception:
        return []
    out: list[tuple[str, float]] = []
    for b in bars:
        d = str(b.get("date") or "")[:10]
        c = _num(b.get("close"))
        if d and c is not None:
            out.append((d, c))
    return out


def _observation_start(cot_earliest: str | None) -> str:
    if cot_earliest:
        return str(cot_earliest)[:10]
    return _FRED_OBS_START


def _apply_price_supplements(
    bars: list[tuple[str, float]],
    meta: dict[str, Any],
    *,
    market: str,
    cot_earliest: str | None,
    need_fallback: bool,
) -> list[tuple[str, float]]:
    """Extend short store history with FRED and/or OANDA supplements."""
    if not need_fallback:
        return bars
    obs = _observation_start(cot_earliest)

    fred_id = _FRED_PRICE_FALLBACK.get(market)
    if fred_id:
        fred_bars = _load_fred_price_bars(fred_id, observation_start=obs)
        if fred_bars:
            meta["fred_series"] = fred_id
            meta["fred_bar_count"] = len(fred_bars)
            bars = _merge_price_series(bars, fred_bars)

    # Re-check after FRED — still short?
    earliest_bar = bars[0][0] if bars else None
    cot_ts = pd.Timestamp(str(cot_earliest)[:10]) if cot_earliest else None
    still_short = cot_ts is not None and (
        not bars or pd.Timestamp(earliest_bar) > cot_ts + pd.Timedelta(days=7)
    )

    oanda_sym = _OANDA_PRICE_FALLBACK.get(market)
    if still_short and oanda_sym:
        oanda_bars = _load_oanda_price_bars(oanda_sym, observation_start=obs)
        if oanda_bars:
            meta["oanda_symbol"] = oanda_sym
            meta["oanda_bar_count"] = len(oanda_bars)
            bars = _merge_price_series(bars, oanda_bars)

    return bars


def _load_price_index() -> tuple[dict[str, list[tuple[str, float]]], str]:
    """market id -> chronological list of (date, close)."""
    if not PRICES_PATH.exists():
        return {}, str(PRICES_PATH)
    try:
        doc = json.loads(PRICES_PATH.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {}, str(PRICES_PATH)
    out: dict[str, list[tuple[str, float]]] = {}
    for market, blk in (doc.get("instruments") or {}).items():
        bars = _bars_from_block(blk)
        if bars:
            out[market] = bars
    return out, str(PRICES_PATH)


def _resolve_price_series(
    market: str,
    price_index: dict[str, list[tuple[str, float]]],
) -> tuple[list[tuple[str, float]] | None, str | None]:
    """Return bars and the price-store key used."""
    if market in price_index:
        return price_index[market], market
    for alias in _PRICE_ALIASES.get(market, []):
        if alias in price_index:
            return price_index[alias], alias
    base = str(market).split("/")[0].strip().lower()
    for key, bars in price_index.items():
        if str(key).split("/")[0].strip().lower() == base:
            return bars, key
    return None, None


def _build_price_series_for_market(
    market: str,
    price_index: dict[str, list[tuple[str, float]]],
    cot_earliest: str | None,
) -> tuple[list[tuple[str, float]], str | None, dict[str, Any]]:
    """Canonical daily timeline with optional supplements for COT window."""
    tl = build_canonical_timeline(market, window_start=cot_earliest)
    if not tl:
        return [], None, {"store_key": None, "store_bar_count": 0}

    meta: dict[str, Any] = {
        "store_key": tl.resolved_store_key,
        "store_bar_count": tl.bar_count,
        "canonical_source": tl.canonical_source,
        "canonical_symbol": tl.canonical_symbol,
        "proxy": tl.proxy,
        "proxy_explanation": tl.proxy_explanation,
        "cot_match_method": COT_MATCH_METHOD,
        "fred_series": tl.supplement_meta.get("fred_series"),
        "fred_bar_count": tl.supplement_meta.get("fred_bar_count", 0),
        "oanda_symbol": tl.supplement_meta.get("oanda_symbol"),
        "oanda_bar_count": tl.supplement_meta.get("oanda_bar_count", 0),
        "resampled_weekly_from_daily": False,
    }

    price_key = tl.resolved_store_key
    if not price_key and meta.get("oanda_bar_count"):
        price_key = f"oanda:{meta.get('oanda_symbol')}"
    elif not price_key and meta.get("fred_series") and meta.get("fred_bar_count"):
        price_key = f"fred:{meta.get('fred_series')}"

    return tl.daily_closes(), price_key, meta


def _series_for_market(g: pd.DataFrame, price_index: dict[str, list[tuple[str, float]]]) -> dict[str, Any]:
    g = g.sort_values("cot_report_date").copy()
    market = str(g["market"].iloc[-1])
    cot_earliest = str(g["cot_report_date"].iloc[0])[:10] if len(g) else None
    price_series, price_key, price_build_meta = _build_price_series_for_market(
        market, price_index, cot_earliest
    )
    latest_source_price_date = price_series[-1][0] if price_series else None
    earliest_source_price_date = price_series[0][0] if price_series else None

    inst_long_col = "nc_long" if "nc_long" in g.columns else "long_value"
    inst_short_col = "nc_short" if "nc_short" in g.columns else "short_value"
    if inst_long_col in g.columns:
        g["_four_week_long"] = pd.to_numeric(g[inst_long_col], errors="coerce").diff(4)
    if inst_short_col in g.columns:
        g["_four_week_short"] = pd.to_numeric(g[inst_short_col], errors="coerce").diff(4)
    if "nrept_net" in g.columns:
        g["_four_week_retail_net"] = pd.to_numeric(g["nrept_net"], errors="coerce").diff(4)

    points: list[dict[str, Any]] = []
    has_price = False
    has_retail = False
    has_commercial = False
    exact_weeks = 0
    prior_weeks = 0
    missing_weeks = 0

    for _, row in g.iterrows():
        date = str(row.get("cot_report_date"))[:10]
        inst_net = _num(row.get("nc_net"))
        if inst_net is None:
            inst_net = _num(row.get("net_value"))
        inst_long = _num(row.get("nc_long"))
        if inst_long is None:
            inst_long = _num(row.get("long_value"))
        inst_short = _num(row.get("nc_short"))
        if inst_short is None:
            inst_short = _num(row.get("short_value"))
        retail_net = _num(row.get("nrept_net"))
        retail_long = _num(row.get("nrept_long"))
        retail_short = _num(row.get("nrept_short"))
        comm_net = _num(row.get("comm_net"))
        long_wow = _num(row.get("nc_long_week_change"))
        if long_wow is None:
            long_wow = _num(row.get("long_weekly_change"))
        short_wow = _num(row.get("nc_short_week_change"))
        if short_wow is None:
            short_wow = _num(row.get("short_weekly_change"))
        if retail_net is not None:
            has_retail = True
        if comm_net is not None:
            has_commercial = True

        pos_state = _compute_positioning_state(
            _num(row.get("net_value")),
            _num(row.get("weekly_change")),
            _num(row.get("four_week_change")),
            _num(row.get("long_weekly_change")),
            _num(row.get("short_weekly_change")),
        )

        price, price_date, price_match, price_lag_days = _match_price_as_of(price_series, date)
        if price is not None:
            has_price = True
            if price_match == "exact":
                exact_weeks += 1
            elif price_match == "prior_close":
                prior_weeks += 1
        else:
            missing_weeks += 1

        points.append(
            {
                "date": date,
                "institutional_net": inst_net,
                "institutional_long": inst_long,
                "institutional_short": inst_short,
                "retail_net": retail_net,
                "retail_long": retail_long,
                "retail_short": retail_short,
                "commercial_net": comm_net,
                "open_interest": _num(row.get("open_interest")),
                "price": price,
                "price_date": price_date,
                "price_match": price_match,
                "price_lag_days": price_lag_days,
                "positioning_state": pos_state,
                "cot_bias": str(row.get("cot_bias") or "") or None,
                "market_state": str(row.get("market_state") or "") or None,
                "cot_score": _num(row.get("cot_score")),
                "one_week_net_change": _num(row.get("weekly_change")),
                "one_week_long_change": long_wow,
                "one_week_short_change": short_wow,
                "four_week_net_change": _num(row.get("four_week_change")),
                "four_week_long_change": _num(row.get("_four_week_long")),
                "four_week_short_change": _num(row.get("_four_week_short")),
                "four_week_retail_net_change": _num(row.get("_four_week_retail_net")),
            }
        )

    latest_cot = points[-1]["date"] if points else None
    last_pt = points[-1] if points else {}
    weeks_after_source = 0
    if latest_source_price_date and latest_cot:
        cot_ts = pd.Timestamp(latest_cot)
        src_ts = pd.Timestamp(latest_source_price_date)
        if cot_ts > src_ts:
            weeks_after_source = sum(
                1 for p in points if pd.Timestamp(p["date"]) > src_ts
            )

    price_note = None
    missing_before = None
    first_price_cot_date = next((p["date"] for p in points if p.get("price") is not None), None)
    if points and first_price_cot_date and first_price_cot_date != points[0]["date"]:
        missing_before = first_price_cot_date

    if not price_series:
        price_note = "Price unavailable — no bars in prices_latest.json for this instrument."
    elif missing_weeks > 0 and missing_before:
        price_note = f"Price history missing before {missing_before} ({missing_weeks} COT weeks without price)."
    elif price_build_meta.get("oanda_symbol"):
        price_note = (
            f"Extended with OANDA {price_build_meta['oanda_symbol']} "
            f"({price_build_meta.get('oanda_bar_count', 0)} bars) before store history "
            f"({earliest_source_price_date if price_build_meta.get('store_bar_count') else 'n/a'})."
        )
    elif price_build_meta.get("fred_series"):
        price_note = (
            f"Extended with FRED {price_build_meta['fred_series']} "
            f"({price_build_meta.get('fred_bar_count', 0)} bars) before store history "
            f"({earliest_source_price_date if price_build_meta.get('store_bar_count') else 'n/a'})."
        )
    elif weeks_after_source > 0:
        price_note = (
            f"Latest price bar is {latest_source_price_date}; "
            f"{weeks_after_source} COT week(s) through {latest_cot} use prior close "
            f"({last_pt.get('price_date')}, lag {last_pt.get('price_lag_days')}d)."
        )
    elif last_pt.get("price_match") == "prior_close":
        price_note = (
            f"Price matched as prior close ({last_pt.get('price_date')}, "
            f"lag {last_pt.get('price_lag_days')}d vs COT {latest_cot})."
        )

    return {
        "market": market,
        "weeks": len(points),
        "earliest_date": points[0]["date"] if points else None,
        "latest_date": points[-1]["date"] if points else None,
        "institutional_group": "Non-Commercial",
        "retail_group": "Non-Reportable",
        "has_price": has_price,
        "has_retail": has_retail,
        "has_commercial": has_commercial,
        "price_weeks": sum(1 for p in points if p["price"] is not None),
        "price_audit": {
            "source_file": str(PRICES_PATH),
            "price_store_key": price_key,
            "canonical_source": price_build_meta.get("canonical_source"),
            "canonical_symbol": price_build_meta.get("canonical_symbol"),
            "proxy": price_build_meta.get("proxy"),
            "proxy_explanation": price_build_meta.get("proxy_explanation"),
            "cot_match_method": price_build_meta.get("cot_match_method"),
            "price_bar_count": len(price_series) if price_series else 0,
            "store_bar_count": price_build_meta.get("store_bar_count"),
            "fred_fallback_series": price_build_meta.get("fred_series"),
            "fred_fallback_bar_count": price_build_meta.get("fred_bar_count"),
            "oanda_fallback_symbol": price_build_meta.get("oanda_symbol"),
            "oanda_fallback_bar_count": price_build_meta.get("oanda_bar_count"),
            "resampled_weekly_from_daily": price_build_meta.get("resampled_weekly_from_daily"),
            "earliest_price_bar_date": earliest_source_price_date,
            "earliest_cot_date": cot_earliest,
            "first_matched_cot_date": first_price_cot_date,
            "missing_before": missing_before,
            "latest_cot_date": latest_cot,
            "latest_price_bar_date": latest_source_price_date,
            "latest_matched_price_date": last_pt.get("price_date"),
            "latest_price_match": last_pt.get("price_match"),
            "latest_price_lag_days": last_pt.get("price_lag_days"),
            "exact_match_weeks": exact_weeks,
            "prior_close_weeks": prior_weeks,
            "missing_price_weeks": missing_weeks,
            "cot_weeks_after_latest_price_bar": weeks_after_source,
            "note": price_note,
        },
        "series": points,
    }


def build_payload() -> dict[str, Any]:
    if not MASTER_PATH.exists():
        raise FileNotFoundError(f"Missing {MASTER_PATH}; run the COT tracked backfill first.")
    df = pd.read_csv(MASTER_PATH)
    df = df[df["market"].notna() & df["cot_report_date"].notna()]
    price_index, price_source = _load_price_index()

    markets: dict[str, Any] = {}
    for market, g in df.groupby("market", sort=False):
        block = _series_for_market(g, price_index)
        if block["weeks"]:
            markets[str(market)] = block

    return {
        "schema_version": 2,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "target_history_weeks": TARGET_HISTORY_WEEKS,
        "min_history_weeks": MIN_HISTORY_WEEKS,
        "window_weeks": TARGET_HISTORY_WEEKS,
        "source": "cot_tracked_master_normalized.csv + hptl.prices.canonical_timeline (canonical daily, COT as-of match)",
        "price_source_file": price_source,
        "notes": "Visual audit layer. Institutional=Non-Commercial net, Retail=Non-Reportable net. "
        "Price uses exact same-date close when available, otherwise nearest prior close (see price_match).",
        "markets": markets,
    }


def write_exports(payload: dict[str, Any]) -> Path:
    text = json.dumps(payload, indent=2, ensure_ascii=False)
    CANONICAL_PATH.parent.mkdir(parents=True, exist_ok=True)
    CANONICAL_PATH.write_text(text, encoding="utf-8")
    PUBLIC_PATH.parent.mkdir(parents=True, exist_ok=True)
    PUBLIC_PATH.write_text(text, encoding="utf-8")
    return CANONICAL_PATH


def audit_market(payload: dict[str, Any], market: str) -> None:
    """Print price-alignment audit for one instrument."""
    blk = (payload.get("markets") or {}).get(market)
    if not blk:
        print(f"No 3Y block for {market!r}")
        return
    audit = blk.get("price_audit") or {}
    print(f"\n=== 3Y price audit: {market} ===")
    print(f"  source file:           {audit.get('source_file')}")
    print(f"  price store key:       {audit.get('price_store_key')}")
    print(f"  price bar count:       {audit.get('price_bar_count')}")
    print(f"  latest COT date:       {audit.get('latest_cot_date')}")
    print(f"  latest price bar date: {audit.get('latest_price_bar_date')}")
    print(f"  latest matched price:  {audit.get('latest_matched_price_date')} ({audit.get('latest_price_match')}, lag {audit.get('latest_price_lag_days')}d)")
    print(f"  price rows in series:  {blk.get('price_weeks')} / {blk.get('weeks')}")
    print(f"  exact / prior / miss:  {audit.get('exact_match_weeks')} / {audit.get('prior_close_weeks')} / {audit.get('missing_price_weeks')}")
    print(f"  COT weeks after bar:   {audit.get('cot_weeks_after_latest_price_bar')}")
    print(f"  missing before:        {audit.get('missing_before')}")
    print(f"  FRED fallback:         {audit.get('fred_fallback_series')} ({audit.get('fred_fallback_bar_count')} bars)")
    if audit.get("note"):
        print(f"  note: {audit['note']}")
    missing = [p["date"] for p in blk.get("series") or [] if p.get("price") is None]
    if missing:
        print(f"  missing price weeks ({len(missing)}): {missing[:5]}{'...' if len(missing) > 5 else ''}")


def run(*, audit: str | None = None) -> Path:
    payload = build_payload()
    path = write_exports(payload)
    n = len(payload["markets"])
    with_price = sum(1 for m in payload["markets"].values() if m["has_price"])
    with_retail = sum(1 for m in payload["markets"].values() if m["has_retail"])
    print(f"Wrote {path} ({n} markets; {with_price} with price, {with_retail} with retail history).")
    if audit:
        audit_market(payload, audit)
    return path


if __name__ == "__main__":
    import sys

    m = sys.argv[1] if len(sys.argv) > 1 else None
    run(audit=m)

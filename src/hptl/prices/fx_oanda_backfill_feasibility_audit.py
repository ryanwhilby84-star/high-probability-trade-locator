"""Historical daily price backfill feasibility audit — OANDA depth vs local store.

Usage:
    python -m hptl.prices.fx_oanda_backfill_feasibility_audit

Writes:
    data/audits/fx_oanda_backfill_feasibility.json
    data/audits/fx_oanda_backfill_feasibility.md

Audit-only. Does not modify live seasonality, valuation, COT, or production price data.
"""

from __future__ import annotations

import json
import sys
from datetime import date, datetime, timedelta, timezone
from typing import Any

from hptl.config import DATA_DIR, get_oanda_api_host, get_oanda_api_key
from hptl.oanda.oanda_client import OandaApiError, api_get
from hptl.oanda.oanda_prices import _parse_candles
from hptl.prices.data_integrity import actual_fetch_meta
from hptl.prices.models import DAILY_BAR_TARGET, WEEKLY_BAR_TARGET, WEEKLY_LOOKBACK_DAYS
from hptl.prices.price_store import load_price_store
from hptl.seasonality.seasonality_v2 import normalize_daily_bars, years_spanned

AUDIT_JSON = DATA_DIR / "audits" / "fx_oanda_backfill_feasibility.json"
AUDIT_MD = DATA_DIR / "audits" / "fx_oanda_backfill_feasibility.md"

OANDA_MAX_COUNT = 5000
RECOMMENDED_CHUNK_DAYS = 500
MIN_YEARS_10Y = 10.0
MIN_BARS_10Y = 252 * 8

# (display symbol, OANDA instrument, HTPL price-store key)
TEST_PAIRS: tuple[tuple[str, str, str], ...] = (
    ("EURUSD", "EUR_USD", "Euro FX / 6E"),
    ("GBPUSD", "GBP_USD", "British Pound / 6B"),
    ("AUDUSD", "AUD_USD", "Australian Dollar / 6A"),
    ("NZDUSD", "NZD_USD", "NZ Dollar / 6N"),
    ("USDJPY", "USD_JPY", "Japanese Yen / 6J"),
    ("USDCAD", "USD_CAD", "Canadian Dollar / 6C"),
    ("USDCHF", "USD_CHF", "Swiss Franc / 6S"),
    ("EURJPY", "EUR_JPY", "EUR/JPY"),
)


def _loader_analysis() -> dict[str, Any]:
    return {
        "module": "src/hptl/oanda/oanda_prices.py",
        "refresh_entry": "src/hptl/prices/run_price_refresh.py",
        "adapter": "src/hptl/prices/unified_adapter.py",
        "daily_bar_target": DAILY_BAR_TARGET,
        "weekly_bar_target": WEEKLY_BAR_TARGET,
        "weekly_lookback_days": WEEKLY_LOOKBACK_DAYS,
        "fetch_parameters": ["granularity", "count", "price=M"],
        "uses_from_to_pagination": False,
        "uses_chunking": False,
        "storage": "data/processed/prices/{instrument}.json via write_instrument_record",
        "merge_strategy": "full replace on refresh (no historical merge)",
        "verdict": "local_loader_limit",
        "explanation": (
            f"OANDA fetch uses count={DAILY_BAR_TARGET} only (~1 trading year). "
            "No from/to date-range pagination is implemented in the live loader."
        ),
    }


def _stored_daily_stats(store_key: str, instruments: dict[str, Any]) -> dict[str, Any]:
    rec = instruments.get(store_key) or {}
    daily = normalize_daily_bars(rec.get("daily") or [])
    src, sym, _dn, _wn, err = actual_fetch_meta(store_key, public=instruments)
    return {
        "store_key": store_key,
        "earliest_stored_daily_bar": daily[0]["date"] if daily else None,
        "latest_stored_daily_bar": daily[-1]["date"] if daily else None,
        "stored_daily_bar_count": len(daily),
        "stored_years_of_coverage": round(years_spanned(daily), 2) if daily else 0.0,
        "data_source": src,
        "fetch_symbol": sym,
        "store_error": err,
    }


def _probe_candles(
    instrument: str,
    *,
    count: int | None = None,
    from_time: str | None = None,
    to_time: str | None = None,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    params: dict[str, str] = {"granularity": "D", "price": "M"}
    if count is not None:
        params["count"] = str(min(count, OANDA_MAX_COUNT))
    if from_time:
        params["from"] = from_time
    if to_time:
        params["to"] = to_time

    meta: dict[str, Any] = {
        "params": {k: v for k, v in params.items()},
        "http_status": None,
        "error": None,
        "candles_returned": 0,
    }
    try:
        doc = api_get(f"/v3/instruments/{instrument}/candles", params=params)
        bars = _parse_candles(doc)
        meta["candles_returned"] = len(bars)
        meta["http_status"] = 200
        return bars, meta
    except OandaApiError as exc:
        meta["error"] = str(exc)[:300]
        meta["http_status"] = exc.status_code
        return [], meta


def _iso_from(d: date) -> str:
    return datetime(d.year, d.month, d.day, tzinfo=timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000000000Z")


def _audit_pair_live(
    display: str,
    oanda_symbol: str,
    store_key: str,
    *,
    instruments: dict[str, Any],
    years: int = 10,
) -> dict[str, Any]:
    stored = _stored_daily_stats(store_key, instruments)
    stored_earliest = stored["earliest_stored_daily_bar"]
    today = date.today()
    ten_year_start = today - timedelta(days=years * 366)

    probes: list[dict[str, Any]] = []

    # Probe A: live loader equivalent (count=260)
    bars_a, meta_a = _probe_candles(oanda_symbol, count=DAILY_BAR_TARGET)
    probes.append({"name": "live_loader_equivalent", **meta_a, "earliest": bars_a[0]["date"] if bars_a else None, "latest": bars_a[-1]["date"] if bars_a else None})

    # Probe B: max single request (count=5000 backward from now)
    bars_b, meta_b = _probe_candles(oanda_symbol, count=OANDA_MAX_COUNT)
    probes.append({"name": "max_count_5000", **meta_b, "earliest": bars_b[0]["date"] if bars_b else None, "latest": bars_b[-1]["date"] if bars_b else None})

    # Probe C: 10-year forward window from start date
    bars_c, meta_c = _probe_candles(
        oanda_symbol,
        from_time=_iso_from(ten_year_start),
        count=OANDA_MAX_COUNT,
    )
    probes.append({"name": "from_10y_start", **meta_c, "earliest": bars_c[0]["date"] if bars_c else None, "latest": bars_c[-1]["date"] if bars_c else None})

    # Probe D: can we fetch strictly before stored earliest?
    bars_d: list[dict[str, Any]] = []
    meta_d: dict[str, Any] = {"name": "before_stored_earliest", "params": {}, "error": None}
    can_fetch_older = False
    if stored_earliest:
        try:
            earliest_dt = datetime.strptime(stored_earliest[:10], "%Y-%m-%d").date()
            to_before = earliest_dt - timedelta(days=1)
            bars_d, meta_d = _probe_candles(oanda_symbol, to_time=_iso_from(to_before), count=500)
            meta_d["name"] = "before_stored_earliest"
            if bars_d:
                oldest_d = bars_d[0]["date"]
                can_fetch_older = oldest_d < stored_earliest[:10]
                meta_d["earliest"] = oldest_d
                meta_d["latest"] = bars_d[-1]["date"]
        except ValueError:
            meta_d["error"] = "invalid stored earliest date"
    else:
        meta_d["error"] = "no stored earliest bar to compare"
        can_fetch_older = bool(bars_b or bars_c)

    probes.append(meta_d)

    # Best depth from successful probes
    all_probe_bars = bars_b or bars_c or bars_a
    earliest_fetchable = all_probe_bars[0]["date"] if all_probe_bars else None
    latest_fetchable = all_probe_bars[-1]["date"] if all_probe_bars else None
    bars_in_10y = len(bars_c) if bars_c else len(bars_b)

    fetch_years = round(years_spanned(all_probe_bars), 2) if all_probe_bars else 0.0
    feasible_10y = fetch_years >= MIN_YEARS_10Y and bars_in_10y >= MIN_BARS_10Y

    api_limits: list[str] = []
    for p in probes:
        if p.get("error"):
            api_limits.append(f"{p.get('name')}: {p['error']}")
    if bars_b and len(bars_b) >= OANDA_MAX_COUNT - 1:
        api_limits.append("max_count_5000 may be truncated — paginate with from/to")

    recommended_chunks = 1 if bars_in_10y <= RECOMMENDED_CHUNK_DAYS * 6 else max(1, (bars_in_10y // RECOMMENDED_CHUNK_DAYS) + 1)

    return {
        "display_symbol": display,
        "oanda_symbol": oanda_symbol,
        "store_key": store_key,
        **stored,
        "can_oanda_fetch_older_than_stored": can_fetch_older,
        "earliest_oanda_fetchable_daily_bar": earliest_fetchable,
        "latest_oanda_fetchable_daily_bar": latest_fetchable,
        "bars_fetchable_10y_range": bars_in_10y,
        "oanda_fetch_years_of_coverage": fetch_years,
        "ten_year_backfill_feasible": feasible_10y,
        "api_limits_encountered": api_limits,
        "recommended_chunk_size_bars": RECOMMENDED_CHUNK_DAYS,
        "recommended_chunk_count_for_10y": recommended_chunks,
        "probes": probes,
    }


def _audit_pair_offline(
    display: str,
    oanda_symbol: str,
    store_key: str,
    *,
    instruments: dict[str, Any],
) -> dict[str, Any]:
    stored = _stored_daily_stats(store_key, instruments)
    return {
        "display_symbol": display,
        "oanda_symbol": oanda_symbol,
        "store_key": store_key,
        **stored,
        "can_oanda_fetch_older_than_stored": None,
        "earliest_oanda_fetchable_daily_bar": None,
        "latest_oanda_fetchable_daily_bar": None,
        "bars_fetchable_10y_range": None,
        "oanda_fetch_years_of_coverage": None,
        "ten_year_backfill_feasible": None,
        "api_limits_encountered": ["OANDA_API_KEY not set — live probes skipped"],
        "recommended_chunk_size_bars": RECOMMENDED_CHUNK_DAYS,
        "recommended_chunk_count_for_10y": None,
        "probes": [],
    }


def _backfill_design() -> dict[str, Any]:
    return {
        "proposed_command": "python -m hptl.prices.backfill_fx_daily --source oanda --years 10",
        "dry_run_command": "python -m hptl.prices.backfill_fx_daily --source oanda --years 10 --dry-run",
        "post_backfill_audit": "python -m hptl.seasonality.fx_seasonality_coverage_audit",
        "rules": [
            "Fetch in chunks (default 500 daily bars) using OANDA from/to pagination",
            "Deduplicate by date per instrument",
            "Merge: keep newest bar when duplicate dates; never replace newer stored close with older fetch",
            "Log bar counts and date ranges only — never API keys",
            "Resume via checkpoint file per instrument",
            "Skip failed pairs and continue",
            "Default --dry-run: probe and print merge plan without writing",
            "Write to data/processed/prices/backfill/ staging until explicitly promoted",
        ],
        "oanda_api_notes": {
            "max_count_per_request": OANDA_MAX_COUNT,
            "recommended_chunk_bars": RECOMMENDED_CHUNK_DAYS,
            "supports_from_to": True,
            "live_loader_uses_count_only": True,
            "live_loader_count": DAILY_BAR_TARGET,
        },
    }


def build_audit(*, years: int = 10) -> dict[str, Any]:
    instruments = load_price_store().get("instruments") or {}
    loader = _loader_analysis()
    key_configured = bool(get_oanda_api_key())

    pairs: list[dict[str, Any]] = []
    for display, oanda_sym, store_key in TEST_PAIRS:
        if key_configured:
            pairs.append(_audit_pair_live(display, oanda_sym, store_key, instruments=instruments, years=years))
        else:
            pairs.append(_audit_pair_offline(display, oanda_sym, store_key, instruments=instruments))

    backfillable = [p["display_symbol"] for p in pairs if p.get("ten_year_backfill_feasible") is True]
    older_fetchable = [p["display_symbol"] for p in pairs if p.get("can_oanda_fetch_older_than_stored") is True]

    oanda_sufficient = len(backfillable) >= len(TEST_PAIRS) // 2 if key_configured else None
    root_cause = "local_loader_limit"
    if key_configured and backfillable:
        root_cause = "local_loader_limit (OANDA API can supply deeper history when using from/to + count up to 5000)"
    elif not key_configured:
        root_cause = "local_loader_limit (OANDA live probes not run — key missing)"

    needs_paid = False
    if key_configured and not backfillable and pairs:
        needs_paid = True

    return {
        "schema_version": 1,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "parser": "hptl.prices.fx_oanda_backfill_feasibility_audit",
        "audit_only": True,
        "oanda_api_key_configured": key_configured,
        "oanda_api_host": get_oanda_api_host() if key_configured else None,
        "loader_analysis": loader,
        "backfill_design": _backfill_design(),
        "summary": {
            "pairs_tested": len(pairs),
            "oanda_sufficient_for_10y_fx_seasonality": oanda_sufficient,
            "root_cause": root_cause,
            "backfillable_pairs": backfillable,
            "can_fetch_older_than_store": older_fetchable,
            "needs_paid_provider": needs_paid,
            "recommended_next_command": (
                "python -m hptl.prices.backfill_fx_daily --source oanda --years 10 --dry-run"
                if key_configured
                else "Set OANDA_API_KEY then re-run this audit"
            ),
        },
        "pairs": pairs,
    }


def _render_markdown(report: dict[str, Any]) -> str:
    la = report.get("loader_analysis") or {}
    s = report.get("summary") or {}
    bd = report.get("backfill_design") or {}
    lines = [
        "# FX OANDA Historical Daily Backfill Feasibility",
        "",
        f"Generated: {report.get('generated_at')}",
        "",
        "**Audit-only** — production price store not modified.",
        "",
        "## Loader analysis",
        "",
        f"- Module: `{la.get('module')}`",
        f"- Daily bar target: **{la.get('daily_bar_target')}** (≈1 trading year)",
        f"- Uses from/to pagination: **{la.get('uses_from_to_pagination')}**",
        f"- Verdict: **{la.get('verdict')}**",
        "",
        la.get("explanation", ""),
        "",
        "## Executive summary",
        "",
        f"1. **Is OANDA enough for 10-year FX seasonality?** "
        f"{'Likely yes (with loader fix)' if s.get('oanda_sufficient_for_10y_fx_seasonality') else 'Unconfirmed or no — see pair table'}",
        f"2. **Root cause:** {s.get('root_cause')}",
        f"3. **Backfillable pairs:** {', '.join(s.get('backfillable_pairs') or []) or 'none probed'}",
        f"4. **Next command:** `{s.get('recommended_next_command')}`",
        f"5. **Paid provider needed?** {'Yes' if s.get('needs_paid_provider') else 'No (if OANDA probes pass)'}",
        "",
        "## Pair probes",
        "",
        "| Pair | OANDA | Stored earliest | Stored bars | Older than store? | OANDA earliest | 10Y bars | 10Y feasible |",
        "|---|---|---:|---:|:---:|---|---:|:---:|",
    ]
    for row in report.get("pairs") or []:
        lines.append(
            "| {sym} | {oanda} | {se} | {sb} | {older} | {oe} | {b10} | {ok} |".format(
                sym=row.get("display_symbol"),
                oanda=row.get("oanda_symbol"),
                se=row.get("earliest_stored_daily_bar") or "—",
                sb=row.get("stored_daily_bar_count"),
                older="Yes" if row.get("can_oanda_fetch_older_than_stored") else "No" if row.get("can_oanda_fetch_older_than_stored") is False else "—",
                oe=row.get("earliest_oanda_fetchable_daily_bar") or "—",
                b10=row.get("bars_fetchable_10y_range") if row.get("bars_fetchable_10y_range") is not None else "—",
                ok="Yes" if row.get("ten_year_backfill_feasible") else "No" if row.get("ten_year_backfill_feasible") is False else "—",
            )
        )

    lines.extend(["", "## Backfill design", ""])
    for rule in bd.get("rules") or []:
        lines.append(f"- {rule}")
    lines.append("")
    lines.append(f"Proposed: `{bd.get('proposed_command')}`")
    lines.append(f"Dry-run: `{bd.get('dry_run_command')}`")
    lines.append(f"After backfill: `{bd.get('post_backfill_audit')}`")
    return "\n".join(lines)


def write_exports(payload: dict[str, Any] | None = None) -> dict[str, Any]:
    payload = payload or build_audit()
    AUDIT_JSON.parent.mkdir(parents=True, exist_ok=True)
    AUDIT_JSON.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
    AUDIT_MD.write_text(_render_markdown(payload), encoding="utf-8")
    return {"json": AUDIT_JSON, "md": AUDIT_MD}


def run() -> dict[str, Any]:
    payload = build_audit()
    paths = write_exports(payload)
    s = payload["summary"]
    print("FX OANDA BACKFILL FEASIBILITY AUDIT (audit-only)")
    print(f"JSON: {paths['json']}")
    print(f"MD:   {paths['md']}")
    print(f"Root cause: {s.get('root_cause')}")
    print(f"10Y backfillable: {s.get('backfillable_pairs')}")
    print(f"Next: {s.get('recommended_next_command')}")
    return payload


def main() -> int:
    run()
    return 0


if __name__ == "__main__":
    sys.exit(main())

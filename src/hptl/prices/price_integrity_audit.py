"""Price integrity audit for workstation and scanner-visible price data."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from hptl.alpha_vantage.mappings import resolve_alpha_mapping
from hptl.config import PROCESSED_DIR, PROJECT_ROOT, get_oanda_api_key
from hptl.markets.instrument_registry import cot_mapped_ids, get_instrument
from hptl.oanda.oanda_client import OandaApiError
from hptl.oanda.oanda_prices import fetch_candles
from hptl.prices.cot_fail_backfill import OANDA_COT_FAIL_PAIRS
from hptl.prices.coverage import load_price_coverage, oanda_symbol_for, select_price_source
from hptl.prices.live_quotes_export import LIVE_QUOTE_OANDA, build_live_quotes_latest
from hptl.prices.workstation_index_ohlc_history import WORKSTATION_INDEX_SOURCES

TARGET_INSTRUMENTS = cot_mapped_ids()

AUDIT_JSON = PROCESSED_DIR / "price_integrity_audit_latest.json"
AUDIT_MD = PROJECT_ROOT / "data" / "audits" / "price_integrity_audit_latest.md"
PUBLIC_JSON = PROJECT_ROOT / "web-dashboard" / "public" / "data" / "price_integrity_audit_latest.json"

MAX_COMPLETED_OHLC_AGE_DAYS = 10
MISMATCH_THRESHOLD_PCT = 5.0

EXPECTED_CONTRACTS: dict[str, str] = {
    "Crude Oil / CL": "WTI crude reference for NYMEX CL; OANDA WTICO_USD preferred.",
    "Gold": "OANDA XAU_USD spot.",
    "Sugar": "Sugar No. 11 reference; OANDA SUGAR_USD preferred.",
    "Soybeans": "Soybeans reference; OANDA SOYBN_USD preferred.",
    "NASDAQ / NQ": "NASDAQ 100 index/futures proxy; OANDA NAS100_USD preferred for workstation OHLC.",
}

OANDA_STORE_SYMBOL: dict[str, str] = {store_key: symbol for _, symbol, store_key in OANDA_COT_FAIL_PAIRS}
OANDA_STORE_SYMBOL["Gold"] = "XAU_USD"
OANDA_STORE_SYMBOL["Silver"] = "XAG_USD"
for _iid, _spec in WORKSTATION_INDEX_SOURCES.items():
    if _spec.get("oanda_symbol"):
        OANDA_STORE_SYMBOL[_iid] = _spec["oanda_symbol"]
for _iid, _spec in LIVE_QUOTE_OANDA.items():
    if _spec.get("oanda_symbol"):
        OANDA_STORE_SYMBOL[_iid] = _spec["oanda_symbol"]

PRICES_DIR = PROCESSED_DIR / "prices"


def _load_json(path: Path) -> dict[str, Any]:
    if not path.is_file():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}


def _num(v: Any) -> float | None:
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    return f if f == f else None


def _parse_dt(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _age_days(date_str: str | None, *, now: datetime) -> int | None:
    if not date_str:
        return None
    try:
        d = datetime.fromisoformat(str(date_str)[:10]).replace(tzinfo=timezone.utc)
    except ValueError:
        return None
    return max(0, (now.date() - d.date()).days)


def _latest_bar(block: dict[str, Any]) -> dict[str, Any] | None:
    weekly = block.get("weekly_ohlc") or block.get("weekly") or []
    if weekly:
        return weekly[-1]
    daily = block.get("daily") or []
    if daily:
        return daily[-1]
    return None


def _coverage_sources(instrument_id: str, coverage: dict[str, Any]) -> list[dict[str, Any]]:
    for row in coverage.get("instruments") or []:
        if row.get("htpl_instrument_id") == instrument_id:
            return list(row.get("sources") or [])
    return []


def _source_symbol(instrument_id: str, coverage: dict[str, Any], ws_block: dict[str, Any]) -> tuple[str | None, str | None]:
    source = ws_block.get("canonical_source") or select_price_source(instrument_id, coverage)
    symbol = ws_block.get("canonical_symbol")
    if symbol:
        return source, symbol

    spec = get_instrument(instrument_id)
    if not spec:
        return source, None
    if source == "oanda":
        return source, oanda_symbol_for(spec, coverage)
    if source == "alpha_vantage":
        mapping = resolve_alpha_mapping(spec)
        return source, mapping.symbol if mapping else None
    return source, None


def _resolve_oanda_symbol(instrument_id: str, coverage: dict[str, Any]) -> str | None:
    if instrument_id in OANDA_STORE_SYMBOL:
        return OANDA_STORE_SYMBOL[instrument_id]
    spec = get_instrument(instrument_id)
    if not spec:
        return None
    return oanda_symbol_for(spec, coverage)


def _raw_oanda_daily(instrument_id: str, coverage: dict[str, Any]) -> dict[str, Any]:
    sym = _resolve_oanda_symbol(instrument_id, coverage)
    if not sym or not get_oanda_api_key():
        return {"provider": "oanda", "symbol": sym, "error": "no_oanda_symbol_or_key"}
    try:
        bars = fetch_candles(sym, granularity="D", count=3)
    except OandaApiError as exc:
        return {"provider": "oanda", "symbol": sym, "error": str(exc)[:200]}
    last = bars[-1] if bars else {}
    return {
        "provider": "oanda",
        "symbol": sym,
        "latest_close": _num(last.get("close")),
        "latest_date": str(last.get("date") or "")[:10] or None,
        "file": "OANDA API /v3/instruments/{instrument}/candles (live)",
    }


def _per_instrument_store(instrument_id: str) -> dict[str, Any]:
    import re

    safe = re.sub(r"[^\w\-]+", "_", instrument_id.strip()).strip("_") or "instrument"
    path = PRICES_DIR / f"{safe}.json"
    if not path.is_file():
        return {"file": str(path), "error": "missing"}
    doc = json.loads(path.read_text(encoding="utf-8"))
    daily = doc.get("daily") or []
    last = daily[-1] if daily else {}
    return {
        "provider": doc.get("_fetched_via"),
        "symbol": (doc.get("price_scale") or {}).get("symbol"),
        "latest_close": _num(last.get("close")),
        "latest_date": str(last.get("date") or "")[:10] or None,
        "file": str(path),
    }


def _wrong_symbol_note(instrument_id: str, source: str | None, symbol: str | None) -> str | None:
    if instrument_id == "NASDAQ / NQ":
        if symbol != "NAS100_USD" and not str(source or "").startswith("oanda"):
            return "NASDAQ / NQ is not using the configured OANDA NAS100_USD workstation proxy."
    return None


def _status_and_notes(
    *,
    hptl_close: float | None,
    hptl_date: str | None,
    reference_price: float | None,
    diff_pct: float | None,
    age_days: int | None,
    wrong_symbol_note: str | None,
    raw_close: float | None = None,
    raw_date: str | None = None,
    raw_vs_displayed_pct: float | None = None,
) -> tuple[str, list[str]]:
    notes: list[str] = []
    if hptl_close is None or not hptl_date:
        return "MISSING", ["No usable HPTL weekly/daily close found."]

    if wrong_symbol_note:
        notes.append(wrong_symbol_note)

    if raw_date and hptl_date and raw_date > hptl_date:
        notes.append(f"Displayed OHLC ({hptl_date}) is older than raw source ({raw_date}).")
        if wrong_symbol_note:
            return "WRONG_SYMBOL", notes
        return "STALE", notes

    if raw_close is not None and raw_vs_displayed_pct is not None and abs(raw_vs_displayed_pct) > MISMATCH_THRESHOLD_PCT:
        notes.append(f"Displayed close differs from raw source by {raw_vs_displayed_pct:+.2f}%.")
        return "MISMATCH", notes

    if age_days is not None and age_days > MAX_COMPLETED_OHLC_AGE_DAYS:
        notes.append(f"Latest HPTL OHLC is {age_days} days old.")
        if wrong_symbol_note:
            return "WRONG_SYMBOL", notes
        return "STALE", notes

    if reference_price is not None and diff_pct is not None and abs(diff_pct) > MISMATCH_THRESHOLD_PCT:
        notes.append(f"HPTL close differs from live reference by {diff_pct:+.2f}%.")
        if wrong_symbol_note:
            return "WRONG_SYMBOL", notes
        return "MISMATCH", notes

    if wrong_symbol_note:
        return "WRONG_SYMBOL", notes

    return "PASS", notes or ["Displayed price matches raw source within threshold."]


def build_price_integrity_audit(
    instruments: list[str] | None = None,
    *,
    fetch_live: bool = True,
) -> dict[str, Any]:
    now = datetime.now(timezone.utc)
    targets = instruments or TARGET_INSTRUMENTS
    coverage = load_price_coverage()
    prices_doc = _load_json(PROCESSED_DIR / "prices_latest.json")
    if not prices_doc:
        prices_doc = _load_json(PROJECT_ROOT / "web-dashboard" / "public" / "data" / "prices_latest.json")
    ws_doc = _load_json(PROCESSED_DIR / "workstation_ohlc_latest.json")
    if not ws_doc:
        ws_doc = _load_json(PROJECT_ROOT / "web-dashboard" / "public" / "data" / "workstation_ohlc_latest.json")
    live_doc = build_live_quotes_latest(fetch_live=fetch_live)

    rows: list[dict[str, Any]] = []
    for iid in targets:
        price_block = (prices_doc.get("instruments") or {}).get(iid) or {}
        ws_block = (ws_doc.get("instruments") or {}).get(iid) or {}
        live_block = (live_doc.get("instruments") or {}).get(iid) or {}

        ws_weekly = (ws_block.get("weekly_ohlc") or [])
        ws_last = ws_weekly[-1] if ws_weekly else {}
        displayed_close = _num(ws_last.get("close"))
        displayed_date = str(ws_last.get("date") or "")[:10] or None

        store_rec = _per_instrument_store(iid)
        processed_close = store_rec.get("latest_close") or _num((_latest_bar(price_block) or {}).get("close"))
        processed_date = store_rec.get("latest_date") or str((_latest_bar(price_block) or {}).get("date") or "")[:10] or None

        raw = _raw_oanda_daily(iid, coverage) if fetch_live else {}
        raw_close = raw.get("latest_close")
        raw_date = raw.get("latest_date")

        source, symbol = _source_symbol(iid, coverage, ws_block)
        ref = _num(live_block.get("live_price")) or raw_close
        ref_source = live_block.get("live_price_source") or (
            f"oanda:{raw.get('symbol')}" if raw.get("symbol") else None
        )

        compare_close = displayed_close if displayed_close is not None else processed_close
        compare_date = displayed_date or processed_date
        diff_pct = ((compare_close - ref) / ref * 100.0) if compare_close is not None and ref else None
        raw_diff_pct = (
            ((compare_close - raw_close) / raw_close * 100.0)
            if compare_close is not None and raw_close
            else None
        )
        age = _age_days(compare_date, now=now)
        wrong_note = _wrong_symbol_note(iid, source, symbol)
        status, notes = _status_and_notes(
            hptl_close=compare_close,
            hptl_date=compare_date,
            reference_price=ref,
            diff_pct=diff_pct,
            age_days=age,
            wrong_symbol_note=wrong_note,
            raw_close=raw_close,
            raw_date=raw_date,
            raw_vs_displayed_pct=raw_diff_pct,
        )

        responsible_file = None
        if status != "PASS":
            if raw.get("error") or (raw_close is None and ref is None):
                responsible_file = raw.get("file") or "OANDA mapping / API"
            elif raw_close is not None and processed_close is not None and abs(raw_close - processed_close) > max(abs(raw_close) * 0.005, 1e-6):
                responsible_file = store_rec.get("file") or str(PRICES_DIR)
            elif processed_close is not None and displayed_close is not None and abs(processed_close - displayed_close) > max(abs(processed_close) * 0.005, 1e-6):
                responsible_file = str(PROCESSED_DIR / "workstation_ohlc_latest.json")
            elif wrong_note:
                responsible_file = "data/price_coverage_audit.json or canonical_timeline mapping"
            else:
                responsible_file = str(PROCESSED_DIR / "workstation_ohlc_latest.json")

        rows.append(
            {
                "instrument": iid,
                "status": status,
                "hptl_displayed_close": displayed_close,
                "hptl_displayed_date": displayed_date,
                "hptl_latest_close": compare_close,
                "hptl_latest_date": compare_date,
                "hptl_latest_age_days": age,
                "source_provider": source,
                "source_symbol": symbol,
                "raw_source_provider": raw.get("provider"),
                "raw_source_symbol": raw.get("symbol"),
                "raw_source_latest_close": raw_close,
                "raw_source_latest_date": raw_date,
                "processed_latest_close": processed_close,
                "processed_latest_date": processed_date,
                "weekly_ohlc_export_close": displayed_close,
                "weekly_ohlc_export_date": displayed_date,
                "source_file": store_rec.get("file") or str(PROCESSED_DIR / "prices_latest.json"),
                "workstation_ohlc_export": str(PROCESSED_DIR / "workstation_ohlc_latest.json"),
                "reference_live_price": ref,
                "reference_live_source": ref_source,
                "reference_live_as_of": live_block.get("live_price_as_of"),
                "difference_pct": round(diff_pct, 4) if diff_pct is not None else None,
                "raw_vs_displayed_pct": round(raw_diff_pct, 4) if raw_diff_pct is not None else None,
                "responsible_file": responsible_file,
                "expected_contract": EXPECTED_CONTRACTS.get(iid),
                "coverage_sources": _coverage_sources(iid, coverage),
                "notes": notes,
            }
        )

    return {
        "version": 1,
        "generated_at": now.isoformat(),
        "generated_from": "hptl.prices.price_integrity_audit",
        "fetch_live": fetch_live,
        "thresholds": {
            "max_completed_ohlc_age_days": MAX_COMPLETED_OHLC_AGE_DAYS,
            "mismatch_threshold_pct": MISMATCH_THRESHOLD_PCT,
        },
        "summary": {
            "targets": len(rows),
            "pass": sum(1 for r in rows if r["status"] == "PASS"),
            "stale": sum(1 for r in rows if r["status"] == "STALE"),
            "mismatch": sum(1 for r in rows if r["status"] == "MISMATCH"),
            "wrong_symbol": sum(1 for r in rows if r["status"] == "WRONG_SYMBOL"),
            "missing": sum(1 for r in rows if r["status"] == "MISSING"),
        },
        "instruments": rows,
    }


def _format_md(doc: dict[str, Any]) -> str:
    lines = [
        "# Price Integrity Audit",
        "",
        f"Generated: {doc.get('generated_at')}",
        f"Targets: {doc.get('summary', {}).get('targets', 0)} COT-mapped instruments",
        "",
        "| Instrument | Status | HPTL displayed | HPTL date | Provider | Symbol | Raw close | Raw date | Ref live | Diff % | Responsible file | Notes |",
        "| --- | --- | ---: | --- | --- | --- | ---: | --- | ---: | ---: | --- | --- |",
    ]
    for row in doc.get("instruments") or []:
        notes = "; ".join(row.get("notes") or []).replace("|", "/")
        lines.append(
            "| {instrument} | {status} | {disp} | {date} | {prov} | {sym} | {raw} | {rawd} | {ref} | {diff} | {file} | {notes} |".format(
                instrument=row.get("instrument"),
                status=row.get("status"),
                disp=row.get("hptl_displayed_close") if row.get("hptl_displayed_close") is not None else row.get("processed_latest_close"),
                date=row.get("hptl_latest_date"),
                prov=row.get("source_provider") or "-",
                sym=row.get("source_symbol") or "-",
                raw=row.get("raw_source_latest_close") if row.get("raw_source_latest_close") is not None else "-",
                rawd=row.get("raw_source_latest_date") or "-",
                ref=row.get("reference_live_price") if row.get("reference_live_price") is not None else "-",
                diff="-" if row.get("difference_pct") is None else f"{row['difference_pct']:+.2f}%",
                file=(row.get("responsible_file") or "-").replace("|", "/"),
                notes=notes,
            )
        )
    return "\n".join(lines) + "\n"


def write_price_integrity_audit(
    instruments: list[str] | None = None,
    *,
    fetch_live: bool = True,
) -> Path:
    doc = build_price_integrity_audit(instruments, fetch_live=fetch_live)
    text = json.dumps(doc, indent=2, ensure_ascii=False)
    AUDIT_JSON.parent.mkdir(parents=True, exist_ok=True)
    AUDIT_JSON.write_text(text, encoding="utf-8")
    PUBLIC_JSON.parent.mkdir(parents=True, exist_ok=True)
    PUBLIC_JSON.write_text(text, encoding="utf-8")
    AUDIT_MD.parent.mkdir(parents=True, exist_ok=True)
    AUDIT_MD.write_text(_format_md(doc), encoding="utf-8")
    return AUDIT_JSON


def main() -> int:
    path = write_price_integrity_audit()
    doc = json.loads(path.read_text(encoding="utf-8"))
    print(_format_md(doc))
    bad = {"STALE", "MISMATCH", "WRONG_SYMBOL", "MISSING"}
    return 1 if any(row.get("status") in bad for row in doc.get("instruments") or []) else 0


if __name__ == "__main__":
    raise SystemExit(main())

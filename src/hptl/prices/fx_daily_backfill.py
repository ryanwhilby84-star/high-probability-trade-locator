"""FX daily OHLC historical backfill engine — staging only, production-safe merge."""

from __future__ import annotations

import json
import logging
import re
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from hptl.config import DATA_DIR, PROJECT_ROOT
from hptl.oanda.oanda_client import OandaApiError
from hptl.prices.fx_oanda_backfill_feasibility_audit import (
    RECOMMENDED_CHUNK_DAYS,
    TEST_PAIRS,
    _iso_from,
    _probe_candles,
)
from hptl.prices.models import (
    InstrumentPriceRecord,
    OhlcBar,
    build_history_meta,
    compute_range_52w,
)
from hptl.prices.price_store import load_instrument_record
from hptl.seasonality.seasonality_v2 import normalize_daily_bars, years_spanned

logger = logging.getLogger(__name__)

STAGING_DIR = DATA_DIR / "processed" / "prices" / "backfill"
CHECKPOINT_DIR = STAGING_DIR / "checkpoints"
SUMMARY_PATH = STAGING_DIR / "backfill_summary.json"

MIN_YEARS_FOR_10Y = 10.0
MIN_DAILY_BARS_FOR_10Y = 252 * 8

BackfillPair = tuple[str, str, str]  # display, oanda, store_key


def _safe_filename(instrument_id: str) -> str:
    s = re.sub(r"[^\w\-]+", "_", instrument_id.strip())
    return s.strip("_") or "instrument"


def staging_path(store_key: str) -> Path:
    return STAGING_DIR / f"{_safe_filename(store_key)}.json"


def checkpoint_path(store_key: str) -> Path:
    return CHECKPOINT_DIR / f"{_safe_filename(store_key)}.json"


def merge_ohlc_bars(
    existing: list[OhlcBar],
    incoming: list[OhlcBar],
    *,
    prefer: str = "existing",
) -> tuple[list[OhlcBar], int]:
    """Merge OHLC bars by date.

    ``prefer=\"existing\"`` — backfill/promotion: keep stored bars on overlap.
    ``prefer=\"incoming\"`` — live refresh: API bars replace same-date stored bars.
    """
    if prefer not in ("existing", "incoming"):
        raise ValueError(f"prefer must be 'existing' or 'incoming', got {prefer!r}")
    by_date: dict[str, OhlcBar] = {str(b["date"])[:10]: b for b in existing if b.get("date")}
    added = 0
    for bar in incoming:
        d = str(bar.get("date") or "")[:10]
        if not d:
            continue
        if d in by_date:
            if prefer == "incoming":
                by_date[d] = bar
            continue
        by_date[d] = bar
        added += 1
    merged = sorted(by_date.values(), key=lambda b: b["date"])
    return merged, added


def merge_daily_bars(
    existing: list[OhlcBar],
    incoming: list[OhlcBar],
) -> tuple[list[OhlcBar], int]:
    """Merge by date; existing records win — never overwrite newer stored bars."""
    return merge_ohlc_bars(existing, incoming, prefer="existing")


def merge_daily_bars_refresh(
    existing: list[OhlcBar],
    incoming: list[OhlcBar],
) -> tuple[list[OhlcBar], int]:
    """Merge by date for live refresh; incoming API bars win on overlap."""
    return merge_ohlc_bars(existing, incoming, prefer="incoming")


def _parse_date(d: str) -> date:
    return datetime.strptime(d[:10], "%Y-%m-%d").date()


def _load_staging_record(store_key: str) -> InstrumentPriceRecord | None:
    path = staging_path(store_key)
    if not path.exists():
        return None
    try:
        doc = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    return {
        "instrument_id": doc.get("instrument_id", store_key),
        "price": doc.get("price"),
        "daily": doc.get("daily") or [],
        "weekly": doc.get("weekly") or [],
        "range_52w": doc.get("range_52w"),
        "history": doc.get("history"),
        "error": doc.get("error"),
        "price_scale": doc.get("price_scale"),
    }


def _baseline_record(store_key: str) -> InstrumentPriceRecord:
    staging = _load_staging_record(store_key)
    if staging is not None:
        return staging
    prod = load_instrument_record(store_key)
    if prod is not None:
        return prod
    return {"instrument_id": store_key, "daily": [], "weekly": []}


def _load_checkpoint(store_key: str) -> dict[str, Any] | None:
    path = checkpoint_path(store_key)
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None


def _write_checkpoint(store_key: str, payload: dict[str, Any]) -> None:
    CHECKPOINT_DIR.mkdir(parents=True, exist_ok=True)
    payload["updated_at"] = datetime.now(timezone.utc).isoformat()
    checkpoint_path(store_key).write_text(
        json.dumps(payload, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )


def _write_staging_record(
    store_key: str,
    record: InstrumentPriceRecord,
    *,
    backfill_meta: dict[str, Any],
) -> Path:
    STAGING_DIR.mkdir(parents=True, exist_ok=True)
    daily = normalize_daily_bars(record.get("daily") or [])
    range_52w = compute_range_52w(daily)
    weekly = record.get("weekly") or []
    history = build_history_meta(daily, weekly, range_52w)

    out: dict[str, Any] = {
        "instrument_id": store_key,
        "price": record.get("price"),
        "daily": daily,
        "weekly": weekly,
        "range_52w": range_52w,
        "history": history,
        "error": record.get("error"),
        "stored_at": datetime.now(timezone.utc).isoformat(),
        "_backfill": backfill_meta,
    }
    if record.get("price_scale"):
        out["price_scale"] = record.get("price_scale")

    path = staging_path(store_key)
    path.write_text(json.dumps(out, indent=2, ensure_ascii=False), encoding="utf-8")
    return path


def fetch_chunked_daily(
    oanda_symbol: str,
    *,
    start: date,
    end: date,
    chunk_size: int = RECOMMENDED_CHUNK_DAYS,
    resume_from: date | None = None,
    on_chunk: Any | None = None,
) -> tuple[list[OhlcBar], list[str]]:
    """Fetch daily candles from OANDA using from/count pagination."""
    warnings: list[str] = []
    collected: list[OhlcBar] = []
    current = resume_from or start

    while current <= end:
        bars, meta = _probe_candles(oanda_symbol, from_time=_iso_from(current), count=chunk_size)
        if meta.get("error"):
            warnings.append(f"chunk from {current}: {meta['error']}")
            break
        if not bars:
            break

        trimmed = [b for b in bars if _parse_date(b["date"]) <= end]
        collected.extend(trimmed)

        if on_chunk and trimmed:
            on_chunk(trimmed)

        last = _parse_date(trimmed[-1]["date"])
        if last >= end:
            break
        if len(bars) < chunk_size:
            break
        current = last + timedelta(days=1)

    return collected, warnings


def backfill_pair(
    display: str,
    oanda_symbol: str,
    store_key: str,
    *,
    years: int,
    chunk_size: int = RECOMMENDED_CHUNK_DAYS,
) -> dict[str, Any]:
    """Backfill one FX pair into staging. Resumes from checkpoint when present."""
    today = date.today()
    target_start = today - timedelta(days=years * 366)
    target_end = today
    warnings: list[str] = []

    checkpoint = _load_checkpoint(store_key)
    baseline: InstrumentPriceRecord | None = None
    merged: list[OhlcBar] = []
    resume_from: date | None = None
    bars_before = 0
    chunks_fetched = 0
    bars_added_total = 0

    if checkpoint and checkpoint.get("status") == "completed":
        rec = _baseline_record(store_key)
        daily = normalize_daily_bars(rec.get("daily") or [])
        latest = daily[-1]["date"] if daily else None
        stale_after = (today - timedelta(days=3)).isoformat()
        if latest and latest >= stale_after:
            logger.info("%s: checkpoint completed — skipping", display)
            return {
                "instrument": store_key,
                "display_symbol": display,
                "oanda_symbol": oanda_symbol,
                "status": "skipped_completed",
                "bars_added": 0,
                "earliest_date": daily[0]["date"] if daily else None,
                "latest_date": latest,
                "total_bars_after_merge": len(daily),
                "years_of_coverage": round(years_spanned(daily), 2) if daily else 0.0,
                "can_10y_seasonality": _can_10y(daily),
                "warnings": ["Resumed from completed checkpoint — no fetch performed"],
            }
        logger.info("%s: checkpoint completed but stale (%s) — incremental refresh", display, latest)
        warnings.append(f"Incremental refresh from {latest}")
        baseline = rec
        merged = daily
        bars_before = len(merged)
        resume_from = _parse_date(latest) + timedelta(days=1) if latest else target_start
    else:
        baseline = _baseline_record(store_key)
        merged = normalize_daily_bars(baseline.get("daily") or [])
        bars_before = len(merged)

        if checkpoint and checkpoint.get("status") == "in_progress":
            resume_from_str = checkpoint.get("next_from")
            if resume_from_str:
                try:
                    resume_from = _parse_date(resume_from_str)
                except ValueError:
                    warnings.append(f"Invalid checkpoint next_from: {resume_from_str}")
            chunks_fetched = int(checkpoint.get("chunks_fetched") or 0)
            bars_added_total = int(checkpoint.get("bars_added_total") or 0)
            warnings.extend(checkpoint.get("warnings") or [])

    assert baseline is not None

    def _persist(next_from: date | None, *, status: str) -> None:
        daily_norm = normalize_daily_bars(merged)
        range_52w = compute_range_52w(daily_norm)
        out_rec: InstrumentPriceRecord = {
            **baseline,
            "instrument_id": store_key,
            "daily": daily_norm,
            "range_52w": range_52w,
            "history": build_history_meta(daily_norm, baseline.get("weekly") or [], range_52w),
        }
        _write_staging_record(
            store_key,
            out_rec,
            backfill_meta={
                "source": "oanda",
                "display_symbol": display,
                "oanda_symbol": oanda_symbol,
                "years_requested": years,
                "target_start": target_start.isoformat(),
                "target_end": target_end.isoformat(),
                "status": status,
            },
        )
        _write_checkpoint(
            store_key,
            {
                "store_key": store_key,
                "display_symbol": display,
                "oanda_symbol": oanda_symbol,
                "years": years,
                "target_start": target_start.isoformat(),
                "target_end": target_end.isoformat(),
                "next_from": next_from.isoformat() if next_from else None,
                "status": status,
                "chunks_fetched": chunks_fetched,
                "bars_added_total": bars_added_total,
                "warnings": warnings,
            },
        )

    _persist(resume_from or target_start, status="in_progress")

    def _on_chunk(chunk: list[OhlcBar]) -> None:
        nonlocal merged, bars_added_total, chunks_fetched, resume_from
        merged, added = merge_daily_bars(merged, chunk)
        bars_added_total += added
        chunks_fetched += 1
        if merged:
            last = _parse_date(merged[-1]["date"])
            resume_from = last + timedelta(days=1) if last < target_end else None
        _persist(resume_from, status="in_progress")
        logger.info(
            "%s chunk %d: +%d bars (total=%d, range=%s..%s)",
            display,
            chunks_fetched,
            added,
            len(merged),
            merged[0]["date"] if merged else "—",
            merged[-1]["date"] if merged else "—",
        )

    try:
        fetched, fetch_warnings = fetch_chunked_daily(
            oanda_symbol,
            start=target_start,
            end=target_end,
            chunk_size=chunk_size,
            resume_from=resume_from,
            on_chunk=_on_chunk,
        )
        warnings.extend(fetch_warnings)
        if not fetched and not merged:
            warnings.append("No candles returned from OANDA")
            status = "failed"
        else:
            status = "completed"
    except OandaApiError as exc:
        warnings.append(str(exc)[:300])
        status = "failed"

    daily_final = normalize_daily_bars(merged)
    bars_added = len(daily_final) - bars_before
    _persist(None, status=status)
    _write_checkpoint(
        store_key,
        {
            "store_key": store_key,
            "display_symbol": display,
            "oanda_symbol": oanda_symbol,
            "years": years,
            "target_start": target_start.isoformat(),
            "target_end": target_end.isoformat(),
            "next_from": None,
            "status": status,
            "chunks_fetched": chunks_fetched,
            "bars_added_total": bars_added,
            "warnings": warnings,
        },
    )

    return {
        "instrument": store_key,
        "display_symbol": display,
        "oanda_symbol": oanda_symbol,
        "status": status,
        "bars_added": bars_added,
        "earliest_date": daily_final[0]["date"] if daily_final else None,
        "latest_date": daily_final[-1]["date"] if daily_final else None,
        "total_bars_after_merge": len(daily_final),
        "years_of_coverage": round(years_spanned(daily_final), 2) if daily_final else 0.0,
        "can_10y_seasonality": _can_10y(daily_final),
        "chunks_fetched": chunks_fetched,
        "staging_file": str(staging_path(store_key).relative_to(PROJECT_ROOT)),
        "warnings": warnings,
    }


def _can_10y(daily: list[OhlcBar]) -> bool:
    return len(daily) >= MIN_DAILY_BARS_FOR_10Y and years_spanned(daily) >= MIN_YEARS_FOR_10Y


def run_backfill(
    *,
    pairs: tuple[BackfillPair, ...] = TEST_PAIRS,
    years: int = 10,
    chunk_size: int = RECOMMENDED_CHUNK_DAYS,
) -> dict[str, Any]:
    """Execute backfill for all pairs; write summary; skip failures and continue."""
    STAGING_DIR.mkdir(parents=True, exist_ok=True)
    results: list[dict[str, Any]] = []
    all_ok = True

    for display, oanda, store_key in pairs:
        logger.info("Backfilling %s (%s -> %s)", display, oanda, store_key)
        try:
            result = backfill_pair(display, oanda, store_key, years=years, chunk_size=chunk_size)
        except Exception as exc:  # noqa: BLE001 — continue on pair failure
            logger.exception("Backfill failed for %s", display)
            result = {
                "instrument": store_key,
                "display_symbol": display,
                "oanda_symbol": oanda,
                "status": "failed",
                "bars_added": 0,
                "earliest_date": None,
                "latest_date": None,
                "total_bars_after_merge": 0,
                "years_of_coverage": 0.0,
                "can_10y_seasonality": False,
                "warnings": [str(exc)[:300]],
            }
        results.append(result)
        if result.get("status") not in ("completed", "skipped_completed"):
            all_ok = False

    summary = {
        "schema_version": 1,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "parser": "hptl.prices.fx_daily_backfill",
        "staging_dir": str(STAGING_DIR.relative_to(PROJECT_ROOT)),
        "production_promoted": False,
        "promotion_note": "Staging only — explicit confirmation required before production merge.",
        "years_requested": years,
        "chunk_size_bars": chunk_size,
        "backfill_completed_successfully": all_ok,
        "pairs": results,
        "totals": {
            "pairs_processed": len(results),
            "completed": sum(1 for r in results if r.get("status") == "completed"),
            "skipped_completed": sum(1 for r in results if r.get("status") == "skipped_completed"),
            "failed": sum(1 for r in results if r.get("status") == "failed"),
            "bars_added": sum(int(r.get("bars_added") or 0) for r in results),
            "can_10y_seasonality": sum(1 for r in results if r.get("can_10y_seasonality")),
        },
    }
    SUMMARY_PATH.write_text(json.dumps(summary, indent=2, ensure_ascii=False), encoding="utf-8")
    return summary


def validate_staging_coverage(
    pairs: tuple[BackfillPair, ...] = TEST_PAIRS,
) -> dict[str, Any]:
    """Validate staging files against 10Y seasonality criteria (does not touch production)."""
    rows: list[dict[str, Any]] = []
    for display, _oanda, store_key in pairs:
        rec = _load_staging_record(store_key)
        daily = normalize_daily_bars((rec or {}).get("daily") or [])
        rows.append(
            {
                "display_symbol": display,
                "instrument": store_key,
                "total_daily_bars": len(daily),
                "years_of_coverage": round(years_spanned(daily), 2) if daily else 0.0,
                "earliest_date": daily[0]["date"] if daily else None,
                "latest_date": daily[-1]["date"] if daily else None,
                "can_10y_seasonality": _can_10y(daily),
            }
        )
    return {
        "staging_dir": str(STAGING_DIR.relative_to(PROJECT_ROOT)),
        "pairs": rows,
        "can_10y_count": sum(1 for r in rows if r["can_10y_seasonality"]),
        "can_10y_pairs": [r["display_symbol"] for r in rows if r["can_10y_seasonality"]],
    }

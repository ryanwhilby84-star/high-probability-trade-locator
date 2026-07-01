"""Phase 1G-C — full FX futures data refresh with per-source logging."""

from __future__ import annotations

import json
import traceback
from datetime import datetime, timezone
from typing import Any

from hptl.config import DATA_DIR
from hptl.fx.currency_map import DX_INSTRUMENT_ID
from hptl.fx.fx_macro_history import ensure_ecb_yield_history_caches, load_ecb_yield_history
from hptl.fx.ingest_currency_rates import ingest
from hptl.prices.coverage import load_price_coverage, select_price_source
from hptl.prices.price_store import (
    load_instrument_record_internal,
    merge_fetched_into_production,
    write_instrument_record,
)
from hptl.prices.run_price_refresh import refresh_instrument_record
from hptl.prices.unified_adapter import UnifiedPriceAdapter
from hptl.valuation.currency_futures_ive_v1 import FUTURES_REGISTRY

FRED_MACRO_SERIES = ("DFF", "DGS2", "DGS10")
LOG_PATH = DATA_DIR / "audits" / "fx_futures_weekend_pull_log.json"


def _log_entry(
    *,
    source_name: str,
    series: str,
    status: str,
    error: str | None = None,
    fallback: str | None = None,
    latest: str | None = None,
    detail: str | None = None,
) -> dict[str, Any]:
    return {
        "source_name": source_name,
        "requested_series": series,
        "status": status,
        "error": error,
        "fallback_attempted": fallback,
        "latest_date": latest,
        "detail": detail,
    }


def _refresh_fred_macro(log: list[dict[str, Any]]) -> None:
    from hptl.macro import fred_client

    for sid in FRED_MACRO_SERIES:
        try:
            df = fred_client.get_series_df(sid, "2016-01-01")
            latest = str(df["date"].max())[:10] if df is not None and not df.empty else None
            log.append(
                _log_entry(
                    source_name="FRED",
                    series=sid,
                    status="ok",
                    latest=latest,
                    detail=f"{len(df)} observations",
                )
            )
        except Exception as exc:
            log.append(
                _log_entry(
                    source_name="FRED",
                    series=sid,
                    status="fail",
                    error=f"{type(exc).__name__}: {exc}",
                    fallback="macro_cache only",
                )
            )


def _refresh_ecb_yields(log: list[dict[str, Any]]) -> None:
    try:
        ensure_ecb_yield_history_caches()
        for key in ("eur_2y_history", "eur_10y_history", "eur_dfr_history"):
            series = load_ecb_yield_history(key)
            log.append(
                _log_entry(
                    source_name="ECB",
                    series=key,
                    status="ok" if series else "fail",
                    latest=max(series) if series else None,
                    error=None if series else "empty cache after fetch",
                    detail=f"n={len(series)}",
                )
            )
    except Exception as exc:
        log.append(
            _log_entry(
                source_name="ECB",
                series="eur_yields",
                status="fail",
                error=f"{type(exc).__name__}: {exc}",
            )
        )


def _refresh_g10_history_leg(log: list[dict[str, Any]], ccy: str) -> None:
    from hptl.fx import fx_macro_history as hist

    loaders = {
        "GBP": lambda: hist.load_gbp_boe_yield_history(),
        "JPY": lambda: (hist.load_jpy_y2_history(), hist.load_jpy_y10_history()),
        "AUD": lambda: hist.load_aud_rba_history(),
        "CAD": lambda: hist.load_cad_valet_history(),
        "CHF": lambda: (hist.load_chf_y2_history(), hist.load_chf_y10_history()),
        "NZD": lambda: (hist.load_nzd_y2_history(), hist.load_nzd_y10_history()),
    }
    try:
        result = loaders[ccy]()
        if ccy in ("GBP", "AUD", "CAD"):
            if ccy == "GBP":
                y2 = result.get("y2") or {}
                latest = max(y2) if y2 else None
            elif ccy == "AUD":
                y2 = result.get("y2") or {}
                latest = max(y2) if y2 else None
            else:
                y2 = {d: v["y2"] for d, v in result.items() if v.get("y2") is not None}
                latest = max(y2) if y2 else None
        else:
            y2_res, _ = result if isinstance(result, tuple) else (result, None)
            y2 = y2_res if isinstance(y2_res, dict) else {}
            latest = max(y2) if y2 else None
        log.append(
            _log_entry(
                source_name=f"{ccy} history",
                series=f"{ccy}.y2",
                status="ok" if y2 else "fail",
                latest=latest,
                detail=f"n={len(y2)}",
            )
        )
    except Exception as exc:
        log.append(
            _log_entry(
                source_name=f"{ccy} history",
                series=f"{ccy}.y2",
                status="fail",
                error=f"{type(exc).__name__}: {exc}",
            )
        )


def refresh_fx_futures_prices(log: list[dict[str, Any]]) -> dict[str, Any]:
    coverage = load_price_coverage()
    adapter = UnifiedPriceAdapter(coverage)
    results: dict[str, Any] = {}

    for sym, spec in FUTURES_REGISTRY.items():
        iid = spec.instrument_id
        source = select_price_source(iid, coverage) or "none"
        try:
            fetched = adapter.fetch(iid)
            err = fetched.get("error")
            merged, meta = merge_fetched_into_production(
                load_instrument_record_internal(iid),
                fetched,
                fetched_via=adapter.source_for(iid) or source,
            )
            write_instrument_record(
                merged,
                fetched_via=meta.get("fetched_via"),
                historical_via=meta.get("historical_via"),
            )
            daily = merged.get("daily") or []
            price = merged.get("price") or {}
            status = "ok" if daily and not merged.get("error") else "fail"
            results[sym] = {
                "instrument": iid,
                "provider": source,
                "status": status,
                "daily_bars": len(daily),
                "last_date": daily[-1]["date"] if daily else None,
                "price_as_of": price.get("as_of"),
                "error": merged.get("error"),
            }
            log.append(
                _log_entry(
                    source_name=f"price:{source}",
                    series=iid,
                    status=status,
                    error=str(err) if err and status == "fail" else None,
                    fallback="stored_history_preserved" if daily else None,
                    latest=daily[-1]["date"] if daily else None,
                    detail=f"bars={len(daily)} proxy={'DTWEXBGS' if sym == 'DX' else 'OANDA FX'}",
                )
            )
        except Exception as exc:
            results[sym] = {"instrument": iid, "status": "fail", "error": str(exc)}
            log.append(
                _log_entry(
                    source_name=f"price:{source}",
                    series=iid,
                    status="fail",
                    error=f"{type(exc).__name__}: {exc}",
                    fallback="none",
                )
            )
    return results


def refresh_fx_futures_macro(log: list[dict[str, Any]]) -> dict[str, Any]:
    _refresh_fred_macro(log)
    _refresh_ecb_yields(log)
    for ccy in ("GBP", "JPY", "AUD", "CAD", "CHF", "NZD"):
        _refresh_g10_history_leg(log, ccy)
    try:
        config = ingest(write=True, verbose=False)
        statuses = {c: b.get("status") for c, b in config.get("currencies", {}).items()}
        for ccy, st in statuses.items():
            rec = config["currencies"][ccy]
            log.append(
                _log_entry(
                    source_name=f"ingest:{ccy}",
                    series=f"{ccy} policy/y2/y10",
                    status="ok" if st == "PASS" else "warn" if st == "WARN" else "fail",
                    latest=rec.get("y2_as_of") or rec.get("policy_rate_as_of"),
                    detail=f"status={st}",
                )
            )
        return {"currency_rates": statuses}
    except Exception as exc:
        log.append(
            _log_entry(
                source_name="ingest",
                series="G10 currency rates",
                status="fail",
                error=f"{type(exc).__name__}: {exc}",
            )
        )
        return {"error": str(exc)}


def refresh_fx_futures_data() -> dict[str, Any]:
    """Run complete Phase 1G-C weekend data pull."""
    log: list[dict[str, Any]] = []
    report: dict[str, Any] = {
        "phase": "1G-C Weekend FX Futures Pull",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "sources": log,
    }
    try:
        report["prices"] = refresh_fx_futures_prices(log)
        report["macro"] = refresh_fx_futures_macro(log)
    except Exception as exc:
        report["fatal_error"] = f"{type(exc).__name__}: {exc}"
        report["traceback"] = traceback.format_exc()
        log.append(
            _log_entry(
                source_name="pipeline",
                series="fx_futures_refresh",
                status="fail",
                error=str(exc),
            )
        )

    LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    LOG_PATH.write_text(json.dumps(report, indent=2), encoding="utf-8")
    report["log_path"] = str(LOG_PATH)
    return report

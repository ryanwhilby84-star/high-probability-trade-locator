"""USD / DXY anchor — auditable direct Dollar Index data vs G10 synthetic fallback."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from hptl.fx.currency_map import COT_CURRENCY_SOURCES, DX_INSTRUMENT_ID
from hptl.macro_hub.config import COT_CFTC_USD_INDEX, FRED_USD_DXY, STALE_COT_DAYS, STALE_FRED_DAYS
from hptl.macro_hub.freshness import freshness_status
from hptl.markets.instrument_registry import get_instrument

USD_SYNTHETIC_LABEL = "Synthetic USD (G10 inverse basket)"
USD_DIRECT_LABEL = "Direct USD / DXY (ICE Dollar Index COT 098662)"


def _load_macro_hub_usd() -> dict[str, Any] | None:
    for rel in (
        "web-dashboard/public/data/macro_hub_latest.json",
        "data/processed/macro_hub_latest.json",
        "data/exports/macro_hub_latest.json",
    ):
        path = Path(rel)
        if path.exists():
            try:
                doc = json.loads(path.read_text(encoding="utf-8"))
                usd = doc.get("usd")
                return usd if isinstance(usd, dict) else None
            except (OSError, json.JSONDecodeError):
                continue
    return None


def load_usd_price_block(*, allow_live: bool = False) -> dict[str, Any]:
    """ICE DX futures from price_store; FRED broad only under its own labelled id."""
    from hptl.macro_hub.price_history import fred_series_block, price_block_from_store
    from hptl.markets.usd_index_identity import BROAD_USD_ID, ICE_DXY_ID

    for iid, label in (
        (ICE_DXY_ID, "US Dollar Index / DXY — ICE DX futures"),
        (DX_INSTRUMENT_ID, "US Dollar Index / DX"),
    ):
        store = price_block_from_store(
            iid,
            label=label,
            stale_after_days=STALE_FRED_DAYS,
        )
        # Refuse FRED-broad bars mis-bound under DX/ICE ids
        src = str(store.get("source") or "")
        if store.get("latest_price") is not None and "fred" not in src.lower() and "DTWEX" not in src:
            return {
                "mode": "price_store",
                "label": f"{label} (price_store)",
                "current_close": store.get("latest_price"),
                "as_of_date": store.get("latest_date"),
                "source": store.get("source") or "price_store",
                "series_id": None,
                "is_fallback": False,
                "fallback_note": None,
                "freshness": store.get("freshness"),
                "history": store.get("history"),
                "confidence": "high" if store.get("freshness", {}).get("status") == "fresh" else "moderate",
            }

    broad_store = price_block_from_store(
        BROAD_USD_ID,
        label=BROAD_USD_ID,
        stale_after_days=STALE_FRED_DAYS,
    )
    if broad_store.get("latest_price") is not None:
        return {
            "mode": "fred_broad_dollar",
            "label": BROAD_USD_ID,
            "current_close": broad_store.get("latest_price"),
            "as_of_date": broad_store.get("latest_date"),
            "source": broad_store.get("source") or "fred",
            "series_id": FRED_USD_DXY.series_id,
            "is_fallback": True,
            "fallback_note": (
                "Broad USD (DTWEXBGS) only — not substituted as ICE DX futures price."
            ),
            "freshness": broad_store.get("freshness"),
            "history": broad_store.get("history"),
            "confidence": "moderate",
        }

    fred = fred_series_block(
        FRED_USD_DXY.series_id,
        label=BROAD_USD_ID,
        obs_start=FRED_USD_DXY.obs_start,
        stale_after_days=STALE_FRED_DAYS,
        allow_live=allow_live,
    )
    if fred.get("latest_value") is not None:
        return {
            "mode": "fred_broad_dollar",
            "label": BROAD_USD_ID,
            "current_close": fred.get("latest_value"),
            "as_of_date": fred.get("latest_date"),
            "source": fred.get("source") or "fred",
            "series_id": FRED_USD_DXY.series_id,
            "is_fallback": True,
            "fallback_note": (
                "FRED DTWEXBGS Nominal Broad USD — not ICE U.S. Dollar Index futures."
            ),
            "freshness": fred.get("freshness"),
            "history": fred.get("history"),
            "confidence": "moderate",
        }

    return {
        "mode": "missing",
        "label": "USD / DXY price unavailable",
        "current_close": None,
        "as_of_date": None,
        "source": None,
        "series_id": None,
        "is_fallback": False,
        "fallback_note": "No price_store record and FRED DTWEXBGS unavailable.",
        "freshness": {"status": "missing", "as_of": None, "age_days": None},
        "history": None,
        "confidence": "none",
    }


def load_usd_cot_block(*, download: bool = False) -> dict[str, Any]:
    """Direct Dollar Index COT — legacy_cot_latest first, then CFTC code lookup."""
    from hptl.macro_hub.cot_snapshot import cot_block_for_instrument, cot_block_from_cftc_code

    block = cot_block_for_instrument(DX_INSTRUMENT_ID)
    if block.get("net") is not None and not block.get("error"):
        block = dict(block)
        block["mode"] = "direct_dxy_cot"
        block["label"] = USD_DIRECT_LABEL
        block["cftc_code"] = block.get("cftc_code") or COT_CFTC_USD_INDEX
        block["confidence"] = "high" if block.get("freshness", {}).get("status") == "fresh" else "moderate"
        return block

    cftc = cot_block_from_cftc_code(
        COT_CFTC_USD_INDEX,
        label="ICE U.S. Dollar Index Futures",
        download=download,
    )
    if cftc.get("net") is not None and not cftc.get("error"):
        out = dict(cftc)
        out["mode"] = "direct_dxy_cot"
        out["label"] = USD_DIRECT_LABEL
        out["confidence"] = "high" if out.get("freshness", {}).get("status") == "fresh" else "moderate"
        return out

    hub = _load_macro_hub_usd()
    if hub and isinstance(hub.get("cot"), dict) and hub["cot"].get("net") is not None:
        cot = hub["cot"]
        return {
            "mode": "macro_hub_cot",
            "label": USD_DIRECT_LABEL,
            "long": cot.get("long"),
            "short": cot.get("short"),
            "net": cot.get("net"),
            "weekly_net_change": cot.get("weekly_net_change"),
            "four_week_net_change": cot.get("four_week_net_change"),
            "open_interest": cot.get("open_interest"),
            "net_percentile_3y": cot.get("net_percentile_3y"),
            "long_percentile_3y": cot.get("long_percentile_3y"),
            "short_percentile_3y": cot.get("short_percentile_3y"),
            "oi_percentile_3y": cot.get("oi_percentile_3y"),
            "report_date": cot.get("report_date"),
            "source": cot.get("source") or "macro_hub_latest.json",
            "freshness": cot.get("freshness") or freshness_status(cot.get("report_date"), stale_after_days=STALE_COT_DAYS),
            "error": None,
            "cftc_code": COT_CFTC_USD_INDEX,
            "confidence": "moderate",
        }

    return {
        "mode": "missing",
        "label": "Direct DXY COT unavailable",
        "long": None,
        "short": None,
        "net": None,
        "weekly_net_change": None,
        "four_week_net_change": None,
        "open_interest": None,
        "net_percentile_3y": None,
        "long_percentile_3y": None,
        "short_percentile_3y": None,
        "oi_percentile_3y": None,
        "report_date": None,
        "source": None,
        "freshness": {"status": "missing", "as_of": None, "age_days": None},
        "error": "no_direct_dxy_cot",
        "cftc_code": COT_CFTC_USD_INDEX,
        "confidence": "none",
    }


def load_usd_valuation_block() -> dict[str, Any]:
    """USD macro inputs for FX valuation audit."""
    from hptl.fx.currency_rates import all_currency_rates

    rates = all_currency_rates()
    usd = rates.get("USD")
    spec = get_instrument(DX_INSTRUMENT_ID)
    hub = _load_macro_hub_usd() or {}
    treas = (hub.get("treasuries") if isinstance(hub, dict) else None) or {}

    if usd is None:
        return {
            "mode": "missing",
            "confidence": "none",
            "error": "USD currency rates unavailable",
        }

    rec = usd.as_dict()
    return {
        "mode": "fx_currency_rates",
        "confidence": rec.get("data_quality") or "moderate",
        "policy_rate": rec.get("policy_rate"),
        "policy_rate_as_of": rec.get("policy_rate_as_of"),
        "y2": rec.get("y2"),
        "y10": rec.get("y10"),
        "cpi_yoy": rec.get("cpi_yoy"),
        "real_yield": rec.get("real_yield"),
        "source": rec.get("source"),
        "stale_fields": rec.get("stale_fields") or [],
        "missing_fields": rec.get("missing_fields") or [],
        "dxy_context": {
            "dxy_price": hub.get("dxy_price"),
            "dxy_price_date": hub.get("dxy_price_date"),
            "dxy_source": hub.get("dxy_source"),
            "dxy_series_id": hub.get("dxy_series_id"),
            "is_fallback": hub.get("dxy_source") == "fred" or bool(hub.get("dxy_series_id") == "DTWEXBGS"),
            "dx_futures_price": hub.get("dx_futures_price"),
            "note": hub.get("dx_futures_note"),
        },
        "treasury_context": {
            "us_2y_yield": treas.get("us_2y_yield") if isinstance(treas, dict) else None,
            "us_10y_yield": treas.get("us_10y_yield") if isinstance(treas, dict) else None,
            "real_yield_10y": treas.get("real_yield_10y") if isinstance(treas, dict) else None,
        },
        "instrument_id": spec.id if spec else DX_INSTRUMENT_ID,
        "cot_market_code": spec.cot_market_code if spec else COT_CFTC_USD_INDEX,
    }


def build_usd_anchor_document(*, allow_live: bool = False, cot_download: bool = False) -> dict[str, Any]:
    """Full auditable USD/DXY snapshot for exports and FX engine."""
    price = load_usd_price_block(allow_live=allow_live)
    cot = load_usd_cot_block(download=cot_download)
    valuation = load_usd_valuation_block()

    direct_available = cot.get("mode") in {"direct_dxy_cot", "macro_hub_cot"} and cot.get("net") is not None
    price_available = price.get("current_close") is not None

    return {
        "instrument_id": DX_INSTRUMENT_ID,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "cftc_code": COT_CFTC_USD_INDEX,
        "registry": {
            "has_cot_mapping": True,
            "cot_market_code": COT_CFTC_USD_INDEX,
            "positioning_status": "cot_available",
            "asset_class": "fx",
            "subgroup": "usd_index",
            "is_macro_driver": True,
            "is_fx_anchor": True,
        },
        "price": price,
        "cot": cot,
        "valuation": valuation,
        "g10_synthetic_note": (
            "G10 synthetic USD is derived from inverse-weighted EUR/GBP/AUD/NZD and "
            "positive JPY/CHF/CAD COT legs — not ICE Dollar Index."
        ),
        "data_quality": {
            "direct_cot_available": direct_available,
            "price_available": price_available,
            "price_is_fallback": bool(price.get("is_fallback")),
            "overall_confidence": (
                "high"
                if direct_available and price_available and not price.get("is_fallback")
                else "moderate"
                if direct_available or price_available
                else "low"
            ),
        },
        "g10_cot_sources": {k: v.get("market") for k, v in COT_CURRENCY_SOURCES.items()},
    }


def write_usd_anchor_document(
    path: Path | None = None,
    public_path: Path | None = None,
    **kwargs: Any,
) -> Path:
    payload = build_usd_anchor_document(**kwargs)
    out = path or Path("data/processed/usd_anchor_latest.json")
    pub = public_path or Path("web-dashboard/public/data/usd_anchor_latest.json")
    text = json.dumps(payload, indent=2, ensure_ascii=False)
    out.parent.mkdir(parents=True, exist_ok=True)
    pub.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(text, encoding="utf-8")
    pub.write_text(text, encoding="utf-8")
    return out


def sync_usd_dxy_price_to_store(*, allow_live: bool = False) -> dict[str, Any]:
    """Persist labelled DXY/FRED proxy daily closes to price_store for DX instrument."""
    from hptl.prices.models import InstrumentPriceRecord, compute_range_52w
    from hptl.prices.price_store import load_instrument_record, write_instrument_record

    block = load_usd_price_block(allow_live=allow_live)
    if block.get("current_close") is None:
        return {"written": False, "reason": block.get("fallback_note") or "no_price"}

    hist = block.get("history") or {}
    daily_all = hist.get("daily_all") if isinstance(hist, dict) else None
    if not daily_all and isinstance(hist, dict):
        daily_all = (hist.get("windows") or {}).get("daily_all", {}).get("closes")
    bars: list[dict[str, Any]] = []
    for row in daily_all or []:
        if not isinstance(row, dict):
            continue
        dt = row.get("date")
        close = row.get("close")
        if dt is None or close is None:
            continue
        try:
            bars.append({"date": str(dt)[:10], "close": float(close)})
        except (TypeError, ValueError):
            continue
    bars.sort(key=lambda x: x["date"])

    record: InstrumentPriceRecord = {
        "instrument_id": DX_INSTRUMENT_ID,
        "price": {
            "mid": float(block["current_close"]),
            "as_of": block.get("as_of_date"),
        },
        "daily": bars,
        "weekly": [],
        "range_52w": compute_range_52w(bars),
        "price_scale": {
            "source": block.get("source"),
            "series_id": block.get("series_id"),
            "is_fallback": block.get("is_fallback"),
            "fallback_note": block.get("fallback_note"),
            "mode": block.get("mode"),
            "label": block.get("label"),
        },
    }
    write_instrument_record(record, fetched_via=f"usd_anchor:{block.get('mode')}")
    return {
        "written": True,
        "bars": len(bars),
        "as_of": block.get("as_of_date"),
        "mode": block.get("mode"),
        "is_fallback": block.get("is_fallback"),
    }

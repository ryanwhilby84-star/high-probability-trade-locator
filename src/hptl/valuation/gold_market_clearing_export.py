"""Publish Gold market-clearing valuation for the dashboard.

Adapts engine outputs only — does not fabricate fair values on solver failure.
"""

from __future__ import annotations

import csv
import json
import math
from pathlib import Path
from typing import Any

from hptl.config import PROJECT_ROOT
from datetime import date

from hptl.data_sources.wgc_gdt_xlsx_ingest import KNOWN_PUBLICATION_DATES
from hptl.valuation.gold_focused_macro_valuation import _build_gold_weekly
from hptl.valuation.gold_market_clearing_valuation import (
    AUDIT_DIR,
    CACHE_PATH,
    HISTORY_CSV,
    JSON_OUT,
    MODEL_ID,
    run_gold_market_clearing_valuation,
    write_outputs,
    _classify_deviation,
)

DATA_OUT = PROJECT_ROOT / "data" / "gold_valuation_latest.json"
PUBLIC_OUT = PROJECT_ROOT / "web-dashboard" / "public" / "data" / "gold_valuation_latest.json"
DIST_OUT = PROJECT_ROOT / "web-dashboard" / "dist" / "data" / "gold_valuation_latest.json"

BUCKET_LABELS = {
    "materially_undervalued": "Materially undervalued",
    "undervalued": "Undervalued",
    "near_fair_value": "Near fair",
    "near_fair": "Near fair",
    "overvalued": "Overvalued",
    "materially_overvalued": "Materially overvalued",
}


def _num(v: Any) -> float | None:
    try:
        if v is None or v == "":
            return None
        n = float(v)
        return n if math.isfinite(n) else None
    except (TypeError, ValueError):
        return None


def _bool(v: Any) -> bool:
    if isinstance(v, bool):
        return v
    return str(v).lower() in {"1", "true", "yes"}


def _dev_pct(price: float | None, fv: float | None) -> float | None:
    if price is None or fv is None or fv <= 0:
        return None
    return 100.0 * (price - fv) / fv


def _scale_from_deviation(dev: float | None) -> dict[str, Any]:
    if dev is None:
        return {"pct": 50, "band": "MODEL INVALID", "deviation_pct": None}
    pct = max(0.0, min(100.0, 50.0 + float(dev) * 2.0))
    bucket = _classify_deviation(float(dev))
    return {
        "pct": round(pct, 1),
        "band": BUCKET_LABELS.get(bucket, bucket),
        "deviation_pct": round(float(dev), 3),
        "bucket": bucket,
    }


def _load_history_rows(payload: dict[str, Any] | None = None) -> list[dict[str, Any]]:
    hist = list((payload or {}).get("_best_history") or [])
    if hist:
        return hist
    if not HISTORY_CSV.exists():
        return []
    with HISTORY_CSV.open(encoding="utf-8", newline="") as fh:
        return list(csv.DictReader(fh))


def _history_series(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out = []
    for r in rows:
        spot = _num(r.get("gold_price") or r.get("market_price") or r.get("reference_price"))
        fv = _num(r.get("fair_value") or r.get("displayed_fair_value"))
        status = str(r.get("solver_status") or ("OK" if _bool(r.get("solve_ok")) else "SOLVER_INVALID"))
        solve_ok = status == "OK" and fv is not None
        # Always recompute displayed deviation from the same FV (never trust stale 0%)
        dev = _dev_pct(spot, fv) if solve_ok else None
        bucket = _classify_deviation(dev) if dev is not None else None
        if spot is None:
            continue
        out.append(
            {
                "quarter": str(r.get("date") or "")[:10],
                "date": str(r.get("date") or "")[:10],
                "market_price": spot,
                "spot_price": spot,
                "fair_value": fv if solve_ok else None,
                "raw_fair_value": _num(r.get("raw_fair_value")),
                "deviation_pct": round(dev, 3) if dev is not None else None,
                "bucket": bucket,
                "total_demand": _num(r.get("total_demand") or r.get("D0")),
                "total_supply": _num(r.get("total_supply") or r.get("S0")),
                "imbalance": _num(r.get("imbalance") or r.get("net_imbalance")),
                "net_elasticity": _num(r.get("net_elasticity")),
                "raw_delta_log_price": _num(r.get("raw_delta_log_price")),
                "bounded_delta_log_price": _num(r.get("bounded_delta_log_price")),
                "delta_log_price": _num(r.get("delta_log_price") or r.get("raw_delta_log_price"))
                if solve_ok
                else None,
                "bound_hit": _bool(r.get("bound_hit")),
                "solve_ok": solve_ok,
                "solver_status": status,
                "publication_date": str(
                    r.get("publication_date") or r.get("usable_date") or ""
                )[:10]
                or None,
                "jewellery": _num(r.get("demand_jewellery") or r.get("observed_jewellery")),
                "technology": _num(r.get("demand_technology") or r.get("observed_technology")),
                "bar_coin": _num(r.get("demand_bar_coin") or r.get("observed_bar_coin")),
                "etf": _num(r.get("demand_etf") or r.get("observed_etf")),
                "central_bank": _num(r.get("demand_cb") or r.get("observed_cb")),
                "mine": _num(r.get("supply_mine") or r.get("observed_mine")),
                "recycling": _num(r.get("supply_recycling") or r.get("observed_recycling")),
                "producer_hedging": _num(r.get("supply_hedging") or r.get("observed_hedging")),
                "otc_other": _num(r.get("observed_otc_other")),
            }
        )
    return out


def _publication_date_for(row: dict[str, Any]) -> str | None:
    q = str(row.get("quarter") or row.get("date") or "")[:10]
    known = KNOWN_PUBLICATION_DATES.get(q)
    if known:
        return known
    pub = str(row.get("publication_date") or row.get("usable_date") or "")[:10]
    return pub or None


def build_display_chart_series(
    history: list[dict[str, Any]],
    *,
    asof: str | None = None,
) -> list[dict[str, Any]]:
    """Canonical weekly Gold price LEFT ASOF JOIN valid quarterly FV by publication date.

    Display-only. Does not alter valuation mathematics.
    """
    weeks, prices, _meta = _build_gold_weekly(start="2000-01-01")
    today = (asof or date.today().isoformat())[:10]

    # Valid valuations sorted by publication date (never before publication)
    pubs: list[dict[str, Any]] = []
    for h in history:
        if not h.get("solve_ok") or h.get("fair_value") is None:
            continue
        pub = _publication_date_for(h)
        if not pub or pub > today:
            continue
        pubs.append(
            {
                "pub": pub,
                "quarter": h.get("quarter") or h.get("date"),
                "fair_value": float(h["fair_value"]),
                "solver_status": h.get("solver_status") or "OK",
            }
        )
    pubs.sort(key=lambda r: r["pub"])

    out: list[dict[str, Any]] = []
    j = -1
    for d, px in zip(weeks, prices):
        d10 = str(d)[:10]
        if d10 > today:
            break
        advanced = False
        while j + 1 < len(pubs) and pubs[j + 1]["pub"] <= d10:
            j += 1
            advanced = True
        if j < 0:
            fv = None
            q = None
            pub = None
            status = None
            carried = False
            bucket = None
            dev = None
            observation = None
        else:
            fv = pubs[j]["fair_value"]
            q = pubs[j]["quarter"]
            pub = pubs[j]["pub"]
            status = pubs[j]["solver_status"]
            # Observation marker on the first weekly bar on/after publication
            carried = not advanced
            observation = round(fv, 3) if advanced else None
            dev = _dev_pct(float(px), fv)
            bucket = _classify_deviation(dev) if dev is not None else None
        out.append(
            {
                "date": d10,
                "market_price": round(float(px), 3),
                "fair_value": round(fv, 3) if fv is not None else None,
                "fair_value_observation": observation,
                "fair_value_quarter": q,
                "fair_value_publication_date": pub,
                "deviation_pct": round(dev, 3) if dev is not None else None,
                "valuation_bucket": bucket,
                "solver_status": status,
                "is_live_price": False,
                "is_carried_forward": carried,
            }
        )
    return out


def _latest_valid(history: list[dict[str, Any]], *, asof: str | None = None) -> dict[str, Any] | None:
    today = (asof or date.today().isoformat())[:10]
    best = None
    best_pub = ""
    for h in history:
        if not h.get("solve_ok") or h.get("fair_value") is None:
            continue
        pub = _publication_date_for(h) or ""
        if not pub or pub > today:
            continue
        if best is None or pub >= best_pub:
            best = h
            best_pub = pub
    return best


def _driver_summary(tip: dict[str, Any], valid: bool) -> str:
    if not valid:
        return (
            "MODEL INVALID — market-clearing solver could not produce a stable fair value "
            "for the latest quarter. Sector tonnes are shown without a fabricated valuation."
        )
    imb = _num(tip.get("net_imbalance_tonnes") or tip.get("imbalance"))
    jew = _num(tip.get("jewellery_or_fabrication"))
    lines = []
    if jew is not None:
        lines.append(
            "Jewellery demand is supporting fair value."
            if jew >= 350
            else "Jewellery demand is soft relative to recent history."
        )
    lines.append("Mine supply remains a core supply-side driver.")
    if imb is not None:
        if imb > 20:
            lines.append("Net market imbalance favours higher equilibrium prices.")
        elif imb < -20:
            lines.append("Excess supply is pressuring equilibrium value lower.")
        else:
            lines.append("Net market imbalance is near balance.")
    return " ".join(lines)


def build_gold_valuation_document(
    payload: dict[str, Any] | None = None,
    *,
    rerun: bool = False,
) -> dict[str, Any]:
    if payload is None:
        if rerun or not JSON_OUT.exists():
            payload = run_gold_market_clearing_valuation()
            if payload.get("ok"):
                write_outputs(payload)
        else:
            payload = json.loads(JSON_OUT.read_text(encoding="utf-8"))

    tip = dict(payload.get("tip") or {})
    hist_rows = _load_history_rows(payload)
    history = _history_series(hist_rows)
    hist_tip = history[-1] if history else {}
    display_chart = build_display_chart_series(history)
    latest_valid = _latest_valid(history)

    status = str(tip.get("solver_status") or hist_tip.get("solver_status") or "")
    if not status:
        status = "OK" if tip.get("solve_ok") and tip.get("fair_value") is not None else "SOLVER_INVALID"
    latest_quarter_valid = status == "OK" and tip.get("fair_value") is not None
    # Display cards use latest *valid* published FV (may be earlier than tip quarter)
    model_valid = latest_valid is not None and latest_valid.get("fair_value") is not None

    model_anchor = _num(tip.get("market_price") or hist_tip.get("market_price"))
    fv = _num(latest_valid.get("fair_value")) if latest_valid else None
    fv_quarter = str((latest_valid or {}).get("quarter") or "")[:10] or None
    fv_pub = _publication_date_for(latest_valid) if latest_valid else None
    # Tip-quarter status kept separately from display FV
    tip_dev = None  # live deviation computed in UI vs live spot
    bucket = _classify_deviation(_dev_pct(model_anchor, fv)) if (fv and model_anchor) else (
        _classify_deviation(_num(latest_valid.get("deviation_pct"))) if latest_valid else None
    )
    scale = _scale_from_deviation(
        _num(latest_valid.get("deviation_pct")) if latest_valid else None
    )

    latest_q = str(tip.get("date") or hist_tip.get("quarter") or "")[:10]
    pub = (
        KNOWN_PUBLICATION_DATES.get(latest_q)
        or tip.get("publication_date")
        or hist_tip.get("publication_date")
    )

    # Sector panel: prefer modelled parts; fall back to observed WGC tonnes
    demand = {
        "jewellery": _num(tip.get("jewellery_or_fabrication") or hist_tip.get("jewellery")),
        "technology": _num(tip.get("technology") or hist_tip.get("technology")),
        "bar_coin": _num(tip.get("bar_coin") or hist_tip.get("bar_coin")),
        "etf": _num(tip.get("etf_investment") or hist_tip.get("etf")),
        "central_bank": _num(tip.get("central_bank") or hist_tip.get("central_bank")),
        "otc_other": _num(tip.get("otc_other") or hist_tip.get("otc_other")),
    }
    if demand["bar_coin"] is None and demand["etf"] is None and tip.get("investment_aggregate") is not None:
        demand["investment"] = _num(tip.get("investment_aggregate"))
    supply = {
        "mine": _num(tip.get("mine_supply") or hist_tip.get("mine")),
        "recycling": _num(tip.get("recycling_supply") or hist_tip.get("recycling")),
        "producer_hedging": _num(tip.get("producer_hedging") or hist_tip.get("producer_hedging")),
    }
    total_demand = _num(tip.get("total_demand")) or _num(hist_tip.get("total_demand"))
    total_supply = _num(tip.get("total_supply")) or _num(hist_tip.get("total_supply"))
    imbalance = _num(tip.get("net_imbalance_tonnes") or tip.get("imbalance") or hist_tip.get("imbalance"))
    raw_delta = _num(tip.get("raw_delta_log_price") or hist_tip.get("raw_delta_log_price"))

    gdt_meta = {}
    if CACHE_PATH.exists():
        try:
            gdt_meta = json.loads(CACHE_PATH.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            gdt_meta = {}

    panel = payload.get("panel") or {}
    missing = payload.get("missing_quarter_audit") or []
    n_valid_hist = sum(1 for h in history if h.get("solve_ok"))
    summary = _driver_summary(tip, latest_quarter_valid)
    implied_pct = (
        (math.exp(raw_delta) - 1.0) * 100.0
        if raw_delta is not None and latest_quarter_valid
        else None
    )

    instrument = {
        "market": "Gold",
        "wired": True,
        "model_valid": model_valid,
        "latest_quarter_valid": latest_quarter_valid,
        "solver_status": status,
        "latest_model_quarter_status": status,
        "model_id": payload.get("model_id") or MODEL_ID,
        "active_model": payload.get("model_id") or MODEL_ID,
        "valuation_pillar": "gold_market_clearing",
        "model_anchor_price": model_anchor,
        # spot_price kept as model-quarter anchor only — UI must use live feed for cards
        "spot_price": model_anchor,
        "fair_value": fv,
        "latest_valid_fair_value": fv,
        "latest_valid_quarter": fv_quarter,
        "latest_valid_publication_date": fv_pub,
        "latest_valid_deviation_pct": _num((latest_valid or {}).get("deviation_pct")),
        "raw_fair_value": _num(tip.get("raw_fair_value") or hist_tip.get("raw_fair_value")),
        "deviation_pct": None,  # UI computes live vs latest_valid_fair_value
        "premium_discount": None,
        "valuation_bucket": bucket,
        "valuation_bucket_label": BUCKET_LABELS.get(
            str(bucket), "No valid fair value" if not model_valid else "—"
        ),
        "valuation_bias": BUCKET_LABELS.get(str(bucket), "—"),
        "valuation_state": BUCKET_LABELS.get(str(bucket), "—"),
        "total_demand": total_demand,
        "total_supply": total_supply,
        "net_imbalance_tonnes": imbalance,
        "demand_elasticity": _num(tip.get("demand_elasticity")),
        "supply_elasticity": _num(tip.get("supply_elasticity")),
        "net_elasticity": _num(tip.get("net_elasticity") or hist_tip.get("net_elasticity")),
        "raw_delta_log_price": raw_delta,
        "bounded_delta_log_price": _num(tip.get("bounded_delta_log_price")),
        "implied_dlog_price": raw_delta if latest_quarter_valid else None,
        "implied_price_change_pct": round(implied_pct, 3) if implied_pct is not None else None,
        "bound_hit": bool(tip.get("bound_hit") or hist_tip.get("bound_hit")),
        "market_quarter": latest_q or None,
        "publication_date": pub,
        "as_of_week": tip.get("publication_date") or hist_tip.get("publication_date"),
        "n_historical_quarters": len(history),
        "n_valid_historical_quarters": n_valid_hist,
        "n_display_points": len(display_chart),
        "panel_quarters": panel.get("n_quarters") or gdt_meta.get("n_quarters"),
        "gdt_quarters_loaded": panel.get("gdt_quarters_loaded") or gdt_meta.get("n_quarters"),
        "best_stage": tip.get("stage") or payload.get("best_stage"),
        "equation": payload.get("equation"),
        "scale": scale,
        "market_contributions": {
            "unit": "tonnes",
            "demand": demand,
            "supply": supply,
            "total_demand": total_demand,
            "total_supply": total_supply,
            "net_imbalance_tonnes": imbalance,
            "implied_dlog_price": raw_delta if latest_quarter_valid else None,
            "implied_price_change_pct": round(implied_pct, 3) if implied_pct is not None else None,
            "fair_value": fv,
        },
        "history": history,
        "display_chart": display_chart,
        "summary_text": summary,
        "driver_summary": summary,
        "model_note": (
            "Display joins canonical weekly Gold prices to valid quarterly FV by publication date. "
            "Latest tip quarter may be invalid without erasing earlier valid valuations. "
            "Live premium/discount = 100×(live_price − latest_valid_FV)/latest_valid_FV."
        ),
        "data_freshness": {
            "gdt_source": gdt_meta.get("source") or panel.get("source"),
            "gdt_earliest": gdt_meta.get("earliest") or panel.get("start"),
            "gdt_latest": gdt_meta.get("latest") or panel.get("end"),
            "gdt_quarters": gdt_meta.get("n_quarters"),
            "panel_quarters": panel.get("n_quarters"),
            "excluded_quarters": panel.get("n_excluded") or len(missing),
            "latest_publication_date": pub,
            "latest_market_quarter": latest_q,
            "latest_valid_quarter": fv_quarter,
            "latest_valid_publication_date": fv_pub,
            "history_points": len(history),
            "valid_history_points": n_valid_hist,
            "display_points": len(display_chart),
            "generated_at": payload.get("generated_at"),
        },
        "missing_quarter_audit": missing,
    }

    return {
        "version": "gold_market_clearing_dashboard_v2",
        "generated_at": payload.get("generated_at"),
        "engine": payload.get("model_id") or MODEL_ID,
        "market": "Gold",
        "active_model": payload.get("model_id") or MODEL_ID,
        "model_valid": model_valid,
        "solver_status": status,
        "research_only": False,
        "legacy_models_retired": [
            "metals_real_yield_v1",
            "dxy_only_valuation",
            "legacy_gold_fair_value",
        ],
        "equation": payload.get("equation"),
        "verdict": payload.get("verdict"),
        "best_stage": payload.get("best_stage"),
        "summary": {
            "wired": True,
            "headline": (
                "Gold market-clearing fair value"
                if model_valid
                else "MODEL INVALID — no fabricated fair value"
            ),
            "active_model": instrument["active_model"],
            "n_historical_quarters": len(history),
            "n_valid_historical_quarters": n_valid_hist,
        },
        "instrument": instrument,
        "tip": tip,
        "audit_dir": str(AUDIT_DIR.relative_to(PROJECT_ROOT)).replace("\\", "/"),
    }


def write_gold_valuation_exports(
    *,
    rerun: bool = True,
    payload: dict[str, Any] | None = None,
) -> dict[str, Path]:
    doc = build_gold_valuation_document(payload, rerun=rerun)
    text = json.dumps(doc, indent=2, ensure_ascii=False)
    written: dict[str, Path] = {}
    for key, path in (("data", DATA_OUT), ("public", PUBLIC_OUT), ("dist", DIST_OUT)):
        if path == DIST_OUT and not path.parent.parent.exists():
            continue
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text, encoding="utf-8")
        written[key] = path
    return written

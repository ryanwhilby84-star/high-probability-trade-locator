"""Agriculture fundamental valuation — separate from FX valuation.

Uses native balance-sheet anchors (USDA WASDE / PSD stocks-to-use) when present on disk.
Does not substitute location, FX carry, or undifferentiated price percentile for valuation.
"""

from __future__ import annotations

import json
import math
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from hptl.config import PROJECT_ROOT
from hptl.prices.price_store import load_instrument_record, load_price_store
from hptl.valuation.engine import BIAS_UNAVAILABLE

MODEL_ID_REGRESSION = "agri_stu_regression_v1"
MODEL_ID_PERCENTILE = "agri_stu_percentile_v1"
MIN_OBS_REGRESSION = 24
MIN_OBS_PERCENTILE = 12
MIN_R2 = 0.08

CONFIG_PATH = PROJECT_ROOT / "data" / "config" / "agri_valuation_sources.json"
BALANCE_SHEET_DIR = PROJECT_ROOT / "data" / "processed" / "agri_balance_sheet"
MACRO_CACHE_DIR = PROJECT_ROOT / "data" / "macro_cache"
PUBLIC_PRICES = PROJECT_ROOT / "web-dashboard" / "public" / "data" / "prices_latest.json"
LEGACY_COT = PROJECT_ROOT / "data" / "legacy_cot_latest.json"

PRIORITY_MARKETS: tuple[str, ...] = (
    "Soybeans",
    "Wheat",
    "Corn",
    "Sugar",
    "Cotton",
)
OPTIONAL_MARKETS: tuple[str, ...] = ("Coffee", "Cocoa")
AGRI_VALUATION_MARKETS: tuple[str, ...] = PRIORITY_MARKETS + OPTIONAL_MARKETS


def is_agri_valuation_market(market: str) -> bool:
    return market in AGRI_VALUATION_MARKETS


def _load_config() -> dict[str, Any]:
    if not CONFIG_PATH.exists():
        return {"instruments": {}}
    return json.loads(CONFIG_PATH.read_text(encoding="utf-8"))


def _finite(v: Any) -> float | None:
    try:
        x = float(v)
        return x if math.isfinite(x) else None
    except (TypeError, ValueError):
        return None


def _bias_from_deviation(dev_pct: float | None) -> str:
    if dev_pct is None or not math.isfinite(dev_pct):
        return BIAS_UNAVAILABLE
    if dev_pct <= -5.0:
        return "Undervalued"
    if dev_pct >= 5.0:
        return "Overvalued"
    return "Fair Value"


def _confidence(r2: float | None, n: int, model_id: str) -> str:
    if model_id == MODEL_ID_REGRESSION and r2 is not None and r2 >= 0.25 and n >= MIN_OBS_REGRESSION:
        return "high"
    if n >= MIN_OBS_REGRESSION:
        return "medium"
    if n >= MIN_OBS_PERCENTILE:
        return "low"
    return "none"


@dataclass(frozen=True)
class BalanceSheetPoint:
    date: str
    stocks_to_use: float
    ending_stocks: float | None = None
    total_use: float | None = None
    production: float | None = None
    exports: float | None = None


def _balance_sheet_path(market: str) -> Path:
    cfg = (_load_config().get("instruments") or {}).get(market) or {}
    fname = cfg.get("balance_sheet_file") or f"{market.replace(' ', '_')}.json"
    return BALANCE_SHEET_DIR / str(fname)


def load_balance_sheet(market: str) -> tuple[list[BalanceSheetPoint], str | None]:
    path = _balance_sheet_path(market)
    if not path.exists():
        return [], None
    try:
        doc = json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return [], str(path)

    rows = doc.get("series") or doc.get("observations") or []
    out: list[BalanceSheetPoint] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        date = str(row.get("date") or row.get("report_date") or "")[:10]
        stu = _finite(row.get("stocks_to_use"))
        if stu is None:
            es = _finite(row.get("ending_stocks"))
            use = _finite(row.get("total_use") or row.get("consumption") or row.get("domestic_use"))
            if es is not None and use is not None and use > 0:
                stu = es / use
        if stu is None or not date:
            continue
        out.append(
            BalanceSheetPoint(
                date=date,
                stocks_to_use=stu,
                ending_stocks=_finite(row.get("ending_stocks")),
                total_use=_finite(row.get("total_use") or row.get("consumption")),
                production=_finite(row.get("production")),
                exports=_finite(row.get("exports")),
            )
        )
    out.sort(key=lambda p: p.date)
    return out, str(path)


def _spot_price(market: str) -> tuple[float | None, str, int]:
    """Current price from price_store or public aggregate."""
    rec = load_instrument_record(market)
    if rec:
        px = _finite((rec.get("price") or {}).get("mid"))
        if px is None:
            daily = rec.get("daily") or []
            if daily:
                px = _finite(daily[-1].get("close"))
        weekly = rec.get("weekly") or []
        depth = len(daily := rec.get("daily") or []) or len(weekly)
        src = (rec.get("price_scale") or {}).get("source") or "price_store"
        if px is not None:
            return px, src, depth

    if PUBLIC_PRICES.exists():
        doc = json.loads(PUBLIC_PRICES.read_text(encoding="utf-8"))
        inst = (doc.get("instruments") or {}).get(market) or {}
        daily = inst.get("daily") or []
        weekly = inst.get("weekly") or []
        bars = daily or weekly
        if bars:
            px = _finite(bars[-1].get("close"))
            src = (inst.get("price_scale") or {}).get("source") or "prices_latest"
            return px, src, len(bars)
    return None, "none", 0


def _price_on_date(market: str, target_date: str) -> float | None:
    rec = load_instrument_record(market)
    bars: list[dict[str, Any]] = []
    if rec:
        bars = rec.get("daily") or rec.get("weekly") or []
    elif PUBLIC_PRICES.exists():
        inst = json.loads(PUBLIC_PRICES.read_text(encoding="utf-8")).get("instruments", {}).get(market) or {}
        bars = inst.get("daily") or inst.get("weekly") or []
    if not bars:
        return None
    target = target_date[:10]
    best = None
    best_diff = 10**9
    for b in bars:
        d = str(b.get("date") or "")[:10]
        if not d:
            continue
        diff = abs(int(d.replace("-", "")) - int(target.replace("-", "")))
        if diff < best_diff:
            best_diff = diff
            best = _finite(b.get("close"))
    return best if best_diff <= 45 else None


def _align_price_stu(market: str, balance: list[BalanceSheetPoint]) -> list[tuple[float, float]]:
    pairs: list[tuple[float, float]] = []
    for pt in balance:
        px = _price_on_date(market, pt.date)
        if px is not None and px > 0:
            pairs.append((pt.stocks_to_use, px))
    return pairs


def _ols_slope_intercept(xs: list[float], ys: list[float]) -> tuple[float, float, float | None]:
    n = len(xs)
    if n < 3:
        return 0.0, 0.0, None
    mx = sum(xs) / n
    my = sum(ys) / n
    num = sum((x - mx) * (y - my) for x, y in zip(xs, ys))
    den = sum((x - mx) ** 2 for x in xs)
    if den == 0:
        return 0.0, my, None
    slope = num / den
    intercept = my - slope * mx
    ss_res = sum((y - (intercept + slope * x)) ** 2 for x, y in zip(xs, ys))
    ss_tot = sum((y - my) ** 2 for y in ys)
    r2 = 1.0 - ss_res / ss_tot if ss_tot > 0 else None
    return slope, intercept, r2


def _fair_from_stu_percentile(pairs: list[tuple[float, float]], current_stu: float) -> float | None:
    if len(pairs) < MIN_OBS_PERCENTILE:
        return None
    stus = [p[0] for p in pairs]
    below = sum(1 for s in stus if s < current_stu)
    pct = 100.0 * below / len(stus)
    # Low S/U historically -> higher prices (inverse relationship)
    target_pct = 100.0 - pct
    sorted_prices = sorted(p[1] for p in pairs)
    idx = int(round((target_pct / 100.0) * (len(sorted_prices) - 1)))
    idx = max(0, min(len(sorted_prices) - 1, idx))
    return sorted_prices[idx]


def discover_instrument_data(market: str) -> dict[str, Any]:
    """Inventory row for audit — what exists on disk for one ag market."""
    cfg = (_load_config().get("instruments") or {}).get(market) or {}
    bs, bs_path = load_balance_sheet(market)
    spot, price_src, price_depth = _spot_price(market)
    pairs = _align_price_stu(market, bs) if bs else []

    macro_price_files: list[str] = []
    if MACRO_CACHE_DIR.exists():
        for spec in cfg.get("price_sources") or []:
            if isinstance(spec, str) and spec.startswith("fred:"):
                sid = spec.split(":", 1)[1]
                hits = list(MACRO_CACHE_DIR.glob(f"{sid}__*.csv"))
                macro_price_files.extend(str(p.relative_to(PROJECT_ROOT)) for p in hits[:2])

    cot_available = False
    if LEGACY_COT.exists():
        cot = json.loads(LEGACY_COT.read_text(encoding="utf-8"))
        inst = (cot.get("instruments") or {}).get(market)
        cot_available = bool((inst or {}).get("groups", {}).get("commercials", {}).get("weeks"))

    has_bs = len(bs) > 0
    has_price = spot is not None
    aligned = len(pairs)

    if has_bs and aligned >= MIN_OBS_REGRESSION:
        model_type = "stu_price_regression"
        confidence = "medium"
    elif has_bs and aligned >= MIN_OBS_PERCENTILE:
        model_type = "stu_percentile_fair_value"
        confidence = "low"
    elif has_price:
        model_type = "blocked_no_balance_sheet"
        confidence = "none"
    else:
        model_type = "blocked_no_data"
        confidence = "none"

    missing: list[str] = []
    if not has_bs:
        missing.append(f"USDA WASDE/PSD balance sheet ({BALANCE_SHEET_DIR / (cfg.get('balance_sheet_file') or market + '.json')})")
    if not has_price:
        missing.append("canonical price history (price_store / prices_latest)")
    if has_bs and aligned < MIN_OBS_PERCENTILE:
        missing.append(f"aligned price+balance-sheet history (have {aligned}, need {MIN_OBS_PERCENTILE}+)")

    return {
        "market": market,
        "futures": cfg.get("futures"),
        "subgroup": cfg.get("subgroup"),
        "priority": cfg.get("priority"),
        "optional": cfg.get("optional", False),
        "balance_sheet_observations": len(bs),
        "balance_sheet_path": bs_path,
        "balance_sheet_on_disk": has_bs,
        "price_spot": spot,
        "price_source": price_src,
        "price_depth_bars": price_depth,
        "aligned_price_stu_pairs": aligned,
        "macro_price_cache_files": macro_price_files,
        "cot_commercial_available": cot_available,
        "recommended_model_type": model_type,
        "confidence": confidence,
        "data_missing": missing,
        "valuation_anchor_possible": has_bs and aligned >= MIN_OBS_PERCENTILE,
    }


def build_data_inventory() -> dict[str, Any]:
    rows = [discover_instrument_data(m) for m in AGRI_VALUATION_MARKETS]
    return {
        "generated_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
        "generated_from": "hptl.valuation.agri_fundamental_valuation",
        "balance_sheet_expected_source": "USDA WASDE / PSD",
        "balance_sheet_ingest_dir": str(BALANCE_SHEET_DIR.relative_to(PROJECT_ROOT)),
        "instruments": rows,
    }


def compute_agri_valuation(*, market: str, as_of_week: str | None = None) -> dict[str, Any]:
    """Fundamental ag valuation or explicit UNAVAILABLE — never FX logic."""
    cfg = (_load_config().get("instruments") or {}).get(market) or {}
    subgroup = cfg.get("subgroup") or ("grains" if market in PRIORITY_MARKETS[:3] else "softs")
    inv = discover_instrument_data(market)

    base: dict[str, Any] = {
        "market": market,
        "as_of_week": as_of_week,
        "asset_class": subgroup,
        "wired": False,
        "valuation_state": BIAS_UNAVAILABLE,
        "valuation_bias": BIAS_UNAVAILABLE,
        "valuation_score": None,
        "fair_value": None,
        "deviation_pct": None,
        "spot_price": inv.get("price_spot"),
        "confidence": "none",
        "model_id": MODEL_ID_REGRESSION if subgroup == "grains" else "softs_balance_sheet_v1",
        "valuation_phase": "Agri Phase 1",
        "driver_summary": "USDA WASDE/PSD stocks-to-use vs price (native ag anchor)",
        "data_depth": inv.get("aligned_price_stu_pairs") or inv.get("price_depth_bars") or 0,
        "price_source": inv.get("price_source"),
        "balance_sheet_observations": inv.get("balance_sheet_observations") or 0,
        "pass": False,
    }

    if not inv.get("balance_sheet_on_disk"):
        reason = (
            "Agri valuation unavailable — USDA WASDE/PSD balance sheet not on disk "
            f"(expected under {BALANCE_SHEET_DIR.relative_to(PROJECT_ROOT)}). "
            "Price history alone is insufficient for stocks-to-use valuation."
        )
        base["valuation_reason"] = reason
        base["unavailable_reason"] = reason
        return base

    spot = inv.get("price_spot")
    if spot is None:
        reason = "Agri valuation unavailable — no canonical price history for spot comparison."
        base["valuation_reason"] = reason
        base["unavailable_reason"] = reason
        return base

    bs, _ = load_balance_sheet(market)
    current = bs[-1]
    pairs = _align_price_stu(market, bs)
    if len(pairs) < MIN_OBS_PERCENTILE:
        reason = (
            f"Agri valuation unavailable — only {len(pairs)} aligned price/stocks-to-use "
            f"observations (need {MIN_OBS_PERCENTILE}+)."
        )
        base["valuation_reason"] = reason
        base["unavailable_reason"] = reason
        return base

    xs = [p[0] for p in pairs]
    ys = [p[1] for p in pairs]
    slope, intercept, r2 = _ols_slope_intercept(xs, ys)
    fair: float | None = None
    model_id = MODEL_ID_PERCENTILE
    model_note = "stocks-to-use percentile fair value (fallback)"

    if len(pairs) >= MIN_OBS_REGRESSION and r2 is not None and r2 >= MIN_R2:
        fair = intercept + slope * current.stocks_to_use
        model_id = MODEL_ID_REGRESSION
        model_note = f"price ~ stocks-to-use OLS (R²={r2:.3f})"
    else:
        fair = _fair_from_stu_percentile(pairs, current.stocks_to_use)

    if fair is None or fair <= 0:
        reason = "Agri valuation unavailable — fair value estimate failed (insufficient aligned history)."
        base["valuation_reason"] = reason
        base["unavailable_reason"] = reason
        return base

    dev_pct = round(100.0 * (spot - fair) / fair, 2)
    bias = _bias_from_deviation(dev_pct)
    conf = _confidence(r2 if model_id == MODEL_ID_REGRESSION else None, len(pairs), model_id)

    base.update(
        {
            "wired": True,
            "valuation_state": bias,
            "valuation_bias": bias,
            "valuation_score": round(max(-100.0, min(100.0, -dev_pct)), 1),
            "fair_value": round(fair, 4),
            "deviation_pct": dev_pct,
            "confidence": conf,
            "model_id": model_id,
            "model_note": model_note,
            "stocks_to_use": round(current.stocks_to_use, 4),
            "stocks_to_use_percentile": None,
            "valuation_reason": model_note,
            "pass": bias != BIAS_UNAVAILABLE,
        }
    )
    return base


def build_all_agri_valuations(*, as_of_week: str | None = None) -> dict[str, Any]:
    instruments: dict[str, Any] = {}
    wired = 0
    for market in AGRI_VALUATION_MARKETS:
        val = compute_agri_valuation(market=market, as_of_week=as_of_week)
        instruments[market] = val
        if val.get("wired"):
            wired += 1
    return {
        "version": 1,
        "generated_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
        "generated_from": "hptl.valuation.agri_fundamental_valuation",
        "pillar": "agri_valuation",
        "engine": "agri_fundamental_valuation",
        "note": "Native agriculture valuation — not FX carry. Requires USDA balance sheet on disk.",
        "summary": {
            "total_instruments": len(AGRI_VALUATION_MARKETS),
            "wired_count": wired,
            "unavailable_count": len(AGRI_VALUATION_MARKETS) - wired,
            "priority_markets": list(PRIORITY_MARKETS),
        },
        "instruments": instruments,
    }

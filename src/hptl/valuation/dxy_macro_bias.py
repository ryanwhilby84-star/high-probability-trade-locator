"""DXY / USD Index macro bias engine (bias-first; no forced fair value).

Keeps tradable/charted USD-index price (FRED DTWEXBGS proxy until ICE DX is wired),
ICE Dollar Index futures positioning (CFTC 098662), and Fed broad-dollar / Treasury
drivers as distinct concepts with explicit classifications.
"""

from __future__ import annotations

import math
import statistics
from datetime import datetime, timezone
from typing import Any, Literal

from hptl.macro.dollar_positioning import DXY_INSTRUMENT, score_dollar_positioning
from hptl.prices.canonical_timeline import load_canonical_timeline

Classification = Literal[
    "VALIDATED_VALUATION_DRIVER",
    "MACRO_BIAS_DRIVER",
    "EXPERIMENTAL",
    "INFORMATIONAL_ONLY",
    "REJECTED",
]

BiasLabel = Literal[
    "Bullish",
    "Moderately Bullish",
    "Neutral",
    "Moderately Bearish",
    "Bearish",
]

MARKET = DXY_INSTRUMENT
DXY_PRICE_SERIES = "DTWEXBGS"
SERIES = {
    "dgs2": ("DGS2", "US 2Y nominal yield"),
    "dgs10": ("DGS10", "US 10Y nominal yield"),
    "dfii10": ("DFII10", "US 10Y TIPS real yield"),
    "t10y2y": ("T10Y2Y", "2s10s Treasury curve"),
    "dff": ("DFF", "Effective federal funds rate"),
    "vix": ("VIXCLS", "VIX risk sentiment"),
    "dtwexbgs": ("DTWEXBGS", "Fed Trade-Weighted Broad USD Index"),
}

# Explicit classifications — bias engine only uses MACRO_BIAS_DRIVER (+ positioning).
DRIVER_CLASS: dict[str, Classification] = {
    "usd_index_price": "INFORMATIONAL_ONLY",  # charted proxy level, not ICE DX
    "fed_broad_usd": "MACRO_BIAS_DRIVER",
    "us_2y_yield": "MACRO_BIAS_DRIVER",
    "us_10y_yield": "MACRO_BIAS_DRIVER",
    "us_10y_real_yield": "MACRO_BIAS_DRIVER",
    "curve_2s10s": "MACRO_BIAS_DRIVER",
    "fed_funds": "MACRO_BIAS_DRIVER",
    "vix": "MACRO_BIAS_DRIVER",
    "ice_dx_cot": "MACRO_BIAS_DRIVER",
    "gold_relationship": "INFORMATIONAL_ONLY",
    "usd_broad_fair_value_v1": "EXPERIMENTAL",  # research OLS — not published FV
}


def _num(v: Any) -> float | None:
    if v is None or isinstance(v, bool):
        return None
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    return f if math.isfinite(f) else None


def _load_fred(series_id: str) -> dict[str, float]:
    from hptl.fx.fx_macro_history import load_fred_daily_map

    try:
        return load_fred_daily_map(series_id)
    except Exception:
        return {}


def _latest(series: dict[str, float]) -> tuple[str | None, float | None]:
    if not series:
        return None, None
    d = max(series.keys())
    return d, series[d]


def _change(series: dict[str, float], days: int = 5) -> float | None:
    if len(series) < 2:
        return None
    dates = sorted(series.keys())
    cur_d, cur_v = dates[-1], series[dates[-1]]
    # Approximate N trading days back
    idx = max(0, len(dates) - 1 - days)
    prev_v = series[dates[idx]]
    return round(cur_v - prev_v, 4)


def _zscore(series: dict[str, float], window: int = 252) -> float | None:
    if len(series) < 20:
        return None
    dates = sorted(series.keys())
    vals = [series[d] for d in dates[-window:]]
    if len(vals) < 20:
        return None
    mu = statistics.mean(vals)
    sd = statistics.pstdev(vals)
    if sd <= 1e-12:
        return 0.0
    return round((vals[-1] - mu) / sd, 3)


def _freshness(date: str | None, *, soft_days: int = 5, hard_days: int = 14) -> dict[str, Any]:
    if not date:
        return {"status": "MISSING", "age_days": None, "as_of": None}
    try:
        d = datetime.fromisoformat(str(date)[:10]).date()
    except ValueError:
        return {"status": "MISSING", "age_days": None, "as_of": date}
    age = max(0, (datetime.now(timezone.utc).date() - d).days)
    if age <= soft_days:
        status = "FRESH"
    elif age <= hard_days:
        status = "SOFT_STALE"
    else:
        status = "STALE"
    return {"status": status, "age_days": age, "as_of": str(date)[:10]}


def _driver_row(
    *,
    key: str,
    label: str,
    value: float | None,
    date: str | None,
    direction_for_usd: str,
    confidence: str,
    source: str,
    explanation: str,
    change: float | None = None,
    zscore: float | None = None,
    extra: dict[str, Any] | None = None,
) -> dict[str, Any]:
    fres = _freshness(date)
    return {
        "key": key,
        "label": label,
        "classification": DRIVER_CLASS.get(key, "EXPERIMENTAL"),
        "value": value,
        "change_5d": change,
        "zscore_1y": zscore,
        "date": date,
        "direction_for_usd": direction_for_usd,
        "confidence": confidence,
        "source": source,
        "freshness": fres,
        "explanation": explanation,
        **(extra or {}),
    }


def _usd_direction_from_z(z: float | None, *, invert: bool = False) -> str:
    if z is None:
        return "Neutral"
    score = -z if invert else z
    if score >= 0.75:
        return "Bullish"
    if score >= 0.25:
        return "Moderately Bullish"
    if score <= -0.75:
        return "Bearish"
    if score <= -0.25:
        return "Moderately Bearish"
    return "Neutral"


def _bias_score(direction: str) -> int:
    return {
        "Bullish": 2,
        "Moderately Bullish": 1,
        "Neutral": 0,
        "Moderately Bearish": -1,
        "Bearish": -2,
    }.get(direction, 0)


def _aggregate_bias(drivers: list[dict[str, Any]]) -> tuple[BiasLabel, str]:
    scored = [
        d
        for d in drivers
        if d.get("classification") == "MACRO_BIAS_DRIVER"
        and d.get("freshness", {}).get("status") != "MISSING"
        and d.get("direction_for_usd")
    ]
    if not scored:
        return "Neutral", "Insufficient fresh macro drivers for a bias call."

    total = sum(_bias_score(str(d["direction_for_usd"])) for d in scored)
    n = len(scored)
    avg = total / n
    if avg >= 1.0:
        label: BiasLabel = "Bullish"
    elif avg >= 0.35:
        label = "Moderately Bullish"
    elif avg <= -1.0:
        label = "Bearish"
    elif avg <= -0.35:
        label = "Moderately Bearish"
    else:
        label = "Neutral"

    parts = []
    for d in scored:
        if d["direction_for_usd"] != "Neutral":
            parts.append(f"{d['label']}: {d['direction_for_usd']}")
    why = "; ".join(parts[:6]) if parts else "Drivers clustered near neutral."
    return label, why


def _load_cot_snapshot() -> dict[str, Any] | None:
    """Load ICE DX futures positioning; prefer the freshest TFF or Legacy tip."""
    import json
    from pathlib import Path

    from hptl.config import PROJECT_ROOT

    candidates: list[dict[str, Any]] = []

    for rel in (
        "web-dashboard/public/data/tff_macro_positioning_latest.json",
        "data/processed/tff_macro_positioning_latest.json",
        "data/tff_macro_positioning_latest.json",
    ):
        path = PROJECT_ROOT / rel
        if not path.exists():
            continue
        try:
            doc = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            continue
        instruments = doc.get("instruments") or doc.get("by_instrument") or {}
        block = None
        if isinstance(instruments, dict):
            block = instruments.get(MARKET)
        elif isinstance(instruments, list):
            block = next(
                (
                    x
                    for x in instruments
                    if isinstance(x, dict)
                    and (
                        x.get("instrument_id") == MARKET
                        or x.get("market") == MARKET
                        or str(x.get("symbol") or "").upper() == "DXY"
                    )
                ),
                None,
            )
        if not isinstance(block, dict):
            continue
        weeks = (
            ((block.get("groups") or {}).get("leveraged") or {}).get("weeks")
            or block.get("weeks")
            or []
        )
        pos = block.get("positioning") if isinstance(block.get("positioning"), dict) else {}
        last = weeks[-1] if weeks and isinstance(weeks[-1], dict) else {}
        candidates.append(
            {
                "net": last.get("net") or last.get("leveraged_net") or pos.get("net"),
                "one_week_net_change": (
                    last.get("net_week_change")
                    or last.get("one_week_net_change")
                    or pos.get("one_week_net_change")
                ),
                "net_percentile_13w": (
                    last.get("net_percentile_13w")
                    or last.get("percentile")
                    or pos.get("net_percentile_13w")
                ),
                "open_interest": last.get("open_interest") or pos.get("open_interest"),
                "report_date": str(
                    last.get("report_date")
                    or last.get("date")
                    or block.get("report_date")
                    or pos.get("report_date")
                    or ""
                )[:10]
                or None,
                "weeks": weeks,
                "source_family": "tff",
            }
        )

    # Legacy noncommercials — often fresher than the short TFF widget history.
    try:
        path = PROJECT_ROOT / "web-dashboard/public/data/legacy_cot_latest.json"
        if path.exists():
            doc = json.loads(path.read_text(encoding="utf-8"))
            block = (doc.get("instruments") or {}).get(MARKET) or {}
            weeks = ((block.get("groups") or {}).get("noncommercials") or {}).get("weeks") or []
            if weeks:
                last = weeks[-1]
                candidates.append(
                    {
                        "net": last.get("net"),
                        "one_week_net_change": last.get("net_week_change"),
                        "net_percentile_13w": None,
                        "open_interest": last.get("open_interest"),
                        "report_date": str(last.get("report_date") or "")[:10] or None,
                        "weeks": weeks,
                        "source_family": "legacy_noncommercial",
                    }
                )
    except Exception:
        pass

    usable = [c for c in candidates if c.get("net") is not None and c.get("report_date")]
    if not usable:
        return None
    usable.sort(key=lambda c: c.get("report_date") or "")
    return usable[-1]


def _gold_driver() -> dict[str, Any]:
    tl = load_canonical_timeline("Gold")
    dxy = _load_fred(DXY_PRICE_SERIES)
    if not tl or not dxy:
        return _driver_row(
            key="gold_relationship",
            label="Gold vs USD",
            value=None,
            date=None,
            direction_for_usd="Neutral",
            confidence="none",
            source="price_store Gold + FRED DTWEXBGS",
            explanation="Gold/USD relationship unavailable.",
        )
    gold_closes = {str(d)[:10]: float(c) for d, c in tl.daily_closes() if c and float(c) > 0}
    # 20-day return correlation sign heuristic via relative performance
    g_chg = _change(gold_closes, 20)
    d_chg = _change(dxy, 20)
    if g_chg is None or d_chg is None:
        direction = "Neutral"
        expl = "Insufficient overlap for gold/USD relationship."
    elif g_chg > 0 and d_chg < 0:
        direction = "Bearish"
        expl = "Gold rising while broad USD soft — typical inverse risk/USD mix."
    elif g_chg < 0 and d_chg > 0:
        direction = "Bullish"
        expl = "Gold soft while broad USD firm — supports USD bias."
    else:
        direction = "Neutral"
        expl = "Gold and broad USD moving without a clear inverse confirmation."
    gd, gv = _latest(gold_closes)
    return _driver_row(
        key="gold_relationship",
        label="Gold vs USD",
        value=gv,
        date=gd,
        direction_for_usd=direction,
        confidence="low",
        source="price_store Gold + FRED DTWEXBGS",
        explanation=expl,
        change=g_chg,
    )


def build_dxy_macro_bias() -> dict[str, Any]:
    drivers: list[dict[str, Any]] = []

    # Charted USD-index proxy level
    dxy = _load_fred(DXY_PRICE_SERIES)
    dxy_d, dxy_v = _latest(dxy)
    dxy_z = _zscore(dxy)
    drivers.append(
        _driver_row(
            key="usd_index_price",
            label="Broad USD index level (chart proxy)",
            value=dxy_v,
            date=dxy_d,
            direction_for_usd=_usd_direction_from_z(dxy_z),
            confidence="medium" if dxy_v is not None else "none",
            source=f"FRED {DXY_PRICE_SERIES}",
            explanation=(
                "FRED Trade-Weighted Broad USD (DTWEXBGS). This is NOT ICE DX / Dixie futures. "
                "Used for workstation price history until an ICE DX feed is wired."
            ),
            change=_change(dxy, 5),
            zscore=dxy_z,
            extra={"is_ice_dx_futures": False, "is_proxy": True},
        )
    )
    drivers.append(
        _driver_row(
            key="fed_broad_usd",
            label="Fed Broad Trade-Weighted USD",
            value=dxy_v,
            date=dxy_d,
            direction_for_usd=_usd_direction_from_z(dxy_z),
            confidence="medium" if dxy_v is not None else "none",
            source=f"FRED {DXY_PRICE_SERIES}",
            explanation="Same DTWEXBGS series used as the macro breadth/trend driver.",
            change=_change(dxy, 5),
            zscore=dxy_z,
        )
    )

    for key, (sid, label) in (
        ("us_2y_yield", SERIES["dgs2"]),
        ("us_10y_yield", SERIES["dgs10"]),
        ("us_10y_real_yield", SERIES["dfii10"]),
        ("curve_2s10s", SERIES["t10y2y"]),
        ("fed_funds", SERIES["dff"]),
        ("vix", SERIES["vix"]),
    ):
        series = _load_fred(sid)
        d, v = _latest(series)
        z = _zscore(series)
        # Higher yields / real yields / funds → USD supportive; higher VIX often USD supportive (safe haven)
        invert = False
        if key == "curve_2s10s":
            # Steeper curve: mildly USD-mixed; treat rising curve as modestly supportive of risk, not USD
            invert = True
        direction = _usd_direction_from_z(z, invert=invert)
        if key == "vix":
            direction = _usd_direction_from_z(z, invert=False)
            expl = "Elevated VIX historically coincides with USD bid (safe-haven); low VIX removes that support."
        elif key == "curve_2s10s":
            expl = "2s10s level vs 1Y history. Deep inversion often reflects policy/recession risk rather than pure USD strength."
        elif key == "us_10y_real_yield":
            expl = "Higher real yields increase USD opportunity cost vs gold/FX — typically USD-supportive."
        elif key in ("us_2y_yield", "us_10y_yield", "fed_funds"):
            expl = "Higher US rates / policy rates typically support the USD via rate differentials."
        else:
            expl = label
        drivers.append(
            _driver_row(
                key=key,
                label=label,
                value=v,
                date=d,
                direction_for_usd=direction,
                confidence="medium" if v is not None else "none",
                source=f"FRED {sid}",
                explanation=expl,
                change=_change(series, 5),
                zscore=z,
            )
        )

    cot_snap = _load_cot_snapshot()
    # score_dollar_positioning expects the TFF export envelope, not a flat row.
    cot_envelope = None
    if cot_snap:
        cot_envelope = {
            "instruments": {
                MARKET: {
                    "available": True,
                    "latest": {
                        "net": cot_snap.get("net"),
                        "one_week_net_change": cot_snap.get("one_week_net_change"),
                        "net_percentile_13w": cot_snap.get("net_percentile_13w"),
                        "open_interest": cot_snap.get("open_interest"),
                        "date": cot_snap.get("report_date"),
                    },
                    "weeks": cot_snap.get("weeks") or [],
                }
            }
        }
    cot_score = score_dollar_positioning(cot_envelope)
    cot_dir = "Neutral"
    if cot_score.available:
        primary = cot_score.primary_label or ""
        if "Crowded Long" in primary or primary in ("Strong Dollar", "Dollar Strengthening"):
            # Crowded long = reversal risk (bearish for further USD upside)
            cot_dir = "Moderately Bearish" if "Crowded" in primary else "Moderately Bullish"
        elif "Crowded Short" in primary or primary in ("Weak Dollar", "Dollar Weakening"):
            cot_dir = "Moderately Bullish" if "Crowded" in primary else "Moderately Bearish"
    drivers.append(
        _driver_row(
            key="ice_dx_cot",
            label="ICE U.S. Dollar Index futures positioning (CFTC)",
            value=cot_score.net,
            date=cot_score.report_date,
            direction_for_usd=cot_dir,
            confidence="medium" if cot_score.available else "none",
            source="CFTC TFF/Legacy code 098662 (ICE U.S. Dollar Index)",
            explanation=cot_score.explanation,
            change=cot_score.one_week_net_change,
            extra={
                "primary_label": cot_score.primary_label,
                "score_labels": list(cot_score.score_labels),
                "cftc_code": "098662",
                "is_ice_dx_futures_positioning": True,
            },
        )
    )

    drivers.append(_gold_driver())

    # Experimental FV research pointer — never published as validated FV here.
    drivers.append(
        _driver_row(
            key="usd_broad_fair_value_v1",
            label="USD broad fair-value research (OLS)",
            value=None,
            date=None,
            direction_for_usd="Neutral",
            confidence="none",
            source="hptl.valuation.usd_broad_fair_value_v1",
            explanation=(
                "EXPERIMENTAL research model (log level ~ Fed funds + real yield + G10 2Y diffs). "
                "Not walk-forward validated for publication — workstation uses MACRO BIAS instead."
            ),
        )
    )

    bias, why = _aggregate_bias(drivers)
    treasuries = {
        "us_2y": next((d for d in drivers if d["key"] == "us_2y_yield"), None),
        "us_10y": next((d for d in drivers if d["key"] == "us_10y_yield"), None),
        "us_10y_real": next((d for d in drivers if d["key"] == "us_10y_real_yield"), None),
        "curve_2s10s": next((d for d in drivers if d["key"] == "curve_2s10s"), None),
    }

    return {
        "schema_version": 1,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "engine": "dxy_macro_bias_v1",
        "market": MARKET,
        "price_instrument": {
            "label": "FRED Trade-Weighted Broad USD (DTWEXBGS)",
            "series_id": DXY_PRICE_SERIES,
            "is_ice_dx_futures": False,
            "is_proxy": True,
            "note": "Charted price is the Fed broad USD index proxy, not ICE DX futures.",
            "latest": dxy_v,
            "as_of": dxy_d,
            "freshness": _freshness(dxy_d),
        },
        "positioning_instrument": {
            "label": "ICE U.S. Dollar Index futures",
            "cftc_code": "098662",
            "is_ice_dx_futures": True,
        },
        "macro_bias": bias,
        "macro_bias_summary": f"DXY macro bias: {bias}. {why}",
        "valuation_status": "NOT_YET_VALIDATED",
        "valuation_note": (
            "No robust walk-forward fair-value model is published for DXY yet. "
            "Workstation publishes MACRO BIAS with explicit driver classifications."
        ),
        "driver_classifications": DRIVER_CLASS,
        "drivers": drivers,
        "treasuries": treasuries,
        "lineage": {
            "source": "FRED + CFTC TFF/Legacy + price_store",
            "raw": "data/macro_cache + FRED API",
            "normalized": "load_fred_daily_map / legacy_cot / tff_macro",
            "transform": "z-score(1y), 5d change, dollar_positioning score",
            "engine": "dxy_macro_bias_v1",
            "export": "dxy_macro_bias_latest.json",
        },
    }

"""Read-only investigation: Freeze v1.0 on Yahoo DX-Y.NYB vs DX1!-adjacent continuous.

Does NOT modify Freeze v1.0 implementation.

Primary alt tape: Investing.com historical id 8827 (US Dollar Index / ICE DX levels),
the closest freely obtainable long continuous that tracks DX1!/ICE DX.
Also runs a calendar-aligned A/B (common trading dates only) so trading-day
alignment is identical across sources.
"""
from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path

from curl_cffi import requests as crequests

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from hptl.markets.usd_index_identity import ICE_DXY_ID  # noqa: E402
from hptl.seasonality_workstation.indexed_seasonality import (  # noqa: E402
    FREEZE_SMOOTH_WINDOW,
    METHOD_VERSION,
    build_freeze_v1_path,
    load_daily_closes_for_seasonality,
)

OUT = ROOT / "data" / "audits" / "seasonality_freeze_v1"
ASOF = "2026-07-23"
LOOKBACK = 15
SMOOTH = FREEZE_SMOOTH_WINDOW


def fetch_investing_daily(curr_id: int = 8827) -> list[tuple[str, float]]:
    url = f"https://api.investing.com/api/financialdata/historical/{curr_id}"
    r = crequests.get(
        url,
        params={
            "start-date": "2008-01-01",
            "end-date": "2026-07-24",
            "time-frame": "Daily",
            "add-missing-rows": "false",
        },
        headers={
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
            ),
            "domain-id": "www",
            "Referer": "https://www.investing.com/",
            "Accept": "application/json",
        },
        impersonate="chrome120",
        timeout=90,
    )
    r.raise_for_status()
    rows = (r.json().get("data") or [])
    out: dict[str, float] = {}
    for row in rows:
        d = str(row.get("rowDateTimestamp") or "")[:10]
        c = row.get("last_closeRaw")
        if d and c is not None:
            out[d] = float(c)
    return sorted(out.items())


def path_stats(path: list[float]) -> dict:
    mu = sum(path) / len(path)
    return {
        "n": len(path),
        "min": round(min(path), 4),
        "max": round(max(path), 4),
        "amplitude": round(max(path) - min(path), 4),
        "argmin_td": int(min(range(len(path)), key=lambda i: path[i]) + 1),
        "argmax_td": int(max(range(len(path)), key=lambda i: path[i]) + 1),
        "mean_abs": round(sum(abs(x) for x in path) / len(path), 4),
        "std": round((sum((x - mu) ** 2 for x in path) / len(path)) ** 0.5, 4),
    }


def td_month(td: int, d_len: int) -> str:
    months = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
    idx = min(11, int((td - 1) / max(d_len - 1, 1) * 12))
    return months[idx]


def major_landmarks(path: list[float]) -> dict:
    """Coarse seasonal landmarks (not micro-wiggle detectors)."""
    n = len(path)
    amin = min(range(n), key=lambda i: path[i])
    amax = max(range(n), key=lambda i: path[i])
    # Secondary: best peak in H1 and H2 excluding global max if distinct
    h1 = range(0, n // 2)
    h2 = range(n // 2, n)
    h1_max = max(h1, key=lambda i: path[i])
    h1_min = min(h1, key=lambda i: path[i])
    h2_max = max(h2, key=lambda i: path[i])
    h2_min = min(h2, key=lambda i: path[i])
    return {
        "global_trough": {
            "trading_day": amin + 1,
            "month": td_month(amin + 1, n),
            "value": round(path[amin], 4),
        },
        "global_peak": {
            "trading_day": amax + 1,
            "month": td_month(amax + 1, n),
            "value": round(path[amax], 4),
        },
        "h1_peak": {
            "trading_day": h1_max + 1,
            "month": td_month(h1_max + 1, n),
            "value": round(path[h1_max], 4),
        },
        "h1_trough": {
            "trading_day": h1_min + 1,
            "month": td_month(h1_min + 1, n),
            "value": round(path[h1_min], 4),
        },
        "h2_peak": {
            "trading_day": h2_max + 1,
            "month": td_month(h2_max + 1, n),
            "value": round(path[h2_max], 4),
        },
        "h2_trough": {
            "trading_day": h2_min + 1,
            "month": td_month(h2_min + 1, n),
            "value": round(path[h2_min], 4),
        },
    }


def path_compare(a: list[float], b: list[float]) -> dict:
    n = min(len(a), len(b))
    a2, b2 = a[:n], b[:n]
    ma, mb = sum(a2) / n, sum(b2) / n
    num = sum((a2[i] - ma) * (b2[i] - mb) for i in range(n))
    den = (sum((x - ma) ** 2 for x in a2) ** 0.5) * (sum((x - mb) ** 2 for x in b2) ** 0.5)
    corr = num / den if den else None
    mae = sum(abs(a2[i] - b2[i]) for i in range(n)) / n
    max_abs = max(abs(a2[i] - b2[i]) for i in range(n))
    return {
        "aligned_n": n,
        "corr": None if corr is None else round(corr, 4),
        "mae": round(mae, 4),
        "max_abs_diff": round(max_abs, 4),
    }


def series_overlap_stats(yahoo: list[tuple[str, float]], alt: list[tuple[str, float]]) -> dict:
    y, a = dict(yahoo), dict(alt)
    common = sorted(set(y) & set(a))
    diffs = [a[d] - y[d] for d in common]
    absdiffs = [abs(x) for x in diffs]
    ys = [y[d] for d in common]
    als = [a[d] for d in common]
    my, ma = sum(ys) / len(ys), sum(als) / len(als)
    num = sum((ys[i] - my) * (als[i] - ma) for i in range(len(common)))
    den = (sum((x - my) ** 2 for x in ys) ** 0.5) * (sum((x - ma) ** 2 for x in als) ** 0.5)
    return {
        "common_days": len(common),
        "first": common[0],
        "last": common[-1],
        "level_corr": round(num / den, 6) if den else None,
        "mean_abs_level_diff": round(sum(absdiffs) / len(absdiffs), 6),
        "max_abs_level_diff": round(max(absdiffs), 6),
        "mean_level_diff_alt_minus_yahoo": round(sum(diffs) / len(diffs), 6),
    }


def run_freeze(daily: list[tuple[str, float]]) -> dict:
    pack = build_freeze_v1_path(daily, asof=ASOF, lookback_years=LOOKBACK, smooth=SMOOTH)
    if not pack.get("available"):
        raise RuntimeError(f"Freeze unavailable: {pack}")
    path = list(pack["smoothed"])
    return {
        "asof": pack.get("asof"),
        "years_used": pack.get("sample_years"),
        "sample_size": pack.get("sample_size"),
        "d_len": pack.get("D") or len(path),
        "path": path,
        "stats": path_stats(path),
        "landmarks": major_landmarks(path),
    }


def landmark_deltas(a: dict, b: dict) -> dict:
    out = {}
    for k in a:
        out[k] = {
            "td_shift_alt_minus_yahoo": b[k]["trading_day"] - a[k]["trading_day"],
            "month_yahoo": a[k]["month"],
            "month_alt": b[k]["month"],
            "value_yahoo": a[k]["value"],
            "value_alt": b[k]["value"],
        }
    return out


def materiality(y: dict, a: dict, cmp_: dict, deltas: dict) -> dict:
    # Amplitude: relative difference > 15%
    amp_y, amp_a = y["stats"]["amplitude"], a["stats"]["amplitude"]
    amp_ratio = amp_a / amp_y if amp_y else None
    amp_material = amp_ratio is not None and abs(amp_ratio - 1.0) > 0.15

    # Turning points: global peak/trough or H1/H2 landmarks shift > 10 trading days
    # OR land in a different calendar month for global peak/trough
    key_shifts = [
        abs(deltas["global_peak"]["td_shift_alt_minus_yahoo"]),
        abs(deltas["global_trough"]["td_shift_alt_minus_yahoo"]),
        abs(deltas["h1_peak"]["td_shift_alt_minus_yahoo"]),
        abs(deltas["h1_trough"]["td_shift_alt_minus_yahoo"]),
        abs(deltas["h2_peak"]["td_shift_alt_minus_yahoo"]),
        abs(deltas["h2_trough"]["td_shift_alt_minus_yahoo"]),
    ]
    max_shift = max(key_shifts)
    month_flip = (
        deltas["global_peak"]["month_yahoo"] != deltas["global_peak"]["month_alt"]
        or deltas["global_trough"]["month_yahoo"] != deltas["global_trough"]["month_alt"]
    )
    tp_material = max_shift > 10 or month_flip

    # If seasonal paths are nearly identical, source swap does not explain reference gap
    near_identical = (
        cmp_["corr"] is not None
        and cmp_["corr"] >= 0.98
        and cmp_["mae"] <= 0.10 * amp_y
        and not tp_material
        and not amp_material
    )
    # Data-source attribution only if Freeze outputs materially diverge under identical maths
    data_attr = (tp_material or amp_material) and not near_identical

    if near_identical:
        interp = (
            "Under identical Freeze v1.0 maths, Yahoo continuous and this DX1!-adjacent "
            "continuous produce essentially the same seasonal path. Remaining gap vs the "
            "OTC/TradingView DX1! reference is NOT attributable to Yahoo vs this continuous "
            "tape (look to display window, as-of, OTC smoother, or a true DX1! back-adjustment "
            "tape not freely available here)."
        )
    elif data_attr:
        interp = (
            "Under identical Freeze v1.0 maths, Yahoo vs this DX1!-adjacent continuous "
            "materially diverge in seasonal shape → at least part of the reference gap is "
            "data-source / continuous-construction, not HPTL equation error."
        )
    else:
        interp = (
            "Paths differ modestly but below material thresholds for turning points/amplitude."
        )

    return {
        "seasonal_turning_points_materially_differ": bool(tp_material),
        "amplitude_materially_differs": bool(amp_material),
        "remaining_discrepancy_attributable_to_data_source_not_hptl_methodology": bool(data_attr),
        "max_landmark_shift_trading_days": max_shift,
        "amplitude_ratio_alt_over_yahoo": None if amp_ratio is None else round(amp_ratio, 4),
        "paths_near_identical": bool(near_identical),
        "interpretation": interp,
    }


def main() -> None:
    yahoo_all, yahoo_meta = load_daily_closes_for_seasonality(ICE_DXY_ID)
    if not yahoo_all:
        raise SystemExit(f"Yahoo/ICE DXY closes unavailable: {yahoo_meta}")
    yahoo = [(d, c) for d, c in yahoo_all if d <= ASOF]

    alt_all = fetch_investing_daily(8827)
    alt = [(d, c) for d, c in alt_all if d <= ASOF]
    overlap = series_overlap_stats(yahoo, alt)

    # Persist alt tape
    raw_path = OUT / "dx_institutional_continuous_closes.json"
    raw_path.write_text(
        json.dumps(
            {
                "source": "investing.com_api_financialdata_historical",
                "instrument_id": 8827,
                "label": "us_dollar_index",
                "proxy_for": (
                    "Closest freely obtainable ICE DX / USD Index continuous used as "
                    "DX1!-adjacent institutional tape (TradingView ICEUS:DX1! history "
                    "API/Stooq DX.F/Barchart DX*0 not available in this environment)."
                ),
                "asof_filter": ASOF,
                "n": len(alt),
                "overlap_vs_yahoo_dx_ynyb": overlap,
                "closes": [{"date": d, "close": c} for d, c in alt],
            },
            indent=2,
        ),
        encoding="utf-8",
    )

    # A) native calendars (each series' own trading days)
    y_native = run_freeze(yahoo)
    a_native = run_freeze(alt)
    cmp_native = path_compare(y_native["path"], a_native["path"])
    deltas_native = landmark_deltas(y_native["landmarks"], a_native["landmarks"])
    # Month labels not comparable across different d_len — use trading-day shifts only for native
    # Recompute materiality with month_flip disabled when d_len differs by >3
    verdict_native = materiality(y_native, a_native, cmp_native, deltas_native)
    if abs(y_native["d_len"] - a_native["d_len"]) > 3:
        # month buckets on unequal axes are not comparable; rely on td shifts only
        max_shift = verdict_native["max_landmark_shift_trading_days"]
        amp_material = verdict_native["amplitude_materially_differs"]
        tp_material = max_shift > 10
        near = (
            cmp_native["corr"] is not None
            and cmp_native["corr"] >= 0.98
            and cmp_native["mae"] <= 0.10 * y_native["stats"]["amplitude"]
            and not tp_material
            and not amp_material
        )
        data_attr = (tp_material or amp_material) and not near
        verdict_native = {
            **verdict_native,
            "seasonal_turning_points_materially_differ": bool(tp_material),
            "remaining_discrepancy_attributable_to_data_source_not_hptl_methodology": bool(data_attr),
            "paths_near_identical": bool(near),
            "note": "Native calendars differ in D; month labels ignored for materiality.",
            "interpretation": (
                "Under identical Freeze v1.0 maths on native calendars, Yahoo vs alt "
                + (
                    "materially diverge → data-source contribution possible."
                    if data_attr
                    else "do not materially diverge on landmarks/amplitude → remaining OTC/DX1! "
                    "reference gap is not explained by this source swap."
                )
            ),
        }

    # B) identical trading-day alignment: intersect dates, then Freeze
    ymap, amap = dict(yahoo), dict(alt)
    common = sorted(d for d in set(ymap) & set(amap) if d <= ASOF)
    y_aligned = [(d, ymap[d]) for d in common]
    a_aligned = [(d, amap[d]) for d in common]
    y_cal = run_freeze(y_aligned)
    a_cal = run_freeze(a_aligned)
    cmp_cal = path_compare(y_cal["path"], a_cal["path"])
    deltas_cal = landmark_deltas(y_cal["landmarks"], a_cal["landmarks"])
    verdict_cal = materiality(y_cal, a_cal, cmp_cal, deltas_cal)

    # Primary verdict = calendar-aligned (controls trading-day alignment exactly)
    primary = verdict_cal

    report = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "method_version": METHOD_VERSION,
        "freeze_unchanged": True,
        "controls": {
            "lookback_years": LOOKBACK,
            "asof": ASOF,
            "smooth": SMOOTH,
            "equations": "identical build_freeze_v1_path",
            "primary_comparison": "calendar_aligned_intersection",
        },
        "sources": {
            "yahoo": {
                "name": "Yahoo DX-Y.NYB via HPTL ICE DXY price store",
                "meta": yahoo_meta,
                "n_bars_to_asof": len(yahoo),
            },
            "institutional_adjacent": {
                "name": "Investing.com US Dollar Index continuous (id 8827)",
                "proxy_for": "TradingView ICEUS DX1! / institutional ICE DX continuous",
                "availability_note": (
                    "True TradingView DX1!, Stooq DX.F CSV, and Barchart DX*0 were not "
                    "retrievable here (auth/JS walls). 8827 is the closest long continuous "
                    "with ICE DX-scale levels and 0.9998 correlation to Yahoo DX-Y.NYB."
                ),
                "n_bars_to_asof": len(alt),
                "raw_overlap_vs_yahoo": overlap,
            },
        },
        "native_calendar_run": {
            "yahoo": {k: v for k, v in y_native.items() if k != "path"},
            "alt": {k: v for k, v in a_native.items() if k != "path"},
            "path_compare": cmp_native,
            "landmark_deltas": deltas_native,
            "verdict": verdict_native,
        },
        "calendar_aligned_run": {
            "common_trading_days": len(common),
            "yahoo": {k: v for k, v in y_cal.items() if k != "path"},
            "alt": {k: v for k, v in a_cal.items() if k != "path"},
            "path_compare": cmp_cal,
            "landmark_deltas": deltas_cal,
            "verdict": verdict_cal,
        },
        "verdict": primary,
        "artifacts": {"raw_alt_closes": str(raw_path.relative_to(ROOT)).replace("\\", "/")},
    }

    out_json = OUT / "freeze_v1_yahoo_vs_institutional_dx.json"
    out_json.write_text(json.dumps(report, indent=2), encoding="utf-8")

    md = [
        "# Freeze v1.0 — Yahoo continuous vs DX1!-adjacent continuous",
        "",
        "Freeze v1.0 **unchanged**. Same lookback / as-of / SMA(5) / equations.",
        "",
        "## Sources",
        "",
        "- Yahoo: `DX-Y.NYB` (HPTL ICE DXY store)",
        "- Alt: Investing.com id `8827` (closest free DX1!-adjacent continuous obtainable)",
        f"- Raw level overlap: corr `{overlap['level_corr']}`, mean |diff| `{overlap['mean_abs_level_diff']}`",
        "",
        "## Primary verdict (calendar-aligned intersection)",
        "",
        f"- Turning points materially differ: **{primary['seasonal_turning_points_materially_differ']}**",
        f"- Amplitude materially differs: **{primary['amplitude_materially_differs']}**",
        f"- Remaining discrepancy attributable to data source (not HPTL methodology): "
        f"**{primary['remaining_discrepancy_attributable_to_data_source_not_hptl_methodology']}**",
        "",
        primary["interpretation"],
        "",
        f"- Path corr: `{cmp_cal['corr']}`, MAE: `{cmp_cal['mae']}`, max|diff|: `{cmp_cal['max_abs_diff']}`",
        f"- Amplitude Yahoo/Alt: `{y_cal['stats']['amplitude']}` / `{a_cal['stats']['amplitude']}` "
        f"(ratio `{primary['amplitude_ratio_alt_over_yahoo']}`)",
        f"- Max landmark shift (trading days): `{primary['max_landmark_shift_trading_days']}`",
        "",
        "### Landmarks (aligned)",
        "",
        "```",
        json.dumps({"yahoo": y_cal["landmarks"], "alt": a_cal["landmarks"], "deltas": deltas_cal}, indent=2),
        "```",
        "",
        "## Native-calendar check (secondary)",
        "",
        f"- D yahoo/alt: `{y_native['d_len']}` / `{a_native['d_len']}`",
        f"- Path corr: `{cmp_native['corr']}`, MAE: `{cmp_native['mae']}`",
        f"- Verdict TP/amp/data-attr: "
        f"{verdict_native['seasonal_turning_points_materially_differ']} / "
        f"{verdict_native['amplitude_materially_differs']} / "
        f"{verdict_native['remaining_discrepancy_attributable_to_data_source_not_hptl_methodology']}",
        "",
    ]
    out_md = OUT / "freeze_v1_yahoo_vs_institutional_dx.md"
    out_md.write_text("\n".join(md), encoding="utf-8")
    print(json.dumps(primary, indent=2))
    print("wrote", out_json)


if __name__ == "__main__":
    main()

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd


ROOT = Path(__file__).resolve().parents[3]
PROCESSED = ROOT / "data" / "processed"

INPUT = PROCESSED / "gold_valuation_drivers_latest.csv"
LATEST_OUT = PROCESSED / "gold_institutional_fair_value_latest.json"
HISTORY_OUT = PROCESSED / "gold_institutional_fair_value_history.json"


DRIVER_CONFIG = {
    "silver_close": {"sign": 1.0, "label": "Silver"},
    "dxy": {"sign": -1.0, "label": "DXY"},
    "dxy_broad": {"sign": -1.0, "label": "DXY Broad"},
    "real_yield_or_tips": {"sign": -1.0, "label": "Real Yield / TIPS"},
    "real_yield_10y": {"sign": -1.0, "label": "10Y Real Yield"},
    "breakeven_inflation": {"sign": 1.0, "label": "Breakeven Inflation"},
    "breakeven_10y": {"sign": 1.0, "label": "10Y Breakeven"},
    "m2": {"sign": 1.0, "label": "M2"},
    "m2_money_supply": {"sign": 1.0, "label": "M2 Money Supply"},
    "central_bank_net_purchases": {"sign": 1.0, "label": "Central Bank Net Purchases"},
    "etf_holdings": {"sign": 1.0, "label": "ETF Holdings"},
    "gold_etf_holdings": {"sign": 1.0, "label": "Gold ETF Holdings"},
}


def zscore(series: pd.Series, window: int = 252) -> pd.Series:
    mean = series.rolling(window, min_periods=60).mean()
    std = series.rolling(window, min_periods=60).std()
    return (series - mean) / std.replace(0, np.nan)


def safe_pct_diff(price: float, fair_value: float) -> float | None:
    if fair_value == 0 or pd.isna(fair_value):
        return None
    return ((price - fair_value) / fair_value) * 100.0


def build_gold_institutional_fair_value_v2(
    input_path: Path = INPUT,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    if not input_path.exists():
        raise FileNotFoundError(f"Missing driver file: {input_path}")

    df = pd.read_csv(input_path)
    if "date" not in df.columns or "gold_close" not in df.columns:
        raise ValueError("Driver file must contain date and gold_close.")

    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df["gold_close"] = pd.to_numeric(df["gold_close"], errors="coerce")
    df = df.dropna(subset=["date", "gold_close"]).sort_values("date").drop_duplicates("date")

    available_drivers = [
        col for col in DRIVER_CONFIG.keys()
        if col in df.columns and df[col].notna().sum() >= 60
    ]

    if not available_drivers:
        raise ValueError("No usable valuation drivers found. Need at least one driver with 60+ values.")

    working = df[["date", "gold_close"] + available_drivers].copy()

    for col in available_drivers:
        working[col] = pd.to_numeric(working[col], errors="coerce")

    driver_scores = []

    for col in available_drivers:
        cfg = DRIVER_CONFIG[col]
        series = working[col]

        if col in {"silver_close", "m2", "m2_money_supply", "etf_holdings", "gold_etf_holdings"}:
            transformed = np.log(series.replace(0, np.nan))
        else:
            transformed = series

        score = zscore(transformed) * cfg["sign"]
        driver_scores.append(score.rename(col))

    score_frame = pd.concat(driver_scores, axis=1)
    working["fair_value_score"] = score_frame.mean(axis=1, skipna=True)

    gold_log = np.log(working["gold_close"].replace(0, np.nan))
    gold_trend = gold_log.rolling(504, min_periods=120).mean()
    gold_vol = gold_log.rolling(504, min_periods=120).std()

    working["fair_value_log"] = gold_trend + (working["fair_value_score"] * gold_vol)
    working["fair_value"] = np.exp(working["fair_value_log"])

    working["upper_band"] = working["fair_value"] * 1.15
    working["lower_band"] = working["fair_value"] * 0.85

    working["premium_discount_pct"] = (
        (working["gold_close"] - working["fair_value"]) / working["fair_value"]
    ) * 100.0

    clean = working.dropna(subset=["fair_value", "premium_discount_pct"]).copy()

    if clean.empty:
        raise ValueError("Fair value model could not produce any valid rows yet.")

    history: list[dict[str, Any]] = []

    for _, row in clean.iterrows():
        history.append(
            {
                "date": row["date"].strftime("%Y-%m-%d"),
                "gold_close": round(float(row["gold_close"]), 4),
                "fair_value": round(float(row["fair_value"]), 4),
                "upper_band": round(float(row["upper_band"]), 4),
                "lower_band": round(float(row["lower_band"]), 4),
                "premium_discount_pct": round(float(row["premium_discount_pct"]), 2),
                "fair_value_score": round(float(row["fair_value_score"]), 4),
            }
        )

    latest_row = clean.iloc[-1]

    latest = {
        "asset": "Gold",
        "model": "gold_institutional_fair_value_v2_partial_driver",
        "date": latest_row["date"].strftime("%Y-%m-%d"),
        "gold_close": round(float(latest_row["gold_close"]), 4),
        "fair_value": round(float(latest_row["fair_value"]), 4),
        "upper_band": round(float(latest_row["upper_band"]), 4),
        "lower_band": round(float(latest_row["lower_band"]), 4),
        "premium_discount_pct": round(float(latest_row["premium_discount_pct"]), 2),
        "fair_value_score": round(float(latest_row["fair_value_score"]), 4),
        "drivers_used": [
            {
                "column": col,
                "label": DRIVER_CONFIG[col]["label"],
                "direction": "bullish_when_higher" if DRIVER_CONFIG[col]["sign"] > 0 else "bearish_when_higher",
            }
            for col in available_drivers
        ],
        "drivers_missing": [
            col for col in DRIVER_CONFIG.keys()
            if col not in available_drivers
        ],
        "history_rows": len(history),
    }

    return latest, history


def write_gold_institutional_fair_value_v2() -> dict[str, Any]:
    latest, history = build_gold_institutional_fair_value_v2(INPUT)

    PROCESSED.mkdir(parents=True, exist_ok=True)

    LATEST_OUT.write_text(json.dumps(latest, indent=2), encoding="utf-8")
    HISTORY_OUT.write_text(json.dumps(history, indent=2), encoding="utf-8")

    print(f"[gold-v2] wrote latest: {LATEST_OUT}")
    print(f"[gold-v2] wrote history: {HISTORY_OUT}")
    print(f"[gold-v2] rows: {len(history)}")
    print(f"[gold-v2] drivers used: {[d['column'] for d in latest['drivers_used']]}")
    print(f"[gold-v2] drivers missing: {latest['drivers_missing']}")
    print(
        f"[gold-v2] latest: price={latest['gold_close']} "
        f"fair_value={latest['fair_value']} "
        f"premium_discount={latest['premium_discount_pct']}%"
    )

    return latest


if __name__ == "__main__":
    write_gold_institutional_fair_value_v2()
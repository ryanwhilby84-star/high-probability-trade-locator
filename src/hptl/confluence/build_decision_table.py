from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from hptl.confluence.build_confluence_history import _build_confluence

PROCESSED_DIR = Path("data/processed")
EXPORT_DIR = Path("data/exports")
OUT_PATH = Path("web-dashboard/public/data/confluence_history_latest.json")

TARGET_MARKETS = [
    "NASDAQ / NQ",
    "S&P 500 / ES",
    "Dow / YM",
    "Gold",
    "Silver",
    "Copper / HG",
    "Crude Oil / CL",
    "Natural Gas / NG",
    "Coffee",
    "Cocoa",
    "Corn",
    "Wheat",
    "Soybeans",
]

MARKET_ALIASES = {
    "NASDAQ / NQ": ["NASDAQ MINI", "E-MINI NASDAQ", "NASDAQ-100", "NASDAQ 100", "NASDAQ 100 STOCK INDEX"],
    "S&P 500 / ES": ["E-MINI S&P 500", "S&P 500 STOCK INDEX", "S&P 500 CONSOLIDATED", "SP 500"],
    "Dow / YM": ["DOW JONES", "DJIA", "E-MINI DOW", "MINI DOW", "DOW JONES U.S. INDEX"],
    "Gold": ["GOLD -", "GOLD"],
    "Silver": ["SILVER -", "SILVER"],
    "Copper / HG": ["COPPER-GRADE #1", "COPPER"],
    "Crude Oil / CL": ["CRUDE OIL, LIGHT SWEET", "CRUDE OIL"],
    "Natural Gas / NG": ["NATURAL GAS"],
    "Corn": ["CORN -"],
    "Soybeans": ["SOYBEANS -"],
    "Wheat": ["WHEAT -"],
    "Coffee": ["COFFEE C -", "COFFEE"],
    "Cocoa": ["COCOA -", "COCOA"],
}


def _normalize_market_text(value: str) -> str:
    return " ".join(str(value).upper().replace("_", " ").replace("/", " ").replace("-", " ").split())


def _map_market(raw_market: str) -> str | None:
    normalized = _normalize_market_text(raw_market)
    for canonical, aliases in MARKET_ALIASES.items():
        if any(_normalize_market_text(alias) in normalized for alias in aliases):
            return canonical
    return None


def _latest_cot_file() -> Path | None:
    files = sorted(PROCESSED_DIR.glob("cot_cleaned_*.csv"), key=lambda p: p.stat().st_mtime)
    return files[-1] if files else None


def _latest_macro_file() -> Path | None:
    files = sorted(EXPORT_DIR.glob("macro_history_*.xlsx"), key=lambda p: p.stat().st_mtime)
    if not files:
        files = sorted(EXPORT_DIR.glob("macro_output_*.xlsx"), key=lambda p: p.stat().st_mtime)
    return files[-1] if files else None


def _load_cot_latest_by_market(path: Path) -> dict[str, dict[str, Any]]:
    df = pd.read_csv(path, low_memory=False)
    for col in ["market_and_exchange_names", "report_date_as_yyyy_mm_dd", "m_money_positions_long_other", "m_money_positions_short_other"]:
        if col not in df.columns:
            raise ValueError(f"COT file missing required column: {col}")

    frame = pd.DataFrame()
    frame["market"] = df["market_and_exchange_names"].astype(str).str.strip().apply(_map_market)
    frame["report_date"] = pd.to_datetime(df["report_date_as_yyyy_mm_dd"], errors="coerce", dayfirst=True)
    frame["long"] = pd.to_numeric(df["m_money_positions_long_other"], errors="coerce")
    frame["short"] = pd.to_numeric(df["m_money_positions_short_other"], errors="coerce")
    frame = frame.dropna(subset=["market", "report_date", "long", "short"]).copy()
    frame["net"] = frame["long"] - frame["short"]

    latest_rows: dict[str, dict[str, Any]] = {}
    for market in TARGET_MARKETS:
        m = frame[frame["market"] == market].sort_values("report_date")
        if m.empty:
            continue
        recent = m.tail(5).copy()
        latest = recent.iloc[-1]
        prev = recent.iloc[-2] if len(recent) >= 2 else None
        four_back = recent.iloc[-5] if len(recent) >= 5 else None
        weekly_change = float(latest["net"] - prev["net"]) if prev is not None else None
        four_week_change = float(latest["net"] - four_back["net"]) if four_back is not None else None

        score = 0
        if latest["net"] > 0:
            score += 4
            bias = "Bullish"
        elif latest["net"] < 0:
            score += 4
            bias = "Bearish"
        else:
            bias = "Neutral"

        if weekly_change is not None:
            if bias == "Bullish" and weekly_change > 0:
                score += 2
            elif bias == "Bearish" and weekly_change < 0:
                score += 2
            else:
                score += 1

        if four_week_change is not None:
            if bias == "Bullish" and four_week_change > 0:
                score += 2
            elif bias == "Bearish" and four_week_change < 0:
                score += 2
            else:
                score += 1

        if abs(float(latest["net"])) > 0:
            score += 2

        latest_rows[market] = {
            "latest_report_date": latest["report_date"].date().isoformat(),
            "cot_bias": bias,
            "cot_score": int(max(0, min(score, 10))),
            "cot_reason": (
                f"net={int(latest['net'])}; "
                + (f"1w_change={int(weekly_change)}; " if weekly_change is not None else "1w_change=N/A; ")
                + (f"4w_change={int(four_week_change)}" if four_week_change is not None else "4w_change=N/A")
            ),
        }
    return latest_rows


def _load_latest_macro(path: Path) -> tuple[str | None, str | None, float | None]:
    sheets = ["Macro_History", "Macro_Dashboard"]
    macro = None
    for sheet in sheets:
        try:
            macro = pd.read_excel(path, sheet_name=sheet)
            break
        except ValueError:
            continue
    if macro is None or macro.empty:
        return None, None, None

    if "macro_snapshot_date" not in macro.columns:
        return None, None, None

    macro = macro.copy()
    macro["macro_snapshot_date"] = pd.to_datetime(macro["macro_snapshot_date"], errors="coerce")
    macro = macro[macro["macro_snapshot_date"].notna()].sort_values("macro_snapshot_date")
    if macro.empty:
        return None, None, None
    row = macro.iloc[-1]
    signal = str(row.get("macro_signal", "")).strip().lower() or None
    score = pd.to_numeric(pd.Series([row.get("macro_score")]), errors="coerce").iloc[0]
    if pd.isna(score):
        score = None
    else:
        score = float(score)
    return row["macro_snapshot_date"].date().isoformat(), signal, score


def run() -> Path:
    cot_path = _latest_cot_file()
    macro_path = _latest_macro_file()

    cot_rows: dict[str, dict[str, Any]] = {}
    if cot_path is not None:
        cot_rows = _load_cot_latest_by_market(cot_path)

    macro_date, macro_signal, macro_score = (None, None, None)
    if macro_path is not None:
        macro_date, macro_signal, macro_score = _load_latest_macro(macro_path)

    records = []
    for market in TARGET_MARKETS:
        cot = cot_rows.get(market)
        if cot is None:
            record = {
                "market": market,
                "latest_report_date": "N/A",
                "cot_bias": "N/A",
                "cot_score": "N/A",
                "cot_reason": "N/A: no relevant COT rows found for this target market.",
                "macro_regime": macro_signal or "N/A",
                "macro_score": macro_score if macro_score is not None else "N/A",
                "final_context": "N/A",
                "technical_action_note": "N/A: waiting for COT data.",
            }
        elif macro_signal is None or macro_score is None:
            record = {
                "market": market,
                "latest_report_date": cot["latest_report_date"],
                "cot_bias": cot["cot_bias"],
                "cot_score": cot["cot_score"],
                "cot_reason": cot["cot_reason"],
                "macro_regime": "N/A",
                "macro_score": "N/A",
                "final_context": "N/A",
                "technical_action_note": "N/A: macro input unavailable.",
            }
        else:
            conf = _build_confluence(cot["cot_bias"], float(cot["cot_score"]), macro_signal, float(macro_score))
            record = {
                "market": market,
                "latest_report_date": cot["latest_report_date"],
                "cot_bias": cot["cot_bias"],
                "cot_score": cot["cot_score"],
                "cot_reason": cot["cot_reason"],
                "macro_regime": macro_signal,
                "macro_score": float(macro_score),
                "final_context": f"{conf['confluence_bias']} {conf['confluence_score']:.0f}",
                "technical_action_note": conf["trade_readiness"],
            }
        if macro_date and record["latest_report_date"] == "N/A":
            record["latest_report_date"] = macro_date
        records.append(record)

    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "records": records,
    }
    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUT_PATH.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    print(f"Wrote {OUT_PATH} with {len(records)} rows")
    return OUT_PATH


if __name__ == "__main__":
    run()

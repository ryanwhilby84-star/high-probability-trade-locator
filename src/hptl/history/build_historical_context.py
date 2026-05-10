from __future__ import annotations

from datetime import datetime
from pathlib import Path

import pandas as pd
from openpyxl import load_workbook

from hptl.cot.exporter import _calculate_cot_scores
from hptl.macro.macro_scoring import REQUIRED_SCORING_INPUTS, score_macro
from hptl.macro.rates_parser import process_rates

EXPORT_DIR = Path("data/exports")
PROCESSED_DIR = Path("data/processed")

HISTORY_START_DATE = pd.Timestamp("2024-05-06")
TARGET_ALIASES = {
    "NASDAQ / NQ": ["NASDAQ MINI", "E-MINI NASDAQ", "NASDAQ-100", "NASDAQ 100", "NASDAQ 100 STOCK INDEX"],
    "S&P 500 / ES": ["E-MINI S&P 500", "S&P 500 STOCK INDEX", "S&P 500 CONSOLIDATED", "SP 500"],
    "Dow / YM / DJIA / S30": ["DOW JONES", "DJIA", "E-MINI DOW", "MINI DOW", "DOW JONES U.S. INDEX"],
    "Gold / GC": ["GOLD -", "GOLD"],
    "Silver / SI": ["SILVER -", "SILVER"],
    "Copper / HG": ["COPPER-GRADE #1", "COPPER"],
    "Crude Oil / CL": ["CRUDE OIL, LIGHT SWEET", "CRUDE OIL"],
    "Natural Gas / NG": ["NATURAL GAS"],
    "Corn / ZC": ["CORN -"],
    "Soybeans / ZS": ["SOYBEANS -"],
    "Wheat / ZW": ["WHEAT -"],
    "Coffee / KC": ["COFFEE C -", "COFFEE"],
    "Cocoa / CC": ["COCOA -", "COCOA"],
}
TARGET_MARKETS = list(TARGET_ALIASES.keys())

OUTPUT_COLUMNS = [
    "market",
    "cot_report_date",
    "cot_bias",
    "cot_score",
    "cot_strength",
    "macro_snapshot_date",
    "macro_signal",
    "macro_score",
    "macro_strength",
    "macro_context_for_trades",
    "confluence_bias",
    "confluence_score",
    "confluence_strength",
    "trade_readiness",
    "summary",
]


def _normalize(v: str) -> str:
    return " ".join(v.upper().replace("_", " ").replace("/", " ").replace("-", " ").split())


def _map_market(raw_market: str) -> str | None:
    norm = _normalize(raw_market)
    for canonical, aliases in TARGET_ALIASES.items():
        if any(_normalize(alias) in norm for alias in aliases):
            return canonical
    return None


def _macro_adjustment(score: float) -> int:
    if score <= 2:
        return 1
    if score <= 5:
        return 2
    if score <= 7:
        return 3
    return 4


def _strength_from_score(score: float) -> str:
    if score >= 8:
        return "Very Strong"
    if score >= 6:
        return "Strong"
    if score >= 3:
        return "Moderate"
    return "Weak"


def _build_confluence(cot_bias: str, cot_score: float, macro_signal: str, macro_score: float) -> dict[str, object]:
    cot_dir = "long" if cot_bias == "Bullish" else "short" if cot_bias == "Bearish" else "neutral"
    macro_dir = "long" if macro_signal == "risk_on" else "short" if macro_signal == "risk_off" else "neutral"

    hard_conflict = (
        cot_dir in {"long", "short"}
        and macro_dir in {"long", "short"}
        and cot_dir != macro_dir
        and cot_score >= 6
        and macro_score >= 6
    )

    if hard_conflict:
        return {
            "confluence_bias": "Conflicted / No Trade",
            "confluence_score": 0,
            "confluence_strength": "Blocked",
            "trade_readiness": "Stand down",
            "summary": f"COT {cot_bias} ({cot_score}) conflicts with macro {macro_signal} ({macro_score}) at high conviction.",
        }

    score = cot_score
    if cot_dir in {"long", "short"} and macro_dir in {"long", "short"}:
        delta = _macro_adjustment(macro_score)
        score = score + delta if cot_dir == macro_dir else score - delta

    score = float(max(0, min(10, score)))

    if cot_dir == "neutral":
        bias = "Neutral / Mixed"
    elif cot_dir == macro_dir == "long":
        bias = "Long Bias"
    elif cot_dir == macro_dir == "short":
        bias = "Short Bias"
    elif cot_dir == "long":
        bias = "Long (Headwind)"
    else:
        bias = "Short (Headwind)"

    if score >= 8:
        readiness = "High conviction"
    elif score >= 6:
        readiness = "Actionable"
    elif score >= 3:
        readiness = "Cautious"
    else:
        readiness = "Low conviction"

    return {
        "confluence_bias": bias,
        "confluence_score": score,
        "confluence_strength": _strength_from_score(score),
        "trade_readiness": readiness,
        "summary": f"COT {cot_bias} ({cot_score}) vs macro {macro_signal} ({macro_score}) => {bias} {score:.1f}.",
    }


def _load_cot_history() -> pd.DataFrame:
    cot_files = sorted(PROCESSED_DIR.glob("cot_cleaned_*.csv"), key=lambda p: p.stat().st_mtime)
    if not cot_files:
        raise FileNotFoundError("No COT files found in data/processed/cot_cleaned_*.csv")

    frames: list[pd.DataFrame] = []
    for cot_file in cot_files:
        print(f"Loading COT: {cot_file}")
        df = pd.read_csv(cot_file, low_memory=False)
        df = df.rename(columns={c: str(c).strip() for c in df.columns})

        required = [
            "market_and_exchange_names",
            "report_date_as_yyyy_mm_dd",
            "m_money_positions_long_other",
            "m_money_positions_short_other",
        ]
        missing = [c for c in required if c not in df.columns]
        if missing:
            print(f"  skipped missing columns: {missing}")
            continue

        d = pd.DataFrame()
        d["market"] = df["market_and_exchange_names"].astype(str).str.strip().map(_map_market)
        d["cot_report_date"] = pd.to_datetime(df["report_date_as_yyyy_mm_dd"], errors="coerce", dayfirst=True).dt.normalize()
        d["managed_money_long"] = pd.to_numeric(df["m_money_positions_long_other"], errors="coerce")
        d["managed_money_short"] = pd.to_numeric(df["m_money_positions_short_other"], errors="coerce")
        d = d[d["market"].notna() & d["cot_report_date"].notna() & d["managed_money_long"].notna() & d["managed_money_short"].notna()].copy()
        frames.append(d)

    if not frames:
        raise ValueError("No usable COT rows found for target markets.")

    out = pd.concat(frames, ignore_index=True)
    out = out.sort_values(["market", "cot_report_date"]).drop_duplicates(["market", "cot_report_date"], keep="last")
    out = out[out["cot_report_date"] >= HISTORY_START_DATE].copy()

    out["managed_money_net"] = out["managed_money_long"] - out["managed_money_short"]
    out["noncommercial_net"] = out["managed_money_net"]
    out["commercial_net"] = out["managed_money_net"]

    g = out.groupby("market", sort=False)
    out["weekly_change"] = g["managed_money_net"].diff(1)
    out["four_week_change"] = g["managed_money_net"].diff(4)
    out["mm_weekly_change"] = g["managed_money_net"].diff(1)

    scored = _calculate_cot_scores(out)
    scored["cot_score"] = pd.to_numeric(scored["cot_score"], errors="coerce").clip(0, 10)
    scored = scored[scored["cot_score"].notna() & scored["cot_bias"].notna()].copy()
    scored["cot_strength"] = scored["cot_strength"].fillna("Unknown")
    return scored[["market", "cot_report_date", "cot_bias", "cot_score", "cot_strength"]]


def _load_macro_history() -> pd.DataFrame:
    rates = process_rates().sort_values("date").copy()
    scored = score_macro(rates)
    valid = scored[scored["macro_score"].notna()].copy()
    if valid.empty:
        raise ValueError("No historical macro rows could be scored from rates history.")

    if valid[REQUIRED_SCORING_INPUTS].isna().any(axis=1).any():
        raise ValueError("Found macro scores with missing required inputs.")

    valid["macro_snapshot_date"] = pd.to_datetime(valid["macro_snapshot_date"], errors="coerce").dt.normalize()
    valid["macro_score"] = pd.to_numeric(valid["macro_score"], errors="coerce").fillna(0).clip(0, 10)
    valid["macro_signal"] = valid["macro_signal"].astype(str).str.strip().str.lower()
    valid = valid[valid["macro_snapshot_date"].notna()]

    return valid[["macro_snapshot_date", "macro_signal", "macro_score", "macro_strength", "macro_context_for_trades"]].drop_duplicates(["macro_snapshot_date"], keep="last").sort_values("macro_snapshot_date")


def _format_worksheet(ws) -> None:
    ws.freeze_panes = "A2"
    ws.auto_filter.ref = ws.dimensions
    for col_cells in ws.columns:
        max_len = max(len(str(c.value)) if c.value is not None else 0 for c in col_cells)
        ws.column_dimensions[col_cells[0].column_letter].width = min(max(max_len + 2, 12), 60)


def run() -> Path:
    print("=" * 70)
    print("Historical context build started")
    print("=" * 70)

    cot = _load_cot_history()
    macro = _load_macro_history()

    print(f"COT date range: {cot['cot_report_date'].min().date()} -> {cot['cot_report_date'].max().date()}")
    print(f"Macro date range: {macro['macro_snapshot_date'].min().date()} -> {macro['macro_snapshot_date'].max().date()}")

    aligned = pd.merge_asof(
        cot.sort_values("cot_report_date"),
        macro.sort_values("macro_snapshot_date"),
        left_on="cot_report_date",
        right_on="macro_snapshot_date",
        direction="backward",
    )
    aligned = aligned[aligned["macro_snapshot_date"].notna()].copy()

    confluence = aligned.apply(
        lambda r: _build_confluence(r["cot_bias"], float(r["cot_score"]), str(r["macro_signal"]), float(r["macro_score"])),
        axis=1,
        result_type="expand",
    )
    out = pd.concat([aligned, confluence], axis=1)
    out = out[(out["cot_report_date"] >= HISTORY_START_DATE)].copy()
    out = out[OUTPUT_COLUMNS].sort_values(["cot_report_date", "market"]).reset_index(drop=True)

    found_markets = sorted(out["market"].dropna().unique().tolist())
    missing_markets = sorted(set(TARGET_MARKETS) - set(found_markets))
    print(f"Markets found ({len(found_markets)}): {found_markets}")
    print(f"Markets missing ({len(missing_markets)}): {missing_markets}")
    print(f"Rows exported: {len(out)}")

    EXPORT_DIR.mkdir(parents=True, exist_ok=True)
    output_path = EXPORT_DIR / f"historical_context_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        out.to_excel(writer, sheet_name="Confluence_Dashboard", index=False)

    wb = load_workbook(output_path)
    ws = wb["Confluence_Dashboard"]
    _format_worksheet(ws)
    wb.save(output_path)

    print(f"Output path: {output_path}")
    print("=" * 70)
    return output_path


if __name__ == "__main__":
    run()

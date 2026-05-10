from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from hptl.confluence.build_confluence_history import _build_confluence
from hptl.cot.exporter import _calculate_cot_scores

PROCESSED_DIR = Path("data/processed")
EXPORT_DIR = Path("data/exports")
OUT_PATH = Path("web-dashboard/public/data/confluence_history_latest.json")
AUDIT_CSV_PATH = Path("data/exports/decision_table_audit.csv")

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


def _load_cot_history() -> pd.DataFrame:
    files = sorted(PROCESSED_DIR.glob("cot_cleaned_*.csv"), key=lambda p: p.stat().st_mtime)
    if not files:
        return pd.DataFrame()

    frames = []
    for path in files:
        df = pd.read_csv(path, low_memory=False)
        if not {"market_and_exchange_names", "report_date_as_yyyy_mm_dd", "m_money_positions_long_other", "m_money_positions_short_other"}.issubset(df.columns):
            continue
        x = pd.DataFrame()
        x["market"] = df["market_and_exchange_names"].astype(str).str.strip().apply(_map_market)
        x["raw_cftc_market_name"] = df["market_and_exchange_names"].astype(str).str.strip()
        x["cot_report_date"] = pd.to_datetime(df["report_date_as_yyyy_mm_dd"], errors="coerce", dayfirst=True).dt.normalize()
        x["long_value"] = pd.to_numeric(df["m_money_positions_long_other"], errors="coerce")
        x["short_value"] = pd.to_numeric(df["m_money_positions_short_other"], errors="coerce")
        x = x.dropna(subset=["market", "cot_report_date", "long_value", "short_value"]).copy()
        x["net_value"] = x["long_value"] - x["short_value"]
        frames.append(x)

    if not frames:
        return pd.DataFrame()

    cot = pd.concat(frames, ignore_index=True)
    cot = cot.sort_values(["market", "cot_report_date"]).drop_duplicates(["market", "cot_report_date"], keep="last")
    cot["weekly_change"] = cot.groupby("market")["net_value"].diff(1)
    cot["four_week_change"] = cot.groupby("market")["net_value"].diff(4)
    cot["managed_money_net"] = cot["net_value"]
    cot["noncommercial_net"] = cot["net_value"]
    cot["commercial_net"] = cot["net_value"]
    cot["mm_weekly_change"] = cot["weekly_change"]
    cot = _calculate_cot_scores(cot)
    return cot


def _load_macro_history() -> pd.DataFrame:
    files = sorted(EXPORT_DIR.glob("macro_history_*.xlsx"), key=lambda p: p.stat().st_mtime)
    if not files:
        files = sorted(EXPORT_DIR.glob("macro_output_*.xlsx"), key=lambda p: p.stat().st_mtime)
    frames = []
    for path in files:
        for sheet in ["Macro_History", "Macro_Dashboard"]:
            try:
                m = pd.read_excel(path, sheet_name=sheet)
                break
            except Exception:
                m = None
        if m is None or m.empty or "macro_snapshot_date" not in m.columns:
            continue
        y = m.copy()
        y["macro_snapshot_date"] = pd.to_datetime(y["macro_snapshot_date"], errors="coerce").dt.normalize()
        y["macro_signal"] = y.get("macro_signal", "").astype(str).str.strip().str.lower()
        y["macro_score"] = pd.to_numeric(y.get("macro_score"), errors="coerce")
        y = y[y["macro_snapshot_date"].notna()].copy()
        frames.append(y[["macro_snapshot_date", "macro_signal", "macro_score"]])
    if not frames:
        return pd.DataFrame(columns=["macro_snapshot_date", "macro_signal", "macro_score"])
    return pd.concat(frames, ignore_index=True).sort_values("macro_snapshot_date").drop_duplicates("macro_snapshot_date", keep="last")


def run() -> Path:
    cot = _load_cot_history()
    macro = _load_macro_history()
    records: list[dict[str, Any]] = []
    if cot.empty:
        payload = {"generated_at": datetime.now(timezone.utc).isoformat(), "records": []}
        OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
        OUT_PATH.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        return OUT_PATH

    all_dates = sorted(cot["cot_report_date"].dropna().dt.strftime("%Y-%m-%d").unique())

    for date_str in all_dates:
        week_date = pd.Timestamp(date_str)
        week_rows = cot[cot["cot_report_date"] == week_date]
        by_market = {m: g.iloc[-1] for m, g in week_rows.groupby("market")}
        macro_row = None
        if not macro.empty:
            avail = macro[macro["macro_snapshot_date"] <= week_date]
            if not avail.empty:
                macro_row = avail.iloc[-1]

        for market in TARGET_MARKETS:
            row = by_market.get(market)
            if row is None:
                records.append({
                    "date": date_str,
                    "market": market,
                    "latest_report_date": "N/A",
                    "cot_bias": "N/A",
                    "cot_score": "N/A",
                    "cot_reason": f"N/A: missing raw COT row for {market} on {date_str}.",
                    "macro_regime": "N/A" if macro_row is None else str(macro_row.get("macro_signal") or "N/A"),
                    "macro_score": "N/A",
                    "final_context": "N/A",
                    "technical_action_note": "N/A: no COT row for selected week.",
                    "final_context_reason": "Cannot score without raw COT market/date row.",
                })
                continue

            cot_bias = str(row["cot_bias"])
            cot_score = float(row["cot_score"])
            weekly = row.get("weekly_change")
            four = row.get("four_week_change")
            net = row.get("net_value")
            if pd.isna(weekly):
                weekly = None
            if pd.isna(four):
                four = None

            cot_reason = (
                f"Managed money net is {int(net)} (long {int(row['long_value'])}, short {int(row['short_value'])}); "
                + (f"1w net change {int(weekly)}; " if weekly is not None else "1w net change N/A; ")
                + (f"4w net change {int(four)}." if four is not None else "4w net change N/A.")
            )

            macro_signal = None if macro_row is None else str(macro_row.get("macro_signal") or "")
            macro_score = None if macro_row is None else pd.to_numeric(pd.Series([macro_row.get("macro_score")]), errors="coerce").iloc[0]
            has_macro = macro_signal not in {None, "", "nan"} and pd.notna(macro_score)

            if has_macro:
                conf = _build_confluence(cot_bias, cot_score, macro_signal, float(macro_score))
                final_context = f"{conf['confluence_bias']} {conf['confluence_score']:.0f}"
                technical_note = conf["trade_readiness"]
                final_reason = conf["summary"]
                macro_regime = macro_signal
                macro_score_out = float(macro_score)
            else:
                final_context = "N/A"
                technical_note = "N/A: macro input unavailable."
                final_reason = "Cannot calculate final context because macro input is missing."
                macro_regime = "N/A"
                macro_score_out = "N/A"

            records.append({
                "date": date_str,
                "market": market,
                "latest_report_date": date_str,
                "cot_bias": cot_bias,
                "cot_score": int(round(cot_score)),
                "cot_reason": cot_reason,
                "macro_regime": macro_regime,
                "macro_score": macro_score_out,
                "final_context": final_context,
                "technical_action_note": technical_note,
                "final_context_reason": final_reason,
                "raw_cftc_market_name": str(row.get("raw_cftc_market_name", "")),
                "trader_group_used": "m_money_positions_*_other",
                "long_value": float(row["long_value"]),
                "short_value": float(row["short_value"]),
                "net_value": float(net),
                "previous_week_net": float(net - weekly) if weekly is not None else None,
                "one_week_net_change": weekly,
                "four_week_net_change": four,
                "bias_rule_used": "net>0 => Bullish; net<0 => Bearish; net==0 => Neutral",
                "score_rule_used": "_calculate_cot_scores from raw managed-money positioning",
                "final_calculated_cot_bias": cot_bias,
                "final_calculated_cot_score": int(round(cot_score)),
            })

    payload = {"generated_at": datetime.now(timezone.utc).isoformat(), "records": records}
    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUT_PATH.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    AUDIT_CSV_PATH.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(records).to_csv(AUDIT_CSV_PATH, index=False)
    print(f"Wrote {OUT_PATH} with {len(records)} rows")
    return OUT_PATH


if __name__ == "__main__":
    run()

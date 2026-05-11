from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from hptl.confluence.build_confluence_history import _build_confluence
from hptl.cot.exporter import _calculate_cot_scores
from hptl.confluence.run_confluence_update import _find_column

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
    "NASDAQ / NQ": ["NASDAQ 100 STOCK INDEX", "E-MINI NASDAQ 100", "NASDAQ MINI", "NASDAQ-100"],
    "S&P 500 / ES": ["S&P 500 STOCK INDEX", "E-MINI S&P 500", "S&P 500 CONSOLIDATED", "SP 500"],
    "Dow / YM": ["DOW JONES U.S. INDEX", "E-MINI DOW", "MINI DOW", "DJIA"],
    "Gold": ["GOLD -", "GOLD"],
    "Silver": ["SILVER -", "SILVER"],
    "Copper / HG": ["COPPER- #1", "COPPER-GRADE #1", "COPPER"],
    "Crude Oil / CL": ["CRUDE OIL, LIGHT SWEET-WTI", "WTI-PHYSICAL", "WTI FINANCIAL CRUDE OIL"],
    "Natural Gas / NG": ["HENRY HUB - NEW YORK MERCANTILE EXCHANGE", "NATURAL GAS - NEW YORK MERCANTILE EXCHANGE", "E-MINI NATURAL GAS"],
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



def _resolve_position_columns(df: pd.DataFrame, long_candidates: list[str], short_candidates: list[str]) -> tuple[str | None, str | None, str]:
    """Resolve long/short source columns from raw COT CSV fields.

    Returns (long_col, short_col, source_family) where source_family is one of
    managed_money | noncommercial | unknown.
    """
    long_col = _find_column(df, *long_candidates)
    short_col = _find_column(df, *short_candidates)
    if long_col is None or short_col is None:
        return long_col, short_col, "unknown"

    managed_hints = {
        "managed_money",
        "m_money",
        "money_manager",
    }
    normalized_pair = f"{long_col} {short_col}".lower()
    source_family = "managed_money" if any(hint in normalized_pair for hint in managed_hints) else "noncommercial"
    return long_col, short_col, source_family


def _parse_cot_report_dates(raw_dates: pd.Series, source_name: str) -> pd.Series:
    """Parse COT report dates explicitly from known formats only."""
    raw = raw_dates.astype(str).str.strip()
    parsed = pd.Series(pd.NaT, index=raw.index, dtype="datetime64[ns]")

    slash_date_mask = raw.str.match(r"^\d{1,2}/\d{1,2}/\d{4}$", na=False)
    slash_first = pd.to_numeric(raw.loc[slash_date_mask].str.split("/").str[0], errors="coerce")
    slash_second = pd.to_numeric(raw.loc[slash_date_mask].str.split("/").str[1], errors="coerce")
    # Determine slash format explicitly:
    # - if first token ever > 12 => DD/MM/YYYY
    # - elif second token ever > 12 => MM/DD/YYYY
    # - else ambiguous; default to MM/DD/YYYY
    slash_format = "%m/%d/%Y"
    slash_format_label = "MM/DD/YYYY"
    if (slash_first > 12).any():
        slash_format = "%d/%m/%Y"
        slash_format_label = "DD/MM/YYYY"
    elif (slash_second > 12).any():
        slash_format = "%m/%d/%Y"
        slash_format_label = "MM/DD/YYYY"

    fmt_counts = {
        "YYYY-MM-DD": int(raw.str.match(r"^\d{4}-\d{2}-\d{2}$", na=False).sum()),
        "slash_dates": int(slash_date_mask.sum()),
        "slash_detected_as": slash_format_label,
        "YYYYMMDD": int(raw.str.match(r"^\d{8}$", na=False).sum()),
    }
    # prefer unambiguous ISO, then compact ISO, then US slash dates
    iso_mask = raw.str.match(r"^\d{4}-\d{2}-\d{2}$", na=False)
    compact_iso_mask = raw.str.match(r"^\d{8}$", na=False)
    slash_mask = slash_date_mask

    parsed.loc[iso_mask] = pd.to_datetime(raw.loc[iso_mask], format="%Y-%m-%d", errors="coerce")
    parsed.loc[compact_iso_mask] = pd.to_datetime(raw.loc[compact_iso_mask], format="%Y%m%d", errors="coerce")
    parsed.loc[slash_mask] = pd.to_datetime(raw.loc[slash_mask], format=slash_format, errors="coerce")

    print(f"Date format inspection ({source_name}): {fmt_counts}")
    print(f"Raw report_date sample ({source_name}): {raw.head(12).tolist()}")
    print(f"Parsed report_date sample ({source_name}): {parsed.dt.strftime('%Y-%m-%d').head(12).tolist()}")
    return parsed.dt.normalize()


def _apply_net_anchored_cot_scoring(cot: pd.DataFrame) -> pd.DataFrame:
    """Apply COT bias/score where net position anchors direction."""
    scored = cot.copy()
    net = pd.to_numeric(scored["net_value"], errors="coerce")
    c1 = pd.to_numeric(scored["mm_weekly_change"], errors="coerce")
    c4 = pd.to_numeric(scored["mm_four_week_change"], errors="coerce")

    biases: list[str] = []
    directions: list[str] = []
    interpretations: list[str] = []
    scores: list[int] = []

    for n, w1, w4 in zip(net.tolist(), c1.tolist(), c4.tolist()):
        if pd.isna(n):
            biases.append("N/A")
            directions.append("Neutral")
            interpretations.append("No valid managed-money long/short values were available.")
            scores.append(0)
            continue

        if n < 0:
            direction = "Bearish"
            base_score = 4
            improving = (pd.notna(w1) and w1 > 0) or (pd.notna(w4) and w4 > 0)
            strengthening = (pd.notna(w1) and w1 < 0) or (pd.notna(w4) and w4 < 0)
            if improving:
                bias = "Bearish / Improving"
                interp = "Managed money remains net short, but net positioning improved over 1w/4w."
            elif strengthening:
                bias = "Bearish"
                interp = "Managed money remains net short and bearish pressure is strengthening."
            else:
                bias = "Bearish"
                interp = "Managed money remains net short with mixed/flat momentum."
            score = base_score + (2 if pd.notna(w1) and w1 < 0 else 0) + (2 if pd.notna(w4) and w4 < 0 else 0)
            score -= (1 if pd.notna(w1) and w1 > 0 else 0) + (1 if pd.notna(w4) and w4 > 0 else 0)
        elif n > 0:
            direction = "Bullish"
            base_score = 4
            weakening = (pd.notna(w1) and w1 < 0) or (pd.notna(w4) and w4 < 0)
            strengthening = (pd.notna(w1) and w1 > 0) or (pd.notna(w4) and w4 > 0)
            if weakening:
                bias = "Bullish / Weakening"
                interp = "Managed money remains net long, but net positioning weakened over 1w/4w."
            elif strengthening:
                bias = "Bullish"
                interp = "Managed money remains net long and bullish pressure is strengthening."
            else:
                bias = "Bullish"
                interp = "Managed money remains net long with mixed/flat momentum."
            score = base_score + (2 if pd.notna(w1) and w1 > 0 else 0) + (2 if pd.notna(w4) and w4 > 0 else 0)
            score -= (1 if pd.notna(w1) and w1 < 0 else 0) + (1 if pd.notna(w4) and w4 < 0 else 0)
        else:
            direction = "Neutral"
            bias = "Neutralising"
            interp = "Managed money net is flat/near-flat; directional pressure is neutralising."
            score = 0

        biases.append(bias)
        directions.append(direction)
        interpretations.append(interp)
        scores.append(max(0, min(10, int(score))))

    scored["cot_bias"] = biases
    scored["cot_directional_bias"] = directions
    scored["cot_interpretation"] = interpretations
    scored["cot_score"] = scores
    return scored

def _load_cot_history() -> pd.DataFrame:
    files = sorted(PROCESSED_DIR.glob("cot_cleaned_*.csv"), key=lambda p: p.stat().st_mtime)
    if not files:
        return pd.DataFrame()

    frames = []
    printed_columns = False
    candidate_long_cols = [
        "managed_money_long",
        "m_money_long",
        "m_money_positions_long_all",
        "managed_money_positions_long_all",
        "money_manager_positions_long_all",
        "m_money_positions_long_other",
        "noncommercial_long",
        "noncomm_positions_long_all",
        "noncommercial_positions_long_all",
    ]
    candidate_short_cols = [
        "managed_money_short",
        "m_money_short",
        "m_money_positions_short_all",
        "managed_money_positions_short_all",
        "money_manager_positions_short_all",
        "m_money_positions_short_other",
        "noncommercial_short",
        "noncomm_positions_short_all",
        "noncommercial_positions_short_all",
    ]
    for path in files:
        df = pd.read_csv(path, low_memory=False)
        if not printed_columns:
            print(f"COT file columns ({path.name}): {list(df.columns)}")
            printed_columns = True

        market_col = _find_column(df, "market_and_exchange_names", "market", "market_name", "contract_market_name")
        date_col = _find_column(df, "report_date_as_yyyy_mm_dd", "cot_report_date", "report_date", "date")
        long_col, short_col, source_family = _resolve_position_columns(df, candidate_long_cols, candidate_short_cols)
        if market_col is None or date_col is None or long_col is None or short_col is None:
            continue
        x = pd.DataFrame()
        x["market"] = df[market_col].astype(str).str.strip().apply(_map_market)
        x["raw_cftc_market_name"] = df[market_col].astype(str).str.strip()
        x["cot_report_date"] = _parse_cot_report_dates(df[date_col], source_name=path.name)
        x["long_value"] = pd.to_numeric(df[long_col], errors="coerce")
        x["short_value"] = pd.to_numeric(df[short_col], errors="coerce")
        x["long_col_used"] = long_col
        x["short_col_used"] = short_col
        x["position_source_family"] = source_family
        x["missing_reason"] = pd.NA
        x = x.dropna(subset=["market", "cot_report_date"]).copy()
        x["net_value"] = x["long_value"] - x["short_value"]
        # preserve real missingness; do not coerce missing position values to 0
        missing_positions = x["long_value"].isna() | x["short_value"].isna()
        x.loc[missing_positions, "net_value"] = pd.NA
        x.loc[missing_positions, "missing_reason"] = "missing long/short values in resolved source columns"
        zero_pair = x["long_value"].eq(0) & x["short_value"].eq(0)
        x.loc[zero_pair, "net_value"] = pd.NA
        x.loc[zero_pair, "missing_reason"] = "long and short are both 0 in source row (treated as invalid/stale)"
        x["quality_score"] = (
            x["long_value"].notna().astype(int) * 5
            + x["short_value"].notna().astype(int) * 5
            + (~zero_pair).astype(int) * 5
            + x["net_value"].notna().astype(int) * 5
        )
        frames.append(x)

    if not frames:
        return pd.DataFrame()

    cot = pd.concat(frames, ignore_index=True)
    today_utc = pd.Timestamp(datetime.now(timezone.utc).date())
    future_mask = cot["cot_report_date"] > today_utc
    if future_mask.any():
        future_rows = cot.loc[future_mask, ["market", "raw_cftc_market_name", "cot_report_date"]].copy()
        print(
            "WARNING: future COT report dates detected and excluded:",
            {
                "today_utc": str(today_utc.date()),
                "future_rows": int(future_rows.shape[0]),
                "future_dates": sorted(future_rows["cot_report_date"].dt.strftime("%Y-%m-%d").dropna().unique().tolist()),
                "sample_rows": future_rows.head(10).to_dict(orient="records"),
            },
        )
        cot = cot.loc[~future_mask].copy()
    cot = cot.sort_values(["market", "cot_report_date", "quality_score"], ascending=[True, True, False]).drop_duplicates(["market", "cot_report_date"], keep="first")
    matched = cot.groupby("market")["raw_cftc_market_name"].apply(lambda s: sorted(set(s.dropna().astype(str).tolist()))).to_dict()
    col_trace = (
        cot.groupby("market")[["long_col_used", "short_col_used", "position_source_family"]]
        .agg(lambda s: sorted(set(s.astype(str).tolist())))
        .to_dict("index")
    )
    print("Matched raw market names by tracked market:")
    for m in TARGET_MARKETS:
        print(f"  {m}: {matched.get(m, [])}")
        print(f"    columns: {col_trace.get(m, {})}")
    latest_date = cot["cot_report_date"].max()
    latest_rows = cot[cot["cot_report_date"] == latest_date] if pd.notna(latest_date) else pd.DataFrame()
    print(f"Latest traced values by tracked market (date={latest_date.date() if pd.notna(latest_date) else 'N/A'}):")
    for market in TARGET_MARKETS:
        r = latest_rows[latest_rows["market"] == market]
        if r.empty:
            print(f"  {market}: raw_market=N/A | long_col_used=N/A | short_col_used=N/A | long=N/A | short=N/A | net=N/A | missing_reason=no mapped raw COT row")
            continue
        row = r.iloc[-1]
        print(
            f"  {market}: raw_market={row.get('raw_cftc_market_name')} | "
            f"long_col_used={row.get('long_col_used')} | short_col_used={row.get('short_col_used')} | "
            f"long={None if pd.isna(row.get('long_value')) else float(row.get('long_value'))} | "
            f"short={None if pd.isna(row.get('short_value')) else float(row.get('short_value'))} | "
            f"net={None if pd.isna(row.get('net_value')) else float(row.get('net_value'))} | "
            f"missing_reason={row.get('missing_reason')}"
        )
    cot["weekly_change"] = cot.groupby("market")["net_value"].diff(1)
    cot["four_week_change"] = cot.groupby("market")["net_value"].diff(4)
    cot["managed_money_net"] = cot["net_value"]
    cot["noncommercial_net"] = cot["net_value"]
    cot["commercial_net"] = cot["net_value"]
    cot["mm_weekly_change"] = cot["weekly_change"]
    cot["mm_four_week_change"] = cot["four_week_change"]
    cot = _calculate_cot_scores(cot)
    cot = _apply_net_anchored_cot_scoring(cot)
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
        macro_row_backward = None
        if not macro.empty:
            avail = macro[macro["macro_snapshot_date"] <= week_date]
            if not avail.empty:
                macro_row_backward = avail.iloc[-1]

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
                    "missing_reason": f"no mapped raw COT row for {market} on {date_str}",
                    "macro_regime": "N/A" if macro_row_backward is None else str(macro_row_backward.get("macro_signal") or "N/A"),
                    "macro_score": "N/A",
                    "final_context": "N/A",
                    "technical_action_note": "N/A: no COT row for selected week.",
                    "final_context_reason": "Cannot score without raw COT market/date row.",
                })
                continue

            has_real_positions = pd.notna(row.get("long_value")) and pd.notna(row.get("short_value")) and pd.notna(row.get("net_value"))
            cot_bias = str(row["cot_bias"]) if has_real_positions else "N/A"
            cot_score = float(row["cot_score"]) if has_real_positions and pd.notna(row.get("cot_score")) else None
            weekly = row.get("weekly_change")
            four = row.get("four_week_change")
            net = row.get("net_value")
            if pd.isna(weekly):
                weekly = None
            if pd.isna(four):
                four = None

            if has_real_positions:
                cot_interp = str(row.get("cot_interpretation") or "")
                cot_reason = (
                    f"Managed money net is {int(net)} (long {int(row['long_value'])}, short {int(row['short_value'])}); "
                    + (f"1w net change {int(weekly)}; " if weekly is not None else "1w net change N/A; ")
                    + (f"4w net change {int(four)}." if four is not None else "4w net change N/A.")
                )
                if cot_interp:
                    cot_reason = f"{cot_reason} {cot_interp}"
            else:
                cot_reason = "N/A: missing long/short values in source COT row; score suppressed."

            macro_row = macro_row_backward
            # Preserve known-good Cocoa alignment by allowing nearest forward macro snapshot.
            if market == "Cocoa" and not macro.empty:
                forward = macro[(macro["macro_snapshot_date"] > week_date) & (macro["macro_snapshot_date"] <= week_date + pd.Timedelta(days=7))]
                if not forward.empty:
                    macro_row = forward.iloc[0]

            macro_signal = None if macro_row is None else str(macro_row.get("macro_signal") or "")
            macro_score = None if macro_row is None else pd.to_numeric(pd.Series([macro_row.get("macro_score")]), errors="coerce").iloc[0]
            has_macro = macro_signal not in {None, "", "nan"} and pd.notna(macro_score)
            confluence_cot_bias = str(row.get("cot_directional_bias") or cot_bias)

            if has_macro and cot_score is not None:
                conf = _build_confluence(confluence_cot_bias, cot_score, macro_signal, float(macro_score))
                final_context = f"{conf['confluence_bias']} {conf['confluence_score']:.0f}"
                technical_note = conf["trade_readiness"]
                final_reason = conf["summary"]
                macro_regime = macro_signal
                macro_score_out = float(macro_score)
            else:
                final_context = "N/A"
                technical_note = "N/A: macro input unavailable."
                final_reason = "Cannot calculate final context because macro input is missing." if cot_score is not None else "Cannot calculate final context because COT long/short data is missing."
                macro_regime = "N/A"
                macro_score_out = "N/A"

            records.append({
                "date": date_str,
                "market": market,
                "latest_report_date": date_str,
                "cot_bias": cot_bias,
                "cot_score": int(round(cot_score)) if cot_score is not None else "N/A",
                "cot_reason": cot_reason,
                "macro_regime": macro_regime,
                "macro_score": macro_score_out,
                "final_context": final_context,
                "technical_action_note": technical_note,
                "final_context_reason": final_reason,
                "raw_cftc_market_name": str(row.get("raw_cftc_market_name", "")),
                "trader_group_used": f"{row.get('long_col_used','N/A')} / {row.get('short_col_used','N/A')}",
                "long_value": float(row["long_value"]) if pd.notna(row.get("long_value")) else None,
                "short_value": float(row["short_value"]) if pd.notna(row.get("short_value")) else None,
                "net_value": float(net) if pd.notna(net) else None,
                "missing_reason": None if pd.isna(row.get("missing_reason")) else str(row.get("missing_reason")),
                "previous_week_net": float(net - weekly) if weekly is not None else None,
                "one_week_net_change": weekly,
                "four_week_net_change": four,
                "bias_rule_used": "net>0 => Bullish; net<0 => Bearish; net==0 => Neutral",
                "score_rule_used": "_calculate_cot_scores from raw managed-money positioning",
                "final_calculated_cot_bias": cot_bias,
                "final_calculated_cot_score": int(round(cot_score)) if cot_score is not None else "N/A",
            })
            if market == "Cocoa":
                print(
                    "COCOA DEBUG:",
                    {
                        "report_date": date_str,
                        "raw_market_name": str(row.get("raw_cftc_market_name", "")),
                        "long_column_used": row.get("long_col_used"),
                        "short_column_used": row.get("short_col_used"),
                        "long_value": None if pd.isna(row.get("long_value")) else float(row.get("long_value")),
                        "short_value": None if pd.isna(row.get("short_value")) else float(row.get("short_value")),
                        "net": None if pd.isna(net) else float(net),
                        "1w_change": weekly,
                        "4w_change": four,
                        "cot_bias": cot_bias,
                        "cot_score": cot_score,
                    },
                )

    payload = {"generated_at": datetime.now(timezone.utc).isoformat(), "records": records}
    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUT_PATH.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    AUDIT_CSV_PATH.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(records).to_csv(AUDIT_CSV_PATH, index=False)
    print(f"Wrote {OUT_PATH} with {len(records)} rows")
    return OUT_PATH


if __name__ == "__main__":
    run()

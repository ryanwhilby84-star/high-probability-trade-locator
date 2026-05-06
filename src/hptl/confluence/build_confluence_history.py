from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd
from openpyxl import load_workbook
from openpyxl.styles import Font, PatternFill
from openpyxl.chart import BarChart, Reference
from openpyxl.utils import get_column_letter

from hptl.cot.exporter import _calculate_cot_scores

EXPORT_DIR = Path("data/exports")
PROCESSED_DIR = Path("data/processed")
HPTL_TARGET_MARKETS = {
    "COCOA - ICE FUTURES U.S.",
    "GOLD - COMMODITY EXCHANGE INC.",
    "CRUDE OIL, LIGHT SWEET - NEW YORK MERCANTILE EXCHANGE",
    "CORN - CHICAGO BOARD OF TRADE",
    "WHEAT - CHICAGO BOARD OF TRADE",
    "SOYBEANS - CHICAGO BOARD OF TRADE",
    "SILVER - COMMODITY EXCHANGE INC.",
    "COPPER-GRADE #1 - COMMODITY EXCHANGE INC.",
    "COFFEE C - ICE FUTURES U.S.",
    "NATURAL GAS - NEW YORK MERCANTILE EXCHANGE",
}
INDEX_MARKETS_TO_WARN = {
    "NASDAQ 100 STOCK INDEX - CHICAGO MERCANTILE EXCHANGE",
    "S&P 500 CONSOLIDATED - CHICAGO MERCANTILE EXCHANGE",
}


def _normalize_column_name(col: str) -> str:
    return " ".join(str(col).strip().lower().replace("_", " ").split())


def _find_column(df: pd.DataFrame, *aliases: str) -> str | None:
    normalized = {_normalize_column_name(col): col for col in df.columns}
    for alias in aliases:
        col = normalized.get(_normalize_column_name(alias))
        if col is not None:
            return col
    return None


def _clean_bias(value: Any) -> str:
    text = str(value).strip().lower()
    if "bull" in text or text in {"long", "buy"}:
        return "Bullish"
    if "bear" in text or text in {"short", "sell"}:
        return "Bearish"
    return "Neutral / Mixed"


def _clean_strength(value: Any) -> str:
    text = str(value).strip()
    if not text or text.lower() == "nan":
        return "Unknown"
    return text


def _macro_alignment_adjustment(score: float) -> int:
    if score <= 2:
        return 1
    if score <= 5:
        return 2
    if score <= 7:
        return 3
    return 4


def _macro_signal_label(signal: str) -> str:
    if signal == "risk_on":
        return "risk-on"
    if signal == "risk_off":
        return "risk-off"
    return "neutral"


def _discover_cot_files() -> list[Path]:
    files = sorted(PROCESSED_DIR.glob("cot_cleaned_*.csv"), key=lambda p: p.stat().st_mtime)
    if not files:
        raise FileNotFoundError("No COT history inputs found. Expected data/processed/cot_cleaned_*.csv")
    return files


def _discover_macro_files() -> tuple[list[Path], str, str]:
    history_files = sorted(EXPORT_DIR.glob("macro_history_*.xlsx"), key=lambda p: p.stat().st_mtime)
    if history_files:
        return history_files, "Macro_History", "macro_history_*.xlsx"

    output_files = sorted(EXPORT_DIR.glob("macro_output_*.xlsx"), key=lambda p: p.stat().st_mtime)
    if output_files:
        return output_files, "Macro_Dashboard", "macro_output_*.xlsx"

    raise FileNotFoundError(
        "No macro inputs found. Expected data/exports/macro_history_*.xlsx (preferred) or macro_output_*.xlsx"
    )


def _load_cot_file(cot_file: Path) -> pd.DataFrame:
    data = pd.read_csv(cot_file, low_memory=False)
    data = data.dropna(how="all")
    data = data.loc[:, ~data.columns.astype(str).str.startswith("Unnamed")]
    data.columns = [str(col).strip() for col in data.columns]

    market_col = _find_column(data, "market_and_exchange_names")
    date_col = _find_column(data, "report_date_as_yyyy_mm_dd")
    long_col = _find_column(data, "m_money_positions_long_other")
    short_col = _find_column(data, "m_money_positions_short_other")

    missing = []
    if market_col is None:
        missing.append("market_and_exchange_names")
    if date_col is None:
        missing.append("report_date_as_yyyy_mm_dd")
    if long_col is None:
        missing.append("m_money_positions_long_other")
    if short_col is None:
        missing.append("m_money_positions_short_other")
    if missing:
        raise ValueError(f"COT file {cot_file} missing required columns: {missing}")

    cleaned = pd.DataFrame()
    cleaned["market"] = data[market_col].astype(str).str.strip()
    cleaned["cot_report_date"] = pd.to_datetime(data[date_col], errors="coerce", dayfirst=True).dt.normalize()
    cleaned["managed_money_long"] = pd.to_numeric(data[long_col], errors="coerce")
    cleaned["managed_money_short"] = pd.to_numeric(data[short_col], errors="coerce")

    cleaned = cleaned[
        cleaned["market"].ne("")
        & cleaned["cot_report_date"].notna()
        & cleaned["managed_money_long"].notna()
        & cleaned["managed_money_short"].notna()
    ].copy()

    cleaned = cleaned[cleaned["market"].isin(HPTL_TARGET_MARKETS)].copy()
    cleaned = cleaned.sort_values(["market", "cot_report_date"]).reset_index(drop=True)

    cleaned["managed_money_net"] = cleaned["managed_money_long"] - cleaned["managed_money_short"]
    cleaned["noncommercial_net"] = cleaned["managed_money_net"]
    cleaned["commercial_net"] = cleaned["managed_money_net"]

    grouped = cleaned.groupby("market", sort=False)
    cleaned["weekly_change"] = grouped["managed_money_net"].diff(1)
    cleaned["four_week_change"] = grouped["managed_money_net"].diff(4)
    cleaned["mm_weekly_change"] = grouped["managed_money_net"].diff(1)

    scored = _calculate_cot_scores(cleaned)
    scored["cot_strength"] = scored["cot_strength"].apply(_clean_strength)
    scored["cot_bias"] = scored["cot_bias"].apply(_clean_bias)

    return scored[
        [
            "market",
            "cot_report_date",
            "cot_bias",
            "cot_score",
            "cot_strength",
            "managed_money_net",
            "weekly_change",
            "four_week_change",
        ]
    ]


def _load_cot_history(cot_files: list[Path]) -> pd.DataFrame:
    frames = []
    for f in cot_files:
        print(f"Loading COT file: {f}")
        frames.append(_load_cot_file(f).assign(_cot_source=str(f)))

    history = pd.concat(frames, ignore_index=True)
    history = history.sort_values(["market", "cot_report_date"]).drop_duplicates(
        subset=["market", "cot_report_date"], keep="last"
    )

    if history.empty:
        raise ValueError("COT history is empty after parsing/deduping.")

    present_markets = set(history["market"].dropna().astype(str).str.strip().unique())
    missing_index_markets = sorted(INDEX_MARKETS_TO_WARN - present_markets)
    if missing_index_markets:
        print("WARNING: The following index markets were not found in cleaned COT data:")
        for market in missing_index_markets:
            print(f"  - {market}")
        print("Continuing without index markets.")

    return history


def _load_macro_history(macro_files: list[Path], sheet_name: str) -> pd.DataFrame:
    frames = []

    for f in macro_files:
        print(f"Loading macro file: {f}")

        try:
            macro = pd.read_excel(f, sheet_name=sheet_name)
        except ValueError:
            print(f"  skipped: {sheet_name} sheet not found")
            continue

        if macro.empty:
            print("  skipped: empty Macro_Dashboard")
            continue

        macro = macro.copy()
        macro.columns = [str(c).strip() for c in macro.columns]

        if "macro_snapshot_date" not in macro.columns:
            print(f"  skipped: missing macro_snapshot_date. Columns: {macro.columns.tolist()}")
            continue

        macro["macro_snapshot_date"] = pd.to_datetime(
            macro["macro_snapshot_date"], errors="coerce"
        ).dt.normalize()

        if "macro_signal" not in macro.columns:
            macro["macro_signal"] = "neutral"
        else:
            macro["macro_signal"] = macro["macro_signal"].astype(str).str.strip().str.lower()

        if "macro_score" not in macro.columns:
            macro["macro_score"] = 0
        else:
            macro["macro_score"] = pd.to_numeric(
                macro["macro_score"], errors="coerce"
            ).fillna(0).clip(0, 10)

        if "macro_strength" not in macro.columns:
            macro["macro_strength"] = "Unknown"

        if "macro_context_for_trades" not in macro.columns:
            macro["macro_context_for_trades"] = ""

        macro["_macro_source"] = str(f)

        usable = macro[
            [
                "macro_snapshot_date",
                "macro_signal",
                "macro_score",
                "macro_strength",
                "macro_context_for_trades",
                "_macro_source",
            ]
        ].copy()

        usable = usable[usable["macro_snapshot_date"].notna()]
        if usable.empty:
            print("  skipped: no usable macro_snapshot_date rows")
            continue

        frames.append(usable)

    if not frames:
        raise ValueError("No usable Macro_Dashboard rows found in macro history files.")

    history = pd.concat(frames, ignore_index=True)
    history = history.sort_values(["macro_snapshot_date", "_macro_source"]).drop_duplicates(
        subset=["macro_snapshot_date"], keep="last"
    )

    if history.empty:
        raise ValueError("Macro history is empty after parsing/deduping.")

    return history


def _build_confluence(cot_bias: str, cot_score: float, macro_signal: str, macro_score: float) -> dict[str, Any]:
    cot_dir = "long" if cot_bias == "Bullish" else "short" if cot_bias == "Bearish" else "neutral"
    macro_dir = "long" if macro_signal == "risk_on" else "short" if macro_signal == "risk_off" else "neutral"
    macro_label = _macro_signal_label(macro_signal)

    hard_conflict = (
        cot_dir in {"long", "short"}
        and macro_dir in {"long", "short"}
        and cot_dir != macro_dir
        and cot_score >= 6
        and macro_score >= 6
    )

    if hard_conflict:
        return {
            "cot_macro_alignment": "Hard Conflict",
            "macro_effect_on_cot": "Blocking",
            "combined_context_score": 0,
            "combined_context_label": "Conflicted / Stand Down",
            "confluence_bias": "Conflicted / No Trade",
            "confluence_score": 0,
            "confluence_strength": "Blocked",
            "trade_readiness": "Stand down",
            "confluence_read": (
                f"Managed money is {cot_bias.lower()} but macro is {macro_label}, creating a hard conflict with "
                "high-conviction context on both sides."
            ),
            "summary": f"COT {cot_bias} ({cot_score}) conflicts with macro {macro_signal} ({macro_score}) at high conviction.",
        }

    score = cot_score

    directional_macro = cot_dir in {"long", "short"} and macro_dir in {"long", "short"}
    if directional_macro:
        delta = _macro_alignment_adjustment(macro_score)
        score = score + delta if cot_dir == macro_dir else score - delta

    score = max(0, min(10, score))

    if cot_dir == "neutral" or macro_dir == "neutral":
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
        strength = "Very Strong"
        readiness = "High conviction"
    elif score >= 6:
        strength = "Strong"
        readiness = "Actionable"
    elif score >= 3:
        strength = "Moderate"
        readiness = "Cautious"
    else:
        strength = "Weak"
        readiness = "Low conviction"

    if cot_dir == "neutral" or macro_dir == "neutral":
        cot_macro_alignment = "Neutral / Mixed"
        macro_effect_on_cot = "Neutral"
    elif cot_dir == macro_dir:
        cot_macro_alignment = "Aligned"
        macro_effect_on_cot = "Boosting"
    else:
        cot_macro_alignment = "Headwind"
        macro_effect_on_cot = "Reducing"

    if score >= 9:
        combined_context_label = "Very Strong Bullish Context" if cot_dir == "long" else "Very Strong Bearish Context"
    elif score >= 7:
        combined_context_label = "Strong Bullish Context" if cot_dir == "long" else "Strong Bearish Context"
    elif score >= 5:
        combined_context_label = "Moderate Bullish Context" if cot_dir == "long" else "Moderate Bearish Context"
    else:
        combined_context_label = "Weak / Neutral Context"

    if cot_dir == "neutral":
        confluence_read = f"Managed money positioning is neutral/mixed and macro is {macro_label}, leaving unclear directional context."
    elif cot_macro_alignment == "Aligned":
        confluence_read = f"Managed money is {cot_bias.lower()} and macro is {macro_label}, giving supportive {cot_dir}-side context."
    elif cot_macro_alignment == "Headwind":
        confluence_read = f"Managed money is {cot_bias.lower()} but macro is {macro_label}, so {cot_dir}-side context faces macro headwind."
    else:
        confluence_read = f"Managed money is {cot_bias.lower()} while macro is {macro_label}, resulting in mixed context."

    return {
        "cot_macro_alignment": cot_macro_alignment,
        "macro_effect_on_cot": macro_effect_on_cot,
        "combined_context_score": score,
        "combined_context_label": combined_context_label,
        "confluence_bias": bias,
        "confluence_score": score,
        "confluence_strength": strength,
        "trade_readiness": readiness,
        "confluence_read": confluence_read,
        "summary": f"COT {cot_bias} ({cot_score}) vs macro {macro_signal} ({macro_score}) => {bias} {score:.1f}.",
    }


def run() -> Path:
    print("=" * 70)
    print("Confluence history build started")
    print("=" * 70)

    cot_files = _discover_cot_files()
    macro_files, macro_sheet, macro_pattern = _discover_macro_files()

    cot = _load_cot_history(cot_files)
    macro = _load_macro_history(macro_files, macro_sheet)

    print(f"COT date range: {cot['cot_report_date'].min()} -> {cot['cot_report_date'].max()}")
    print(f"Macro date range: {macro['macro_snapshot_date'].min()} -> {macro['macro_snapshot_date'].max()}")
    print("Alignment mode: backward (as-of historical snapshot)")

    aligned = pd.merge_asof(
        cot.sort_values("cot_report_date"),
        macro.sort_values("macro_snapshot_date"),
        left_on="cot_report_date",
        right_on="macro_snapshot_date",
        direction="backward",
    )

    aligned = aligned[aligned["macro_snapshot_date"].notna()].copy()
    aligned_rows = len(aligned)
    dropped_rows = len(cot) - aligned_rows
    if dropped_rows > 0:
        print("WARNING: Macro history does not fully cover COT history.")
        print(f"  COT date range: {cot['cot_report_date'].min()} -> {cot['cot_report_date'].max()}")
        print(f"  Macro date range: {macro['macro_snapshot_date'].min()} -> {macro['macro_snapshot_date'].max()}")
        print(f"  Rows aligned: {aligned_rows}")
        print(f"  Rows dropped: {dropped_rows}")

    if aligned.empty:
        raise ValueError("No COT rows could be aligned to available macro snapshots.")

    aligned["macro_alignment_gap_days"] = (
        aligned["cot_report_date"] - aligned["macro_snapshot_date"]
    ).dt.days

    confluence_bits = aligned.apply(
        lambda r: _build_confluence(
            cot_bias=str(r["cot_bias"]),
            cot_score=float(r["cot_score"]),
            macro_signal=str(r["macro_signal"]),
            macro_score=float(r["macro_score"]),
        ),
        axis=1,
        result_type="expand",
    )

    out = pd.concat([aligned, confluence_bits], axis=1)
    out["date"] = out["cot_report_date"].dt.date
    out = out.sort_values(["market", "cot_report_date"]).reset_index(drop=True)
    cot_score_change = out.groupby("market", sort=False)["cot_score"].diff(1)
    out["positioning_trend"] = "Flat / Unclear"
    out.loc[cot_score_change.isna(), "positioning_trend"] = "New / Insufficient History"
    out.loc[cot_score_change >= 1.0, "positioning_trend"] = "Strengthening"
    out.loc[cot_score_change <= -1.0, "positioning_trend"] = "Weakening"

    final_columns = [
        "date",
        "market",
        "combined_context_label",
        "combined_context_score",
        "cot_macro_alignment",
        "macro_effect_on_cot",
        "positioning_trend",
        "confluence_read",
        "cot_bias",
        "cot_score",
        "cot_strength",
        "macro_snapshot_date",
        "macro_signal",
        "macro_score",
        "macro_strength",
        "macro_context_for_trades",
        "managed_money_net",
        "weekly_change",
        "four_week_change",
        "_cot_source",
        "_macro_source",
        "macro_alignment_gap_days",
        "confluence_bias",
        "confluence_score",
        "confluence_strength",
        "trade_readiness",
        "summary",
    ]

    out = out[final_columns].sort_values(["date", "market"]).reset_index(drop=True)

    EXPORT_DIR.mkdir(parents=True, exist_ok=True)
    output_path = EXPORT_DIR / f"confluence_history_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"

    latest_date = out["date"].max()
    dashboard_columns = [
        "date",
        "market",
        "combined_context_label",
        "combined_context_score",
        "cot_macro_alignment",
        "macro_effect_on_cot",
        "positioning_trend",
        "confluence_read",
        "cot_bias",
        "cot_score",
        "cot_strength",
        "macro_signal",
        "macro_score",
        "macro_strength",
        "macro_context_for_trades",
    ]
    dashboard = out[out["date"] == latest_date][dashboard_columns].sort_values("market").reset_index(drop=True)

    cot_input_columns = [
        "date",
        "market",
        "cot_bias",
        "cot_score",
        "cot_strength",
        "managed_money_net",
        "weekly_change",
        "four_week_change",
    ]
    cot_input = out[cot_input_columns].copy()

    macro_input_columns = [
        "macro_snapshot_date",
        "macro_signal",
        "macro_score",
        "macro_strength",
        "macro_context_for_trades",
    ]
    for optional_col in ["rates_bias", "curve_context", "policy_pressure", "macro_summary"]:
        if optional_col in macro.columns:
            macro_input_columns.append(optional_col)
    macro_input = macro[macro_input_columns].sort_values("macro_snapshot_date").reset_index(drop=True)

    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        dashboard.to_excel(writer, sheet_name="Confluence_Dashboard", index=False)
        out.to_excel(writer, sheet_name="Confluence_History", index=False)
        cot_input.to_excel(writer, sheet_name="COT_Input", index=False)
        macro_input.to_excel(writer, sheet_name="Macro_Input", index=False)
        pd.DataFrame().to_excel(writer, sheet_name="Summary_Charts", index=False)

    wb = load_workbook(output_path)
    history_ws = wb["Confluence_History"]
    dashboard_ws = wb["Confluence_Dashboard"]
    cot_input_ws = wb["COT_Input"]
    macro_input_ws = wb["Macro_Input"]
    summary_ws = wb["Summary_Charts"]

    def _format_table_sheet(ws) -> None:
        ws.freeze_panes = "A2"
        ws.auto_filter.ref = ws.dimensions
        for cell in ws[1]:
            cell.font = Font(bold=True)
        for col_cells in ws.columns:
            width = max(len(str(c.value)) if c.value is not None else 0 for c in col_cells)
            ws.column_dimensions[get_column_letter(col_cells[0].column)].width = min(width + 2, 80)

    for ws in [dashboard_ws, history_ws, cot_input_ws, macro_input_ws]:
        _format_table_sheet(ws)

    green_fill = PatternFill(fill_type="solid", fgColor="C6EFCE")
    amber_fill = PatternFill(fill_type="solid", fgColor="FFEB9C")
    red_fill = PatternFill(fill_type="solid", fgColor="FFC7CE")

    def _apply_confluence_colors(ws) -> None:
        header_to_index = {cell.value: idx + 1 for idx, cell in enumerate(ws[1])}
        score_col = header_to_index.get("combined_context_score")
        alignment_col = header_to_index.get("cot_macro_alignment")
        if score_col:
            for row in range(2, ws.max_row + 1):
                cell = ws.cell(row=row, column=score_col)
                value = cell.value
                if isinstance(value, (int, float)):
                    if value >= 8:
                        cell.fill = green_fill
                    elif value >= 5:
                        cell.fill = amber_fill
                    else:
                        cell.fill = red_fill
        if alignment_col:
            for row in range(2, ws.max_row + 1):
                cell = ws.cell(row=row, column=alignment_col)
                value = str(cell.value).strip()
                if value == "Aligned":
                    cell.fill = green_fill
                elif value == "Headwind":
                    cell.fill = amber_fill
                elif value == "Hard Conflict":
                    cell.fill = red_fill

    _apply_confluence_colors(dashboard_ws)
    _apply_confluence_colors(history_ws)

    summary_ws["A1"] = "Dashboard uses latest available date. Historical rows can be filtered by date in Confluence_History."
    summary_ws["A1"].font = Font(bold=True)

    summary_ws["A3"] = "Market"
    summary_ws["B3"] = "combined_context_score"
    for idx, row in enumerate(dashboard.itertuples(index=False), start=4):
        summary_ws.cell(row=idx, column=1, value=row.market)
        summary_ws.cell(row=idx, column=2, value=row.combined_context_score)
    for header_cell in summary_ws[3]:
        header_cell.font = Font(bold=True)

    bar_chart = BarChart()
    bar_chart.title = f"Combined Context Score by Market ({latest_date})"
    bar_chart.y_axis.title = "Score"
    bar_chart.x_axis.title = "Market"
    data = Reference(summary_ws, min_col=2, min_row=3, max_row=3 + len(dashboard))
    categories = Reference(summary_ws, min_col=1, min_row=4, max_row=3 + len(dashboard))
    bar_chart.add_data(data, titles_from_data=True)
    bar_chart.set_categories(categories)
    bar_chart.width = 12
    bar_chart.height = 6
    summary_ws.add_chart(bar_chart, "D3")

    category_specs = [
        ("combined_context_label", "Combined Context Label Counts", "A20", "D20"),
        ("cot_macro_alignment", "COT/Macro Alignment Counts", "F20", "I20"),
        ("macro_signal", "Macro Signal Counts", "A38", "D38"),
    ]
    for field, chart_title, start_cell, chart_cell in category_specs:
        start_col = ord(start_cell[0]) - ord("A") + 1
        start_row = int(start_cell[1:])
        counts = dashboard[field].value_counts(dropna=False).sort_index()
        summary_ws.cell(row=start_row, column=start_col, value=field)
        summary_ws.cell(row=start_row, column=start_col + 1, value="count")
        summary_ws.cell(row=start_row, column=start_col,).font = Font(bold=True)
        summary_ws.cell(row=start_row, column=start_col + 1).font = Font(bold=True)
        for r_offset, (label, cnt) in enumerate(counts.items(), start=1):
            summary_ws.cell(row=start_row + r_offset, column=start_col, value=str(label))
            summary_ws.cell(row=start_row + r_offset, column=start_col + 1, value=int(cnt))
        count_chart = BarChart()
        count_chart.title = chart_title
        count_chart.y_axis.title = "Count"
        count_chart.x_axis.title = field
        count_data = Reference(
            summary_ws, min_col=start_col + 1, min_row=start_row, max_row=start_row + len(counts)
        )
        count_categories = Reference(
            summary_ws, min_col=start_col, min_row=start_row + 1, max_row=start_row + len(counts)
        )
        count_chart.add_data(count_data, titles_from_data=True)
        count_chart.set_categories(count_categories)
        count_chart.width = 10
        count_chart.height = 6
        summary_ws.add_chart(count_chart, chart_cell)

    for col_cells in summary_ws.columns:
        width = max(len(str(c.value)) if c.value is not None else 0 for c in col_cells)
        summary_ws.column_dimensions[get_column_letter(col_cells[0].column)].width = min(width + 2, 45)

    wb.save(output_path)

    print("=" * 70)
    print(f"COT input files used: {len(cot_files)}")
    for f in cot_files:
        print(f"  - {f}")
    print(f"Macro input files used ({macro_pattern}, sheet={macro_sheet}): {len(macro_files)}")
    for f in macro_files:
        print(f"  - {f}")
    print(f"Date range covered: {out['date'].min()} -> {out['date'].max()}")
    print(f"Rows exported: {len(out)}")
    print(f"Output file path: {output_path}")
    print("=" * 70)

    return output_path


if __name__ == "__main__":
    run()

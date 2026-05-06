from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd
from openpyxl import load_workbook
from openpyxl.styles import Font
from openpyxl.utils import get_column_letter

from hptl.cot.exporter import _calculate_cot_scores

EXPORT_DIR = Path("data/exports")
PROCESSED_DIR = Path("data/processed")


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
    long_col = _find_column(data, "nonrept_positions_long_other")
    short_col = _find_column(data, "nonrept_positions_short_other")

    missing = []
    if market_col is None:
        missing.append("market_and_exchange_names")
    if date_col is None:
        missing.append("report_date_as_yyyy_mm_dd")
    if long_col is None:
        missing.append("nonrept_positions_long_other")
    if short_col is None:
        missing.append("nonrept_positions_short_other")
    if missing:
        raise ValueError(f"COT file {cot_file} missing required columns: {missing}")

    cleaned = pd.DataFrame()
    cleaned["market"] = data[market_col].astype(str).str.strip()
    cleaned["cot_report_date"] = pd.to_datetime(data[date_col], errors="coerce", dayfirst=True).dt.normalize()
    cleaned["noncommercial_long"] = pd.to_numeric(data[long_col], errors="coerce")
    cleaned["noncommercial_short"] = pd.to_numeric(data[short_col], errors="coerce")

    cleaned = cleaned[
        cleaned["market"].ne("")
        & cleaned["cot_report_date"].notna()
        & cleaned["noncommercial_long"].notna()
        & cleaned["noncommercial_short"].notna()
    ].copy()

    cleaned = cleaned.sort_values(["market", "cot_report_date"]).reset_index(drop=True)

    cleaned["noncommercial_net"] = cleaned["noncommercial_long"] - cleaned["noncommercial_short"]
    cleaned["commercial_net"] = cleaned["noncommercial_net"]

    grouped = cleaned.groupby("market", sort=False)
    cleaned["weekly_change"] = grouped["noncommercial_net"].diff(1)
    cleaned["four_week_change"] = grouped["noncommercial_net"].diff(4)
    cleaned["mm_weekly_change"] = grouped["noncommercial_net"].diff(1)

    scored = _calculate_cot_scores(cleaned)
    scored["cot_strength"] = scored["cot_strength"].apply(_clean_strength)
    scored["cot_bias"] = scored["cot_bias"].apply(_clean_bias)

    return scored[["market", "cot_report_date", "cot_bias", "cot_score", "cot_strength"]]


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

    return {
        "confluence_bias": bias,
        "confluence_score": score,
        "confluence_strength": strength,
        "trade_readiness": readiness,
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

    final_columns = [
        "date",
        "market",
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

    out = out[final_columns].sort_values(["date", "market"]).reset_index(drop=True)

    EXPORT_DIR.mkdir(parents=True, exist_ok=True)
    output_path = EXPORT_DIR / f"confluence_history_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"

    out.to_excel(output_path, sheet_name="Confluence_History", index=False)

    wb = load_workbook(output_path)
    ws = wb["Confluence_History"]

    ws.freeze_panes = "A2"
    ws.auto_filter.ref = ws.dimensions

    for cell in ws[1]:
        cell.font = Font(bold=True)

    for col_cells in ws.columns:
        width = max(len(str(c.value)) if c.value is not None else 0 for c in col_cells)
        ws.column_dimensions[get_column_letter(col_cells[0].column)].width = min(width + 2, 80)

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

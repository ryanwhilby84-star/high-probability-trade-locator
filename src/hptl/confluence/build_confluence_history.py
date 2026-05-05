from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd
from openpyxl import load_workbook
from openpyxl.styles import Font
from openpyxl.utils import get_column_letter

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


def _strength_to_score(strength: Any) -> float:
    text = str(strength).strip().lower()
    if "very strong" in text or "strongly" in text:
        return 8
    if text == "strong":
        return 7
    if text == "moderate":
        return 5
    if text == "weak":
        return 2
    return 0


def _macro_alignment_adjustment(score: float) -> int:
    if score <= 2:
        return 1
    if score <= 5:
        return 2
    if score <= 7:
        return 3
    return 4


def _discover_cot_files() -> list[Path]:
    files = sorted(
        list(EXPORT_DIR.glob("cot_update_*.xlsx")) + list(PROCESSED_DIR.glob("cot_cleaned_*.csv")),
        key=lambda p: p.stat().st_mtime,
    )
    if not files:
        raise FileNotFoundError(
            "No COT history inputs found. Expected data/exports/cot_update_*.xlsx and/or data/processed/cot_cleaned_*.csv"
        )
    return files


def _discover_macro_files() -> list[Path]:
    files = sorted(list(EXPORT_DIR.glob("macro_output_*.xlsx")), key=lambda p: p.stat().st_mtime)
    if not files:
        raise FileNotFoundError(
            "No macro history inputs found. Expected data/exports/macro_output_*.xlsx"
        )
    return files


def _load_cot_file(cot_file: Path) -> pd.DataFrame:
    if cot_file.suffix.lower() == ".xlsx":
        raw = pd.read_excel(cot_file, sheet_name="Dashboard", header=None)
        header_idx = None
        for idx, row in raw.iterrows():
            if row.astype(str).str.strip().eq("Market").any():
                header_idx = idx
                break
        if header_idx is None:
            raise ValueError(f"Could not find 'Market' header row in {cot_file}")

        header = raw.iloc[header_idx].astype(str).str.strip().tolist()
        data = raw.iloc[header_idx + 1 :].copy()
        data.columns = header
    else:
        data = pd.read_csv(cot_file)

    data = data.dropna(how="all")
    data = data.loc[:, ~data.columns.astype(str).str.startswith("Unnamed")]
    data.columns = [str(col).strip() for col in data.columns]

    market_col = _find_column(data, "market")
    date_col = _find_column(data, "cot_report_date", "report date", "date")
    bias_col = _find_column(data, "cot_bias", "bias", "signal")
    score_col = _find_column(data, "cot_score", "score")
    strength_col = _find_column(data, "cot_strength", "strength")

    if market_col is None:
        raise ValueError(f"COT data missing market column in {cot_file}")
    if date_col is None:
        raise ValueError(f"COT data missing report date column in {cot_file}")

    cleaned = pd.DataFrame()
    cleaned["market"] = data[market_col].astype(str).str.strip()
    cleaned["cot_report_date"] = pd.to_datetime(data[date_col], errors="coerce").dt.normalize()
    cleaned["cot_bias"] = data[bias_col].apply(_clean_bias) if bias_col else "Neutral / Mixed"
    cleaned["cot_score"] = pd.to_numeric(data[score_col], errors="coerce") if score_col else pd.NA
    cleaned["cot_strength"] = data[strength_col].apply(_clean_strength) if strength_col else "Unknown"

    cleaned = cleaned[cleaned["market"].ne("") & cleaned["cot_report_date"].notna()].copy()

    missing_score = cleaned["cot_score"].isna()
    cleaned.loc[missing_score, "cot_score"] = pd.to_numeric(
        cleaned.loc[missing_score, "cot_strength"].apply(_strength_to_score), errors="coerce"
    )
    cleaned["cot_score"] = pd.to_numeric(cleaned["cot_score"], errors="coerce").fillna(0).clip(0, 10)

    return cleaned


def _load_cot_history(cot_files: list[Path]) -> pd.DataFrame:
    frames = []
    for f in cot_files:
        df = _load_cot_file(f)
        df["_cot_source"] = str(f)
        frames.append(df)

    history = pd.concat(frames, ignore_index=True)
    history = history.sort_values(["market", "cot_report_date"]).drop_duplicates(
        subset=["market", "cot_report_date"], keep="last"
    )
    if history.empty:
        raise ValueError("COT history is empty after parsing/deduping.")
    return history


def _load_macro_history(macro_files: list[Path]) -> pd.DataFrame:
    frames = []
    for f in macro_files:
        macro = pd.read_excel(f, sheet_name="Macro_Dashboard")
        if macro.empty:
            continue
        macro = macro.copy()
        macro["macro_snapshot_date"] = pd.to_datetime(macro.get("macro_snapshot_date"), errors="coerce").dt.normalize()
        macro["macro_signal"] = macro.get("macro_signal", "").astype(str).str.strip().str.lower()
        macro["macro_score"] = pd.to_numeric(macro.get("macro_score"), errors="coerce").fillna(0).clip(0, 10)
        macro["macro_strength"] = macro.get("macro_strength")
        macro["macro_context_for_trades"] = macro.get("macro_context_for_trades")
        macro["_macro_source"] = str(f)
        frames.append(macro[["macro_snapshot_date", "macro_signal", "macro_score", "macro_strength", "macro_context_for_trades", "_macro_source"]])

    if not frames:
        raise ValueError("No usable Macro_Dashboard rows found in macro history files.")

    history = pd.concat(frames, ignore_index=True)
    history = history[history["macro_snapshot_date"].notna()].copy()
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
            "summary": f"{cot_bias} COT conflicts with {macro_signal} macro at high conviction.",
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
    macro_files = _discover_macro_files()

    cot = _load_cot_history(cot_files)
    macro = _load_macro_history(macro_files)

    aligned = pd.merge_asof(
        cot.sort_values("cot_report_date"),
        macro.sort_values("macro_snapshot_date"),
        left_on="cot_report_date",
        right_on="macro_snapshot_date",
        direction="backward",
    )

    aligned = aligned[aligned["macro_snapshot_date"].notna()].copy()
    if aligned.empty:
        raise ValueError("No COT rows could be aligned to macro snapshots on/before report dates.")

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

    bold = Font(bold=True)
    for cell in ws[1]:
        cell.font = bold

    for col_cells in ws.columns:
        width = max(len(str(c.value)) if c.value is not None else 0 for c in col_cells)
        ws.column_dimensions[get_column_letter(col_cells[0].column)].width = min(width + 2, 80)

    wb.save(output_path)

    print(f"COT input files used ({len(cot_files)}):")
    for f in cot_files:
        print(f"  - {f}")
    print(f"Macro input files used ({len(macro_files)}):")
    for f in macro_files:
        print(f"  - {f}")

    min_date = out["date"].min()
    max_date = out["date"].max()
    print(f"Date range covered: {min_date} -> {max_date}")
    print(f"Rows exported: {len(out)}")
    print(f"Output file path: {output_path}")
    print("=" * 70)

    return output_path


if __name__ == "__main__":
    run()

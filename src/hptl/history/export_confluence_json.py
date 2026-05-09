from __future__ import annotations

import argparse
import json
import math
from datetime import date, datetime
from pathlib import Path

import pandas as pd

EXPORTS_DIR = Path("data/exports")
PUBLIC_OUTPUT_PATH = Path("web-dashboard/public/data/confluence_history_latest.json")


PREFERRED_SHEETS = ["Confluence_History", "Confluence_Dashboard", "Dashboard", "Trader_Report"]


REQUIRED_COLUMNS = [
    "date",
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


def _normalize_column_name(col: str) -> str:
    return "_".join(str(col).strip().lower().replace("/", " ").replace("-", " ").split())


def _find_latest_workbook() -> Path:
    files = sorted(EXPORTS_DIR.glob("confluence_history_*.xlsx"), key=lambda p: p.stat().st_mtime)
    if not files:
        raise FileNotFoundError("No files found matching data/exports/confluence_history_*.xlsx")
    return files[-1]


def _resolve_workbook(input_path: str | None) -> Path:
    if input_path:
        workbook = Path(input_path)
        if not workbook.exists():
            raise FileNotFoundError(f"Input workbook not found: {workbook}")
        return workbook
    return _find_latest_workbook()


def _pick_sheet(workbook: Path) -> str:
    xl = pd.ExcelFile(workbook)
    for sheet in PREFERRED_SHEETS:
        if sheet in xl.sheet_names:
            return sheet
    return xl.sheet_names[0]


def _load_and_clean(workbook: Path, sheet_name: str) -> pd.DataFrame:
    df = pd.read_excel(workbook, sheet_name=sheet_name)
    df = df.dropna(how="all").copy()
    renames = {c: _normalize_column_name(c) for c in df.columns}
    df = df.rename(columns=renames)

    if "cot_report_date" not in df.columns and "date" in df.columns:
        df["cot_report_date"] = df["date"]
    if "date" not in df.columns and "cot_report_date" in df.columns:
        df["date"] = df["cot_report_date"]

    missing = [c for c in REQUIRED_COLUMNS if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns in {workbook.name}:{sheet_name}: {missing}")

    date_like_columns = ["cot_report_date", "date", "macro_snapshot_date"]
    for col in date_like_columns:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce").dt.strftime("%Y-%m-%d")
    df["confluence_score"] = pd.to_numeric(df["confluence_score"], errors="coerce")
    df["cot_score"] = pd.to_numeric(df["cot_score"], errors="coerce")
    df["macro_score"] = pd.to_numeric(df["macro_score"], errors="coerce")

    df["date"] = df["date"].fillna(df["cot_report_date"])
    df["cot_report_date"] = df["cot_report_date"].fillna(df["date"])
    df = df[df["date"].notna()].copy()
    df = df.sort_values(["cot_report_date", "market"]).reset_index(drop=True)
    return df


def _sanitize_json_values(value):
    if isinstance(value, dict):
        return {k: _sanitize_json_values(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_sanitize_json_values(v) for v in value]

    if value is pd.NA or value is pd.NaT:
        return None

    if isinstance(value, (pd.Timestamp, datetime, date)):
        return value.isoformat()

    if hasattr(value, "item") and not isinstance(value, (str, bytes)):
        try:
            value = value.item()
        except Exception:
            pass

    if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
        return None

    return value


def run(input_path: str | None = None) -> Path:
    workbook = _resolve_workbook(input_path)
    sheet = _pick_sheet(workbook)

    print(f"Selected workbook: {workbook}")
    print(f"Selected sheet: {sheet}")

    data = _load_and_clean(workbook, sheet)

    records = data[REQUIRED_COLUMNS].copy()
    records["macro_regime"] = records["macro_signal"]
    records["final_bias"] = records["confluence_bias"]
    records["final_score"] = records["confluence_score"]
    records["readiness_label"] = records["trade_readiness"]
    latest_report_date = records["cot_report_date"].dropna().max() if not records.empty else None

    payload = {
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "latest_report_date": latest_report_date,
        "total_rows": int(len(records)),
        "source_files_used": [str(workbook)],
        "source_sheet": sheet,
        "records": records.to_dict(orient="records"),
    }
    payload = _sanitize_json_values(payload)

    PUBLIC_OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    PUBLIC_OUTPUT_PATH.write_text(json.dumps(payload, indent=2, allow_nan=False, default=str), encoding="utf-8")

    print("Dashboard JSON written: yes")
    print(f"Output path: {PUBLIC_OUTPUT_PATH}")
    print(f"Row count: {len(records)}")
    print(f"Latest report date: {latest_report_date}")
    print("Confirm: valid JSON written")
    return PUBLIC_OUTPUT_PATH


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Export confluence history workbook to JSON")
    parser.add_argument(
        "--input",
        dest="input_path",
        type=str,
        default=None,
        help="Path to a confluence history workbook (.xlsx)",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    run(input_path=args.input_path)

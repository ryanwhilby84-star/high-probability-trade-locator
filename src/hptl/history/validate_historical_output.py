from __future__ import annotations

from pathlib import Path

import pandas as pd

EXPORT_DIR = Path("data/exports")
TARGET_MARKETS = [
    "NASDAQ",
    "S&P 500",
    "Gold",
    "Silver",
    "Copper",
    "Crude Oil",
    "Natural Gas",
    "Corn",
    "Soybeans",
    "Wheat",
    "Coffee",
    "Cocoa",
]
REQUIRED_COLUMNS = [
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
MIN_DATE = pd.Timestamp("2024-05-06")


def _find_latest_workbook() -> Path:
    candidates = [
        *EXPORT_DIR.glob("historical_context_*.xlsx"),
        *EXPORT_DIR.glob("confluence*.xlsx"),
        *EXPORT_DIR.glob("*confluence*.xlsx"),
    ]
    candidates = [p for p in candidates if p.is_file()]
    if not candidates:
        raise FileNotFoundError("No historical/confluence Excel workbook found in data/exports")
    return max(candidates, key=lambda p: p.stat().st_mtime)


def _contains_target_market(value: object) -> str | None:
    text = str(value).strip().lower()
    for target in TARGET_MARKETS:
        if target.lower() in text:
            return target
    return None


def _print_result(rule_num: int, description: str, passed: bool, details: str = "") -> bool:
    status = "PASS" if passed else "FAIL"
    suffix = f" | {details}" if details else ""
    print(f"Rule {rule_num}: {status} - {description}{suffix}")
    return passed


def run() -> int:
    all_passed = True

    try:
        workbook_path = _find_latest_workbook()
        print(f"Validating workbook: {workbook_path}")
    except Exception as exc:  # noqa: BLE001
        print(f"Unable to find workbook: {exc}")
        for i, description in [
            (1, "Workbook must contain sheet Confluence_Dashboard."),
            (2, "Required columns must exist exactly."),
            (3, "cot_report_date must not be blank."),
            (4, "Date range must include rows from at least 2024-05-06 onward."),
            (5, "Must include target instruments."),
            (6, "No unrelated random CFTC markets allowed."),
            (7, "Each target market should have multiple weekly rows."),
            (8, "macro_snapshot_date must not be reused as one single date for all historical rows."),
            (9, "confluence_score must be numeric 0-10."),
        ]:
            _print_result(i, description, False, "No workbook available")
        return 1

    excel_file = pd.ExcelFile(workbook_path)

    has_sheet = "Confluence_Dashboard" in excel_file.sheet_names
    all_passed &= _print_result(1, "Workbook must contain sheet Confluence_Dashboard.", has_sheet)
    if not has_sheet:
        for i, description in [
            (2, "Required columns must exist exactly."),
            (3, "cot_report_date must not be blank."),
            (4, "Date range must include rows from at least 2024-05-06 onward."),
            (5, "Must include target instruments."),
            (6, "No unrelated random CFTC markets allowed."),
            (7, "Each target market should have multiple weekly rows."),
            (8, "macro_snapshot_date must not be reused as one single date for all historical rows."),
            (9, "confluence_score must be numeric 0-10."),
        ]:
            all_passed &= _print_result(i, description, False, "Missing required sheet")
        return 1

    df = pd.read_excel(workbook_path, sheet_name="Confluence_Dashboard")

    columns_exact = list(df.columns) == REQUIRED_COLUMNS
    details = "" if columns_exact else f"found={list(df.columns)}"
    all_passed &= _print_result(2, "Required columns must exist exactly.", columns_exact, details)

    cot_dates = pd.to_datetime(df.get("cot_report_date"), errors="coerce")
    cot_dates_non_blank = cot_dates.notna().all() and len(cot_dates) > 0
    all_passed &= _print_result(3, "cot_report_date must not be blank.", cot_dates_non_blank)

    date_range_ok = cot_dates.notna().any() and cot_dates.max() >= MIN_DATE
    date_details = "" if date_range_ok else f"max_cot_report_date={cot_dates.max()}"
    all_passed &= _print_result(4, "Date range must include rows from at least 2024-05-06 onward.", date_range_ok, date_details)

    matched_targets = df["market"].map(_contains_target_market).dropna() if "market" in df.columns else pd.Series(dtype=str)
    present_targets = sorted(set(matched_targets.tolist()))
    missing_targets = [m for m in TARGET_MARKETS if m not in present_targets]
    targets_ok = len(missing_targets) == 0
    all_passed &= _print_result(5, "Must include target instruments.", targets_ok, f"missing={missing_targets}" if missing_targets else "")

    allowed_set = set(TARGET_MARKETS)
    mapped = df["market"].map(_contains_target_market) if "market" in df.columns else pd.Series(dtype=object)
    unrelated_values = sorted(df.loc[mapped.isna(), "market"].dropna().astype(str).str.strip().unique().tolist())
    no_unrelated = len(unrelated_values) == 0
    all_passed &= _print_result(6, "No unrelated random CFTC markets allowed.", no_unrelated, f"unrelated={unrelated_values}" if unrelated_values else "")

    multiple_rows_ok = True
    row_counts: dict[str, int] = {}
    if "market" in df.columns:
        for target in TARGET_MARKETS:
            count = int(df["market"].astype(str).str.lower().str.contains(target.lower(), regex=False).sum())
            row_counts[target] = count
            if count < 2:
                multiple_rows_ok = False
    all_passed &= _print_result(7, "Each target market should have multiple weekly rows.", multiple_rows_ok, f"counts={row_counts}")

    macro_dates = pd.to_datetime(df.get("macro_snapshot_date"), errors="coerce")
    non_null_unique_macro_dates = macro_dates.dropna().nunique()
    macro_dates_ok = non_null_unique_macro_dates > 1
    all_passed &= _print_result(8, "macro_snapshot_date must not be reused as one single date for all historical rows.", macro_dates_ok, f"unique_dates={non_null_unique_macro_dates}")

    confluence_scores = pd.to_numeric(df.get("confluence_score"), errors="coerce")
    confluence_ok = confluence_scores.notna().all() and confluence_scores.between(0, 10).all()
    all_passed &= _print_result(9, "confluence_score must be numeric 0-10.", confluence_ok)

    print(f"Overall: {'PASS' if all_passed else 'FAIL'}")
    return 0 if all_passed else 1


if __name__ == "__main__":
    raise SystemExit(run())

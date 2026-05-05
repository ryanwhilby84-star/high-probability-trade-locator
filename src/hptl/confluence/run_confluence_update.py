from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd

EXPORT_DIR = Path("data/exports")
PROCESSED_DIR = Path("data/processed")


def _latest_file(paths: list[Path]) -> Path:
    existing = [p for p in paths if p.exists()]
    if not existing:
        raise FileNotFoundError("No candidate files found.")
    return max(existing, key=lambda p: p.stat().st_mtime)


def _latest_cot_file() -> Path:
    candidates = list(EXPORT_DIR.glob("cot_update_*.xlsx")) + list(
        PROCESSED_DIR.glob("cot_cleaned_*.csv")
    )
    if not candidates:
        raise FileNotFoundError(
            "No COT input found. Expected data/exports/cot_update_*.xlsx or data/processed/cot_cleaned_*.csv"
        )
    return max(candidates, key=lambda p: p.stat().st_mtime)


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


def _load_cot(cot_file: Path) -> pd.DataFrame:
    if cot_file.suffix.lower() == ".xlsx":
        raw = pd.read_excel(cot_file, sheet_name="Dashboard", header=None)
        header_idx = None
        for idx, row in raw.iterrows():
            if row.astype(str).str.strip().eq("Market").any():
                header_idx = idx
                break
        if header_idx is None:
            raise ValueError("Could not find 'Market' header row in COT Dashboard sheet.")

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
        raise ValueError("COT data missing Market column.")

    cleaned = pd.DataFrame()
    cleaned["market"] = data[market_col].astype(str).str.strip()
    cleaned = cleaned[cleaned["market"].ne("")]

    if date_col is not None:
        cleaned["cot_report_date"] = pd.to_datetime(data[date_col], errors="coerce").dt.date
    else:
        cleaned["cot_report_date"] = pd.NaT

    cleaned["cot_bias"] = data[bias_col].apply(_clean_bias) if bias_col else "Neutral / Mixed"

    if score_col:
        cleaned["cot_score"] = pd.to_numeric(data[score_col], errors="coerce")
    else:
        cleaned["cot_score"] = pd.NA

    cleaned["cot_strength"] = data[strength_col].apply(_clean_strength) if strength_col else "Unknown"

    missing_score = cleaned["cot_score"].isna()
    fallback_scores = pd.to_numeric(
        cleaned.loc[missing_score, "cot_strength"].apply(_strength_to_score),
        errors="coerce",
    )
    cleaned.loc[missing_score, "cot_score"] = fallback_scores
    cleaned["cot_score"] = (
        pd.to_numeric(cleaned["cot_score"], errors="coerce").fillna(0).clip(lower=0, upper=10)
    )

    return cleaned.reset_index(drop=True)


def _macro_alignment_adjustment(score: float) -> int:
    if score <= 2:
        return 1
    if score <= 5:
        return 2
    if score <= 7:
        return 3
    return 4


def run() -> None:
    print("=" * 70)
    print("Confluence update started")
    print("=" * 70)

    cot_file = _latest_cot_file()
    macro_file = _latest_file(list(EXPORT_DIR.glob("macro_output_*.xlsx")))

    print(f"COT file used: {cot_file}")
    print(f"Macro file used: {macro_file}")

    cot = _load_cot(cot_file)
    print(f"Cleaned COT rows: {len(cot)}")

    macro = pd.read_excel(macro_file, sheet_name="Macro_Dashboard")
    if macro.empty:
        raise ValueError("Macro_Dashboard sheet is empty.")

    macro_row = macro.iloc[-1]
    macro_score = float(pd.to_numeric(macro_row.get("macro_score"), errors="coerce") or 0)
    macro_signal = str(macro_row.get("macro_signal", "")).strip().lower()

    print(f"Macro signal / score used: {macro_signal} / {macro_score}")

    macro_snapshot_date = pd.to_datetime(macro_row.get("snapshot_date"), errors="coerce")
    if pd.isna(macro_snapshot_date):
        macro_snapshot_date = pd.Timestamp(datetime.utcnow().date())

    def build_confluence(row: pd.Series) -> pd.Series:
        cot_bias = row["cot_bias"]
        cot_score = float(row["cot_score"])

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
            return pd.Series(
                {
                    "confluence_bias": "Conflicted / No Trade",
                    "confluence_score": 0,
                    "confluence_strength": "Blocked",
                    "trade_readiness": "Stand down",
                    "summary": f"{cot_bias} COT conflicts with {macro_signal} macro at high conviction.",
                }
            )

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

        return pd.Series(
            {
                "confluence_bias": bias,
                "confluence_score": score,
                "confluence_strength": strength,
                "trade_readiness": readiness,
                "summary": f"COT {cot_bias} ({cot_score}) vs macro {macro_signal} ({macro_score}) => {bias} {score:.1f}.",
            }
        )

    confluence = cot.copy()
    confluence["macro_snapshot_date"] = macro_snapshot_date.date()
    confluence["macro_signal"] = macro_signal
    confluence["macro_score"] = macro_score
    confluence["macro_strength"] = macro_row.get("macro_strength")
    confluence["macro_context_for_trades"] = macro_row.get("macro_context_for_trades")

    confluence = pd.concat([confluence, confluence.apply(build_confluence, axis=1)], axis=1)

    bias_order = {
        "Long Bias": 0,
        "Short Bias": 0,
        "Long (Headwind)": 1,
        "Short (Headwind)": 1,
        "Neutral / Mixed": 2,
        "Conflicted / No Trade": 3,
    }
    confluence["_bias_rank"] = confluence["confluence_bias"].map(bias_order).fillna(4)
    confluence = confluence.sort_values(
        by=["_bias_rank", "confluence_score"], ascending=[True, False]
    ).drop(columns=["_bias_rank"])

    final_columns = [
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
    confluence = confluence[final_columns]

    output_path = EXPORT_DIR / f"confluence_output_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
    source_notes = pd.DataFrame(
        {
            "field": ["cot_file_used", "macro_file_used", "generated_at_utc"],
            "value": [str(cot_file), str(macro_file), datetime.utcnow().isoformat()],
        }
    )

    with pd.ExcelWriter(output_path) as writer:
        confluence.to_excel(writer, sheet_name="Confluence_Dashboard", index=False)
        cot.to_excel(writer, sheet_name="COT_Input", index=False)
        macro.to_excel(writer, sheet_name="Macro_Input", index=False)
        source_notes.to_excel(writer, sheet_name="Source_Notes", index=False)

    print(f"Output path saved: {output_path}")


if __name__ == "__main__":
    run()

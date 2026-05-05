from pathlib import Path
import pandas as pd
from datetime import datetime

EXPORT_DIR = Path("data/exports")


def _latest_file(pattern: str) -> Path:
    files = sorted(EXPORT_DIR.glob(pattern), reverse=True)
    if not files:
        raise FileNotFoundError(f"No files found for pattern: {pattern}")
    return files[0]


def run():
    print("=" * 70)
    print("Confluence update started")
    print("=" * 70)

    cot_file = _latest_file("cot_update_*.xlsx")
    macro_file = _latest_file("macro_output_*.xlsx")

    print(f"Using COT file: {cot_file.name}")
    print(f"Using Macro file: {macro_file.name}")

    cot = pd.read_excel(cot_file)
    macro = pd.read_excel(macro_file, sheet_name="Macro_Dashboard")

    macro_row = macro.iloc[0]

    confluence = cot.copy()

    confluence["macro_signal"] = macro_row.get("macro_signal")
    confluence["macro_score"] = macro_row.get("macro_score")
    confluence["macro_strength"] = macro_row.get("macro_strength")
    confluence["macro_context"] = macro_row.get("macro_context_for_trades")

    def combine(cot_bias, macro_signal):
        if cot_bias == "Bullish" and macro_signal == "risk_on":
            return "Strong Long Bias"
        if cot_bias == "Bullish" and macro_signal == "risk_off":
            return "Long (Headwind)"
        if cot_bias == "Bearish" and macro_signal == "risk_off":
            return "Strong Short Bias"
        if cot_bias == "Bearish" and macro_signal == "risk_on":
            return "Short (Headwind)"
        return "Neutral"

    confluence["confluence_bias"] = confluence.apply(
        lambda r: combine(r.get("cot_bias"), r.get("macro_signal")), axis=1
    )

    output_path = EXPORT_DIR / f"confluence_output_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"

    with pd.ExcelWriter(output_path) as writer:
        confluence.to_excel(writer, sheet_name="Confluence_Dashboard", index=False)
        cot.to_excel(writer, sheet_name="COT_Input", index=False)
        macro.to_excel(writer, sheet_name="Macro_Input", index=False)

    print("=" * 70)
    print("Confluence update complete")
    print(f"Saved: {output_path}")
    print("=" * 70)


if __name__ == "__main__":
    run()
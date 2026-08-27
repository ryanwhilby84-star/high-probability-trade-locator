"""One-shot FX-FINAL report — all 8 FX futures markets."""
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from hptl.valuation.currency_futures_ive_v1 import (
    compute_futures_instrument,
    write_currency_futures_ive_export,
)
from hptl.fx.fx_macro_history import currency_histories

SYMBOLS = ["DX", "6E", "6B", "6A", "6C", "6J", "6S", "6N"]


def map_status(block: dict) -> tuple[str, str, str]:
    ms = block.get("model_status", "")
    codes = block.get("blocker_codes") or []
    reg = (block.get("inputs") or {}).get("regression") or {}
    r2 = reg.get("r_squared")
    reason = block.get("blocker_reason", "")
    if " — " in reason:
        reason = reason.split(" — ", 1)[1]
    if ms == "VALIDATED":
        return "VALIDATED", "YES", "-"
    if "failed_model_validation" in codes or (r2 is not None and r2 < 0.08):
        return "REBUILD_REQUIRED", "NO", reason
    return "WITHHELD", "NO", reason


def main() -> None:
    histories = currency_histories()
    rows = []
    for sym in SYMBOLS:
        b = compute_futures_instrument(sym, histories=histories)
        st, pub, reason = map_status(b)
        reg = (b.get("inputs") or {}).get("regression") or {}
        ph = (b.get("inputs") or {}).get("price_history") or {}
        rows.append(
            {
                "symbol": sym,
                "status": st,
                "valuation_pct": b.get("valuation_pct"),
                "label": b.get("valuation_label"),
                "model": b.get("model_name"),
                "publish": pub,
                "reason": reason if st != "VALIDATED" else "-",
                "internal_status": b.get("model_status"),
                "r2": reg.get("r_squared"),
                "mae": reg.get("mae"),
                "n": reg.get("n"),
                "panel": (b.get("inputs") or {}).get("panel_observations"),
                "price_latest": ph.get("latest"),
                "price_bars": ph.get("bar_count"),
                "price_source": ph.get("source"),
                "missing": (b.get("inputs") or {}).get("_missing_inputs") or [],
                "stale": (b.get("inputs") or {}).get("_stale_inputs") or [],
                "coef": reg.get("coefficients"),
                "blocker_codes": b.get("blocker_codes"),
            }
        )

    write_currency_futures_ive_export()

    print("SYMBOL|STATUS|VAL%|LABEL|MODEL|PUBLISH|REASON")
    for r in rows:
        vp = r["valuation_pct"]
        vps = f"{vp:+.2f}%" if vp is not None else "-"
        lab = r["label"] if r["label"] else "-"
        print(
            f"{r['symbol']}|{r['status']}|{vps}|{lab}|{r['model']}|{r['publish']}|{r['reason']}"
        )

    out = Path(__file__).resolve().parents[1] / "data" / "audits" / "fx_final_report.json"
    out.write_text(json.dumps(rows, indent=2), encoding="utf-8")


if __name__ == "__main__":
    main()

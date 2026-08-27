"""Generate valuation coverage matrix for Market Radar instruments."""
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
RADAR = [
    "NASDAQ / NQ", "S&P 500 / ES", "Dow / YM",
    "Euro FX / 6E", "British Pound / 6B", "Japanese Yen / 6J", "Swiss Franc / 6S",
    "Australian Dollar / 6A", "Canadian Dollar / 6C", "NZ Dollar / 6N",
    "Gold", "Silver", "Copper / HG", "Platinum", "Palladium",
    "Crude Oil / CL", "Natural Gas / NG", "Coffee", "Cocoa", "Corn", "Wheat", "Soybeans", "Sugar",
    "Bitcoin", "US Dollar Index / DX",
    "US 2-Year Treasury Yield", "US 10-Year Treasury Yield", "US 30-Year Treasury Yield",
    "2s10s Yield Curve", "10-Year Real Yield",
]

ASSET_CLASS = {
    "NASDAQ / NQ": "Indices", "S&P 500 / ES": "Indices", "Dow / YM": "Indices",
    "Euro FX / 6E": "FX", "British Pound / 6B": "FX", "Japanese Yen / 6J": "FX",
    "Swiss Franc / 6S": "FX", "Australian Dollar / 6A": "FX", "Canadian Dollar / 6C": "FX",
    "NZ Dollar / 6N": "FX", "US Dollar Index / DX": "FX",
    "Gold": "Metals", "Silver": "Metals", "Copper / HG": "Metals", "Platinum": "Metals", "Palladium": "Metals",
    "Crude Oil / CL": "Energy", "Natural Gas / NG": "Energy",
    "Coffee": "Agriculture", "Cocoa": "Agriculture", "Corn": "Agriculture", "Wheat": "Agriculture",
    "Soybeans": "Agriculture", "Sugar": "Agriculture",
    "Bitcoin": "Crypto",
    "US 2-Year Treasury Yield": "Rates", "US 10-Year Treasury Yield": "Rates",
    "US 30-Year Treasury Yield": "Rates", "2s10s Yield Curve": "Rates", "10-Year Real Yield": "Rates",
}


def load_json(name):
    p = ROOT / "web-dashboard/public/data" / name
    return json.loads(p.read_text(encoding="utf-8")) if p.exists() else {}


def classify(block):
    if not block:
        return "Missing", "Not in valuation export"
    wired = block.get("wired")
    dev = block.get("deviation_pct")
    state = block.get("valuation_state") or block.get("valuation_bias")
    conf = block.get("confidence")
    if wired and dev is not None:
        if str(conf).lower() in {"low", "none", "none"}:
            return "Partially valued", f"{state} (low confidence)"
        return "Currently valued", state
    if wired:
        return "Partially valued", state or "wired but no deviation"
    reason = (
        block.get("valuation_reason")
        or block.get("reason")
        or block.get("blocker")
        or state
        or "UNAVAILABLE"
    )
    return "Missing", str(reason)


def main():
    val = load_json("valuation_latest.json")
    inst = val.get("instruments", {})
    scanner = load_json("scanner_latest.json")
    rows = scanner.get("rows") or []
    sby = {r["market"]: r for r in rows if r.get("market")}

    results = []
    counts = {"Currently valued": 0, "Partially valued": 0, "Missing": 0}
    for m in RADAR:
        b = inst.get(m, {})
        status, why = classify(b)
        counts[status] += 1
        results.append({
            "instrument": m,
            "asset_class": ASSET_CLASS[m],
            "status": status,
            "why": why,
            "deviation_pct": b.get("deviation_pct"),
            "model": b.get("model_id") or b.get("valuation_model_id"),
            "wired": b.get("wired"),
            "confidence": b.get("confidence"),
            "scanner_valuation_mode": sby.get(m, {}).get("valuation_mode"),
            "scanner_valuation_bias": sby.get(m, {}).get("valuation_bias"),
        })

    out = ROOT / "data/audits/valuation_coverage_radar_matrix.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps({"radar_count": len(RADAR), "counts": counts, "instruments": results}, indent=2), encoding="utf-8")

    print("SUMMARY", counts)
    print(f"Valued rate: {(counts['Currently valued'] + counts['Partially valued']) / len(RADAR) * 100:.1f}%")
    print(f"Full valued rate: {counts['Currently valued'] / len(RADAR) * 100:.1f}%")
    for r in results:
        print(f"{r['status']:20} | {r['asset_class']:12} | {r['instrument']:28} | {r['deviation_pct']} | {r['model']}")


if __name__ == "__main__":
    main()

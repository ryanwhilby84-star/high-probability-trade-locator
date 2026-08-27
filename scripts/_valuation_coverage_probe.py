"""Probe valuation coverage for Market Radar instruments."""
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

def load(name):
    p = ROOT / "web-dashboard/public/data" / name
    if p.exists():
        return json.loads(p.read_text(encoding="utf-8"))
    return None

val = load("valuation_latest.json") or {}
markets = val.get("markets", val)
scanner = load("scanner_latest.json") or {}
rows = scanner.get("rows") or scanner.get("markets") or []
scanner_by = {r.get("market"): r for r in rows if isinstance(r, dict) and r.get("market")}

fx_v3 = load("fx_valuation_v3_latest.json") or {}
agri = load("agri_valuation_latest.json") or {}

print("=== valuation_latest ===")
for m in RADAR:
    block = markets.get(m) if isinstance(markets, dict) else None
    sr = scanner_by.get(m, {})
    if block:
        keys = sorted(block.keys())
        print(m)
        print("  val keys:", keys[:25])
        for k in ["mode", "status", "available", "wired", "valuation_mode", "fair_value_pct",
                  "deviation_pct", "valuation_fair", "valuation_label", "label", "reason",
                  "blocker", "model", "confidence", "engine"]:
            if k in block:
                print(f"  {k}: {block[k]}")
        if sr:
            for k in ["valuation_bias", "valuation_reason", "valuation_fair", "valuation_mode"]:
                if k in sr:
                    print(f"  scanner.{k}: {sr[k]}")
    else:
        print(f"{m}: NOT IN valuation_latest")
        if sr:
            for k in ["valuation_bias", "valuation_reason", "valuation_fair", "valuation_mode"]:
                if k in sr:
                    print(f"  scanner.{k}: {sr[k]}")
    print()

print("\n=== fx v3 pairs ===")
if isinstance(fx_v3, dict):
    for k in list(fx_v3.keys())[:20]:
        print(k, fx_v3[k] if not isinstance(fx_v3[k], dict) else {x: fx_v3[k][x] for x in list(fx_v3[k].keys())[:8]})

print("\n=== agri ===")
agri_m = agri.get("markets", agri)
if isinstance(agri_m, dict):
    for m in ["Corn", "Wheat", "Coffee", "Cocoa"]:
        if m in agri_m:
            b = agri_m[m]
            print(m, {k: b.get(k) for k in ["wired", "model", "deviation_pct", "blocker", "reason"]})

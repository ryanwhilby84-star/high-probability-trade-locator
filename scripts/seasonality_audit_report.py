"""One-off seasonality audit data extraction — read-only."""
from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

AUDIT_MARKETS = {
    "USD": "US Dollar Index / DX",
    "EUR": "Euro FX / 6E",
    "GBP": "British Pound / 6B",
    "CHF": "Swiss Franc / 6S",
    "JPY": "Japanese Yen / 6J",
    "CAD": "Canadian Dollar / 6C",
    "AUD": "Australian Dollar / 6A",
    "NZD": "NZ Dollar / 6N",
    "Gold": "Gold",
    "Silver": "Silver",
    "Copper": "Copper / HG",
    "Platinum": "Platinum",
    "Palladium": "Palladium",
    "Wheat": "Wheat",
    "Corn": "Corn",
    "Soybeans": "Soybeans",
    "Sugar": "Sugar",
    "Cotton": "Cotton",
    "Coffee": "Coffee",
    "Crude Oil": "Crude Oil / CL",
    "Natural Gas": "Natural Gas / NG",
}

ASSET_CLASS = {
    "USD": "FX",
    "EUR": "FX",
    "GBP": "FX",
    "CHF": "FX",
    "JPY": "FX",
    "CAD": "FX",
    "AUD": "FX",
    "NZD": "FX",
    "Gold": "Metals",
    "Silver": "Metals",
    "Copper": "Metals",
    "Platinum": "Metals",
    "Palladium": "Metals",
    "Wheat": "Ag",
    "Corn": "Ag",
    "Soybeans": "Ag",
    "Sugar": "Ag",
    "Cotton": "Ag",
    "Coffee": "Ag",
    "Crude Oil": "Energy",
    "Natural Gas": "Energy",
}


def load(rel: str):
    fp = ROOT / rel
    if not fp.exists():
        return None
    return json.loads(fp.read_text(encoding="utf-8"))


def rank_row(r: dict) -> str:
    if not r["avail"]:
        return "Not usable"
    g = r["trust"]
    hy = r["hist_years"] or 0
    if g == "A" and hy >= 10:
        return "High confidence"
    if g in ("A", "B") and hy >= 5:
        return "Medium confidence"
    if g == "B":
        return "Medium confidence"
    if g == "C" and r["avail"]:
        return "Low confidence"
    return "Not usable"


def main() -> None:
    price = load("web-dashboard/public/data/seasonality_price_latest.json")
    v1 = load("web-dashboard/public/data/seasonality_latest.json")
    cov = load("web-dashboard/public/data/seasonality_coverage_audit_latest.json")
    foundation = load("data/processed/seasonality_foundation_audit.json")

    markets = (price or {}).get("markets", {})
    v1_inst = (v1 or {}).get("instruments", {})
    cov_inst = (cov or {}).get("instruments", {}) if cov else {}
    foundation_rows = {}
    if foundation:
        for row in foundation.get("instruments", []):
            foundation_rows[row.get("instrument_id") or row.get("market")] = row

    print("PRICE_EXPORT", (price or {}).get("schema_version"), (price or {}).get("generated_at"))
    print("V1_EXPORT", (v1 or {}).get("generated_at"))
    print()

    rows = []
    for label, mid in AUDIT_MARKETS.items():
        b = markets.get(mid) or {}
        v1b = v1_inst.get(mid) or {}
        cb = cov_inst.get(mid) or {}
        fb = foundation_rows.get(mid) or {}

        avail = b.get("available", False)
        earliest = b.get("timeline_start") or cb.get("earliest_date") or fb.get("earliest_date") or "—"
        latest = (
            b.get("timeline_end")
            or (b.get("latest_price") or {}).get("date")
            or cb.get("latest_date")
            or fb.get("latest_date")
            or "—"
        )
        hist_years = b.get("years_of_history") or b.get("years_used") or fb.get("years_used")
        obs = b.get("weekly_bars_count") or cb.get("weekly_bars") or fb.get("weekly_bars_count")
        source = b.get("canonical_source") or b.get("price_store_key") or cb.get("price_source") or "—"
        fwd8 = ((b.get("forward_read") or {}).get("next_8w") or {})
        r = {
            "label": label,
            "market_id": mid,
            "asset": ASSET_CLASS[label],
            "avail": avail,
            "source": source,
            "bar_source": b.get("bar_source") or "—",
            "earliest": earliest,
            "latest": latest,
            "hist_years": hist_years,
            "years_avail": b.get("years_available"),
            "obs": obs,
            "windows": ",".join(b.get("windows_available") or []),
            "trust": b.get("trust_grade") or cb.get("trust_grade") or "—",
            "conf": (b.get("confidence") or {}).get("level") or "—",
            "fwd8_dir": fwd8.get("direction"),
            "fwd8_ret": fwd8.get("avg_return_pct"),
            "fwd8_n": fwd8.get("sample_years"),
            "fwd4": (b.get("forward_read") or {}).get("next_4w") or {},
            "fwd12": (b.get("forward_read") or {}).get("next_12w") or {},
            "phase": b.get("seasonal_phase"),
            "v1_bias": v1b.get("seasonality_bias") or v1b.get("bias"),
            "v1_wired": v1b.get("wired"),
            "v1_score": v1b.get("seasonality_score"),
            "v1_month_samples": v1b.get("month_sample_weeks"),
            "stale": bool(b.get("price_stale_note")),
            "proxy": b.get("proxy"),
            "reason": b.get("reason") if not avail else None,
            "avg_wpy": b.get("avg_weeks_per_year"),
            "s3_weeks": b.get("seasonal_3y_weeks"),
            "confluence_eligible": b.get("confluence_eligible"),
            "foundation_pass": fb.get("pass"),
            "anchor_week": b.get("current_week"),
            "anchor_index": (b.get("latest_price") or {}).get("index"),
            "path_alignment": b.get("path_alignment"),
            "divergence": (b.get("divergence_read") or {}).get("divergence"),
        }
        r["rank"] = rank_row(r)
        rows.append(r)

    for r in rows:
        print(
            f"{r['label']:12} {r['market_id']:28} {r['asset']:6} "
            f"avail={str(r['avail']):5} trust={r['trust']:1} rank={r['rank']:18} "
            f"hist={r['hist_years']} obs={r['obs']} {r['earliest']}..{r['latest']} "
            f"win={r['windows']} 8w={r['fwd8_dir']} {r['fwd8_ret']}% n={r['fwd8_n']} conf={r['conf']} "
            f"v1={r['v1_bias']} eligible={r['confluence_eligible']}"
        )
        if r["reason"]:
            print(f"    REASON: {r['reason']}")

    print("\nUNAVAILABLE:", [r["label"] for r in rows if not r["avail"]])
    print("NOT CONFLUENCE ELIGIBLE:", [r["label"] for r in rows if r["avail"] and not r["confluence_eligible"]])
    print("HIGH:", [r["label"] for r in rows if r["rank"] == "High confidence"])
    print("MEDIUM:", [r["label"] for r in rows if r["rank"] == "Medium confidence"])
    print("LOW:", [r["label"] for r in rows if r["rank"] == "Low confidence"])
    print("NOT USABLE:", [r["label"] for r in rows if r["rank"] == "Not usable"])


if __name__ == "__main__":
    main()

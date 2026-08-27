from hptl.pillars.confluence_attach import pillar_fields_for_market_week
from hptl.valuation.engine import compute_valuation

METALS = ["Gold", "Silver", "Copper / HG", "Platinum", "Palladium"]
week = "2026-06-09"
for m in METALS:
    val = compute_valuation(market=m, as_of_week=week)
    pillar = pillar_fields_for_market_week(m, week)
    reason = val.get("valuation_reason") or val.get("unavailable_reason") or ""
    print("---", m, "---")
    print("  compute_valuation:", val.get("wired"), val.get("deviation_pct"), val.get("valuation_bias"), reason[:120])
    print("  pillar attach:", pillar.get("valuation_wired"), pillar.get("deviation_pct"), pillar.get("valuation_bias"), pillar.get("data_integrity"))

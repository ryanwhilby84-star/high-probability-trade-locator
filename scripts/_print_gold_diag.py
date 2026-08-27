import json
from pathlib import Path

gold = json.loads(Path("web-dashboard/public/data/valuation_latest.json").read_text())["instruments"]["Gold"]
print("publish", gold.get("publish"))
print("blocker", gold.get("blocker_reason"))
print("valuation_reason", gold.get("valuation_reason"))
diag = gold.get("sign_gate_diagnostic") or (gold.get("institutional_audit") or {}).get("sign_gate_diagnostic")
if diag:
    print("--- diagnostic ---")
    print(diag.get("summary"))
    for f in diag.get("failed_features", []):
        print(f"  {f['feature']}: beta={f['coefficient']}, corr={f.get('univariate_corr_log_price')}")
        print(f"    {f['explanation']}")
c = json.loads(Path("data/cache/metals_drivers/wgc_cb_gold_net_purchases.json").read_text())
print("cache", c["observation_count"], c["frequency"], c["latest_date"])

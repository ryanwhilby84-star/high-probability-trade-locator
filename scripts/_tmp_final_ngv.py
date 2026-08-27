import json
from pathlib import Path

i = json.loads(Path("data/natural_gas_valuation_latest.json").read_text(encoding="utf-8"))[
    "instrument"
]
for k, v in i["driver_classifications"].items():
    print(f"{k}: {v.get('classification')} | {v.get('reason')}")
print("---")
mc = i["model_comparison"]
print("rec", mc["recommended_spec"], mc["validated_features"])
for s in mc["specifications"]:
    print(
        s["spec"],
        "oos_r2",
        s.get("oos_r2"),
        "oos_rmse",
        s.get("oos_rmse"),
        "impr%",
        s.get("oos_rmse_improvement_pct_vs_baseline"),
        "signs",
        s.get("signs_ok"),
        "r2",
        s.get("r_squared"),
        "adj",
        s.get("adj_r_squared"),
        "mrev",
        s.get("mean_reversion_corr_8w"),
        "ext",
        s.get("extreme_fv_rate_25pct"),
    )
print("prev", i.get("previous_unvalidated_snapshot"))
print(
    "fair",
    i.get("fair_value"),
    "dev",
    i.get("deviation_pct"),
    "conf",
    i.get("confidence"),
    "bias",
    i.get("institutional_bias"),
)

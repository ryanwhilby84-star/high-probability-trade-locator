"""Audit exported participant net positions and rolling percentiles against the COT chart dataset."""
import json
from pathlib import Path
from hptl.cot.positioning_percentiles import empirical_percentile_rank
from hptl.cot.weekly_inspector_export import _load_cot3y

def main():
    source = _load_cot3y()
    export = json.loads(Path("web-dashboard/public/data/cot_weekly_inspector_latest.json").read_text())
    assert export["measure"] == "net_positioning_rolling_156_week_percentile"
    keys = ("commercial_net", "institutional_net", "retail_net")
    checks = 0
    for name, block in source["markets"].items():
        series = block.get("series") or []
        if not series:
            continue
        rows = export["markets"][name]["rows"]
        assert len(rows) == len(series), (name, "row count", len(rows), len(series))
        for i, (raw, packed) in enumerate(zip(series, rows)):
            assert str(raw["date"]) == packed[0], (name, i, "date")
            for group_idx, key in enumerate(keys, start=1):
                value = raw.get(key)
                actual_net = packed[group_idx][0]
                assert (value is None and actual_net is None) or (value is not None and actual_net is not None and abs(float(value) - float(actual_net)) < 0.001), (name, raw["date"], key, "net", value, actual_net)
                window = [float(r[key]) for r in series[max(0, i - 155):i + 1] if r.get(key) is not None]
                expected = round(float(empirical_percentile_rank(window, float(value))), 2) if value is not None and window else None
                actual = packed[group_idx][4]
                assert (expected is None and actual is None) or (expected is not None and actual is not None and abs(expected - float(actual)) <= 0.02), (name, raw["date"], key, "percentile", expected, actual)
                for width, offset in ((260, 12), (780, 13)):
                    history = [float(r[key]) for r in series[max(0, i - width + 1):i + 1] if r.get(key) is not None]
                    expected_extra = round(float(empirical_percentile_rank(history, float(value))), 2) if value is not None and history else None
                    actual_extra = packed[group_idx][offset]
                    assert (expected_extra is None and actual_extra is None) or (expected_extra is not None and actual_extra is not None and abs(expected_extra - float(actual_extra)) <= 0.02), (name, raw["date"], key, width, expected_extra, actual_extra)
                    checks += 1
                checks += 1
    assert checks > 0, "No COT rows audited"
    print(f"PASS: {checks} source-net and 3y/5y/15y percentile comparisons across {len(source['markets'])} markets")

if __name__ == "__main__":
    main()

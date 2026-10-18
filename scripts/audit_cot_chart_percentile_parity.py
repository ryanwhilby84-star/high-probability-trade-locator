"""Audit exported participant net positions and rolling percentiles against the COT chart dataset."""
import json
from pathlib import Path
from hptl.cot.positioning_percentiles import net_range_pct

def range_percentile(values, current):
    if current is None or len(values) < 2 or max(values) == min(values):
        return None
    return round(net_range_pct(current, min(values), max(values)), 2)
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
                expected = range_percentile(window, float(value)) if value is not None else None
                actual = packed[group_idx][4]
                assert (expected is None and actual is None) or (expected is not None and actual is not None and abs(expected - float(actual)) <= 0.02), (name, raw["date"], key, "percentile", expected, actual)
                for width, offset in ((260, 12), (520, 14), (780, 13)):
                    history = [float(r[key]) for r in series[max(0, i - width + 1):i + 1] if r.get(key) is not None]
                    expected_extra = range_percentile(history, float(value)) if value is not None else None
                    actual_extra = packed[group_idx][offset]
                    assert (expected_extra is None and actual_extra is None) or (expected_extra is not None and actual_extra is not None and abs(expected_extra - float(actual_extra)) <= 0.02), (name, raw["date"], key, width, expected_extra, actual_extra)
                    checks += 1
                all_history = [float(r[key]) for r in series[:i + 1] if r.get(key) is not None]
                expected_all = range_percentile(all_history, float(value)) if value is not None else None
                actual_all = packed[group_idx][15] if len(packed[group_idx]) > 15 else None
                assert (expected_all is None and actual_all is None) or (expected_all is not None and actual_all is not None and abs(expected_all - float(actual_all)) <= 0.02), (name, raw["date"], key, "all", expected_all, actual_all)
                checks += 2
    assert checks > 0, "No COT rows audited"
    print(f"PASS: {checks} source-net and 3y/5y/10y/15y/ALL percentile comparisons across {len(source['markets'])} markets")

if __name__ == "__main__":
    main()

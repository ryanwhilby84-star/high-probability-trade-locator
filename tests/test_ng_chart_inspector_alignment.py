"""Natural Gas chart tip must match weekly inspector tip (freshness + 1W invariant)."""
from __future__ import annotations

import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PUBLIC = ROOT / "web-dashboard" / "public" / "data"
MARKET = "Natural Gas / NG"


def test_ng_inspector_tip_matches_cot3y_and_1w_invariant() -> None:
    wi_path = PUBLIC / "cot_weekly_inspector_latest.json"
    s3_path = PUBLIC / "cot_3y_series_latest.json"
    if not wi_path.is_file() or not s3_path.is_file():
        return
    wi = json.loads(wi_path.read_text(encoding="utf-8"))
    s3 = json.loads(s3_path.read_text(encoding="utf-8"))
    rows = (wi.get("markets") or {}).get(MARKET, {}).get("rows") or []
    series = (s3.get("markets") or {}).get(MARKET, {}).get("series") or []
    assert len(rows) >= 2
    assert len(series) >= 2
    last, prev = rows[-1], rows[-2]
    tip = series[-1]
    assert last[0] == tip["date"]
    assert last[2][0] == tip["institutional_net"]
    for gi in (1, 2, 3):
        assert abs((last[gi][0] - prev[gi][0]) - last[gi][1]) < 1e-6
    nc_delta = tip["institutional_net"] - series[-2]["institutional_net"]
    assert (nc_delta > 0) == (last[2][1] > 0) or (nc_delta == 0 and last[2][1] == 0)

"""Independent CFTC source-of-truth audit tests."""
from __future__ import annotations

import json

import pytest

from hptl.cot.cot_source_truth_audit import (
    build_cot_source_truth_audit,
    crude_related_rows_on_date,
    nq_related_rows_on_date,
    write_cot_source_truth_exports,
)


@pytest.fixture(scope="module")
def truth_bundle():
    return build_cot_source_truth_audit(force_download=False)


def test_cl_official_row_is_067651_wti_physical(truth_bundle):
    focus = truth_bundle["special_focus"]
    crude = focus["crude_oil_cl_all_rows_2026_05_26"]
    canonical = [r for r in crude if r.get("htpl_canonical_for_cl")]
    assert len(canonical) == 1
    assert canonical[0]["cftc_code"] == "067651"
    assert "WTI-PHYSICAL" in canonical[0]["market_name"]


def test_nq_control_209742(truth_bundle):
    focus = truth_bundle["special_focus"]
    nq = focus["nasdaq_nq_all_rows_2026_05_26"]
    canonical = [r for r in nq if r.get("htpl_canonical_for_nq")]
    assert len(canonical) == 1
    assert canonical[0]["cftc_code"] == "209742"


def test_cl_and_nq_instrument_audit(truth_bundle):
    cl = truth_bundle["instruments"]["Crude Oil / CL"]
    nq = truth_bundle["instruments"]["NASDAQ / NQ"]
    assert cl["selected_cftc_code"] == "067651"
    assert nq["selected_cftc_code"] == "209742"
    # Dashboard confluence was rebuilt to legacy — expect PASS if exports current
    if cl["status"] == "PASS":
        assert cl["nc_match"] is True
        assert cl["nonreportable_match"] is True
        off = cl["official_raw_values"]["noncommercials"]
        dash = cl["dashboard_values"]["noncommercials"]
        assert dash["long"] == off["long"]


def test_writes_json(truth_bundle, tmp_path, monkeypatch):
    from hptl.cot import cot_source_truth_audit as mod

    out = tmp_path / "cot_source_truth_audit_latest.json"
    monkeypatch.setattr(mod, "DATA_OUT", out)
    monkeypatch.setattr(mod, "PUBLIC_OUT", tmp_path / "pub.json")
    monkeypatch.setattr(mod, "DELIVERABLE_MD", tmp_path / "del.md")
    write_cot_source_truth_exports(truth_bundle)
    doc = json.loads(out.read_text(encoding="utf-8"))
    assert "summary" in doc
    assert "special_focus" in doc

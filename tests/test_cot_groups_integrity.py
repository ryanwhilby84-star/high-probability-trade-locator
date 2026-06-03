"""COT group integrity layer — crude CL proof vs CFTC disaggregated row."""
from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

from hptl.cot.cot_groups_integrity import (
    INSTRUMENT_PREFERRED_CFTC_CODE,
    build_cot_groups_payload,
    run_cot_groups_integrity,
)

PROCESSED = Path("data/processed")
CL = "Crude Oil / CL"
REPORT_DATE = "2026-05-26"


@pytest.fixture(scope="module")
def groups_payload():
    payload, _audit = build_cot_groups_payload(weeks=13)
    return payload


def test_crude_cl_contract_locked_to_wti_physical(groups_payload):
    assert INSTRUMENT_PREFERRED_CFTC_CODE[CL] == "067651"
    inst = groups_payload["instruments"][CL]
    assert inst["cftc_market_code"] == "067651"
    assert "WTI-PHYSICAL" in (inst["cftc_market_name"] or "").upper()
    assert inst["report_type"] == "disaggregated_futures_only"


def test_crude_cl_institutions_match_cftc_managed_money_2026_05_26(groups_payload):
    weeks = groups_payload["instruments"][CL]["groups"]["institutions"]["weeks"]
    row = next(w for w in weeks if w["report_date"] == REPORT_DATE)
    assert row["long"] == 200581
    assert row["short"] == 120657
    assert row["net"] == 79924
    assert row["total_open_interest"] == 2003795


def test_crude_cl_commercials_match_cftc_prod_merc(groups_payload):
    weeks = groups_payload["instruments"][CL]["groups"]["commercials"]["weeks"]
    row = next(w for w in weeks if w["report_date"] == REPORT_DATE)
    assert row["long"] == 691310
    assert row["short"] == 325169
    assert row["net"] == 366141


def test_crude_cl_retail_proxy_match_nonreportable(groups_payload):
    weeks = groups_payload["instruments"][CL]["groups"]["retail_proxy"]["weeks"]
    row = next(w for w in weeks if w["report_date"] == REPORT_DATE)
    assert row["long"] == 82778
    assert row["short"] == 48303
    assert row["net"] == 34475


def test_crude_cl_does_not_merge_other_reportable_into_institutions(groups_payload):
    """White Oak-style 378088 = mm + other_rept; integrity layer must stay mm-only."""
    weeks = groups_payload["instruments"][CL]["groups"]["institutions"]["weeks"]
    row = next(w for w in weeks if w["report_date"] == REPORT_DATE)
    assert row["long"] != 378088
    assert row["long"] + 177507 == 378088  # other_rept long from CFTC same week


def test_all_legacy_cot_instruments_resolved(groups_payload):
    from hptl.markets.instrument_registry import cot_mapped_ids

    missing = []
    for iid in cot_mapped_ids():
        inst = groups_payload["instruments"].get(iid)
        if not inst or not inst.get("groups") or not inst["groups"].get("institutions", {}).get("weeks"):
            missing.append(iid)
    assert not missing, f"No institution weeks for: {missing}"


def test_export_writes_json_files(tmp_path, monkeypatch):
    monkeypatch.setattr(
        "hptl.cot.cot_groups_integrity.DATA_OUT",
        tmp_path / "cot_groups_latest.json",
    )
    monkeypatch.setattr(
        "hptl.cot.cot_groups_integrity.AUDIT_OUT",
        tmp_path / "cot_group_audit_latest.json",
    )
    monkeypatch.setattr(
        "hptl.cot.cot_groups_integrity.PUBLIC_OUT",
        tmp_path / "public_cot_groups_latest.json",
    )
    monkeypatch.setattr(
        "hptl.cot.cot_groups_integrity.PUBLIC_AUDIT_OUT",
        tmp_path / "public_cot_group_audit_latest.json",
    )
    paths = run_cot_groups_integrity(weeks=2)
    assert paths["groups"].exists()
    doc = json.loads(paths["groups"].read_text(encoding="utf-8"))
    assert CL in doc["instruments"]


def test_raw_csv_cross_check_067651():
    files = sorted(PROCESSED.glob("cot_cleaned_*.csv"), key=lambda p: p.stat().st_mtime)
    assert files, "need cot_cleaned CSV"
    df = pd.read_csv(files[-1], low_memory=False)
    df["rd"] = pd.to_datetime(df["report_date_as_yyyy_mm_dd"], errors="coerce")
    row = df[(df["rd"] == REPORT_DATE) & (df["cftc_contract_market_code"].astype(str).str.contains("067651"))].iloc[0]
    assert row["m_money_positions_long_all"] == 200581
    assert row["prod_merc_positions_long_all"] == 691310
    assert row["nonrept_positions_long_all"] == 82778

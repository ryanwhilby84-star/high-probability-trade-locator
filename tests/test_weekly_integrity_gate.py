"""Weekly COT integrity gate and quarantine helpers."""

from __future__ import annotations

import json
from unittest.mock import patch

from hptl.cot.cot_quarantine import (
    QUARANTINE_PATH,
    clear_quarantine,
    quarantined_instrument_ids,
    write_quarantine,
)
from hptl.cot.weekly_integrity_gate import _collect_failures, print_weekly_summary, WeeklyIntegrityGateResult


def test_collect_failures_merges_source_truth_and_lineage(tmp_path, monkeypatch):
    truth = {
        "instruments": {
            "Gold": {"status": "PASS"},
            "Swiss Franc / 6S": {"status": "FAIL"},
        }
    }
    lineage = {
        "instruments": {
            "Gold": {"overall_status": "PASS"},
            "Swiss Franc / 6S": {
                "overall_status": "FAIL",
                "first_divergence_layer": "thesis",
                "failure_reasons": ["scanner→thesis nc_net: expected=1 actual=2"],
            },
        }
    }
    with patch(
        "hptl.cot.weekly_integrity_gate.cot_mapped_ids",
        return_value=["Gold", "Swiss Franc / 6S"],
    ):
        fails = _collect_failures(truth_doc=truth, lineage_doc=lineage)
    assert len(fails) == 1
    assert fails[0]["instrument"] == "Swiss Franc / 6S"
    assert "source_truth:FAIL" in fails[0]["reasons"][0]


def test_quarantine_write_and_load(tmp_path, monkeypatch):
    monkeypatch.setattr("hptl.cot.cot_quarantine.QUARANTINE_PATH", tmp_path / "q.json")
    monkeypatch.setattr("hptl.cot.cot_quarantine.PUBLIC_QUARANTINE_PATH", tmp_path / "pub_q.json")

    write_quarantine(
        report_date="2026-05-26",
        failed=[{"instrument": "Swiss Franc / 6S", "reasons": ["lineage:FAIL"]}],
        passed_count=22,
        checked_count=23,
    )
    assert quarantined_instrument_ids(report_date="2026-05-26") == {"Swiss Franc / 6S"}

    clear_quarantine(report_date="2026-05-26", checked_count=23)
    doc = json.loads((tmp_path / "q.json").read_text(encoding="utf-8"))
    assert doc["failed_count"] == 0
    assert quarantined_instrument_ids(report_date="2026-05-26") == set()


def test_print_weekly_summary_format(capsys):
    result = WeeklyIntegrityGateResult(
        checked_count=23,
        passed_count=22,
        failed_count=1,
        failed_instruments=["Swiss Franc / 6S"],
    )
    print_weekly_summary(result)
    out = capsys.readouterr().out
    assert "23 instruments checked" in out
    assert "22 passed" in out
    assert "1 failed" in out
    assert "Swiss Franc / 6S" in out

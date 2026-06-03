"""Tests for COT proof layer."""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from hptl.cot.cot_proof import build_cot_proof, write_cot_proof_exports


@pytest.fixture(scope="module")
def proof_bundle():
    return build_cot_proof(download=False)


def test_nq_legacy_panel_matches_raw(proof_bundle):
    nq = proof_bundle["instruments"]["NASDAQ / NQ"]
    nc = nq["groups"]["noncommercials"]
    assert nc["legacy_panel"]["long"]["match"] is True
    assert nc["legacy_panel"]["long"]["dashboard_value"] == 85248.0
    assert nc["legacy_panel"]["long"]["raw_cftc_value"] == 85248.0
    assert nq["cftc_code"] == "209742"


def test_proof_writes_json(proof_bundle, tmp_path, monkeypatch):
    from hptl.cot import cot_proof as mod

    out = tmp_path / "cot_proof_latest.json"
    pub = tmp_path / "public" / "cot_proof_latest.json"
    monkeypatch.setattr(mod, "DATA_PROOF", out)
    monkeypatch.setattr(mod, "PUBLIC_PROOF", pub)
    paths = write_cot_proof_exports(proof_bundle)
    assert paths["proof"].exists()
    doc = json.loads(out.read_text(encoding="utf-8"))
    assert doc["summary"]["total_instruments_checked"] >= 20


def test_stale_confluence_fails_nq_when_present(proof_bundle):
    """If confluence still has TFF headline, NQ overall must FAIL."""
    nq = proof_bundle["instruments"].get("NASDAQ / NQ") or {}
    if not nq.get("confluence_present"):
        pytest.skip("no confluence row")
    conf = nq["groups"]["noncommercials"]["confluence_headline"]
    if conf["long"]["dashboard_value"] == 52861.0:
        assert nq["overall_status"] == "FAIL"
        assert any("confluence" in r.lower() or "52861" in r for r in nq.get("mismatch_reasons") or [])

"""Universe integrity — identity table + audit smoke tests."""

from __future__ import annotations

from hptl.cot.legacy_cot import CANONICAL_LEGACY_CODE
from hptl.markets.canonical_identity import (
    BY_ID,
    CANONICAL_INSTRUMENTS,
    assert_universe_complete,
)
from hptl.markets.instrument_registry import LEGACY_COT_MARKETS, load_registry
from hptl.markets.universe_integrity_audit import (
    audit_identity,
    run_universe_integrity_audit,
)


def test_canonical_universe_matches_legacy_cot_markets():
    assert_universe_complete()
    assert len(CANONICAL_INSTRUMENTS) == 26
    assert len(LEGACY_COT_MARKETS) == 26


def test_no_duplicate_cftc_or_exchange_symbols():
    codes = [c.cftc_market_code for c in CANONICAL_INSTRUMENTS]
    exch = [c.exchange_symbol for c in CANONICAL_INSTRUMENTS]
    assert len(codes) == len(set(codes))
    assert len(exch) == len(set(exch))


def test_registry_cot_code_is_real_cftc_not_oanda():
    reg = load_registry()
    for mid in LEGACY_COT_MARKETS:
        canon = BY_ID[mid]
        spec = reg[mid]
        assert spec.cot_market_code == canon.cftc_market_code
        assert spec.cot_market_code == CANONICAL_LEGACY_CODE[mid]
        # Must not store OANDA-looking symbols as CFTC codes
        code = str(spec.cot_market_code)
        assert "_" not in code
        assert not code.endswith("USD")
        assert not code.isalpha()


def test_identity_phase_passes_after_registry_fix():
    for mid in ("Gold", "Japanese Yen / 6J", "Cotton", "Bitcoin"):
        result = audit_identity(mid)
        assert result.status == "pass", (mid, result.issues)


def test_audit_runs_and_writes_summary_shape():
    report = run_universe_integrity_audit(seed=1)
    assert report["summary"]["total_markets"] == 26
    assert "passed" in report["summary"]
    assert "failed" in report["summary"]
    assert len(report["instruments"]) == 26

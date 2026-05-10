from hptl.confluence.build_confluence_history import _build_confluence


def test_cocoa_known_good_row_reproduced() -> None:
    out = _build_confluence("Bearish", 8.0, "risk_off", 2.0)

    assert out["confluence_bias"] == "Short Bias"
    assert out["confluence_score"] == 9.0
    assert out["confluence_strength"] == "Very Strong"
    assert out["trade_readiness"] == "High conviction"
    assert out["summary"] == "COT Bearish (8.0) vs macro risk_off (2.0) => Short Bias 9.0."

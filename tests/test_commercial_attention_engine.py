"""Tests for Commercial-led COT Attention Engine V1."""

from __future__ import annotations

from hptl.cot.commercial_attention_engine import (
    EVENT_ALIGN_DEVELOPING,
    EVENT_COMMERCIAL_EXTREME,
    EVENT_COMMERCIAL_SURGE,
    EVENT_NC_FLIP,
    EVENT_NR_DIVERGENCE,
    MIN_HISTORY_WEEKS,
    SURGE_ABS_CHANGE_PCTILE,
    analyze_instrument,
    build_commercial_attention,
)


def _week(date: str, *, c_net: float, nc_net: float, nr_net: float, oi: float = 100_000.0) -> dict:
    return {
        "report_date": date,
        "commercials": {"long": max(c_net, 0) + oi / 4, "short": max(-c_net, 0) + oi / 4, "net": c_net, "open_interest": oi},
        "noncommercials": {
            "long": max(nc_net, 0) + oi / 4,
            "short": max(-nc_net, 0) + oi / 4,
            "net": nc_net,
            "open_interest": oi,
        },
        "nonreportables": {
            "long": max(nr_net, 0) + oi / 4,
            "short": max(-nr_net, 0) + oi / 4,
            "net": nr_net,
            "open_interest": oi,
        },
    }


def _build_inst(weeks: list[dict]) -> dict:
    """Assemble instrument doc with computed week changes."""
    groups = {"commercials": {"weeks": []}, "noncommercials": {"weeks": []}, "nonreportables": {"weeks": []}}
    for key in ("commercials", "noncommercials", "nonreportables"):
        prev_net = None
        for w in weeks:
            g = w[key]
            net = g["net"]
            row = {
                "report_date": w["report_date"],
                "long": g["long"],
                "short": g["short"],
                "net": net,
                "open_interest": g["open_interest"],
                "net_week_change": None if prev_net is None else net - prev_net,
            }
            groups[key]["weeks"].append(row)
            prev_net = net
    return {"instrument_id": "Test Market", "groups": groups}


def _make_history(
    *,
    n: int = MIN_HISTORY_WEEKS + 8,
    base_c: float = 1000.0,
    base_nc: float = -800.0,
    base_nr: float = -200.0,
    c_noise: float = 50.0,
) -> list[dict]:
    weeks = []
    for i in range(n):
        # Deterministic mild noise so percentiles are well-defined
        c = base_c + ((i % 7) - 3) * c_noise
        nc = base_nc + ((i % 5) - 2) * 40.0
        nr = base_nr + ((i % 3) - 1) * 30.0
        weeks.append(_week(f"2024-{(i % 12) + 1:02d}-{((i % 28) + 1):02d}", c_net=c, nc_net=nc, nr_net=nr))
    # Fix dates to be strictly increasing weekly-ish
    from datetime import date, timedelta

    start = date(2024, 1, 2)
    out = []
    for i, w in enumerate(weeks):
        d = (start + timedelta(weeks=i)).isoformat()
        out.append({**w, "report_date": d})
    return out


def test_surge_uses_own_history_not_raw_size():
    """Large absolute Δ in a quiet market ranks as surge; same Δ may not in a volatile market."""
    quiet = _make_history(base_c=1000.0, c_noise=10.0)
    # Final week: unusually large move for quiet market
    quiet[-1] = _week(quiet[-1]["report_date"], c_net=quiet[-2]["commercials"]["net"] + 500.0, nc_net=-800.0, nr_net=200.0)
    att_q = analyze_instrument("Quiet", _build_inst(quiet))
    assert EVENT_COMMERCIAL_SURGE in att_q.events
    assert att_q.commercial.change_1w_abs_percentile is not None
    assert att_q.commercial.change_1w_abs_percentile >= SURGE_ABS_CHANGE_PCTILE


def test_raw_contract_giant_does_not_auto_rank_first():
    """Ranking prefers percentile significance over raw |1W| contract count."""
    small = _make_history(base_c=500.0, c_noise=5.0, base_nc=-400.0, base_nr=100.0)

    def with_oi(weeks, oi):
        out = []
        for w in weeks:
            out.append(
                _week(
                    w["report_date"],
                    c_net=w["commercials"]["net"],
                    nc_net=w["noncommercials"]["net"],
                    nr_net=w["nonreportables"]["net"],
                    oi=oi,
                )
            )
        return out

    small = with_oi(small, 50_000)
    giant = with_oi(
        _make_history(base_c=200_000.0, c_noise=15_000.0, base_nc=-180_000.0, base_nr=20_000.0),
        5_000_000,
    )

    # Small market: surge (+200 vs noise 5)
    small[-1] = _week(small[-1]["report_date"], c_net=small[-2]["commercials"]["net"] + 200.0, nc_net=-400.0, nr_net=100.0, oi=50_000)
    # Giant market: larger raw Δ but typical for its history (+20k vs noise 15k)
    giant[-1] = _week(
        giant[-1]["report_date"],
        c_net=giant[-2]["commercials"]["net"] + 20_000.0,
        nc_net=-180_000.0,
        nr_net=20_000.0,
        oi=5_000_000,
    )

    doc = {
        "instruments": {
            "Small Spec": _build_inst(small),
            "Giant Spec": _build_inst(giant),
        }
    }
    payload = build_commercial_attention(doc, as_of=small[-1]["report_date"])
    board = payload["attention_board"]
    assert board, "expected at least one attention row"
    # Small market surge should outrank giant's ordinary move when both qualify,
    # or giant may not even get SURGE.
    by_name = {r["instrument"]: r for r in board}
    if "Small Spec" in by_name and "Giant Spec" in by_name:
        assert board.index(by_name["Small Spec"]) < board.index(by_name["Giant Spec"])
    else:
        assert "Small Spec" in by_name
        assert EVENT_COMMERCIAL_SURGE in by_name["Small Spec"]["events"]


def test_no_lookahead_percentile():
    weeks = _make_history()
    # Spike in the middle should not use future weeks
    mid = len(weeks) // 2
    weeks[mid] = _week(
        weeks[mid]["report_date"],
        c_net=weeks[mid - 1]["commercials"]["net"] + 800.0,
        nc_net=-800.0,
        nr_net=200.0,
    )
    # Also put an even larger future spike
    weeks[-1] = _week(
        weeks[-1]["report_date"],
        c_net=weeks[-2]["commercials"]["net"] + 5000.0,
        nc_net=-800.0,
        nr_net=200.0,
    )
    inst = _build_inst(weeks)
    mid_att = analyze_instrument("X", inst, as_of=weeks[mid]["report_date"])
    last_att = analyze_instrument("X", inst, as_of=weeks[-1]["report_date"])
    assert mid_att.history_weeks_used == mid + 1
    assert last_att.history_weeks_used == len(weeks)
    # Mid percentile computed without the final spike in the window length
    assert mid_att.commercial.change_1w_abs_percentile is not None


def test_nr_divergence_both_directions():
    weeks = _make_history(base_c=5000.0, base_nc=-2000.0, base_nr=-3000.0, c_noise=20.0)
    # Commercial bullish extreme-ish; NR bearish
    weeks[-1] = _week(weeks[-1]["report_date"], c_net=12_000.0, nc_net=-2000.0, nr_net=-8_000.0)
    att = analyze_instrument("Div", _build_inst(weeks))
    # May or may not hit EXTREME depending on history; force check divergence path
    # Rebuild with stronger history of mild nets then extreme opposite NR
    weeks2 = _make_history(base_c=2000.0, base_nc=-1000.0, base_nr=-500.0, c_noise=30.0)
    for i in range(len(weeks2)):
        # Keep commercial gradually more bullish
        weeks2[i] = _week(
            weeks2[i]["report_date"],
            c_net=1000 + i * 40,
            nc_net=-800,
            nr_net=-400,
        )
    weeks2[-1] = _week(
        weeks2[-1]["report_date"],
        c_net=1000 + (len(weeks2) - 1) * 40,
        nc_net=-800,
        nr_net=-9000,
    )
    att2 = analyze_instrument("Div2", _build_inst(weeks2))
    assert att2.commercial.direction == "bullish"
    assert att2.nonreportable.direction == "bearish"
    assert EVENT_NR_DIVERGENCE in att2.events


def test_nc_flip_not_noise():
    """Single-week noise without prior opposition must not fire NC FLIP."""
    weeks = _make_history(base_c=3000.0, base_nc=2000.0, base_nr=-500.0, c_noise=25.0)
    # Commercial bullish established; NC also bullish recently — no opposition
    for i in range(-4, 0):
        weeks[i] = _week(
            weeks[i]["report_date"],
            c_net=4000 + i,
            nc_net=2000 + i * 10,  # bullish nets, mild changes
            nr_net=-500,
        )
    att = analyze_instrument("NoFlip", _build_inst(weeks))
    assert EVENT_NC_FLIP not in att.events

    # Now create opposition then flip
    weeks2 = _make_history(base_c=5000.0, base_nc=-3000.0, base_nr=-1000.0, c_noise=20.0)
    # Ensure commercial stays bullish with high net
    for i in range(len(weeks2)):
        weeks2[i] = _week(weeks2[i]["report_date"], c_net=3000 + i * 10, nc_net=-2000 - (i % 3) * 50, nr_net=-800)
    # Prior weeks: NC flowing bearish; final week material bullish flip toward Commercial.
    base_date_idx = len(weeks2) - 5
    nets_nc = [-2000.0, -2800.0, -3600.0, -4400.0, -1500.0]  # oppose then +2900 flip
    nets_c = [5000.0, 5200.0, 5400.0, 5600.0, 5800.0]
    for j, idx in enumerate(range(base_date_idx, len(weeks2))):
        weeks2[idx] = _week(
            weeks2[idx]["report_date"],
            c_net=nets_c[j],
            nc_net=nets_nc[j],
            nr_net=-1000.0,
        )
    att2 = analyze_instrument("Flip", _build_inst(weeks2))
    assert EVENT_NC_FLIP in att2.events
    assert EVENT_ALIGN_DEVELOPING in att2.events
    assert att2.noncommercial.change_1w_abs_percentile is not None
    assert att2.noncommercial.change_1w_abs_percentile >= 55.0


def test_insufficient_history_skipped():
    weeks = _make_history(n=20)
    att = analyze_instrument("ShortHist", _build_inst(weeks))
    assert att.eligible is False
    assert att.skip_reason and "insufficient_history" in att.skip_reason


def test_live_legacy_doc_runs():
    """Smoke: real legacy_cot_latest produces a board without crashing."""
    from hptl.cot.legacy_cot_loader import load_legacy_cot_document

    doc = load_legacy_cot_document()
    if not (doc.get("instruments") or {}):
        return
    payload = build_commercial_attention(doc)
    assert payload["version"] == "commercial_attention_v1"
    assert payload["source_week"]
    assert "attention_board" in payload
    # Ranking must not be raw |1W| order exclusively — verify sort key fields exist
    for row in payload["attention_board"][:5]:
        assert "events" in row
        assert "evidence_points" in row
        assert "rank_reasons" in row
        assert row["commercial"]["change_1w_abs_percentile"] is not None or row["evidence_points"] >= 0

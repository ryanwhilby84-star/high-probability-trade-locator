"""Dedicated FX Valuation Panel — first-class per-pair valuation readout."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from hptl.fx.fx_valuation_history import history_deltas_for_pair

STATUS_RIPENING = "Ripening"
STATUS_STRONG = "Strong"
STATUS_EXTENDED = "Extended"
STATUS_NEUTRAL = "Neutral"
STATUS_DETERIORATING = "Deteriorating"

MOMENTUM_IMPROVING = "Improving"
MOMENTUM_WEAKENING = "Weakening"
MOMENTUM_UNCHANGED = "Unchanged"


def _num(v: Any) -> float | None:
    if v is None:
        return None
    try:
        f = float(v)
        return f if f == f else None
    except (TypeError, ValueError):
        return None


def _normalize_bias(raw: str | None) -> str:
    b = str(raw or "").strip().lower()
    if "bull" in b:
        return "Bullish"
    if "bear" in b:
        return "Bearish"
    return "Neutral"


def _clamp_score(v: float, lo: float = 0.0, hi: float = 10.0) -> float:
    return round(max(lo, min(hi, float(v))), 1)


def _score_from_differential(adj_diff: float | None) -> float:
    if adj_diff is None:
        return 0.0
    return _clamp_score(min(10.0, abs(adj_diff) / 25.0 * 10.0))


def _momentum_from_delta(delta: float | None, *, threshold: float = 1.5) -> str:
    if delta is None:
        return MOMENTUM_UNCHANGED
    if delta >= threshold:
        return MOMENTUM_IMPROVING
    if delta <= -threshold:
        return MOMENTUM_WEAKENING
    return MOMENTUM_UNCHANGED


def _valuation_status(*, score: float, momentum: str, gap_pct: float | None) -> str:
    gap = abs(gap_pct) if gap_pct is not None else 0.0
    if momentum == MOMENTUM_WEAKENING:
        return STATUS_DETERIORATING
    if momentum == MOMENTUM_IMPROVING and score >= 4.0 and gap < 12.0:
        return STATUS_RIPENING
    if gap >= 12.0 or score >= 9.0:
        return STATUS_EXTENDED
    if score >= 7.0 and momentum != MOMENTUM_WEAKENING:
        return STATUS_STRONG
    return STATUS_NEUTRAL


def _driver_line(label: str, value: Any, *, suffix: str = "") -> dict[str, Any]:
    n = _num(value)
    display = f"{n:+.3f}{suffix}" if n is not None else "—"
    return {"label": label, "value": n, "display": display}


def _fmt_delta(v: float | None) -> str:
    if v is None:
        return "—"
    return f"{v:+.1f}"


@dataclass
class FxValuationPanelBlock:
    pair: str
    score: float
    bias: str
    momentum: str
    daily_change: float | None
    weekly_change: float | None
    status: str
    drivers: dict[str, Any]
    narrative: str
    gap_pct: float | None = None
    pair_score_differential: float | None = None
    adjusted_score_differential: float | None = None

    def as_dict(self) -> dict[str, Any]:
        return {
            "pair": self.pair,
            "valuation_score": self.score,
            "score_display": f"{round(self.score)}/10",
            "bias": self.bias,
            "momentum": self.momentum,
            "daily_change": self.daily_change,
            "weekly_change": self.weekly_change,
            "daily_change_display": _fmt_delta(self.daily_change),
            "weekly_change_display": _fmt_delta(self.weekly_change),
            "status": self.status,
            "drivers": self.drivers,
            "narrative": self.narrative,
            "valuation_gap_pct": self.gap_pct,
            "pair_score_differential": self.pair_score_differential,
            "adjusted_score_differential": self.adjusted_score_differential,
        }


def build_valuation_narrative(
    *,
    pair: str,
    bias: str,
    status: str,
    momentum: str,
    base: str,
    quote: str,
    base_score: float | None,
    quote_score: float | None,
    macro_doc: dict[str, Any] | None,
    yield_2y_diff: float | None = None,
) -> str:
    macro = macro_doc or {}
    dxy = (macro.get("dollar_positioning") or {}).get("primary_label") or "neutral"
    treas = (macro.get("treasury_positioning") or {}).get("yield_bias") or "neutral"

    opener = f"{pair} valuation is {status.lower()}"
    if momentum != MOMENTUM_UNCHANGED:
        opener += f" and {momentum.lower()}"

    leg_bits: list[str] = []
    if base_score is not None and quote_score is not None:
        if base_score > quote_score + 5:
            leg_bits.append(f"{base} macro score improved while {quote} weakened")
        elif quote_score > base_score + 5:
            leg_bits.append(f"{quote} macro score improved while {base} weakened")
        else:
            leg_bits.append(f"{base} and {quote} macro scores remain balanced")

    if yield_2y_diff is not None and momentum == MOMENTUM_IMPROVING:
        leg_bits.append("2Y yield differential improved")
    elif yield_2y_diff is not None and momentum == MOMENTUM_WEAKENING:
        leg_bits.append("2Y yield differential weakened")

    leg_bits.append(f"DXY remains {str(dxy).lower()}")
    leg_bits.append(f"Treasury regime is {str(treas).lower()}")

    body = leg_bits[0] if leg_bits else "Macro drivers are mixed"
    if len(leg_bits) > 1:
        body = f"{body}; {', '.join(leg_bits[1:])}"
    return f"{opener} because {body}."


def build_fx_valuation_panel(
    pair: str,
    val_block: dict[str, Any] | None,
    *,
    macro_doc: dict[str, Any] | None = None,
    history: dict[str, Any] | None = None,
    currencies: dict[str, Any] | None = None,
) -> FxValuationPanelBlock:
    if not val_block or val_block.get("supported") is False:
        return FxValuationPanelBlock(
            pair=pair,
            score=0.0,
            bias="Neutral",
            momentum=MOMENTUM_UNCHANGED,
            daily_change=None,
            weekly_change=None,
            status=STATUS_NEUTRAL,
            drivers={},
            narrative=f"{pair} valuation is unavailable — run fx valuation export.",
        )

    base = str(val_block.get("base") or pair.split("/")[0]).upper()
    quote = str(val_block.get("quote") or pair.split("/")[1]).upper()
    adj = _num(val_block.get("positioning_adjusted_score_differential"))
    raw = _num(val_block.get("pair_score_differential"))
    diff = adj if adj is not None else raw
    bias = _normalize_bias(val_block.get("valuation_bias") or val_block.get("positioning_bias"))
    gap = _num(val_block.get("valuation_gap_pct"))
    direction = "long" if bias == "Bullish" else "short" if bias == "Bearish" else "neutral"

    from hptl.setup_ranking.layers import score_valuation_edge_layer

    edge_layer = score_valuation_edge_layer(pair=pair, direction=direction, val_block=val_block)
    score = edge_layer.score

    overlay = val_block.get("macro_positioning_overlay") or {}
    deltas = history_deltas_for_pair(pair, diff)
    daily_change = deltas.get("daily")
    weekly_change = deltas.get("weekly")
    if weekly_change is None and raw is not None and adj is not None:
        weekly_change = round(adj - raw, 1)

    momentum = _momentum_from_delta(weekly_change if weekly_change is not None else daily_change)
    status = _valuation_status(score=score, momentum=momentum, gap_pct=gap)

    dxy_block = overlay.get("dxy_positioning") or (macro_doc or {}).get("dollar_positioning") or {}
    treas_block = overlay.get("treasury_positioning") or (macro_doc or {}).get("treasury_positioning") or {}

    drivers = {
        "policy_rate_differential": _driver_line("Policy rate differential", val_block.get("policy_rate_diff"), suffix=" pp"),
        "yield_2y_differential": _driver_line("2Y yield differential", val_block.get("yield_2y_diff"), suffix=" pp"),
        "real_yield_differential": _driver_line("Real yield differential", val_block.get("real_yield_diff"), suffix=" pp"),
        "fair_value_gap": _driver_line("Fair-value gap", gap, suffix="%"),
        "spot_vs_fair_value": _driver_line("Spot vs fair value", val_block.get("spot_deviation_pct"), suffix="%"),
        "dxy_tff_overlay": {
            "label": "DXY TFF overlay",
            "value": dxy_block.get("net"),
            "display": dxy_block.get("primary_label") or "—",
            "detail": dxy_block.get("explanation") or "",
        },
        "treasury_positioning_overlay": {
            "label": "Treasury positioning overlay",
            "value": treas_block.get("aggregate_net"),
            "display": treas_block.get("score_label") or treas_block.get("bond_bias") or "—",
            "detail": treas_block.get("explanation") or "",
        },
    }

    cur = currencies or {}
    base_cv = _num((cur.get(base) or {}).get("valuation_score"))
    quote_cv = _num((cur.get(quote) or {}).get("valuation_score"))

    narrative = build_valuation_narrative(
        pair=pair,
        bias=bias,
        status=status,
        momentum=momentum,
        base=base,
        quote=quote,
        base_score=base_cv,
        quote_score=quote_cv,
        macro_doc=macro_doc,
        yield_2y_diff=_num(val_block.get("yield_2y_diff")),
    )

    return FxValuationPanelBlock(
        pair=pair,
        score=score,
        bias=bias,
        momentum=momentum,
        daily_change=daily_change,
        weekly_change=weekly_change,
        status=status,
        drivers=drivers,
        narrative=narrative,
        gap_pct=gap,
        pair_score_differential=raw,
        adjusted_score_differential=adj,
    )


def build_all_valuation_panels(
    val_doc: dict[str, Any] | None,
    *,
    history: dict[str, Any] | None = None,
) -> dict[str, dict[str, Any]]:
    if not val_doc:
        return {}
    macro = val_doc.get("macro_positioning") or {}
    currencies = val_doc.get("currencies") or val_doc.get("currency_scores") or {}
    out: dict[str, dict[str, Any]] = {}
    for block in val_doc.get("pairs") or []:
        if not block or block.get("supported") is False:
            continue
        pid = str(block.get("pair") or "")
        if not pid:
            continue
        panel = build_fx_valuation_panel(pid, block, macro_doc=macro, history=history, currencies=currencies)
        out[pid] = panel.as_dict()
    return out

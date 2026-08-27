"""US Dollar Index (DXY) positioning score from CFTC TFF leveraged-money cohort."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from hptl.cot.tff_macro_contracts import TFF_MACRO_SYMBOLS

DXY_INSTRUMENT = "US Dollar Index / DX"

STRONG_DOLLAR = "Strong Dollar"
WEAK_DOLLAR = "Weak Dollar"
DOLLAR_STRENGTHENING = "Dollar Strengthening"
DOLLAR_WEAKENING = "Dollar Weakening"
CROWDED_LONG_DOLLAR = "Crowded Long Dollar"
CROWDED_SHORT_DOLLAR = "Crowded Short Dollar"
NEUTRAL = "Neutral"

_CROWDED_HIGH = 85.0
_CROWDED_LOW = 15.0
_NET_STRONG = 5000.0
_CHANGE_THRESHOLD = 1500.0
_OI_TREND_THRESHOLD = 0.02  # 2% weekly OI change


@dataclass(frozen=True)
class DollarPositioningScore:
    score_labels: tuple[str, ...]
    primary_label: str
    net: float | None
    one_week_net_change: float | None
    net_percentile_13w: float | None
    open_interest: float | None
    oi_weekly_change_pct: float | None
    report_date: str | None
    available: bool
    explanation: str

    def as_dict(self) -> dict[str, Any]:
        return {
            "score_labels": list(self.score_labels),
            "primary_label": self.primary_label,
            "net": self.net,
            "one_week_net_change": self.one_week_net_change,
            "net_percentile_13w": self.net_percentile_13w,
            "open_interest": self.open_interest,
            "oi_weekly_change_pct": self.oi_weekly_change_pct,
            "report_date": self.report_date,
            "available": self.available,
            "symbol": TFF_MACRO_SYMBOLS.get(DXY_INSTRUMENT, "DXY"),
            "explanation": self.explanation,
        }


def _oi_trend(weeks: list[dict[str, Any]]) -> float | None:
    if len(weeks) < 2:
        return None
    prev_oi = weeks[-2].get("open_interest")
    cur_oi = weeks[-1].get("open_interest")
    if prev_oi is None or cur_oi is None or float(prev_oi) == 0:
        return None
    return round((float(cur_oi) - float(prev_oi)) / float(prev_oi), 4)


def score_dollar_positioning(snapshot: dict[str, Any] | None) -> DollarPositioningScore:
    """Derive dollar positioning score from TFF DXY snapshot."""
    empty = DollarPositioningScore(
        score_labels=(NEUTRAL,),
        primary_label=NEUTRAL,
        net=None,
        one_week_net_change=None,
        net_percentile_13w=None,
        open_interest=None,
        oi_weekly_change_pct=None,
        report_date=None,
        available=False,
        explanation="TFF DXY positioning unavailable.",
    )
    if not snapshot:
        return empty

    block = (snapshot.get("instruments") or {}).get(DXY_INSTRUMENT) or {}
    if not block.get("available"):
        return empty

    latest = block.get("latest") or {}
    weeks = block.get("weeks") if isinstance(block.get("weeks"), list) else []
    net = latest.get("net")
    chg = latest.get("one_week_net_change")
    pct = latest.get("net_percentile_13w")
    oi = latest.get("open_interest")
    oi_trend = _oi_trend(weeks)

    labels: list[str] = []
    if net is not None:
        if float(net) >= _NET_STRONG:
            labels.append(STRONG_DOLLAR)
        elif float(net) <= -_NET_STRONG:
            labels.append(WEAK_DOLLAR)

    if chg is not None:
        if float(chg) >= _CHANGE_THRESHOLD:
            labels.append(DOLLAR_STRENGTHENING)
        elif float(chg) <= -_CHANGE_THRESHOLD:
            labels.append(DOLLAR_WEAKENING)

    if pct is not None:
        p = float(pct)
        if p >= _CROWDED_HIGH:
            labels.append(CROWDED_LONG_DOLLAR)
        elif p <= _CROWDED_LOW:
            labels.append(CROWDED_SHORT_DOLLAR)

    if not labels:
        labels = [NEUTRAL]

    primary = labels[0]
    parts = [f"DXY TFF net {net:,.0f}" if net is not None else "DXY TFF net n/a"]
    if chg is not None:
        parts.append(f"Δ1w {chg:,.0f}")
    if pct is not None:
        parts.append(f"13w %ile {pct:.1f}")
    if oi_trend is not None:
        parts.append(f"OI Δ {oi_trend * 100:.1f}%")

    return DollarPositioningScore(
        score_labels=tuple(labels),
        primary_label=primary,
        net=net,
        one_week_net_change=chg,
        net_percentile_13w=pct,
        open_interest=oi,
        oi_weekly_change_pct=oi_trend,
        report_date=latest.get("date"),
        available=True,
        explanation=" · ".join(parts),
    )

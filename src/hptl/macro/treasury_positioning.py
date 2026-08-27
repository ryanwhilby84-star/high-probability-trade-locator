"""Treasury futures positioning score from CFTC TFF leveraged-money cohort."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from hptl.cot.tff_macro_contracts import TREASURY_TFF_INSTRUMENTS

# Primary score labels (user-facing).
BULLISH_BONDS = "Bullish Bonds"
BEARISH_BONDS = "Bearish Bonds"
BULLISH_YIELDS = "Bullish Yields"
BEARISH_YIELDS = "Bearish Yields"
NEUTRAL = "Neutral"

# Tenor weights for aggregate (2Y/5Y/10Y/30Y per spec; TN informational only).
_TENOR_WEIGHTS: dict[str, float] = {
    "US 2-Year T-Note / ZT": 0.20,
    "US 5-Year T-Note / ZF": 0.25,
    "US 10-Year T-Note / ZN": 0.30,
    "US 30-Year T-Bond / ZB": 0.25,
}

_NET_THRESHOLD = 5000.0
_CHANGE_THRESHOLD = 2000.0


@dataclass(frozen=True)
class TreasuryPositioningScore:
    score_label: str
    bond_bias: str
    yield_bias: str
    aggregate_net: float | None
    aggregate_weekly_change: float | None
    tenor_nets: dict[str, float | None]
    tenor_changes: dict[str, float | None]
    report_date: str | None
    available: bool
    explanation: str

    def as_dict(self) -> dict[str, Any]:
        return {
            "score_label": self.score_label,
            "bond_bias": self.bond_bias,
            "yield_bias": self.yield_bias,
            "aggregate_net": self.aggregate_net,
            "aggregate_weekly_change": self.aggregate_weekly_change,
            "tenor_nets": dict(self.tenor_nets),
            "tenor_changes": dict(self.tenor_changes),
            "report_date": self.report_date,
            "available": self.available,
            "explanation": self.explanation,
        }


def _inst_latest(snapshot: dict[str, Any], instrument_id: str) -> dict[str, Any] | None:
    block = (snapshot.get("instruments") or {}).get(instrument_id) or {}
    if not block.get("available"):
        return None
    latest = block.get("latest")
    return latest if isinstance(latest, dict) else None


def score_treasury_positioning(snapshot: dict[str, Any] | None) -> TreasuryPositioningScore:
    """Derive treasury positioning score from TFF macro snapshot."""
    if not snapshot:
        return TreasuryPositioningScore(
            score_label=NEUTRAL,
            bond_bias=NEUTRAL,
            yield_bias=NEUTRAL,
            aggregate_net=None,
            aggregate_weekly_change=None,
            tenor_nets={},
            tenor_changes={},
            report_date=None,
            available=False,
            explanation="TFF treasury positioning unavailable.",
        )

    tenor_nets: dict[str, float | None] = {}
    tenor_changes: dict[str, float | None] = {}
    weighted_net = 0.0
    weighted_change = 0.0
    weight_sum = 0.0
    report_dates: list[str] = []

    for iid in TREASURY_TFF_INSTRUMENTS:
        latest = _inst_latest(snapshot, iid)
        net = latest.get("net") if latest else None
        chg = latest.get("one_week_net_change") if latest else None
        tenor_nets[iid] = net
        tenor_changes[iid] = chg
        w = _TENOR_WEIGHTS.get(iid, 0.0)
        if net is not None and w > 0:
            weighted_net += w * float(net)
            weight_sum += w
        if chg is not None and w > 0:
            weighted_change += w * float(chg)
        if latest and latest.get("date"):
            report_dates.append(str(latest["date"]))

    if weight_sum <= 0:
        return TreasuryPositioningScore(
            score_label=NEUTRAL,
            bond_bias=NEUTRAL,
            yield_bias=NEUTRAL,
            aggregate_net=None,
            aggregate_weekly_change=None,
            tenor_nets=tenor_nets,
            tenor_changes=tenor_changes,
            report_date=max(report_dates) if report_dates else snapshot.get("generated_at"),
            available=False,
            explanation="No TFF treasury net positioning rows resolved.",
        )

    agg_net = round(weighted_net / weight_sum, 1)
    agg_chg = round(weighted_change / weight_sum, 1) if weighted_change else 0.0

    if agg_net >= _NET_THRESHOLD or (agg_chg >= _CHANGE_THRESHOLD and agg_net > 0):
        bond_bias = BULLISH_BONDS
        yield_bias = BEARISH_YIELDS
        label = BULLISH_BONDS
        expl = f"Leveraged money net long duration (agg net {agg_net:,.0f}, Δ1w {agg_chg:,.0f})."
    elif agg_net <= -_NET_THRESHOLD or (agg_chg <= -_CHANGE_THRESHOLD and agg_net < 0):
        bond_bias = BEARISH_BONDS
        yield_bias = BULLISH_YIELDS
        label = BEARISH_BONDS
        expl = f"Leveraged money net short duration (agg net {agg_net:,.0f}, Δ1w {agg_chg:,.0f})."
    else:
        bond_bias = NEUTRAL
        yield_bias = NEUTRAL
        label = NEUTRAL
        expl = f"Treasury positioning mixed/neutral (agg net {agg_net:,.0f}, Δ1w {agg_chg:,.0f})."

    return TreasuryPositioningScore(
        score_label=label,
        bond_bias=bond_bias,
        yield_bias=yield_bias,
        aggregate_net=agg_net,
        aggregate_weekly_change=agg_chg,
        tenor_nets=tenor_nets,
        tenor_changes=tenor_changes,
        report_date=max(report_dates) if report_dates else None,
        available=True,
        explanation=expl,
    )

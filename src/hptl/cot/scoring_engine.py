"""Weighted probabilistic COT scoring: signal strength separate from confidence.

Does not change COT parsing. Produces continuous scores (1 decimal), persistence- and
price-response-aware confidence, and richer positioning briefs.

All numeric comparisons route through :mod:`hptl.validation` for fault tolerance.
"""
from __future__ import annotations

import math
import os
from dataclasses import dataclass, field
from typing import Any

import pandas as pd

from hptl.validation import (
    ScoringComponentStatus,
    ValidationReport,
    coerce_series_numeric,
    safe_abs,
    safe_float,
    safe_gt,
    safe_gte,
    safe_is_negative,
    safe_is_positive,
    safe_is_zero,
    safe_int,
    safe_lt,
    safe_lte,
    safe_numeric,
    validate_fields,
)

# Contract-scale normalization (typical large futures books).
_NET_SCALE = 45_000.0
_FLOW_SCALE = 12_000.0

COT_SCORING_FIELD_NAMES = (
    "net_value",
    "weekly_change",
    "four_week_change",
    "long_weekly_change",
    "short_weekly_change",
    "price_week_pct",
)

CRITICAL_COT_FIELDS = ("net_value", "weekly_change")
OPTIONAL_COT_FIELDS = ("four_week_change", "long_weekly_change", "short_weekly_change", "price_week_pct")

# Backward-compatible alias used across context layers.
_finite = safe_float


@dataclass(frozen=True)
class CotScoreResult:
    cot_bias: str
    cot_directional_bias: str
    signal_strength: float
    score_confidence: float
    cot_score: float
    cot_strength: str
    market_state: str
    cot_interpretation: str
    cot_summary: str


@dataclass(frozen=True)
class CotScoreDiagnostics:
    result: CotScoreResult
    validation: ValidationReport
    components: tuple[ScoringComponentStatus, ...] = field(default_factory=tuple)

    def summary_text(self) -> str:
        return self.validation.summary_text()


def _clamp(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))


def _tanh_norm(v: float | None, scale: float) -> float:
    if v is None or scale <= 0:
        return 0.0
    return math.tanh(v / scale)


def _strength_label(score: float) -> str:
    if safe_lte(score, 3.0):
        return "Weak"
    if safe_lte(score, 5.5):
        return "Moderate"
    if safe_lte(score, 7.5):
        return "Strong"
    return "Very Strong"


def validate_cot_scoring_inputs(
    *,
    net: Any = None,
    w1: Any = None,
    w4: Any = None,
    long_w1: Any = None,
    short_w1: Any = None,
    price_week_pct: Any = None,
    debug: bool | None = None,
) -> ValidationReport:
    """Validate raw COT scoring inputs before numeric logic runs."""
    return validate_fields(
        {
            "net_value": net,
            "weekly_change": w1,
            "four_week_change": w4,
            "long_weekly_change": long_w1,
            "short_weekly_change": short_w1,
            "price_week_pct": price_week_pct,
        },
        debug=debug,
    )


def _direction_from_net(net: float | None, w1: float | None) -> str:
    if safe_is_negative(net):
        return "Bearish"
    if safe_is_positive(net):
        return "Bullish"
    if safe_is_negative(w1):
        return "Bearish"
    if safe_is_positive(w1):
        return "Bullish"
    return "Neutral"


def _bias_label(direction: str, net: float | None, w1: float | None, w4: float | None) -> str:
    if direction == "Neutral":
        return "Neutralising"
    improving = False
    weakening = False
    if safe_is_negative(net):
        improving = safe_is_positive(w1) or safe_is_positive(w4)
        weakening = safe_is_negative(w1) or safe_is_negative(w4)
    elif safe_is_positive(net):
        improving = safe_is_positive(w1) or safe_is_positive(w4)
        weakening = safe_is_negative(w1) or safe_is_negative(w4)
    if direction == "Bearish" and improving:
        return "Bearish / Improving"
    if direction == "Bullish" and weakening:
        return "Bullish / Weakening"
    return direction


def _flows_aligned(n: float | None, w: float | None) -> bool:
    return (safe_is_negative(n) and safe_is_negative(w)) or (safe_is_positive(n) and safe_is_positive(w))


def _flows_opposed(n: float | None, w: float | None) -> bool:
    return (safe_is_negative(n) and safe_is_positive(w)) or (safe_is_positive(n) and safe_is_negative(w))


def _persistence_features(hist: pd.DataFrame) -> dict[str, float]:
    """Multi-week behaviour from prior rows only (excludes current row)."""
    out = {
        "aligned_weeks": 0.0,
        "opposed_weeks": 0.0,
        "accel_ratio": 0.0,
        "participation_expansion": 0.0,
        "oi_persistence": 0.0,
    }
    if hist.shape[0] < 2:
        return out

    h = hist.copy()
    if "cot_report_date" in h.columns:
        h["cot_report_date"] = pd.to_datetime(h["cot_report_date"], errors="coerce")
    h = h.sort_values("cot_report_date").tail(6)

    def _num_series(name: str) -> pd.Series:
        col = h.get(name)
        if col is None:
            return pd.Series([float("nan")] * len(h), index=h.index)
        return coerce_series_numeric(col)

    w1s = _num_series("weekly_change")
    nets = _num_series("net_value")
    long_d = _num_series("long_weekly_change")
    short_d = _num_series("short_weekly_change")

    aligned = 0
    opposed = 0
    for i in range(1, len(h)):
        w = safe_float(w1s.iloc[i]) if i < len(w1s) else None
        n = safe_float(nets.iloc[i - 1]) if i - 1 < len(nets) else None
        if w is None or n is None or safe_is_zero(n):
            continue
        if _flows_aligned(n, w):
            aligned += 1
        elif _flows_opposed(n, w):
            opposed += 1
    out["aligned_weeks"] = float(aligned)
    out["opposed_weeks"] = float(opposed)

    w1_last = safe_float(w1s.iloc[-1]) if len(w1s) else None
    w4_proxy = None
    if len(w1s) >= 4:
        chunk = w1s.iloc[-4:]
        if chunk.notna().all():
            w4_proxy = safe_float(chunk.sum())
    if w1_last is not None and w4_proxy is not None and safe_gt(safe_abs(w4_proxy), 1):
        out["accel_ratio"] = _clamp(w1_last / (w4_proxy / 4.0), -2.0, 2.0)

    exp = 0
    for i in range(1, len(h)):
        dl = safe_float(long_d.iloc[i]) if i < len(long_d) else None
        ds = safe_float(short_d.iloc[i]) if i < len(short_d) else None
        if dl is not None and ds is not None and safe_gt(dl, 500) and safe_gt(ds, 500):
            exp += 1
        elif dl is not None and ds is not None and safe_lt(dl, -500) and safe_lt(ds, -500):
            exp -= 1
    out["participation_expansion"] = float(exp)

    if len(h) >= 3 and long_d is not None and short_d is not None:
        lt = long_d.tail(3)
        st = short_d.tail(3)
        both_up = int(((lt > 0) & (st > 0)).sum())
        both_down = int(((lt < 0) & (st < 0)).sum())
        out["oi_persistence"] = _clamp((both_up - both_down) / 3.0, -1.0, 1.0)

    return out


def _price_alignment(direction: str, price_week_pct: float | None) -> float:
    """1 = confirming, 0.5 = neutral/unknown, 0 = diverging."""
    p = safe_float(price_week_pct)
    if p is None:
        return 0.5
    if safe_lt(safe_abs(p), 0.15):
        return 0.55
    if direction == "Bullish":
        if safe_gt(p, 0.25):
            return 1.0
        if safe_lt(p, -0.25):
            return 0.0
        return 0.45
    if direction == "Bearish":
        if safe_lt(p, -0.25):
            return 1.0
        if safe_gt(p, 0.25):
            return 0.0
        return 0.45
    return 0.5


def _classify_market_state(
    direction: str,
    net: float | None,
    w1: float | None,
    w4: float | None,
    persist: dict[str, float],
    price_align: float,
    long_w1: float | None,
    short_w1: float | None,
) -> str:
    aligned = persist.get("aligned_weeks", 0)
    opposed = persist.get("opposed_weeks", 0)
    part = persist.get("participation_expansion", 0)

    if safe_lte(part, -2):
        return "participation_collapse"
    if direction == "Bearish" and safe_lte(price_align, 0.25) and safe_is_negative(w1):
        return "squeeze_risk"
    if direction == "Bullish" and safe_lte(price_align, 0.25) and safe_is_positive(w1):
        return "distribution_during_strength"
    if direction == "Bearish" and safe_gte(price_align, 0.75) and safe_gte(opposed, 2):
        return "positioning_failure"
    if direction == "Bullish" and safe_lte(price_align, 0.35) and safe_is_positive(w1):
        return "accumulation_during_weakness"
    if direction == "Bullish" and safe_gte(price_align, 0.75) and safe_gte(aligned, 2):
        return "trend_persistence"
    if direction == "Bearish" and safe_gte(price_align, 0.75) and safe_gte(aligned, 2):
        return "trend_persistence"
    if (
        safe_gte(aligned, 3)
        and w1 is not None
        and w4 is not None
        and ((safe_is_positive(w1) and safe_is_negative(w4)) or (safe_is_negative(w1) and safe_is_positive(w4)))
    ):
        return "exhaustion_risk"
    if long_w1 is not None and short_w1 is not None and net is not None:
        if safe_is_negative(net) and safe_is_positive(long_w1) and safe_is_negative(short_w1):
            return "accumulation_during_weakness"
        if safe_is_positive(net) and safe_is_negative(long_w1) and safe_is_positive(short_w1):
            return "distribution_during_strength"
    return "mixed_transition"


def _brief_for_state(
    state: str,
    direction: str,
    bias: str,
    net: int | None,
    persist: dict[str, float],
    price_align: float,
    signal: float,
    confidence: float,
) -> str:
    aligned = int(persist.get("aligned_weeks", 0))
    part = persist.get("participation_expansion", 0)
    conf_word = "high" if safe_gte(confidence, 0.72) else "moderate" if safe_gte(confidence, 0.48) else "low"

    if state == "squeeze_risk":
        return (
            f"Persistent {bias.lower()} positioning is failing to suppress price despite "
            f"{'expanding' if safe_gt(part, 0) else 'active'} participation over recent weeks. "
            f"Squeeze risk is elevated; signal strength {signal:.1f}/10 with {conf_word} confidence "
            f"because price diverges from the book."
        )
    if state == "positioning_failure":
        return (
            f"{bias} managed-money pressure has not translated into price follow-through for "
            f"{aligned} consecutive aligned weeks. Positioning failure raises fade risk; "
            f"context ranks {signal:.1f}/10 with {conf_word} confidence."
        )
    if state == "trend_persistence":
        return (
            f"{bias} positioning shows multi-week persistence ({aligned} aligned weeks) with "
            f"price confirming the flow. Trend persistence supports asymmetry in the "
            f"{direction.lower()} direction ({signal:.1f}/10 signal, {conf_word} confidence)."
        )
    if state == "accumulation_during_weakness":
        return (
            f"Specs remain {bias.lower()} on net but are adding length while price lags—classic "
            f"accumulation during weakness. Upside risk builds if price responds; "
            f"{signal:.1f}/10 signal, {conf_word} confidence."
        )
    if state == "distribution_during_strength":
        return (
            f"Net-long positioning is softening into firm price—distribution during strength. "
            f"Supply reactions carry better asymmetry than fresh chase longs "
            f"({signal:.1f}/10, {conf_word} confidence)."
        )
    if state == "participation_collapse":
        return (
            f"Both sides are reducing exposure—participation collapse. Prior directional edge is "
            f"decaying; treat as low-conviction ({signal:.1f}/10 signal, {conf_word} confidence)."
        )
    if state == "exhaustion_risk":
        return (
            f"Short-horizon momentum opposes the four-week drift—exhaustion risk in the current "
            f"{bias.lower()} lean. Signal {signal:.1f}/10 with {conf_word} confidence until flows realign."
        )
    side = "short" if safe_is_negative(net) else "long" if safe_is_positive(net) else "flat"
    return (
        f"{bias} {side} book with {aligned} weeks of flow aligned to net. "
        f"Price {'confirms' if safe_gte(price_align, 0.7) else 'does not confirm' if safe_lte(price_align, 0.3) else 'is mixed vs'} "
        f"positioning ({signal:.1f}/10 signal, {conf_word} confidence)—ranking context only, not a forecast."
    )


def _build_component_status(
    *,
    name: str,
    raw: Any,
    parsed: float | None,
    score: float | str,
    weight: float,
    contribution: float | None,
) -> ScoringComponentStatus:
    if parsed is None:
        status = "UNKNOWN"
        score_out: float | str = "UNKNOWN"
        contribution_out = None
    else:
        status = "OK"
        score_out = score
        contribution_out = contribution
    return ScoringComponentStatus(
        name=name,
        raw_value=raw,
        parsed_value=parsed,
        score=score_out,
        weight=weight,
        contribution=contribution_out,
        status=status,
    )


def score_cot_row_with_diagnostics(
    *,
    net: Any = None,
    w1: Any = None,
    w4: Any = None,
    long_w1: Any = None,
    short_w1: Any = None,
    persist: dict[str, float],
    price_week_pct: Any = None,
    debug: bool | None = None,
) -> CotScoreDiagnostics:
    """Score one COT row with validation report and component breakdown."""
    validation = validate_cot_scoring_inputs(
        net=net,
        w1=w1,
        w4=w4,
        long_w1=long_w1,
        short_w1=short_w1,
        price_week_pct=price_week_pct,
        debug=debug,
    )

    net_f = safe_float(net)
    w1_f = safe_float(w1)
    w4_f = safe_float(w4)
    long_w1_f = safe_float(long_w1)
    short_w1_f = safe_float(short_w1)
    price_f = safe_float(price_week_pct)

    direction = _direction_from_net(net_f, w1_f)
    if direction == "Neutral" and (w1_f is None or safe_is_zero(w1_f)):
        result = CotScoreResult(
            cot_bias="Neutralising",
            cot_directional_bias="Neutral",
            signal_strength=0.0,
            score_confidence=round(0.35 * validation.confidence_multiplier(critical=CRITICAL_COT_FIELDS, optional=OPTIONAL_COT_FIELDS), 2),
            cot_score=0.0,
            cot_strength="Weak",
            market_state="mixed_transition",
            cot_interpretation="Managed-money net is flat and weekly flow is unchanged.",
            cot_summary="No directional managed-money impulse; positioning is neutralising.",
        )
        components = (
            _build_component_status(name="net_magnitude", raw=net, parsed=net_f, score="UNKNOWN", weight=0.22, contribution=None),
            _build_component_status(name="weekly_momentum", raw=w1, parsed=w1_f, score="UNKNOWN", weight=0.28, contribution=None),
        )
        return CotScoreDiagnostics(result=result, validation=validation, components=components)

    bias = _bias_label(direction, net_f, w1_f, w4_f)
    sign = -1.0 if direction == "Bearish" else 1.0

    net_mag = _tanh_norm(net_f, _NET_SCALE) if net_f is not None else 0.0
    m1_raw = sign * _tanh_norm(w1_f, _FLOW_SCALE) if w1_f is not None else None
    m4_raw = sign * _tanh_norm(w4_f, _FLOW_SCALE * 1.6) if w4_f is not None else None
    m1 = _clamp(m1_raw, 0.0, 1.0) if m1_raw is not None else None
    m4 = _clamp(m4_raw, 0.0, 1.0) if m4_raw is not None else None

    aligned = persist.get("aligned_weeks", 0)
    opposed = persist.get("opposed_weeks", 0)
    persist_score = _clamp((aligned - 0.35 * opposed) / 4.0, 0.0, 1.0)
    accel = persist.get("accel_ratio", 0.0)
    accel_score = _clamp(0.5 + 0.25 * sign * accel, 0.0, 1.0)
    part = persist.get("participation_expansion", 0)
    part_score = _clamp(0.5 + 0.12 * part, 0.0, 1.0)

    net_contrib = 0.22 * net_mag if net_f is not None else 0.0
    m1_contrib = 0.28 * m1 if m1 is not None else 0.0
    m4_contrib = 0.18 * m4 if m4 is not None else 0.0
    persist_contrib = 0.17 * persist_score
    accel_contrib = 0.08 * accel_score
    part_contrib = 0.07 * part_score

    conviction = net_contrib + m1_contrib + m4_contrib + persist_contrib + accel_contrib + part_contrib
    conviction = _clamp(conviction, 0.0, 1.0)
    signal_strength = round(1.0 + 9.0 * conviction, 1)

    price_align = _price_alignment(direction, price_f)
    data_quality = 0.85 if w1_f is not None and w4_f is not None else 0.55 if w1_f is not None else 0.4
    persistence_conf = 0.45 + 0.55 * persist_score
    price_conf = 0.35 + 0.65 * price_align
    single_week_penalty = 0.72 if safe_lt(aligned, 1) and safe_lt(opposed, 1) else 1.0

    score_confidence = round(
        _clamp(
            data_quality
            * persistence_conf
            * price_conf
            * single_week_penalty
            * validation.confidence_multiplier(critical=CRITICAL_COT_FIELDS, optional=OPTIONAL_COT_FIELDS),
            0.12,
            0.95,
        ),
        2,
    )

    if safe_lte(price_align, 0.2) and safe_gte(conviction, 0.55):
        score_confidence = round(min(score_confidence, 0.42), 2)

    state = _classify_market_state(direction, net_f, w1_f, w4_f, persist, price_align, long_w1_f, short_w1_f)
    net_i = safe_int(net_f)
    summary = _brief_for_state(state, direction, bias, net_i, persist, price_align, signal_strength, score_confidence)

    interp_parts = [f"Managed money net {net_i if net_i is not None else 'N/A'}"]
    if w1_f is not None:
        interp_parts.append(f"1w net Δ {int(w1_f):+d}")
    if w4_f is not None:
        interp_parts.append(f"4w net Δ {int(w4_f):+d}")
    if price_f is not None:
        interp_parts.append(f"price ~{price_f:+.1f}% over the week")
    interp_parts.append(f"state={state.replace('_', ' ')}")
    interpretation = "; ".join(interp_parts) + "."

    result = CotScoreResult(
        cot_bias=bias,
        cot_directional_bias=direction,
        signal_strength=signal_strength,
        score_confidence=score_confidence,
        cot_score=signal_strength,
        cot_strength=_strength_label(signal_strength),
        market_state=state,
        cot_interpretation=interpretation,
        cot_summary=summary,
    )

    components = (
        _build_component_status(name="net_magnitude", raw=net, parsed=net_f, score=net_mag, weight=0.22, contribution=net_contrib),
        _build_component_status(name="weekly_momentum", raw=w1, parsed=w1_f, score=m1 if m1 is not None else "UNKNOWN", weight=0.28, contribution=m1_contrib if m1 is not None else None),
        _build_component_status(name="four_week_momentum", raw=w4, parsed=w4_f, score=m4 if m4 is not None else "UNKNOWN", weight=0.18, contribution=m4_contrib if m4 is not None else None),
        _build_component_status(name="trend_persistence", raw={"aligned": aligned, "opposed": opposed}, parsed=persist_score, score=persist_score, weight=0.17, contribution=persist_contrib),
        _build_component_status(name="acceleration", raw=accel, parsed=accel, score=accel_score, weight=0.08, contribution=accel_contrib),
        _build_component_status(name="participation", raw=part, parsed=part, score=part_score, weight=0.07, contribution=part_contrib),
    )
    return CotScoreDiagnostics(result=result, validation=validation, components=components)


def score_cot_row(
    *,
    net: Any = None,
    w1: Any = None,
    w4: Any = None,
    long_w1: Any = None,
    short_w1: Any = None,
    persist: dict[str, float],
    price_week_pct: Any = None,
) -> CotScoreResult:
    """Score one COT row; never raises on bad numeric inputs."""
    return score_cot_row_with_diagnostics(
        net=net,
        w1=w1,
        w4=w4,
        long_w1=long_w1,
        short_w1=short_w1,
        persist=persist,
        price_week_pct=price_week_pct,
    ).result


def _load_price_week_pct_series(market: str) -> pd.Series | None:
    if os.environ.get("HPTL_SKIP_PRICE_SCORING", "").strip().lower() in ("1", "true", "yes"):
        return None
    try:
        from hptl.macro.macro_relationship_maps import _profiles
        from hptl.macro.fred_relationship_pair import _fred_series_csv

        profile = _profiles().get(market)
        if not profile:
            return None
        sid = profile.get("price_fred_id")
        start = profile.get("observation_start") or "2018-01-01"
        if not sid:
            return None
        df = _fred_series_csv(str(sid), str(start))
        if df.empty:
            return None
        s = df.set_index("date")["value"].sort_index().pct_change(5) * 100.0  # ~1w on daily
        return s.dropna()
    except Exception:
        return None


_price_cache: dict[str, pd.Series | None] = {}


def price_week_pct_for_date(market: str, week: pd.Timestamp) -> float | None:
    if market not in _price_cache:
        _price_cache[market] = _load_price_week_pct_series(market)
    series = _price_cache[market]
    if series is None or series.empty:
        return None
    target = pd.Timestamp(week).normalize()
    avail = series.index[series.index <= target]
    if len(avail) == 0:
        return None
    return safe_float(series.loc[avail[-1]])


def apply_probabilistic_cot_scoring(cot: pd.DataFrame) -> pd.DataFrame:
    """Score all rows with history-aware persistence and optional FRED price response."""
    if cot.empty:
        return cot

    scored = cot.copy()
    scored["cot_report_date"] = pd.to_datetime(scored["cot_report_date"], errors="coerce")

    bias_list: list[str] = []
    dir_list: list[str] = []
    signal_list: list[float] = []
    conf_list: list[float] = []
    score_list: list[float] = []
    strength_list: list[str] = []
    state_list: list[str] = []
    interp_list: list[str] = []
    summary_list: list[str] = []

    for market, grp in scored.groupby("market", sort=False):
        g = grp.sort_values("cot_report_date")
        for idx, row in g.iterrows():
            week = row["cot_report_date"]
            hist = g.loc[g["cot_report_date"] < week]
            persist = _persistence_features(hist)
            price_pct = price_week_pct_for_date(str(market), week) if pd.notna(week) else None

            res = score_cot_row(
                net=row.get("net_value"),
                w1=row.get("weekly_change"),
                w4=row.get("four_week_change"),
                long_w1=row.get("long_weekly_change"),
                short_w1=row.get("short_weekly_change"),
                persist=persist,
                price_week_pct=price_pct,
            )
            bias_list.append((idx, res.cot_bias))
            dir_list.append((idx, res.cot_directional_bias))
            signal_list.append((idx, res.signal_strength))
            conf_list.append((idx, res.score_confidence))
            score_list.append((idx, res.cot_score))
            strength_list.append((idx, res.cot_strength))
            state_list.append((idx, res.market_state))
            interp_list.append((idx, res.cot_interpretation))
            summary_list.append((idx, res.cot_summary))

    def _assign(col: str, pairs: list[tuple[Any, Any]]) -> None:
        for i, v in pairs:
            scored.loc[i, col] = v

    _assign("cot_bias", bias_list)
    _assign("cot_directional_bias", dir_list)
    _assign("signal_strength", signal_list)
    _assign("score_confidence", conf_list)
    _assign("cot_score", score_list)
    _assign("cot_strength", strength_list)
    _assign("market_state", state_list)
    _assign("cot_interpretation", interp_list)
    _assign("cot_summary", summary_list)
    return scored

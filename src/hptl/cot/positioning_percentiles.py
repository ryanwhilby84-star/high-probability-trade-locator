"""Reusable multi-year positioning-percentile infrastructure.

Pure, framework-free helpers for ranking current COT positioning inside a
rolling historical window (e.g. 156 weeks = 3 years). Intentionally decoupled
from pandas/dataframes so the same primitives can power:

* the dashboard ``rolling_3y_history_context`` block,
* the institutional scoring engine (short-term vs multi-year extremes),
* future exhaustion / reversal / crowding / regime models.

Nothing here reads files or mutates global state — callers pass in raw values.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Iterable, Sequence

# 3 years of weekly COT reports.
WINDOW_WEEKS_3Y = 156

# Metric keys used across the percentile API.
METRIC_LONG = "long"
METRIC_SHORT = "short"
METRIC_NET = "net"
METRIC_OI = "open_interest"
POSITIONING_METRICS = (METRIC_LONG, METRIC_SHORT, METRIC_NET, METRIC_OI)

# Classification bands (lower-inclusive, upper-exclusive except the final band).
CLASS_EXTREME_LOW = "Extreme Low"
CLASS_LOW = "Low"
CLASS_NEUTRAL = "Neutral"
CLASS_HIGH = "High"
CLASS_EXTREME_HIGH = "Extreme High"
CLASS_NA = "N/A"

_PERCENTILE_BANDS: tuple[tuple[float, float, str], ...] = (
    (0.0, 10.0, CLASS_EXTREME_LOW),
    (10.0, 30.0, CLASS_LOW),
    (30.0, 70.0, CLASS_NEUTRAL),
    (70.0, 90.0, CLASS_HIGH),
    (90.0, 100.0, CLASS_EXTREME_HIGH),
)

_HIGH_CLASSES = frozenset({CLASS_HIGH, CLASS_EXTREME_HIGH})
_LOW_CLASSES = frozenset({CLASS_EXTREME_LOW, CLASS_LOW})


def _is_finite(value: float | int | None) -> bool:
    return value is not None and isinstance(value, (int, float)) and math.isfinite(float(value))


def classify_percentile(pct: float | None) -> str:
    """Map a 0–100 percentile to a five-band classification label."""
    if not _is_finite(pct):
        return CLASS_NA
    p = float(pct)
    if p < 0:
        p = 0.0
    if p > 100:
        p = 100.0
    for lo, hi, label in _PERCENTILE_BANDS:
        if label == CLASS_EXTREME_HIGH:
            if p >= lo:
                return label
        elif lo <= p < hi:
            return label
    return CLASS_EXTREME_HIGH


def is_high_class(label: str | None) -> bool:
    return label in _HIGH_CLASSES


def is_low_class(label: str | None) -> bool:
    return label in _LOW_CLASSES


def empirical_percentile_rank(window: Sequence[float], value: float | None) -> float:
    """Percentile rank in [0, 100]: tie-aware ``(below + 0.5*equal) / n``.

    Mirrors the convention used by the expanding/full-loaded history blocks so
    percentiles are comparable across windows.
    """
    if not _is_finite(value):
        return float("nan")
    finite = [float(x) for x in window if _is_finite(x)]
    n = len(finite)
    if n == 0:
        return float("nan")
    if n == 1:
        return 50.0
    v = float(value)
    below = sum(1 for x in finite if x < v)
    equal = sum(1 for x in finite if x == v)
    return 100.0 * (below + 0.5 * equal) / n


@dataclass(frozen=True)
class WindowStat:
    """Min / max / average over a rolling window for one metric."""

    minimum: float | None
    maximum: float | None
    average: float | None
    count: int

    def as_dict(self) -> dict[str, float | int | None]:
        return {
            "min": self.minimum,
            "max": self.maximum,
            "avg": self.average,
            "count": self.count,
        }


def window_stat(values: Iterable[float]) -> WindowStat:
    """Min/max/avg over the finite values already sliced to the desired window."""
    finite = [float(x) for x in values if _is_finite(x)]
    if not finite:
        return WindowStat(minimum=None, maximum=None, average=None, count=0)
    return WindowStat(
        minimum=min(finite),
        maximum=max(finite),
        average=sum(finite) / len(finite),
        count=len(finite),
    )


# --- Interpretation copy (per task spec) ------------------------------------

_INTERPRETATIONS: dict[str, dict[str, str]] = {
    METRIC_NET: {
        CLASS_EXTREME_HIGH: "Institutional positioning strongly bullish versus the last 3 years.",
        CLASS_HIGH: "Institutional positioning leaning bullish versus the last 3 years.",
        CLASS_NEUTRAL: "Net positioning mid-range versus the last 3 years.",
        CLASS_LOW: "Institutional positioning leaning bearish versus the last 3 years.",
        CLASS_EXTREME_LOW: "Institutional positioning strongly bearish versus the last 3 years.",
    },
    METRIC_LONG: {
        CLASS_EXTREME_HIGH: "Crowded long positioning.",
        CLASS_HIGH: "Crowded long positioning.",
        CLASS_NEUTRAL: "Moderate long participation versus the last 3 years.",
        CLASS_LOW: "Very little long participation.",
        CLASS_EXTREME_LOW: "Very little long participation.",
    },
    METRIC_SHORT: {
        CLASS_EXTREME_HIGH: "Crowded short positioning.",
        CLASS_HIGH: "Crowded short positioning.",
        CLASS_NEUTRAL: "Moderate short participation versus the last 3 years.",
        CLASS_LOW: "Very little short participation.",
        CLASS_EXTREME_LOW: "Very little short participation.",
    },
    METRIC_OI: {
        CLASS_EXTREME_HIGH: "Strong participation and institutional engagement.",
        CLASS_HIGH: "Strong participation and institutional engagement.",
        CLASS_NEUTRAL: "Moderate participation versus the last 3 years.",
        CLASS_LOW: "Weak participation and reduced conviction.",
        CLASS_EXTREME_LOW: "Weak participation and reduced conviction.",
    },
}


def interpret_metric(metric: str, pct: float | None) -> str:
    """Human-readable interpretation for a metric given its 3Y percentile."""
    label = classify_percentile(pct)
    if label == CLASS_NA:
        return "N/A: insufficient multi-year history for this metric."
    return _INTERPRETATIONS.get(metric, {}).get(label, "")


# Short, label-style classification lines (mirrors the UI "Classification:" block).
_METRIC_NOUN: dict[str, str] = {
    METRIC_NET: "Net Positioning",
    METRIC_LONG: "Long Positioning",
    METRIC_SHORT: "Short Positioning",
    METRIC_OI: "Participation",
}


def classification_line(metric: str, pct: float | None) -> str | None:
    """e.g. ``"High Net Positioning"`` / ``"Extreme High Short Positioning"``."""
    label = classify_percentile(pct)
    if label == CLASS_NA:
        return None
    noun = _METRIC_NOUN.get(metric, metric.title())
    return f"{label} {noun}"


# --- Absolute multi-year extremes (vs-3Y-max % / net range %) ----------------
#
# Percentiles answer "where are we relative to history?". The ratios below
# answer "how close are we to the largest positioning extreme observed in the
# last 3 years?". Both views are kept — they are complementary.

CROWD_NA = "N/A"

# Long / short crowding bands on the vs-3Y-max ratio (0-100%).
_LONG_CROWDING_BANDS: tuple[tuple[float, float, str], ...] = (
    (0.0, 50.0, "Low Long Participation"),
    (50.0, 75.0, "Normal Long Participation"),
    (75.0, 90.0, "High Long Participation"),
    (90.0, 100.0, "Crowded Long Positioning"),
)
_SHORT_CROWDING_BANDS: tuple[tuple[float, float, str], ...] = (
    (0.0, 50.0, "Low Short Participation"),
    (50.0, 75.0, "Normal Short Participation"),
    (75.0, 90.0, "High Short Participation"),
    (90.0, 100.0, "Crowded Short Positioning"),
)
_OI_PARTICIPATION_BANDS: tuple[tuple[float, float, str], ...] = (
    (0.0, 50.0, "Weak Participation"),
    (50.0, 75.0, "Normal Participation"),
    (75.0, 90.0, "Strong Participation"),
    (90.0, 100.0, "Extreme Participation"),
)


def _band_label(pct: float | None, bands: tuple[tuple[float, float, str], ...]) -> str:
    if not _is_finite(pct):
        return CROWD_NA
    p = min(100.0, max(0.0, float(pct)))
    for lo, hi, label in bands:
        if hi >= 100.0:
            if p >= lo:
                return label
        elif lo <= p < hi:
            return label
    return bands[-1][2]


def classify_long_crowding(pct: float | None) -> str:
    return _band_label(pct, _LONG_CROWDING_BANDS)


def classify_short_crowding(pct: float | None) -> str:
    return _band_label(pct, _SHORT_CROWDING_BANDS)


def classify_oi_participation(pct: float | None) -> str:
    return _band_label(pct, _OI_PARTICIPATION_BANDS)


def ratio_vs_max_pct(current: float | None, maximum: float | None) -> float:
    """``current / max * 100`` — how close current sits to the 3Y maximum."""
    if not _is_finite(current) or not _is_finite(maximum) or float(maximum) <= 0:
        return float("nan")
    return 100.0 * float(current) / float(maximum)


def net_range_pct(current: float | None, minimum: float | None, maximum: float | None) -> float:
    """Position of current net within the 3Y [min, max] band, 0-100%."""
    if not (_is_finite(current) and _is_finite(minimum) and _is_finite(maximum)):
        return float("nan")
    span = float(maximum) - float(minimum)
    if span <= 0:
        return float("nan")
    return 100.0 * (float(current) - float(minimum)) / span


@dataclass(frozen=True)
class AbsolutePositioningContext:
    """Current positioning expressed against absolute 3Y extremes (engine-facing)."""

    long_vs_3y_max_pct: float | None
    short_vs_3y_max_pct: float | None
    net_range_pct: float | None
    oi_vs_3y_max_pct: float | None
    long_crowding: str
    short_crowding: str
    oi_participation: str
    crowding_classification_lines: list[str]

    def as_dict(self) -> dict[str, object]:
        return {
            "long_vs_3y_max_pct": self.long_vs_3y_max_pct,
            "short_vs_3y_max_pct": self.short_vs_3y_max_pct,
            "net_range_pct": self.net_range_pct,
            "oi_vs_3y_max_pct": self.oi_vs_3y_max_pct,
            "long_crowding": self.long_crowding,
            "short_crowding": self.short_crowding,
            "oi_participation": self.oi_participation,
            "crowding_classification_lines": list(self.crowding_classification_lines),
        }


def _round1(v: float) -> float | None:
    return None if not _is_finite(v) else round(float(v), 1)


def compute_absolute_positioning(
    *,
    current_long: float | None,
    long_max: float | None,
    current_short: float | None,
    short_max: float | None,
    current_net: float | None,
    net_min: float | None,
    net_max: float | None,
    current_oi: float | None,
    oi_max: float | None,
) -> AbsolutePositioningContext:
    """Build the absolute multi-year crowding view from current values + extremes.

    Reusable by both the dashboard export and the scoring engine. Percentiles are
    computed separately and intentionally left untouched.
    """
    long_pct = _round1(ratio_vs_max_pct(current_long, long_max))
    short_pct = _round1(ratio_vs_max_pct(current_short, short_max))
    nr_pct = _round1(net_range_pct(current_net, net_min, net_max))
    oi_pct = _round1(ratio_vs_max_pct(current_oi, oi_max))

    long_crowding = classify_long_crowding(long_pct)
    short_crowding = classify_short_crowding(short_pct)
    oi_participation = classify_oi_participation(oi_pct)
    lines = [
        line
        for line in (long_crowding, short_crowding, oi_participation)
        if line and line != CROWD_NA
    ]
    return AbsolutePositioningContext(
        long_vs_3y_max_pct=long_pct,
        short_vs_3y_max_pct=short_pct,
        net_range_pct=nr_pct,
        oi_vs_3y_max_pct=oi_pct,
        long_crowding=long_crowding,
        short_crowding=short_crowding,
        oi_participation=oi_participation,
        crowding_classification_lines=lines,
    )


# --- Multi-year confluence (short-term vs genuine institutional extreme) -----

@dataclass(frozen=True)
class MultiYearConfluence:
    """Engine-facing verdict combining a short-term signal with 3Y context."""

    net_3y_percentile: float | None
    net_3y_class: str
    long_3y_percentile: float | None
    long_3y_class: str
    short_3y_percentile: float | None
    short_3y_class: str
    oi_3y_percentile: float | None
    oi_3y_class: str
    multiyear_extreme: bool
    squeeze_risk: bool
    verdict: str
    interpretation: str

    def as_dict(self) -> dict[str, object]:
        return {
            "net_3y_percentile": self.net_3y_percentile,
            "net_3y_class": self.net_3y_class,
            "long_3y_percentile": self.long_3y_percentile,
            "long_3y_class": self.long_3y_class,
            "short_3y_percentile": self.short_3y_percentile,
            "short_3y_class": self.short_3y_class,
            "oi_3y_percentile": self.oi_3y_percentile,
            "oi_3y_class": self.oi_3y_class,
            "multiyear_extreme": self.multiyear_extreme,
            "squeeze_risk": self.squeeze_risk,
            "verdict": self.verdict,
            "interpretation": self.interpretation,
        }


# Verdict codes (stable identifiers for downstream models).
VERDICT_NONE = "none"
VERDICT_SHORT_TERM_ONLY = "short_term_only"
VERDICT_MULTIYEAR_EXTREME = "multiyear_institutional_extreme"
VERDICT_SQUEEZE_RISK = "crowded_bearish_squeeze_risk"
VERDICT_MULTIYEAR_CONTEXT = "multiyear_context"

# Percentile thresholds for "genuine" multi-year extremes.
_MULTIYEAR_HIGH_THRESHOLD = 90.0
_MULTIYEAR_LOW_THRESHOLD = 10.0
_MULTIYEAR_CROWDED_THRESHOLD = 90.0


def assess_multiyear_confluence(
    *,
    net_3y_percentile: float | None,
    long_3y_percentile: float | None = None,
    short_3y_percentile: float | None = None,
    oi_3y_percentile: float | None = None,
    short_term_net_high: bool = False,
    short_term_net_low: bool = False,
    short_term_short_high: bool = False,
) -> MultiYearConfluence:
    """Distinguish short-term 13-week extremes from multi-year institutional extremes.

    Implements the engine cases from the spec:

    * 13W Net High + 3Y Net ~mid  -> short-term strength only.
    * 13W Net High + 3Y Net >=90  -> major institutional positioning extreme.
    * 13W Short High + 3Y Short >=90 -> crowded bearish trade / squeeze risk.
    """
    net_class = classify_percentile(net_3y_percentile)
    long_class = classify_percentile(long_3y_percentile)
    short_class = classify_percentile(short_3y_percentile)
    oi_class = classify_percentile(oi_3y_percentile)

    net_pct = float(net_3y_percentile) if _is_finite(net_3y_percentile) else None
    short_pct = float(short_3y_percentile) if _is_finite(short_3y_percentile) else None

    net_3y_extreme = net_pct is not None and (
        net_pct >= _MULTIYEAR_HIGH_THRESHOLD or net_pct <= _MULTIYEAR_LOW_THRESHOLD
    )
    short_3y_crowded = short_pct is not None and short_pct >= _MULTIYEAR_CROWDED_THRESHOLD

    squeeze_risk = bool(short_term_short_high and short_3y_crowded)

    verdict = VERDICT_NONE
    interpretation = ""

    if squeeze_risk:
        verdict = VERDICT_SQUEEZE_RISK
        interpretation = (
            "Crowded bearish trade: shorts are extreme both short-term and over the last 3 "
            "years — potential squeeze risk on any bullish catalyst."
        )
    elif (short_term_net_high or short_term_net_low) and net_3y_extreme:
        verdict = VERDICT_MULTIYEAR_EXTREME
        if net_pct is not None and net_pct >= _MULTIYEAR_HIGH_THRESHOLD:
            interpretation = (
                "Major institutional positioning extreme: net longs are stretched versus the "
                "last 3 years, not just the recent 13-week window."
            )
        else:
            interpretation = (
                "Major institutional positioning extreme: net shorts are stretched versus the "
                "last 3 years, not just the recent 13-week window."
            )
    elif (short_term_net_high or short_term_net_low) and net_pct is not None:
        verdict = VERDICT_SHORT_TERM_ONLY
        interpretation = (
            "Short-term strength only: positioning is extreme in the last 13 weeks but sits "
            f"mid-range ({net_pct:.0f}th percentile) over the last 3 years."
        )
    elif net_3y_extreme:
        verdict = VERDICT_MULTIYEAR_EXTREME
        interpretation = (
            "Net positioning is at a multi-year extreme even without a fresh 13-week signal."
        )
    elif net_pct is not None:
        verdict = VERDICT_MULTIYEAR_CONTEXT
        interpretation = interpret_metric(METRIC_NET, net_pct)

    return MultiYearConfluence(
        net_3y_percentile=net_pct,
        net_3y_class=net_class,
        long_3y_percentile=float(long_3y_percentile) if _is_finite(long_3y_percentile) else None,
        long_3y_class=long_class,
        short_3y_percentile=short_pct,
        short_3y_class=short_class,
        oi_3y_percentile=float(oi_3y_percentile) if _is_finite(oi_3y_percentile) else None,
        oi_3y_class=oi_class,
        multiyear_extreme=bool(net_3y_extreme),
        squeeze_risk=squeeze_risk,
        verdict=verdict,
        interpretation=interpretation,
    )

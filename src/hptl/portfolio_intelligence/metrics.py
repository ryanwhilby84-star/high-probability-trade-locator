"""Phase 3 portfolio mathematics — deterministic metrics only.

Formulas (revised)
------------------
Weights:
    w_i = r_i / Σ r_k   (equal weights if Σ r = 0)

Q = wᵀ C w
    where C_ii = 1 and C_ij = direction-adjusted correlation ρ_ij

N_eff_raw = 1 / max(Q, ε)
N_eff     = clamp(N_eff_raw, 1.0, n)   # n = populated trades

Diversification D = 0 if n ≤ 1 else 100 * clamp((N_eff - 1)/(n - 1), 0, 1)
Duplication     U = 100 - D

Exposure clusters: connected components where |ρ_ij| ≥ EXPOSURE_CLUSTER_ABS_THRESHOLD
"""

from __future__ import annotations

import math
from typing import Any

from hptl.portfolio_intelligence.config import (
    EXPOSURE_CLUSTER_ABS_THRESHOLD,
    PAIR_STRENGTH_BANDS,
    Q_EPSILON,
)


def classify_pair_strength(adjusted_correlation: float) -> dict[str, Any]:
    """Classify by |ρ|; note negative as negative relationship.

    Thresholds come from ``PAIR_STRENGTH_BANDS`` (config), not ad-hoc literals.
    """
    try:
        rho = float(adjusted_correlation)
    except (TypeError, ValueError):
        return {
            "strength": "Minimal",
            "abs_correlation": None,
            "sign": 0,
            "relationship": "unknown",
        }
    if not math.isfinite(rho):
        return {
            "strength": "Minimal",
            "abs_correlation": None,
            "sign": 0,
            "relationship": "unknown",
        }
    abs_r = abs(rho)
    # Bands listed high→low; first match whose lower bound is satisfied wins.
    strength = PAIR_STRENGTH_BANDS[-1][2]
    for lo, _hi, label in PAIR_STRENGTH_BANDS:
        if abs_r + 1e-15 >= lo:
            strength = label
            break
    return {
        "strength": strength,
        "abs_correlation": round(abs_r, 6),
        "sign": 0 if abs_r < 1e-15 else (1 if rho > 0 else -1),
        "relationship": "positive" if rho > 0 else ("negative" if rho < 0 else "neutral"),
    }


def risk_weights(risks: list[float]) -> list[float]:
    cleaned = []
    for r in risks:
        try:
            v = float(r)
        except (TypeError, ValueError):
            v = 0.0
        if not math.isfinite(v) or v < 0:
            v = 0.0
        cleaned.append(v)
    total = sum(cleaned)
    n = len(cleaned)
    if n == 0:
        return []
    if total <= 0:
        return [1.0 / n] * n
    return [v / total for v in cleaned]


def build_adjusted_matrix(
    n: int,
    pairs: list[dict[str, Any]],
    trade_keys: list[tuple[str, str]],
) -> list[list[float]]:
    """Square matrix C; keys are (instrument_id, direction) in basket order."""
    index = {k: i for i, k in enumerate(trade_keys)}
    c = [[0.0] * n for _ in range(n)]
    for i in range(n):
        c[i][i] = 1.0
    for p in pairs:
        a = (p["trade_a_instrument_id"], p["trade_a_direction"])
        b = (p["trade_b_instrument_id"], p["trade_b_direction"])
        if a not in index or b not in index:
            continue
        i, j = index[a], index[b]
        rho = float(p["direction_adjusted_correlation"])
        c[i][j] = rho
        c[j][i] = rho
    return c


def quadratic_form(weights: list[float], matrix: list[list[float]]) -> float:
    n = len(weights)
    q = 0.0
    for i in range(n):
        for j in range(n):
            q += weights[i] * weights[j] * matrix[i][j]
    return q


def effective_independent_trades(q: float, n: int) -> float:
    """N_eff = clamp(1/max(Q,ε), 1, n)."""
    if n <= 0:
        return 0.0
    if n == 1:
        return 1.0
    denom = max(float(q), Q_EPSILON)
    raw = 1.0 / denom
    return float(min(max(raw, 1.0), float(n)))


def diversification_score(n_eff: float, n: int) -> float:
    if n <= 1:
        return 0.0
    x = (n_eff - 1.0) / (n - 1.0)
    x = min(max(x, 0.0), 1.0)
    return 100.0 * x


def duplication_score(diversification: float) -> float:
    return 100.0 - float(diversification)


def exposure_clusters(
    trade_keys: list[tuple[str, str]],
    matrix: list[list[float]],
    risks: list[float],
    *,
    threshold: float = EXPOSURE_CLUSTER_ABS_THRESHOLD,
) -> list[dict[str, Any]]:
    """Connected components where |ρ_ij| ≥ threshold."""
    n = len(trade_keys)
    parent = list(range(n))

    def find(x: int) -> int:
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    def union(a: int, b: int) -> None:
        ra, rb = find(a), find(b)
        if ra != rb:
            parent[rb] = ra

    for i in range(n):
        for j in range(i + 1, n):
            if abs(matrix[i][j]) + 1e-15 >= threshold:
                union(i, j)

    groups: dict[int, list[int]] = {}
    for i in range(n):
        r = find(i)
        groups.setdefault(r, []).append(i)

    clusters: list[dict[str, Any]] = []
    for members in groups.values():
        members = sorted(members)
        risk_sum = sum(risks[i] for i in members)
        clusters.append(
            {
                "members": [
                    {
                        "instrument_id": trade_keys[i][0],
                        "direction": trade_keys[i][1],
                        "risk_percent": risks[i],
                    }
                    for i in members
                ],
                "size": len(members),
                "risk_percent_sum": round(risk_sum, 6),
            }
        )
    clusters.sort(key=lambda c: (-c["risk_percent_sum"], -c["size"]))
    return clusters


def pick_extreme_pairs(pairs: list[dict[str, Any]]) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
    """Highest / lowest by direction-adjusted correlation (signed)."""
    if not pairs:
        return None, None

    def adj(p: dict[str, Any]) -> float:
        return float(p["direction_adjusted_correlation"])

    highest = max(pairs, key=adj)
    lowest = min(pairs, key=adj)

    def pack(p: dict[str, Any]) -> dict[str, Any]:
        rho = float(p["direction_adjusted_correlation"])
        return {
            "trade_a_instrument_id": p["trade_a_instrument_id"],
            "trade_a_direction": p["trade_a_direction"],
            "trade_b_instrument_id": p["trade_b_instrument_id"],
            "trade_b_direction": p["trade_b_direction"],
            "direction_adjusted_correlation": rho,
            "raw_correlation": p.get("raw_correlation"),
            "classification": classify_pair_strength(rho),
        }

    return pack(highest), pack(lowest)


def compute_portfolio_intelligence(
    *,
    trades: list[dict[str, Any]],
    pairs: list[dict[str, Any]],
    exposure_cluster_threshold: float = EXPOSURE_CLUSTER_ABS_THRESHOLD,
) -> dict[str, Any]:
    """Compute Phase 3 metrics from a Phase 2A basket result fragment."""
    n = len(trades)
    if n == 0:
        return {
            "status": "empty",
            "trades_entered": 0,
            "effective_independent_trades": 0.0,
            "diversification_score": 0.0,
            "duplication_score": 0.0,
            "largest_exposure_cluster": None,
            "highest_correlated_pair": None,
            "lowest_correlated_pair": None,
            "total_planned_risk": 0.0,
            "largest_risk_concentration": 0.0,
            "exposure_clusters": [],
            "pair_classifications": [],
            "diagnostics": {},
        }

    trade_keys = [(t["instrument_id"], t["direction"]) for t in trades]
    risks = [float(t.get("risk_percent") or 0.0) for t in trades]
    weights = risk_weights(risks)
    matrix = build_adjusted_matrix(n, pairs, trade_keys)
    q = quadratic_form(weights, matrix)
    n_eff = effective_independent_trades(q, n)
    div = diversification_score(n_eff, n)
    dup = duplication_score(div)
    total_risk = sum(risks)
    clusters = exposure_clusters(
        trade_keys,
        matrix,
        risks,
        threshold=exposure_cluster_threshold,
    )
    largest_cluster = clusters[0] if clusters else None
    largest_conc = 0.0
    if largest_cluster and total_risk > 0:
        largest_conc = largest_cluster["risk_percent_sum"] / total_risk
    elif largest_cluster and n > 0:
        largest_conc = largest_cluster["size"] / n

    highest, lowest = pick_extreme_pairs(pairs)
    pair_classifications = []
    for p in pairs:
        rho = float(p["direction_adjusted_correlation"])
        pair_classifications.append(
            {
                "trade_a_instrument_id": p["trade_a_instrument_id"],
                "trade_a_direction": p["trade_a_direction"],
                "trade_b_instrument_id": p["trade_b_instrument_id"],
                "trade_b_direction": p["trade_b_direction"],
                "direction_adjusted_correlation": rho,
                "classification": classify_pair_strength(rho),
            }
        )

    return {
        "status": "ok",
        "trades_entered": n,
        "effective_independent_trades": round(n_eff, 1),
        "diversification_score": round(div, 1),
        "duplication_score": round(dup, 1),
        "largest_exposure_cluster": largest_cluster,
        "highest_correlated_pair": highest,
        "lowest_correlated_pair": lowest,
        "total_planned_risk": round(total_risk, 6),
        "largest_risk_concentration": round(largest_conc, 6),
        "exposure_clusters": clusters,
        "pair_classifications": pair_classifications,
        "config": {
            "exposure_cluster_abs_threshold": exposure_cluster_threshold,
        },
        "diagnostics": {
            "q": round(q, 12),
            "n_eff_before_clamp_display": round(n_eff, 6),
            "weights": [round(w, 8) for w in weights],
            "risk_percents": risks,
        },
    }

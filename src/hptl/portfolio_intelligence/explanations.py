"""Deterministic explanation strings — mathematics only, no advice."""

from __future__ import annotations

from typing import Any


def build_explanations(intel: dict[str, Any]) -> list[str]:
    lines: list[str] = []
    n = int(intel.get("trades_entered") or 0)
    if n <= 0:
        return ["No populated trades in the basket."]

    n_eff = float(intel.get("effective_independent_trades") or 0)
    div = float(intel.get("diversification_score") or 0)
    dup = float(intel.get("duplication_score") or 0)
    conc = float(intel.get("largest_risk_concentration") or 0)
    cluster = intel.get("largest_exposure_cluster") or {}
    cluster_size = int(cluster.get("size") or 0)

    if n == 1:
        lines.append("One populated trade — effective independent trades equals 1.0 by definition.")
    elif n_eff <= 1.15:
        lines.append(
            f"{n} proposed trades behave like roughly one independent risk bet "
            f"(effective independent trades = {n_eff:.1f})."
        )
    elif n_eff >= n - 0.15:
        lines.append(
            f"Effective independent trades ({n_eff:.1f}) are close to the number of "
            f"populated trades ({n}), indicating limited historical overlap."
        )
    else:
        lines.append(
            f"Effective independent trades = {n_eff:.1f} out of {n} populated trades."
        )

    if div >= 70:
        lines.append(f"Portfolio diversification is high (score {div:.1f}/100).")
    elif div >= 40:
        lines.append(f"Portfolio diversification is moderate (score {div:.1f}/100).")
    else:
        lines.append(f"Portfolio diversification is low (score {div:.1f}/100).")

    if dup >= 70:
        lines.append(
            f"Duplication is elevated (score {dup:.1f}/100) — proposed trades share substantial historical overlap."
        )
    elif dup <= 30:
        lines.append(f"Duplication is low (score {dup:.1f}/100).")

    if cluster_size >= 3:
        lines.append(
            f"Largest exposure cluster contains {cluster_size} trades linked by "
            f"|adjusted correlation| at or above the cluster threshold."
        )
    elif cluster_size == 2 and n >= 3:
        lines.append("Largest exposure cluster links two trades above the cluster threshold.")

    if conc >= 0.6 and n >= 2:
        lines.append(
            f"Most planned risk is concentrated within one exposure cluster "
            f"({conc * 100:.0f}% of total planned risk)."
        )

    hi = intel.get("highest_correlated_pair")
    lo = intel.get("lowest_correlated_pair")
    if hi:
        lines.append(
            "Highest correlated pair: "
            f"{hi['trade_a_instrument_id']} {hi['trade_a_direction']} × "
            f"{hi['trade_b_instrument_id']} {hi['trade_b_direction']} "
            f"(adjusted {hi['direction_adjusted_correlation']:+.2f})."
        )
    if lo and lo is not hi:
        lines.append(
            "Lowest correlated pair: "
            f"{lo['trade_a_instrument_id']} {lo['trade_a_direction']} × "
            f"{lo['trade_b_instrument_id']} {lo['trade_b_direction']} "
            f"(adjusted {lo['direction_adjusted_correlation']:+.2f})."
        )

    return lines

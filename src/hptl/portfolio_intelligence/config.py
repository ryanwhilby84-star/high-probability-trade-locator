"""Phase 3 portfolio intelligence — application constants.

Grouping threshold is configurable; default 0.60 (High band).
"""

from __future__ import annotations

# Absolute |direction-adjusted correlation| required to link trades into an
# exposure cluster (connected component). Future UI may expose this.
EXPOSURE_CLUSTER_ABS_THRESHOLD: float = 0.60

# Numerical floor for Q = wᵀ C w before inversion.
Q_EPSILON: float = 1e-12

# Pair strength bands on |ρ_adj|
PAIR_STRENGTH_BANDS: tuple[tuple[float, float, str], ...] = (
    (0.80, 1.00, "Very High"),
    (0.60, 0.79, "High"),
    (0.40, 0.59, "Moderate"),
    (0.20, 0.39, "Low"),
    (0.00, 0.19, "Minimal"),
)

ENGINE_VERSION = "portfolio_intelligence_v3"

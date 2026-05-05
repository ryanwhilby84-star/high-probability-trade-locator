from __future__ import annotations

import pandas as pd


def build_ratio_context(price_df: pd.DataFrame | None = None, rates_df: pd.DataFrame | None = None) -> pd.DataFrame:
    """Placeholder for future price-to-rates / market-to-rates context.

    Future purpose:
    - compare watched futures instruments against rates/macro regime;
    - flag overbought/underbought relative to rates;
    - detect divergence between price behaviour and rates context;
    - confirm when price behaviour agrees with rates regime.

    This function intentionally returns no ratio signals unless actual futures
    price inputs are supplied by a future price-data layer.
    """
    if price_df is None or price_df.empty:
        return pd.DataFrame(
            columns=[
                "market_name",
                "ratio_context_available",
                "ratio_context_summary",
            ]
        )

    # TODO: integrate futures price collector before calculating real ratios.
    raise NotImplementedError("Ratio context requires futures price inputs; no fake ratio data is produced.")

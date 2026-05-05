from __future__ import annotations

import pandas as pd


def build_news_risk_context(events_df: pd.DataFrame | None = None) -> pd.DataFrame:
    """Placeholder for future red-folder news/event-risk filtering.

    Future purpose:
    - flag major scheduled macro events;
    - mark whether event risk supports, conflicts with, or invalidates a setup;
    - caution around CPI, FOMC, NFP, rate decisions, and other red-folder events.

    No live scraping and no fake news data are used here. This placeholder does
    not affect macro_score.
    """
    if events_df is None or events_df.empty:
        return pd.DataFrame(
            {
                "event_risk_available": [False],
                "event_risk_summary": ["Future red-folder event-risk integration placeholder; no event data loaded."],
            }
        )

    # TODO: integrate scheduled event/calendar source before using event risk.
    raise NotImplementedError("News/event risk requires a real event calendar source.")

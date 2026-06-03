"""Macro + news + sentiment *intelligence* (context only, not a trading bot).

Layers:
  3. Economic calendar (``calendar_*``)
  4. Narratives / GDELT (``gdelt_downloader``, ``headline_classifier``, ``narrative_engine``)
  5. Sentiment interference (``sentiment_interference``)

Human execution stays outside this package; nothing here emits trade signals.
"""

from hptl.news.contracts import (
    CalendarEventRecord,
    NarrativeSnapshot,
    SentimentInterferenceLevel,
    SentimentInterferenceReport,
)

__all__ = [
    "CalendarEventRecord",
    "NarrativeSnapshot",
    "SentimentInterferenceLevel",
    "SentimentInterferenceReport",
]

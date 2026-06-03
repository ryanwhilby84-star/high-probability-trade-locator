"""HPTL Thesis Tracker — persistent, evolving trade theses (planning only).

Converts one-week radar alerts into multi-week theses with weekly snapshots,
a composite conviction trend, and an evolution log. Read-only with respect to
COT / macro / confluence / valuation / seasonality / radar logic: this package
only *reads* already-exported numbers and stores its own narrative state.
"""

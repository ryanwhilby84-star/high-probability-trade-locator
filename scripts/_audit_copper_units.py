import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from hptl.prices.canonical_timeline import load_canonical_timeline
from hptl.seasonality.seasonality_price_bars import weekly_closes_for_instrument

tl = load_canonical_timeline("Copper / HG")
daily = list(tl.daily_closes())
print("canonical_source", tl.canonical_source, "symbol", tl.canonical_symbol)
print("date_start", tl.date_start, "date_end", tl.date_end, "n_daily", len(daily))

small = [(d, c) for d, c in daily if c < 50]
large = [(d, c) for d, c in daily if c >= 50]
if small:
    print("daily small (<50):", len(small), f"range {min(c for _, c in small):.4f}-{max(c for _, c in small):.4f}")
if large:
    print("daily large (>=50):", len(large), f"range {min(c for _, c in large):.2f}-{max(c for _, c in large):.2f}")

prev = None
transitions = []
for d, c in daily:
    if prev and prev[1] > 0 and (c / prev[1] > 10 or prev[1] / c > 10):
        transitions.append((prev[0], prev[1], d, c))
    prev = (d, c)
print("daily >10x jumps:", len(transitions))
for t in transitions[:10]:
    print(f"  {t[0]} {t[1]:.4f} -> {t[2]} {t[3]:.4f} ({t[3]/t[1]:.1f}x)")

bars, bar_source, _ = weekly_closes_for_instrument("Copper / HG")
print("bar_source", bar_source)
print("weekly spikes >1000:")
for d, c in bars:
    if c > 1000:
        print(f"  {d}  {c:,.4f}")

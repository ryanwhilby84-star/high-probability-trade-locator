"""Independent numerical checks for rolling COT percentile calculations."""
import unittest
from hptl.cot.weekly_inspector_flow import _rolling_net_percentiles

class CotPercentileChecks(unittest.TestCase):
    def test_no_lookahead(self):
        history = [float(i % 19) for i in range(160)]
        self.assertEqual(_rolling_net_percentiles(history), _rolling_net_percentiles(history + [9999.0])[:160])

    def test_rolling_window_156(self):
        history = [9999.0] + [float(i) for i in range(156)]
        actual = _rolling_net_percentiles(history)[-1]
        window = history[-156:]
        current = history[-1]
        reference = round(100 * (sum(x < current for x in window) + 0.5 * sum(x == current for x in window)) / len(window), 2)
        self.assertEqual(actual, reference)

    def test_missing_not_zero(self):
        values = _rolling_net_percentiles([None, -10.0, None, 10.0])
        self.assertIsNone(values[0])
        self.assertIsNone(values[2])
        self.assertEqual(values[-1], 75.0)

if __name__ == "__main__":
    unittest.main()

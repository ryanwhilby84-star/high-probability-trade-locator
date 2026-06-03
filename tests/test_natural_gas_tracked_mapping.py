"""Henry Hub NYMEX (CFTC 023651) must map to Natural Gas / NG in the confluence merge."""

from __future__ import annotations

import unittest

from hptl.confluence.build_decision_table import _map_market


class TestNaturalGasTrackedMapping(unittest.TestCase):
    def test_map_market_nymex_nat_gas_nyme_label(self):
        self.assertEqual(_map_market("NAT GAS NYME - NEW YORK MERCANTILE EXCHANGE"), "Natural Gas / NG")

    def test_map_market_legacy_natural_gas_nymex_label(self):
        self.assertEqual(_map_market("NATURAL GAS - NEW YORK MERCANTILE EXCHANGE"), "Natural Gas / NG")

    def test_map_market_does_not_match_ice_ld1_strip(self):
        self.assertIsNone(_map_market("NAT GAS ICE LD1 - ICE FUTURES ENERGY DIV"))


if __name__ == "__main__":
    unittest.main()

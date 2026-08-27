from __future__ import annotations

from hptl.valuation.gold_institutional_fair_value_v2 import (
    write_gold_institutional_fair_value_v2,
)


if __name__ == "__main__":
    latest = write_gold_institutional_fair_value_v2()
    print(latest)
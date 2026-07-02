"""CFTC Traders in Financial Futures — macro positioning contracts (TFF)."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Final

# CFTC contract market codes verified against fut_fin_txt annual files.
TFF_CFTC_DXY = "098662"
TFF_CFTC_ZT = "042601"  # 2-Year T-Note
TFF_CFTC_ZF = "044601"  # 5-Year T-Note
TFF_CFTC_ZN = "043602"  # 10-Year T-Note
TFF_CFTC_TN = "043607"  # Ultra 10-Year T-Note
TFF_CFTC_ZB = "020601"  # 30-Year T-Bond

TFF_MACRO_CODE_TO_INSTRUMENT: Final[dict[str, str]] = {
    TFF_CFTC_DXY: "US Dollar Index / DX",
    TFF_CFTC_ZT: "US 2-Year T-Note / ZT",
    TFF_CFTC_ZF: "US 5-Year T-Note / ZF",
    TFF_CFTC_ZN: "US 10-Year T-Note / ZN",
    TFF_CFTC_TN: "Ultra 10-Year T-Note / TN",
    TFF_CFTC_ZB: "US 30-Year T-Bond / ZB",
}

TFF_MACRO_SYMBOLS: Final[dict[str, str]] = {
    "US Dollar Index / DX": "DXY",
    "US 2-Year T-Note / ZT": "ZT",
    "US 5-Year T-Note / ZF": "ZF",
    "US 10-Year T-Note / ZN": "ZN",
    "Ultra 10-Year T-Note / TN": "TN",
    "US 30-Year T-Bond / ZB": "ZB",
}

TREASURY_TFF_INSTRUMENTS: Final[tuple[str, ...]] = (
    "US 2-Year T-Note / ZT",
    "US 5-Year T-Note / ZF",
    "US 10-Year T-Note / ZN",
    "US 30-Year T-Bond / ZB",
)

TREASURY_TFF_CODES: Final[dict[str, str]] = {
    "US 2-Year T-Note / ZT": TFF_CFTC_ZT,
    "US 5-Year T-Note / ZF": TFF_CFTC_ZF,
    "US 10-Year T-Note / ZN": TFF_CFTC_ZN,
    "US 30-Year T-Bond / ZB": TFF_CFTC_ZB,
}

WEEKS_HISTORY = 13
WEEKS_PERCENTILE = 13


@dataclass(frozen=True)
class TffMacroContractSpec:
    instrument_id: str
    symbol: str
    cftc_code: str
    label: str
    asset_class: str
    subgroup: str


TFF_MACRO_CONTRACTS: Final[tuple[TffMacroContractSpec, ...]] = (
    TffMacroContractSpec(
        "US Dollar Index / DX",
        "DXY",
        TFF_CFTC_DXY,
        "US Dollar Index",
        "fx",
        "usd_index",
    ),
    TffMacroContractSpec(
        "US 2-Year T-Note / ZT",
        "ZT",
        TFF_CFTC_ZT,
        "2-Year Treasury Note",
        "macro",
        "us_treasury_futures",
    ),
    TffMacroContractSpec(
        "US 5-Year T-Note / ZF",
        "ZF",
        TFF_CFTC_ZF,
        "5-Year Treasury Note",
        "macro",
        "us_treasury_futures",
    ),
    TffMacroContractSpec(
        "US 10-Year T-Note / ZN",
        "ZN",
        TFF_CFTC_ZN,
        "10-Year Treasury Note",
        "macro",
        "us_treasury_futures",
    ),
    TffMacroContractSpec(
        "Ultra 10-Year T-Note / TN",
        "TN",
        TFF_CFTC_TN,
        "Ultra 10-Year Treasury Note",
        "macro",
        "us_treasury_futures",
    ),
    TffMacroContractSpec(
        "US 30-Year T-Bond / ZB",
        "ZB",
        TFF_CFTC_ZB,
        "30-Year Treasury Bond",
        "macro",
        "us_treasury_futures",
    ),
)

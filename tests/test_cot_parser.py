from __future__ import annotations

import pandas as pd

from hptl.cot.parser import (
    clean_column_name,
    clean_columns,
    deduplicate_market_weeks,
    filter_cme_index_markets,
    filter_cme_index_history,
    filter_good_workbook_markets,
    normalise_cot_dataframe,
    parse_cme_futures_only_text,
)


def test_clean_column_name_converts_to_snake_case() -> None:
    assert clean_column_name("Market and Exchange Names") == "market_and_exchange_names"
    assert clean_column_name("% of Open Interest") == "pct_of_open_interest"


def test_clean_columns_returns_copy_with_snake_case_columns() -> None:
    df = pd.DataFrame({"Report Date As YYYY-MM-DD": ["2026-01-01"]})
    cleaned = clean_columns(df)
    assert list(cleaned.columns) == ["report_date_as_yyyy_mm_dd"]
    assert list(df.columns) == ["Report Date As YYYY-MM-DD"]


def test_normalise_cot_dataframe_adds_market_and_report_date() -> None:
    df = pd.DataFrame(
        {
            "Market and Exchange Names": ["GOLD - COMMODITY EXCHANGE INC."],
            "Report Date As YYYY-MM-DD": ["2026-01-06"],
            "Open Interest All": [1000],
        }
    )
    cleaned = normalise_cot_dataframe(df)
    assert cleaned.loc[0, "market_name_clean"] == "GOLD - COMMODITY EXCHANGE INC."
    assert str(cleaned.loc[0, "report_date"].date()) == "2026-01-06"


def test_parse_cme_futures_only_text_maps_index_contracts() -> None:
    sample = """
NASDAQ MINI - CHICAGO MERCANTILE EXCHANGE                            Code-209742
    FUTURES ONLY POSITIONS AS OF 04/28/26                         |
(CONTRACTS OF $20 X NASDAQ 100 INDEX)               OPEN INTEREST:      313,141
COMMITMENTS
  33,679   29,147    9,000  240,000  247,000  282,679  285,147   30,462   27,994
CHANGES FROM 04/21/26 (CHANGE IN OPEN INTEREST:      2,000)
   1,200     -300      100    2,000    1,000    3,300      800     -100     200

E-MINI S&P 500 - CHICAGO MERCANTILE EXCHANGE                         Code-13874A
    FUTURES ONLY POSITIONS AS OF 04/28/26                         |
(CONTRACTS OF $50 X S&P 500 STOCK INDEX)             OPEN INTEREST:    1,889,421
COMMITMENTS
 500,000  600,000   10,000  900,000  750,000 1410000 1360000 479421 529421
CHANGES FROM 04/21/26 (CHANGE IN OPEN INTEREST:      3,000)
  10,000    5,000        0   -2,000    2,000    8,000    7,000  -5,000 -4,000
"""
    parsed = parse_cme_futures_only_text(sample)
    filtered = filter_cme_index_markets(parsed)

    assert set(filtered["dashboard_market"]) == {"NASDAQ", "S&P 500"}
    nasdaq = filtered[filtered["dashboard_market"] == "NASDAQ"].iloc[0]
    assert nasdaq["open_interest"] == 313141
    assert nasdaq["noncommercial_net"] == 4532
    assert nasdaq["weekly_change"] == 1500
    assert nasdaq["bias"] == "BULLISH"
    assert pd.isna(nasdaq["four_week_change"])


def test_filter_cme_index_history_backfills_multiple_financial_weeks_and_rejects_ice() -> None:
    sample = pd.DataFrame(
        {
            "Market and Exchange Names": [
                "E-MINI NASDAQ 100 - CHICAGO MERCANTILE EXCHANGE",
                "NASDAQ MINI - CHICAGO MERCANTILE EXCHANGE",
                "E-MINI S&P 500 - CHICAGO MERCANTILE EXCHANGE",
                "CAISO SP-15 - ICE FUTURES ENERGY DIV",
                "CALIF CARBON ALL VINTAGE 2025 - ICE FUTURES ENERGY DIV",
            ],
            "Report Date as YYYY-MM-DD": ["2026-01-06", "2026-01-13", "2026-01-06", "2026-01-06", "2026-01-13"],
            "CFTC Contract Market Code": ["209742", "209742", "13874A", "209742", "13874A"],
            "Open Interest All": [1000, 1100, 2000, 3000, 4000],
            "Lev Money Positions-Long All": [600, 700, 800, 100, 200],
            "Lev Money Positions-Short All": [400, 300, 900, 200, 300],
            "Dealer Positions-Long All": [100, 110, 500, 10, 20],
            "Dealer Positions-Short All": [150, 160, 450, 20, 30],
            "Asset Mgr Positions-Long All": [150, 150, 500, 30, 40],
            "Asset Mgr Positions-Short All": [150, 150, 500, 40, 50],
            "Change in Lev Money Long All": [10, 20, 30, 40, 50],
            "Change in Lev Money Short All": [5, 15, 50, 60, 70],
        }
    )

    filtered = filter_cme_index_history(sample)

    assert len(filtered) == 3
    assert set(filtered["market_name"]) == {"NASDAQ", "S&P 500"}
    assert list(filtered[filtered["market_name"] == "NASDAQ"]["report_date"].dt.strftime("%Y-%m-%d")) == [
        "2026-01-06",
        "2026-01-13",
    ]
    assert filtered.loc[filtered["market_name"] == "S&P 500", "noncommercial_net"].iloc[0] == -100
    assert not filtered["market_name"].astype(str).str.contains("CAISO|CARBON", case=False).any()


def test_filter_good_workbook_markets_rejects_irrelevant_energy() -> None:
    sample = pd.DataFrame(
        {
            "market_name": ["GOLD - COMMODITY EXCHANGE INC.", "AECO FIN BASIS - ICE FUTURES ENERGY DIV", "CAISO SP-15 - ICE FUTURES ENERGY DIV", "NASDAQ"],
            "report_date": ["2026-01-06"] * 4,
            "commercial_net": [1, 2, 3, 4],
        }
    )

    filtered = filter_good_workbook_markets(sample)

    assert list(filtered["market_name"]) == ["GOLD", "NASDAQ"]
    assert not filtered["market_name"].astype(str).str.contains("AECO|CAISO", case=False).any()


def test_deduplicate_market_weeks_keeps_one_populated_index_row() -> None:
    sample = pd.DataFrame(
        {
            "market_name": ["NASDAQ", "NASDAQ"],
            "report_date": ["2026-01-06", "2026-01-06"],
            "commercial_net": [pd.NA, 100],
            "noncommercial_net": [pd.NA, -50],
        }
    )

    deduped = deduplicate_market_weeks(sample)

    assert len(deduped) == 1
    assert deduped.iloc[0]["commercial_net"] == 100


def test_disaggregated_history_maps_managed_money_and_producer_merchant_fields():
    import pandas as pd
    from hptl.cot.parser import cot_history_to_dashboard_rows, filter_good_workbook_markets

    sample = pd.DataFrame(
        {
            "Report_Date_as_YYYY-MM-DD": ["2026-01-06"],
            "Market_and_Exchange_Names": ["COCOA - ICE FUTURES U.S."],
            "Open_Interest_All": [127271],
            "Prod_Merc_Positions_Long_All": [36956],
            "Prod_Merc_Positions_Short_All": [47126],
            "M_Money_Positions_Long_All": [27319],
            "M_Money_Positions_Short_All": [27178],
            "Change_in_Prod_Merc_Long_All": [100],
            "Change_in_Prod_Merc_Short_All": [25],
        }
    )

    rows = cot_history_to_dashboard_rows(sample, source_report="disaggregated_futures_only")
    rows = filter_good_workbook_markets(rows)

    assert len(rows) == 1
    assert rows.iloc[0]["market_name"] == "COCOA"
    assert rows.iloc[0]["commercial_long"] == 36956
    assert rows.iloc[0]["commercial_short"] == 47126
    assert rows.iloc[0]["commercial_net"] == -10170
    assert rows.iloc[0]["noncommercial_long"] == 27319
    assert rows.iloc[0]["noncommercial_short"] == 27178
    assert rows.iloc[0]["noncommercial_net"] == 141
    assert rows.iloc[0]["weekly_change"] == 75


def test_good_workbook_market_order_is_exact_required_set():
    from hptl.cot.contracts import GOOD_WORKBOOK_MARKET_ORDER

    assert GOOD_WORKBOOK_MARKET_ORDER == [
        "NASDAQ",
        "S&P 500",
        "GOLD",
        "SILVER",
        "COPPER",
        "CRUDE OIL",
        "NATURAL GAS",
        "COFFEE",
        "COCOA",
        "CORN",
        "WHEAT",
        "SOYBEANS",
    ]


def test_filter_good_workbook_markets_keeps_required_split_commodity_names():
    sample = pd.DataFrame(
        {
            "market_name": [
                "COPPER- #1",
                "WTI FINANCIAL CRUDE OIL",
                "NAT GAS NYME",
                "COFFEE C",
                "COCOA",
                "WHEAT-SRW",
                "AECO FIN BASIS",
            ],
            "exchange": [
                "COMMODITY EXCHANGE INC.",
                "NEW YORK MERCANTILE EXCHANGE",
                "NEW YORK MERCANTILE EXCHANGE",
                "ICE FUTURES U.S.",
                "ICE FUTURES U.S.",
                "CHICAGO BOARD OF TRADE",
                "ICE FUTURES ENERGY DIV",
            ],
            "report_date": ["2026-01-06"] * 7,
            "commercial_long": [1, 2, 3, 4, 5, 6, 7],
            "commercial_short": [0, 0, 0, 0, 0, 0, 0],
            "commercial_net": [1, 2, 3, 4, 5, 6, 7],
        }
    )

    filtered = filter_good_workbook_markets(sample)

    assert list(filtered["market_name"]) == [
        "COPPER",
        "CRUDE OIL",
        "NATURAL GAS",
        "COFFEE",
        "COCOA",
        "WHEAT",
    ]
    assert "AECO FIN BASIS" not in set(filtered["market_name"])


def test_disaggregated_history_keeps_exact_required_commodity_set_when_present():
    from hptl.cot.parser import cot_history_to_dashboard_rows, filter_good_workbook_markets

    source_names = [
        "GOLD - COMMODITY EXCHANGE INC.",
        "SILVER - COMMODITY EXCHANGE INC.",
        "COPPER- #1 - COMMODITY EXCHANGE INC.",
        "WTI FINANCIAL CRUDE OIL - NEW YORK MERCANTILE EXCHANGE",
        "NAT GAS NYME - NEW YORK MERCANTILE EXCHANGE",
        "COFFEE C - ICE FUTURES U.S.",
        "COCOA - ICE FUTURES U.S.",
        "CORN - CHICAGO BOARD OF TRADE",
        "WHEAT-SRW - CHICAGO BOARD OF TRADE",
        "SOYBEANS - CHICAGO BOARD OF TRADE",
        "CAISO SP-15 - ICE FUTURES ENERGY DIV",
    ]
    sample = pd.DataFrame(
        {
            "Report_Date_as_YYYY-MM-DD": ["2026-01-06"] * len(source_names),
            "Market_and_Exchange_Names": source_names,
            "Open_Interest_All": [1000] * len(source_names),
            "Prod_Merc_Positions_Long_All": list(range(100, 100 + len(source_names))),
            "Prod_Merc_Positions_Short_All": [50] * len(source_names),
            "M_Money_Positions_Long_All": [80] * len(source_names),
            "M_Money_Positions_Short_All": [40] * len(source_names),
        }
    )

    rows = filter_good_workbook_markets(cot_history_to_dashboard_rows(sample))

    assert list(rows["market_name"]) == [
        "GOLD",
        "SILVER",
        "COPPER",
        "CRUDE OIL",
        "NATURAL GAS",
        "COFFEE",
        "COCOA",
        "CORN",
        "WHEAT",
        "SOYBEANS",
    ]
    assert not rows["market_name"].astype(str).str.contains("CAISO|AECO|CARBON|ELECTRIC", case=False).any()



def test_trader_master_calculates_changes_bias_and_exact_markets():
    from hptl.cot.contracts import GOOD_WORKBOOK_MARKET_ORDER
    from hptl.cot.exporter import _calculate_trader_master

    rows = []
    for market in GOOD_WORKBOOK_MARKET_ORDER:
        for i, d in enumerate(["2026-01-06", "2026-01-13", "2026-01-20", "2026-01-27", "2026-02-03"]):
            rows.append(
                {
                    "report_date": d,
                    "market_name": market,
                    "open_interest": 1000 + i,
                    "commercial_long": 100 + i * 10,
                    "commercial_short": 50 + i,
                    "noncommercial_long": 80 + i * 5,
                    "noncommercial_short": 40 + i,
                    # Deliberately wrong stale values; master must recalculate.
                    "commercial_net": 999999,
                    "weekly_change": -999999,
                    "bias": "HARDCODED",
                }
            )
        rows.append({"report_date": "2026-01-06", "market_name": "CAISO SP-15", "commercial_long": 1})

    master, warnings = _calculate_trader_master(pd.DataFrame(rows))

    assert list(master["market_name"].drop_duplicates()) == GOOD_WORKBOOK_MARKET_ORDER
    assert "CAISO SP-15" not in set(master["market_name"])
    sample = master[(master["market_name"] == "GOLD")].reset_index(drop=True)
    assert sample.loc[0, "commercial_net"] == 50
    assert pd.isna(sample.loc[0, "weekly_change"])
    assert sample.loc[1, "weekly_change"] == 9
    assert sample.loc[4, "four_week_change"] == 36
    assert sample.loc[1, "mm_weekly_change"] == 4
    assert sample.loc[1, "bias"] == "Bullish"
    assert "HARDCODED" not in set(master["bias"])
    assert warnings == []


def test_data_checks_reports_all_required_markets():
    from hptl.cot.contracts import GOOD_WORKBOOK_DISPLAY_NAMES, GOOD_WORKBOOK_MARKET_ORDER
    from hptl.cot.exporter import _calculate_trader_master, _prepare_data_checks

    sample = pd.DataFrame(
        [
            {
                "report_date": "2026-01-06",
                "market_name": market,
                "commercial_long": 100,
                "commercial_short": 50,
                "noncommercial_long": 80,
                "noncommercial_short": 40,
            }
            for market in GOOD_WORKBOOK_MARKET_ORDER
        ]
    )
    master, _ = _calculate_trader_master(sample)
    checks = _prepare_data_checks(master)

    assert list(checks["market_name"]) == [GOOD_WORKBOOK_DISPLAY_NAMES[m] for m in GOOD_WORKBOOK_MARKET_ORDER]
    assert checks["row_count"].tolist() == [1] * len(GOOD_WORKBOOK_MARKET_ORDER)
    assert list(checks.columns) == [
        "market_name",
        "source_report_type",
        "primary_long_column_used",
        "primary_short_column_used",
        "primary_net_column_used",
        "commercial_net_column_used",
        "row_count",
        "first_date",
        "last_date",
    ]
    assert set(checks["primary_net_column_used"]) == {"noncommercial_net"}


def test_trader_master_sorts_and_calculates_within_same_market_after_deduplication():
    from hptl.cot.exporter import _calculate_trader_master

    sample = pd.DataFrame(
        [
            # Deliberately scrambled dates and interleaved markets.
            {"report_date": "2026-01-20", "market_name": "GOLD", "commercial_long": 130, "commercial_short": 50, "noncommercial_long": 90, "noncommercial_short": 40},
            {"report_date": "2026-01-06", "market_name": "SILVER", "commercial_long": 200, "commercial_short": 150, "noncommercial_long": 100, "noncommercial_short": 60},
            {"report_date": "2026-01-06", "market_name": "GOLD", "commercial_long": 100, "commercial_short": 50, "noncommercial_long": 80, "noncommercial_short": 40},
            {"report_date": "2026-01-13", "market_name": "GOLD", "commercial_long": 115, "commercial_short": 50, "noncommercial_long": 85, "noncommercial_short": 40},
            # Duplicate GOLD/date with missing values must lose to populated row above.
            {"report_date": "2026-01-13", "market_name": "GOLD", "commercial_long": pd.NA, "commercial_short": pd.NA, "noncommercial_long": pd.NA, "noncommercial_short": pd.NA},
        ]
    )

    master, warnings = _calculate_trader_master(sample)
    gold = master[master["market_name"] == "GOLD"].reset_index(drop=True)

    assert gold["report_date"].dt.strftime("%Y-%m-%d").tolist() == ["2026-01-06", "2026-01-13", "2026-01-20"]
    assert gold["commercial_net"].tolist() == [50, 65, 80]
    assert pd.isna(gold.loc[0, "weekly_change"])
    assert gold.loc[1, "weekly_change"] == 15
    assert gold.loc[2, "weekly_change"] == 15
    assert gold.loc[1, "bias"] == "Bullish"
    assert len(gold) == 3
    assert any("duplicate market/date" in warning for warning in warnings)


def test_data_checks_uses_exact_primary_signal_fields():
    from hptl.cot.exporter import _calculate_trader_master, _prepare_data_checks

    sample = pd.DataFrame(
        [
            {"report_date": "2026-01-13", "market_name": "NASDAQ", "commercial_long": 120, "commercial_short": 100, "noncommercial_long": 90, "noncommercial_short": 50, "source_report": "Financial Futures Only Historical", "primary_long_column_used": "lev_money_positions_long_all", "primary_short_column_used": "lev_money_positions_short_all"},
            {"report_date": "2026-01-06", "market_name": "NASDAQ", "commercial_long": 110, "commercial_short": 100, "noncommercial_long": 80, "noncommercial_short": 50, "source_report": "Financial Futures Only Historical", "primary_long_column_used": "lev_money_positions_long_all", "primary_short_column_used": "lev_money_positions_short_all"},
        ]
    )
    master, _ = _calculate_trader_master(sample)
    checks = _prepare_data_checks(master)
    nasdaq = checks[checks["market_name"] == "NASDAQ"].iloc[0]

    assert list(checks.columns) == [
        "market_name",
        "source_report_type",
        "primary_long_column_used",
        "primary_short_column_used",
        "primary_net_column_used",
        "commercial_net_column_used",
        "row_count",
        "first_date",
        "last_date",
    ]
    assert nasdaq["primary_long_column_used"] == "lev_money_positions_long_all"
    assert nasdaq["primary_short_column_used"] == "lev_money_positions_short_all"
    assert nasdaq["primary_net_column_used"] == "noncommercial_net"



def test_cot_scoring_engine_calculates_managed_money_led_columns():
    from hptl.cot.exporter import _calculate_trader_master

    sample = pd.DataFrame(
        [
            # Managed money improves for multiple weeks and becomes net long.
            # Commercials are supportive context but are not the primary signal.
            {"report_date": "2026-01-06", "market_name": "GOLD", "commercial_long": 100, "commercial_short": 100, "noncommercial_long": 100, "noncommercial_short": 110},
            {"report_date": "2026-01-13", "market_name": "GOLD", "commercial_long": 105, "commercial_short": 100, "noncommercial_long": 105, "noncommercial_short": 105},
            {"report_date": "2026-01-20", "market_name": "GOLD", "commercial_long": 110, "commercial_short": 100, "noncommercial_long": 120, "noncommercial_short": 100},
            {"report_date": "2026-01-27", "market_name": "GOLD", "commercial_long": 115, "commercial_short": 100, "noncommercial_long": 140, "noncommercial_short": 95},
            {"report_date": "2026-02-03", "market_name": "GOLD", "commercial_long": 120, "commercial_short": 100, "noncommercial_long": 160, "noncommercial_short": 90},
        ]
    )

    master, _ = _calculate_trader_master(sample)
    latest = master[master["market_name"] == "GOLD"].iloc[-1]

    assert latest["cot_bias"] == "Bullish"
    assert latest["cot_score"] == 10
    assert latest["cot_strength"] == "Very Strong"
    assert "Managed money" in latest["cot_summary"]
    assert not latest["cot_summary"].startswith("Commercial")


def test_cot_scoring_columns_flow_into_trader_report_and_dashboard():
    from hptl.cot.exporter import _calculate_trader_master, _prepare_dashboard_table, _prepare_trader_report

    sample = pd.DataFrame(
        [
            {"report_date": "2026-01-06", "market_name": "NASDAQ", "commercial_long": 100, "commercial_short": 100, "noncommercial_long": 120, "noncommercial_short": 90},
            {"report_date": "2026-01-13", "market_name": "NASDAQ", "commercial_long": 90, "commercial_short": 100, "noncommercial_long": 110, "noncommercial_short": 95},
        ]
    )

    master, _ = _calculate_trader_master(sample)
    trader = _prepare_trader_report(master)
    dashboard = _prepare_dashboard_table(master.groupby("market_name", as_index=False).tail(1))

    for column in ["cot_bias", "cot_score", "cot_strength", "cot_summary"]:
        assert column in dashboard.columns
        assert column in trader.columns

    latest = trader.iloc[-1]
    assert latest["cot_bias"] == "Bearish"
    assert latest["cot_score"] >= 2
    assert latest["cot_strength"] in {"Weak", "Moderate", "Strong", "Very Strong"}
    assert latest["cot_summary"].startswith("Managed money")


def test_managed_money_net_position_can_score_without_commercial_bias():
    from hptl.cot.exporter import _calculate_trader_master

    sample = pd.DataFrame(
        [
            {"report_date": "2026-01-06", "market_name": "GOLD", "commercial_long": 100, "commercial_short": 90, "noncommercial_long": 100, "noncommercial_short": 80},
            {"report_date": "2026-01-13", "market_name": "GOLD", "commercial_long": 100, "commercial_short": 90, "noncommercial_long": 100, "noncommercial_short": 80},
        ]
    )

    master, _ = _calculate_trader_master(sample)
    latest = master[master["market_name"] == "GOLD"].iloc[-1]

    assert latest["weekly_change"] == 0
    assert latest["mm_weekly_change"] == 0
    assert latest["cot_bias"] == "Bullish"
    assert latest["cot_score"] == 2  # net long contributes to bullish, but no momentum confirms it
    assert latest["cot_strength"] == "Weak"
    assert float(latest["cot_score"]).is_integer()

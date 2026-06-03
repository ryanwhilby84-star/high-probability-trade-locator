"""One-off NQ COT audit for 2026-05-26 — read-only investigation."""
from __future__ import annotations

import io
import zipfile
from pathlib import Path

import pandas as pd

DATE = "2026-05-26"
ROOT = Path(__file__).resolve().parents[1]
PROCESSED = ROOT / "data" / "processed"
RAW = ROOT / "data" / "raw"


def _code_str(raw) -> str:
    s = str(raw).strip().upper()
    if s.endswith(".0") and s[:-2].isdigit():
        s = s[:-2]
    return s


def scan_financial():
    fp = PROCESSED / "cot_financial_index_2026.csv"
    if not fp.exists():
        print("MISSING", fp)
        return
    df = pd.read_csv(fp, low_memory=False)
    df["rd"] = pd.to_datetime(df["report_date_as_yyyy_mm_dd"], errors="coerce")
    t = df[df["rd"] == DATE]
    nas = t[
        t["market_and_exchange_names"].astype(str).str.contains(
            "NASDAQ|NAS 100|E-MINI NASDAQ|NQ", case=False, na=False
        )
    ]
    print("=" * 72)
    print(f"SOURCE: {fp.name} (Traders in Financial Futures / TFF)")
    print(f"DATE: {DATE} — {len(nas)} NASDAQ-related rows")
    print("=" * 72)
    cols = [
        "cftc_contract_market_code",
        "lev_money_positions_long_all",
        "lev_money_positions_short_all",
        "asset_mgr_positions_long_all",
        "asset_mgr_positions_short_all",
        "dealer_positions_long_all",
        "dealer_positions_short_all",
        "nonrept_positions_long_all",
        "nonrept_positions_short_all",
        "open_interest_all",
    ]
    for idx, r in nas.iterrows():
        print(f"\nrow_index={idx}")
        print(f"  market: {r['market_and_exchange_names']}")
        print(f"  code: {_code_str(r.get('cftc_contract_market_code'))}")
        for c in cols:
            if c in df.columns:
                v = r.get(c)
                if pd.notna(v):
                    print(f"  {c}: {v}")
        if _code_str(r.get("cftc_contract_market_code")) == "209742":
            lev_l = r.get("lev_money_positions_long_all")
            lev_s = r.get("lev_money_positions_short_all")
            if pd.notna(lev_l) and pd.notna(lev_s):
                print(f"  >>> HPTL uses lev_money net: {float(lev_l) - float(lev_s)}")


def scan_disaggregated():
    for fp in sorted(PROCESSED.glob("cot_cleaned_2026*.csv")):
        df = pd.read_csv(fp, low_memory=False)
        df["rd"] = pd.to_datetime(df["report_date_as_yyyy_mm_dd"], errors="coerce")
        t = df[df["rd"] == DATE]
        nas = t[
            t["market_and_exchange_names"].astype(str).str.contains(
                "NASDAQ|NAS 100|E-MINI NASDAQ", case=False, na=False
            )
        ]
        if nas.empty:
            continue
        print("\n" + "=" * 72)
        print(f"SOURCE: {fp.name} (Disaggregated futures only)")
        print(f"DATE: {DATE} — {len(nas)} rows")
        print("=" * 72)
        for idx, r in nas.iterrows():
            print(f"\nrow_index={idx}")
            print(f"  market: {r['market_and_exchange_names']}")
            print(f"  code: {_code_str(r.get('cftc_contract_market_code'))}")
            for c in [
                "m_money_positions_long_all",
                "m_money_positions_short_all",
                "lev_money_positions_long_all",
                "lev_money_positions_short_all",
            ]:
                if c in df.columns and pd.notna(r.get(c)):
                    print(f"  {c}: {r.get(c)}")


def scan_legacy():
    zips = sorted(RAW.glob("cot_legacy_futures_only_2026*.zip"))
    if not zips:
        print("\nNo legacy zip in data/raw")
        return
    zpath = zips[-1]
    print("\n" + "=" * 72)
    print(f"SOURCE: {zpath.name} (Legacy futures only)")
    print("=" * 72)
    with zipfile.ZipFile(zpath) as z:
        raw = z.read("annual.txt")
    df = pd.read_csv(io.BytesIO(raw), low_memory=False)
    dc = "As of Date in Form YYYY-MM-DD"
    mc = "Market and Exchange Names"
    cc = "CFTC Contract Market Code"
    df["rd"] = pd.to_datetime(df[dc], errors="coerce")
    t = df[df["rd"] == DATE]
    if t.empty:
        print(f"No rows for {DATE} in legacy (max date {df['rd'].max()})")
        t = df[df["rd"] == df["rd"].max()]
        print(f"Using latest legacy date: {t['rd'].iloc[0]}")
    nas = t[
        t[mc].astype(str).str.contains("NASDAQ|NAS 100|NQ", case=False, na=False)
    ]
    print(f"NASDAQ-related rows: {len(nas)}")
    ncol = "Noncommercial Positions-Long (All)"
    ncs = "Noncommercial Positions-Short (All)"
    for idx, r in nas.iterrows():
        code = _code_str(r.get(cc))
        nl = r.get(ncol)
        ns = r.get(ncs)
        net = float(nl) - float(ns) if pd.notna(nl) and pd.notna(ns) else None
        print(f"\nrow_index={idx}")
        print(f"  market: {r[mc]}")
        print(f"  code: {code}")
        print(f"  Noncommercial Long (All): {nl}")
        print(f"  Noncommercial Short (All): {ns}")
        print(f"  Noncommercial Net: {net}")
        if code in ("209742", "20974") or "20974" in code:
            print("  >>> matches user Legacy screenshot contract family")


def main():
    print("HPTL DASHBOARD VALUES (2026-05-26):")
    print("  Long 52,861 | Short 104,540 | Net -51,679")
    print("  Contract locked: 209742 NASDAQ MINI")
    print("  Columns: lev_money_positions_long_all / short_all")
    print("  Report pipeline: financial_futures (TFF)\n")
    scan_financial()
    scan_disaggregated()
    scan_legacy()


if __name__ == "__main__":
    main()

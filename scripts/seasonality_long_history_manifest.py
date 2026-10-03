"""Institutional Edge long-history acquisition manifest.

Research-only source plan for extending the 23 core seasonality markets toward a
48-year daily-price horizon. This file intentionally separates genuine futures
history from proxy/predecessor history. It does not download licensed data and
never silently labels a proxy as exchange futures.
"""

TARGET_YEARS = 48

MARKETS = {
    "NASDAQ / NQ": {"root": "NQ", "venue": "CME", "tier": "proxy_required", "note": "NQ futures are younger than 48y; use documented underlying/predecessor bridge only if approved."},
    "S&P 500 / ES": {"root": "ES", "venue": "CME", "tier": "proxy_required", "note": "ES futures are younger than 48y; underlying S&P index can extend research history."},
    "Dow / YM": {"root": "YM", "venue": "CBOT", "tier": "proxy_required", "note": "YM futures are younger than 48y; DJIA underlying can extend research history."},
    "Euro FX / 6E": {"root": "6E", "venue": "CME", "tier": "proxy_required", "note": "Euro futures begin 1999; predecessor/ECU/FX bridge requires explicit provenance."},
    "British Pound / 6B": {"root": "6B", "venue": "CME", "tier": "native_long", "note": "Old CME currency future; priority native-history candidate."},
    "Japanese Yen / 6J": {"root": "6J", "venue": "CME", "tier": "native_long", "note": "Old CME currency future; Bernd validation market and highest priority."},
    "Swiss Franc / 6S": {"root": "6S", "venue": "CME", "tier": "native_long", "note": "Old CME currency future; Bernd validation market and highest priority."},
    "Australian Dollar / 6A": {"root": "6A", "venue": "CME", "tier": "proxy_required", "note": "Futures inception too recent for full 48y; spot FX bridge possible."},
    "Canadian Dollar / 6C": {"root": "6C", "venue": "CME", "tier": "native_long", "note": "Old CME currency future; Bernd validation market and highest priority."},
    "NZ Dollar / 6N": {"root": "6N", "venue": "CME", "tier": "proxy_required", "note": "Futures inception too recent for full 48y; spot FX bridge possible."},
    "Gold": {"root": "GC", "venue": "COMEX", "tier": "native_long", "note": "Long futures history candidate."},
    "Silver": {"root": "SI", "venue": "COMEX", "tier": "native_long", "note": "Long futures history candidate."},
    "Copper / HG": {"root": "HG", "venue": "COMEX", "tier": "native_long", "note": "Long futures history candidate."},
    "Crude Oil / CL": {"root": "CL", "venue": "NYMEX", "tier": "proxy_required", "note": "CL futures history is shorter than 48y; external Bernd window remains validation-only."},
    "Natural Gas / NG": {"root": "NG", "venue": "NYMEX", "tier": "proxy_required", "note": "NG futures history is shorter than 48y."},
    "Coffee": {"root": "KC", "venue": "ICE_US", "tier": "native_long", "note": "ICE/legacy softs history; source/licence differs from CME."},
    "Cocoa": {"root": "CC", "venue": "ICE_US", "tier": "native_long", "note": "ICE/legacy softs history; source/licence differs from CME."},
    "Corn": {"root": "ZC", "venue": "CBOT", "tier": "native_long", "note": "Very long CBOT futures history candidate."},
    "Wheat": {"root": "ZW", "venue": "CBOT", "tier": "native_long", "note": "Very long CBOT futures history candidate."},
    "Soybeans": {"root": "ZS", "venue": "CBOT", "tier": "native_long", "note": "Very long CBOT futures history candidate."},
    "Sugar": {"root": "SB", "venue": "ICE_US", "tier": "native_long", "note": "ICE/legacy softs history; source/licence differs from CME."},
    "Platinum": {"root": "PL", "venue": "NYMEX", "tier": "native_long", "note": "Verify licensed native start date before declaring 48y-ready."},
    "Palladium": {"root": "PA", "venue": "NYMEX", "tier": "native_long", "note": "Verify licensed native start date before declaring 48y-ready."},
}

BERND_VALIDATION = {"Japanese Yen / 6J", "Canadian Dollar / 6C", "Swiss Franc / 6S", "Euro FX / 6E", "US Dollar Index / DX", "Australian Dollar / 6A", "Crude Oil / CL"}

if __name__ == "__main__":
    print("INSTITUTIONAL EDGE — LONG-HISTORY ACQUISITION MANIFEST")
    print(f"target={TARGET_YEARS} years | core markets={len(MARKETS)}")
    for name, cfg in MARKETS.items():
        print(f"{name:26} {cfg['tier']:14} {cfg['venue']:7} {cfg['root']:4} | {cfg['note']}")

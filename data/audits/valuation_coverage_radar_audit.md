# Valuation Coverage Audit — Market Radar Instruments

- **Generated:** 2026-06-14  
- **Scope:** 30 instruments in `RADAR_ELIGIBLE` (`web-dashboard/src/radarEligibility.js`)  
- **Sources:** `valuation_latest.json`, `fx_valuation_v3_audit.json`, `agri_valuation_audit.md`, `index_valuation_v2_audit.md`, `fx_valuation_data_foundation_audit.md`, `engine.py` roadmap  
- **Policy:** Fundamental fair value only — no location/percentile substitution (`hptl.valuation.engine`)

---

## Executive summary

| Coverage tier | Count | Share of radar |
|---|---:|---:|
| **Currently valued** (wired + deviation, audit pass) | 10 | 33% |
| **Partially valued** (wired, low confidence or weak model) | 0* |
| **Missing** (not wired / gate fail / no model) | 20 | 67% |

\*FX and grains with **Low** confidence are counted as **Currently valued** for coverage purposes (deviation published), but flagged for quality.

**Headline:** Valuation pillar covers **10 / 30 (33%)** radar instruments with a published fair-value deviation. To reach **>90% (≥27 instruments)**, the fastest path is: finish FX (2 gaps), extend agri PSD to softs (2), ship metals V3.1 shadow (5), energy inventory proxy (2), rates curve framework (5), indices ERP for US benchmarks (3), crypto liquidity proxy (1) — **~20 incremental wires** across three phases below.

---

## Coverage matrix

| Tier | Instruments |
|---|---|
| **Currently valued** | Euro FX / 6E, Japanese Yen / 6J, Swiss Franc / 6S, Australian Dollar / 6A, Canadian Dollar / 6C, NZ Dollar / 6N, Corn, Wheat, Soybeans, Sugar |
| **Partially valued** | *(none formally — see quality notes)* |
| **Missing** | NASDAQ / NQ, S&P 500 / ES, Dow / YM, British Pound / 6B, Gold, Silver, Copper / HG, Platinum, Palladium, Crude Oil / CL, Natural Gas / NG, Coffee, Cocoa, Bitcoin, US Dollar Index / DX, US 2Y / 10Y / 30Y Treasury Yield, 2s10s Yield Curve, 10-Year Real Yield |

### Quality notes on “Currently valued”

| Instrument | Deviation | Confidence | Caveat |
|---|---:|---|---|
| Euro FX / 6E | +7.17% | Low | R²=0.10; stale CPI legs |
| Japanese Yen / 6J | −5.44% | Low | Strong R²=0.64 |
| Swiss Franc / 6S | −28.62% | Low | Large deviation — verify CHF real-yield inputs |
| Australian Dollar / 6A | −4.65% | Low | R²=0.33 |
| Canadian Dollar / 6C | −8.06% | Low | R²=0.48 |
| NZ Dollar / 6N | +4.50% | Low | Stale NZD yield/CPI |
| Corn | +27.09% | **Medium** | USDA PSD wired; check price scale |
| Wheat | −2.83% | Low | 18 aligned price/SU obs |
| Soybeans | −3.19% | Low | 18 aligned obs |
| Sugar | −27.07% | Low | Softs S/U model; verify units |

---

## Instrument detail by asset class

### FX (8 radar instruments)

**Institutional framework (production-grade):**

| Driver layer | Inputs | Role |
|---|---|---|
| Carry / rate differential | Policy rate, 2Y OIS/govt, real yield | Primary fair-value anchor |
| Inflation differential | CPI YoY (headline or core) | PPP / real-rate adjustment |
| USD regime | DXY percentile, Treasury 2s10s slope | Risk-on/off overlay |
| Cross validation | Rolling R², foundation audit, stale-input gate | Publish gate |

Model: **`fx_carry_real_yield_v3`** (live pillar engine).

| Instrument | Status | Why unavailable | Required data sources | Difficulty | Recommended model |
|---|---|---|---|---|---|
| Euro FX / 6E | **Valued** (+7.17%) | — | EUR/USD spot, ECB policy, EUR 2Y/10Y, EUR CPI, USD legs, DXY, UST curve | **Low** | `fx_carry_real_yield_v3` *(live)* |
| Japanese Yen / 6J | **Valued** (−5.44%) | — | USD/JPY spot, BoJ policy, JGB 2Y, CPI, USD legs | **Low** | `fx_carry_real_yield_v3` *(live)* |
| Swiss Franc / 6S | **Valued** (−28.62%) | — | USD/CHF spot, SNB policy, CHF yields, CPI | **Low** | `fx_carry_real_yield_v3` *(live)* |
| Australian Dollar / 6A | **Valued** (−4.65%) | — | AUD/USD spot, RBA policy, ACGB 2Y, CPI | **Low** | `fx_carry_real_yield_v3` *(live)* |
| Canadian Dollar / 6C | **Valued** (−8.06%) | — | USD/CAD spot, BoC policy, CAD 2Y, CPI | **Low** | `fx_carry_real_yield_v3` *(live)* |
| NZ Dollar / 6N | **Valued** (+4.50%) | — | NZD/USD spot, RBNZ policy, NZGB 2Y, CPI | **Low** | `fx_carry_real_yield_v3` *(live)* |
| British Pound / 6B | **Missing** | Foundation FAIL; R²=0.025 < gate 0.08 | GBP/USD spot, BoE rate **history from 2025-06**, BoE GLC 2Y/10Y, UK CPI | **Medium** | `fx_carry_real_yield_v3` after BoE policy backfill + R² review |
| US Dollar Index / DX | **Missing** | No V3 pair mapping (index, not G10 cross) | Broad USD index (ICE DX or FRED DTWEXBGS), weighted G10 fair-value basket, real-yield composite | **High** | `usd_broad_fair_value_v1` — basket of wired G10 crosses + rate-weighted synthetic |

**FX coverage:** 6 / 8 valued (75%). Gap to 90% radar: fix **6B**, define **DX** basket model.

---

### Indices (3)

**Institutional framework:**

| Driver layer | Inputs | Role |
|---|---|---|
| Earnings yield | CAPE inverse, forward EPS yield, Shiller EY | Equity cash-flow anchor |
| Risk-free rate | US 10Y nominal, 10Y real (TIPS) | Discount rate |
| Equity risk premium | EY − 10Y (or vs real yield) | Fair-value spread |
| Historical context | CAPE / ERP 10Y percentile | Regime label (cheap/fair/rich) |
| Cross-asset | Credit spreads, UMCSENT *(context only)* | Confidence modifier |

Model target: **`indices_erp_cape_v3`** (planned V3.2; audit-only today).

| Instrument | Status | Why unavailable | Required data sources | Difficulty | Recommended model |
|---|---|---|---|---|---|
| S&P 500 / ES | **Missing** | No approved model; CAPE/ERP inputs failed audit (no FRED key / Yale fetch) | Yale Shiller CAPE, S&P div yield, DGS10, earnings yield, Wilshire optional | **Medium** | `indices_erp_cape_v3` — **ES primary** |
| NASDAQ / NQ | **Missing** | Same; no free NASDAQ CAPE series | NDX price, NASDAQ earnings yield (FMP premium or estimate), DGS10 | **High** | `indices_erp_cape_v3` — NDX variant or ES-relative ERP |
| Dow / YM | **Missing** | Same | DJIA price, dividend yield proxy, DGS10 | **Medium** | `indices_erp_cape_v3` — Dow div-yield + ERP vs ES |

**Indices coverage:** 0 / 3 (0%).

---

### Metals (5)

**Institutional framework:**

| Driver layer | Inputs | Role |
|---|---|---|
| Real rates | US 10Y TIPS (DFII10) or nominal − breakeven | Primary gold/platinum driver |
| USD | DXY / broad dollar percentile | Inverse FX overlay |
| Inflation expectations | 5Y5Y breakeven, CPI | Nominal vs real decomposition |
| Industrial demand | China PMI, copper-specific *(Cu/Pd)* | Cyclical overlay |
| Cost / supply | AISC curve, mine supply *(research)* | Long-run floor/ceiling |
| Historical percentile | Real-rate-adjusted gold regression residual | Fallback fair band |

Model target: **`metals_real_yield_dxy_v3`** (planned V3.1).

| Instrument | Status | Why unavailable | Required data sources | Difficulty | Recommended model |
|---|---|---|---|---|---|
| Gold | **Missing** | Engine stub only | Spot (OANDA XAU), DFII10/DGS10, DTWEXBGS, breakeven | **Low** | `metals_real_yield_dxy_v3` — gold baseline |
| Silver | **Missing** | Engine stub | Same + gold beta ratio | **Low** | Same family, Ag/Au ratio overlay |
| Copper / HG | **Missing** | Engine stub | Spot, DGS10, China PMI (FRED), DXY | **Medium** | `metals_real_yield_dxy_v3` + China demand term |
| Platinum | **Missing** | Engine stub | Spot, real yields, auto/industrial proxy | **Medium** | Real-yield + ratio vs gold |
| Palladium | **Missing** | Engine stub | Spot, real yields, auto catalyst proxy | **High** | Real-yield + supply-concentration overlay |

**Metals coverage:** 0 / 5 (0%). **Quick win:** macro_cache already has DGS10, DTWEXBGS paths — metals V3.1 shadow buildable offline.

---

### Energy (2)

**Institutional framework:**

| Driver layer | Inputs | Role |
|---|---|---|
| Inventory | EIA crude/gas storage vs 5Y avg | Primary CL/NG anchor |
| Term structure | Front vs 12M spread (contango/backwardation) | Carry fair value |
| USD | DXY percentile | Dollar-priced commodity overlay |
| Seasonal deviation | Storage vs seasonal norm | NG-specific |
| Macro demand | OECD leading indicator, PMI | Demand context |

Model target: **`energy_inventory_dxy_v3`** (planned V3.3).

| Instrument | Status | Why unavailable | Required data sources | Difficulty | Recommended model |
|---|---|---|---|---|---|
| Crude Oil / CL | **Missing** | No inventory engine | WTI spot (DCOILWTICO), EIA weekly stocks, Cushing, DXY | **Medium** | `energy_inventory_dxy_v3` — CL inventory + curve |
| Natural Gas / NG | **Missing** | No storage engine | Henry Hub (DHHNGSP), EIA working gas storage, HDD/CDD | **Medium** | Same — NG storage vs seasonal norm |

**Energy coverage:** 0 / 2 (0%). FRED WTI/NG prices cached; **EIA API** is the main new feed.

---

### Agriculture (7)

**Institutional framework:**

| Driver layer | Inputs | Role |
|---|---|---|
| Balance sheet | USDA WASDE / PSD ending stocks, total use | S/U ratio |
| Stocks-to-use percentile | Historical S/U vs price regression | Fair value |
| Export / production | WASDE production, exports | Supply shock context |
| USD | DXY | Export competitiveness |
| Origin-specific | ICE/COTECO for softs | Coffee/Cocoa |

Models: **`agri_stu_percentile_v1`** *(live)* → upgrade to **`grains_stocks_to_use_v3`**.

| Instrument | Status | Why unavailable | Required data sources | Difficulty | Recommended model |
|---|---|---|---|---|---|
| Corn | **Valued** (+27.09%) | — | USDA PSD (wired), spot | **Low** | `agri_stu_percentile_v1` *(live)* |
| Wheat | **Valued** (−2.83%) | — | USDA PSD (wired) | **Low** | Same |
| Soybeans | **Valued** (−3.19%) | — | USDA PSD (wired) | **Low** | Same |
| Sugar | **Valued** (−27.07%) | — | USDA PSD (wired) | **Low** | Same |
| Coffee | **Missing** | No PSD commodity map; no balance sheet on disk | ICE KC price *(live)*, Brazil/US USDA softs PSD or CONAB | **Medium** | `softs_balance_sheet_v1` → ICE origin S/U |
| Cocoa | **Missing** | Same | ICCO/grind stats, origin production | **High** | `softs_balance_sheet_v1` — origin balance sheet |

**Agriculture coverage:** 4 / 6 radar ag markets valued (67%). Coffee/Cocoa are the gaps.

---

### Crypto (1)

**Institutional framework:**

| Driver layer | Inputs | Role |
|---|---|---|
| On-chain | MVRV, SOPR, exchange reserves | Cycle positioning |
| Macro liquidity | Fed balance sheet, real yields, DXY | Risk-appetite overlay |
| Flow | ETF AUM, stablecoin supply | Demand proxy |
| Historical percentile | Real-yield-adjusted BTC regression | Fallback without on-chain |

Model target: **`crypto_liquidity_risk_v3`** (planned V3.5).

| Instrument | Status | Why unavailable | Required data sources | Difficulty | Recommended model |
|---|---|---|---|---|---|
| Bitcoin | **Missing** | No approved model | BTC spot (wired), DGS10/DFII10, DXY, MVRV (Glassnode/CoinMetrics) | **Medium** | Phase 1: macro-only `crypto_liquidity_risk_v3`; Phase 2: MVRV overlay |

**Crypto coverage:** 0 / 1 (0%).

---

### Rates (5)

**Institutional framework:**

Rates on radar are **yield levels**, not FX-style crosses. Fair value = model-implied yield vs spot yield.

| Driver layer | Inputs | Role |
|---|---|---|
| Policy anchor | Fed funds, dot plot / forwards | Short-end fair value |
| Inflation expectations | Breakeven, survey CPI | Real rate decomposition |
| Term premium | ACM/NY Fed model, 2s10s | Curve shape fair value |
| Historical percentile | Yield vs 10Y rolling band | Regime context |
| Macro surprise | CPI, NFP, FOMC | Event overlay *(display only)* |

Model target: **`rates_curve_fair_value_v1`** *(new — not in roadmap yet)*.

| Instrument | Status | Why unavailable | Required data sources | Difficulty | Recommended model |
|---|---|---|---|---|---|
| US 2-Year Treasury Yield | **Missing** | `macro` asset class undefined in engine | DGS2, Fed funds, OIS forwards | **Low** | Policy-rate fair value + term premium |
| US 10-Year Treasury Yield | **Missing** | Same | DGS10, DGS2, breakeven, ACM term premium | **Medium** | 10Y fair = real rate + inflation + term premium |
| US 30-Year Treasury Yield | **Missing** | Same | DGS30, DGS10, longevity/term premium | **Medium** | 30Y = 10Y + long-end premium model |
| 2s10s Yield Curve | **Missing** | Same | DGS2, DGS10 | **Low** | Derived from 2Y/10Y fair values |
| 10-Year Real Yield | **Missing** | Same | DFII10, DGS10, breakeven | **Low** | TIPS fair value vs nominal decomposition |

**Rates coverage:** 0 / 5 (0%). **Quick win:** DGS2/DGS10/DGS30 already in macro pipeline — rates are **Phase 1** with no new external feed.

---

## Asset-class framework summary

| Asset class | Production framework | Primary model ID | Radar valued | Radar total |
|---|---|---|---:|---:|
| FX | Real yield + policy + 2Y diff + CPI diff + DXY/UST regime | `fx_carry_real_yield_v3` | 6 | 8 |
| Indices | CAPE / earnings yield − 10Y = ERP; percentile bands | `indices_erp_cape_v3` | 0 | 3 |
| Metals | Real rates + DXY + (Cu: China PMI); percentile fallback | `metals_real_yield_dxy_v3` | 0 | 5 |
| Energy | EIA inventory vs norm + term structure + DXY | `energy_inventory_dxy_v3` | 0 | 2 |
| Agriculture | USDA WASDE S/U percentile regression | `agri_stu_percentile_v1` | 4 | 6 |
| Crypto | MVRV + real rates + liquidity + percentile fallback | `crypto_liquidity_risk_v3` | 0 | 1 |
| Rates | Policy anchor + term premium + fair yield curve | `rates_curve_fair_value_v1` | 0 | 5 |

---

## Roadmap to >90% radar coverage (≥27 / 30)

### Phase 1 — Quick wins (existing data, ~4–6 weeks)

| # | Work item | Instruments unlocked | Est. coverage after |
|---|---|---|---:|
| 1 | **GBP foundation repair** — extend BoE policy history; re-run V3 audit | British Pound / 6B | +1 → 11/30 |
| 2 | **Rates fair-value module** — DGS2/10/30 + DFII10 from macro_cache; wire 5 macro yield instruments | US 2Y/10Y/30Y, 2s10s, Real Yield | +5 → 16/30 |
| 3 | **Metals V3.1 shadow** — real yield + DXY regression on cached series; audit gate then wire | Gold, Silver, Copper, Platinum, Palladium | +5 → 21/30 |
| 4 | **BTC macro proxy** — real yield + DXY + liquidity index (Fed BS from FRED) without MVRV | Bitcoin | +1 → 22/30 |
| 5 | **Index ERP v1 (ES only)** — Yale Shiller CSV offline + DGS10; shadow until audit pass | S&P 500 / ES | +1 → 23/30 |
| 6 | **Agri confidence uplift** — already wired; improve alignment depth (no new feed) | *(quality, not count)* | — |

**Phase 1 target:** ~**23 / 30 (77%)** with ES + rates + metals + GBP fix.

### Phase 2 — New data feeds (~8–12 weeks)

| # | Work item | Instruments unlocked | Est. coverage after |
|---|---|---|---:|
| 7 | **EIA weekly inventory ingest** (API key) + CL/NG fair value | Crude Oil / CL, Natural Gas / NG | +2 → 25/30 |
| 8 | **USDA PSD extend** — Coffee (`0911000`) + Cocoa ICCO/grind adapter | Coffee, Cocoa | +2 → 27/30 |
| 9 | **Index ERP extend** — NQ earnings proxy (FMP or estimate); Dow div yield | NASDAQ / NQ, Dow / YM | +2 → 29/30 |
| 10 | **DX basket model** — synthetic from G10 fair values + weights | US Dollar Index / DX | +1 → **30/30 (100%)** |
| 11 | **BTC MVRV overlay** — Glassnode or CoinMetrics API | Bitcoin *(quality)* | — |

**Phase 2 target:** **≥27 / 30 (90%+)** — meets stated goal.

### Phase 3 — Research projects (quality / institution-grade)

| Project | Purpose |
|---|---|
| **Copper China composite** | Replace single PMI with credit impulse + IP + PMI blend |
| **Platinum/Palladium cost curve** | AISC-based floor/ceiling for PGM fair value |
| **Energy term-structure model** | Full calendar spread fair value vs inventory |
| **International index ERP** | FTSE/DAX/Nikkei CAPE sourcing (paid/OECD) |
| **FX confidence upgrade** | Refresh stale CPI legs; EUR policy history extension |
| **Agri v3 upgrade** | Move from percentile v1 to multi-factor S/U + export demand |
| **Confluence integration gates** | Only promote valuation to confluence when confidence ≥ Medium |

---

## Path to 90% — critical path

```
Today (33%) ──► Phase 1: rates + metals + GBP + ES (77%)
                      │
                      ▼
               Phase 2: EIA + softs PSD + NQ/YM + DX (100%)
                      │
                      ▼
               Phase 3: model quality, confluence gates, research overlays
```

**Minimum viable 90% bundle:** Phase 1 items 1–3 + Phase 2 items 7–10 = **27 instruments** without waiting for crypto on-chain or index international expansion.

---

## Data inventory already on disk (Phase 1 enablers)

| Feed | Location / ID | Valuation use |
|---|---|---|
| G10 FX spot + rates | `data/cache/fx_rates/`, BIS/central bank files | FX V3 *(live)* |
| USDA PSD balance sheets | `data/processed/agri_balance_sheet/` | Agri *(live for 5 markets)* |
| US Treasury yields | FRED DGS2/10/30 in macro_cache | Rates + metals + indices |
| WTI / NG spot | FRED DCOILWTICO, DHHNGSP | Energy price layer |
| Broad USD | FRED DTWEXBGS / DX futures | Metals, energy, crypto overlay |
| Canonical prices | `prices_latest.json` / OANDA backfill | All asset classes |

**Known blockers:** FRED API key for live CAPE refresh; EIA API for inventories; Coffee/Cocoa absent from `usda_psd_commodity_map.json`; GBP BoE policy shallow history; DX not mapped to FX V3 pair schema.

---

## Appendix — engine roadmap reference

From `src/hptl/valuation/engine.py` (`ASSET_CLASS_ROADMAP`):

| Phase | Model ID | Asset class |
|---|---|---|
| V3.0 | `fx_carry_real_yield_v3` | FX *(live)* |
| V3.1 | `metals_real_yield_dxy_v3` | Metals |
| V3.2 | `indices_erp_cape_v3` | Indices |
| V3.3 | `energy_inventory_dxy_v3` | Energy |
| V3.4 | `grains_stocks_to_use_v3` | Grains |
| V3.5 | `crypto_liquidity_risk_v3` | Crypto |
| V3.6 | `softs_balance_sheet_v3` | Softs |

JSON matrix: `data/audits/valuation_coverage_radar_matrix.json`

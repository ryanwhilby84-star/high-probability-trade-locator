# UI impact audit — valuation reset vs current workstation

**Audit date:** 2026-06-12  
**Scope:** What the trader sees in the web dashboard (Scanner, Instrument, COT charts, pillars).

---

## 1. What was visible BEFORE the valuation reset

| Surface | What you saw | Data source |
|---------|----------------|-------------|
| **Scanner — Valuation column** | 52-week price percentile + bias/score for most markets (e.g. Gold “69th pct Bearish”) | `valuation_latest.json` (v1, **91 wired**) + confluence row `valuation_*` |
| **Scanner — FX rows** | Institutional Macro V2 gap % + bias when confluence carried `fx_valuation` | Confluence row + `fx_valuation_latest.json` |
| **Instrument — COT workstation charts** | Price, **Location** (52w percentile line), Institutional net, Retail net; Valuation strip often showed percentile-based score | `cot_3y_series_latest.json` + confluence history `valuation_score` / percentile |
| **Instrument — FX block** | Policy / 2Y / real-yield differentials, fair-value estimate, gap %, V2 condition | Confluence `fx_valuation` object |
| **Instrument — Institutional positioning** | Snapshot grid, flow, pressure, 13w trail, macro map | `confluence_history_latest.json` |
| **Opportunity pillars** | Valuation + Seasonality pass/fail rows | Confluence `valuation_bias` / `seasonality_*` |
| **Thesis tracker pillars** | Location + Valuation lines on cards | Row overlays |

---

## 2. What is visible NOW (after reset)

| Surface | What you see now | Why |
|---------|------------------|-----|
| **Scanner — Valuation column (non-FX)** | **“UNAVAILABLE”** for almost all commodities/indices | `ValuationCell` requires `location_wired` on the row; confluence rows **do not carry** `location_*` fields. `location_latest.json` (**106 wired**) is **not merged** into rows. |
| **Scanner — Valuation column (FX)** | Usually still shows V2 gap/bias **if** confluence row has `fx_valuation` | Row data intact; `fx_valuation_latest.json` still populated |
| **Instrument — COT workstation — Location chart** | **Empty / warning** “Insufficient price history for 52-week location” | `CotWorkstationSection` does **not** pass `locationDoc`; history map looks for `location_wired` on rows (missing). |
| **Instrument — COT workstation — Valuation chart** | **“UNAVAILABLE”** subtitle on every market | `valuation_latest.json` reset to **0 wired / all UNAVAILABLE**; chart enrichment sets `valuation_fair: null` always. |
| **Instrument — FX Institutional Valuation block** | Still renders **when** `row.fx_valuation` exists (e.g. Euro FX) | Confluence export unchanged |
| **Instrument — Opportunity pillars** | **Hidden** (returns null) when `valuation_bias` is UNAVAILABLE | Pillar panel bails out if no wired valuation |
| **Instrument — Intelligence chart (EUR pilot)** | Valuation panel may show “unavailable” if history bundle thin | Pilot-only; separate from main workstation |
| **Macro relationship map / COT tables / flow** | Still present | Unaffected by valuation export |

---

## 3. What data was REMOVED (export layer)

| Export | Before | After |
|--------|--------|-------|
| `valuation_latest.json` | v1, **91 instruments wired** with `valuation_bias`, `valuation_score`, 52w percentile reason | v2 pillar split, **0 wired**, all `valuation_state: UNAVAILABLE`, explicit “do not substitute location” |
| Confluence row `valuation_*` | Still present on latest rows (legacy percentile fields) | **Not removed** from `confluence_history_latest.json` |
| `location_latest.json` | N/A at reset (new file) | **106 wired** — location pillar live |
| `fx_valuation_latest.json` | V2 macro rates + pair panels | **Still populated** |
| `fx_valuation_v3_latest.json` | N/A | **New** — V3 model output + drivers (6/13 audit pass) |
| `fx_valuation_data_foundation_audit.json` | N/A | **New** — per-pair foundation PASS/FAIL + blockers |

**Important:** COT, macro, seasonality, prices, and FX V2 row payloads were **not** deleted. The reset removed **display wiring** to location/valuation exports and zeroed the valuation pillar JSON.

---

## 4. Data that EXISTS in exports but is NOT shown

| Data | File | Status |
|------|------|--------|
| Location bias, 52w percentile, score | `location_latest.json` | Loaded only on **COT Positioning** page hook — **not** on Scanner or Instrument workstation |
| V3 fair value, drivers, DXY/Treasury regime | `fx_valuation_v3_latest.json` | **Not wired** to any panel |
| Foundation PASS/FAIL + blockers | `fx_valuation_data_foundation_audit.json` | **Not wired** |
| FX currency rates (policy, 2Y, real yield) | `fx_valuation_latest.json` | Used by scanner FX cell indirectly; **not** in V3 dev panel |
| Confluence `valuation_price_percentile_52w` + `valuation_score` | `confluence_history_latest.json` | **Not** used for Location chart after reset (field rename not bridged) |
| Confluence `fx_valuation` object | Same | Shown in FX block only — **not** in chart valuation strip |
| Macro relationship maps | `macro_relationship_maps_latest.json` | Wired on instrument detail |

---

## 5. Panels that went EMPTY because of the reset

| Panel | Symptom | Root cause |
|-------|---------|------------|
| Scanner **Valuation** cell (non-FX) | UNAVAILABLE | No `location_*` on row; no `location_latest` merge |
| COT workstation **Location** chart | Flat / warning | No `locationDoc`; no legacy percentile bridge on history |
| COT workstation **Valuation** chart | UNAVAILABLE strip | `valuation_latest.json` all unwired; enrichment nulls fair value |
| **Opportunity pillars — Valuation** | Section missing | `valuation_bias: UNAVAILABLE` from new export semantics |
| **Intelligence chart — Valuation** (pilot) | “Valuation history unavailable” | Depends on `fx_valuation_history_latest.json` panels |

---

## 6. Panels still working (unchanged)

- Institutional positioning (levels, flow, pressure, 13w table, history modes)
- Retail positioning (when COT non-reportable in 3Y series)
- Price chart (when `cot_3y` + `prices_latest` align)
- COT institutional / retail net charts in workstation
- Macro relationship map (when map export present)
- Week backdrop / global regime (from confluence payload)
- FX Institutional Valuation block (when `row.fx_valuation` present)
- Seasonality section (when staging export available)

---

## 7. Remediation applied in this pass

1. Merge **`location_latest.json`** into scanner cells and chart workstation meta.
2. Bridge **legacy confluence** `valuation_score` / `valuation_price_percentile_52w` → Location history line (not Valuation).
3. Pass **`locationDoc`** + **`v3Doc`** into `CotWorkstation` from instrument page.
4. Add **`FxValuationV3DevPanel`** on FX instrument pages — macro evidence + foundation status + blockers.
5. Valuation chart strip shows **honest “In development”** or **V3 audit-pass deviation** only — never fake percentile-as-valuation.
6. Scanner column renamed **Location / FX val** — non-FX reads location export; FX keeps V2 gap from confluence.

---

## 8. What you should see after remediation

| Surface | Expected |
|---------|----------|
| Scanner **Location / FX val** | Gold etc.: `69th pct · Bearish` from `location_latest.json`; FX: V2 gap % + bias |
| COT workstation **Location** chart | 52w percentile line + subtitle when export wired |
| COT workstation **Valuation** chart | “In development” note; V3 deviation only when audit PASS |
| FX instrument **Valuation V3** panel | Policy / 2Y / real-yield spreads, DXY + Treasury regime, Foundation PASS/NEAR PASS/FAIL, blockers |
| Institutional / Retail / Price / COT charts | Unchanged — still from confluence + `cot_3y_series_latest.json` |
| FX Institutional Valuation (V2) block | Still shown when `row.fx_valuation` present |

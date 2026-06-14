# Valuation Update Profile — `run_valuation_update`

- Generated: 2026-06-13T19:35:06Z
- Command profiled: `python -m hptl.valuation.run_valuation_update`
- Mode: **offline** (`HPTL_SKIP_LIVE_FEEDS=1`) — no network cache refresh
- Total wall time: **1820.5s (~30.3 min)**
- Instruments in universe: **138** (`TARGET_MARKETS`)
- FX V3 pairs: **13**

## Slowest stage

**`run FX V3 audit`** — **793.7s** (43.6% of total)

This stage calls `run_fx_v3_audit()` inside `write_fx_v3_audit_artifacts()`. It reloads macro history, re-runs all 13 pair valuations, builds audit rows (spot + alignment per pair), and computes COT market blocks.

## Stage breakdown

| Stage | Duration (s) | Records | Notes |
|---|---:|---:|---|
| module import (confluence TARGET_MARKETS) | 0.0 | 138 | Paid on first import (~12s before timing started) |
| build location_latest | 33.1 | 138 | `load_canonical_timeline` per market |
| export location_latest.json (+ dashboard copies) | 0.04 | 138 | JSON write — negligible |
| refresh caches (`ensure_fx_macro_caches`) | 0.0 | 0 | Offline: no network; **online default is `refresh_caches=True`** |
| load macro history (`currency_histories`) | 30.9 | 8 | G10 legs; BoE GLC parse dominates |
| V3 single pair (macro re-loaded each call) | 33.8 | 1 | Diagnostic: default `compute_fx_pair_v3` |
| V3 single pair (shared histories injected) | 1.0 | 1 | Diagnostic: same pair with `histories=` passed |
| **run V3 valuation (`build_all_fx_v3_pairs`)** | **377.3** | 13 | **~29s × 13 pairs — reloads `currency_histories()` per pair** |
| **run FX V3 audit (`refresh_caches=False`)** | **793.7** | 13 | Includes another full V3 pass + spot/audit rows |
| export FX V3 audit files (+ dashboard copies) | 0.17 | 13 | JSON + MD writes — negligible |
| build valuation_latest.json | 374.0 | 138 | Per-market canonical timeline + V3 cache merge |
| export valuation_latest.json (+ dashboard copies) | 0.07 | 138 | JSON write — negligible |
| duplicate: build location_latest (main pass) | 31.8 | 138 | Second full rebuild in `main()` for summary only |
| duplicate: build valuation_latest (main pass) | 144.7 | 138 | Second rebuild in `main()` for summary only |
| run V2 valuation (`fx_institutional_valuation`) | 0.0 | 0 | **NOT INVOKED** on this path |

## Ranked by duration

1. run FX V3 audit — 793.7s
2. run V3 valuation (`build_all_fx_v3_pairs`) — 377.3s
3. build valuation_latest.json — 374.0s
4. duplicate: build valuation_latest (main pass) — 144.7s
5. build location_latest — 33.1s (+ 31.8s duplicate)
6. load macro history — 30.9s (once; then repeated inside V3 loops)

## Key findings (no optimization applied)

1. **Repeated macro loading is the dominant cost.** `compute_fx_pair_v3()` calls `currency_histories()` when `histories` is not passed (~31s/call). `build_all_fx_v3_pairs` runs 13 pairs → ~377s. `run_fx_v3_audit` runs `build_all_fx_v3_pairs` again inside the same pipeline.

2. **FX V3 audit is the slowest single stage** because it combines macro reload + full 13-pair valuation + per-pair spot/alignment audit rows + COT market computation.

3. **`build valuation_latest.json` is the second-largest build stage** (~374s) — mostly iterating 138 markets with `load_canonical_timeline`, not FX math.

4. **File exports are fast** (<0.2s each). Time is in computation, not I/O.

5. **`run_valuation_update.main()` duplicates work** — after `write_pillar_exports()` already built location + valuation, `main()` calls `build_location_latest()` and `build_valuation_latest()` again (~176s extra).

6. **FX V2 is not on this path.** Only V3 (`fx_carry_real_yield_v3`) via `fx_v3_audit`.

7. **Online runs may be slower still.** Default `write_fx_v3_audit_artifacts()` uses `run_fx_v3_audit(refresh_caches=True)`, which can fetch BIS/MoF/BoE/CAD feeds before macro load. This profile measured refresh as 0s offline.

## Profiler artifact

- JSON: `data/audits/valuation_update_profile.json`
- Script: `scripts/profile_valuation_update.py`

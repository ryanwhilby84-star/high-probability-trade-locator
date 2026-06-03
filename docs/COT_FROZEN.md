# COT layer — frozen

The COT positioning stack is **operational and frozen** as of the weekly integrity gate rollout.

## What runs automatically

Every COT import (`python -m hptl.cot.run_update`) executes:

1. Download / parse / tracked master rebuild  
2. Confluence rebuild  
3. **Weekly integrity gate** (`hptl.cot.weekly_integrity_gate`)  
   - Source truth validation (official CFTC Legacy Futures Only vs dashboard)  
   - Thesis snapshot refresh (all COT-mapped instruments)  
   - Lineage validation (Source Truth → Dashboard → Scanner → Thesis → Scoring)  
4. Quarantine failed instruments and republish confluence without them  

Standalone gate (no download):

```powershell
$env:PYTHONPATH = "src"
python -m hptl.cot.run_weekly_integrity_gate
```

## Quarantine behaviour

Instruments that fail source-truth or lineage checks are written to `data/cot_quarantine_latest.json` and are **excluded** from:

- Confluence row build / scoring  
- Scanner attention board  
- Thesis seeding  
- Priority board ranking  

## Policy

**No further COT feature work** unless a weekly integrity failure is detected and must be remediated.

Allowed maintenance:

- Fixing a detected integrity failure for a specific instrument  
- CFTC mapping corrections  
- Gate threshold tuning if false positives appear  

Not allowed without explicit approval:

- New COT audit pages or dashboards  
- New diagnostic UIs or deliverable markdown reports  
- Alternate positioning sources (TFF / disaggregated managed money for legacy markets)  

## Primary development track

**Valuation engine** — see `docs/VALUATION_ENGINE_PLAN.md`.

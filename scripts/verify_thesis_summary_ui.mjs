#!/usr/bin/env node
/**
 * Renders Thesis Summary pillar values using the same code path as the dashboard UI.
 * Run after export rebuild: node scripts/verify_thesis_summary_ui.mjs
 */
import { readFileSync } from 'node:fs'
import { fileURLToPath } from 'node:url'
import { dirname, join } from 'node:path'
import { hydrateThesesFromConfluence } from '../web-dashboard/src/thesisTracker/confluenceOverlay.js'
import { buildOpportunity } from '../web-dashboard/src/thesisTracker/alignmentEngine.js'

const ROOT = join(dirname(fileURLToPath(import.meta.url)), '..')
const conf = JSON.parse(readFileSync(join(ROOT, 'web-dashboard/public/data/confluence_history_latest.json'), 'utf8'))
const thesisDoc = JSON.parse(readFileSync(join(ROOT, 'web-dashboard/public/data/thesis_tracker_latest.json'), 'utf8'))

const MARKETS = ['NASDAQ / NQ', 'S&P 500 / ES', 'Dow / YM']

function renderSummary(thesis) {
  const opp = buildOpportunity(thesis)
  const s = opp.summary || {}
  return {
    instrument: s.instrument_display || thesis.market,
    alignment: opp.alignment?.label,
    valuation_state: s.valuation?.state,
    valuation_score: s.valuation?.score_display,
    seasonality_state: s.seasonality?.state,
    seasonality_score: s.seasonality?.score_display,
    institutions_state: s.institutions?.state,
    institutions_score: s.institutions?.score_display,
    retail_state: s.retail?.state,
    location_state: s.location?.state,
    action: opp.action,
    data_source: thesis.confluenceRow ? 'confluence_history_latest.json' : 'thesis snapshot fallback',
    confluence_week: thesis.confluenceRow?.cot_report_date || null,
  }
}

const hydrated = hydrateThesesFromConfluence(thesisDoc.theses, conf)

console.log('=== Thesis Summary UI values (confluence-first hydration) ===\n')
for (const market of MARKETS) {
  const thesis = hydrated.find((t) => t.market === market)
  if (!thesis) {
    console.log(`${market}: NOT FOUND`)
    continue
  }
  const ui = renderSummary(thesis)
  console.log(`${ui.instrument}`)
  console.log(`  source: ${ui.data_source} (${ui.confluence_week})`)
  console.log(`  alignment: ${ui.alignment}`)
  console.log(`  valuation: ${ui.valuation_state} ${ui.valuation_score}`)
  console.log(`  seasonality: ${ui.seasonality_state} ${ui.seasonality_score}`)
  console.log(`  institutions: ${ui.institutions_state} ${ui.institutions_score}`)
  console.log(`  retail: ${ui.retail_state}`)
  console.log(`  location: ${ui.location_state}`)
  console.log(`  action: ${ui.action}`)
  console.log('')
}

// Prove stale thesis snapshots cannot override confluence scoring fields.
const nasdaq = hydrated.find((t) => t.market === 'NASDAQ / NQ')
if (nasdaq) {
  const poisoned = {
    ...nasdaq,
    snapshots: [
      ...(nasdaq.snapshots || []).slice(0, -1),
      {
        ...(nasdaq.snapshots?.[nasdaq.snapshots.length - 1] || {}),
        valuation_bias: 'UNAVAILABLE',
        valuation_score: 0,
        valuation_wired: false,
        seasonality_bias: 'UNAVAILABLE',
        seasonality_score: 0,
        seasonality_wired: false,
      },
    ],
  }
  const ui = renderSummary(poisoned)
  const ok =
    ui.valuation_state === 'BEARISH' &&
    ui.valuation_score === '10.0 / 10' &&
    ui.seasonality_state === 'BULLISH' &&
    ui.seasonality_score === '5.7 / 10'
  console.log(`Stale snapshot override test: ${ok ? 'PASS' : 'FAIL'}`)
  if (!ok) {
    console.log('  got', ui)
    process.exit(1)
  }
}

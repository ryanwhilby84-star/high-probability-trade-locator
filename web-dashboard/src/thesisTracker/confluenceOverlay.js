/** Confluence export is the source of truth for thesis summary scoring fields. */

const SCORING_FIELDS = [
  'cot_bias',
  'cot_score',
  'zone_focus',
  'retail_net',
  'positioning_state',
  'valuation_bias',
  'valuation_score',
  'valuation_reason',
  'valuation_wired',
  'valuation_price_percentile_52w',
  'seasonality_bias',
  'seasonality_score',
  'seasonality_reason',
  'seasonality_wired',
  'seasonality_calendar_month',
  'data_integrity',
]

export function latestConfluenceByMarket(records) {
  const byMarket = {}
  for (const row of records || []) {
    const market = String(row?.market || '').trim()
    if (!market) continue
    const dateKey = String(row.cot_report_date || row.week || '')
    const prev = byMarket[market]
    const prevKey = prev ? String(prev.cot_report_date || prev.week || '') : ''
    if (!prev || dateKey.localeCompare(prevKey) > 0) {
      byMarket[market] = row
    }
  }
  return byMarket
}

/** Map a confluence_history row into the snapshot shape used by evaluatePillars(). */
export function confluenceRowToScoringSnap(row) {
  if (!row) return {}
  const nr = row.cot_positioning_groups?.nonreportable
  const snap = {}
  for (const key of SCORING_FIELDS) {
    if (Object.prototype.hasOwnProperty.call(row, key)) {
      snap[key] = row[key]
    }
  }
  snap.cot_bias = row.cot_bias ?? row.final_calculated_cot_bias ?? snap.cot_bias
  snap.cot_score = row.cot_score ?? row.final_calculated_cot_score ?? snap.cot_score
  snap.zone_focus =
    row.zone_focus ?? row.institutional_context?.tactical?.zone_focus ?? snap.zone_focus
  if (!isNum(snap.retail_net) && isNum(nr?.net)) {
    snap.retail_net = nr.net
  }
  return snap
}

function isNum(v) {
  return typeof v === 'number' && Number.isFinite(v)
}

/**
 * Attach latest confluence row per market; strip stale exported opportunity cache.
 * Thesis snapshots remain for history/status — scoring reads confluenceRow only.
 */
export function hydrateThesesFromConfluence(theses, confluenceDoc) {
  const byMarket = latestConfluenceByMarket(confluenceDoc?.records)
  return (theses || []).map((thesis) => {
    if (!thesis?.market) return thesis
    const { opportunity: _stale, ...rest } = thesis
    const confluenceRow = byMarket[thesis.market] || null
    return { ...rest, confluenceRow }
  })
}

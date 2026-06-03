/**
 * Radar display gate (P1 + P2 of the data-cleanup remediation plan).
 *
 * This is a DISPLAY-ONLY filter. Scoring, confluence, and COT logic are unchanged
 * and keep running for the full instrument universe in the background — this module
 * only decides which instruments the radar/scanner is allowed to show.
 *
 * Eligible = canonical primary assets with a real (direct) CFTC COT market.
 * Suppressed = duplicates (proxy COT), derived crosses (leg-derived / ratio),
 * orphaned assets (crypto / EM / exotic / non-canonical foreign indices), and
 * no-data canonical assets awaiting P4/P5 data wiring (Brent, rates, DAX/FTSE/Nikkei/Russell).
 *
 * The keep-list below is the 23 direct-COT canonical markets identified by the
 * asset universe audit (classification == PRIMARY). It will expand in P4/P5 once
 * price symbols, Treasury COT mappings, and missing FRED macro maps are connected.
 */

export const RADAR_ELIGIBLE = new Set([
  // US equity indices (direct COT)
  'NASDAQ / NQ',
  'S&P 500 / ES',
  'Dow / YM',
  // FX currency legs (direct COT)
  'Euro FX / 6E',
  'British Pound / 6B',
  'Japanese Yen / 6J',
  'Swiss Franc / 6S',
  'Australian Dollar / 6A',
  'Canadian Dollar / 6C',
  'NZ Dollar / 6N',
  // Metals (direct COT)
  'Gold',
  'Silver',
  'Copper / HG',
  'Platinum',
  'Palladium',
  // Energy (direct COT)
  'Crude Oil / CL',
  'Natural Gas / NG',
  // Agriculture / softs (direct COT)
  'Coffee',
  'Cocoa',
  'Corn',
  'Wheat',
  'Soybeans',
  'Sugar',
])

/**
 * True when an instrument is allowed on the radar.
 *
 * Matches the RAW instrument id only. We deliberately do NOT fall back to
 * canonicalMarketId(): that helper collapses derived/duplicate instruments onto
 * their base economic exposure via substring heuristics (e.g. "Silver/EUR" -> "Silver",
 * "Gold/GBP" -> "British Pound / 6B", "Brent Crude Oil" -> "Crude Oil / CL"), which
 * re-admitted exactly the duplicate/derived rows this gate is meant to suppress.
 * Confluence records carry the exact instrument id in `market`, so a raw match is correct.
 * @param {string} market
 */
export function isRadarEligible(market) {
  if (!market) return false
  return RADAR_ELIGIBLE.has(market)
}

/**
 * Suppression reason for a row, or null when eligible. Used for the debug view /
 * tooltips. Derives from the COT status already present in the payload — it does
 * not recompute or alter any scoring.
 * @param {{ market?: string, cot_status?: string, instrument_meta?: { cot_status?: string } }} row
 */
export function radarSuppressionReason(row) {
  if (!row) return 'no_data'
  if (isRadarEligible(row.market)) return null
  const status = row.cot_status || row.instrument_meta?.cot_status || ''
  if (status === 'proxy_cot') return 'duplicate'
  if (status === 'leg_derived_cot') return 'derived'
  if (status === 'macro_only') return 'derived'
  return 'orphaned_or_no_data'
}

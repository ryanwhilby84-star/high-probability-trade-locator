import test from 'node:test'
import assert from 'node:assert/strict'
import {
  TRACKED_MARKET_IDS,
  canonicalMarketId,
  recordCotReportDate,
  resolveRowForMarketWeek,
  defaultDashboardWeek,
} from './marketResolution.js'

const resolved = (market, date, cotDate = date) => ({
  market, date, cot_report_date: cotDate, cot_bias: 'Bullish',
  cot_score: 70, net_value: 123, institutional_flow_summary: 'CFTC report present',
})

test('tracked COT universe has 26 distinct canonical markets', () => {
  assert.equal(TRACKED_MARKET_IDS.length, 26)
  assert.equal(new Set(TRACKED_MARKET_IDS.map(canonicalMarketId)).size, 26)
})

test('missing week falls back without changing the actual CFTC report date', () => {
  const rows = [resolved('Gold', '2026-09-29'), resolved('Silver', '2026-10-06')]
  const match = resolveRowForMarketWeek(rows, 'Gold', '2026-10-06')
  assert.equal(match.matchMode, 'backward_calendar_week')
  assert.equal(recordCotReportDate(match.row), '2026-09-29')
  assert.notEqual(recordCotReportDate(match.row), '2026-10-06')
})

test('resolver never borrows a report from the future', () => {
  const rows = [resolved('Gold', '2026-10-13')]
  const match = resolveRowForMarketWeek(rows, 'Gold', '2026-10-06')
  assert.equal(match.row, null)
  assert.equal(match.matchMode, 'none')
})

test('unresolved current week prefers a resolved earlier report', () => {
  const rows = [
    resolved('Gold', '2026-09-29'),
    { market: 'Gold', date: '2026-10-06', cot_bias: 'N/A', institutional_flow_summary: 'N/A: no COT row for this market and date' },
  ]
  const match = resolveRowForMarketWeek(rows, 'Gold', '2026-10-06')
  assert.equal(match.matchMode, 'backward_calendar_week')
  assert.equal(recordCotReportDate(match.row), '2026-09-29')
})

test('FX crosses and dollar price identities do not collapse into COT leg', () => {
  assert.equal(canonicalMarketId('EUR/JPY'), 'EUR/JPY')
  assert.equal(canonicalMarketId('GBP/NZD'), 'GBP/NZD')
  assert.equal(canonicalMarketId('DTWEXBGS'), 'Broad US Dollar Index — DTWEXBGS')
  assert.equal(canonicalMarketId('ICE DX'), 'US Dollar Index / DXY — ICE DX futures')
  assert.equal(canonicalMarketId('DXY'), 'US Dollar Index / DX')
})

test('default dashboard week requires all three indices to have resolved COT', () => {
  const rows = [
    resolved('NASDAQ / NQ', '2026-09-29'),
    resolved('S&P 500 / ES', '2026-09-29'),
    resolved('Dow / YM', '2026-09-29'),
    resolved('NASDAQ / NQ', '2026-10-06'),
    resolved('S&P 500 / ES', '2026-10-06'),
  ]
  assert.equal(defaultDashboardWeek(rows), '2026-10-06')
  // The 6 Oct view legitimately reuses the 29 Sep Dow report.
  const dow = resolveRowForMarketWeek(rows, 'Dow / YM', '2026-10-06')
  assert.equal(recordCotReportDate(dow.row), '2026-09-29')
})

test('calendar-aligned rows cannot reveal future CFTC report dates', () => {
  const rows = [
    resolved('Gold', '2026-09-29'),
    resolved('Gold', '2026-10-06', '2026-10-13'),
  ]
  const match = resolveRowForMarketWeek(rows, 'Gold', '2026-10-06')
  assert.equal(recordCotReportDate(match.row), '2026-09-29')
  assert.equal(match.matchMode, 'backward_calendar_week')
  const futureOnly = resolveRowForMarketWeek(rows.slice(1), 'Gold', '2026-10-06')
  assert.equal(futureOnly.matchMode, 'exact_unresolved')
})

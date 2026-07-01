/**
 * Price engine validation — PASS only when all consumers match authoritative stores.
 */

import { LivePriceStore } from './stores/LivePriceStore.js'
import { WeeklyOHLCStore } from './stores/WeeklyOHLCStore.js'
import { HistoricalCOTStore } from './stores/HistoricalCOTStore.js'

const FP_EPS = 1e-6

const isNum = (v) => typeof v === 'number' && Number.isFinite(v)

function fpEqual(a, b) {
  if (!isNum(a) && !isNum(b)) return a == null && b == null
  if (!isNum(a) || !isNum(b)) return false
  return Math.abs(a - b) <= FP_EPS
}

function check(name, expected, actual, component) {
  const pass = fpEqual(expected, actual)
  return {
    name,
    pass,
    status: pass ? 'PASS' : 'FAIL',
    expected: isNum(expected) ? expected : expected ?? null,
    actual: isNum(actual) ? actual : actual ?? null,
    component,
    store: name.includes('Weekly')
      ? WeeklyOHLCStore.STORE_NAME
      : name.includes('Historical') || name.includes('COT')
        ? HistoricalCOTStore.STORE_NAME
        : LivePriceStore.STORE_NAME,
  }
}

/**
 * @param {string} marketId
 * @param {object} consumers — values read from UI components at render time
 */
export function validatePriceEngine(marketId, consumers = {}) {
  const liveQuote = LivePriceStore.getQuote(marketId)
  const liveMid = liveQuote?.mid ?? null
  const weekly = WeeklyOHLCStore.getCompletedWeekly(marketId)
  const weeklyClose = weekly?.close ?? null

  const cotDate = consumers.historicalHeaderDate ?? consumers.crosshairDate ?? null
  const cotClose =
    cotDate != null ? HistoricalCOTStore.getHistoricalCloseAtDate(marketId, cotDate) : null

  const checks = [
    check('Live marker == LivePriceStore mid', liveMid, consumers.liveMarkerPrice, 'Live marker'),
    check('Valuation live == LivePriceStore mid', liveMid, consumers.valuationLivePrice, 'Valuation overlay'),
    check('Scanner live == LivePriceStore mid', liveMid, consumers.scannerLivePrice, 'Scanner'),
    check('Truth table live == LivePriceStore mid', liveMid, consumers.truthTableLivePrice, 'Truth table'),
    check('Header live label == LivePriceStore mid', liveMid, consumers.headerLivePrice, 'Header live label'),
    check('Weekly chart == WeeklyOHLCStore close', weeklyClose, consumers.weeklyChartClose, 'Weekly chart'),
    check('CandleBadge == WeeklyOHLCStore close', weeklyClose, consumers.candleBadgeClose, 'CandleBadge'),
    check(
      'Historical header == HistoricalCOTStore close',
      cotClose,
      consumers.historicalHeaderPrice,
      'Historical header / crosshair',
    ),
  ]

  const evaluated = checks.filter((c) => c.expected != null || c.actual != null)
  const failures = evaluated.filter((c) => !c.pass)
  const pass = failures.length === 0

  return {
    marketId,
    pass,
    status: pass ? 'PASS' : 'FAIL',
    checks: evaluated,
    failures,
    authoritative: {
      liveMid,
      liveStatus: LivePriceStore.getStatus(marketId),
      weeklyClose,
      weeklyDate: weekly?.date ?? null,
      historicalCotClose: cotClose,
      historicalCotDate: cotDate,
    },
  }
}

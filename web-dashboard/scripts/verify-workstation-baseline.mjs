/**
 * Baseline verification — static JSON coverage + COT workstation render checks.
 * Usage: npm run build && npx vite preview --port 4173
 *        node scripts/verify-workstation-baseline.mjs
 */
import { readFileSync, existsSync } from 'fs'
import { fileURLToPath } from 'url'
import { dirname, join } from 'path'
import { chromium } from 'playwright'

import { resolveMarketBlock } from '../src/charts/marketBlockResolve.js'

const __dirname = dirname(fileURLToPath(import.meta.url))
const root = join(__dirname, '..')
const dataDir = join(root, 'public', 'data')
const baseUrl = process.env.DEV_URL || 'http://127.0.0.1:4173'

const REQUIRED_EXPORTS = [
  'cot_3y_series_latest.json',
  'workstation_ohlc_latest.json',
  'instrument_registry.json',
  'valuation_latest.json',
  'scanner_latest.json',
]

const OPTIONAL_EXPORTS = ['instrument_valuation_history_latest.json']

function loadJson(name) {
  const path = join(dataDir, name)
  if (!existsSync(path)) return { ok: false, path, error: 'missing' }
  try {
    return { ok: true, path, doc: JSON.parse(readFileSync(path, 'utf8')) }
  } catch (err) {
    return { ok: false, path, error: String(err?.message || err) }
  }
}

function checkStaticExports() {
  const files = {}
  for (const name of REQUIRED_EXPORTS) {
    const r = loadJson(name)
    files[name] = r.ok ? 'ok' : r.error
  }
  for (const name of OPTIONAL_EXPORTS) {
    const r = loadJson(name)
    files[name] = r.ok ? 'ok' : `missing (${r.error})`
  }
  return files
}

function checkCotCoverage() {
  const reg = loadJson('instrument_registry.json')
  const cot = loadJson('cot_3y_series_latest.json')
  const ohlc = loadJson('workstation_ohlc_latest.json')
  if (!reg.ok || !cot.ok || !ohlc.ok) {
    return { error: 'missing registry/cot/ohlc export', failures: [] }
  }

  const markets = reg.doc.markets.filter((m) => m.has_cot_mapping).map((m) => m.id)
  const failures = []
  const warnings = []

  for (const id of markets) {
    const { block } = resolveMarketBlock(cot.doc, id)
    const rows = block?.series ?? block?.rows ?? []
    const ohlcBlock = ohlc.doc.instruments?.[id]
    const issues = []
    if (!block) issues.push('missing_cot_block')
    else if (!rows.length) issues.push('empty_cot_series')
    if (!ohlcBlock) issues.push('missing_ohlc_block')
    else if (!ohlcBlock.weekly_ohlc?.length) issues.push('empty_ohlc')
    const critical = issues.filter((i) => i !== 'empty_ohlc' && i !== 'missing_ohlc_block')
    if (critical.length) failures.push({ market: id, issues: critical })
    else if (issues.length) warnings.push({ market: id, issues })
  }

  return { marketCount: markets.length, failures, warnings }
}

async function checkCotWorkstationRoutes(markets) {
  const browser = await chromium.launch({ headless: true })
  const results = {}

  for (const market of markets) {
    const page = await browser.newPage()
    const pageErrors = []
    page.on('pageerror', (e) => pageErrors.push(e.message))
    page.on('console', (msg) => {
      if (msg.type() === 'error') {
        const text = msg.text()
        if (text.includes('ERR_INSUFFICIENT_RESOURCES')) return
        pageErrors.push(text)
      }
    })

    const url = `${baseUrl}/#/instrument/${encodeURIComponent(market)}/cot-workstation`
    await page.goto(url, { waitUntil: 'domcontentloaded', timeout: 30000 })

    try {
      await page.waitForSelector('.cot-workstation[data-charts-ready="1"]', { timeout: 12000 })
    } catch {
      /* timeout captured below */
    }

    const snap = await page.evaluate(() => ({
      chartsReady: document.querySelector('.cot-workstation')?.getAttribute('data-charts-ready') === '1',
      panelCount: document.querySelectorAll('.cot-ws-panel').length,
      canvasReady: document.querySelectorAll('.cot-ws-chart-canvas--ready').length,
      hasError: document.querySelector('.cot-ws-status--error') != null,
      errorText: document.querySelector('.cot-ws-status--error')?.innerText?.slice(0, 120) || null,
    }))

    results[market] = {
      pageErrors: pageErrors.slice(0, 3),
      ...snap,
      ok:
        !pageErrors.length &&
        snap.chartsReady &&
        snap.panelCount === 4 &&
        snap.canvasReady >= 4 &&
        !snap.hasError,
    }

    await page.close()
  }

  await browser.close()
  return results
}

const staticFiles = checkStaticExports()
const cotCoverage = checkCotCoverage()

const priorityMarkets = [
  'Gold',
  'NASDAQ / NQ',
  'Platinum',
  'Australian Dollar / 6A',
  'Sugar',
]

const regDoc = loadJson('instrument_registry.json')
const allCotMarkets =
  regDoc.ok && Array.isArray(regDoc.doc?.markets)
    ? regDoc.doc.markets.filter((m) => m.has_cot_mapping).map((m) => m.id)
    : priorityMarkets

let renderResults = {}
try {
  renderResults = await checkCotWorkstationRoutes(allCotMarkets)
} catch (err) {
  renderResults = { _error: String(err?.message || err) }
}

const report = {
  staticFiles,
  cotCoverage,
  renderSummary: {
    total: allCotMarkets.length,
    passed: allCotMarkets.filter((m) => renderResults[m]?.ok).length,
    failed: allCotMarkets.filter((m) => !renderResults[m]?.ok),
  },
  renderResults,
}

console.log(JSON.stringify(report, null, 2))

const staticOk = REQUIRED_EXPORTS.every((f) => staticFiles[f] === 'ok')
const cotOk = (cotCoverage.failures?.length ?? 0) === 0
const renderOk = allCotMarkets.every((m) => renderResults[m]?.ok)

process.exit(staticOk && cotOk && renderOk ? 0 : 1)

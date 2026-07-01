#!/usr/bin/env node
/**
 * Browser verification: live_quotes_latest.json fetch + chart price labels.
 * Run: npm run preview (separate terminal) then node scripts/verify-live-quotes-browser.mjs
 */
import fs from 'node:fs'
import path from 'node:path'
import { fileURLToPath } from 'node:url'
import { chromium } from 'playwright'

const __dirname = path.dirname(fileURLToPath(import.meta.url))
const root = path.join(__dirname, '..')
const baseUrl = process.env.PREVIEW_URL || 'http://127.0.0.1:4173'
const liveDoc = JSON.parse(
  fs.readFileSync(path.join(root, 'public/data/live_quotes_latest.json'), 'utf8'),
)

const cases = [
  { marketId: 'Gold', hash: '#instrument/Gold' },
  { marketId: 'NASDAQ / NQ', hash: '#instrument/NASDAQ%20%2F%20NQ' },
]

async function main() {
  const browser = await chromium.launch({ headless: true })
  const page = await browser.newPage()
  const results = []

  for (const { marketId, hash } of cases) {
    const liveQuote = liveDoc.instruments?.[marketId]
    const networkHits = []
    const consoleHits = []

    page.removeAllListeners('request')
    page.removeAllListeners('console')

    page.on('request', (req) => {
      const u = req.url()
      if (u.includes('live_quotes_latest.json')) networkHits.push(u)
    })
    page.on('console', (msg) => {
      const t = msg.text()
      if (t.includes('[price-context]') && t.includes(marketId)) consoleHits.push(t)
    })

    await page.goto(`${baseUrl}/${hash}`, { waitUntil: 'networkidle', timeout: 60000 })

    await page.waitForTimeout(2000)

    const labels = await page.locator('.pos-chart-panel-price-label').allTextContents()
    const labelText = labels.join(' | ')

    const chartCloseMatch = labelText.match(/Chart close: completed weekly OHLC\s+([\d,.]+)/)
    const liveMatch = labelText.match(/Live price:\s+([\d,.]+)/)
    const valSourceMatch = labelText.match(/Valuation price source:\s+(\S+)/)

    const chartClose = chartCloseMatch ? Number(chartCloseMatch[1].replace(/,/g, '')) : null
    const livePrice = liveMatch ? Number(liveMatch[1].replace(/,/g, '')) : null
    const valSource = valSourceMatch?.[1] ?? null

    const weeklyExpected = liveQuote?.latest_completed_ohlc_close
    const liveExpected = liveQuote?.live_price

    const checks = {
      networkFetchedLiveQuotes: networkHits.some((u) => u.includes('live_quotes_latest.json?v=')),
      consolePriceContext: consoleHits.length > 0,
      chartCloseLabel: chartClose != null && Math.abs(chartClose - weeklyExpected) < 0.02,
      livePriceLabel:
        livePrice != null && liveExpected != null && Math.abs(livePrice - liveExpected) < 0.02,
      valuationSourceLabel: valSource === 'live/latest',
    }

    results.push({ marketId, checks, labelText, networkHits: networkHits.slice(0, 2), consoleHits })
  }

  await browser.close()

  let failures = 0
  for (const r of results) {
    console.log('='.repeat(60))
    console.log(r.marketId)
    console.log('labels:', r.labelText || '(none)')
    console.log('network:', r.networkHits[0] || '(none)')
    console.log('console:', r.consoleHits[0]?.slice(0, 200) || '(none)')
    for (const [k, ok] of Object.entries(r.checks)) {
      console.log(ok ? 'PASS' : 'FAIL', k)
      if (!ok) failures++
    }
  }

  console.log('')
  console.log(failures ? `BROWSER RESULT: ${failures} failure(s)` : 'BROWSER RESULT: ALL PASS')
  process.exit(failures ? 1 : 0)
}

main().catch((err) => {
  console.error(err.message)
  process.exit(2)
})

import { chromium } from 'playwright'

const url = process.env.DEV_URL || 'http://127.0.0.1:5173'
const browser = await chromium.launch({ headless: true })
const page = await browser.newPage({ viewport: { width: 1440, height: 900 } })
await page.goto(`${url}/#/instrument/Sugar/cot-workstation`, {
  waitUntil: 'domcontentloaded',
  timeout: 30000,
})
await page.waitForFunction(
  () => document.querySelector('.cot-workstation')?.dataset?.chartsReady === '1',
  { timeout: 45000 },
)
await page.waitForTimeout(1000)

const result = await page.evaluate(() => {
  const panes = window.__COT_WS_CAMERA__?.getPaneRanges?.()
  const bounds = window.__COT_WS_CAMERA__?.getTimelineBounds?.()
  const priceChart = window.__COT_WS_PRICE_CHART__
  const out = { lastIdx: (bounds?.count ?? 0) - 1, trials: [] }
  // We cannot reach the chart object directly; use the debug hook if present.
  return out
})

// Probe scrollToPosition via a dedicated debug method we add below.
const probe = await page.evaluate(() => {
  const dbg = window.__COT_WS_CAMERA__
  if (!dbg?.probeScroll) return 'no probeScroll'
  return dbg.probeScroll()
})

console.log(JSON.stringify({ result, probe }, null, 2))
await browser.close()

import { chromium } from 'playwright'

const baseUrl = process.env.DEV_URL || 'http://127.0.0.1:5173'
const browser = await chromium.launch({ headless: true })
const page = await browser.newPage({ viewport: { width: 1440, height: 900 } })

await page.goto(`${baseUrl}/#/instrument/Sugar/cot-workstation`, { waitUntil: 'domcontentloaded' })
await page.waitForTimeout(5000)

const result = await page.evaluate(() => {
  const tm = window.__COT_WS_TIMELINE__
  if (!tm) return { error: 'no timeline api' }
  const panesBefore = tm.getPaneRanges()
  const paneCount = Object.keys(panesBefore).length
  const before = tm.getMaster()
  tm.fitAll(495)
  const after = tm.getMaster()
  const panesAfter = tm.getPaneRanges()
  return {
    paneCount,
    before,
    after,
    panesBefore,
    panesAfter,
    diag: window.__COT_WS_DIAG__?.get?.(),
  }
})

console.log(JSON.stringify(result, null, 2))
await browser.close()

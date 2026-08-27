/** Capture full (non-minified) React error + component stack for Cocoa cot-workstation. */
import { chromium } from 'playwright'

const baseUrl = process.env.DEV_URL || 'http://127.0.0.1:5199'
const market = process.env.MARKET || 'Cocoa'

const browser = await chromium.launch({ headless: true })
const page = await browser.newPage()
const errors = []
page.on('pageerror', (e) => errors.push({ type: 'pageerror', message: String(e?.message || e), stack: e?.stack }))
page.on('console', (msg) => {
  if (msg.type() === 'error') {
    const t = msg.text()
    if (t.includes('ERR_INSUFFICIENT_RESOURCES')) return
    errors.push({ type: 'console', message: t })
  }
})

let frameNavs = 0
page.on('framenavigated', () => { frameNavs += 1 })

const url = `${baseUrl}/#/instrument/${encodeURIComponent(market)}/cot-workstation`
try {
  await page.goto(url, { waitUntil: 'domcontentloaded', timeout: 20000 })
} catch (e) {
  errors.push({ type: 'nav', message: String(e?.message || e) })
}
await page.waitForTimeout(6000)
console.log(`MARKET=${market} frameNavs=${frameNavs}`)
console.log(JSON.stringify(errors.slice(0, 8), null, 2))
await browser.close()
process.exit(0)

import { chromium } from 'playwright'

const browser = await chromium.launch()
const page = await browser.newPage()
const errors = []
const logs = []

page.on('pageerror', (e) => errors.push(`PAGEERROR: ${e.message}\n${e.stack}`))
page.on('console', (m) => {
  const text = m.text()
  if (m.type() === 'error' || text.includes('workstation') || text.includes('candlestick')) {
    logs.push(`[${m.type()}] ${text}`)
  }
})

await page.goto('http://localhost:5173/#/instrument/NASDAQ%20/%20NQ', {
  waitUntil: 'networkidle',
  timeout: 90000,
})
await page.waitForTimeout(8000)

const body = await page.locator('body').innerText()
const candles = await page.locator('.pos-chart-panel--candles').count()
const linePrice = await page.locator('.positioning-chart-stack--legacy-line').count()
const crashMsg = body.includes('Candlestick chart crashed')

console.log(JSON.stringify({ candles, linePrice, crashMsg, errors, logs: logs.slice(-20) }, null, 2))

await browser.close()

import { chromium } from 'playwright'

const URL = 'http://localhost:5173/#/instrument/Gold'

const browser = await chromium.launch()
const page = await browser.newPage()

const consoleLogs = []
const pageErrors = []

page.on('console', (msg) => {
  const text = msg.text()
  if (text.includes('[workstation]') || msg.type() === 'error') {
    consoleLogs.push({ type: msg.type(), text })
  }
})
page.on('pageerror', (err) => pageErrors.push(err.message))

await page.goto(URL, { waitUntil: 'networkidle', timeout: 120000 })
await page.waitForTimeout(5000)

const workstation = page.locator('#instrument-research-workstation')
const chart = page.locator('.irw-candle-chart')
const canvas = page.locator('.irw-candle-chart canvas')

const result = {
  url: URL,
  pageErrors,
  consoleLogs,
  hasWorkstation: (await workstation.count()) > 0,
  workstationText: (await workstation.innerText().catch(() => '')).slice(0, 400),
  hasChartDiv: (await chart.count()) > 0,
  canvasCount: await canvas.count(),
  hasLegacyDetail: (await page.locator('.instrument-detail, #valuation-evidence').count()) > 0,
}

console.log(JSON.stringify(result, null, 2))
await browser.close()
process.exit(pageErrors.length || result.canvasCount === 0 ? 1 : 0)

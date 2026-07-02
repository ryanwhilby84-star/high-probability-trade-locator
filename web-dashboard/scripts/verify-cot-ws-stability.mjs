import { chromium } from 'playwright'

const baseUrl = process.env.DEV_URL || 'http://127.0.0.1:4173'

const markets = [
  'Gold',
  'NASDAQ%20%2F%20NQ',
  'Platinum',
  'AUD%20%2F%206A',
  'Sugar',
]

const browser = await chromium.launch({ headless: true })
const page = await browser.newPage()
const pageErrors = []

page.on('pageerror', (e) => pageErrors.push(e.message))
page.on('console', (msg) => {
  if (msg.type() === 'error') pageErrors.push(msg.text())
})

const results = {}

for (const market of markets) {
  pageErrors.length = 0
  const url = `${baseUrl}/#/instrument/${market}/cot-workstation`
  await page.goto(url, { waitUntil: 'domcontentloaded', timeout: 30000 })
  await page.waitForTimeout(4000)

  const diag = await page.evaluate(() => {
    const root = document.querySelector('.cot-workstation')
    return {
      chartsReady: root?.getAttribute('data-charts-ready') === '1',
      panelCount: document.querySelectorAll('.cot-ws-panel').length,
      canvasReady: document.querySelectorAll('.cot-ws-chart-canvas--ready').length,
      diag: window.__COT_WS_DIAG__?.get?.() ?? null,
    }
  })

  results[market] = {
    pageErrors: [...pageErrors].slice(0, 3),
    ...diag,
  }
}

console.log(JSON.stringify(results, null, 2))
await browser.close()

const ok = markets.every((m) => {
  const r = results[m]
  return (
    !r?.pageErrors?.length &&
    r?.chartsReady &&
    r?.panelCount === 4 &&
    r?.canvasReady >= 4
  )
})

process.exit(ok ? 0 : 1)

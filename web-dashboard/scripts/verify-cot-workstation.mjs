import { chromium } from 'playwright'

const baseUrl = process.env.DEV_URL || 'http://127.0.0.1:4173'

const routes = [
  { name: 'scanner', url: `${baseUrl}/#/scanner` },
  { name: 'goldInstrument', url: `${baseUrl}/#/instrument/Gold` },
  { name: 'goldCotWs', url: `${baseUrl}/#/instrument/Gold/cot-workstation` },
  { name: 'nasdaqCotWs', url: `${baseUrl}/#/instrument/NASDAQ%20%2F%20NQ/cot-workstation` },
]

const browser = await chromium.launch({ headless: true })
const page = await browser.newPage()
const pageErrors = []

page.on('pageerror', (e) => pageErrors.push(e.message))
page.on('console', (msg) => {
  if (msg.type() === 'error') pageErrors.push(msg.text())
})

const results = {}

for (const route of routes) {
  pageErrors.length = 0
  await page.goto(route.url, { waitUntil: 'domcontentloaded', timeout: 30000 })
  await page.waitForTimeout(route.name === 'scanner' ? 2000 : 5000)

  results[route.name] = {
    pageErrors: [...pageErrors].slice(0, 5),
    hasCotWorkstation: (await page.locator('.cot-workstation').count()) > 0,
    hasFullscreenCot: (await page.locator('.cot-workstation--fullscreen').count()) > 0,
    panelCount: await page.locator('.cot-ws-panel').count(),
    has10YBtn: (await page.locator('button.cot-ws-range-btn:has-text("10Y")').count()) > 0,
    hasRawDataTabs: (await page.locator('.instrument-positioning-tabs').count()) > 0,
    hasOpenCotBtn: (await page.locator('text=Open COT Workstation').count()) > 0,
    bodyHasContent: ((await page.locator('body').innerText()) || '').length > 100,
  }
}

console.log(JSON.stringify(results, null, 2))
await browser.close()

const ok =
  results.scanner?.bodyHasContent &&
  !results.scanner?.pageErrors?.length &&
  results.goldInstrument?.hasOpenCotBtn &&
  results.goldInstrument?.hasRawDataTabs &&
  !results.goldInstrument?.hasCotWorkstation &&
  !results.goldInstrument?.pageErrors?.length &&
  results.goldCotWs?.hasCotWorkstation &&
  results.goldCotWs?.hasFullscreenCot &&
  results.goldCotWs?.has10YBtn &&
  results.goldCotWs?.panelCount === 4 &&
  !results.goldCotWs?.pageErrors?.length &&
  results.nasdaqCotWs?.hasCotWorkstation &&
  results.nasdaqCotWs?.panelCount === 4 &&
  !results.nasdaqCotWs?.pageErrors?.length

process.exit(ok ? 0 : 1)

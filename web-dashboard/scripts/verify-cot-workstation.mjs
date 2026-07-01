import { chromium } from 'playwright'

const baseUrl = process.env.DEV_URL || 'http://127.0.0.1:4173'

const routes = [
  { name: 'scanner', url: `${baseUrl}/#/scanner` },
  { name: 'gold', url: `${baseUrl}/#/instrument/Gold` },
  { name: 'nasdaq', url: `${baseUrl}/#/instrument/NASDAQ%20%2F%20NQ` },
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
    hasCot3yClass: (await page.locator('.positioning-chart-stack--cot3y').count()) > 0,
    panelCount: await page.locator('.cot-ws-panel').count(),
    bodyHasContent: ((await page.locator('body').innerText()) || '').length > 100,
  }
}

console.log(JSON.stringify(results, null, 2))
await browser.close()

const ok =
  results.scanner?.bodyHasContent &&
  !results.scanner?.pageErrors?.length &&
  results.gold?.hasCotWorkstation &&
  results.gold?.panelCount === 4 &&
  !results.gold?.pageErrors?.length &&
  results.nasdaq?.hasCotWorkstation &&
  results.nasdaq?.panelCount === 4 &&
  !results.nasdaq?.pageErrors?.length

process.exit(ok ? 0 : 1)

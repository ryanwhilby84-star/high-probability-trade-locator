import { chromium } from 'playwright'

const baseUrl = process.env.DEV_URL || 'http://127.0.0.1:5173'

const browser = await chromium.launch({ headless: true })
const page = await browser.newPage()
const pageErrors = []

page.on('pageerror', (e) => pageErrors.push(e.message))
page.on('console', (msg) => {
  if (msg.type() === 'error') pageErrors.push(msg.text())
})

await page.goto(`${baseUrl}/#/instrument/Gold`, { waitUntil: 'networkidle', timeout: 90000 })
await page.getByRole('button', { name: 'Positioning' }).click().catch(() => {})
await page.waitForTimeout(4000)

const result = {
  workstationDrawingToolbarError: pageErrors.some((e) => e.includes('WorkstationDrawingToolbar')),
  pageErrors: pageErrors.slice(0, 5),
  hasCot3yStack: (await page.locator('.positioning-chart-stack--cot3y').count()) > 0,
  hasLegacyFallbackText: (await page.locator('text=Falling back to legacy confluence charts').count()) > 0,
  hasWorkstationError: (await page.locator('text=Positioning workstation error').count()) > 0,
  hasGoldTruthPanel: (await page.locator('.gold-truth-panel').count()) > 0,
  hasDrawingToolbar: (await page.locator('.ws-drawing-toolbar').count()) > 0,
}

console.log(JSON.stringify(result, null, 2))
await browser.close()
process.exit(result.hasCot3yStack && !result.workstationDrawingToolbarError && !result.hasWorkstationError ? 0 : 1)

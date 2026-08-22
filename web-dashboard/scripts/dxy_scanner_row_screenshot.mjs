import { chromium } from 'playwright'
import path from 'path'

const outDir = path.resolve('../data/audits')
const browser = await chromium.launch({ headless: true })
const page = await browser.newPage({ viewport: { width: 1440, height: 1100 } })
await page.goto('http://127.0.0.1:4176/', { waitUntil: 'networkidle', timeout: 90000 })
await page.waitForTimeout(2500)

await page.getByRole('button', { name: /All markets/i }).first().click()
await page.waitForTimeout(800)
await page.getByLabel('Search markets').fill('US Dollar Index')
await page.waitForTimeout(1500)

const table = page.locator('.scanner-table').first()
await table.scrollIntoViewIfNeeded()
const row = page.locator('tr', { hasText: 'US Dollar Index / DX' }).first()
const count = await row.count()
console.log('ROW_COUNT', count)
if (count) {
  await row.scrollIntoViewIfNeeded()
}
await page.screenshot({
  path: path.join(outDir, 'dxy_scanner_table_row.png'),
  fullPage: false,
})
await browser.close()
if (!count) process.exit(3)
console.log('SCANNER_ROW_OK')

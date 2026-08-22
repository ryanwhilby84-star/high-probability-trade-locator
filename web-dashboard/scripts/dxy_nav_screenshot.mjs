import { chromium } from 'playwright'
import fs from 'fs'
import path from 'path'

const outDir = path.resolve('../data/audits')
fs.mkdirSync(outDir, { recursive: true })

const browser = await chromium.launch({ headless: true })
const page = await browser.newPage({ viewport: { width: 1440, height: 960 } })

await page.goto('http://127.0.0.1:4176/', { waitUntil: 'networkidle', timeout: 90000 })
await page.waitForTimeout(3000)

// Prefer dedicated sidebar button (always visible after our fix)
const sideBtn = page.getByRole('button', { name: 'US Dollar Index / DX', exact: true }).first()
await sideBtn.waitFor({ state: 'visible', timeout: 15000 })
await page.screenshot({
  path: path.join(outDir, 'dxy_sidebar_visible.png'),
  fullPage: false,
})

// Also prove scanner search finds it under radar-eligible filter
await page.getByRole('button', { name: /All markets/i }).first().click()
await page.waitForTimeout(1000)
const search = page.getByLabel('Search markets')
if (await search.count()) {
  await search.fill('Dollar Index')
  await page.waitForTimeout(1500)
}
await page.screenshot({
  path: path.join(outDir, 'dxy_scanner_visible.png'),
  fullPage: false,
})

// Open instrument via scanner row if present, else sidebar button
const row = page.locator('tr', { hasText: 'US Dollar Index / DX' }).first()
if (await row.count()) {
  await row.click()
} else {
  await sideBtn.click()
}
await page.waitForTimeout(3000)
await page.screenshot({
  path: path.join(outDir, 'dxy_instrument_page.png'),
  fullPage: false,
})

const hash = await page.evaluate(() => location.hash)
const bodyText = await page.locator('body').innerText()
const proof = {
  hash,
  url: page.url(),
  hasInstrumentTitle: bodyText.includes('US Dollar Index / DX'),
  hasCotWorkstationButton: bodyText.includes('Open COT Workstation'),
  hasMacroBiasButton: bodyText.includes('Open DXY Macro Bias'),
}
fs.writeFileSync(path.join(outDir, 'dxy_nav_proof.json'), JSON.stringify(proof, null, 2))
console.log(JSON.stringify(proof, null, 2))

if (!proof.hasInstrumentTitle) {
  await browser.close()
  process.exit(2)
}

await browser.close()
console.log('SCREENSHOTS_OK')

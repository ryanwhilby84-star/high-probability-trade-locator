import { chromium } from 'playwright'
import fs from 'fs'

const url = process.env.DEV_URL || 'http://127.0.0.1:5173'
const outDir = 'scripts/.zoom-check'
fs.mkdirSync(outDir, { recursive: true })

const browser = await chromium.launch({ headless: true })
const page = await browser.newPage({ viewport: { width: 1440, height: 900 } })
await page.goto(`${url}/#/instrument/Sugar/cot-workstation`, {
  waitUntil: 'domcontentloaded',
  timeout: 30000,
})
await page.waitForFunction(
  () => document.querySelector('.cot-workstation')?.dataset?.chartsReady === '1',
  { timeout: 45000 },
)
await page.waitForTimeout(1000)

const measure = () =>
  page.evaluate(() => {
    const panes = window.__COT_WS_CAMERA__?.getPaneRanges?.()
    return {
      priceLogical: panes?.price?.logical ?? null,
      commercialLogical: panes?.commercial?.logical ?? null,
      priceBarSpacing: panes?.price?.scale?.barSpacing ?? null,
    }
  })

const box = await page.locator('[data-panel="price"] canvas').first().boundingBox()
const cx = box.x + box.width * 0.55
const cy = box.y + box.height * 0.5

const before = await measure()
await page.screenshot({ path: `${outDir}/before.png` })

for (let i = 0; i < 3; i += 1) {
  await page.mouse.move(cx, cy)
  await page.mouse.wheel(0, -120)
  await page.waitForTimeout(160)
}
await page.waitForTimeout(400)
const after = await measure()
await page.screenshot({ path: `${outDir}/after.png` })

const spanBefore = before.priceLogical ? before.priceLogical.to - before.priceLogical.from : null
const spanAfter = after.priceLogical ? after.priceLogical.to - after.priceLogical.from : null
const priceCommercialAligned =
  after.priceLogical &&
  after.commercialLogical &&
  Math.abs(after.priceLogical.from - after.commercialLogical.from) < 0.5 &&
  Math.abs(after.priceLogical.to - after.commercialLogical.to) < 0.5

console.log(
  JSON.stringify(
    {
      before,
      after,
      spanBefore,
      spanAfter,
      windowShrank: spanAfter < spanBefore * 0.4,
      candlesWider: after.priceBarSpacing > before.priceBarSpacing * 2,
      priceCommercialAligned,
    },
    null,
    2,
  ),
)
await browser.close()

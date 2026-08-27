import { chromium } from 'playwright'
import fs from 'fs'

const url = 'http://127.0.0.1:5173'
const outDir = 'scripts/.zoom-check'
fs.mkdirSync(outDir, { recursive: true })

const browser = await chromium.launch({ headless: true })
const page = await browser.newPage({ viewport: { width: 1440, height: 1600 } })
await page.goto(`${url}/#/instrument/Sugar/cot-workstation`, {
  waitUntil: 'domcontentloaded',
  timeout: 45000,
})
await page.waitForFunction(
  () => document.querySelector('.cot-ws-build-badge')?.textContent?.includes('BUILD 10'),
  { timeout: 45000 },
)
await page.waitForFunction(
  () => document.querySelector('.cot-workstation')?.dataset?.chartsReady === '1',
  { timeout: 45000 },
)
await page.waitForTimeout(1500)

const box = await page.locator('[data-panel="price"] canvas').first().boundingBox()
const cx = box.x + box.width * 0.55
const cy = box.y + box.height * 0.5

for (let i = 0; i < 6; i += 1) {
  await page.mouse.move(cx, cy)
  await page.mouse.wheel(0, -120)
  await page.waitForTimeout(150)
}
await page.waitForTimeout(600)
await page.screenshot({ path: `${outDir}/vstretch-before.png` })

await page.keyboard.down('Shift')
for (let i = 0; i < 10; i += 1) {
  await page.mouse.move(cx, cy)
  await page.mouse.wheel(0, -120)
  await page.waitForTimeout(150)
}
await page.keyboard.up('Shift')
await page.waitForTimeout(800)
await page.screenshot({ path: `${outDir}/vstretch-after.png` })

console.log(`Wrote ${outDir}/vstretch-before.png`)
console.log(`Wrote ${outDir}/vstretch-after.png`)
await browser.close()

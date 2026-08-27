import { chromium } from 'playwright'
import fs from 'fs'

const url = 'http://127.0.0.1:5173'
const outDir = 'scripts/.zoom-check'
fs.mkdirSync(outDir, { recursive: true })

const browser = await chromium.launch({ headless: true })
const page = await browser.newPage({ viewport: { width: 1440, height: 900 } })
await page.goto(`${url}/#/instrument/Sugar/cot-workstation`, {
  waitUntil: 'domcontentloaded',
  timeout: 30000,
})
await page.waitForFunction(
  () => document.querySelector('.cot-ws-build-badge')?.textContent?.includes('BUILD 10'),
  { timeout: 45000 },
)
await page.waitForFunction(
  () => document.querySelector('.cot-workstation')?.dataset?.chartsReady === '1',
  { timeout: 45000 },
)
await page.waitForTimeout(1000)

const readState = () =>
  page.evaluate(() => {
    const cam = window.__COT_WS_CAMERA__?.getCamera?.()
    const verticalStretch = window.__COT_WS_CAMERA__?.getVerticalStretch?.()
    const verticalMetrics = window.__COT_WS_CAMERA__?.getVerticalMetrics?.()
    const panes = window.__COT_WS_CAMERA__?.getPaneRanges?.()
    return {
      cam,
      verticalStretch,
      verticalMetrics,
      priceLogical: panes?.price?.logical,
      commercialLogical: panes?.commercial?.logical,
      priceSpacing: panes?.price?.scale?.barSpacing,
    }
})

const before = await readState()
await page.screenshot({ path: `${outDir}/before.png` })

const box = await page.locator('[data-panel="price"] canvas').first().boundingBox()
const cx = box.x + box.width * 0.55
const cy = box.y + box.height * 0.5

for (let i = 0; i < 6; i += 1) {
  await page.mouse.move(cx, cy)
  await page.mouse.wheel(0, -120)
  await page.waitForTimeout(120)
}
await page.waitForTimeout(400)
const zoomed = await readState()
await page.screenshot({ path: `${outDir}/zoomed.png` })

// Pan right at same magnification
await page.mouse.move(cx, cy)
await page.mouse.down()
for (let i = 0; i < 20; i += 1) {
  await page.mouse.move(cx + i * 12, cy)
  await page.waitForTimeout(24)
}
await page.mouse.up()
await page.waitForTimeout(400)
const panned = await readState()
await page.screenshot({ path: `${outDir}/panned.png` })

// Vertical stretch (Shift + wheel) — same horizontal window
const vstretchBefore = await readState()
await page.screenshot({ path: `${outDir}/vstretch-before.png` })

await page.keyboard.down('Shift')
for (let i = 0; i < 8; i += 1) {
  await page.mouse.move(cx, cy)
  await page.mouse.wheel(0, -120)
  await page.waitForTimeout(120)
}
await page.keyboard.up('Shift')
await page.waitForTimeout(500)
await page.waitForFunction(
  () => {
    const stretch = window.__COT_WS_CAMERA__?.getVerticalStretch?.() ?? 1
    if (stretch <= 1.5) return false
    const m = window.__COT_WS_CAMERA__?.getVerticalMetrics?.()
    const beforeSpan = 0.20932123753072143
    const afterSpan = m?.price?.visiblePriceSpan ?? beforeSpan
    return afterSpan < beforeSpan * 0.85
  },
  { timeout: 5000 },
)
const vstretchAfter = await readState()
await page.screenshot({ path: `${outDir}/vstretch-after.png` })

// Horizontal still works after vertical stretch
for (let i = 0; i < 3; i += 1) {
  await page.mouse.move(cx, cy)
  await page.mouse.wheel(0, -120)
  await page.waitForTimeout(120)
}
await page.waitForTimeout(400)
const deep = await readState()
await page.screenshot({ path: `${outDir}/deep-zoom.png` })

// Home resets vertical + horizontal
await page.evaluate(() => window.__COT_WS_CAMERA__?.goHome?.())
await page.waitForTimeout(500)
const home = await readState()
await page.screenshot({ path: `${outDir}/home-reset.png` })

console.log(
  JSON.stringify(
    {
      before,
      zoomed,
      panned,
      vstretchBefore,
      vstretchAfter,
      deep,
      home,
      spacingIncreased: zoomed.cam?.barSpacing > before.cam?.barSpacing * 2,
      panKeptSpacing: Math.abs(panned.cam?.barSpacing - zoomed.cam?.barSpacing) < 1,
      verticalStretchIncreased: vstretchAfter.verticalStretch > vstretchBefore.verticalStretch * 1.5,
      horizontalUnchangedByVertical:
        Math.abs(vstretchAfter.cam?.barSpacing - vstretchBefore.cam?.barSpacing) < 1 &&
        Math.abs(vstretchAfter.cam?.rightOffset - vstretchBefore.cam?.rightOffset) < 1,
      priceSpanPxIncreased:
        (vstretchBefore.verticalMetrics?.price?.visiblePriceSpan ?? Infinity) >
        (vstretchAfter.verticalMetrics?.price?.visiblePriceSpan ?? 0) * 1.3,
      commercialSpanPxIncreased:
        (vstretchBefore.verticalMetrics?.commercial?.visiblePriceSpan ?? Infinity) >
        (vstretchAfter.verticalMetrics?.commercial?.visiblePriceSpan ?? 0) * 1.3,
      aligned:
        zoomed.priceLogical &&
        zoomed.commercialLogical &&
        Math.abs(zoomed.priceLogical.from - zoomed.commercialLogical.from) < 2,
      homeResetsVertical: home.verticalStretch <= 1.01,
      homeResetsHorizontal: home.cam?.barSpacing < zoomed.cam?.barSpacing,
    },
    null,
    2,
  ),
)
await browser.close()

/**
 * Manual interaction contract — checks gesture outcomes, not just range equality.
 * Run with dev server: npm run dev -- --host 127.0.0.1 --port 5173
 */
import { chromium } from 'playwright'

const baseUrl = process.env.DEV_URL || 'http://127.0.0.1:5173'
const market = process.env.MARKET || 'Sugar'

const browser = await chromium.launch({ headless: true })
const page = await browser.newPage({ viewport: { width: 1440, height: 900 } })
const pageErrors = []
page.on('pageerror', (e) => pageErrors.push(e.message))

const readState = () =>
  page.evaluate(() => {
    const cam = window.__COT_WS_CAMERA__?.getCamera?.() ?? null
    const bounds = window.__COT_WS_CAMERA__?.getTimelineBounds?.() ?? null
    const panes = window.__COT_WS_CAMERA__?.getPaneRanges?.() ?? null
    const commercial = panes?.commercial
    return {
      cam,
      bounds,
      barSpacing: commercial?.scale?.barSpacing ?? null,
      logical: commercial?.range ?? null,
      paneCount: panes ? Object.keys(panes).length : 0,
    }
  })

const cameraMoved = (before, after) => {
  if (!before?.cam || !after?.cam) return false
  if (before.cam.logicalFrom != null && after.cam.logicalFrom != null) {
    return Math.abs(before.cam.logicalFrom - after.cam.logicalFrom) > 0.05
  }
  return before.cam.timeFrom !== after.cam.timeFrom
}

const rangesMatch = (panes) => {
  const entries = Object.values(panes || {}).filter(Boolean)
  if (entries.length < 4) return false
  const first = entries[0]?.range
  if (!first) return false
  return entries.every((p) => p?.range && p.range.from === first.from && p.range.to === first.to)
}

await page.goto(`${baseUrl}/#/instrument/${market}/cot-workstation`, {
  waitUntil: 'domcontentloaded',
  timeout: 30000,
})
await page.waitForFunction(
  () => document.querySelector('.cot-workstation')?.dataset?.chartsReady === '1',
  { timeout: 25000 },
)
await page.waitForTimeout(600)

const initial = await readState()
if (initial.paneCount < 4) throw new Error(`Charts not ready for ${market}`)

const priceCanvas = page.locator('[data-panel="price"] canvas').first()
const commercialCanvas = page.locator('[data-panel="commercial"] canvas').first()
const pb = await priceCanvas.boundingBox()
const cb = await commercialCanvas.boundingBox()
if (!pb || !cb) throw new Error('canvas not found')

// 1. Wheel zoom should stretch timeline (barSpacing up, visible span down)
const zcx = pb.x + pb.width * 0.55
const zcy = pb.y + pb.height * 0.5
for (let i = 0; i < 10; i += 1) {
  await page.mouse.move(zcx, zcy)
  await page.mouse.wheel(0, -120)
  await page.waitForTimeout(50)
}
await page.waitForTimeout(400)
const afterZoom = await readState()
const zoomStretch =
  afterZoom.barSpacing != null &&
  initial.barSpacing != null &&
  afterZoom.barSpacing > initial.barSpacing * 1.15
const zoomNarrows =
  afterZoom.cam &&
  initial.cam &&
  afterZoom.cam.timeTo - afterZoom.cam.timeFrom < initial.cam.timeTo - initial.cam.timeFrom

// 2. Drag on commercial panel pans entire surface (drag right → earlier history)
const pcx = cb.x + cb.width * 0.5
const pcy = cb.y + cb.height * 0.5
await page.mouse.move(pcx, pcy)
await page.mouse.down()
for (let i = 0; i < 24; i += 1) {
  await page.mouse.move(pcx + i * 18, pcy)
  await page.waitForTimeout(24)
}
await page.mouse.up()
await page.waitForTimeout(400)
const afterPan = await readState()
const panMoved = cameraMoved(afterZoom, afterPan)
const panSynced = await page.evaluate(() => {
  const panes = window.__COT_WS_CAMERA__?.getPaneRanges?.()
  const entries = Object.values(panes || {}).filter(Boolean)
  if (entries.length < 4) return false
  const first = entries[0]?.range
  return entries.every((p) => p?.range?.from === first?.from && p?.range?.to === first?.to)
})

// 3. All restores complete history
await page.click('.cot-ws-range-btn--fit')
await page.waitForTimeout(700)
const afterAll = await readState()
const allFull =
  afterAll.bounds &&
  afterAll.cam &&
  afterAll.cam.timeFrom === afterAll.bounds.first &&
  afterAll.cam.timeTo === afterAll.bounds.last

// 4. Home restores working window (3Y default)
await page.click('.cot-ws-range-btn:not(.cot-ws-range-btn--fit)')
await page.waitForTimeout(700)
const afterHome = await readState()
const homeWindow =
  afterHome.bounds &&
  afterHome.cam &&
  afterHome.cam.timeTo === afterHome.bounds.last &&
  afterHome.cam.timeFrom !== afterHome.bounds.first
const homeNotAll =
  afterHome.cam &&
  afterAll.cam &&
  afterHome.cam.timeFrom !== afterAll.cam.timeFrom

// 5. All again after deep zoom still works
for (let i = 0; i < 12; i += 1) {
  await page.mouse.move(zcx, zcy)
  await page.mouse.wheel(0, -120)
  await page.waitForTimeout(40)
}
await page.waitForTimeout(300)
await page.click('.cot-ws-range-btn--fit')
await page.waitForTimeout(700)
const afterAllAgain = await readState()
const allAgainFull =
  afterAllAgain.bounds &&
  afterAllAgain.cam &&
  afterAllAgain.cam.timeFrom === afterAllAgain.bounds.first &&
  afterAllAgain.cam.timeTo === afterAllAgain.bounds.last

const report = {
  market,
  pageErrors,
  checks: {
    zoomStretch,
    zoomNarrows,
    panMoved,
    panSynced,
    allFull,
    homeWindow,
    homeNotAll,
    allAgainFull,
  },
  initial: { barSpacing: initial.barSpacing, cam: initial.cam },
  afterZoom: { barSpacing: afterZoom.barSpacing, cam: afterZoom.cam },
  afterPan: { cam: afterPan.cam },
  afterAll: { cam: afterAll.cam, bounds: afterAll.bounds },
  afterHome: { cam: afterHome.cam },
  afterAllAgain: { cam: afterAllAgain.cam },
}

console.log(JSON.stringify(report, null, 2))
await browser.close()

const ok =
  !pageErrors.length &&
  zoomStretch &&
  zoomNarrows &&
  panMoved &&
  panSynced &&
  allFull &&
  homeWindow &&
  homeNotAll &&
  allAgainFull

process.exit(ok ? 0 : 1)

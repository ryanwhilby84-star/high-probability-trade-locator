import { chromium } from 'playwright'

const baseUrl = process.env.DEV_URL || 'http://127.0.0.1:5173'
const market = process.env.MARKET || 'Sugar'

function rangesMatch(panes) {
  const entries = Object.values(panes || {}).filter(Boolean)
  if (entries.length < 4) return false
  const first = entries[0]?.range
  if (!first) return false
  return entries.every(
    (p) => p?.range && p.range.from === first.from && p.range.to === first.to,
  )
}

const browser = await chromium.launch({ headless: true })
const page = await browser.newPage({ viewport: { width: 1440, height: 900 } })
const pageErrors = []
page.on('pageerror', (e) => pageErrors.push(e.message))

await page.goto(`${baseUrl}/#/instrument/${market}/cot-workstation`, {
  waitUntil: 'domcontentloaded',
  timeout: 30000,
})
await page.waitForFunction(
  () => document.querySelector('.cot-workstation')?.dataset?.chartsReady === '1',
  { timeout: 25000 },
)
await page.waitForTimeout(800)

const readPanes = () => page.evaluate(() => window.__COT_WS_CAMERA__?.getPaneRanges?.() ?? null)
const readCamera = () => page.evaluate(() => window.__COT_WS_CAMERA__?.getCamera?.() ?? null)
const timelineBounds = () =>
  page.evaluate(() => {
    const rows = window.__COT_WS_CAMERA__?.getCamera?.()
    return null
  })

const bounds = await page.evaluate(() => {
  const panes = window.__COT_WS_CAMERA__?.getPaneRanges?.()
  const cam = window.__COT_WS_CAMERA__?.getCamera?.()
  return { panes, cam }
})

const initial = await readPanes()
if (!initial || Object.keys(initial).length < 4) {
  throw new Error(`Charts not ready for ${market}`)
}

const initialMeta = await page.evaluate(() => {
  const cam = window.__COT_WS_CAMERA__?.getCamera?.()
  const panes = window.__COT_WS_CAMERA__?.getPaneRanges?.()
  return {
    camera: cam,
    commercial: panes?.commercial?.range ?? null,
    price: panes?.price?.range ?? null,
    weeksLabel: document.querySelector('.cot-ws-weeks')?.textContent?.trim() ?? null,
    rangeNote: document.querySelector('.cot-ws-range-note')?.textContent?.trim() ?? null,
    diag: document.querySelector('.cot-ws-diag')?.textContent?.trim() ?? null,
    hasError: Boolean(document.querySelector('.cot-ws-status--error')),
    panelCount: document.querySelectorAll('.cot-ws-panel').length,
    canvasReady: document.querySelectorAll('.cot-ws-chart-canvas--ready').length,
  }
})

const initialSynced = rangesMatch(initial)
const initial3yAligned =
  initialMeta.price?.from === initialMeta.commercial?.from &&
  initialMeta.price?.to === initialMeta.commercial?.to

const priceCanvas = page.locator('[data-panel="price"] canvas').first()
const commercialCanvas = page.locator('[data-panel="commercial"] canvas').first()
const pb = await priceCanvas.boundingBox()
const cb = await commercialCanvas.boundingBox()
if (!pb || !cb) throw new Error('canvas not found')

const zcx = pb.x + pb.width * 0.55
const zcy = pb.y + pb.height * 0.5
for (let i = 0; i < 8; i += 1) {
  await page.mouse.move(zcx, zcy)
  await page.mouse.wheel(0, -140)
  await page.waitForTimeout(70)
}
await page.waitForTimeout(350)
const afterZoom = await readPanes()
const zoomSynced = rangesMatch(afterZoom)

const pcx = cb.x + cb.width * 0.55
const pcy = cb.y + cb.height * 0.5
await page.mouse.move(pcx, pcy)
await page.mouse.down()
for (let i = 0; i < 28; i += 1) {
  await page.mouse.move(pcx - i * 22, pcy)
  await page.waitForTimeout(28)
}
await page.mouse.up()
await page.waitForTimeout(450)
const afterPan = await readPanes()
const panSynced = rangesMatch(afterPan)

await page.click('.cot-ws-range-btn--fit')
await page.waitForTimeout(800)
const afterAll = await readPanes()
const camera = await readCamera()
const fitSynced = rangesMatch(afterAll)

const rowSpan = await page.evaluate(() => {
  const cam = window.__COT_WS_CAMERA__?.getCamera?.()
  const panes = window.__COT_WS_CAMERA__?.getPaneRanges?.()
  const commercial = panes?.commercial?.range
  if (!cam || !commercial) return null
  return { cam, commercial }
})

const fitFull = await page.evaluate(() => {
  const cam = window.__COT_WS_CAMERA__?.getCamera?.()
  const panes = window.__COT_WS_CAMERA__?.getPaneRanges?.()
  if (!cam || !panes?.commercial?.range) return false
  const first = panes.commercial.range.from
  const last = panes.commercial.range.to
  return cam.timeFrom === first && cam.timeTo === last
})

const allFull = await page.evaluate(() => {
  const cam = window.__COT_WS_CAMERA__?.getCamera?.()
  const panes = window.__COT_WS_CAMERA__?.getPaneRanges?.()
  if (!cam || !panes?.price?.range || !panes?.commercial?.range) return false
  const ranges = Object.values(panes).map((p) => p.range)
  const froms = ranges.map((r) => r.from)
  const tos = ranges.map((r) => r.to)
  const minFrom = Math.min(...froms)
  const maxTo = Math.max(...tos)
  const span = maxTo - minFrom
  const commercialSpan = panes.commercial.range.to - panes.commercial.range.from
  return (
    cam.timeFrom === panes.commercial.range.from &&
    cam.timeTo === panes.commercial.range.to &&
    panes.price.range.from === panes.commercial.range.from &&
    panes.price.range.to === panes.commercial.range.to &&
    commercialSpan > span * 0.85
  )
})

const report = {
  market,
  pageErrors,
  zoomSynced,
  panSynced,
  fitSynced,
  allFull,
  afterZoom,
  afterPan,
  afterAll,
  camera,
  rowSpan,
}

console.log(JSON.stringify(report, null, 2))
await browser.close()

const ok =
  !pageErrors.length &&
  zoomSynced &&
  panSynced &&
  fitSynced &&
  allFull &&
  Object.keys(initial).length >= 4

process.exit(ok ? 0 : 1)

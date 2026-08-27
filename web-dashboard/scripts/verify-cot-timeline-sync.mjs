import { chromium } from 'playwright'

const baseUrl = process.env.DEV_URL || 'http://127.0.0.1:5173'
const market = process.env.MARKET || 'Sugar'

function rangesMatch(panes) {
  const entries = Object.values(panes).filter(Boolean)
  if (entries.length < 4) return false
  const [first, ...rest] = entries
  const eps = 0.05
  return rest.every(
    (p) =>
      Math.abs(p.range.from - first.range.from) < eps &&
      Math.abs(p.range.to - first.range.to) < eps,
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
await page.waitForTimeout(4000)

const readPanes = () => page.evaluate(() => window.__COT_WS_TIMELINE__?.getPaneRanges?.() ?? null)

const initial = await readPanes()
if (!initial || Object.keys(initial).length < 4) {
  const snap = await page.evaluate(() => ({
    chartsReady: document.querySelector('.cot-workstation')?.getAttribute('data-charts-ready'),
    hasTimeline: Boolean(window.__COT_WS_TIMELINE__),
    panels: document.querySelectorAll('.cot-ws-panel').length,
  }))
  throw new Error(`Charts not ready: ${JSON.stringify(snap)}`)
}
const priceCanvas = page.locator('[data-panel="price"] canvas').first()
const commercialCanvas = page.locator('[data-panel="commercial"] canvas').first()
const box = await priceCanvas.boundingBox()
const panBox = (await commercialCanvas.boundingBox()) ?? box
if (!box) throw new Error('price canvas not found')

const cx = box.x + box.width * 0.55
const cy = box.y + box.height * 0.5

// Zoom in at cursor (wheel up = zoom in on LWC time scale)
for (let i = 0; i < 6; i += 1) {
  await page.mouse.move(cx, cy)
  await page.mouse.wheel(0, -120)
  await page.waitForTimeout(80)
}
await page.waitForTimeout(400)

const afterZoom = await readPanes()
const zoomSynced = rangesMatch(afterZoom)

// Pan left through history (drag on commercial — full COT timeline)
const pcx = panBox.x + panBox.width * 0.55
const pcy = panBox.y + panBox.height * 0.5
await page.mouse.move(pcx, pcy)
await page.mouse.down()
for (let i = 0; i < 24; i += 1) {
  await page.mouse.move(pcx - i * 18, pcy)
  await page.waitForTimeout(30)
}
await page.mouse.up()
await page.waitForTimeout(400)

const afterPan = await readPanes()
const panSynced = rangesMatch(afterPan)

// Fit all (API + button — API avoids flaky toolbar hit targets)
await page.evaluate(() => window.__COT_WS_TIMELINE__?.fitAll?.())
await page.waitForTimeout(400)
await page.click('.cot-ws-range-btn--fit')
await page.waitForTimeout(600)

const afterFit = await readPanes()
const master = await page.evaluate(() => window.__COT_WS_TIMELINE__?.getMaster?.() ?? null)
const diag = await page.evaluate(() => window.__COT_WS_DIAG__?.get?.() ?? null)
const fitSynced = rangesMatch(afterFit)
const fitFull =
  master?.logicalRange &&
  Math.abs(master.logicalRange.from) < 0.1 &&
  master.rowCount > 0 &&
  Math.abs(master.logicalRange.to - (master.rowCount - 1)) < 1.5

const report = {
  market,
  pageErrors,
  initialPaneCount: initial ? Object.keys(initial).length : 0,
  zoomSynced,
  panSynced,
  fitSynced,
  fitFull,
  afterZoom,
  afterPan,
  afterFit,
  master,
  diag,
}

console.log(JSON.stringify(report, null, 2))
await browser.close()

const ok =
  !pageErrors.length &&
  zoomSynced &&
  panSynced &&
  fitSynced &&
  fitFull &&
  initial &&
  Object.keys(initial).length >= 4

process.exit(ok ? 0 : 1)

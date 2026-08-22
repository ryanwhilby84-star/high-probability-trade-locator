import { chromium } from 'playwright'
import fs from 'fs'
import path from 'path'

const outDir = path.resolve('../data/audits')
fs.mkdirSync(outDir, { recursive: true })

const markets = [
  { id: 'NZ Dollar / 6N', file: 'nzd' },
  { id: 'Corn', file: 'corn' },
]

const browser = await chromium.launch({ headless: true })
const page = await browser.newPage({ viewport: { width: 1600, height: 1100 } })
const results = []

async function waitReady(timeoutMs = 90000) {
  const start = Date.now()
  while (Date.now() - start < timeoutMs) {
    const ready = await page.evaluate(
      () => document.querySelector('.cot-workstation')?.dataset?.chartsReady === '1',
    )
    if (ready) return true
    await page.waitForTimeout(400)
  }
  return false
}

for (const m of markets) {
  const url = `http://127.0.0.1:5173/#/instrument/${encodeURIComponent(m.id)}/cot-workstation`
  await page.goto(url, { waitUntil: 'commit', timeout: 120000 })
  await page.waitForSelector('.cot-workstation', { timeout: 60000 })
  if (!(await waitReady())) {
    console.error('not ready', m.id)
    await browser.close()
    process.exit(1)
  }
  await page.waitForTimeout(1200)

  const closedShot = path.join(outDir, `weekly_inspector_drawer_closed_${m.file}.png`)
  await page.screenshot({ path: closedShot, fullPage: false })

  const closed = await page.evaluate(() => {
    const ws = document.querySelector('.cot-workstation')
    const insp = document.querySelector('.cot-ws-weekly-inspector')
    const stage = document.querySelector('.cot-ws-research-stage')
    return {
      inspectorOpenAttr: ws?.dataset?.inspectorOpen || '0',
      inspectorInDom: Boolean(insp),
      stageHeight: stage ? Math.round(stage.getBoundingClientRect().height) : null,
    }
  })

  const commercial = page.locator('[data-panel="commercial"] .cot-ws-chart-canvas').first()
  const box = await commercial.boundingBox()
  await page.mouse.move(box.x + box.width * 0.7, box.y + box.height * 0.4)
  await page.waitForTimeout(300)
  const hoverTip = (await page.locator('.cot-ws-week-hover-chip').count()) > 0
  const hoverOpenedInspector = await page.evaluate(() =>
    Boolean(document.querySelector('.cot-ws-weekly-inspector')),
  )

  await page.mouse.click(box.x + box.width * 0.7, box.y + box.height * 0.4)
  await page.waitForTimeout(450)

  const openShot = path.join(outDir, `weekly_inspector_drawer_open_${m.file}.png`)
  await page.screenshot({ path: openShot, fullPage: false })

  const opened = await page.evaluate(() => {
    const ws = document.querySelector('.cot-workstation')
    const insp = document.querySelector('.cot-ws-weekly-inspector')
    const stage = document.querySelector('.cot-ws-research-stage')
    const ir = insp?.getBoundingClientRect()
    const sr = stage?.getBoundingClientRect()
    return {
      inspectorOpenAttr: ws?.dataset?.inspectorOpen || '0',
      isDrawer: insp?.classList.contains('cot-ws-weekly-inspector--drawer') || false,
      aboveCharts: Boolean(ir && sr && ir.bottom <= sr.top + 2),
      stageHeight: stage ? Math.round(sr.height) : null,
    }
  })

  await page.getByRole('button', { name: 'Close' }).click()
  await page.waitForTimeout(350)
  const afterClose = await page.evaluate(() => {
    const ws = document.querySelector('.cot-workstation')
    const insp = document.querySelector('.cot-ws-weekly-inspector')
    const stage = document.querySelector('.cot-ws-research-stage')
    return {
      inspectorOpenAttr: ws?.dataset?.inspectorOpen || '0',
      inspectorInDom: Boolean(insp),
      stageHeight: stage ? Math.round(stage.getBoundingClientRect().height) : null,
    }
  })

  results.push({
    market: m.id,
    closed,
    hoverTip,
    hoverOpenedInspector,
    opened,
    afterClose,
    closedShot,
    openShot,
    stageGrewOnClose:
      afterClose.stageHeight != null &&
      opened.stageHeight != null &&
      afterClose.stageHeight >= opened.stageHeight,
  })
}

fs.writeFileSync(
  path.join(outDir, 'weekly_inspector_drawer_proof.json'),
  JSON.stringify(results, null, 2),
)
console.log(JSON.stringify(results, null, 2))

const ok = results.every(
  (r) =>
    r.closed.inspectorOpenAttr === '0' &&
    !r.closed.inspectorInDom &&
    r.hoverTip &&
    !r.hoverOpenedInspector &&
    r.opened.inspectorOpenAttr === '1' &&
    r.opened.isDrawer &&
    r.opened.aboveCharts &&
    r.afterClose.inspectorOpenAttr === '0' &&
    !r.afterClose.inspectorInDom &&
    r.stageGrewOnClose,
)

await browser.close()
process.exit(ok ? 0 : 1)

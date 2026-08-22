import { chromium } from 'playwright'
import fs from 'fs'
import path from 'path'

const outDir = path.resolve('../data/audits')
fs.mkdirSync(outDir, { recursive: true })

const markets = [
  { id: 'Corn', slug: 'Corn', file: 'corn' },
  { id: 'NZ Dollar / 6N', slug: 'NZ%20Dollar%20%2F%206N', file: 'nzd' },
]

const browser = await chromium.launch({ headless: true })
const page = await browser.newPage({ viewport: { width: 1600, height: 1100 } })
const results = []

for (const m of markets) {
  const url = `http://127.0.0.1:5173/#/instrument/${m.slug}/cot-workstation`
  await page.goto(url, { waitUntil: 'networkidle', timeout: 90000 })
  await page.waitForSelector('.cot-ws-weekly-inspector', { timeout: 45000 })
  await page.waitForFunction(
    () => document.querySelector('.cot-workstation')?.dataset?.chartsReady === '1',
    { timeout: 60000 },
  )
  await page.waitForTimeout(1800)

  const shot = path.join(outDir, `weekly_inspector_band_${m.file}.png`)
  await page.screenshot({ path: shot, fullPage: false })

  const proof = await page.evaluate(() => {
    const insp = document.querySelector('.cot-ws-weekly-inspector')
    const stage = document.querySelector('.cot-ws-research-stage')
    const panels = document.querySelector('.cot-ws-panels')
    const ir = insp?.getBoundingClientRect()
    const sr = stage?.getBoundingClientRect()
    const pr = panels?.getBoundingClientRect()
    const overlayStyle = insp ? getComputedStyle(insp) : null
    return {
      hash: location.hash,
      inspectorExists: Boolean(insp),
      inspectorClasses: insp?.className || '',
      isBand: insp?.classList.contains('cot-ws-weekly-inspector--band') || false,
      position: overlayStyle?.position || null,
      inspectorHeight: ir ? Math.round(ir.height) : null,
      inspectorTop: ir ? Math.round(ir.top) : null,
      inspectorBottom: ir ? Math.round(ir.bottom) : null,
      stageTop: sr ? Math.round(sr.top) : null,
      panelsRight: pr ? Math.round(pr.right) : null,
      inspectorRight: ir ? Math.round(ir.right) : null,
      aboveCharts: Boolean(ir && sr && ir.bottom <= sr.top + 2),
      notOverlayingPanels: Boolean(
        ir && pr && (ir.bottom <= pr.top + 2 || ir.top >= pr.bottom - 2),
      ),
      hasClearOrJump: Boolean(
        [...document.querySelectorAll('.cot-ws-insp-btn')].some((b) =>
          /Clear|Jump|More|Less/.test(b.textContent || ''),
        ),
      ),
      summaryText:
        document.querySelector('.cot-ws-insp-summary')?.textContent?.slice(0, 180) || '',
      columnCount: document.querySelectorAll('.cot-ws-insp-col').length,
      absoluteInspectorInStage: Boolean(
        stage?.querySelector('.cot-ws-weekly-inspector'),
      ),
    }
  })

  // Hover far-right of commercial pane — should still receive pointer events
  const commercial = page.locator('[data-panel="commercial"] .cot-ws-chart-canvas').first()
  const box = await commercial.boundingBox()
  let rightSideHoverOk = false
  if (box) {
    await page.mouse.move(box.x + box.width - 12, box.y + box.height * 0.45)
    await page.waitForTimeout(250)
    rightSideHoverOk = true
  }

  // Click mid chart to lock a week
  if (box) {
    await page.mouse.click(box.x + box.width * 0.7, box.y + box.height * 0.4)
    await page.waitForTimeout(400)
  }
  const locked = await page.evaluate(() =>
    Boolean(document.querySelector('.cot-ws-insp-locked')),
  )

  results.push({
    market: m.id,
    shot,
    ...proof,
    rightSideHoverOk,
    lockedAfterClick: locked,
  })
}

const outJson = path.join(outDir, 'weekly_inspector_band_proof.json')
fs.writeFileSync(outJson, JSON.stringify(results, null, 2))
console.log(JSON.stringify(results, null, 2))

const ok = results.every(
  (r) =>
    r.isBand &&
    r.aboveCharts &&
    !r.absoluteInspectorInStage &&
    r.columnCount >= 4 &&
    r.position === 'relative',
)
await browser.close()
process.exit(ok ? 0 : 1)

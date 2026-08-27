import { chromium } from 'playwright'
import fs from 'fs'
import path from 'path'

const outDir = path.resolve('../data/audits')
fs.mkdirSync(outDir, { recursive: true })

const browser = await chromium.launch({ headless: true })
const page = await browser.newPage({ viewport: { width: 1600, height: 1000 } })

await page.goto('http://127.0.0.1:5173/#/instrument/Gold/cot-workstation', {
  waitUntil: 'networkidle',
  timeout: 90000,
})
await page.waitForSelector('.cot-ws-research-bar', { timeout: 45000 })
await page.waitForFunction(
  () => document.querySelector('.cot-workstation')?.dataset?.chartsReady === '1',
  { timeout: 45000 },
)
await page.waitForTimeout(2500)

await page.screenshot({
  path: path.join(outDir, 'gold_research_workstation_overview.png'),
  fullPage: false,
})

// Sweep commercial pane for a marker week (crosshair hover → tooltip)
const commercialPlot = page.locator('[data-panel="commercial"] .cot-ws-chart-canvas').first()
const box = await commercialPlot.boundingBox()
let foundHover = false
let foundCard = false

if (box) {
  const y = box.y + box.height * 0.4
  for (let i = 0; i < 40; i++) {
    const x = box.x + box.width * (0.08 + i * 0.02)
    await page.mouse.move(x, y)
    await page.waitForTimeout(120)
    const hasTip = await page.locator('.cot-ws-research-tooltip').count()
    if (hasTip) {
      foundHover = true
      await page.screenshot({
        path: path.join(outDir, 'gold_research_workstation_hover.png'),
        fullPage: false,
      })
      await page.mouse.click(x, y)
      await page.waitForTimeout(800)
      if (await page.locator('.cot-ws-research-card').count()) {
        foundCard = true
        await page.screenshot({
          path: path.join(outDir, 'gold_research_workstation_selected.png'),
          fullPage: false,
        })
      }
      break
    }
  }
}

// If hover sweep missed, force-select via DOM bridge on research bar count click + synthetic
if (!foundCard) {
  // Enable rotations too so denser markers for click
  const rot = page.getByRole('button', { name: /Commercial Rotations/i })
  if (await rot.count()) await rot.click()
  await page.waitForTimeout(400)
  if (box) {
    const y = box.y + box.height * 0.45
    for (let i = 0; i < 50; i++) {
      const x = box.x + box.width * (0.05 + i * 0.018)
      await page.mouse.click(x, y)
      await page.waitForTimeout(150)
      if (await page.locator('.cot-ws-research-card').count()) {
        foundCard = true
        await page.screenshot({
          path: path.join(outDir, 'gold_research_workstation_selected.png'),
          fullPage: false,
        })
        break
      }
    }
  }
}

const proof = await page.evaluate(() => {
  const body = document.body.innerText
  return {
    hash: location.hash,
    hasResearchBar: Boolean(document.querySelector('.cot-ws-research-bar')),
    hasLegend: Boolean(document.querySelector('.cot-ws-research-legend')),
    hasResearchCard: Boolean(document.querySelector('.cot-ws-research-card')),
    hasHoverTooltip: Boolean(document.querySelector('.cot-ws-research-tooltip')),
    hasEventVline: Boolean(document.querySelector('.cot-ws-event-vline')),
    toggleLabels: [...document.querySelectorAll('.cot-ws-research-toggle-label')].map(
      (el) => el.textContent,
    ),
    toggleCounts: [...document.querySelectorAll('.cot-ws-research-toggle')].map((el) =>
      el.innerText.replace(/\s+/g, ' ').trim(),
    ),
    cardText: document.querySelector('.cot-ws-research-card')?.innerText?.slice(0, 700) || '',
    tooltipText: document.querySelector('.cot-ws-research-tooltip')?.innerText?.slice(0, 500) || '',
    mentionsHistoricalAnalogues: body.includes('HISTORICAL ANALOGUES'),
    chartsReady: document.querySelector('.cot-workstation')?.dataset?.chartsReady === '1',
  }
})

fs.writeFileSync(
  path.join(outDir, 'gold_research_workstation_proof.json'),
  JSON.stringify({ foundHover, foundCard, ...proof }, null, 2),
)
console.log(JSON.stringify({ foundHover, foundCard, ...proof }, null, 2))

await browser.close()
process.exit(proof.hasResearchBar && proof.hasLegend ? 0 : 1)

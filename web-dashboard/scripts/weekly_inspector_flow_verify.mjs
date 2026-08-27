import { chromium } from 'playwright'
import fs from 'fs'
import path from 'path'

const outDir = path.resolve('../data/audits')
fs.mkdirSync(outDir, { recursive: true })

const browser = await chromium.launch({ headless: true })
const page = await browser.newPage({ viewport: { width: 1600, height: 1100 } })

const url = `http://127.0.0.1:5173/#/instrument/${encodeURIComponent('NZ Dollar / 6N')}/cot-workstation`
await page.goto(url, { waitUntil: 'commit', timeout: 120000 })
await page.waitForSelector('.cot-workstation', { timeout: 60000 })
for (let i = 0; i < 90; i += 1) {
  const ready = await page.evaluate(
    () => document.querySelector('.cot-workstation')?.dataset?.chartsReady === '1',
  )
  if (ready) break
  await page.waitForTimeout(400)
}

const commercial = page.locator('[data-panel="commercial"] .cot-ws-chart-canvas').first()
const box = await commercial.boundingBox()
await page.mouse.click(box.x + box.width * 0.85, box.y + box.height * 0.4)
await page.waitForTimeout(500)

const shot = path.join(outDir, 'weekly_inspector_flow_nzd.png')
await page.screenshot({ path: shot, fullPage: false })

const proof = await page.evaluate(() => {
  const insp = document.querySelector('.cot-ws-weekly-inspector')
  const text = insp?.innerText || ''
  const unavailable = (text.match(/Unavailable/g) || []).length
  const hasFlow = Boolean(document.querySelector('.cot-ws-insp-flow'))
  const hasNetPctile = /Net pctile/i.test(text)
  const hasArrow = /[▲▼→]/.test(text)
  return {
    open: Boolean(insp),
    unavailableCount: unavailable,
    hasFlow,
    hasNetPctile,
    hasArrow,
    snippet: text.slice(0, 900),
  }
})

fs.writeFileSync(
  path.join(outDir, 'weekly_inspector_flow_proof.json'),
  JSON.stringify({ ...proof, shot }, null, 2),
)
console.log(JSON.stringify(proof, null, 2))

await browser.close()
process.exit(proof.open && proof.hasFlow && proof.hasNetPctile && proof.unavailableCount < 6 ? 0 : 1)

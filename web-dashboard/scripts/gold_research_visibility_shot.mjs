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
await page.waitForTimeout(3000)

// Ensure extremes + divergence on, rotations/nr off for clean view
const toggles = page.locator('.cot-ws-research-toggle')
const count = await toggles.count()
for (let i = 0; i < count; i++) {
  const t = toggles.nth(i)
  const label = await t.locator('.cot-ws-research-toggle-label').innerText()
  const on = await t.evaluate((el) => el.classList.contains('is-on'))
  const wantOn =
    label.includes('Commercial Extremes') || label.includes('Comm↔NR Divergence')
  if (wantOn !== on) await t.click()
}
await page.waitForTimeout(800)

// Wait for large pins
await page.waitForSelector('.cot-ws-research-pin-badge', { timeout: 15000 })
const pinStats = await page.evaluate(() => {
  const badges = [...document.querySelectorAll('.cot-ws-research-pin-badge')]
  const by = {}
  for (const b of badges) {
    const t = b.textContent.trim()
    by[t] = (by[t] || 0) + 1
  }
  return {
    totalBadges: badges.length,
    byLabel: by,
    extremePins: document.querySelectorAll('.cot-ws-research-pin--extreme').length,
    divPins: document.querySelectorAll('.cot-ws-research-pin--divergence').length,
  }
})

await page.screenshot({
  path: path.join(outDir, 'gold_visibility_overview.png'),
  fullPage: false,
})

// Click a visible EX pin on price pane (center-ish of plot)
const exPins = page.locator(
  '[data-panel="price"] .cot-ws-research-pin--extreme',
)
const exCount = await exPins.count()
let clicked = false
for (let i = 0; i < exCount; i++) {
  const pin = exPins.nth(i)
  const box = await pin.boundingBox()
  if (!box || box.x < 80 || box.x > 1500) continue
  await pin.click({ force: true })
  clicked = true
  await page.waitForTimeout(1000)
  break
}
if (!clicked && exCount) {
  await exPins.nth(Math.min(2, exCount - 1)).click({ force: true })
  await page.waitForTimeout(1000)
}

await page.screenshot({
  path: path.join(outDir, 'gold_visibility_selected_extreme.png'),
  fullPage: false,
})

// Click a visible DIV pin
const divPins = page.locator(
  '[data-panel="price"] .cot-ws-research-pin--divergence',
)
const divCount = await divPins.count()
let divClicked = false
for (let i = 0; i < divCount; i++) {
  const pin = divPins.nth(i)
  const box = await pin.boundingBox()
  if (!box || box.x < 80 || box.x > 1500) continue
  await pin.click({ force: true })
  divClicked = true
  await page.waitForTimeout(1000)
  break
}
if (!divClicked && divCount) {
  await divPins.nth(Math.min(1, divCount - 1)).click({ force: true })
  await page.waitForTimeout(1000)
}

await page.screenshot({
  path: path.join(outDir, 'gold_visibility_selected_divergence.png'),
  fullPage: false,
})

const proof = await page.evaluate(() => {
  const card = document.querySelector('.cot-ws-research-card')
  const badges = [...document.querySelectorAll('.cot-ws-research-pin-badge')].map((b) =>
    b.textContent.trim(),
  )
  return {
    pinBadgeCount: badges.length,
    exCount: badges.filter((b) => b === 'EX').length,
    divCount: badges.filter((b) => b === 'DIV').length,
    hasCard: Boolean(card),
    cardText: card?.innerText?.slice(0, 800) || '',
    hasEventVline: Boolean(document.querySelector('.cot-ws-event-vline')),
    eventVlineVisible:
      document.querySelector('.cot-ws-event-vline')?.style?.display !== 'none',
  }
})

fs.writeFileSync(
  path.join(outDir, 'gold_visibility_proof.json'),
  JSON.stringify({ pinStats, ...proof }, null, 2),
)
console.log(JSON.stringify({ pinStats, ...proof }, null, 2))

await browser.close()

const ok =
  pinStats.extremePins >= 3 &&
  pinStats.divPins >= 2 &&
  proof.hasCard &&
  proof.cardText.includes('HISTORICAL ANALOGUES')
process.exit(ok ? 0 : 1)

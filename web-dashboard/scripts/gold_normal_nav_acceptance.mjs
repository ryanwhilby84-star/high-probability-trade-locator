/**
 * Acceptance: normal dashboard navigation on localhost:5173.
 * Does NOT force research toggles. Does NOT deep-link past Open COT Workstation.
 */
import { chromium } from 'playwright'
import fs from 'fs'
import path from 'path'

const outDir = path.resolve('../data/audits')
fs.mkdirSync(outDir, { recursive: true })

const BASE = 'http://127.0.0.1:5173/'
const browser = await chromium.launch({ headless: true })
const page = await browser.newPage({ viewport: { width: 1600, height: 1000 } })

const forcedToggle = false // explicit: this script must never force layer toggles

// 1) Dashboard / scanner entry
await page.goto(BASE, { waitUntil: 'networkidle', timeout: 90000 })
await page.waitForTimeout(1500)

// 2) Select Gold (normal instrument open)
await page.goto(`${BASE}#/instrument/Gold`, {
  waitUntil: 'networkidle',
  timeout: 90000,
})
await page.waitForTimeout(2000)

// 3) Click Open COT Workstation (user path)
const openBtn = page.getByRole('button', { name: /Open COT Workstation/i })
await openBtn.waitFor({ state: 'visible', timeout: 30000 })
await openBtn.click()

await page.waitForFunction(
  () => location.hash.includes('/cot-workstation'),
  { timeout: 15000 },
)
await page.waitForSelector('.cot-workstation', { timeout: 30000 })
await page.waitForFunction(
  () => document.querySelector('.cot-workstation')?.dataset?.chartsReady === '1',
  { timeout: 60000 },
)

// Wait for research layer from normal load (defaults only — no toggle clicks)
await page.waitForSelector('.cot-ws-research-bar', { timeout: 30000 })
await page.waitForSelector('.cot-ws-research-pin-badge', { timeout: 30000 })
await page.waitForTimeout(2000)

const afterLoad = await page.evaluate(() => {
  const badges = [...document.querySelectorAll('.cot-ws-research-pin-badge')].map((el) =>
    el.textContent.trim(),
  )
  const toggles = [...document.querySelectorAll('.cot-ws-research-toggle')].map((el) => ({
    text: el.innerText.replace(/\s+/g, ' ').trim(),
    on: el.classList.contains('is-on'),
  }))
  return {
    hash: location.hash,
    href: location.href,
    badges,
    ex: badges.filter((b) => b === 'EX').length,
    div: badges.filter((b) => b === 'DIV').length,
    toggles,
    hasCard: Boolean(document.querySelector('.cot-ws-research-card')),
  }
})

await page.screenshot({
  path: path.join(outDir, 'gold_normal_nav_load.png'),
  fullPage: false,
})

// 4) Click one EX event (no toggle forcing)
const exPin = page.locator('[data-panel="price"] .cot-ws-research-pin--extreme').first()
await exPin.waitFor({ state: 'visible', timeout: 15000 })
await exPin.click()
await page.waitForSelector('.cot-ws-research-card', { timeout: 10000 })
await page.waitForTimeout(800)

const afterEx = await page.evaluate(() => {
  const card = document.querySelector('.cot-ws-research-card')
  return {
    cardText: card?.innerText?.slice(0, 900) || '',
    hasCard: Boolean(card),
  }
})
await page.screenshot({
  path: path.join(outDir, 'gold_normal_nav_ex_click.png'),
  fullPage: false,
})

// 5) Close the EX card (normal UI), stay on default layers, click a DIV pin.
const closeCard = page.locator('.cot-ws-research-card-close')
if (await closeCard.count()) {
  await closeCard.click()
  await page.waitForTimeout(400)
}
await page.getByRole('button', { name: 'Home', exact: true }).click()
await page.waitForTimeout(1200)

const divPin = page.locator('.cot-ws-research-pin--divergence').first()
await divPin.waitFor({ state: 'visible', timeout: 20000 })
await divPin.click()
await page.waitForTimeout(800)

const afterDiv = await page.evaluate(() => {
  const card = document.querySelector('.cot-ws-research-card')
  return {
    cardText: card?.innerText?.slice(0, 900) || '',
    hasCard: Boolean(card),
  }
})
await page.screenshot({
  path: path.join(outDir, 'gold_normal_nav_div_click.png'),
  fullPage: false,
})

const extremesOn = afterLoad.toggles.some(
  (t) => t.text.includes('Commercial Extremes') && t.on,
)
const divergenceOn = afterLoad.toggles.some(
  (t) => t.text.includes('Comm↔NR Divergence') && t.on,
)

const pass =
  afterLoad.hash.includes('/instrument/Gold/cot-workstation') &&
  afterLoad.ex >= 3 &&
  afterLoad.div >= 1 &&
  extremesOn &&
  divergenceOn &&
  !afterLoad.hasCard &&
  afterEx.hasCard &&
  /HISTORICAL ANALOGUES/i.test(afterEx.cardText) &&
  afterDiv.hasCard &&
  /HISTORICAL ANALOGUES/i.test(afterDiv.cardText) &&
  /DIVERGENCE|DIV/i.test(afterDiv.cardText)

const report = {
  forcedToggle,
  route: afterLoad.hash,
  href: afterLoad.href,
  server: BASE,
  afterLoad,
  afterEx,
  afterDiv,
  pass,
  screenshots: {
    load: 'data/audits/gold_normal_nav_load.png',
    exClick: 'data/audits/gold_normal_nav_ex_click.png',
    divClick: 'data/audits/gold_normal_nav_div_click.png',
  },
}

fs.writeFileSync(
  path.join(outDir, 'gold_normal_nav_acceptance.json'),
  JSON.stringify(report, null, 2),
)
console.log(JSON.stringify(report, null, 2))

await browser.close()
process.exit(pass ? 0 : 1)

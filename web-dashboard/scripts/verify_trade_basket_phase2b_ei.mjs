/**
 * Phase 2B Trade Basket Workstation EI verification.
 */
import { chromium } from 'playwright'
import fs from 'fs'
import path from 'path'
import { fileURLToPath } from 'url'

const __dirname = path.dirname(fileURLToPath(import.meta.url))
const root = path.resolve(__dirname, '../..')
const outDir = path.join(root, 'data', 'audits', 'trade_basket_phase2b')
fs.mkdirSync(outDir, { recursive: true })

const BASE = process.env.HPTL_EI_BASE || 'http://127.0.0.1:5173'
const url = `${BASE}/#/trade-basket`

const proof = { url, passed: false, checks: {}, screenshots: [], consoleErrors: [], errors: [] }

const browser = await chromium.launch({ headless: true })
const page = await browser.newPage({ viewport: { width: 1500, height: 1100 } })
page.on('console', (msg) => {
  if (msg.type() === 'error') proof.consoleErrors.push(msg.text())
})
page.on('pageerror', (err) => proof.consoleErrors.push(String(err)))

try {
  await page.goto(url, { waitUntil: 'domcontentloaded', timeout: 60000 })
  await page.waitForSelector('[data-tbw-phase="2B"]', { timeout: 60000 })
  await page.waitForFunction(
    () => document.querySelector('.tbw-table') || document.querySelector('.tbw-error'),
    null,
    { timeout: 180000 },
  )
  await page.waitForTimeout(800)

  const before = await page.evaluate(() => ({
    cards: document.querySelectorAll('[data-trade-card]').length,
    pairRows: document.querySelectorAll('.tbw-table tbody tr').length,
    summary: document.querySelector('.tbw-summary')?.innerText || '',
    hasBuilder: !!document.querySelector('[aria-label="Basket builder"]'),
    hasControls: !!document.querySelector('[aria-label="Basket controls"]'),
    hasPairs: !!document.querySelector('[aria-label="Pairwise relationships"]'),
  }))

  await page.screenshot({ path: path.join(outDir, '01_default_gold_silver.png'), fullPage: false })
  proof.screenshots.push('data/audits/trade_basket_phase2b/01_default_gold_silver.png')

  // Flip Silver to LONG — adjusted sign should become positive (same as raw)
  const shortBtn = page.locator('[data-trade-card="2"] button', { hasText: 'SHORT' })
  const longBtn = page.locator('[data-trade-card="2"] button', { hasText: 'LONG' })
  await longBtn.click()
  await page.waitForTimeout(1500)
  await page.waitForFunction(
    () => !document.body.innerText.includes('Calculating…'),
    null,
    { timeout: 120000 },
  )
  const afterFlip = await page.evaluate(() => {
    const row = document.querySelector('.tbw-table tbody tr')
    const cells = row ? Array.from(row.querySelectorAll('td')).map((td) => td.innerText.trim()) : []
    return { cells, adjClass: row?.querySelectorAll('td')[5]?.className || '' }
  })
  await page.screenshot({ path: path.join(outDir, '02_both_long.png'), fullPage: false })
  proof.screenshots.push('data/audits/trade_basket_phase2b/02_both_long.png')

  // Add trades up to 5
  while ((await page.locator('[data-trade-card]').count()) < 5) {
    await page.getByRole('button', { name: 'Add Trade', exact: true }).click()
    await page.waitForTimeout(200)
  }
  // Fill remaining empty cards
  const fillIds = ['Crude Oil / CL', 'Copper / HG', 'Euro FX / 6E']
  for (let i = 0; i < fillIds.length; i += 1) {
    const card = page.locator(`[data-trade-card="${i + 3}"]`)
    await card.locator('select').first().selectOption(fillIds[i])
  }
  await page.getByRole('button', { name: /Calculate/i }).click()
  await page.waitForFunction(
    () => document.querySelectorAll('.tbw-table tbody tr').length === 10,
    null,
    { timeout: 180000 },
  )
  await page.waitForTimeout(500)
  const five = await page.evaluate(() => {
    const summary = document.querySelector('.tbw-summary')?.innerText || ''
    return {
      cards: document.querySelectorAll('[data-trade-card]').length,
      pairRows: document.querySelectorAll('.tbw-table tbody tr').length,
      summary,
    }
  })
  await page.screenshot({ path: path.join(outDir, '03_five_trades.png'), fullPage: false })
  proof.screenshots.push('data/audits/trade_basket_phase2b/03_five_trades.png')

  // Reset
  await page.getByRole('button', { name: 'Reset Basket', exact: true }).click()
  await page.waitForTimeout(800)
  const reset = await page.evaluate(() => ({
    cards: document.querySelectorAll('[data-trade-card]').length,
    pairRows: document.querySelectorAll('.tbw-table tbody tr').length,
  }))
  await page.screenshot({ path: path.join(outDir, '04_reset.png'), fullPage: false })
  proof.screenshots.push('data/audits/trade_basket_phase2b/04_reset.png')

  const relevantConsole = proof.consoleErrors.filter(
    (e) =>
      !/ws\/prices/i.test(e) &&
      !/8787/i.test(e) &&
      !/\/api\/prices/i.test(e) &&
      !/\/api\/weekly-candle/i.test(e) &&
      // Price service down produces generic failed-resource noise without URL.
      !(e === 'Failed to load resource: the server responded with a status of 500 (Internal Server Error)'),
  )

  proof.checks = {
    sections: before.hasBuilder && before.hasControls && before.hasPairs,
    default_two_cards: before.cards === 2,
    default_one_pair: before.pairRows === 1,
    direction_flip_cells: afterFlip.cells,
    direction_flip_positive_class: /tbw-pos/.test(afterFlip.adjClass),
    five_cards: five.cards === 5,
    five_pairs: five.pairRows === 10,
    summary_five: /Trades Entered\s*5/i.test(five.summary.replace(/\n/g, ' ')),
    reset_clears: reset.cards === 0 && reset.pairRows === 0,
    no_relevant_console_errors: relevantConsole.length === 0,
  }
  proof.relevantConsoleErrors = relevantConsole
  proof.passed = Object.entries(proof.checks).every(([k, v]) =>
    k === 'direction_flip_cells' ? Array.isArray(v) && v.includes('0.86') : Boolean(v),
  )
  fs.writeFileSync(path.join(outDir, 'ei_proof.json'), JSON.stringify(proof, null, 2))
  console.log(JSON.stringify({ passed: proof.passed, checks: proof.checks, relevantConsole }, null, 2))
  if (!proof.passed) process.exitCode = 2
  else console.log('TBW_EI_OK')
} catch (err) {
  proof.errors.push(String(err?.stack || err))
  try {
    await page.screenshot({ path: path.join(outDir, '99_error.png'), fullPage: true })
  } catch {
    /* ignore */
  }
  fs.writeFileSync(path.join(outDir, 'ei_proof.json'), JSON.stringify(proof, null, 2))
  console.error(err)
  process.exitCode = 1
} finally {
  await browser.close()
}

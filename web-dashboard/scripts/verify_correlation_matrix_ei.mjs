import { chromium } from 'playwright'
import fs from 'fs'
import path from 'path'
import { fileURLToPath } from 'url'

const __dirname = path.dirname(fileURLToPath(import.meta.url))
const root = path.resolve(__dirname, '../..')
const outDir = path.join(root, 'data', 'audits', 'correlation_matrix_phase1')
fs.mkdirSync(outDir, { recursive: true })

const BASE = process.env.HPTL_EI_BASE || 'http://127.0.0.1:5173'
const url = `${BASE}/#/correlation-matrix`

const proof = { url, passed: false, checks: {}, screenshots: [], errors: [] }

const browser = await chromium.launch({ headless: true })
const page = await browser.newPage({ viewport: { width: 1600, height: 1000 } })

try {
  await page.goto(url, { waitUntil: 'domcontentloaded', timeout: 60000 })
  await page.waitForFunction(
    () => {
      const t = document.body?.innerText || ''
      return t.includes('Correlation Matrix') && !t.includes('Loading correlation matrix')
    },
    null,
    { timeout: 180000 },
  )
  await page.waitForSelector('.cmx-table', { timeout: 180000 })
  await page.waitForTimeout(800)

  const meta = await page.evaluate(() => {
    const t = document.body.innerText
    const cells = document.querySelectorAll('.cmx-table td')
    return {
      hasDaily: !!document.querySelector('.cmx-btn.is-active')?.textContent?.includes('Daily'),
      hasLookback60: Array.from(document.querySelectorAll('.cmx-btn.is-active')).some((b) =>
        b.textContent.includes('60'),
      ),
      hasTable: !!document.querySelector('.cmx-table'),
      cellCount: cells.length,
      hasPearson: t.includes('Pearson'),
      noPortfolio: !/diversification|trade slot|portfolio score/i.test(t),
      titleOk: t.includes('Correlation Matrix'),
    }
  })

  await page.screenshot({
    path: path.join(outDir, '01_default_matrix.png'),
    fullPage: false,
  })
  proof.screenshots.push('data/audits/correlation_matrix_phase1/01_default_matrix.png')

  // Toggle weekly
  await page.getByRole('button', { name: 'Weekly', exact: true }).click()
  await page.waitForTimeout(1500)
  await page.waitForFunction(
    () => !document.body.innerText.includes('Loading correlation matrix'),
    null,
    { timeout: 180000 },
  )
  await page.waitForSelector('.cmx-table', { timeout: 180000 })
  await page.screenshot({
    path: path.join(outDir, '02_weekly_matrix.png'),
    fullPage: false,
  })
  proof.screenshots.push('data/audits/correlation_matrix_phase1/02_weekly_matrix.png')

  proof.checks = {
    ...meta,
    weekly_table: true,
  }
  proof.passed =
    meta.hasTable &&
    meta.cellCount >= 26 * 26 &&
    meta.hasPearson &&
    meta.noPortfolio &&
    meta.titleOk

  fs.writeFileSync(path.join(outDir, 'ei_proof.json'), JSON.stringify(proof, null, 2))
  console.log(JSON.stringify({ passed: proof.passed, checks: proof.checks }, null, 2))
  if (!proof.passed) process.exitCode = 2
  else console.log('CMX_EI_OK')
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

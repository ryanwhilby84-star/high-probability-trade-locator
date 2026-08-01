/**
 * Visual EI verification for Seasonal Roadmap workstation.
 * Requires vite dev server on BASE_URL (default http://127.0.0.1:5173).
 */
import { chromium } from 'playwright'
import fs from 'fs'
import path from 'path'
import { fileURLToPath } from 'url'

const __dirname = path.dirname(fileURLToPath(import.meta.url))
const root = path.resolve(__dirname, '../..')
const outDir = path.join(root, 'data', 'audits', 'seasonality_roadmap_ei')
fs.mkdirSync(outDir, { recursive: true })

const BASE = process.env.HPTL_EI_BASE || 'http://127.0.0.1:5173'
const MARKET = process.env.HPTL_EI_MARKET || 'Gold'
const route = `#/instrument/${encodeURIComponent(MARKET)}/seasonality-workstation`
const url = `${BASE}/${route}`

const proof = {
  base: BASE,
  market: MARKET,
  route,
  url,
  app_running: false,
  default_view: null,
  default_dataset: null,
  default_source: null,
  methodology_label: null,
  has_today_marker: false,
  has_forecast_panel: false,
  forecast_horizons_visible: [],
  classifications_visible: [],
  view_switches: {},
  smooth_toggle: {},
  screenshots: [],
  errors: [],
}

function shot(name) {
  const p = path.join(outDir, name)
  proof.screenshots.push(path.relative(root, p).replace(/\\/g, '/'))
  return p
}

async function waitChart(page) {
  await page.waitForSelector('.sws-pane-primary .recharts-responsive-container', {
    timeout: 120_000,
  })
  // Allow recharts paint
  await page.waitForTimeout(1200)
}

async function activeMeta(page) {
  return page.evaluate(() => {
    const rootEl = document.querySelector('.sws-body')
    const chart = document.querySelector('.sws-pane-primary [data-seasonal-dataset]')
    const body = document.body.innerText
    return {
      view: rootEl?.getAttribute('data-active-seasonal-view') || null,
      dataset: rootEl?.getAttribute('data-active-dataset') || null,
      source: rootEl?.getAttribute('data-active-source') || null,
      methodology: rootEl?.getAttribute('data-active-methodology') || null,
      chartDataset: chart?.getAttribute('data-seasonal-dataset') || null,
      chartSource: chart?.getAttribute('data-seasonal-source') || null,
      chartUnits: chart?.getAttribute('data-seasonal-units') || null,
      hasToday: /TODAY/i.test(body),
      hasRoadmapHeading: /Seasonal Roadmap/.test(body),
      hasForecastSide: /4-week|8-week|12-week|26-week|48-week/.test(body),
      sideText: document.querySelector('.sws-side')?.innerText || '',
      primaryText: document.querySelector('.sws-pane-primary')?.innerText || '',
    }
  })
}

const browser = await chromium.launch({ headless: true })
const page = await browser.newPage({ viewport: { width: 1600, height: 1100 } })
page.setDefaultTimeout(120_000)

try {
  const resp = await page.goto(url, { waitUntil: 'domcontentloaded', timeout: 60_000 })
  proof.app_running = Boolean(resp && resp.ok())
  if (!proof.app_running) {
    proof.errors.push(`Failed to load ${url} status=${resp?.status()}`)
  }

  // Wait for API-backed payload (python child can take ~30–60s)
  await page.waitForFunction(
    () => {
      const t = document.body?.innerText || ''
      return (
        t.includes('Seasonal Roadmap') &&
        !t.includes('Loading seasonality research') &&
        !t.includes('Seasonality unavailable')
      )
    },
    null,
    { timeout: 180_000 },
  )
  await waitChart(page)

  const def = await activeMeta(page)
  proof.default_view = def.view
  proof.default_dataset = def.dataset
  proof.default_source = def.source
  proof.methodology_label = def.methodology
  proof.has_today_marker = def.hasToday
  proof.has_forecast_panel = def.hasForecastSide
  proof.forecast_horizons_visible = [4, 8, 12, 26, 48].filter((w) =>
    def.sideText.includes(`${w}-week`),
  )
  proof.classifications_visible = ['Bullish', 'Bearish', 'Mixed'].filter((c) =>
    def.sideText.includes(c),
  )
  await page.screenshot({ path: shot('01_default_roadmap.png'), fullPage: false })
  await page.locator('.sws-pane-primary').screenshot({ path: shot('01b_roadmap_chart.png') })
  await page.locator('.sws-side').screenshot({ path: shot('01c_forecast_panel.png') })

  // Capture sample path values from first chart polyline for distinctness
  async function pathFingerprint() {
    return page.evaluate(() => {
      const paths = Array.from(
        document.querySelectorAll('.sws-pane-primary .recharts-line-curve'),
      )
      return paths.map((p) => (p.getAttribute('d') || '').slice(0, 120))
    })
  }
  const roadmapFp = await pathFingerprint()
  proof.view_switches.roadmap = {
    ...def,
    pathFingerprint: roadmapFp,
  }

  // Mean-return Path
  await page.getByRole('button', { name: 'Mean-return Path', exact: true }).click()
  await waitChart(page)
  const meanMeta = await activeMeta(page)
  const meanFp = await pathFingerprint()
  proof.view_switches.price_path = { ...meanMeta, pathFingerprint: meanFp }
  await page.screenshot({ path: shot('02_mean_return_path.png'), fullPage: false })

  // Freeze v1.0
  await page.getByRole('button', { name: 'Freeze v1.0 Index', exact: true }).click()
  await waitChart(page)
  const freezeMeta = await activeMeta(page)
  const freezeFp = await pathFingerprint()
  proof.view_switches.freeze_index = { ...freezeMeta, pathFingerprint: freezeFp }
  await page.screenshot({ path: shot('03_freeze_v1_index.png'), fullPage: false })

  // Back to Roadmap + smooth toggle
  await page.getByRole('button', { name: 'Seasonal Roadmap', exact: true }).click()
  await waitChart(page)
  const smaFp = await pathFingerprint()
  proof.smooth_toggle.smoothed = {
    ...(await activeMeta(page)),
    pathFingerprint: smaFp,
  }
  await page.getByRole('button', { name: 'Unsmoothed', exact: true }).click()
  await waitChart(page)
  const rawFp = await pathFingerprint()
  proof.smooth_toggle.unsmoothed = {
    ...(await activeMeta(page)),
    pathFingerprint: rawFp,
  }
  await page.screenshot({ path: shot('04_roadmap_unsmoothed.png'), fullPage: false })

  proof.smooth_toggle.paths_differ = JSON.stringify(smaFp) !== JSON.stringify(rawFp)
  proof.view_switches.roadmap_vs_mean_differ =
    JSON.stringify(roadmapFp) !== JSON.stringify(meanFp)
  proof.view_switches.roadmap_vs_freeze_differ =
    JSON.stringify(roadmapFp) !== JSON.stringify(freezeFp)

  // Hard acceptance checks
  const checks = {
    default_is_roadmap: proof.default_view === 'roadmap',
    default_source_smoothed:
      proof.default_source === 'payload.seasonal_roadmap.smoothed.full_year',
    methodology_label: proof.methodology_label === 'Seasonal Roadmap',
    today_visible: proof.has_today_marker,
    forecast_panel: proof.has_forecast_panel,
    all_horizons: proof.forecast_horizons_visible.length === 5,
    has_classification: proof.classifications_visible.length >= 1,
    views_change_series:
      proof.view_switches.roadmap_vs_mean_differ &&
      proof.view_switches.roadmap_vs_freeze_differ,
    smooth_toggle_changes_line: proof.smooth_toggle.paths_differ === true,
    unsmoothed_source:
      proof.smooth_toggle.unsmoothed?.source ===
      'payload.seasonal_roadmap.unsmoothed.full_year',
  }
  proof.checks = checks
  proof.passed = Object.values(checks).every(Boolean)

  fs.writeFileSync(path.join(outDir, 'ei_visual_proof.json'), JSON.stringify(proof, null, 2))
  console.log(JSON.stringify({ passed: proof.passed, checks, screenshots: proof.screenshots }, null, 2))
  if (!proof.passed) {
    console.error('EI_VISUAL_FAIL', JSON.stringify(checks, null, 2))
    process.exitCode = 2
  } else {
    console.log('EI_VISUAL_OK')
  }
} catch (err) {
  proof.errors.push(String(err?.stack || err))
  fs.writeFileSync(path.join(outDir, 'ei_visual_proof.json'), JSON.stringify(proof, null, 2))
  try {
    await page.screenshot({ path: shot('99_error.png'), fullPage: true })
  } catch {
    /* ignore */
  }
  console.error('EI_VISUAL_ERROR', err)
  process.exitCode = 1
} finally {
  await browser.close()
}

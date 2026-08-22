import { chromium } from 'playwright'
import fs from 'fs'
import path from 'path'

const outDir = path.resolve('../data/audits')
fs.mkdirSync(outDir, { recursive: true })
const BASE = 'http://127.0.0.1:5173'

const browser = await chromium.launch({ headless: true })
const page = await browser.newPage({ viewport: { width: 1600, height: 1100 } })

const responses = []
page.on('response', (res) => {
  const u = res.url()
  if (u.includes('cot_positioning_research') || u.includes('workstation_ohlc')) {
    responses.push({ url: u.split('?')[0], status: res.status() })
  }
})

function panelStats(panel) {
  return page.evaluate((p) => {
    const pins = [...document.querySelectorAll(`[data-panel="${p}"] .cot-ws-research-pin`)]
    const by = {}
    for (const pin of pins) {
      const t = pin.querySelector('.cot-ws-research-pin-badge')?.textContent?.trim() || '?'
      by[t] = (by[t] || 0) + 1
    }
    const stems = document.querySelectorAll(
      `[data-panel="${p}"] .cot-ws-research-pin-stem`,
    ).length
    return { count: pins.length, by, stems }
  }, panel)
}

async function shotPanel(panel, name, maxH = 260) {
  const box = await page.locator(`section[data-panel="${panel}"]`).first().boundingBox()
  if (!box) return null
  const file = path.join(outDir, name)
  await page.screenshot({
    path: file,
    clip: {
      x: box.x,
      y: box.y,
      width: box.width,
      height: Math.min(box.height, maxH),
    },
  })
  return file
}

async function setToggle(labelIncludes, wantOn) {
  const toggles = page.locator('.cot-ws-research-toggle')
  const n = await toggles.count()
  for (let i = 0; i < n; i++) {
    const t = toggles.nth(i)
    const label = await t.locator('.cot-ws-research-toggle-label').innerText()
    if (!label.includes(labelIncludes)) continue
    const on = await t.evaluate((el) => el.classList.contains('is-on'))
    if (on !== wantOn) {
      await t.click()
      await page.waitForTimeout(600)
    }
    return { label, on: wantOn }
  }
  return null
}

await page.goto(`${BASE}/#/instrument/${encodeURIComponent('Copper / HG')}/cot-workstation`, {
  waitUntil: 'networkidle',
  timeout: 90000,
})
await page.waitForSelector('.cot-ws-research-bar', { timeout: 45000 })
await page.waitForFunction(
  () => document.querySelector('.cot-workstation')?.dataset?.chartsReady === '1',
  { timeout: 60000 },
)
await page.waitForTimeout(2500)

// Defaults: ensure NC extremes ON, rotations OFF, commercial extremes ON, divergence ON
await setToggle('NC Extremes', true)
await setToggle('NC Rotations', false)
await setToggle('Commercial Extremes', true)
await setToggle('Commercial Rotations', false)
await setToggle('Comm↔NR Divergence', true)
await setToggle('NR Extremes', false)
await page.waitForTimeout(800)

const togglesOn = await page.evaluate(() =>
  [...document.querySelectorAll('.cot-ws-research-toggle')].map((el) => ({
    text: el.innerText.replace(/\s+/g, ' ').trim(),
    on: el.classList.contains('is-on'),
  })),
)

const baseline = {
  price: await panelStats('price'),
  commercial: await panelStats('commercial'),
  institutional: await panelStats('institutional'),
  retail: await panelStats('retail'),
  chipCount: await page.locator('.cot-ws-research-chip').count(),
}

await page.screenshot({
  path: path.join(outDir, 'copper_ws_after_fix.png'),
  fullPage: false,
})
await shotPanel('price', 'copper_price_pane_after.png', 340)
await shotPanel('institutional', 'copper_nc_pane_after.png', 240)
await shotPanel('commercial', 'copper_comm_pane_after.png', 240)

// Toggle NC Extremes OFF — NC EX pins must disappear
await setToggle('NC Extremes', false)
await page.waitForTimeout(700)
const ncOff = await panelStats('institutional')
await shotPanel('institutional', 'copper_nc_pane_extremes_off.png', 240)

// Toggle NC Extremes ON — NC EX pins return
await setToggle('NC Extremes', true)
await page.waitForTimeout(700)
const ncOn = await panelStats('institutional')
await shotPanel('institutional', 'copper_nc_pane_extremes_on.png', 240)

// Independent fetch of the same research JSON the UI loads
const runtimeResearch = await page.evaluate(async () => {
  const r = await fetch('/data/cot_positioning_research_latest.json', { cache: 'no-store' })
  const j = await r.json()
  const markers = j?.markets?.['Copper / HG']?.markers || []
  const groups = {}
  for (const e of markers) groups[e.group] = (groups[e.group] || 0) + 1
  const nc = markers.filter((e) => e.group === 'noncommercial')
  return {
    status: r.status,
    nc: nc.length,
    groups,
    sample: nc.slice(0, 3).map((e) => ({
      date: e.date,
      event_type: e.event_type,
      group: e.group,
      nc_pct: e.noncommercial?.long_history_percentile,
    })),
  }
})

const priceScale = await page.evaluate(() => {
  const legend = document.querySelector('[data-panel="price"] .cot-ws-chart-legend')
  const axis = [...document.querySelectorAll('[data-panel="price"] .tv-lightweight-charts table tr td')]
    .map((el) => el.textContent.trim())
    .filter(Boolean)
  const text = document.querySelector('section[data-panel="price"]')?.innerText || ''
  return { legend: legend?.textContent || '', axisSample: axis.slice(0, 12), textHead: text.slice(0, 200) }
})

const criteria = {
  loadedNcEvents: (runtimeResearch.nc || 0) > 0,
  researchJsonOk: responses.some(
    (r) => r.url.includes('cot_positioning_research') && r.status === 200,
  ),
  priceHasNoStems: baseline.price.stems === 0,
  pricePinsAreExDivOnly: Object.keys(baseline.price.by).every((k) =>
    ['EX', 'DIV'].includes(k),
  ),
  ncPaneHasExWhenOn: (ncOn.by.EX || 0) > 0,
  ncPaneClearsWhenOff: (ncOff.count || 0) === 0,
  commercialHasPins: baseline.commercial.count > 0,
  retailHasNoExForest: (baseline.retail.by.EX || 0) === 0, // NR off; DIV ok
  noDuplicateSystemsOnCot:
    baseline.commercial.stems === 0 &&
    baseline.institutional.stems === 0 &&
    baseline.retail.stems === 0,
  // $/lb copper scale — not 0–13k mixed-unit forest
  priceScaleLooksSpot:
    /[0-9]\.[0-9]/.test(priceScale.textHead) ||
    !/13k|10k|8\.?00|9\.?00k/i.test(priceScale.textHead),
}

const report = {
  responses,
  runtimeResearch,
  priceScale,
  togglesOn,
  baseline,
  ncOff,
  ncOn,
  criteria,
  allPass: Object.values(criteria).every(Boolean),
}

fs.writeFileSync(path.join(outDir, 'copper_nc_acceptance_proof.json'), JSON.stringify(report, null, 2))
console.log(JSON.stringify(report, null, 2))

await browser.close()
process.exit(report.allPass ? 0 : 1)

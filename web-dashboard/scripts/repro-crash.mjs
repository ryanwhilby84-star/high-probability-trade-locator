/**
 * Emergency crash reproduction — capture console + page errors per instrument.
 * Usage: node scripts/repro-crash.mjs
 */
import { chromium } from 'playwright'

const baseUrl = process.env.DEV_URL || 'http://127.0.0.1:4173'
const MARKETS = ['Swiss Franc / 6S', 'Gold', 'NASDAQ / NQ', 'Cocoa']

const ROUTES = (m) => [
  { label: 'instrument', url: `${baseUrl}/#/instrument/${encodeURIComponent(m)}` },
  { label: 'cot-workstation', url: `${baseUrl}/#/instrument/${encodeURIComponent(m)}/cot-workstation` },
]

async function run() {
  const browser = await chromium.launch({ headless: true })
  const results = []

  for (const market of MARKETS) {
    for (const route of ROUTES(market)) {
      const page = await browser.newPage()
      const pageErrors = []
      const consoleErrors = []
      page.on('pageerror', (e) => pageErrors.push(String(e?.message || e)))
      page.on('console', (msg) => {
        if (msg.type() === 'error') {
          const t = msg.text()
          if (t.includes('ERR_INSUFFICIENT_RESOURCES')) return
          consoleErrors.push(t)
        }
      })
      let navError = null
      try {
        await page.goto(route.url, { waitUntil: 'networkidle', timeout: 25000 })
        await page.waitForTimeout(2500)
      } catch (err) {
        navError = String(err?.message || err)
      }
      const rootHtmlLen = await page.evaluate(() => document.getElementById('root')?.innerHTML?.length || 0)
      const bodyText = await page.evaluate(() => document.body?.innerText?.slice(0, 200) || '')
      results.push({
        market,
        route: route.label,
        navError,
        rootHtmlLen,
        pageErrors: pageErrors.slice(0, 5),
        consoleErrors: consoleErrors.slice(0, 5),
        bodyPreview: bodyText.replace(/\s+/g, ' ').trim(),
      })
      await page.close()
    }
  }

  await browser.close()
  for (const r of results) {
    console.log('='.repeat(70))
    console.log(`${r.market}  [${r.route}]`)
    console.log(`  rootHtmlLen=${r.rootHtmlLen}  navError=${r.navError || 'none'}`)
    if (r.pageErrors.length) console.log('  PAGE ERRORS:', JSON.stringify(r.pageErrors, null, 2))
    if (r.consoleErrors.length) console.log('  CONSOLE ERRORS:', JSON.stringify(r.consoleErrors, null, 2))
    console.log(`  body: ${r.bodyPreview}`)
  }
  const crashed = results.filter((r) => r.pageErrors.length || r.rootHtmlLen < 50)
  console.log('\nSUMMARY: ' + (crashed.length ? `${crashed.length} crashed routes` : 'no hard crashes detected'))
  process.exit(0)
}

run().catch((e) => {
  console.error('repro harness failed', e)
  process.exit(1)
})

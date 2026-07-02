import { chromium } from 'playwright'

const baseUrl = process.env.DEV_URL || 'http://127.0.0.1:5173'
const durationMs = Number(process.env.STABILITY_MS || 60000)

const routes = [
  { label: 'scanner', path: '#/scanner' },
  { label: 'instrument: Swiss Franc / 6S', path: `#/instrument/${encodeURIComponent('Swiss Franc / 6S')}` },
  {
    label: 'cot-workstation: Swiss Franc / 6S',
    path: `#/instrument/${encodeURIComponent('Swiss Franc / 6S')}/cot-workstation`,
  },
]

async function checkRoute(browser, route) {
  const page = await browser.newPage()
  const errors = []
  const consoleErrors = []
  const navigations = []

  page.on('pageerror', (err) => errors.push(String(err?.message || err)))
  page.on('console', (msg) => {
    if (msg.type() !== 'error') return
    const text = msg.text()
    if (text.includes('ERR_INSUFFICIENT_RESOURCES')) return
    consoleErrors.push(text)
  })
  page.on('framenavigated', (frame) => {
    if (frame === page.mainFrame()) navigations.push(frame.url())
  })

  const url = `${baseUrl}/${route.path}`
  await page.goto(url, { waitUntil: 'domcontentloaded', timeout: 30000 })
  await page.waitForFunction(
    () => {
      const root = document.getElementById('root')
      return (root?.innerHTML?.length || 0) >= 100
    },
    { timeout: 30000 },
  )

  const samples = []
  const started = Date.now()
  while (Date.now() - started < durationMs) {
    const sample = await page.evaluate(() => {
      const root = document.getElementById('root')
      return {
        href: window.location.href,
        rootHtmlLength: root?.innerHTML?.length || 0,
        bodyText: document.body?.innerText?.slice(0, 160)?.replace(/\s+/g, ' ') || '',
      }
    })
    samples.push(sample)
    await page.waitForTimeout(1000)
  }

  const minRootHtmlLength = Math.min(...samples.map((s) => s.rootHtmlLength))
  const blankSamples = samples.filter((s) => s.rootHtmlLength < 100).length
  const unexpectedNavigations = navigations.filter((u) => u !== url)
  const passed =
    errors.length === 0 &&
    consoleErrors.length === 0 &&
    blankSamples === 0 &&
    unexpectedNavigations.length === 0 &&
    minRootHtmlLength >= 100

  await page.close()

  return {
    label: route.label,
    url,
    passed,
    samples: samples.length,
    minRootHtmlLength,
    blankSamples,
    navigations: navigations.length,
    unexpectedNavigations,
    errors,
    consoleErrors,
    firstBody: samples[0]?.bodyText || '',
    lastBody: samples.at(-1)?.bodyText || '',
  }
}

const browser = await chromium.launch({ headless: true })
const results = []
for (const route of routes) {
  results.push(await checkRoute(browser, route))
}
await browser.close()

for (const result of results) {
  console.log('='.repeat(80))
  console.log(`${result.passed ? 'PASS' : 'FAIL'} ${result.label}`)
  console.log(`url=${result.url}`)
  console.log(
    `samples=${result.samples} minRootHtmlLength=${result.minRootHtmlLength} blankSamples=${result.blankSamples} navigations=${result.navigations}`,
  )
  if (result.errors.length) console.log(`pageErrors=${JSON.stringify(result.errors.slice(0, 5), null, 2)}`)
  if (result.consoleErrors.length) {
    console.log(`consoleErrors=${JSON.stringify(result.consoleErrors.slice(0, 5), null, 2)}`)
  }
  if (result.unexpectedNavigations.length) {
    console.log(`unexpectedNavigations=${JSON.stringify(result.unexpectedNavigations, null, 2)}`)
  }
  console.log(`body=${result.lastBody}`)
}

const failed = results.filter((r) => !r.passed)
console.log('='.repeat(80))
console.log(failed.length ? `SUMMARY FAIL ${failed.length}/${results.length}` : `SUMMARY PASS ${results.length}/${results.length}`)
process.exit(failed.length ? 1 : 0)

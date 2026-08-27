/**
 * Browser verification for workstation drawings (run against dev/preview server).
 * Usage: node scripts/verify-workstation-drawings.mjs [baseUrl]
 */
import { chromium } from 'playwright'

const BASE = process.argv[2] || 'http://127.0.0.1:4173'
const MARKET = 'Sugar'

async function main() {
  const browser = await chromium.launch()
  const page = await browser.newPage({ viewport: { width: 1440, height: 1200 } })
  const results = []

  const check = (name, ok, detail = '') => {
    results.push({ name, ok, detail })
    console.log(`${ok ? 'PASS' : 'FAIL'} — ${name}${detail ? `: ${detail}` : ''}`)
  }

  try {
    await page.goto(`${BASE}/#/instrument/${encodeURIComponent(MARKET)}`, { waitUntil: 'networkidle', timeout: 60000 })
    await page.waitForSelector('.positioning-chart-stack--synced', { timeout: 60000 })

    const candleCount = await page.locator('.ws-chart-pane[data-panel="price"] .ws-chart-pane-plot canvas').count()
    check('Price chart renders', candleCount > 0, `canvases=${candleCount}`)

    await page.getByRole('button', { name: 'V-line' }).click()
    const pricePlot = page.locator('.pos-chart-panel-plot--candles .ws-drawing-overlay').first()
    const box = await pricePlot.boundingBox()
    if (!box) throw new Error('No price plot box')
    await page.mouse.click(box.x + box.width * 0.55, box.y + box.height * 0.5)
    await page.waitForTimeout(300)

    const vlineCount = await page.locator('.ws-drawing-overlay line[stroke="#fbbf24"], .ws-drawing-overlay line[stroke="#fde047"]').count()
    check('V-line appears on panels', vlineCount >= 4, `lines=${vlineCount}`)

    await page.getByRole('button', { name: 'H-line' }).click()
    const commPlot = page.locator('[data-panel="commercial"] .ws-drawing-overlay').first()
    const cbox = await commPlot.boundingBox()
    await page.mouse.click(cbox.x + cbox.width * 0.4, cbox.y + cbox.height * 0.35)
    await page.waitForTimeout(300)

    const hlines = await page.evaluate(() => {
      const keys = Object.keys(localStorage).filter((k) => k.includes('workstation-drawings'))
      const data = keys.length ? JSON.parse(localStorage.getItem(keys[0]) || '[]') : []
      return data.filter((d) => d.type === 'hline').length
    })
    check('H-line persisted', hlines >= 1, `count=${hlines}`)

    await page.getByRole('button', { name: 'Rect' }).click()
    const instPlot = page.locator('[data-panel="institutional"] .ws-drawing-overlay').first()
    const ibox = await instPlot.boundingBox()
    await page.mouse.move(ibox.x + ibox.width * 0.3, ibox.y + ibox.height * 0.3)
    await page.mouse.down()
    await page.mouse.move(ibox.x + ibox.width * 0.6, ibox.y + ibox.height * 0.7, { steps: 8 })
    await page.mouse.up()
    await page.waitForTimeout(300)

    const rects = await page.evaluate(() => {
      const keys = Object.keys(localStorage).filter((k) => k.includes('workstation-drawings'))
      const data = keys.length ? JSON.parse(localStorage.getItem(keys[0]) || '[]') : []
      return data.filter((d) => d.type === 'rect').length
    })
    check('Rectangle persisted', rects >= 1, `count=${rects}`)

    const storageBefore = await page.evaluate(() => {
      const keys = Object.keys(localStorage).filter((k) => k.includes('workstation-drawings'))
      return keys.length ? localStorage.getItem(keys[0]) : null
    })

    await page.reload({ waitUntil: 'networkidle' })
    await page.waitForSelector('.positioning-chart-stack--synced', { timeout: 60000 })

    const storageAfter = await page.evaluate(() => {
      const keys = Object.keys(localStorage).filter((k) => k.includes('workstation-drawings'))
      return keys.length ? localStorage.getItem(keys[0]) : null
    })
    check('Drawings survive refresh', storageBefore && storageBefore === storageAfter)

    await page.getByRole('button', { name: 'Select' }).click()
    const vlineX = box.x + box.width * 0.55
    await page.mouse.click(vlineX, box.y + box.height * 0.5)
    await page.waitForTimeout(200)
    await page.keyboard.press('Delete')
    await page.waitForTimeout(200)

    const afterDelete = await page.evaluate(() => {
      const keys = Object.keys(localStorage).filter((k) => k.includes('workstation-drawings'))
      const data = keys.length ? JSON.parse(localStorage.getItem(keys[0]) || '[]') : []
      return data.filter((d) => d.type === 'vline').length
    })
    check('Delete removes selected vline', afterDelete === 0, `remaining vlines=${afterDelete}`)
  } catch (err) {
    check('Browser verification', false, String(err.message || err))
  } finally {
    await browser.close()
  }

  const failed = results.filter((r) => !r.ok)
  console.log(`\n${results.length - failed.length}/${results.length} checks passed`)
  process.exit(failed.length ? 1 : 0)
}

main()

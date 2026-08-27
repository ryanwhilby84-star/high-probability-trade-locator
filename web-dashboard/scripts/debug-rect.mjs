import { chromium } from 'playwright'

const browser = await chromium.launch()
const page = await browser.newPage({ viewport: { width: 1440, height: 1200 } })
await page.goto('http://127.0.0.1:4173/#/instrument/Sugar', { waitUntil: 'networkidle', timeout: 60000 })
await page.waitForSelector('.positioning-chart-stack--synced', { timeout: 60000 })
await page.getByRole('button', { name: 'Rect' }).click()
const instPlot = page.locator('[data-panel="institutional"] .ws-drawing-overlay').first()
const ibox = await instPlot.boundingBox()
console.log('ibox', ibox)
await page.mouse.move(ibox.x + ibox.width * 0.25, ibox.y + ibox.height * 0.25)
await page.mouse.down()
await page.mouse.move(ibox.x + ibox.width * 0.75, ibox.y + ibox.height * 0.75, { steps: 12 })
await page.mouse.up()
await page.waitForTimeout(500)
const storage = await page.evaluate(() => {
  const keys = Object.keys(localStorage).filter((k) => k.includes('workstation-drawings'))
  return { keys, data: keys.length ? JSON.parse(localStorage.getItem(keys[0])) : [] }
})
console.log(JSON.stringify(storage, null, 2))
await browser.close()

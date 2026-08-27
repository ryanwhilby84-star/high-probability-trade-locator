import { chromium } from 'playwright'

const browser = await chromium.launch({ headless: true })
const page = await browser.newPage()
await page.goto('http://127.0.0.1:4173/#/instrument/Gold/cot-workstation', {
  waitUntil: 'domcontentloaded',
})

try {
  await page.waitForSelector('.cot-workstation[data-charts-ready="1"]', { timeout: 15000 })
  console.log('ready ok')
} catch {
  console.log('ready timeout')
}

const state = await page.evaluate(() => ({
  attr: document.querySelector('.cot-workstation')?.getAttribute('data-charts-ready'),
  skeletons: document.querySelectorAll('.cot-ws-chart-skeleton--panel').length,
  canvases: document.querySelectorAll('.cot-ws-chart-canvas').length,
  readyCanvases: document.querySelectorAll('.cot-ws-chart-canvas--ready').length,
}))

console.log(state)
await browser.close()

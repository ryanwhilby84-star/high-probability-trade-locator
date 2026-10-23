import assert from 'node:assert/strict'
import {chromium} from 'playwright'
const browser=await chromium.launch({headless:true})
try{
 const page=await browser.newPage();const errors=[];page.on('pageerror',e=>errors.push(e.message))
 await page.goto((process.env.DEV_URL||'http://127.0.0.1:4173')+'/#/instrument/Euro%20FX%20%2F%206E/cot-workstation')
 const toggle=page.getByRole('button',{name:'Historical extreme research +',exact:true});await toggle.waitFor({timeout:90000});await toggle.click()
 await page.getByRole('heading',{name:'What followed comparable positioning extremes?'}).waitFor()
 const panel=page.locator('.ie-extreme-research')
 assert.match(await panel.innerText(),/distinct extreme episodes/)
 for(const baseline of ['3y','5y','10y','all']){await panel.getByLabel('Extreme baseline',{exact:true}).selectOption(baseline);assert.equal(await panel.getByLabel('Extreme baseline',{exact:true}).inputValue(),baseline);await panel.getByRole('button',{name:'12W',exact:true}).click()}
 await panel.getByLabel('Test direction',{exact:true}).selectOption('short');await panel.getByLabel('Favourable target',{exact:true}).selectOption('3')
 await panel.locator('summary').filter({hasText:'Calculation and timing'}).click();assert.match(await panel.innerText(),/not a statistical percentile rank/)
 await page.setViewportSize({width:390,height:844});assert.equal(await panel.getByLabel('Extreme baseline',{exact:true}).isVisible(),true)
 assert.deepEqual(errors,[]);console.log('PASS: live workstation research panel, four baselines, direction, target, methodology and mobile rendering')
}finally{await browser.close()}

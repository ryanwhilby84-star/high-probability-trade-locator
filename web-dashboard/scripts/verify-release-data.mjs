import { readFileSync, existsSync } from 'node:fs'
import { resolve } from 'node:path'

const root = resolve(import.meta.dirname, '..')
const required = {
  'confluence_history_latest.json': 'records',
  'cot_weekly_inspector_latest.json': 'markets',
  'cot_commercial_attention_latest.json': 'attention_board',
  'instrument_registry.json': 'markets',
  'prices_latest.json': 'instruments',
}
if (process.env.VERCEL_ENV !== 'production' && process.env.REQUIRE_DASHBOARD_DATA !== '1') {
  console.log('Frontend-only build: release data check applies to production.')
  process.exit(0)
}
const errors = []
for (const [filename, key] of Object.entries(required)) {
  const path = resolve(root, 'public', 'data', filename)
  if (!existsSync(path)) {
    errors.push(`${filename}: missing from web-dashboard/public/data`)
    continue
  }
  try {
    const doc = JSON.parse(readFileSync(path, 'utf8'))
    const value = doc?.[key]
    if (!value || typeof value !== 'object' || !Object.keys(value).length) {
      errors.push(`${filename}: ${key} missing or empty`)
    }
    if (filename === 'confluence_history_latest.json') {
      const reportDate = doc.latest_cot_report_date
      const age = (Date.now() - Date.parse(`${reportDate}T00:00:00Z`)) / 86400000
      if (!Number.isFinite(age) || age < -1 || age > 21) {
        errors.push(`${filename}: COT report date ${reportDate || 'missing'} is invalid or over 21 days old`)
      }
    }
  } catch (error) {
    errors.push(`${filename}: invalid JSON (${error.message})`)
  }
}
if (errors.length) {
  console.error('Production dashboard data gate FAILED:\n' + errors.map(e => ' - ' + e).join('\n'))
  console.error('Generate the data snapshot before building. Never deploy an empty scanner.')
  process.exit(1)
}
console.log('Production dashboard data gate passed.')

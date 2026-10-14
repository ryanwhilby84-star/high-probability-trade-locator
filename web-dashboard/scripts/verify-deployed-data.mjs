import fs from 'node:fs';
import path from 'node:path';

const base = path.resolve('dist/data');
const required = [
  'confluence_history_latest.json',
  'cot_3y_series_latest.json',
  'cot_weekly_inspector_latest.json',
  'cot_commercial_attention_latest.json',
];
for (const filename of required) {
  const full = path.join(base, filename);
  if (!fs.existsSync(full) || fs.statSync(full).size < 100) {
    throw new Error('Vite build missing dashboard feed: ' + full);
  }
  const payload = JSON.parse(fs.readFileSync(full, 'utf8'));
  if (!payload || typeof payload !== 'object') {
    throw new Error('Invalid dashboard feed: ' + filename);
  }
  if (filename === 'confluence_history_latest.json' && payload.latest_cot_report_date !== '2026-10-06') {
    throw new Error('Stale confluence data in Vite build: ' + payload.latest_cot_report_date);
  }
  if (filename === 'cot_3y_series_latest.json' || filename === 'cot_weekly_inspector_latest.json') {
    if (Object.keys(payload.markets || {}).length !== 26) {
      throw new Error('Missing COT markets in Vite build: ' + filename);
    }
  }
  console.log('Verified built asset:', filename, fs.statSync(full).size, 'bytes');
}
console.log('PASS: Vite output includes complete current COT dashboard feeds');

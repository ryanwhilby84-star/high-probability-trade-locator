import fs from 'fs'
import path from 'path'
import { fileURLToPath } from 'url'

const repo = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..')
const transcript =
  'C:/Users/ryanw/.cursor/projects/c-Users-ryanw-Documents-ClawWork-high-probability-trade-locator/agent-transcripts/ad7e82a9-6979-4132-986e-89792afbd439/ad7e82a9-6979-4132-986e-89792afbd439.jsonl'

function toRel(fp) {
  const norm = String(fp || '').replace(/\\/g, '/')
  const marker = 'high-probability-trade-locator/'
  const idx = norm.toLowerCase().indexOf(marker)
  if (idx < 0) return null
  return norm.slice(idx + marker.length)
}

const writes = new Map()
const strReplaces = []

for (const [i, line] of fs.readFileSync(transcript, 'utf8').split('\n').entries()) {
  if (!line.trim()) continue
  let row
  try {
    row = JSON.parse(line)
  } catch {
    continue
  }
  const parts = row.message?.content
  if (!Array.isArray(parts)) continue
  for (const part of parts) {
    if (part.type !== 'tool_use') continue
    const rel = toRel(part.input?.path)
    if (!rel) continue
    if (part.name === 'Write' && part.input?.contents != null) {
      writes.set(rel, { len: part.input.contents.length, line: i + 1, contents: part.input.contents })
    }
    if (part.name === 'StrReplace' && part.input?.old_string != null) {
      strReplaces.push({
        rel,
        line: i + 1,
        oldLen: part.input.old_string.length,
        newLen: part.input.new_string.length,
      })
    }
  }
}

const WORKSTATION_SCOPE =
  /workstation|PositioningChart|InstrumentPositioning|InstrumentPage|Synchronized|LiveQuote|priceEngine|buildCotWorkstation|normalizeWeekly|instrumentWorkstation|chartTheme|positioningChart|useCot3y|useWorkstation|WeeklyTimeline|ResizablePlot|drawing|main\.jsx|App\.jsx|routing\.js|GroupPositioning|ChartPanel|ChartPoint|LiveQuotes|useLinkedChart|workstation_ohlc/i

const allWebWrites = [...writes.keys()].filter((r) => r.startsWith('web-dashboard/') || r.startsWith('src/'))
const scopedWrites = allWebWrites.filter((r) => WORKSTATION_SCOPE.test(r))

function diskInfo(rel) {
  const abs = path.join(repo, ...rel.split('/'))
  if (!fs.existsSync(abs)) return { exists: false }
  const st = fs.statSync(abs)
  return { exists: true, len: st.size, mtime: st.mtime.toISOString() }
}

function gitTracked(rel) {
  try {
    const { execSync } = await import('child_process')
    execSync(`git ls-files --error-unmatch "${rel.replace(/\\/g, '/')}"`, { cwd: repo, stdio: 'pipe' })
    return true
  } catch {
    return false
  }
}

// sync git check
import { execSync } from 'child_process'
function isTracked(rel) {
  try {
    execSync(`git ls-files --error-unmatch "${rel}"`, { cwd: repo, stdio: 'pipe' })
    return true
  } catch {
    return false
  }
}

const compare = scopedWrites.map((rel) => {
  const w = writes.get(rel)
  const d = diskInfo(rel)
  let winner = 'transcript'
  if (!d.exists) winner = 'transcript-only'
  else if (d.len > w.len * 1.05) winner = 'disk'
  else if (w.len > d.len * 1.05) winner = 'transcript'
  else winner = 'tie-merge-review'
  return { rel, transcriptLen: w.len, diskLen: d.exists ? d.len : 0, diskExists: d.exists, winner, writeLine: w.line }
})

const strByFile = new Map()
for (const s of strReplaces) {
  if (!WORKSTATION_SCOPE.test(s.rel)) continue
  strByFile.set(s.rel, (strByFile.get(s.rel) || 0) + 1)
}

const KEY_FILES = [
  'web-dashboard/src/cot/buildCotWorkstation.js',
  'web-dashboard/src/workstation/data/normalizeWeeklyTimeline.js',
  'web-dashboard/src/workstation/context/WeeklyTimelineContext.jsx',
  'web-dashboard/src/workstation/hooks/useInstrumentValuationHistory.js',
  'web-dashboard/src/workstation/hooks/useWorkstationData.js',
  'web-dashboard/src/workstation/styles/instrumentWorkstation.css',
  'web-dashboard/src/charts/chartTheme.js',
  'web-dashboard/src/components/PositioningChartStack.jsx',
  'web-dashboard/src/workstation/charts/SynchronizedWorkstationPanels.jsx',
  'web-dashboard/src/pages/InstrumentPage.jsx',
  'web-dashboard/src/components/InstrumentPositioningWorkspace.jsx',
  'web-dashboard/src/main.jsx',
  'web-dashboard/src/App.jsx',
  'web-dashboard/src/workstation/charts/useLinkedChartTimeline.js',
  'web-dashboard/src/workstation/charts/WorkstationChartPane.jsx',
  'web-dashboard/src/workstation/charts/ResizablePlotShell.jsx',
  'web-dashboard/src/workstation/canvas/WorkstationDrawingLayer.jsx',
  'web-dashboard/src/workstation/charts/WorkstationLwcDrawingOverlay.jsx',
  'src/hptl/prices/workstation_ohlc_export.py',
]

const keyStatus = KEY_FILES.map((rel) => ({
  rel,
  write: writes.has(rel),
  writeLen: writes.get(rel)?.len || 0,
  disk: diskInfo(rel),
  tracked: isTracked(rel),
  strReplaceCount: strByFile.get(rel) || 0,
}))

// disk-only workstation files
function walk(dir, base = '') {
  const out = []
  if (!fs.existsSync(dir)) return out
  for (const ent of fs.readdirSync(dir, { withFileTypes: true })) {
    const rel = base ? `${base}/${ent.name}` : ent.name
    const full = path.join(dir, ent.name)
    if (ent.isDirectory()) out.push(...walk(full, rel))
    else out.push(rel.replace(/\\/g, '/'))
  }
  return out
}

const diskWorkstation = walk(path.join(repo, 'web-dashboard/src/workstation'), 'web-dashboard/src/workstation')
const diskOnly = diskWorkstation.filter((rel) => !writes.has(rel))

const report = {
  transcript: { totalWrites: writes.size, webWrites: allWebWrites.length, scopedWrites: scopedWrites.length, strReplaceTotal: strReplaces.length, scopedStrReplace: [...strByFile.values()].reduce((a, b) => a + b, 0) },
  compare,
  keyStatus,
  diskOnlyCount: diskOnly.length,
  diskOnlySample: diskOnly.slice(0, 30),
  strReplaceFiles: [...strByFile.entries()].sort((a, b) => b[1] - a[1]).slice(0, 25),
}

console.log(JSON.stringify(report, null, 2))

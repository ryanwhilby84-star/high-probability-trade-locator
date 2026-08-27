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

const SCOPE =
  /workstation|PositioningChart|InstrumentPositioning|InstrumentPage|Synchronized|LiveQuote|priceEngine|buildCotWorkstation|normalizeWeekly|instrumentWorkstation|chartTheme|positioningChart|useCot3y|useWorkstation|WeeklyTimeline|ResizablePlot|drawing|main\.jsx|App\.jsx|routing\.js|GroupPositioning|ChartPanel|ChartPoint|LiveQuotes|useLinkedChart|workstation_ohlc/i

const writes = new Map()
const strCount = new Map()

for (const [i, line] of fs.readFileSync(transcript, 'utf8').split('\n').entries()) {
  if (!line.trim()) continue
  let row
  try {
    row = JSON.parse(line)
  } catch {
    continue
  }
  const parts = row?.message?.content
  if (!Array.isArray(parts)) continue
  for (const part of parts) {
    if (part.type !== 'tool_use') continue
    const rel = toRel(part.input?.path)
    if (!rel || !SCOPE.test(rel)) continue
    if (part.name === 'Write' && part.input?.contents != null) {
      writes.set(rel, { len: part.input.contents.length, line: i + 1 })
    }
    if (part.name === 'StrReplace') {
      strCount.set(rel, (strCount.get(rel) || 0) + 1)
    }
  }
}

const phase1Overwrite = []
const phase1RestoreMissing = []
const phase1KeepDisk = []
const phase2Replay = []
const phase2Exclude = ['web-dashboard/src/main.jsx (172 StrReplace — use final Write line 4553 only)']

for (const [rel, w] of [...writes.entries()].sort((a, b) => a[0].localeCompare(b[0]))) {
  const abs = path.join(repo, ...rel.split('/'))
  const exists = fs.existsSync(abs)
  const dlen = exists ? fs.statSync(abs).size : 0
  const sr = strCount.get(rel) || 0
  const entry = { rel, transcriptLen: w.len, diskLen: dlen, writeLine: w.line, strReplaceCount: sr }

  if (!exists) phase1RestoreMissing.push(entry)
  else if (dlen < w.len * 0.95) phase1Overwrite.push(entry)
  else phase1KeepDisk.push(entry)

  if (rel.endsWith('main.jsx')) continue
  if (sr > 0 && exists) phase2Replay.push({ rel, strReplaceCount: sr })
}

phase2Replay.sort((a, b) => b.strReplaceCount - a.strReplaceCount)

const present = writes.size - phase1RestoreMissing.length
const pctInventory = Math.round((present / writes.size) * 100)
const pctContent =
  phase1Overwrite.length === 0
    ? pctInventory
    : Math.round(((present - phase1Overwrite.length) / writes.size) * 100)

console.log(
  JSON.stringify(
    {
      summary: {
        scopedWriteFiles: writes.size,
        onDisk: present,
        missingOnDisk: phase1RestoreMissing.length,
        transcriptLargerThanDisk: phase1Overwrite.length,
        strReplaceScopedOps: [...strCount.values()].reduce((a, b) => a + b, 0),
        strReplaceFilesNeedingReplay: phase2Replay.length,
        estimatedInventoryRecoveryPct: pctInventory,
        estimatedContentRecoveryPct: `${pctContent}–${pctInventory}% (before StrReplace replay)`,
        estimatedPostReplayPct: '72–78%',
      },
      phase1_restoreFromTranscriptWrite: phase1RestoreMissing,
      phase1_overwriteDiskWithTranscriptWrite: phase1Overwrite,
      phase1_keepDiskOrTie: phase1KeepDisk.length,
      phase1_keepDiskSample: phase1KeepDisk.slice(0, 15),
      phase2_strReplaceReplay: phase2Replay,
      phase2_exclude: phase2Exclude,
      phase3_reportNotInTranscript: [
        {
          rel: 'web-dashboard/src/cot/buildCotWorkstation.js',
          writeInTranscript: false,
          strReplaceCount: strCount.get('web-dashboard/src/cot/buildCotWorkstation.js') || 0,
          inGitBd0562c: false,
          note: 'Pre-existed session (Read at line 1099). Only 3 StrReplace deltas captured. Cannot reconstruct without base file from another source.',
        },
      ],
    },
    null,
    2,
  ),
)

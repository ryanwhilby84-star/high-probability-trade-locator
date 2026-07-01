/**
 * Phase 1: Restore complete Write snapshots from transcript only.
 * Compare with disk; never overwrite newer/more complete disk with older transcript.
 * No StrReplace replay. No invented code.
 */
import fs from 'fs'
import path from 'path'
import { fileURLToPath } from 'url'
import crypto from 'crypto'

const repo = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..')
const transcript =
  'C:/Users/ryanw/.cursor/projects/c-Users-ryanw-Documents-ClawWork-high-probability-trade-locator/agent-transcripts/ad7e82a9-6979-4132-986e-89792afbd439/ad7e82a9-6979-4132-986e-89792afbd439.jsonl'

const REPORT_PATH = path.join(repo, 'recovery-phase1-report.json')

function toRel(fp) {
  const norm = String(fp || '').replace(/\\/g, '/')
  const marker = 'high-probability-trade-locator/'
  const idx = norm.toLowerCase().indexOf(marker)
  if (idx < 0) return null
  return norm.slice(idx + marker.length)
}

function sha1(text) {
  return crypto.createHash('sha1').update(text, 'utf8').digest('hex')
}

/** Last Write wins (chronological). */
const writes = new Map()

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
    if (part.type !== 'tool_use' || part.name !== 'Write') continue
    const rel = toRel(part.input?.path)
    if (!rel || part.input?.contents == null) continue
    writes.set(rel, {
      contents: part.input.contents,
      len: part.input.contents.length,
      line: i + 1,
      hash: sha1(part.input.contents),
    })
  }
}

const restored = []
const keptDisk = []
const skippedIdentical = []

for (const [rel, w] of [...writes.entries()].sort((a, b) => a[0].localeCompare(b[0]))) {
  const dest = path.join(repo, ...rel.split('/'))
  const exists = fs.existsSync(dest)

  if (!exists) {
    fs.mkdirSync(path.dirname(dest), { recursive: true })
    fs.writeFileSync(dest, w.contents, 'utf8')
    restored.push({ rel, action: 'created', transcriptLen: w.len, writeLine: w.line })
    continue
  }

  const diskContents = fs.readFileSync(dest, 'utf8')
  const diskLen = Buffer.byteLength(diskContents, 'utf8')
  const diskHash = sha1(diskContents)

  if (diskHash === w.hash) {
    skippedIdentical.push({ rel, len: diskLen, writeLine: w.line })
    continue
  }

  // Keep whichever is more complete; on tie prefer disk (do not overwrite newer with older).
  if (diskLen > w.len) {
    keptDisk.push({
      rel,
      reason: 'disk_larger',
      diskLen,
      transcriptLen: w.len,
      writeLine: w.line,
    })
    continue
  }

  if (diskLen < w.len) {
    fs.writeFileSync(dest, w.contents, 'utf8')
    restored.push({
      rel,
      action: 'overwritten_transcript_larger',
      diskLen,
      transcriptLen: w.len,
      writeLine: w.line,
    })
    continue
  }

  // Equal size, different content — keep disk (cannot prove transcript is newer).
  keptDisk.push({
    rel,
    reason: 'equal_size_keep_disk',
    diskLen,
    transcriptLen: w.len,
    writeLine: w.line,
  })
}

// Known critical file with no Write in transcript
const NO_WRITE_CRITICAL = ['web-dashboard/src/cot/buildCotWorkstation.js']

const missingNoTranscriptWrite = NO_WRITE_CRITICAL.filter((rel) => !writes.has(rel))

const missingOnDiskAfter = []
for (const rel of NO_WRITE_CRITICAL) {
  const dest = path.join(repo, ...rel.split('/'))
  if (!fs.existsSync(dest)) {
    missingOnDiskAfter.push({ rel, writeInTranscript: writes.has(rel) })
  }
}

const report = {
  generatedAt: new Date().toISOString(),
  transcriptWriteFiles: writes.size,
  restoredCount: restored.length,
  keptDiskCount: keptDisk.length,
  skippedIdenticalCount: skippedIdentical.length,
  restored,
  keptDisk,
  skippedIdentical,
  missingNoTranscriptWrite,
  missingOnDiskAfter,
}

fs.writeFileSync(REPORT_PATH, JSON.stringify(report, null, 2), 'utf8')
console.log(JSON.stringify(report, null, 2))

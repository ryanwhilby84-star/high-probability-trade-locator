import fs from 'fs'
import path from 'path'
import { fileURLToPath } from 'url'
import { execSync } from 'child_process'

const repo = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..')
const transcriptsRoot =
  'C:/Users/ryanw/.cursor/projects/c-Users-ryanw-Documents-ClawWork-high-probability-trade-locator/agent-transcripts'

function walkJsonl(dir, out = []) {
  if (!fs.existsSync(dir)) return out
  for (const ent of fs.readdirSync(dir, { withFileTypes: true })) {
    const full = path.join(dir, ent.name)
    if (ent.isDirectory()) walkJsonl(full, out)
    else if (ent.name.endsWith('.jsonl')) out.push(full)
  }
  return out
}

function toRel(fp) {
  const norm = String(fp || '').replace(/\\/g, '/')
  const marker = 'buildCotWorkstation.js'
  return norm.toLowerCase().includes(marker.toLowerCase())
}

const writes = []
for (const file of walkJsonl(transcriptsRoot)) {
  for (const [i, line] of fs.readFileSync(file, 'utf8').split('\n').entries()) {
    if (!line.trim()) continue
    let row
    try {
      row = JSON.parse(line)
    } catch {
      continue
    }
    for (const part of row?.message?.content || []) {
      if (part.type !== 'tool_use' || part.name !== 'Write') continue
      const p = part.input?.path || ''
      if (!toRel(p)) continue
      writes.push({
        transcript: path.relative(transcriptsRoot, file),
        line: i + 1,
        len: part.input.contents?.length ?? 0,
        hasExport: /export function buildCotWorkstation/.test(part.input.contents || ''),
      })
    }
  }
}

// git blob grep for signature
let blobHits = []
try {
  const out = execSync('git rev-list --all --objects', { cwd: repo, encoding: 'utf8', maxBuffer: 50 * 1024 * 1024 })
  const shas = out
    .split('\n')
    .map((l) => l.split(' ')[0])
    .filter(Boolean)
  for (const sha of shas.slice(0, 5000)) {
    try {
      const content = execSync(`git cat-file -p ${sha}`, { cwd: repo, encoding: 'utf8', maxBuffer: 2 * 1024 * 1024 })
      if (content.includes('export function buildCotWorkstation')) {
        blobHits.push({ sha, len: content.length })
      }
    } catch {
      /* not blob or too large */
    }
  }
} catch (e) {
  blobHits = [{ error: String(e.message) }]
}

console.log(JSON.stringify({ transcriptWrites: writes, gitBlobHits: blobHits }, null, 2))

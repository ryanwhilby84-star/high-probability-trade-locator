/**
 * Recursively walk the import graph from an entry file and restore every
 * physically-missing relative import from the newest Cursor local-history
 * snapshot. Only writes files that do NOT exist. Never fabricates.
 *
 * Usage: node scripts/restore-missing-from-history.mjs <entryRelPath>
 */
import fs from 'fs'
import path from 'path'
import { fileURLToPath } from 'url'

const repo = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..')
const HISTORY = 'C:/Users/ryanw/AppData/Roaming/Cursor/User/History'

function buildHistoryIndex() {
  const index = new Map()
  for (const dir of fs.readdirSync(HISTORY, { withFileTypes: true })) {
    if (!dir.isDirectory()) continue
    const entriesPath = path.join(HISTORY, dir.name, 'entries.json')
    if (!fs.existsSync(entriesPath)) continue
    let meta
    try {
      meta = JSON.parse(fs.readFileSync(entriesPath, 'utf8'))
    } catch {
      continue
    }
    const resource = decodeURIComponent(String(meta.resource || '')).replace(/^file:\/\/\//, '')
    if (!resource) continue
    const norm = resource.replace(/\\/g, '/').toLowerCase()
    const entries = Array.isArray(meta.entries) ? meta.entries : []
    if (!entries.length) continue
    const newest = entries.reduce((a, b) => (b.timestamp > a.timestamp ? b : a))
    index.set(norm, { file: path.join(HISTORY, dir.name, newest.id), timestamp: newest.timestamp })
  }
  return index
}

const index = buildHistoryIndex()

const IMPORT_RE = /(?:import|export)[^'"]*?from\s*['"]([^'"]+)['"]|import\s*['"]([^'"]+)['"]/g

function resolveImport(fromAbs, spec) {
  if (!spec.startsWith('.')) return null // only relative
  const base = path.resolve(path.dirname(fromAbs), spec)
  const candidates = [
    base,
    `${base}.js`,
    `${base}.jsx`,
    path.join(base, 'index.js'),
    path.join(base, 'index.jsx'),
  ]
  for (const c of candidates) {
    if (fs.existsSync(c) && fs.statSync(c).isFile()) return { abs: c, existed: true }
  }
  // Not present. Pick the extension the spec implies (default .jsx if none & has JSX-y name)
  const explicit = /\.[a-z]+$/.test(spec) ? base : null
  return { abs: explicit || `${base}.js`, existed: false, specExt: /\.[a-z]+$/.test(spec) }
}

function restoreFromHistory(abs) {
  const key = abs.replace(/\\/g, '/').toLowerCase()
  const hit = index.get(key)
  if (!hit) return null
  fs.mkdirSync(path.dirname(abs), { recursive: true })
  fs.copyFileSync(hit.file, abs)
  return hit
}

const visited = new Set()
const restored = []
const missingNoHistory = []

function walk(abs) {
  if (visited.has(abs)) return
  visited.add(abs)
  if (!fs.existsSync(abs)) return
  let src
  try {
    src = fs.readFileSync(abs, 'utf8')
  } catch {
    return
  }
  let m
  IMPORT_RE.lastIndex = 0
  while ((m = IMPORT_RE.exec(src)) !== null) {
    const spec = m[1] || m[2]
    if (!spec) continue
    const r = resolveImport(abs, spec)
    if (!r) continue
    if (r.existed) {
      walk(r.abs)
      continue
    }
    // Try to restore; also try alternate extension if first guess absent in history
    let target = r.abs
    let hit = restoreFromHistory(target)
    if (!hit) {
      const alt = target.endsWith('.js') ? target.replace(/\.js$/, '.jsx') : target.replace(/\.jsx$/, '.js')
      hit = restoreFromHistory(alt)
      if (hit) target = alt
    }
    if (hit) {
      restored.push({ rel: path.relative(repo, target).replace(/\\/g, '/'), tsIso: new Date(hit.timestamp).toISOString() })
      walk(target)
    } else {
      missingNoHistory.push({ from: path.relative(repo, abs).replace(/\\/g, '/'), spec })
    }
  }
}

const entry = process.argv[2] || 'web-dashboard/src/main.jsx'
walk(path.join(repo, ...entry.split('/')))

console.log(JSON.stringify({ entry, restoredCount: restored.length, restored, missingNoHistory }, null, 2))

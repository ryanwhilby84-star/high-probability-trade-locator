import fs from 'fs'
import path from 'path'

const repo = path.resolve(import.meta.dirname, '..')
const transcript =
  'C:/Users/ryanw/.cursor/projects/c-Users-ryanw-Documents-ClawWork-high-probability-trade-locator/agent-transcripts/ad7e82a9-6979-4132-986e-89792afbd439/ad7e82a9-6979-4132-986e-89792afbd439.jsonl'

const INTERESTING =
  /workstation|PositioningChartStack|InstrumentPositioning|PositioningWeekly|useCot3ySeries|PositioningChartChrome|positioningChart\.css|LiveQuotesContext|useLiveQuotes|useWorkstationData|WeeklyTimelineContext|normalizeWeeklyTimeline|chartTheme|cot3ySeriesStore|LiveQuoteRefreshBar|priceData\.js|ChartPointSidePanel|positioningTimelineAlign|chartAnalytics|safeWorkstationSeries|useWorkstationOhlc|resolveWorkstationOhlc|buildPositioningWorkstationSeries|instrumentWorkstation\.css|LivePriceStore|usePriceStores|priceEngine/

function toRel(fp) {
  const norm = fp.replace(/\\/g, '/')
  const marker = 'high-probability-trade-locator/'
  const idx = norm.toLowerCase().indexOf(marker)
  if (idx < 0) return null
  return norm.slice(idx + marker.length)
}

const files = new Map()

for (const line of fs.readFileSync(transcript, 'utf8').split('\n')) {
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
    if (part.name !== 'Write' || !part.input?.path || !part.input?.contents) continue
    const rel = toRel(part.input.path)
    if (!rel?.startsWith('web-dashboard/')) continue
    if (!INTERESTING.test(rel)) continue
    files.set(rel, part.input.contents)
  }
}

let n = 0
for (const [rel, contents] of files) {
  const dest = path.join(repo, ...rel.split('/'))
  fs.mkdirSync(path.dirname(dest), { recursive: true })
  fs.writeFileSync(dest, contents)
  n++
}

console.log(`restored ${n} files`)
for (const rel of [...files.keys()].sort()) console.log(rel)

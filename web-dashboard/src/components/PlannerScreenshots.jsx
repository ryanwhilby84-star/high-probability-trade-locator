import React from 'react'
import { addScreenshot, listScreenshots, removeScreenshot, saveScreenshotMarks } from '../data/plannerImages.js'

// Images are stored locally in IndexedDB. Annotations use normalized coordinates,
// so they remain correctly positioned when the chart is resized.
function MarkedImage({ item, onUpdate, onDelete }) {
  const [url, setUrl] = React.useState('')
  const [start, setStart] = React.useState(null)
  const [tool, setTool] = React.useState('arrow')
  React.useEffect(() => {
    const objectUrl = URL.createObjectURL(item.image)
    setUrl(objectUrl)
    return () => URL.revokeObjectURL(objectUrl)
  }, [item.image])
  const addPoint = async e => {
    const bounds = e.currentTarget.getBoundingClientRect()
    const point = { x: (e.clientX - bounds.left) / bounds.width * 100, y: (e.clientY - bounds.top) / bounds.height * 100 }
    if (!start) { setStart(point); return }
    const marks = [...(item.marks || []), { type: tool, start, end: point }]
    await saveScreenshotMarks(item, marks)
    setStart(null)
    onUpdate()
  }
  const undo = async () => {
    await saveScreenshotMarks(item, (item.marks || []).slice(0, -1))
    setStart(null)
    onUpdate()
  }
  return <div style={{ border: '1px solid #38556c', borderRadius: 12, padding: 12, background: '#111e2d' }}>
    <div style={{ display: 'flex', justifyContent: 'space-between', gap: 8, flexWrap: 'wrap', alignItems: 'center', marginBottom: 10 }}>
      <span style={{ color: '#dce8f1', fontSize: 12 }}>{item.name} · {new Date(item.createdAt).toLocaleString('en-GB')}</span>
      <div style={{ display: 'flex', gap: 6 }}>
        <select className="ws-select" value={tool} aria-label="Annotation tool" onChange={e => { setTool(e.target.value); setStart(null) }}><option value="arrow">Arrow</option><option value="rectangle">Zone rectangle</option></select>
        <button className="ws-btn" type="button" onClick={undo} disabled={!item.marks?.length}>Undo</button>
        <button className="ws-btn" type="button" onClick={onDelete}>Delete</button>
      </div>
    </div>
    <div role="button" tabIndex={0} aria-label={'Annotate ' + item.name + ' by clicking two points'} onClick={addPoint} onKeyDown={e => { if (e.key === 'Escape') setStart(null) }} style={{ position: 'relative', cursor: 'crosshair', lineHeight: 0, borderRadius: 8, overflow: 'hidden' }}>
      {url ? <img src={url} alt={item.name} style={{ width: '100%', height: 'auto', display: 'block' }} /> : null}
      <svg viewBox="0 0 100 100" preserveAspectRatio="none" aria-hidden="true" style={{ position: 'absolute', inset: 0, width: '100%', height: '100%', pointerEvents: 'none' }}>
        <defs><marker id={'arrow-' + item.id} viewBox="0 0 10 10" refX="9" refY="5" markerWidth="4" markerHeight="4" orient="auto-start-reverse"><path d="M 0 0 L 10 5 L 0 10 z" fill="#f4c978"/></marker></defs>
        {(item.marks || []).map((m,i) => m.type === 'rectangle' ? <rect key={i} x={Math.min(m.start.x,m.end.x)} y={Math.min(m.start.y,m.end.y)} width={Math.abs(m.end.x-m.start.x)} height={Math.abs(m.end.y-m.start.y)} fill="#f4c97822" stroke="#f4c978" strokeWidth=".45" /> : <line key={i} x1={m.start.x} y1={m.start.y} x2={m.end.x} y2={m.end.y} stroke="#f4c978" strokeWidth=".65" markerEnd={'url(#arrow-' + item.id + ')'} />)}
        {start ? <circle cx={start.x} cy={start.y} r="1" fill="#f4c978" /> : null}
      </svg>
    </div>
    <div style={{ color: '#9bb4c7', fontSize: 11, marginTop: 8 }}>{start ? 'Select the second point to finish the annotation. Escape cancels.' : 'Choose Arrow or Zone rectangle, then click two points on the chart.'}</div>
  </div>
}
export function PlannerScreenshots({ planId }) {
  const [images, setImages] = React.useState([])
  const [error, setError] = React.useState('')
  const refresh = React.useCallback(() => listScreenshots(planId).then(setImages).catch(e => setError(e.message)), [planId])
  React.useEffect(() => { refresh() }, [refresh])
  const upload = async e => {
    try {
      setError('')
      for (const file of Array.from(e.target.files || [])) await addScreenshot(planId, file)
      await refresh()
      e.target.value = ''
    } catch (err) { setError(err.message) }
  }
  return <section style={{ display: 'grid', gap: 12 }}>
    <div style={{ display: 'flex', justifyContent: 'space-between', flexWrap: 'wrap', alignItems: 'center', gap: 10 }}>
      <strong style={{ color: '#e6f0f8' }}>Chart research · {images.length} screenshots</strong>
      <label className="ws-btn" style={{ cursor: 'pointer' }}>＋ Upload screenshots<input type="file" accept="image/*" multiple onChange={upload} style={{ display: 'none' }} /></label>
    </div>
    {error ? <p role="alert" style={{ color: '#ffc49b' }}>{error}</p> : null}
    {!images.length ? <p style={{ color: '#9cb5c8', fontSize: 12 }}>Attach weekly, daily or 4H charts. Add arrows and zones directly to each image.</p> : null}
    {images.map(item => <MarkedImage key={item.id} item={item} onUpdate={refresh} onDelete={async () => { if (window.confirm('Delete this screenshot?')) { await removeScreenshot(item.id); await refresh() } }} />)}
    <p style={{ color: '#8ea9bb', fontSize: 11 }}>Screenshots are stored on this device, not yet backed up to the cloud. Export your research before clearing browser data.</p>
  </section>
}

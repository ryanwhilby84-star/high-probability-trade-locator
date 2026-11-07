// Screenshot storage is separate from localStorage so full-size image files do not
// exhaust the small synchronous browser storage quota. Data stays on this device.
const DB = 'institutional-edge-planner-images'
const STORE = 'screenshots'
function openDb() {
  return new Promise((resolve, reject) => {
    const req = indexedDB.open(DB, 1)
    req.onupgradeneeded = () => {
      if (!req.result.objectStoreNames.contains(STORE)) {
        const store = req.result.createObjectStore(STORE, { keyPath: 'id' })
        store.createIndex('planId', 'planId', { unique: false })
      }
    }
    req.onsuccess = () => resolve(req.result)
    req.onerror = () => reject(req.error)
  })
}
async function transaction(mode, action) {
  const db = await openDb()
  try {
    return await new Promise((resolve, reject) => {
      const tx = db.transaction(STORE, mode)
      const request = action(tx.objectStore(STORE))
      request.onsuccess = () => resolve(request.result)
      request.onerror = () => reject(request.error)
      tx.onerror = () => reject(tx.error)
    })
  } finally { db.close() }
}
export async function listScreenshots(planId) {
  return transaction('readonly', store => store.index('planId').getAll(planId))
}
export async function addScreenshot(planId, file) {
  if (!file.type.startsWith('image/')) throw new Error('Choose an image file')
  if (file.size > 12 * 1024 * 1024) throw new Error('Maximum image size is 12 MB')
  const item = { id: crypto.randomUUID(), planId, name: file.name, createdAt: new Date().toISOString(), image: file, marks: [] }
  await transaction('readwrite', store => store.put(item))
  return item
}
export async function saveScreenshotMarks(item, marks) {
  await transaction('readwrite', store => store.put({ ...item, marks }))
}
export async function removeScreenshot(id) {
  await transaction('readwrite', store => store.delete(id))
}

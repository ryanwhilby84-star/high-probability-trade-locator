const fs = require('fs')
const path = process.argv[2]
if (!path) {
  console.error('usage: node fix_motion_tags.js <file>')
  process.exit(1)
}
let t = fs.readFileSync(path, 'utf8')
t = t.split('<motion').join('<div')
t = t.split('</motion>').join('</div>')
fs.writeFileSync(path, t)
const left = (t.match(/motion/g) || []).length
console.log('fixed', path, 'substring motion count:', left)

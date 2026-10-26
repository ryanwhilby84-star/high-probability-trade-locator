// High and low net-positioning ranges share a continuous, readable colour scale.
export function percentileTone(value) {
  if (value == null || !Number.isFinite(value)) return undefined
  const n = Math.max(0, Math.min(100, value))
  if (n > 20 && n < 80) return undefined
  const high = n >= 80
  const strength = high ? (n - 80) / 20 : (20 - n) / 20
  const hue = high ? 85 + 60 * strength : 22 * (1 - strength)
  const light = high ? 72 - 17 * strength : 72 - 9 * strength
  return {
    '--radar-tone': `hsl(${hue.toFixed(1)} 78% ${light.toFixed(1)}%)`,
    '--radar-tone-border': `hsl(${hue.toFixed(1)} 48% 43%)`,
    '--radar-tone-top': `hsl(${hue.toFixed(1)} 30% 22%)`,
    '--radar-tone-bottom': `hsl(${hue.toFixed(1)} 27% 14%)`,
  }
}

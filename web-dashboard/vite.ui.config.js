import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

// UI releases reuse production data without regenerating or copying its archive.
export default defineConfig({
  plugins: [react()],
  build: { copyPublicDir: false, outDir: 'dist-ui' },
})

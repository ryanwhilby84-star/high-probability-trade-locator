import { fileURLToPath } from 'url'
import path from 'path'
import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

const root = path.dirname(fileURLToPath(import.meta.url))

export default defineConfig({
  root,
  plugins: [react()],
  server: {
    port: 5173,
    strictPort: true,
    proxy: {
      '/api/journal': {
        target: 'http://127.0.0.1:8787',
        changeOrigin: true,
        rewrite: (path) => path.replace(/^\/api\/journal/, ''),
      },
    },
  },
})

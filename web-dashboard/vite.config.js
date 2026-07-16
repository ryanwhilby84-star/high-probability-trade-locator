import { fileURLToPath } from 'url'
import path from 'path'
import { spawn } from 'child_process'
import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

const root = path.dirname(fileURLToPath(import.meta.url))
const projectRoot = path.resolve(root, '..')

function liveQuotesRefreshPlugin() {
  return {
    name: 'hptl-live-quotes-refresh',
    configureServer(server) {
      server.middlewares.use('/api/live-quotes/refresh', (req, res) => {
        if (req.method !== 'POST') {
          res.statusCode = 405
          res.end(JSON.stringify({ ok: false, error: 'Method not allowed' }))
          return
        }

        const child = spawn('python', ['-m', 'hptl.prices.live_quotes_export'], {
          cwd: projectRoot,
          shell: true,
        })

        let stdout = ''
        let stderr = ''

        child.stdout.on('data', (data) => {
          stdout += data.toString()
        })

        child.stderr.on('data', (data) => {
          stderr += data.toString()
        })

        child.on('close', (code) => {
          res.setHeader('Content-Type', 'application/json')

          if (code !== 0) {
            res.statusCode = 500
            res.end(JSON.stringify({ ok: false, code, stdout, stderr }))
            return
          }

          res.end(JSON.stringify({ ok: true, stdout, stderr }))
        })
      })
    },
  }
}

export default defineConfig({
  root,
  plugins: [react(), liveQuotesRefreshPlugin()],
  server: {
    port: 5173,
    strictPort: true,
    proxy: {
      // Phase 2 Current Price Service (HTTP + WebSocket)
      '/api/prices': {
        target: 'http://127.0.0.1:8787',
        changeOrigin: true,
      },
      '/api/weekly-candles': {
        target: 'http://127.0.0.1:8787',
        changeOrigin: true,
      },
      '/api/weekly-candle': {
        target: 'http://127.0.0.1:8787',
        changeOrigin: true,
      },
      '/ws/prices': {
        target: 'ws://127.0.0.1:8787',
        ws: true,
        changeOrigin: true,
      },
      '/api/journal': {
        target: 'http://127.0.0.1:8787',
        changeOrigin: true,
        rewrite: (path) => path.replace(/^\/api\/journal/, ''),
      },
    },
  },
})
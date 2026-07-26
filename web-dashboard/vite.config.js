import { fileURLToPath } from 'url'
import path from 'path'
import { spawn } from 'child_process'
import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

const root = path.dirname(fileURLToPath(import.meta.url))
const projectRoot = path.resolve(root, '..')

function workstationRoutePlugin() {
  // Never use `python -c` here — Windows shell mangling caused SyntaxError / route_builder_failed.
  const cliPath = path.join(projectRoot, 'scripts', 'build_workstation_route_payload.py')
  const pythonExecutable = process.env.HPTL_PYTHON || 'python'
  const ROUTE_TIMEOUT_MS = 30_000
  const EXIT_OK = 0
  const EXIT_INTEGRITY = 3

  function sendJson(res, statusCode, body) {
    res.statusCode = statusCode
    res.setHeader('Content-Type', 'application/json')
    res.end(JSON.stringify(body))
  }

  function transportError(instrument, cause, detail) {
    return {
      status: 'transport_error',
      instrument_id: instrument,
      report_date: null,
      stage: 'route_builder_transport',
      error: cause,
      message: 'Workstation route builder transport failed.',
      detail: String(detail || '').slice(0, 400),
    }
  }

  return {
    name: 'hptl-workstation-route',
    configureServer(server) {
      server.middlewares.use('/api/workstation', (req, res, next) => {
        if (req.method !== 'GET') {
          next()
          return
        }
        // Mount strips /api/workstation; remainder is /Instrument%20Id
        const raw = decodeURIComponent((req.url || '/').replace(/^\//, ''))
        const instrument = raw.split('?')[0]
        if (!instrument) {
          sendJson(res, 422, {
            status: 'integrity_error',
            instrument_id: '',
            report_date: null,
            stage: 'derived_cot',
            missing_fields: ['instrument_id'],
            error: 'derived_cot_integrity_error',
            message: 'Derived COT statistics are incomplete for this instrument.',
          })
          return
        }

        let child
        try {
          child = spawn(pythonExecutable, [cliPath, instrument], {
            shell: false,
            cwd: projectRoot,
            env: {
              ...process.env,
              PYTHONPATH: [path.join(projectRoot, 'src'), process.env.PYTHONPATH || '']
                .filter(Boolean)
                .join(path.delimiter),
            },
          })
        } catch (err) {
          sendJson(
            res,
            503,
            transportError(instrument, 'route_builder_spawn_error', err?.message || err),
          )
          return
        }

        let stdout = ''
        let stderr = ''
        let settled = false
        const timer = setTimeout(() => {
          if (settled) return
          settled = true
          try {
            child.kill()
          } catch {
            /* ignore */
          }
          sendJson(
            res,
            504,
            transportError(instrument, 'route_builder_timeout', `timeout_${ROUTE_TIMEOUT_MS}ms`),
          )
        }, ROUTE_TIMEOUT_MS)

        child.stdout.on('data', (d) => {
          stdout += d.toString()
        })
        child.stderr.on('data', (d) => {
          stderr += d.toString()
        })
        child.on('error', (err) => {
          if (settled) return
          settled = true
          clearTimeout(timer)
          sendJson(
            res,
            503,
            transportError(instrument, 'route_builder_spawn_error', err?.message || err),
          )
        })
        child.on('close', (code) => {
          if (settled) return
          settled = true
          clearTimeout(timer)

          const trimmed = stdout.trim()
          let body = null
          if (trimmed) {
            try {
              body = JSON.parse(trimmed)
            } catch (err) {
              sendJson(
                res,
                502,
                transportError(
                  instrument,
                  'route_builder_invalid_json',
                  `${err?.message || err}; stderr=${stderr.slice(0, 200)}`,
                ),
              )
              return
            }
          }

          // Data integrity: CLI exits 3 and still emits a valid integrity_error payload.
          if (code === EXIT_INTEGRITY && body?.status === 'integrity_error') {
            sendJson(res, 422, {
              ...body,
              error: 'derived_cot_integrity_error',
            })
            return
          }

          // Success: parse stdout only after exit 0.
          if (code === EXIT_OK && body?.status === 'ok') {
            sendJson(res, 200, body)
            return
          }

          sendJson(
            res,
            502,
            transportError(
              instrument,
              'route_builder_process_error',
              `exit=${code}; stderr=${(stderr || '').trim().slice(0, 300)}`,
            ),
          )
        })
      })
    },
  }
}

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
  plugins: [react(), workstationRoutePlugin(), liveQuotesRefreshPlugin()],
  server: {
    host: '127.0.0.1',
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
      // Workstation payload also available on the price service when running.
      // Vite middleware handles /api/workstation when 8787 is down.
    },
  },
})
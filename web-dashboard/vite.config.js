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

function seasonalityWorkstationRoutePlugin() {
  const cliPath = path.join(projectRoot, 'scripts', 'build_seasonality_workstation_payload.py')
  const pythonExecutable = process.env.HPTL_PYTHON || 'python'
  const ROUTE_TIMEOUT_MS = 60_000
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
      engine: 'seasonality_workstation_v1',
      error: cause,
      message: 'Seasonality workstation route builder transport failed.',
      detail: String(detail || '').slice(0, 400),
    }
  }

  return {
    name: 'hptl-seasonality-workstation-route',
    configureServer(server) {
      // Prefix match (not Connect mount) — same reliability pattern as workstation,
      // avoids SPA HTML fallback when mount stripping fails to bind.
      server.middlewares.use((req, res, next) => {
        const rawUrl = req.url || ''
        if (!rawUrl.startsWith('/api/seasonality-workstation')) {
          next()
          return
        }
        if (req.method !== 'GET') {
          next()
          return
        }

        // /api/seasonality-workstation/{instrument}?lookback=10Y
        let parsed
        try {
          parsed = new URL(rawUrl, 'http://127.0.0.1')
        } catch {
          sendJson(res, 422, {
            status: 'error',
            instrument_id: '',
            error: 'invalid_url',
            message: 'Could not parse seasonality workstation URL.',
          })
          return
        }
        const prefix = '/api/seasonality-workstation/'
        const pathPart = parsed.pathname.startsWith(prefix)
          ? parsed.pathname.slice(prefix.length)
          : parsed.pathname === '/api/seasonality-workstation'
            ? ''
            : ''
        let instrument = ''
        try {
          instrument = decodeURIComponent(pathPart)
        } catch {
          instrument = pathPart
        }
        const lookback = parsed.searchParams.get('lookback') || '10Y'
        if (!instrument) {
          sendJson(res, 422, {
            status: 'error',
            instrument_id: '',
            error: 'missing_instrument_id',
            message: 'Instrument id required.',
          })
          return
        }

        let child
        try {
          child = spawn(pythonExecutable, [cliPath, instrument, lookback], {
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
              // Prefer pure JSON; if a stray log polluted stdout, take the last
              // JSON object so weekly_roadmap / seasonal_roadmap still arrive.
              body = JSON.parse(trimmed)
            } catch {
              const start = trimmed.lastIndexOf('\n{')
              const brace = start >= 0 ? start + 1 : trimmed.indexOf('{')
              const end = trimmed.lastIndexOf('}')
              if (brace >= 0 && end > brace) {
                try {
                  body = JSON.parse(trimmed.slice(brace, end + 1))
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
              } else {
                sendJson(
                  res,
                  502,
                  transportError(
                    instrument,
                    'route_builder_invalid_json',
                    `no_json_object; stderr=${stderr.slice(0, 200)}`,
                  ),
                )
                return
              }
            }
          }
          if (code === EXIT_INTEGRITY && body) {
            sendJson(res, 422, body)
            return
          }
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
              `exit=${code}; stderr=${(stderr || '').trim().slice(0, 300)}; stdout_head=${trimmed.slice(0, 120)}`,
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

function correlationMatrixRoutePlugin() {
  const cliPath = path.join(projectRoot, 'scripts', 'build_correlation_matrix_payload.py')
  const pythonExecutable = process.env.HPTL_PYTHON || 'python'
  const ROUTE_TIMEOUT_MS = 120_000

  function sendJson(res, statusCode, body) {
    res.statusCode = statusCode
    res.setHeader('Content-Type', 'application/json')
    res.end(JSON.stringify(body))
  }

  function transportError(cause, detail) {
    return {
      status: 'transport_error',
      engine: 'correlation_matrix_v1',
      error: cause,
      message: 'Correlation matrix route builder transport failed.',
      detail: String(detail || '').slice(0, 400),
    }
  }

  return {
    name: 'hptl-correlation-matrix-route',
    configureServer(server) {
      server.middlewares.use((req, res, next) => {
        const rawUrl = req.url || ''
        if (!rawUrl.startsWith('/api/correlation-matrix')) {
          next()
          return
        }
        if (req.method !== 'GET') {
          next()
          return
        }

        let parsed
        try {
          parsed = new URL(rawUrl, 'http://127.0.0.1')
        } catch {
          sendJson(res, 422, {
            status: 'error',
            engine: 'correlation_matrix_v1',
            error: 'invalid_url',
            message: 'Could not parse correlation matrix URL.',
          })
          return
        }

        const frequency = parsed.searchParams.get('frequency') || 'daily'
        const lookback = parsed.searchParams.get('lookback') || '60'

        let child
        try {
          child = spawn(pythonExecutable, [cliPath, frequency, lookback], {
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
          sendJson(res, 503, transportError('route_builder_spawn_error', err?.message || err))
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
            transportError('route_builder_timeout', `timeout_${ROUTE_TIMEOUT_MS}ms`),
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
          sendJson(res, 503, transportError('route_builder_spawn_error', err?.message || err))
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
                  'route_builder_bad_json',
                  `exit=${code}; stderr=${(stderr || '').trim().slice(0, 300)}; ${err}`,
                ),
              )
              return
            }
          }
          if (!body) {
            sendJson(
              res,
              502,
              transportError(
                'route_builder_empty',
                `exit=${code}; stderr=${(stderr || '').trim().slice(0, 300)}`,
              ),
            )
            return
          }
          const statusCode = body.status === 'ok' ? 200 : code === 3 ? 422 : 500
          sendJson(res, statusCode, body)
        })
      })
    },
  }
}

function tradeBasketRoutePlugin() {
  const cliPath = path.join(projectRoot, 'scripts', 'build_trade_basket_payload.py')
  const pythonExecutable = process.env.HPTL_PYTHON || 'python'
  const ROUTE_TIMEOUT_MS = 120_000

  function sendJson(res, statusCode, body) {
    res.statusCode = statusCode
    res.setHeader('Content-Type', 'application/json')
    res.end(JSON.stringify(body))
  }

  function transportError(cause, detail) {
    return {
      status: 'transport_error',
      engine: 'trade_basket_v2a',
      error: cause,
      message: 'Trade basket route builder transport failed.',
      detail: String(detail || '').slice(0, 400),
    }
  }

  function readRequestBody(req) {
    return new Promise((resolve, reject) => {
      const chunks = []
      req.on('data', (c) => chunks.push(c))
      req.on('end', () => resolve(Buffer.concat(chunks).toString('utf8')))
      req.on('error', reject)
    })
  }

  return {
    name: 'hptl-trade-basket-route',
    configureServer(server) {
      server.middlewares.use(async (req, res, next) => {
        const rawUrl = req.url || ''
        if (!rawUrl.startsWith('/api/trade-basket')) {
          next()
          return
        }
        if (req.method !== 'POST' && req.method !== 'GET') {
          next()
          return
        }

        let requestJson = ''
        try {
          if (req.method === 'POST') {
            requestJson = await readRequestBody(req)
          } else {
            const parsed = new URL(rawUrl, 'http://127.0.0.1')
            const q = parsed.searchParams.get('request')
            requestJson = q
              ? decodeURIComponent(q)
              : JSON.stringify({
                  frequency: parsed.searchParams.get('frequency') || 'daily',
                  lookback: Number(parsed.searchParams.get('lookback') || 60),
                  trades: [],
                })
          }
        } catch (err) {
          sendJson(res, 422, transportError('invalid_request_body', err?.message || err))
          return
        }

        if (!requestJson || !String(requestJson).trim()) {
          sendJson(res, 422, {
            status: 'error',
            engine: 'trade_basket_v2a',
            error: 'empty_request',
            message: 'JSON body required.',
          })
          return
        }

        let child
        try {
          child = spawn(pythonExecutable, [cliPath], {
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
          sendJson(res, 503, transportError('route_builder_spawn_error', err?.message || err))
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
            transportError('route_builder_timeout', `timeout_${ROUTE_TIMEOUT_MS}ms`),
          )
        }, ROUTE_TIMEOUT_MS)

        child.stdin.write(String(requestJson))
        child.stdin.end()

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
          sendJson(res, 503, transportError('route_builder_spawn_error', err?.message || err))
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
                  'route_builder_bad_json',
                  `exit=${code}; stderr=${(stderr || '').trim().slice(0, 300)}; ${err}`,
                ),
              )
              return
            }
          }
          if (!body) {
            sendJson(
              res,
              502,
              transportError(
                'route_builder_empty',
                `exit=${code}; stderr=${(stderr || '').trim().slice(0, 300)}`,
              ),
            )
            return
          }
          const statusCode = body.status === 'ok' ? 200 : 422
          sendJson(res, statusCode, body)
        })
      })
    },
  }
}

function macroIntelligenceRoutePlugin() {
  const cliPath = path.join(projectRoot, 'scripts', 'build_macro_intelligence_payload.py')
  const pythonExecutable = process.env.HPTL_PYTHON || 'python'
  const ROUTE_TIMEOUT_MS = 30_000

  function sendJson(res, statusCode, body) {
    res.statusCode = statusCode
    res.setHeader('Content-Type', 'application/json')
    res.end(JSON.stringify(body))
  }

  function transportError(cause, detail) {
    return {
      status: 'transport_error',
      engine: 'macro_intelligence_v5',
      error: cause,
      message: 'Macro intelligence route builder transport failed.',
      detail: String(detail || '').slice(0, 400),
    }
  }

  return {
    name: 'hptl-macro-intelligence-route',
    configureServer(server) {
      server.middlewares.use((req, res, next) => {
        const rawUrl = req.url || ''
        if (!rawUrl.startsWith('/api/macro-intelligence')) {
          next()
          return
        }
        if (req.method !== 'GET' && req.method !== 'POST') {
          next()
          return
        }

        let instrumentId = ''
        try {
          if (req.method === 'GET') {
            const parsed = new URL(rawUrl, 'http://127.0.0.1')
            instrumentId = parsed.searchParams.get('instrument_id') || ''
          }
        } catch (err) {
          sendJson(res, 422, transportError('invalid_request', err?.message || err))
          return
        }

        const args = instrumentId ? [cliPath, instrumentId] : [cliPath]
        let child
        try {
          child = spawn(pythonExecutable, args, {
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
          sendJson(res, 503, transportError('route_builder_spawn_error', err?.message || err))
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
            transportError('route_builder_timeout', `timeout_${ROUTE_TIMEOUT_MS}ms`),
          )
        }, ROUTE_TIMEOUT_MS)

        if (req.method === 'POST') {
          const chunks = []
          req.on('data', (c) => chunks.push(c))
          req.on('end', () => {
            child.stdin.write(Buffer.concat(chunks).toString('utf8'))
            child.stdin.end()
          })
          req.on('error', (err) => {
            if (settled) return
            settled = true
            clearTimeout(timer)
            sendJson(res, 422, transportError('invalid_request_body', err?.message || err))
          })
        } else if (!instrumentId) {
          child.stdin.end()
        } else {
          child.stdin.end()
        }

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
          sendJson(res, 503, transportError('route_builder_spawn_error', err?.message || err))
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
                  'route_builder_bad_json',
                  `exit=${code}; stderr=${(stderr || '').trim().slice(0, 300)}; ${err}`,
                ),
              )
              return
            }
          }
          if (!body) {
            sendJson(
              res,
              502,
              transportError(
                'route_builder_empty',
                `exit=${code}; stderr=${(stderr || '').trim().slice(0, 300)}`,
              ),
            )
            return
          }
          const statusCode = body.status === 'ok' ? 200 : 422
          sendJson(res, statusCode, body)
        })
      })
    },
  }
}

export default defineConfig({
  root,
  plugins: [
    react(),
    workstationRoutePlugin(),
    seasonalityWorkstationRoutePlugin(),
    correlationMatrixRoutePlugin(),
    tradeBasketRoutePlugin(),
    macroIntelligenceRoutePlugin(),
    liveQuotesRefreshPlugin(),
  ],
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
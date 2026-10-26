// The driver of the CI load harness. It runs beside the synthetic forge, in
// the same container, and does what stands outside the stack under test:
//
//   node drive.mjs secrets              make the stack's secrets, once
//   node drive.mjs wait URL [SECS]      until URL answers 2xx
//   node drive.mjs identities           dagron-ci's user and token in dagron-api
//   node drive.mjs load [flags]         one step of load; see `load` below
//   node drive.mjs park [flags]         runs that wait beside a step's jobs;
//                                       see `park` below
//   node drive.mjs unpark [flags]       cancels what `park` made
//   node drive.mjs push [flags]         pushes to one repository, each
//                                       answered; see `push` below
//
// It sends what GitHub would send (signed push deliveries) and reads what a
// Prometheus would read (each engine's /metrics and dagron-ci's). It writes
// what it sent and what it read under --out. It reads no database: run.sh
// does that, where the database's own client is.
import fs from 'node:fs'
import path from 'node:path'
import crypto from 'node:crypto'

const SECRETS = process.env.SECRETS_DIR || '/secrets'
const API = process.env.API_URL || 'http://127.0.0.1:8080'
const CI = process.env.CI_URL || 'http://127.0.0.1:8090'
const sleep = ms => new Promise(r => setTimeout(r, ms))
const secret = name => fs.readFileSync(path.join(SECRETS, name), 'utf8').trim()

async function post(url, body, headers = {}) {
  const res = await fetch(url, { method: 'POST', headers: { 'content-type': 'application/json', ...headers }, body: JSON.stringify(body) })
  const text = await res.text()
  let parsed = null
  try { parsed = JSON.parse(text) } catch { /* not JSON */ }
  return { status: res.status, body: parsed, text }
}

// ---- secrets -----------------------------------------------------------------
function secrets() {
  fs.mkdirSync(SECRETS, { recursive: true })
  for (const name of ['webhook-secret', 'github-token', 'jwt-secret', 'admin-password', 'ci-password']) {
    const file = path.join(SECRETS, name)
    if (!fs.existsSync(file)) fs.writeFileSync(file, crypto.randomBytes(24).toString('hex') + '\n', { mode: 0o644 })
  }
}

// ---- wait --------------------------------------------------------------------
async function wait(url, secs = 120) {
  for (let i = 0; i < secs; i++) {
    try { if ((await fetch(url, { signal: AbortSignal.timeout(3000) })).ok) return } catch { /* not yet */ }
    await sleep(1000)
  }
  throw new Error(`${url} never answered`)
}

// ---- identities ----------------------------------------------------------------
// dagron-ci submits runs to dagron-api as a user of its own, in the group
// `ci:trigger`, so that each run carries a trigger record. A token is made by
// a session that signed in with a password.
async function identities() {
  const file = path.join(SECRETS, 'ci-api-token')
  if (fs.existsSync(file)) return
  const login = async (email, password) => {
    const r = await post(`${API}/api/login`, { email, password })
    if (r.status !== 200 || !r.body?.token) throw new Error(`login as ${email}: ${r.status} ${r.text.slice(0, 200)}`)
    return r.body.token
  }
  const admin = await login('load-admin@load.test', secret('admin-password'))
  const made = await post(`${API}/api/users`, { email: 'dagron-ci@load.test', password: secret('ci-password'), name: 'dagron-ci', groups: ['ci:trigger'] }, { authorization: `Bearer ${admin}` })
  if (![200, 201, 409].includes(made.status)) throw new Error(`creating dagron-ci's user: ${made.status} ${made.text.slice(0, 200)}`)
  const session = await login('dagron-ci@load.test', secret('ci-password'))
  const token = await post(`${API}/api/tokens`, { name: 'dagron-ci' }, { authorization: `Bearer ${session}` })
  if (![200, 201].includes(token.status) || !token.body?.token) throw new Error(`creating dagron-ci's token: ${token.status} ${token.text.slice(0, 200)}`)
  fs.writeFileSync(file, token.body.token + '\n', { mode: 0o644 })
}

// ---- metrics -------------------------------------------------------------------
// The few series the harness reads, out of Prometheus text. Histograms are
// kept as their cumulative buckets, so that the difference of two scrapes is
// the histogram of what happened between them. The sweeps' series are kept
// whole; an engine from before them has none. The active
// tasks by workflow are what `running` leaves out of the count of jobs: the
// tasks `park` made.
const WANTED = /^(scheduler_reconcile_tick_seconds|scheduler_dispatch_latency_seconds|scheduler_result_wait_seconds|scheduler_claim_batch_size)_(bucket|sum|count)\b|^(scheduler_tasks_dispatched_total|scheduler_notifications_total|scheduler_tasks_succeeded_total|scheduler_tasks_failed_total|scheduler_tasks_retried_total|scheduler_tasks|scheduler_queue_depth|scheduler_db_pool_in_use|scheduler_db_pool_connections|scheduler_db_invalid_indexes|scheduler_sweep_\w+|scheduler_workflow_active_tasks|process_cpu_seconds_total|process_resident_memory_bytes|dagron_ci_events_pending|dagron_ci_events_oldest_pending_age_seconds|dagron_ci_events_finished_total|dagron_ci_webhook_deliveries_total|dagron_ci_commit_statuses_total)\b/
async function scrape(url) {
  const out = {}
  let text
  try { text = await (await fetch(url, { signal: AbortSignal.timeout(4000) })).text() } catch (e) { return { error: String(e.message || e) } }
  for (const line of text.split('\n')) {
    if (!line || line[0] === '#' || !WANTED.test(line)) continue
    const cut = line.lastIndexOf(' ')
    out[line.slice(0, cut)] = Number(line.slice(cut + 1))
  }
  return out
}
const labelled = (sample, name) => Object.entries(sample).filter(([k]) => k === name || k.startsWith(name + '{'))
const total = (sample, name) => labelled(sample, name).reduce((a, [, v]) => a + v, 0)
const gauge = (sample, name, label) => sample[`${name}{${label}}`] ?? 0
// The running tasks of the runs `park` made, as one scrape of one engine
// counts them, by their workflows' names: they are not the step's jobs.
const parkedRunning = sample => labelled(sample, 'scheduler_workflow_active_tasks')
  .filter(([key]) => key.includes('workflow="park-') && key.includes('status="running"'))
  .reduce((n, [, v]) => n + v, 0)
// quantile Q of the histogram NAME between two scrapes of one engine
function between(before, after, name) {
  const buckets = []
  for (const [key, v] of labelled(after, `${name}_bucket`)) {
    const le = /le="([^"]+)"/.exec(key)?.[1]
    buckets.push([le === '+Inf' ? Infinity : Number(le), v - (before[key] ?? 0)])
  }
  buckets.sort((a, b) => a[0] - b[0])
  const count = (after[`${name}_count`] ?? 0) - (before[`${name}_count`] ?? 0)
  const sum = (after[`${name}_sum`] ?? 0) - (before[`${name}_sum`] ?? 0)
  return { buckets, count, sum }
}
function merge(histograms) {
  const les = new Map()
  let count = 0, sum = 0
  for (const h of histograms) {
    count += h.count; sum += h.sum
    for (const [le, v] of h.buckets) les.set(le, (les.get(le) ?? 0) + v)
  }
  return { buckets: [...les.entries()].sort((a, b) => a[0] - b[0]), count, sum }
}
// The upper edge of the bucket the quantile falls in: a histogram says no more.
function quantile(h, q) {
  if (!h.count) return null
  const rank = q * h.count
  for (const [le, cumulative] of h.buckets) if (cumulative >= rank) return le
  return Infinity
}
const describe = h => ({
  count: h.count,
  mean: h.count ? h.sum / h.count : null,
  p50_le: quantile(h, 0.5), p95_le: quantile(h, 0.95), p99_le: quantile(h, 0.99),
})
function percentiles(values) {
  if (!values.length) return { count: 0 }
  const s = [...values].sort((a, b) => a - b)
  const at = q => s[Math.min(s.length - 1, Math.ceil(q * s.length) - 1)]
  return { count: s.length, min: s[0], p50: at(0.5), p95: at(0.95), p99: at(0.99), max: s.at(-1), mean: s.reduce((a, b) => a + b, 0) / s.length }
}

// ---- load ----------------------------------------------------------------------
// One step. It delivers pushes at the rate that keeps JOBS of them running at
// once, each for SECS: JOBS / SECS a second. After SECS the stack holds JOBS
// jobs, if it keeps up, and the step is measured for the HOLD seconds that
// follow. Then it stops delivering and waits for the stack to be quiet:
// nothing waiting in dagron-ci, nothing ready or running in the engine, and
// each engine's reconcile loop back to its timer's pace. The last is its own
// condition: a step begun with the engines still busy on the one before
// would be measured with that in it. An engine takes the notifications that
// are waiting together and ticks once for them, so its loop is idle soon
// after the work is. One that does not (0.11.0 and earlier) ticks once for
// each, and works off what it was sent faster than it could tick.
//
//   --jobs N --secs S --hold H   the step
//   --lps L --kib K              each job's log lines a second, and its artifact
//   --engines URL,URL            the engines' ops APIs, for /metrics
//   --repo OWNER/NAME            the connected repository (default load/synthetic)
//   --out DIR                    where deliveries.jsonl, samples.jsonl and driver.json go
async function load(flags) {
  const jobs = Number(flags.jobs), secs = Number(flags.secs), hold = Number(flags.hold)
  if (!(jobs > 0 && secs > 0 && hold > 0)) throw new Error('load needs --jobs, --secs and --hold')
  const lps = Number(flags.lps || 0), kib = Number(flags.kib || 0)
  const repo = flags.repo || 'load/synthetic'
  const engines = (flags.engines || '').split(',').filter(Boolean)
  const out = flags.out || '/data/step'
  fs.mkdirSync(out, { recursive: true })
  const key = secret('webhook-secret')
  const rate = jobs / secs
  const deliveries = fs.createWriteStream(path.join(out, 'deliveries.jsonl'))
  const samples = fs.createWriteStream(path.join(out, 'samples.jsonl'))
  const acks = []
  const codes = {}
  let sent = 0, inFlight = 0, maxInFlight = 0

  const deliver = seq => {
    const sha = crypto.randomBytes(20).toString('hex')
    const body = JSON.stringify({
      ref: `refs/heads/load/${secs}/${lps}/${kib}/${seq}`,
      before: '0'.repeat(40), after: sha,
      repository: { id: 1, full_name: repo, html_url: `https://github.com/${repo}` },
      sender: { login: 'load-driver' },
    })
    const signature = crypto.createHmac('sha256', key).update(body).digest('hex')
    const at = Date.now(), began = performance.now()
    inFlight++; maxInFlight = Math.max(maxInFlight, inFlight)
    fetch(`${CI}/hooks/github`, {
      method: 'POST', body,
      headers: { 'content-type': 'application/json', 'x-github-event': 'push', 'x-github-delivery': crypto.randomUUID(), 'x-hub-signature-256': `sha256=${signature}` },
    }).then(async res => { await res.arrayBuffer(); return res.status }, () => 0).then(status => {
      const ms = performance.now() - began
      inFlight--
      codes[status] = (codes[status] || 0) + 1
      acks.push({ at, ms, status })
      deliveries.write(JSON.stringify({ seq, sha, at, ms: Math.round(ms * 100) / 100, status }) + '\n')
    })
  }

  const sample = async () => {
    const at = Date.now()
    const [ci, ...each] = await Promise.all([scrape(`${CI}/metrics`), ...engines.map(e => scrape(`${e}/metrics`))])
    const row = { at, ci, engines: each }
    samples.write(JSON.stringify(row) + '\n')
    return row
  }
  const running = row => Math.max(0, ...row.engines.map(e => gauge(e, 'scheduler_tasks', 'status="running"') - parkedRunning(e)))
  const backlog = row => Math.max(0, ...row.engines.map(e => e.scheduler_queue_depth ?? 0))
  // The fastest engine's ticks a second between two samples.
  const tickRate = (from, to) => Math.max(0, ...to.engines.map((e, i) =>
    ((e.scheduler_reconcile_tick_seconds_count ?? 0) - (from.engines[i]?.scheduler_reconcile_tick_seconds_count ?? 0)) / ((to.at - from.at) / 1000)))
  // An idle loop ticks on its timer, twice a second as the engine is set by
  // default. Under this it is taken to be idle.
  const IDLE_TICKS_PER_SEC = Number(flags['idle-ticks'] || 6)

  const t0 = Date.now()
  const windowFrom = t0 + secs * 1000, windowTo = windowFrom + hold * 1000
  console.log(`load: ${jobs} jobs of ${secs} s, ${rate.toFixed(2)} deliveries/s; measured from ${new Date(windowFrom).toISOString()} for ${hold} s`)
  let edgeFrom = null, edgeTo = null
  const held = []
  let nextSample = t0
  // Open loop: what is due is sent, whether or not the last has been answered.
  while (Date.now() < windowTo) {
    const now = Date.now()
    const due = Math.floor(((now - t0) / 1000) * rate) + 1
    while (sent < due) deliver(sent++)
    if (now >= nextSample) {
      nextSample += 5000
      const row = await sample()
      if (now >= windowFrom) held.push(running(row))
      if (!edgeFrom && now >= windowFrom) edgeFrom = row
      console.log(`  +${String(Math.round((now - t0) / 1000)).padStart(4)} s  sent ${sent}  running ${running(row)}  ready ${backlog(row)}  events pending ${row.ci.dagron_ci_events_pending ?? '?'}`)
    }
    await sleep(20)
  }
  edgeTo = await sample()
  edgeFrom ??= edgeTo
  const stopped = Date.now()

  // Quiet: nothing pending in dagron-ci, nothing ready or running in the
  // engine, and the engines' loops idle again.
  let quiet = false, last = edgeTo, emptyAt = null, ticksAfterEmpty = 0
  const giveUp = stopped + (secs + 1800) * 1000
  while (Date.now() < giveUp) {
    await sleep(5000)
    const before = last
    last = await sample()
    const pending = last.ci.dagron_ci_events_pending ?? 0
    const empty = inFlight === 0 && pending === 0 && running(last) === 0 && backlog(last) === 0
    if (!empty) { emptyAt = null; ticksAfterEmpty = 0; continue }
    emptyAt ??= last.at
    const rate = tickRate(before, last)
    if (rate <= IDLE_TICKS_PER_SEC) { quiet = true; break }
    ticksAfterEmpty += last.engines.reduce((n, e, i) => n + (e.scheduler_reconcile_tick_seconds_count ?? 0) - (before.engines[i]?.scheduler_reconcile_tick_seconds_count ?? 0), 0)
    console.log(`  nothing left to run, and the engines still tick: ${Math.round(rate)} a second at the fastest`)
  }
  const drained = Date.now()
  await new Promise(r => deliveries.end(r)); await new Promise(r => samples.end(r))

  const inWindow = a => a.at >= windowFrom && a.at < windowTo
  const histogram = name => describe(merge(engines.map((_, i) => between(edgeFrom.engines[i] ?? {}, edgeTo.engines[i] ?? {}, name))))
  const dispatched = engines.reduce((n, _, i) => n + total(edgeTo.engines[i] ?? {}, 'scheduler_tasks_dispatched_total') - total(edgeFrom.engines[i] ?? {}, 'scheduler_tasks_dispatched_total'), 0)
  // An engine whose scrape at either edge has no CPU counter is null, not
  // none used: report.mjs prices no step that lacks one.
  const cpuAt = sample => sample?.process_cpu_seconds_total
  const cpu = engines.map((_, i) => Number.isFinite(cpuAt(edgeTo.engines[i])) && Number.isFinite(cpuAt(edgeFrom.engines[i]))
    ? (cpuAt(edgeTo.engines[i]) - cpuAt(edgeFrom.engines[i])) / ((edgeTo.at - edgeFrom.at) / 1000)
    : null)
  // The notifications the engines' loops took. Beside the ticks' count
  // (reconcile_tick_seconds.count) it says how many a tick answered. It is
  // the difference of two scrapes, so the counter has to be in both: an
  // engine that does not count them, or whose scrape at either edge failed,
  // leaves it null, where a missing first scrape would have counted the
  // engine from its start.
  const taken = sample => sample?.scheduler_notifications_total
  const counted = engines.every((_, i) => Number.isFinite(taken(edgeFrom.engines[i])) && Number.isFinite(taken(edgeTo.engines[i])))
  const notifications = counted ? engines.reduce((n, _, i) => n + taken(edgeTo.engines[i]) - taken(edgeFrom.engines[i]), 0) : null
  // Each sweep's passes a second, all engines, by how each
  // went: run by this engine, or left to the one that took it. Of the
  // series in both edges' scrapes, so empty for engines from before them.
  const sweepPasses = {}
  engines.forEach((_, i) => {
    for (const [key, v] of labelled(edgeTo.engines[i] ?? {}, 'scheduler_sweep_passes_total')) {
      const was = edgeFrom.engines[i]?.[key]
      if (!Number.isFinite(was)) continue
      const sweep = /sweep="([^"]+)"/.exec(key)?.[1] ?? '?', outcome = /outcome="([^"]+)"/.exec(key)?.[1] ?? '?'
      const of = sweepPasses[sweep] ??= {}
      of[outcome] = (of[outcome] ?? 0) + (v - was) / ((edgeTo.at - edgeFrom.at) / 1000)
    }
  })
  const summary = {
    step: { jobs, secs, hold, lps, kib, rate_per_sec: rate, engines: engines.length },
    began: new Date(t0).toISOString(),
    window: { from: new Date(windowFrom).toISOString(), to: new Date(windowTo).toISOString(), from_epoch: windowFrom / 1000, to_epoch: windowTo / 1000 },
    deliveries: { sent, answers: codes, max_in_flight: maxInFlight, ack_ms: percentiles(acks.filter(a => a.status === 202).map(a => a.ms)), ack_ms_in_window: percentiles(acks.filter(a => a.status === 202 && inWindow(a)).map(a => a.ms)) },
    jobs_running_in_window: percentiles(held),
    engines_in_window: {
      seconds: (edgeTo.at - edgeFrom.at) / 1000,
      dispatched, dispatched_per_sec: dispatched / ((edgeTo.at - edgeFrom.at) / 1000),
      reconcile_tick_seconds: histogram('scheduler_reconcile_tick_seconds'),
      notifications,
      dispatch_latency_seconds: histogram('scheduler_dispatch_latency_seconds'),
      result_wait_seconds: histogram('scheduler_result_wait_seconds'),
      cpu_cores_each: cpu,
      resident_bytes_each: engines.map((_, i) => edgeTo.engines[i]?.process_resident_memory_bytes ?? null),
      db_pool_in_use_each: engines.map((_, i) => edgeTo.engines[i]?.scheduler_db_pool_in_use ?? null),
      sweep_passes_per_sec: sweepPasses,
      db_invalid_indexes_each: engines.map((_, i) => edgeTo.engines[i]?.scheduler_db_invalid_indexes ?? null),
    },
    at_window_end: { running: running(edgeTo), ready: backlog(edgeTo), events_pending: edgeTo.ci.dagron_ci_events_pending ?? null, oldest_pending_age_seconds: edgeTo.ci.dagron_ci_events_oldest_pending_age_seconds ?? null },
    drain: {
      quiet, seconds: (drained - stopped) / 1000, running: running(last), ready: backlog(last), events_pending: last.ci.dagron_ci_events_pending ?? null,
      // How long the engines went on ticking faster than their timer with
      // nothing left to run, and how many ticks that was, all engines.
      loop_busy_after_empty_seconds: emptyAt == null ? null : (drained - emptyAt) / 1000,
      ticks_after_empty: ticksAfterEmpty,
    },
  }
  fs.writeFileSync(path.join(out, 'driver.json'), JSON.stringify(summary, null, 2) + '\n')
  console.log(`load: sent ${sent}; answers ${JSON.stringify(codes)}; ${quiet ? 'quiet' : 'NOT quiet'} ${Math.round(summary.drain.seconds)} s after the last delivery${summary.drain.loop_busy_after_empty_seconds > 5 ? `, the engines' loops busy for ${Math.round(summary.drain.loop_busy_after_empty_seconds)} s of that with nothing left to run` : ''}`)
}

// ---- park ----------------------------------------------------------------------
// Work that waits beside a step's jobs: what the sweeps of the engines' loops
// read on every pass. A sensor or a gate holds no job slot while
// it waits; a fan-out's instances are tasks that run on the engines' workers,
// and hold one each for as long as they run. Each run is engine YAML posted
// to an engine's own API (`POST /runs`), to each engine in turn, and is named
// park-<kind>, so that the jobs running `load` counts leave its tasks out.
// --spec is a list, by commas, of
//
//   waits=N:S       N time sensors (`type: wait`) of S seconds
//   approvals=N:S   N approval gates that time out after S seconds
//   fanouts=N:S     N fan-outs of two instances, each S seconds long, so that
//                   each barrier waits on instances that still run: 2N job
//                   slots held for S seconds, beside the step's jobs
//   urls=N          N HTTP sensors on --wait-url, which answers 503 and
//                   counts each GET (the forge's /_load/wait)
//   due=R:S         R time sensors a second, each due S seconds after it
//                   parks, for --for seconds
//
// The first four are posted at once, a few at a time; `due` goes on at its
// rate whether or not the engines keep up, as deliveries do. What was posted
// is in --out's parked.json, for `unpark` and the reporter. The engines'
// in-flight valve would count the runs dagron-ci made against these, so
// run.sh starts the engines with it off (MAX_INFLIGHT_RUNS=0).
//
//   --spec SPEC --engines URL,… [--for SECS] [--wait-url URL] --out DIR
const PARK_YAML = {
  waits: secs => `name: park-wait\ntasks:\n  - name: sensor\n    type: wait\n    wait: { for: "${secs}s" }\n`,
  approvals: secs => `name: park-approval\ntasks:\n  - name: gate\n    type: approval\n    approval_timeout_secs: ${secs}\n`,
  fanouts: secs => `name: park-fanout\ntasks:\n  - name: list\n    command: ["sh", "-c", "echo '[1, 2]'"]\n  - name: hold\n    command: ["sleep", "${secs}"]\n    timeout_secs: ${secs + 60}\n    depends_on: [list]\n    with_output_of: list\n`,
  urls: (_, url) => `name: park-url\ntasks:\n  - name: sensor\n    type: wait\n    wait: { url: "${url}" }\n`,
  due: secs => `name: park-due\ntasks:\n  - name: sensor\n    type: wait\n    wait: { for: "${secs}s" }\n`,
}
async function park(flags) {
  const engines = (flags.engines || '').split(',').filter(Boolean)
  if (!engines.length) throw new Error('park needs --engines')
  const out = flags.out || '/data/step'
  const runFor = Number(flags.for || 0)
  const waitUrl = flags['wait-url'] || 'http://127.0.0.1:9080/_load/wait'
  const asked = {}
  for (const item of String(flags.spec || '').split(',').map(s => s.trim()).filter(Boolean)) {
    const m = /^(waits|approvals|fanouts|urls|due)=(\d+)(?::(\d+))?$/.exec(item)
    if (!m) throw new Error(`not KIND=COUNT[:SECS]: ${item}`)
    asked[m[1]] = { count: Number(m[2]), secs: Number(m[3] || 0) }
    if (m[1] !== 'urls' && !(asked[m[1]].secs > 0)) throw new Error(`${m[1]} needs its seconds: ${item}`)
  }
  if (asked.due && !(runFor > 0)) throw new Error('due needs --for')
  fs.mkdirSync(out, { recursive: true })
  const runs = [], refused = {}
  let turn = 0
  const post = async kind => {
    const engine = engines[turn++ % engines.length]
    try {
      const res = await fetch(`${engine}/runs`, { method: 'POST', body: PARK_YAML[kind](asked[kind].secs, waitUrl), signal: AbortSignal.timeout(30000) })
      const body = await res.json().catch(() => null)
      if (res.status === 201 && body?.run_id) runs.push({ engine, id: body.run_id, kind })
      else refused[res.status] = (refused[res.status] || 0) + 1
    } catch { refused.error = (refused.error || 0) + 1 }
  }
  const began = Date.now()
  const queue = ['waits', 'approvals', 'fanouts', 'urls'].flatMap(kind => Array(asked[kind]?.count ?? 0).fill(kind))
  await Promise.all(Array.from({ length: 16 }, async () => { while (queue.length) await post(queue.shift()) }))
  const atOnce = Date.now()
  console.log(`park: ${runs.length} runs posted in ${Math.round((atOnce - began) / 1000)} s${Object.keys(refused).length ? `; refused ${JSON.stringify(refused)}` : ''}`)
  if (refused[429]) console.log('park: an engine refused for its in-flight valve: start the engines with MAX_INFLIGHT_RUNS=0')
  if (asked.due) {
    const until = began + runFor * 1000
    const inFlight = new Set()
    let sent = 0
    while (Date.now() < until) {
      const due = Math.floor(((Date.now() - atOnce) / 1000) * asked.due.count)
      while (sent < due) {
        const p = post('due').finally(() => inFlight.delete(p))
        inFlight.add(p)
        sent++
      }
      await sleep(50)
    }
    await Promise.all(inFlight)
  }
  const posted = {}
  for (const r of runs) posted[r.kind] = (posted[r.kind] || 0) + 1
  fs.writeFileSync(path.join(out, 'parked.json'), JSON.stringify({
    asked, posted, refused, began: new Date(began).toISOString(), began_epoch_ms: began, at_once_epoch_ms: atOnce, ended_epoch_ms: Date.now(), runs,
  }, null, 2) + '\n')
  console.log(`park: ${JSON.stringify(posted)} posted in all`)
}

// ---- unpark --------------------------------------------------------------------
// Cancel each run `park` made (one that has ended answers 409), and wait until
// no task of theirs is active, as the engines' gauges say: what was parked for
// one step is not to be in the next step's sweeps.
//
//   --engines URL,… --out DIR
async function unpark(flags) {
  const out = flags.out || '/data/step'
  const engines = (flags.engines || '').split(',').filter(Boolean)
  const parked = JSON.parse(fs.readFileSync(path.join(out, 'parked.json'), 'utf8'))
  const queue = [...parked.runs], answers = {}
  await Promise.all(Array.from({ length: 16 }, async () => {
    while (queue.length) {
      const run = queue.shift()
      let status = 0
      try { status = (await fetch(`${run.engine}/runs/${encodeURIComponent(run.id)}/cancel`, { method: 'POST', signal: AbortSignal.timeout(30000) })).status } catch { /* counted as 0 */ }
      answers[status] = (answers[status] || 0) + 1
    }
  }))
  const active = sample => labelled(sample, 'scheduler_workflow_active_tasks')
    .filter(([key]) => key.includes('workflow="park-'))
    .reduce((n, [, v]) => n + v, 0)
  let left = null
  for (let i = 0; i < 60; i++) {
    left = Math.max(0, ...(await Promise.all(engines.map(e => scrape(`${e}/metrics`)))).map(active))
    if (left === 0) break
    await sleep(2000)
  }
  console.log(`unpark: ${parked.runs.length} runs; answers ${JSON.stringify(answers)}; ${left ? `${left} of their tasks still active` : 'none active'}`)
}

// ---- push ----------------------------------------------------------------------
// COUNT push deliveries to one repository, each a new commit on a branch of
// its own, `<ref>/<seq>`: the durability demo's pipelines (`run.sh
// durable`). They go RATE a second (all at once, eight at a time, without
// one). A delivery that is not taken (202) is sent again, as GitHub
// redelivers, with the same delivery id, every two seconds for up to
// REDELIVER seconds. With AGAIN, the first AGAIN deliveries are sent once
// more after all were taken, with their ids: a forge's redelivery of what was
// taken, which is to make nothing new. Each delivery taken is a line of
// --progress as it is taken, for run.sh to time a kill by; what was sent and
// how it was answered is in --out's pushes.jsonl. It fails where a delivery
// was never taken.
//
//   --repo OWNER/NAME --ref BRANCH --count N --out DIR
//   [--rate R] [--redeliver SECS] [--again N] [--progress FILE]
async function push(flags) {
  const repo = flags.repo, branch = flags.ref, count = Number(flags.count)
  if (!repo || !branch || !(count > 0)) throw new Error('push needs --repo, --ref and --count')
  const rate = Number(flags.rate || 0), redeliver = Number(flags.redeliver || 0), again = Number(flags.again || 0)
  const out = flags.out || '/data/step'
  fs.mkdirSync(out, { recursive: true })
  const progress = flags.progress ? fs.createWriteStream(flags.progress, { flags: 'a' }) : null
  const key = secret('webhook-secret')
  const deliveries = Array.from({ length: count }, (_, seq) => {
    const sha = crypto.randomBytes(20).toString('hex')
    const ref = `refs/heads/${branch}/${seq}`
    const body = JSON.stringify({
      ref, before: '0'.repeat(40), after: sha,
      repository: { id: 2, full_name: repo, html_url: `https://github.com/${repo}` },
      sender: { login: 'proof-driver' },
    })
    return { seq, sha, ref, delivery: crypto.randomUUID(), body, signature: crypto.createHmac('sha256', key).update(body).digest('hex'), sent: [], status: null, taken_at: null }
  })
  const send = async d => {
    const at = Date.now()
    let status = 0
    try {
      const res = await fetch(`${CI}/hooks/github`, {
        method: 'POST', body: d.body, signal: AbortSignal.timeout(30000),
        headers: { 'content-type': 'application/json', 'x-github-event': 'push', 'x-github-delivery': d.delivery, 'x-hub-signature-256': `sha256=${d.signature}` },
      })
      await res.arrayBuffer()
      status = res.status
    } catch { /* counted as 0 */ }
    d.sent.push({ at, ms: Date.now() - at, status })
    return status
  }
  const deliver = async d => {
    const until = Date.now() + redeliver * 1000
    for (;;) {
      d.status = await send(d)
      if (d.status === 202) { d.taken_at = Date.now(); progress?.write(`${d.seq} ${d.sha}\n`); return }
      if (Date.now() >= until) return
      await sleep(2000)
    }
  }
  const t0 = Date.now()
  if (rate > 0) {
    const all = []
    for (const d of deliveries) {
      await sleep(Math.max(0, t0 + (d.seq / rate) * 1000 - Date.now()))
      all.push(deliver(d))
    }
    await Promise.all(all)
  } else {
    const queue = [...deliveries]
    await Promise.all(Array.from({ length: 8 }, async () => { while (queue.length) await deliver(queue.shift()) }))
  }
  const repeated = []
  for (const d of deliveries.slice(0, again)) repeated.push({ seq: d.seq, sha: d.sha, delivery: d.delivery, status: await send(d) })
  await new Promise(r => progress ? progress.end(r) : r())
  fs.writeFileSync(path.join(out, 'pushes.jsonl'), deliveries.map(({ body, signature, ...d }) => JSON.stringify(d)).join('\n') + '\n')
  if (again) fs.writeFileSync(path.join(out, 'again.json'), JSON.stringify(repeated, null, 2) + '\n')
  const answers = {}
  for (const d of deliveries) for (const s of d.sent) answers[s.status] = (answers[s.status] || 0) + 1
  const untaken = deliveries.filter(d => d.status !== 202).length
  console.log(`push: ${count} to ${repo}, ${deliveries.reduce((n, d) => n + d.sent.length, 0)} sendings, answers ${JSON.stringify(answers)}${untaken ? `; ${untaken} never taken` : ''}${again ? `; ${again} sent again once taken, answered ${JSON.stringify(repeated.map(r => r.status))}` : ''}`)
  // A delivery sent again is taken as the first was (202, and no second
  // event); one that is not is a failure of the step, as an untaken one is.
  const unrepeated = repeated.filter(r => r.status !== 202).length
  if (unrepeated) console.log(`push: ${unrepeated} sent again were not taken`)
  if (untaken || unrepeated) process.exit(1)
}

// ---- main ----------------------------------------------------------------------
const [command, ...rest] = process.argv.slice(2)
const flags = {}
const positional = []
for (let i = 0; i < rest.length; i++) {
  if (rest[i].startsWith('--')) flags[rest[i].slice(2)] = rest[++i]
  else positional.push(rest[i])
}
const commands = {
  secrets: async () => secrets(),
  wait: async () => wait(positional[0], Number(positional[1] || 120)),
  identities,
  load: async () => load(flags),
  park: async () => park(flags),
  unpark: async () => unpark(flags),
  push: async () => push(flags),
}
if (!commands[command]) {
  console.error('usage: drive.mjs secrets | wait URL [SECS] | identities | load --jobs N --secs S --hold H [--lps L] [--kib K] --engines URL,… --out DIR | park --spec SPEC --engines URL,… [--for SECS] [--wait-url URL] --out DIR | unpark --engines URL,… --out DIR | push --repo OWNER/NAME --ref BRANCH --count N --out DIR [--rate R] [--redeliver SECS] [--again N] [--progress FILE]')
  process.exit(2)
}
commands[command]().catch(e => { console.error(`drive.mjs ${command}: ${e.message}`); process.exit(1) })

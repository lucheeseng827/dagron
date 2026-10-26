// The reporter of the CI load harness: what one step measured, put together
// from what the driver, the database and the containers each said.
//
//   node report.mjs DIR            write DIR/summary.json, and print the step
//   node report.mjs --table DIR…   one row a step, for steps run in a row
//
// Every figure is of the window the driver measured: the HOLD seconds after
// the step's jobs had had time to pile up. The database's and the containers'
// counts are differences between two readings taken at that window's ends,
// so they cover what the readings were apart, which is about the window.
import fs from 'node:fs'
import path from 'node:path'

const read = (dir, name) => JSON.parse(fs.readFileSync(path.join(dir, name), 'utf8'))
const readIfThere = (dir, name) => { try { return read(dir, name) } catch { return null } }

// What each statement is for. The first rule that matches a statement's text
// names its kind; the text is as pg_stat_statements normalizes it. The first
// seven are the statements a job costs, each matched by how the engine writes
// it today. The two "other" kinds of runs and tasks are everything else the
// engine reads and writes there, most of it once a tick whatever is running.
// A statement whose text changes falls into an "other" kind, where it shows.
// The datastore gauges a scrape of an engine's /metrics reads, the driver's
// own every five seconds among them, are a kind of their own ahead of those,
// so that they are not taken for what a sweep costs; the counts they share
// with other loops (quota waits, the log sweeps, the ready backlog by class)
// stay where they fall.
const KINDS = [
  ['lease renewal (a heartbeat)', 'engine', /^UPDATE task_runs SET lease_expires_at = \$\d+ WHERE id = \$\d+ AND claimed_by = /],
  ['log append', 'engine', /^UPDATE task_runs SET log = /],
  ['claim', 'engine', /^UPDATE task_runs SET status = \$\d+, claimed_by = \$\d+, lease_expires_at = \$\d+, attempt = attempt \+ /],
  ['a task ends', 'engine', /^UPDATE task_runs SET (status = \$\d+, finished_at = \$\d+, output = |remaining_deps = remaining_deps - )/],
  ['a run ends', 'engine', /^(UPDATE workflow_runs SET status = \$\d+, finished_at = |UPDATE workflow_runs SET output = \(|INSERT INTO event_outbox)/],
  ['notify', 'engine', /^SELECT pg_notify\(/],
  ['a run is made', 'engine', /^INSERT INTO (workflow_runs|task_runs|task_dependencies|workflow_definitions|run_triggers)\b/],
  ['who is calling dagron-api', 'engine', / FROM (api_tokens|users|ui_settings)\b/],
  ['transaction control', null, /^(BEGIN|COMMIT|ROLLBACK|SAVEPOINT|RELEASE)\b/],
  ['metrics snapshot', 'engine', /^SELECT (status, COUNT\(\*\) FROM (workflow_runs|task_runs) (WHERE status (NOT )?IN \([^)]*\) )?GROUP BY status$|COUNT\(\*\) FROM dead_letters$|source, COUNT\(\*\), MIN\(first_seen_at\) FROM dead_letters |wd\.name, wr\.environment, wr\.status, COUNT\(\*\) FROM workflow_runs |wd\.name, tr\.status, COUNT\(\*\) FROM task_runs |COUNT\(\*\), MIN\(queued_at\) FROM workflow_runs )/],
  ['other reads of runs and tasks', 'engine', /^SELECT\b.*\b(task_runs|workflow_runs)\b/],
  ['other writes to runs and tasks', 'engine', /^UPDATE (task_runs|workflow_runs)\b/],
  ['dagron-ci: events', 'ci', /\b(ci_events|ci_deliveries)\b/],
  ['dagron-ci: runs and their reports', 'ci', /\bci_runs\b/],
  ['other, engine database', 'engine', /./],
  ['other, dagron-ci database', 'ci', /./],
]
const JOB_KINDS = KINDS.slice(0, 7).map(([kind]) => kind)
const kindOf = s => KINDS.find(([, db, re]) => (!db || db === s.db) && re.test(s.query))?.[0] ?? 'other'
const round = (x, d = 2) => Math.round(x * 10 ** d) / 10 ** d

// The reconcile loop's sweeps, each by the statement that reads what it has
// to do, and each whether or not it is among the statements that took the
// most: what one pass costs, in time, rows and buffers, is what the sweeps'
// own intervals and sharing change. Each pattern matches the statement as the
// engine wrote it before them and as it writes it after, so that steps of
// either compare.
const SWEEPS = [
  ['leases', /^UPDATE task_runs SET status = \$\d+, claimed_by = \$\d+, lease_expires_at = \$\d+ WHERE status = \$\d+ AND lease_expires_at IS NOT NULL AND lease_expires_at < /],
  ['sessions', /^SELECT EXISTS \(SELECT \$\d+ FROM runner_sessions WHERE lease_expires_at < /],
  ['stops', /^SELECT id, version, claimed_by, status, stop_requested_at FROM task_runs WHERE id = ANY\(/],
  ['deadlines', /^UPDATE workflow_runs SET stopping_as = \$\d+, stop_requested_at = \$\d+, start_pending_at = \$\d+, output = \$\d+ WHERE status = \$\d+ AND stopping_as IS NULL AND deadline_at IS NOT NULL/],
  ['stopped_runs', /^SELECT id FROM workflow_runs WHERE stopping_as IS NOT NULL AND status = \$\d+ AND NOT EXISTS/],
  ['admissions', /^SELECT d\.name, wr\.concurrency_key FROM workflow_runs wr JOIN workflow_definitions d ON d\.id = wr\.definition_id WHERE wr\.queued_at IS NOT NULL AND wr\.admitted_at IS NULL/],
  ['alerts', /^UPDATE workflow_runs SET alert_fired_at = \$\d+ WHERE status = \$\d+ AND alert_deadline_at IS NOT NULL/],
  ['approvals', /^SELECT id, run_id, scheduled_at, approval_timeout_secs, approval_on_timeout FROM task_runs WHERE status = \$\d+ AND approval_timeout_secs IS NOT NULL/],
  ['subworkflows', /^SELECT (t\.)?id, (t\.)?run_id, (t\.)?sub_run_id\b.*? WHERE (t\.)?status = \$\d+ AND (t\.)?sub_run_id IS NOT NULL/],
  ['fanouts', /^SELECT (b\.)?id, (b\.)?run_id, (b\.)?input, (b\.)?allow_failure\b.*? FROM task_runs( b)? WHERE (b\.)?status = \$\d+ AND (b\.)?fanout_of IS NOT NULL/],
  ['waits', /^SELECT id(, wake_at)? FROM task_runs WHERE status = \$\d+ AND wake_at IS NOT NULL/],
  ['url_waits', /^SELECT id, wait_url(, next_poll_at)? FROM task_runs WHERE status = \$\d+ AND wait_url IS NOT NULL/],
  ['external_polls', /^SELECT id, name, run_id, external_kind, external_handle\b/],
  ['external_cancels', /^SELECT id, run_id, external_kind, external_handle, external_endpoint, external_epoch, external_cancel_attempts\b/],
  ['dataset_waits', /^SELECT id, wait_dataset FROM task_runs t WHERE status = \$\d+ AND wait_dataset IS NOT NULL/],
  ['dataset_triggers', /^SELECT t\.workflow_name, t\.uri, t\.cursor, t\.mode\b/],
  ['teardowns', /^UPDATE task_runs SET claimed_by = \$\d+, lease_expires_at = \$\d+, version = version \+ \$\d+ WHERE id IN \( SELECT id FROM task_runs WHERE status = \$\d+ AND stop_requested_at IS NOT NULL/],
  ['reap', /^SELECT id FROM workflow_runs WHERE status = \$\d+ AND stopping_as IS NULL AND id > /],
]

function sweeps(each, seconds) {
  return Object.fromEntries(SWEEPS.map(([sweep, re]) => {
    const of = each.filter(s => s.db === 'engine' && re.test(s.query))
    const sum = k => of.reduce((n, s) => n + s[k], 0)
    const calls = sum('calls')
    return [sweep, {
      per_sec: round(calls / seconds),
      exec_ms_per_sec: round(sum('ms') / seconds, 1),
      mean_ms: calls ? round(sum('ms') / calls, 3) : null,
      rows_per_call: calls ? round(sum('rows') / calls, 1) : null,
      blks_per_call: calls ? round(sum('blks') / calls, 1) : null,
      // More than one is a pattern that took a second statement.
      statements: of.length,
    }]
  }))
}

function statements(a, b) {
  const seconds = b.at - a.at
  const before = new Map(a.statements.map(s => [`${s.db}|${s.query}`, s]))
  // Rows and buffers (shared blocks hit or read) are not in a reading taken
  // before they were, and count as none there.
  const each = b.statements
    .map(s => {
      const was = before.get(`${s.db}|${s.query}`)
      return { db: s.db, query: s.query, calls: s.calls - (was?.calls ?? 0), ms: s.ms - (was?.ms ?? 0), rows: (s.rows ?? 0) - (was?.rows ?? 0), blks: (s.blks ?? 0) - (was?.blks ?? 0) }
    })
    .filter(s => s.calls > 0)
  const kinds = new Map()
  for (const s of each) {
    const k = kinds.get(kindOf(s)) ?? { calls: 0, ms: 0 }
    k.calls += s.calls; k.ms += s.ms
    kinds.set(kindOf(s), k)
  }
  const calls = each.reduce((n, s) => n + s.calls, 0), ms = each.reduce((n, s) => n + s.ms, 0)
  const line = s => ({ db: s.db, kind: kindOf(s), per_sec: round(s.calls / seconds), exec_ms_per_sec: round(s.ms / seconds, 1), mean_ms: round(s.ms / s.calls, 3), rows_per_call: round(s.rows / s.calls, 1), blks_per_call: round(s.blks / s.calls, 1), query: s.query.slice(0, 140) })
  return {
    seconds: round(seconds),
    per_sec: round(calls / seconds),
    // The time Postgres spent executing statements, as a share of one core.
    exec_seconds_per_sec: round(ms / 1000 / seconds, 3),
    by_kind: Object.fromEntries([...kinds.entries()].sort((x, y) => y[1].calls - x[1].calls)
      .map(([kind, k]) => [kind, { per_sec: round(k.calls / seconds), exec_ms_per_sec: round(k.ms / seconds, 1), share_of_calls: round(k.calls / calls, 3), share_of_time: round(k.ms / ms, 3) }])),
    top_by_time: [...each].sort((x, y) => y.ms - x.ms).slice(0, 12).map(line),
    top_by_calls: [...each].sort((x, y) => y.calls - x.calls).slice(0, 12).map(line),
    sweeps: sweeps(each, seconds),
    database: Object.fromEntries(Object.entries(b.database).map(([db, v]) => [db, Object.fromEntries(Object.entries(v).map(([k, n]) => [k + '_per_sec', round((n - (a.database[db]?.[k] ?? 0)) / seconds)]))])),
  }
}

// The engine's two busiest tables over the window: their updates, and the
// share of them that were HOT (no index written, so no index grows); and each
// of their indexes that was read, how often and how many rows a read. Null for
// readings taken before run.sh took these.
function tables(a, b) {
  if (!a.tables || !b.tables) return null
  const seconds = b.at - a.at
  const by = (now, was, k) => (now?.[k] ?? 0) - (was?.[k] ?? 0)
  return {
    tables: Object.fromEntries(Object.entries(b.tables).map(([table, v]) => {
      const updates = by(v, a.tables[table], 'n_tup_upd'), hot = by(v, a.tables[table], 'n_tup_hot_upd')
      return [table, { updates_per_sec: round(updates / seconds), hot_share: updates ? round(hot / updates, 3) : null }]
    })),
    indexes: Object.fromEntries(Object.entries(b.indexes ?? {})
      .map(([index, v]) => [index, v.table, by(v, a.indexes?.[index], 'idx_scan'), by(v, a.indexes?.[index], 'idx_tup_read')])
      .filter(([, , scans]) => scans > 0)
      .sort((x, y) => y[2] - x[2])
      .map(([index, table, scans, read]) => [index, { table, scans_per_sec: round(scans / seconds), rows_read_per_scan: round(read / scans, 1) }])),
  }
}

// What `drive.mjs park` posted beside the step's jobs, without the runs' ids;
// and how often each HTTP sensor was polled, by the forge's count of GETs from
// when they were posted to when it was read.
function parked(dir, forge) {
  const p = readIfThere(dir, 'parked.json')
  if (!p) return null
  const urls = p.posted?.urls ?? 0
  const minutes = forge?.at && p.at_once_epoch_ms ? (forge.at - p.at_once_epoch_ms) / 60000 : 0
  return {
    asked: p.asked, posted: p.posted, refused: p.refused,
    url_sensor_gets_per_minute_each: urls && minutes > 0 && forge.waits != null ? round(forge.waits / urls / minutes) : null,
  }
}

function cpu(a, b) {
  const seconds = b.cpu_at - a.cpu_at
  return Object.fromEntries(Object.entries(b.cpu_usec).map(([name, usec]) =>
    [name, usec == null || a.cpu_usec[name] == null || seconds <= 0 ? null : round((usec - a.cpu_usec[name]) / 1e6 / seconds)]))
}

// Nearest rank, as the driver's.
function percentiles(values) {
  const v = values.filter(Number.isFinite)
  if (!v.length) return { count: 0 }
  const s = [...v].sort((x, y) => x - y)
  const at = q => s[Math.min(s.length - 1, Math.ceil(q * s.length) - 1)]
  return { count: s.length, min: s[0], p50: at(0.5), p95: at(0.95), p99: at(0.99), max: s.at(-1) }
}

// Webhook to job, run by run, for the events received in the
// window, from what the system wrote down (run.sh's latency-ci.json and
// latency-engine.json): the event received (`ci_events.received_at`), the
// engine's run made (`workflow_runs.created_at`), its job claimed (the
// outbox's `task.started`, written in the claim's transaction), dagron-ci's
// run recorded (`ci_runs.created_at`) and its first pending status posted
// (`ci_report_log`). All of these are of one Postgres's clock. Then two that
// are not: the job's own first line, of the job's clock, and the delivery
// sent, of the driver's; on this machine every container reads one kernel's
// clock. Null for a step from before run.sh took these.
function latency(dir, window) {
  const ci = readIfThere(dir, 'latency-ci.json'), engine = readIfThere(dir, 'latency-engine.json')
  if (!ci || !engine) return null
  const sent = new Map()
  try {
    for (const l of fs.readFileSync(path.join(dir, 'deliveries.jsonl'), 'utf8').split('\n').filter(Boolean)) {
      const d = JSON.parse(l)
      if (d.status === 202) sent.set(d.sha, d.at)
    }
  } catch { /* none */ }
  const job = new Map(engine.map(r => [r.run, r]))
  const from = window.from_epoch * 1000, to = window.to_epoch * 1000
  // Every event received in the window is counted, and one that never
  // became a run is counted as that: it has no span to measure, and is not
  // to vanish from the counts. A row is an event and one of its runs, or an
  // event alone (the LEFT JOIN's); `event` is its id, where run.sh wrote it.
  const received = ci.filter(c => c.received >= from && c.received < to)
  const events = new Set(received.map(c => c.event ?? c.sha))
  const withARun = new Set(received.filter(c => c.run).map(c => c.event ?? c.sha))
  const rows = received.filter(c => c.run).map(c => ({ ...c, ...job.get(c.run), sent: sent.get(c.sha) }))
  const of = f => percentiles(rows.map(r => { const v = f(r); return v == null || Number.isNaN(v) ? null : v }))
  const d = (x, y) => r => (r[x] == null || r[y] == null ? null : r[y] - r[x])
  return {
    events: events.size,
    events_without_a_run: events.size - withARun.size,
    // The spans below are of these: each run of an event received in the
    // window.
    runs: rows.length,
    without_a_claim: count(rows, r => r.claimed == null),
    ms: {
      // The system's own timestamps, end to end, on one clock.
      received_to_claimed: of(d('received', 'claimed')),
      // and the parts of it
      received_to_made: of(d('received', 'made')),
      made_to_claimed: of(d('made', 'claimed')),
      // From the claim to the job's first command, by the job's clock.
      claimed_to_first_line: of(d('claimed', 'first_line')),
      received_to_first_line: of(d('received', 'first_line')),
      // The delivery as the driver sent it, to the event received.
      sent_to_received: of(d('sent', 'received')),
      sent_to_first_line: of(d('sent', 'first_line')),
      // What dagron-ci itself records, and what the forge is first told.
      received_to_recorded: of(d('received', 'recorded')),
      received_to_pending_posted: of(d('received', 'pending')),
    },
  }
}
const count = (xs, f) => xs.filter(f).length

// What it costs, per 1,000 jobs, to run the cell's own parts:
// the compute and memory of Postgres, dagron-api, dagron-ci and the engines'
// own processes over the window, over the jobs that ended in it, at a price
// per vCPU-second and per GiB-second. The jobs' compute is not in it: with
// bring-your-own compute it is the customer's machines', and here it is
// shown apart, as the engines' cgroups' CPU less the engine processes' own
// (`process_cpu_seconds_total`), since the local executor runs a job inside
// its engine's container. Memory is each container's cgroup (memory.current,
// with the page cache it caused) for Postgres, dagron-api and dagron-ci, and
// each engine process's resident set at the window's end. The price is the
// environment's PRICE_VCPU_SECOND and PRICE_GB_SECOND, or AWS Fargate's
// Linux/x86 on-demand price in US East (N. Virginia), as its pricing page
// gave it on 2026-10-24: $0.000011244 a vCPU-second and $0.000001235 a
// GB-second (a GiB here). Null for a step from before run.sh took these.
const PRICE = {
  vcpu_second: Number(process.env.PRICE_VCPU_SECOND || 0.000011244),
  gb_second: Number(process.env.PRICE_GB_SECOND || 0.000001235),
  source: process.env.PRICE_SOURCE || 'AWS Fargate, Linux/x86, US East (N. Virginia), on demand, https://aws.amazon.com/fargate/pricing/, read 2026-10-24',
}
// The parts priced, each a container that run.sh names <NAME>-<part>.
const PRICED = [['Postgres', 'pg'], ['dagron-api', 'api'], ['dagron-ci', 'ci']]
// What a price needs that a step's readings lack, each said: a priced part's
// CPU time and memory at both of the window's ends, and each engine process's
// CPU and resident set. peek.mjs writes null for a container it could not
// read, and run.sh empty maps where peek.mjs failed; none of it is a zero.
function missingReadings(a, b, e, engineCount) {
  const missing = []
  const names = new Set([a.cpu_usec, b.cpu_usec, a.mem_bytes, b.mem_bytes].flatMap(m => Object.keys(m ?? {})))
  for (const [part, suffix] of PRICED) {
    const name = [...names].find(n => n.endsWith(`-${suffix}`))
    if (!name) { missing.push(`${part}: no reading of its container`); continue }
    for (const [what, at, m] of [['CPU', 'start', a.cpu_usec], ['CPU', 'end', b.cpu_usec], ['memory', 'start', a.mem_bytes], ['memory', 'end', b.mem_bytes]]) {
      if (!Number.isFinite(m?.[name])) missing.push(`${part} (${name}): its cgroup's ${what} at the window's ${at}`)
    }
  }
  for (const [what, each] of [['CPU', e?.cpu_cores_each], ['resident set', e?.resident_bytes_each]]) {
    for (let i = 0; i < engineCount; i++) {
      if (!Number.isFinite(each?.[i])) missing.push(`engine ${i + 1}: its process's ${what} (its /metrics)`)
    }
  }
  return missing
}
function cost(a, b, driver, sql, dir) {
  const engineRows = readIfThere(dir, 'latency-engine.json')
  if (!engineRows || !a.mem_bytes || !b.mem_bytes) return null
  const seconds = b.at - a.at
  const jobs = count(engineRows, r => r.finished != null && r.finished >= a.at * 1000 && r.finished < b.at * 1000)
  if (!jobs || seconds <= 0) return { jobs, seconds }
  const e = driver.engines_in_window
  // A part with no reading is not priced as nothing: the step has no price.
  const engineCount = driver.step.engines ?? e?.cpu_cores_each?.length ?? 0
  const missing = missingReadings(a, b, e, engineCount)
  if (missing.length) return { seconds: round(seconds), jobs_ended: jobs, incomplete: true, missing, price: PRICE }
  const used = name => (b.cpu_usec[name] == null || a.cpu_usec[name] == null ? null : (b.cpu_usec[name] - a.cpu_usec[name]) / 1e6)
  const names = Object.keys(b.cpu_usec)
  const engines = names.filter(n => /-engine-\d+$/.test(n))
  const others = names.filter(n => !engines.includes(n))
  const engineProcess = e.cpu_cores_each.reduce((n, c) => n + c, 0) * e.seconds
  // The engines' cgroups are not priced; the jobs' share of them is shown,
  // and is unknown where one of them has no reading.
  const engineCgroup = engines.length === engineCount && engines.every(c => used(c) != null) ? engines.reduce((n, c) => n + used(c), 0) : null
  const cpuSeconds = Object.fromEntries(others.map(n => [n, used(n)]))
  const controlCpu = others.reduce((n, c) => n + (used(c) ?? 0), 0) + engineProcess
  const meanBytes = n => (a.mem_bytes[n] == null || b.mem_bytes[n] == null ? 0 : (a.mem_bytes[n] + b.mem_bytes[n]) / 2)
  const GiB = 2 ** 30
  const memGiB = (others.reduce((n, c) => n + meanBytes(c), 0) + e.resident_bytes_each.reduce((n, r) => n + (r ?? 0), 0)) / GiB
  const per = x => round((x * 1000) / jobs, 3)
  const dbBytes = Object.keys(b.db_bytes ?? {}).reduce((n, db) => n + b.db_bytes[db] - (a.db_bytes?.[db] ?? b.db_bytes[db]), 0)
  const vcpuSeconds = per(controlCpu), gibSeconds = per(memGiB * seconds)
  return {
    seconds: round(seconds), jobs_ended: jobs,
    cpu_seconds_in_window: { ...Object.fromEntries(Object.entries(cpuSeconds).map(([k, v]) => [k, v == null ? null : round(v)])), engine_processes: round(engineProcess), jobs_in_the_engines: engineCgroup == null ? null : round(engineCgroup - engineProcess) },
    memory_gib: { mean_by_container: Object.fromEntries(others.map(n => [n, round(meanBytes(n) / GiB, 3)])), engine_processes_at_end: round(e.resident_bytes_each.reduce((n, r) => n + (r ?? 0), 0) / GiB, 3), total: round(memGiB, 3) },
    per_1000_jobs: {
      vcpu_seconds: vcpuSeconds,
      gib_seconds: gibSeconds,
      postgres_exec_seconds: per(sql.exec_seconds_per_sec * seconds),
      database_growth_bytes: Math.round((dbBytes * 1000) / jobs),
      jobs_vcpu_seconds_not_priced: engineCgroup == null ? null : per(engineCgroup - engineProcess),
      dollars: round(vcpuSeconds * PRICE.vcpu_second + gibSeconds * PRICE.gb_second, 4),
    },
    price: PRICE,
  }
}

function summarize(dir) {
  const driver = read(dir, 'driver.json'), a = read(dir, 'at-window-start.json'), b = read(dir, 'at-window-end.json')
  const ci = read(dir, 'ci.json'), engine = read(dir, 'engine.json')
  const sql = statements(a, b)
  const held = driver.jobs_running_in_window.mean ?? 0
  const forge = readIfThere(dir, 'forge.json')
  // Whether dagron-ci had done with the step's runs when the forge and the
  // databases were read (run.sh's followed.json): where it had not, the
  // counts are not final, and the step's report says so. Null for a step
  // from before run.sh waited.
  const followed = readIfThere(dir, 'followed.json')
  const summary = {
    step: driver.step,
    ...(followed ? { final: followed.final === true, followed } : {}),
    began: driver.began,
    window: driver.window,
    held: { asked: driver.step.jobs, running: driver.jobs_running_in_window, at_window_end: driver.at_window_end },
    deliveries: driver.deliveries,
    events: ci,
    jobs: engine,
    // What the forge was asked and told over the whole step, the pile-up and
    // the drain included: a listing and a file read for each event, and each
    // job's statuses.
    forge,
    parked: parked(dir, forge),
    engines: driver.engines_in_window,
    statements: sql,
    tables: tables(a, b),
    // What a running job costs the database each minute it runs, by kind.
    statements_per_running_job_per_minute: held ? Object.fromEntries(JOB_KINDS.map(k => [k, round((sql.by_kind[k]?.per_sec ?? 0) * 60 / held)])) : null,
    cpu_cores: cpu(a, b),
    drain: driver.drain,
    webhook_to_job: latency(dir, driver.window),
    cost: cost(a, b, driver, sql, dir),
  }
  fs.writeFileSync(path.join(dir, 'summary.json'), JSON.stringify(summary, null, 2) + '\n')
  return summary
}

const ms = v => v == null ? '—' : v >= 1000 ? `${(v / 1000).toFixed(1)} s` : `${Math.round(v)} ms`
const secs = v => v == null ? '—' : v === Infinity ? 'over 300 s' : v >= 1 ? `${v} s` : `${round(v * 1000, 1)} ms`
function row(s) {
  const failed = (s.jobs.jobs_by_status?.failed ?? 0) + (s.events.events_by_state?.failed ?? 0)
  return [
    s.step.jobs,
    Math.round(s.held.running.mean ?? 0),
    s.step.rate_per_sec.toFixed(2),
    `${ms(s.deliveries.ack_ms_in_window.p50)} / ${ms(s.deliveries.ack_ms_in_window.p99)}`,
    `${ms(s.events.event_to_run_ms_in_window?.p50)} / ${ms(s.events.event_to_run_ms_in_window?.p95)}`,
    `${ms(s.jobs.run_to_job_started_ms_in_window?.p50)} / ${ms(s.jobs.run_to_job_started_ms_in_window?.p95)}`,
    `${secs(s.engines.reconcile_tick_seconds.p50_le)} / ${secs(s.engines.reconcile_tick_seconds.p99_le)}`,
    Math.round(s.statements.per_sec),
    round(s.statements.by_kind['lease renewal (a heartbeat)']?.per_sec ?? 0, 1),
    s.cpu_cores[Object.keys(s.cpu_cores)[0]] ?? '—',
    s.held.at_window_end.events_pending ?? '—',
    failed,
  ]
}
const HEAD = ['jobs asked', 'jobs running', 'deliveries/s', 'ack p50 / p99', 'event to run p50 / p95', 'run to job start p50 / p95', 'tick p50 / p99 (bucket)', 'statements/s', 'heartbeats/s', 'Postgres cores', 'events waiting at the end', 'failed']
const table = rows => [HEAD, HEAD.map(() => '---'), ...rows].map(r => `| ${r.join(' | ')} |`).join('\n')

// What a step's row is not to be taken for, each a line after its table.
const notFinal = s => s.final === false
  ? `dagron-ci still followed ${s.followed.runs_still_followed ?? 'an unknown number'} of its runs after ${s.followed.waited_seconds} s, so its forge's and databases' counts were read before they were final`
  : null
const unpriced = s => s.cost?.incomplete ? `no price: ${s.cost.missing.join('; ')}` : null
const unrun = s => s.webhook_to_job?.events_without_a_run ? `${s.webhook_to_job.events_without_a_run} of the ${s.webhook_to_job.events} events received in the window had no run` : null
const unclaimed = s => s.webhook_to_job?.without_a_claim ? `${s.webhook_to_job.without_a_claim} of its ${s.webhook_to_job.runs} runs had no claim` : null
const notes = (named, checks) => named.flatMap(([name, s]) => checks.map(f => f(s)).filter(Boolean).map(n => `- **${name}**: ${n}.`))
const withNotes = (text, lines) => lines.length ? `${text}\n\n${lines.join('\n')}` : text

// The public proof's two figures, a row a step: webhook to job, and what the
// cell's own parts cost per 1,000 jobs.
const q3 = p => p?.count ? `${ms(p.p50)} / ${ms(p.p95)} / ${ms(p.p99)}` : '—'
const PROOF_HEAD = ['step', 'jobs asked', 'job length', 'runs measured', 'received to claimed p50 / p95 / p99', 'received to first line p50 / p95 / p99', 'sent to first line p50 / p95 / p99', 'received to pending posted p50 / p95', 'vCPU-s / 1,000 jobs', 'GiB-s / 1,000 jobs', 'Postgres exec s / 1,000 jobs', 'jobs\' own vCPU-s / 1,000 (not priced)', '$ / 1,000 jobs']
function proofRow(name, s) {
  const l = s.webhook_to_job, c = s.cost?.per_1000_jobs
  return [s.final === false ? `${name} (not final)` : name, s.step.jobs, `${s.step.secs} s`, l?.runs ?? '—', q3(l?.ms.received_to_claimed), q3(l?.ms.received_to_first_line), q3(l?.ms.sent_to_first_line),
    l?.ms.received_to_pending_posted.count ? `${ms(l.ms.received_to_pending_posted.p50)} / ${ms(l.ms.received_to_pending_posted.p95)}` : '—',
    c?.vcpu_seconds ?? '—', c?.gib_seconds ?? '—', c?.postgres_exec_seconds ?? '—', c?.jobs_vcpu_seconds_not_priced ?? '—', c ? `$${c.dollars}` : s.cost?.incomplete ? 'no price: readings missing' : '—']
}

const args = process.argv.slice(2)
if (args[0] === '--table') {
  const named = args.slice(1).map(dir => [path.basename(dir), read(dir, 'summary.json')])
  console.log(withNotes(table(named.map(([, s]) => row(s))), notes(named, [notFinal])))
} else if (args[0] === '--proof') {
  const named = args.slice(1).map(dir => [path.basename(dir), read(dir, 'summary.json')])
  const rows = named.map(([name, s]) => proofRow(name, s))
  console.log(withNotes([PROOF_HEAD, PROOF_HEAD.map(() => '---'), ...rows].map(r => `| ${r.join(' | ')} |`).join('\n'), notes(named, [notFinal, unrun, unclaimed, unpriced])))
  console.log(`\nPriced at $${PRICE.vcpu_second} a vCPU-second and $${PRICE.gb_second} a GiB-second: ${PRICE.source}.`)
} else if (args.length === 1) {
  const s = summarize(args[0])
  console.log(table([row(s)]))
  if (s.final === false) console.log(`\nNOT FINAL: ${notFinal(s)}`)
  console.log('\nstatements a second, by what they are for (and the time Postgres spent on them):')
  for (const [kind, k] of Object.entries(s.statements.by_kind)) console.log(`  ${String(k.per_sec).padStart(9)}  ${String(k.exec_ms_per_sec).padStart(8)} ms/s  ${kind}`)
  console.log('\nthe sweeps that ran, passes a second, and what a pass took (time, rows, buffers):')
  for (const [sweep, w] of Object.entries(s.statements.sweeps)) {
    if (w.per_sec) console.log(`  ${String(w.per_sec).padStart(9)}  ${String(w.mean_ms).padStart(8)} ms  ${String(w.rows_per_call).padStart(7)} rows  ${String(w.blks_per_call).padStart(7)} buffers  ${sweep}`)
  }
  const hot = s.tables?.tables?.task_runs?.hot_share
  if (hot != null) console.log(`\ntask_runs' updates that were HOT: ${round(hot * 100, 1)}%`)
  if (s.parked) console.log(`\nparked beside the jobs: ${JSON.stringify(s.parked.posted)}${s.parked.url_sensor_gets_per_minute_each != null ? `; each HTTP sensor was polled ${s.parked.url_sensor_gets_per_minute_each} times a minute` : ''}`)
  console.log(`\nCPU, in cores: ${Object.entries(s.cpu_cores).map(([n, c]) => `${n} ${c}`).join(', ')}`)
  // A step whose jobs failed has measured something else as well: say what.
  for (const f of s.jobs.failed_jobs_say ?? []) console.log(`\n${f.jobs} failed jobs said: ${f.output || '(nothing)'}`)
  if (s.drain.loop_busy_after_empty_seconds > 5) console.log(`\nwith nothing left to run, the engines went on ticking for ${Math.round(s.drain.loop_busy_after_empty_seconds)} s: ${s.drain.ticks_after_empty} ticks`)
  if (!s.drain.quiet) console.log(`\nNOT quiet ${Math.round(s.drain.seconds)} s after the last delivery: ${s.drain.running} running, ${s.drain.ready} ready, ${s.drain.events_pending} events waiting`)
  if (s.webhook_to_job) {
    const l = s.webhook_to_job.ms
    const w = s.webhook_to_job
    console.log(`\nwebhook to job, ${w.events} events received in the window, ${w.events_without_a_run} of them without a run; ${w.runs} runs of them, ${w.without_a_claim} without a claim; p50 / p95 / p99: received to claimed ${q3(l.received_to_claimed)}; to the job's first line ${q3(l.received_to_first_line)}; sent to first line ${q3(l.sent_to_first_line)}`)
  }
  if (s.cost?.incomplete) console.log(`\nno cost per 1,000 jobs, since readings are missing: ${s.cost.missing.join('; ')}`)
  if (s.cost?.per_1000_jobs) {
    const c = s.cost.per_1000_jobs
    console.log(`\nper 1,000 jobs of ${s.step.secs} s (${s.cost.jobs_ended} ended in the window): ${c.vcpu_seconds} vCPU-s and ${c.gib_seconds} GiB-s of the cell's own parts, ${c.postgres_exec_seconds} s of Postgres statements, $${c.dollars}; the jobs' own ${c.jobs_vcpu_seconds_not_priced} vCPU-s not priced`)
  }
} else {
  console.error('usage: report.mjs DIR | report.mjs --table DIR…')
  process.exit(2)
}

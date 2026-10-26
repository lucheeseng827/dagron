// The durability demo's checks: what a step of `run.sh durable`
// left in its directory, held to what is to be true after a part of the
// cell was killed with SIGKILL mid-pipeline and started again.
//
//   node durable.mjs DIR            write DIR/durable.json, print each check;
//                                   exit 1 if one does not hold
//   node durable.mjs --table DIR…   one row a step
//
// It reads what the system wrote down and what the jobs did, never what the
// driver thought: dagron-ci's events, runs and statuses posted (ci.json),
// the engine's runs and tasks (engine.json), the forge's record of every
// status it was sent (statuses.jsonl), and the jobs' own marks of each
// execution's start and end (marks.txt, on a volume every engine shares).
// pushes.jsonl and kill.json say what was sent, and when the kill was.
import fs from 'node:fs'
import path from 'node:path'

const JOBS = ['build', 'test', 'ship']
const AFTER = { test: 'build', ship: 'test' }
const read = (dir, name) => JSON.parse(fs.readFileSync(path.join(dir, name), 'utf8'))
const lines = (dir, name) => { try { return fs.readFileSync(path.join(dir, name), 'utf8').split('\n').filter(Boolean) } catch { return [] } }
const count = (xs, f) => xs.filter(f).length
const by = (xs, key) => { const m = new Map(); for (const x of xs) { const k = key(x); m.set(k, [...(m.get(k) ?? []), x]) } return m }
const s = ms => ms == null ? '—' : `${(ms / 1000).toFixed(1)} s`

function check(dir) {
  const kill = read(dir, 'kill.json')
  const pushes = lines(dir, 'pushes.jsonl').map(l => JSON.parse(l))
  const again = fs.existsSync(path.join(dir, 'again.json')) ? read(dir, 'again.json') : []
  const ci = read(dir, 'ci.json'), engine = read(dir, 'engine.json')
  const shas = new Set(pushes.map(p => p.sha))
  const statuses = lines(dir, 'statuses.jsonl').map(l => JSON.parse(l)).filter(x => shas.has(x.sha))
  // start|done <execution> <ns> <container> <run id> <task id> <job>
  const marks = lines(dir, 'marks.txt').map(l => l.split(' ')).filter(m => m.length === 7)
    .map(([what, exec, ns, host, run, task, job]) => ({ what, exec, at: Number(ns) / 1e6, host, run, task, job }))
  const killedHosts = new Set(kill.hostnames)
  const out = []
  const ok = (name, holds, detail) => out.push({ check: name, holds: Boolean(holds), detail })

  // Intake: one event a push, one run an event, one engine run a commit.
  const events = ci.events, runs = ci.runs
  const eventsBySha = by(events, e => e.sha), runsByEvent = by(runs, r => r.event)
  ok('every push is one event', events.length === pushes.length && [...shas].every(sha => eventsBySha.get(sha)?.length === 1),
    `${pushes.length} pushes, ${events.length} events; ${pushes.reduce((n, p) => n + p.sent.length, 0)} deliveries sent in all, ${pushes.reduce((n, p) => n + p.sent.length - 1, 0) - again.length} of them again after one not taken, and ${again.length} again once taken (answered ${JSON.stringify(again.map(a => a.status))})`)
  ok('every event is done, with one run', events.every(e => e.state === 'done' && runsByEvent.get(e.id)?.length === 1),
    `events by state ${JSON.stringify(Object.fromEntries([...by(events, e => e.state)].map(([k, v]) => [k, v.length])))}; runs by event: ${JSON.stringify(Object.fromEntries([...by(events, e => runsByEvent.get(e.id)?.length ?? 0)].map(([k, v]) => [k, v.length])))}; event attempts ${JSON.stringify(Object.fromEntries([...by(events, e => e.attempts)].map(([k, v]) => [k, v.length])))}`)
  const engineBySha = by(engine.runs, r => r.sha)
  ok('the engine made one run for each commit', [...shas].every(sha => engineBySha.get(sha)?.length === 1) && engine.runs.length === shas.size,
    `${engine.runs.length} engine runs for ${shas.size} commits`)

  // The pipelines' ends.
  ok('every pipeline succeeded, in the engine and in dagron-ci',
    engine.runs.length === shas.size && engine.runs.every(r => r.status === 'succeeded') && runs.every(r => r.status === 'succeeded'),
    `engine ${JSON.stringify(Object.fromEntries([...by(engine.runs, r => r.status)].map(([k, v]) => [k, v.length])))}; dagron-ci ${JSON.stringify(Object.fromEntries([...by(runs, r => r.status)].map(([k, v]) => [k, v.length])))}`)
  const tasksByRun = by(engine.tasks, t => t.run)
  ok('every job succeeded', engine.runs.every(r => (tasksByRun.get(r.id) ?? []).length === JOBS.length && tasksByRun.get(r.id).every(t => t.status === 'succeeded')),
    `jobs by status ${JSON.stringify(Object.fromEntries([...by(engine.tasks, t => t.status)].map(([k, v]) => [k, v.length])))}; by attempt ${JSON.stringify(Object.fromEntries([...by(engine.tasks, t => t.attempt)].map(([k, v]) => [k, v.length])))}`)

  // Executions, by the jobs' own marks.
  const execs = new Map()
  for (const m of marks) {
    const e = execs.get(m.exec) ?? { exec: m.exec, host: m.host, run: m.run, task: m.task, job: m.job }
    e[m.what] = m.at
    execs.set(m.exec, e)
  }
  const byTask = by([...execs.values()], e => e.task)
  const tasks = engine.tasks
  ok('every job ran to its end exactly once', tasks.every(t => count(byTask.get(t.id) ?? [], e => e.done != null) === 1),
    `${count([...execs.values()], e => e.done != null)} executions ended, for ${tasks.length} jobs`)
  const cut = [...execs.values()].filter(e => e.done == null)
  const cutByKill = cut.filter(e => killedHosts.has(e.host) && e.start < kill.killed_by)
  ok('an execution that never ended was one the kill cut short', cut.length === cutByKill.length,
    `${cut.length} cut short, ${cutByKill.length} of them on a container killed, begun before the kill${cut.length > cutByKill.length ? `; not so: ${JSON.stringify(cut.filter(e => !cutByKill.includes(e)).slice(0, 3))}` : ''}`)
  // Order within a job: the execution that ended is the one begun last, and
  // none began after one had ended.
  const startedTwice = tasks.filter(t => (byTask.get(t.id) ?? []).length > 1)
  const lastNotEnded = [], againAfterEnd = []
  for (const t of tasks) {
    const xs = byTask.get(t.id) ?? []
    if (!xs.length) continue
    const last = xs.reduce((a, b) => (b.start > a.start ? b : a))
    if (last.done == null) lastNotEnded.push({ task: t.id, job: t.name })
    if (xs.some(e => e.done != null && xs.some(f => f !== e && f.start >= e.done))) againAfterEnd.push({ task: t.id, job: t.name })
  }
  ok("a job's execution that ended was the last to begin, and none began after it ended",
    lastNotEnded.length === 0 && againAfterEnd.length === 0,
    `${startedTwice.length} jobs began more than once (${JSON.stringify(Object.fromEntries([...by(startedTwice, t => t.name)].map(([k, v]) => [k, v.length])))}); ${lastNotEnded.length} whose last to begin did not end, ${againAfterEnd.length} begun again after an end${lastNotEnded.length + againAfterEnd.length ? `: ${JSON.stringify([...lastNotEnded, ...againAfterEnd].slice(0, 3))}` : ''}`)
  // Order: no execution of a job began before the execution of the job it
  // depends on that ended.
  const endOf = (run, job) => [...execs.values()].find(e => e.run === run && e.job === job && e.done != null)?.done
  const early = [...execs.values()].filter(e => AFTER[e.job] && !(endOf(e.run, AFTER[e.job]) <= e.start))
  ok('no job began before the one it depends on ended', early.length === 0, `${early.length} began early`)

  // Statuses at the forge: per commit and job, the last is success, posted
  // once, and nothing said failure.
  const contexts = JOBS.map(j => `dagron-ci/proof/${j} (push)`)
  const finals = [], twice = [], bad = []
  for (const sha of shas) {
    for (const context of contexts) {
      const of = statuses.filter(x => x.context === context && x.sha === sha)
      finals.push(of.at(-1)?.state ?? 'none')
      if (count(of, x => x.state === 'success') !== 1) twice.push({ sha, context, success: count(of, x => x.state === 'success') })
      bad.push(...of.filter(x => x.state !== 'pending' && x.state !== 'success'))
    }
  }
  ok("each job's last status at the forge is success, posted once", finals.every(f => f === 'success') && twice.length === 0 && bad.length === 0,
    `last states ${JSON.stringify(Object.fromEntries([...by(finals, f => f)].map(([k, v]) => [k, v.length])))}; statuses ${JSON.stringify(Object.fromEntries([...by(statuses, x => x.state)].map(([k, v]) => [k, v.length])))}${twice.length ? `; not once: ${JSON.stringify(twice.slice(0, 3))}` : ''}${bad.length ? `; ${bad.length} neither pending nor success` : ''}`)
  // dagron-ci's own record against the forge's: each (commit, context,
  // state) the forge was sent, as many times, is a row of ci_report_log that
  // says posted. dagron-ci writes the row after the post, so a dagron-ci
  // killed between the two leaves a post with no row: that is allowed only
  // when dagron-ci was what was killed, and the post came between the kill's
  // start and its end. A row the forge never had is never allowed.
  const notPosted = ci.posted.filter(p => p.outcome !== 'posted')
  const said = x => `${x.sha} ${x.context} ${x.state}`
  const rows = new Map([...by(ci.posted.filter(p => p.outcome === 'posted'), said)].map(([k, v]) => [k, v.length]))
  const unrecorded = []
  for (const x of statuses) {
    const n = rows.get(said(x)) ?? 0
    if (n > 0) rows.set(said(x), n - 1)
    else unrecorded.push(x)
  }
  const neverSent = [...rows].filter(([, n]) => n > 0)
  const asKilled = unrecorded.filter(x => kill.target === 'ci' && x.at >= kill.killed_at && x.at <= kill.killed_by)
  const unrecordedElse = unrecorded.filter(x => !asKilled.includes(x))
  ok("dagron-ci's own record (ci_report_log) has each status the forge was sent, but those posted as it was killed",
    notPosted.length === 0 && neverSent.length === 0 && unrecordedElse.length === 0,
    `${statuses.length} at the forge, ${ci.posted.length} in ci_report_log; ${unrecorded.length} at the forge with no row${unrecorded.length ? `, ${asKilled.length} of them posted as dagron-ci was killed (${JSON.stringify(Object.fromEntries([...by(asKilled, x => x.state)].map(([k, v]) => [k, v.length])))}, ${asKilled.length ? `${Math.min(...asKilled.map(x => x.at)) - kill.killed_at} to ${Math.max(...asKilled.map(x => x.at)) - kill.killed_at} ms after the kill began, which took ${kill.killed_by - kill.killed_at} ms` : 'none'})` : ''}; ${neverSent.reduce((n, [, k]) => n + k, 0)} rows the forge never had${notPosted.length ? `; ${notPosted.length} not posted: ${JSON.stringify(Object.fromEntries([...by(notPosted, p => p.outcome)].map(([k, v]) => [k, v.length])))}` : ''}${unrecordedElse.length ? `; with no row and not as it was killed: ${JSON.stringify(unrecordedElse.slice(0, 3))}` : ''}${neverSent.length ? `; rows the forge never had: ${JSON.stringify(neverSent.slice(0, 3))}` : ''}`)

  // That the kill caught something: a demo that killed an idle part shows
  // nothing.
  const down = e => e >= kill.killed_at && e <= kill.ready_at
  const engineKilled = kill.target === 'engine' || kill.target === 'engines'
  let caught
  if (engineKilled) caught = { holds: cutByKill.length > 0, detail: `${cutByKill.length} executions were running on what was killed` }
  else if (kill.at === 'mid') {
    const n = count([...execs.values()], e => down(e.start) || down(e.done))
    caught = { holds: n > 0, detail: `${n} executions began or ended while ${kill.target} was down` }
  } else {
    const late = count(runs, r => r.recorded > kill.killed_at)
    caught = { holds: late > 0, detail: `${late} of ${runs.length} runs were recorded after the kill` }
  }
  ok('the kill caught the pipelines in flight', caught.holds, caught.detail)

  // How long it took: the kill to each job begun again, and to the last
  // pipeline's end, against the same pipelines with nothing killed.
  const again2 = cutByKill.map(e => (byTask.get(e.task) ?? []).filter(x => x.start > e.start).map(x => x.start - kill.killed_at)).flat()
  const lastEnd = Math.max(...runs.map(r => r.ended ?? 0))
  const firstPush = Math.min(...pushes.map(p => p.sent[0]?.at ?? Infinity))
  const shape = kill.shape.split('/').map(Number)
  const summary = {
    target: kill.target, at: kill.at, down: kill.down, pipelines: kill.pipelines, shape: kill.shape,
    killed: kill.victims, killed_at: new Date(kill.killed_at).toISOString(),
    checks: out, held: out.every(c => c.holds),
    timing: {
      kill_to_ready_ms: kill.ready_at - kill.killed_at,
      kill_to_jobs_begun_again_ms: again2.length ? { min: Math.min(...again2), max: Math.max(...again2) } : null,
      first_push_to_last_pipeline_recorded_ended_ms: Number.isFinite(firstPush) && lastEnd ? lastEnd - firstPush : null,
      jobs_alone_ms: shape.reduce((a, b) => a + b, 0) * 1000,
    },
    executions: execs.size, cut_short: cutByKill.length, jobs: tasks.length, jobs_begun_more_than_once: startedTwice.length,
  }
  fs.writeFileSync(path.join(dir, 'durable.json'), JSON.stringify(summary, null, 2) + '\n')
  return summary
}

function print(summary) {
  console.log(`\n${summary.target} killed at ${summary.at} (${summary.killed.join(', ')}), down ${summary.down} s, ${summary.pipelines} pipelines of ${summary.shape} s:`)
  for (const c of summary.checks) console.log(`  ${c.holds ? 'ok  ' : 'FAIL'}  ${c.check}: ${c.detail}`)
  const t = summary.timing
  console.log(`  the kill to answering again ${s(t.kill_to_ready_ms)}; to the jobs begun again ${t.kill_to_jobs_begun_again_ms ? `${s(t.kill_to_jobs_begun_again_ms.min)} to ${s(t.kill_to_jobs_begun_again_ms.max)}` : '—'}; first push to the last pipeline's end in dagron-ci ${s(t.first_push_to_last_pipeline_recorded_ended_ms)}, where the jobs alone are ${s(t.jobs_alone_ms)}`)
}

const HEAD = ['step', 'killed', 'at', 'down', 'pipelines', 'jobs', 'executions cut short', 'jobs begun more than once', 'kill to jobs begun again', 'first push to last end', 'checks']
const row = (name, x) => [name, x.target, x.at, `${x.down} s`, x.pipelines, x.jobs, x.cut_short, x.jobs_begun_more_than_once,
  x.timing.kill_to_jobs_begun_again_ms ? `${s(x.timing.kill_to_jobs_begun_again_ms.min)} to ${s(x.timing.kill_to_jobs_begun_again_ms.max)}` : '—',
  s(x.timing.first_push_to_last_pipeline_recorded_ended_ms), x.held ? `all ${x.checks.length} hold` : `FAILED: ${x.checks.filter(c => !c.holds).map(c => c.check).join('; ')}`]

const args = process.argv.slice(2)
if (args[0] === '--table') {
  const rows = args.slice(1).map(dir => row(path.basename(dir), read(dir, 'durable.json')))
  console.log([HEAD, HEAD.map(() => '---'), ...rows].map(r => `| ${r.join(' | ')} |`).join('\n'))
} else if (args.length === 1) {
  const summary = check(args[0])
  print(summary)
  if (!summary.held) process.exit(1)
} else {
  console.error('usage: durable.mjs DIR | durable.mjs --table DIR…')
  process.exit(2)
}

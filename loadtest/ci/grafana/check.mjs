// Does a live cell give the dashboard what it draws?
//
//   node check.mjs DASHBOARD.json URL…
//       Each URL is a /metrics to read: every engine's, and dagron-ci's. A
//       series the dashboard names and none of them publishes is printed.
//
//   node check.mjs DASHBOARD.json --prometheus URL [WINDOW]
//       Each panel's every query is asked of that Prometheus for the last
//       WINDOW (15m unless given), and one that is refused, or had no series
//       at any time in it, is printed. A window and not this instant: a
//       gauge of running tasks is not published while none is running.
//
// Either way the exit is 1 when something is printed. Read it while the cell
// has done some work: a counter with a label is not published before its
// first count, and a rate has nothing to say of an idle minute.
//
// The series of other exporters (cAdvisor, postgres-exporter) are named and,
// read from /metrics, not looked for: no cell publishes them itself. Asked of
// a Prometheus they are looked for like the rest, and reported apart, since
// only a Prometheus that scrapes those exporters has them.
import fs from 'node:fs'

const [file, ...rest] = process.argv.slice(2)
if (!file || rest.length === 0) {
  console.error('usage: check.mjs DASHBOARD.json URL… | check.mjs DASHBOARD.json --prometheus URL [WINDOW]')
  process.exit(2)
}
const EXTERNAL = /\b(container_|pg_stat_|kube_|node_)/
const WORDS = new Set(['sum', 'max', 'min', 'avg', 'rate', 'by', 'le', 'histogram_quantile', 'outcome', 'status', 'datname', 'pod', 'container', 'job', 'instance'])
const dashboard = JSON.parse(fs.readFileSync(file, 'utf8'))
const queries = dashboard.panels.flatMap(panel => (panel.targets ?? []).map(target => ({ panel: panel.title, expr: target.expr })))

if (rest[0] === '--prometheus') {
  const base = rest[1], window = rest[2] || '15m'
  if (!base) { console.error('--prometheus needs a URL'); process.exit(2) }
  // The dashboard's own variables, at what they default to.
  const variables = Object.fromEntries(dashboard.templating.list.filter(v => v.current?.value != null).map(v => [v.name, v.current.value]))
  variables.__rate_interval = '1m'
  let empty = 0, external = 0, answered = 0
  for (const { panel, expr } of queries) {
    const query = expr.replace(/\$(\w+)/g, (whole, name) => variables[name] ?? whole)
    const over = `last_over_time((${query})[${window}:15s])`
    const res = await fetch(`${base}/api/v1/query?query=${encodeURIComponent(over)}`)
    const body = await res.json().catch(() => ({}))
    if (body.status !== 'success') { empty++; console.log(`REFUSED  ${panel}: ${query}\n         ${body.error ?? res.status}`); continue }
    if (body.data.result.length === 0) {
      if (EXTERNAL.test(expr)) { external++; console.log(`no series, and it is another exporter's: ${panel}`) }
      else { empty++; console.log(`NO SERIES  ${panel}: ${query}`) }
      continue
    }
    answered++
  }
  console.log(`${queries.length} queries; ${answered} answered with series${external ? `; ${external} of another exporter's with none` : ''}`)
  process.exit(empty ? 1 : 0)
}

const named = new Set()
for (const { expr } of queries) {
  // A metric's name is a word with an underscore that is no function, no
  // label and no variable.
  const bare = expr.replace(/\$\w+/g, ' ').replace(/"[^"]*"/g, ' ')
  for (const word of bare.match(/[a-zA-Z_:][a-zA-Z0-9_:]*/g) ?? []) {
    if (!WORDS.has(word) && word.includes('_')) named.add(word)
  }
}
const published = new Set()
for (const url of rest) {
  const text = await (await fetch(url)).text()
  for (const line of text.split('\n')) {
    if (!line || line[0] === '#') continue
    published.add(line.split(/[{ ]/)[0])
  }
}
const external = [...named].filter(n => EXTERNAL.test(n)).sort()
const missing = [...named].filter(n => !EXTERNAL.test(n) && !published.has(n)).sort()
console.log(`${named.size} series named; ${named.size - external.length - missing.length} published by this cell`)
if (external.length) console.log(`from other exporters, not looked for: ${external.join(', ')}`)
if (missing.length) {
  console.log(`NOT published by this cell: ${missing.join(', ')}`)
  process.exit(1)
}

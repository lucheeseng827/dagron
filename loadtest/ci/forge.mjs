// The synthetic forge of the CI load harness: the part of GitHub's REST API
// that dagron-ci calls for a push, answered from memory.
//
// A push delivery names a commit. dagron-ci then asks the forge which
// pipelines the repository has at that commit (the contents API, a listing
// and then each file), and tells it how each job went (the statuses API).
// This answers both for any repository and any commit: every commit "has"
// the pipeline files in PIPELINES_DIR, and every status is taken and written
// down with the time it came. There are no git objects here, so a job that
// checks its commit out has nothing to fetch: the harness's jobs do not.
//
// It is not GitHub. It answers what dagron-ci's code asks of GitHub
// (crates/dagron-ci/src/forge.rs) and nothing was checked against github.com.
// It never refuses, limits or pages: what a forge does on a bad day is a
// functional test's business, not a load test's.
//
//   PORT            where it listens (default 9080)
//   PIPELINES_DIR   the *.yaml files every commit is said to hold; a
//                   repository with a directory of its own there,
//                   PIPELINES_DIR/<owner>/<name>/, holds those instead
//   DATA_DIR        where statuses.jsonl is appended (default /data)
//
//   GET  /_load/health   200 once it is serving
//   GET  /_load/stats    what it has answered and been told, as counts
//   POST /_load/reset    forgets the counts (statuses.jsonl is kept)
//   GET  /_load/wait     503, and counted: the endpoint of the HTTP sensors
//                        `drive.mjs park` makes, which are never ready, so
//                        that how often the engines poll each one shows
import http from 'node:http'
import fs from 'node:fs'
import path from 'node:path'

const PORT = Number(process.env.PORT || 9080)
const PIPELINES_DIR = process.env.PIPELINES_DIR || path.join(import.meta.dirname, 'pipelines')
const DATA_DIR = process.env.DATA_DIR || '/data'
const DIR = '.dagron/ci'

// Read once: a load test must not measure this process's disk.
const filesOf = dir => fs.readdirSync(dir)
  .filter(name => name.endsWith('.yaml') || name.endsWith('.yml'))
  .sort()
  .map(name => ({ name, text: fs.readFileSync(path.join(dir, name)) }))
const listingOf = files => Buffer.from(JSON.stringify(files.map(p => ({
  type: 'file', name: p.name, path: `${DIR}/${p.name}`, size: p.text.length,
}))))
const pipelines = filesOf(PIPELINES_DIR)
if (pipelines.length === 0) throw new Error(`no pipeline files in ${PIPELINES_DIR}`)
const listing = listingOf(pipelines)
// The repositories with pipelines of their own, PIPELINES_DIR/<owner>/<name>/:
// the durability demo's, which no load step's commit holds.
const own = new Map()
const dirs = dir => fs.readdirSync(dir, { withFileTypes: true }).filter(d => d.isDirectory()).map(d => d.name)
for (const owner of dirs(PIPELINES_DIR)) {
  for (const name of dirs(path.join(PIPELINES_DIR, owner))) {
    const files = filesOf(path.join(PIPELINES_DIR, owner, name))
    if (files.length) own.set(`${owner}/${name}`.toLowerCase(), { files, listing: listingOf(files) })
  }
}
const of = (owner, name) => own.get(`${owner}/${name}`.toLowerCase()) ?? { files: pipelines, listing }

fs.mkdirSync(DATA_DIR, { recursive: true })
const statusLog = fs.createWriteStream(path.join(DATA_DIR, 'statuses.jsonl'), { flags: 'a' })
let counts
const reset = () => { counts = { listings: 0, files: 0, statuses: {}, waits: 0, other: {}, since: Date.now() } }
reset()

const send = (res, status, body, type = 'application/json') => {
  res.writeHead(status, { 'content-type': type, 'content-length': body.length })
  res.end(body)
}
const json = (res, status, value) => send(res, status, Buffer.from(JSON.stringify(value)))

const server = http.createServer((req, res) => {
  const url = new URL(req.url, 'http://forge')
  const parts = url.pathname.split('/').filter(Boolean).map(decodeURIComponent)
  if (parts[0] === '_load') {
    if (parts[1] === 'health') return json(res, 200, { ok: true, pipelines: pipelines.map(p => p.name) })
    if (parts[1] === 'stats') return json(res, 200, { ...counts, at: Date.now() })
    if (parts[1] === 'reset' && req.method === 'POST') { reset(); return json(res, 200, { ok: true }) }
    if (parts[1] === 'wait') { counts.waits++; return json(res, 503, { ready: false }) }
    return json(res, 404, { message: 'Not Found' })
  }
  // /repos/{owner}/{name}/…
  if (parts[0] === 'repos' && parts.length >= 4) {
    const rest = parts.slice(3)
    const repo = of(parts[1], parts[2])
    if (req.method === 'GET' && rest[0] === 'contents' && rest.slice(1).join('/') === DIR) {
      counts.listings++
      return send(res, 200, repo.listing)
    }
    if (req.method === 'GET' && rest[0] === 'contents' && rest.slice(1, -1).join('/') === DIR) {
      const file = repo.files.find(p => p.name === rest.at(-1))
      if (!file) return json(res, 404, { message: 'Not Found' })
      counts.files++
      return send(res, 200, file.text, 'application/vnd.github.raw+json')
    }
    if (req.method === 'POST' && rest[0] === 'statuses' && rest.length === 2) {
      const at = Date.now()
      const chunks = []
      req.on('data', c => chunks.push(c))
      req.on('end', () => {
        let body
        try { body = JSON.parse(Buffer.concat(chunks).toString('utf8')) } catch { return json(res, 400, { message: 'Problems parsing JSON' }) }
        if (!body.state || !body.context) return json(res, 422, { message: 'Validation Failed' })
        counts.statuses[body.state] = (counts.statuses[body.state] || 0) + 1
        statusLog.write(JSON.stringify({ at, repo: `${parts[1]}/${parts[2]}`, sha: rest[1], state: body.state, context: body.context }) + '\n')
        json(res, 201, { state: body.state, context: body.context })
      })
      return
    }
  }
  // Anything else is something dagron-ci asked that this does not answer: it
  // is counted, so that a run shows it and nobody takes a 404 for a result.
  const key = `${req.method} /${parts.slice(0, 1).join('/')}/…/${parts.slice(3, 4).join('/')}`
  counts.other[key] = (counts.other[key] || 0) + 1
  json(res, 404, { message: 'Not Found' })
})
server.listen(PORT, '0.0.0.0', () => console.log(`synthetic forge on :${PORT}, ${pipelines.length} pipeline file(s)${own.size ? `, and ${[...own.keys()].join(', ')} with their own` : ''}`))
for (const signal of ['SIGTERM', 'SIGINT']) process.on(signal, () => { statusLog.end(); server.close(() => process.exit(0)) })

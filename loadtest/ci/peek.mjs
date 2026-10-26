// What each of the cell's containers has used so far, read from the
// runtime's cgroup tree as its machine has it: CPU time and memory. It runs
// in a helper container that is given that tree (`run.sh up` starts it as
// <NAME>-peek, with the host's cgroup namespace and /sys/fs/cgroup at
// /host-cgroup), so that it reads containers with no shell of their own:
// dagron-api's and dagron-ci's images are distroless.
//
//   node peek.mjs NAME=ID …
//
// prints a fragment of JSON, `"cpu_usec": {NAME: …}, "mem_bytes": {…},
// "anon_bytes": {…}`, for run.sh's snapshot. A container whose cgroup is not
// found is null in each. Memory is the cgroup's `memory.current`, which
// counts the page cache it caused, and its `anon` alone.
import fs from 'node:fs'
import path from 'node:path'

const ROOT = process.env.CGROUP_ROOT || '/host-cgroup'
const wanted = process.argv.slice(2).map(a => a.split('=')).filter(([n, id]) => n && id)

// Each container's cgroup is a directory named for its id: Podman's
// `libpod-<id>.scope`, Docker's `docker-<id>.scope`; conmon's is not it.
const found = new Map()
const walk = (dir, depth) => {
  if (depth > 10 || found.size === wanted.length) return
  let entries
  try { entries = fs.readdirSync(dir, { withFileTypes: true }) } catch { return }
  for (const e of entries) {
    if (!e.isDirectory()) continue
    const full = path.join(dir, e.name)
    if (e.name.endsWith('.scope') && !e.name.includes('conmon')) {
      const hit = wanted.find(([, id]) => e.name.includes(id))
      if (hit) { found.set(hit[0], full); continue }
    }
    walk(full, depth + 1)
  }
}
walk(ROOT, 0)

const read = (dir, file) => { try { return fs.readFileSync(path.join(dir, file), 'utf8') } catch { return null } }
const field = (text, key) => {
  const m = text && new RegExp(`^${key} (\\d+)$`, 'm').exec(text)
  return m ? Number(m[1]) : null
}
const cpu = {}, mem = {}, anon = {}
for (const [name] of wanted) {
  const dir = found.get(name)
  cpu[name] = dir ? field(read(dir, 'cpu.stat'), 'usage_usec') : null
  const current = dir ? read(dir, 'memory.current') : null
  mem[name] = current == null ? null : Number(current.trim())
  anon[name] = dir ? field(read(dir, 'memory.stat'), 'anon') : null
}
process.stdout.write(`"cpu_usec": ${JSON.stringify(cpu)}, "mem_bytes": ${JSON.stringify(mem)}, "anon_bytes": ${JSON.stringify(anon)}`)

# dagron console (`mancube/dagron-frontend`)

> ## ⚠️ Deprecated — removed in 1.0.0
>
> The console now ships **inside `mancube/dagron-api`**, which serves it at `/` with
> the API under `/api` on the same origin. This image is a Node runtime with nothing
> left to do, and it is 281 MB against the 16 MB the console actually needs.
>
> **Migrating:** drop the `frontend` service and its `:3000` publish, and open the
> API's port instead. One origin means no proxy — which also fixes live updates,
> since the proxy this image ran buffered `text/event-stream` and never opened it.
>
> It keeps publishing until 1.0.0 so a pinned tag does not vanish underneath you.


**The dagron operator console — a Next.js UI for workflows, runs, schedules, and metrics, talking to `dagron-api`.**

- **Image:** `mancube/dagron-frontend` — Next.js **standalone** server on a **Chainguard (distroless, Wolfi) node** base, runs **nonroot** (uid 65532).
- **Arch:** `linux/amd64`, `linux/arm64`
- **Runtime:** `node server.js` · **Exposes:** `3000`
- **Talks to:** `dagron-api` (the auth + management API)
- **Website:** dagron.dev · **Source / full docs:** github.com/lucheeseng827/dagron · Apache-2.0

## Tags

| Tag | Notes |
|---|---|
| `latest` | newest release |
| `0.7.0` | pinned version (= current `latest`) |
| `0.7` | floating minor — newest `0.7.x` |

Pin in production: `mancube/dagron-frontend:0.7.0`.

## Run

```bash
docker run -p 3000:3000 mancube/dagron-frontend:0.7.0
# then open http://localhost:3000  (sign in with the dagron-api admin user)
```

## Configuring the API host (build-time)

The frontend proxies `/api/*` to `dagron-api`, and that destination is **baked at build time** (Next.js rewrite), not read at runtime. The published image targets `http://dagron-api:8080` (the compose/Helm service name). To point it elsewhere, rebuild with the build arg:

```bash
docker build --build-arg DAGRON_API_URL=https://api.your-host.example.com \
  -t your/dagron-frontend ./frontend
```

> Run alongside `dagron-api` + `dagron-engine` (reachable as `dagron-api:8080` on the same network), or deploy the full stack with the Helm chart (`oci://registry-1.docker.io/mancube/dagron`), which wires the hosts for you.

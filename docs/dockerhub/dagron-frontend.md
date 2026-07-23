# dagron console (`mancube/dagron-frontend`)

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
| `0.4.3` | pinned version (= current `latest`) |

Pin in production: `mancube/dagron-frontend:0.4.3`.

## Run

```bash
docker run -p 3000:3000 mancube/dagron-frontend:0.4.3
# then open http://localhost:3000  (sign in with the dagron-api admin user)
```

## Configuring the API host (build-time)

The frontend proxies `/api/*` to `dagron-api`, and that destination is **baked at build time** (Next.js rewrite), not read at runtime. The published image targets `http://dagron-api:8080` (the compose/Helm service name). To point it elsewhere, rebuild with the build arg:

```bash
docker build --build-arg DAGRON_API_URL=https://api.your-host.example.com \
  -t your/dagron-frontend ./frontend
```

> Run alongside `dagron-api` + `dagron-engine` (reachable as `dagron-api:8080` on the same network), or deploy the full stack with the Helm chart (`oci://registry-1.docker.io/mancube/dagron`), which wires the hosts for you.

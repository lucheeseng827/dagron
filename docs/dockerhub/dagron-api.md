# dagron API gateway (`mancube/dagron-api`)

**The auth + management API for dagron — username/password login to an HttpOnly JWT session, and the REST surface the console (and your scripts) call.**

`dagron-api` sits between the `dagron-frontend` console and the engine's datastore: it authenticates users, mints a signed session cookie, and serves the workflow/run/schedule management API.

- **Image:** `mancube/dagron-api` — Rust binary on **distroless/cc** (glibc), runs as **nonroot**, no shell.
- **Arch:** `linux/amd64`, `linux/arm64`
- **Binary inside:** `/usr/local/bin/dagron-api` (entrypoint) · **Exposes:** `8080`
- **Datastore:** Postgres (`DATABASE_URL`, shared with the engine)
- **Source / full docs:** github.com/lucheeseng827/dagron · Apache-2.0

## Tags

| Tag | Notes |
|---|---|
| `latest` | newest release |
| `0.2.0` | pinned version (= current `latest`) |

Pin in production: `mancube/dagron-api:0.2.0`.

## Run

```bash
docker run -p 8080:8080 \
  -e DATABASE_URL=postgres://dagron:dagron@postgres:5432/workflow \
  -e DAGRON_JWT_SECRET=replace-with-a-32+char-signing-key \
  -e DAGRON_ADMIN_EMAIL=admin@local \
  -e DAGRON_ADMIN_PASSWORD=replace-with-a-strong-password \
  mancube/dagron-api:0.2.0
```

## Configuration (env)

| Var | Meaning |
|---|---|
| `DATABASE_URL` | Postgres connection string (required; same DB as the engine). |
| `PORT` | listen port (default `8080`). |
| `DAGRON_JWT_SECRET` | **required** — HS256 signing key for session cookies (≥32 chars). |
| `DAGRON_ADMIN_EMAIL` / `DAGRON_ADMIN_PASSWORD` / `DAGRON_ADMIN_NAME` | bootstrap admin user, created on first start. |
| `DAGRON_COOKIE_SECURE` | `true` in production (HTTPS); `false` for local HTTP. |
| `RUST_LOG` | log level (`info`). |

> Set a strong `DAGRON_JWT_SECRET` and admin password before exposing this. Pair with `dagron-engine` + `dagron-frontend`, or deploy the whole stack via the Helm chart (`oci://registry-1.docker.io/mancube/dagron`).

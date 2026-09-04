# dagron console (`mancube/dagron-frontend`)

> ## ⛔ Discontinued — **0.8.1 is the last tag**
>
> The console now ships **inside `mancube/dagron-api`**, which serves it at `/` with
> the API under `/api` on the same origin. This image is a Node runtime with nothing
> left to do, and it is 281 MB against the 16 MB the console actually needs.
>
> **Migrating:** drop the `frontend` service and its `:3000` publish, and open the
> API's port instead. One origin means no proxy — which also fixes live updates,
> since the proxy this image ran buffered `text/event-stream` and never opened it.
>
> **This image is no longer published.** The plan was to keep publishing it
> until 1.0.0 so a pinned tag would not vanish underneath anyone, but the same
> change that moved the console made the image unbuildable: the console now
> builds with `output: "export"`, which emits `out/` and never the
> `.next/standalone` this image ran. Nor would repointing it help — the console
> calls `/api` as a *relative* path and the proxy is gone, so served from
> anywhere but `dagron-api` every call 404s.
>
> A tag that builds into a dead console is worse than no tag. **0.8.1 is the
> last working release**; `mancube/dagron-api` carries the console from 0.9
> onward.


**The dagron operator console — a Next.js UI for workflows, runs, schedules, and metrics, talking to `dagron-api`.**

- **Image:** `mancube/dagron-frontend` — Next.js **standalone** server on a **Chainguard (distroless, Wolfi) node** base, runs **nonroot** (uid 65532). *Frozen at 0.8.1.*
- **Arch:** `linux/amd64`, `linux/arm64`
- **Runtime:** `node server.js` · **Exposes:** `3000`
- **Talks to:** `dagron-api` (the auth + management API)
- **Website:** dagron.dev · **Source / full docs:** github.com/lucheeseng827/dagron · Apache-2.0

## Tags

**`0.8.1` is the last tag.** Nothing newer will be pushed. `latest` and `0.8`
still resolve to it; treat all three as frozen.

## What to run instead

```bash
docker run -p 8080:8080 mancube/dagron-api:0.9.1
# then open http://localhost:8080  (same admin user, same session cookie)
```

The console is at `/` and the API under `/api` on that one port. Nothing to
configure: the old `DAGRON_API_URL` build arg existed because this image baked a
Next.js rewrite destination at build time, and there is no rewrite any more —
the console calls `/api` on its own origin.

For the full stack use `compose.quickstart.yaml` in the repo, or the Helm chart
(`oci://registry-1.docker.io/mancube/dagron`), where `frontend.enabled` is
`false` and the ingress routes `/` to `dagron-api`.

# dagron GitOps worker (`mancube/dagron-gitops`)

**Reconciles workflow definitions from Git into dagron — the deploy half of "your DAGs live in a repo".**

- **Image:** `mancube/dagron-gitops` — a Rust binary on **debian-slim**, running as **nonroot**.
- **Arch:** `linux/amd64`, `linux/arm64`
- **Runtime:** a polling worker · **no ports** · needs `DATABASE_URL`
- **Talks to:** the same Postgres `dagron-api` writes, plus your Git forge over HTTPS or SSH
- **Website:** dagron.dev · **Source / full docs:** github.com/lucheeseng827/dagron · Apache-2.0

## Why it is a separate image

This is the **one dagron image that carries a `git` binary**, and that is the
whole point of the split. GitOps shells out to `git`; every other runtime image
stays distroless and subprocess-free for the majority of deployments that never
connect a repository. You deploy this one **only if you use GitOps** — and when
it is absent the console says so plainly rather than pretending auto-sync is on
while nothing is polling.

## What it does

Scans the configured repos for YAML files carrying a `tasks:` key, validates each
one with the **engine's own parser** (so a file that syncs is a file the engine
can actually run), and upserts it as a workflow definition. Files that aren't
specs are skipped rather than reported as errors. The reconcile is idempotent —
the same commit synced twice is a no-op beyond `updated_at`.

## Configuration

| Variable | Default | What it does |
|---|---|---|
| `DATABASE_URL` | **required** | The Postgres dagron-api owns (this worker waits for its schema). |
| `GITOPS_POLL_SECS` | `60` | Seconds between auto-sync reconciles per repo. Manual sync is immediate. |
| `DAGRON_ENV_SECRET_KEY` | unset | Decrypts the per-repository credentials set from the console. **Must match dagron-api's.** Without it, a repo that has one fails its sync saying so. |
| `DAGRON_GIT_TOKEN` | unset | Fallback token for repos with no credential of their own. Sent only to trusted forge hosts over HTTPS; leave unset for public repos. |
| `DAGRON_GIT_TRUSTED_HOSTS` | unset | Extra hosts (and subdomains) the fallback token may be sent to — your GHE / self-managed GitLab. Comma-separated. |
| `DAGRON_GIT_SSH_STRICT` | `false` | Refuse an SSH sync for a repo with no pinned `known_hosts` instead of trusting whatever host answers. |
| `RUST_LOG` | `info` | Log level. |

## Authenticating to private repositories

Two ways, and the per-repository one is the one to reach for:

- **Per repository, from the console** — an HTTPS token or an SSH private key,
  attached to one repo on the GitOps page. Stored AES-256-GCM encrypted by
  dagron-api and decrypted here; never readable back through the API. Rotating
  one is a form submission, not a redeploy, and an SSH key is the only thing that
  works against a forge that does not offer HTTPS clone.
- **`DAGRON_GIT_TOKEN`, worker-wide** — one token for every repo, changed only by
  redeploying this container. Nobody bound it to a particular repository, so it
  is sent only to trusted forge hosts (`github.com`, `gitlab.com`,
  `bitbucket.org`, their subdomains, plus anything in
  `DAGRON_GIT_TRUSTED_HOSTS`). Still the simplest option when every repo lives on
  one forge under one account.

Neither secret ever reaches this process's command line — a token goes to a
0600 `credential.helper` file and a key to a 0600 file named by
`GIT_SSH_COMMAND`, both in a scratch directory deleted when the sync ends. The
image ships `openssh-client` for the SSH transport.

## Tags

| Tag | Notes |
|---|---|
| `latest` | newest release |
| `0.5.0` | pinned version (= current `latest`); first release of this image |

## Quick start

Repositories are connected from the console (**GitOps** in the sidebar) or the
API; this worker only reconciles what is registered there.

```bash
docker run -d --name dagron-gitops \
  -e DATABASE_URL='postgres://dagron:PW@db:5432/dagron' \
  -e GITOPS_POLL_SECS=60 \
  mancube/dagron-gitops:latest
```

In the bundled compose stack it is opt-in, matching how it deploys:

```bash
podman compose --profile gitops up -d
```

Repos, sync status and the "no GitOps worker running" indicator all live in the
console; this image is the thing that makes that indicator go green.

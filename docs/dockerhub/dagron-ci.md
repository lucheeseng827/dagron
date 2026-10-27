# dagron-ci (`mancube/dagron-ci`)

**The CI service of dagron: a forge's events in, pipeline runs in dagron-api, commit statuses back.**

- **Image:** `mancube/dagron-ci` — a Rust binary on **distroless**, running as **nonroot** (65532), with no shell and no `git`.
- **Arch:** `linux/amd64`, `linux/arm64`
- **Runtime:** an HTTP service on **8090** (`/healthz`, `/readyz`, `/metrics`) · needs `CI_DATABASE_URL`
- **Talks to:** its database (Postgres, or SQLite on one node), `dagron-api`, and your forge over HTTPS
- **Website:** dagron.dev · **Source / full docs:** github.com/lucheeseng827/dagron · Apache-2.0

## What it does

It takes events three ways: a GitHub webhook, a poll sweep of each connected
repository, and a dispatch through its own run API. For each it reads the
pipelines at the event's commit (`.dagron/ci/*.yaml`), compiles the ones the
event selects into dagron runs, submits them to `dagron-api`, follows them, and
reports each job as a commit status.

It runs no job itself. A pipeline's jobs are dagron tasks: the engine runs
them, on whichever executor it is configured for. So this image is one more
service beside `dagron-api`, and it holds no build tooling.

## Configuration

With `CI_DATABASE_URL` alone it starts and turns ready, and events it is given
stay queued. The settings that make it of use:

| Variable | Default | What it does |
|---|---|---|
| `CI_DATABASE_URL` | **required** | `postgres://…` or `sqlite://…`; holds the event queue. On Postgres its tables live in a schema of their own (`CI_DATABASE_SCHEMA`, `dagron_ci`), so it can share dagron-api's database. |
| `CI_CELLS` | unset (events stay queued) | `name=url` pairs: the dagron-api each repository's runs go to. Each is `https://`, or `http://` to the same machine: its token for dagron-api is sent there, and it starts on no cell over plain http. |
| `CI_API_TOKEN_FILE` | unset (required with `CI_CELLS`) | File holding the dagron-api API token it submits runs with. |
| `CI_GITHUB_WEBHOOK_SECRET_FILE` | unset (the receiver answers 404) | File holding the GitHub webhook's secret. |
| `CI_GITHUB_APP_ID`, `CI_GITHUB_APP_KEY_FILE` | unset | A GitHub App: each repository's calls, and each checkout, get a token for that repository alone. |
| `CI_GITHUB_TOKEN_FILE` | unset | A GitHub token, for the repositories no App is installed on. |
| `CI_POLL_SECS` | `60` | How often each connected repository is looked at, for a forge that sends no webhook. |
| `CI_GITHUB_CHECKS` | `off` | `on`: each job is a check run (with the App's `checks: write`). GitHub's Re-run button on one reruns it only where GitHub's webhook reaches dagron-ci: polling does not see it. |
| `CI_GITHUB_PR_COMMENT` | `off` | `on`: one comment on each pull request, edited in place as its runs go (with the App's `pull_requests: write`). |
| `CI_LISTEN` | `0.0.0.0:8090` | HTTP listen address. |
| `RUST_LOG` | `info` | Log level. |

Every secret is read from a **file**, never from the environment, and once, at
start. The full list is in `crates/dagron-ci/README.md` in the source.

## Tags

| Tag | Meaning |
|---|---|
| `latest` | newest stable release |
| `0.11.0` | pinned version (= current `latest`) |
| `0.11` | floating minor: newest `0.11.x` |

First published with 0.11.0. Run it with the engine and `dagron-api` of the
same release, and point the engine's `DAGRON_CI_EXEC_IMAGE` at
[`mancube/dagron-ci-exec`](https://hub.docker.com/r/mancube/dagron-ci-exec)
of that release too.

The quickest way to see it work is the source's `compose.ci.yaml`: the whole
stack on one machine, with a GitHub App and no public address (dagron-ci polls
your repositories). `docs/CI_QUICKSTART.md` walks through it.

## Quick start

`db` and `dagron-api.example.com` stand for your Postgres and your dagron-api.
A container finds another by its name only on a network both are on, which
Docker's default bridge is not. So the network is made first, and a Postgres
that runs as the container `db` joins it
(`docker network connect dagron db`).

```bash
docker network create dagron
docker run -d --name dagron-ci -p 8090:8090 --network dagron \
  -e CI_DATABASE_URL='postgres://dagron:PW@db:5432/dagron' \
  -e CI_CELLS='default=https://dagron-api.example.com' \
  -e CI_API_TOKEN_FILE=/run/secrets/dagron-api-token \
  -e CI_GITHUB_WEBHOOK_SECRET_FILE=/run/secrets/github-webhook \
  -v "$PWD/secrets:/run/secrets:ro" \
  mancube/dagron-ci:latest
```

Its other commands run from the same image, against the same database:

```bash
docker exec dagron-ci dagron-ci repo add github.com/OWNER/NAME
docker exec dagron-ci dagron-ci repo list
```

On Kubernetes, the `mancube/dagron` chart deploys it with `ci.enabled=true`,
as a container in the engine's pod, each secret from a Secret you name.

## What is not in it

`dagron-ci run`, which runs a checkout's pipelines on the machine it is run
on, needs that checkout, a `dagron` engine beside it and, by default, that
machine's Docker. It is for the binary on a workstation, not for this image.

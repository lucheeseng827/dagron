# dagron-ci

CI on dagron. A forge event arrives, a pipeline runs, and a status goes back.

> **New to it?** [`docs/CI_QUICKSTART.md`](../../docs/CI_QUICKSTART.md) gets a
> first check green on one machine. [`docs/CI_PIPELINES.md`](../../docs/CI_PIPELINES.md)
> is the reference for a pipeline, and [`docs/CI.md`](../../docs/CI.md) says
> what dagron-ci refuses and what this release does not do. This file is the
> whole of it: every door, setting and metric, and why each is as it is.

The pipeline runs as an ordinary dagron run: dagron-ci sits in front of
dagron-api's submit path and behind its run progress, and the engine never
learns what a pull request, a check run or a CI job is.

**Status: events in, runs out.** dagron-ci receives GitHub events, polls each
repository for what a webhook missed, takes runs asked for through its API,
queues all three, and runs each trusted event's pipelines (and, where the
operator has such run, an untrusted pull request's) through dagron-api,
once per commit. A repository's pipelines are its `.dagron/ci/*.yaml` files at
the event's commit, each compiled to an ordinary dagron run; a commit without
any runs a workflow you bind to the repository, stored in dagron-api. Each job
starts with the event's commit checked out in its workspace, fetched with a
credential for that attempt alone, and goes up on the commit as a status,
pending while it waits or runs, then its outcome (below).

## What it does today

- **`POST /hooks/github`** takes GitHub webhook deliveries:
  - it checks `X-Hub-Signature-256` (HMAC-SHA256 over the raw body, compared in
    constant time) before it parses anything;
  - it dedupes on `X-GitHub-Delivery`, in the same transaction as the event, so
    a redelivered or replayed delivery never queues a second event and a
    restart forgets nothing;
  - it decides the event's trust class (below) and queues one row in
    `ci_events`;
  - it answers 202 within GitHub's 10-second window.
- **`POST /hooks/gitlab`** takes GitLab webhook deliveries the same way:
  - it compares `X-Gitlab-Token` with the webhook's secret token, in
    constant time, before it parses anything (GitLab signs nothing);
  - it dedupes on `webhook-id`, which GitLab keeps across its retries, or
    `Idempotency-Key`; on `X-Gitlab-Event-UUID` only where GitLab sends
    neither, since a webhook another webhook set off shares that one;
  - a push (`Push Hook`) and a tag (`Tag Push Hook`) are a push's and a
    tag's events, and a merge request opened, reopened or given new
    commits (`Merge Request Hook`) a pull request's, at
    `refs/merge-requests/{iid}/head`; one closed or merged is a closed
    pull request's (`pull_request_closed`), which runs nothing and, as on
    GitHub, cancels nothing. A merge request from the project's own branch is trusted, and
    one from a fork is not ([GitLab](#gitlab)). A merge request's pipeline
    (`Pipeline Hook`) that GitLab's pipelines API says is at
    `refs/merge-requests/{iid}/train`, its merge train's, is a merge group's,
    and one cancelled a group let go of.
- **Events it queues:** a push to a branch (`push`), a tag pushed or moved
  (`tag`), a branch or tag deleted (`delete`), a pull request opened, updated or
  reopened (`pull_request`) or closed (`pull_request_closed`), a merge-queue
  group asking for checks (`merge_group`), and a group the queue let go of
  without merging it (`merge_group_destroyed`, for `destroyed` with the reason
  `invalidated` or `dequeued`), whose runs still going are cancelled.
  Everything else, `ping` and a group that merged included, is acknowledged
  and dropped.
- **The poll sweep** reads each repository every `CI_POLL_SECS` (300) and
  queues what changed since its last sweep, as a webhook would have (below).
  It needs no inbound path, so it also serves a forge that cannot reach
  dagron-ci.
- **`POST /ci/v1/runs`** runs a repository's pipelines at a ref, for a
  dagron-api user who asks; `GET /ci/v1/runs/{id}` says what became of it
  (the run API, below). With **a patch bundle** (`POST /ci/v1/bundles`), it
  runs them on a change nobody pushed (patch bundles, below).
- **`POST /ci/v1/runs/{id}/rerun`** runs an event's failed and cancelled runs
  again, each from its failed jobs, or, with `?failed=false`, each of its
  runs that is over, whole. And **a run that runs again is followed
  again**, however it was made to: its checks, its checkouts' credentials
  and its place under a build's limit (running a run again, below).
- **`POST /ci/v1/runs/{id}/cancel`** stops an event's runs, and withdraws
  an event that still waits to be dispatched. **`GET /ci/v1/runs/{id}/logs`**
  reads what its jobs wrote. Each of these doors takes the run's own id too,
  the one a commit status links to. And `dagron-ci run --remote`, `rerun`,
  `cancel` and `logs` ask them from a terminal (acting on a run, below).
- **Event workers** claim queued events and run the repository's pipelines at
  each event's commit, each as its own dagron run (below).
- **Commit statuses**: a reporter posts each run's, one per job, pending and
  then its outcome, on github.com repositories (below).
- **Checkouts**: each job's workspace starts with the event's commit, which
  the engine fetches before the steps run. Its credential is for that attempt
  alone: an installation token of dagron-ci's GitHub App, for the one
  repository and read-only, minted when the engine asks
  (`POST /ci/v1/checkout-tokens`) and revoked when the attempt is over
  (below).
- **`dagron-ci repo add|set|list|show|rm`** connects the repositories it
  serves, says how each is served (`--mode native|compat|fleet`), binds
  each to the workflow a commit without pipelines runs, and sets how often
  it is polled. **`/ci/v1/repos`** does the same for an admin with a
  dagron-api credential, naming a repository by its forge's name for it or
  by its id, and each change is recorded with who made it. A disconnected
  repository keeps its events and runs
  ([below](#connecting-a-repository)).
  A delivery for a repository that is not connected is refused (404) and
  writes nothing.
- **Fleet mode** runs GitHub Actions' and GitLab CI's own jobs on your
  capacity: a job whose `runs-on` asks for one of an operator's labels, or
  whose `tags` name one, is given the forge's own runner, started as a
  dagron run for that one job, with quotas, a floor for a fork's job,
  runners kept warm, a job run again when its runner died, and a count of
  what the runners did ([below](#fleet-mode)). Off unless
  `CI_GITHUB_FLEET_FILE` or `CI_GITLAB_FLEET_FILE` is set.
- **A pull request is given one comment** that says how the runs of its
  latest commit went, where that is asked for
  ([below](#a-pull-requests-comment)). Off unless `CI_GITHUB_PR_COMMENT=on`.
- **A job that keeps flaking is filed an issue**: one that passed only
  after an attempt of it failed, so many times within so many days
  ([below](#a-flaky-jobs-issue)). Off unless `CI_FLAKE_ISSUES` is set.
- **`dagron-ci events`** lists the newest events, their state, and why.
- **`dagron-ci fleet usage`** says what fleet mode's runners did, by
  repository, team or label ([below](#showback)).
- **`dagron-ci validate`** checks pipeline files, `.dagron/ci/*.yaml`;
  **`dagron-ci compile`** prints the dagron workflow one compiles to for an
  event, or why it does not run (below); **`dagron-ci schema`** prints their
  JSON Schema for an editor.
- **`dagron-ci run`** runs a checkout's pipelines on this machine, on a
  `dagron` engine of their own, with the working tree in every task (below).
- **`dagron-ci import`** writes a checkout's GitHub workflows,
  `.gitlab-ci.yml` and Jenkinsfile as pipelines, each held to this crate's
  own reader and compiler ([below](#from-another-ci-dagron-ci-import)).
- **`/healthz`, `/readyz`, `/metrics`** for an orchestrator and Prometheus.

**Getting the command.** From 0.11.1, each release carries
`dagron-ci-<version>-<target>` for Linux x86_64 and arm64 (static, musl),
macOS arm64 and Windows x86_64, beside `dagron`, which `dagron-ci run` needs.
0.11.0 shipped it in the `mancube/dagron-ci` image alone; with that release, a command
that needs no server is
`docker run --rm -v "$PWD:/repo" -w /repo docker.io/mancube/dagron-ci:0.11.0 validate`.

### Trust classes

Opening a pull request must not be the same as authoring a workflow. dagron-ci
decides each event's class from fields the forge vouches for, and records it
with the event:

| Class | Events |
|---|---|
| `trusted` | a push or tag (only a writer can push); a merge-queue group; a pull request whose head is a branch of the repository itself **and** whose author is an owner, member or collaborator |
| `untrusted` | a pull request from a fork, one whose fork is gone, and one opened by anyone else |
| `agent` | runs an agent asks for through the run API: a caller whose dagron-api subject is `agent:<name>`. Refused, unless `CI_AGENT_RUNS=untrusted` |

The class decides what a run may touch once runs exist: secrets, write tokens,
caches, and the isolation it runs under.

## Running it

```sh
export CI_DATABASE_URL=postgres://ci:…@db:5432/dagron   # or sqlite:///var/lib/dagron-ci/ci.db
export CI_GITHUB_WEBHOOK_SECRET_FILE=/run/secrets/github-webhook
export CI_CELLS=default=https://dagron-api.example.com   # where runs go, over TLS
export CI_API_TOKEN_FILE=/run/secrets/dagron-api-token   # a dagron-api API token
dagron-ci repo add github.com/acme/widgets --workflow <workflow id>
dagron-ci serve
```

**As an image.** `mancube/dagron-ci` is this binary on distroless, as
nonroot, published with each release from 0.11.0
(`crates/dagron-ci/Dockerfile`). Its entrypoint serves, and the other
commands run from the same image:

```sh
docker run -d --name dagron-ci -p 8090:8090 -e CI_DATABASE_URL=… mancube/dagron-ci
docker exec dagron-ci dagron-ci repo add github.com/acme/widgets
```

**On Kubernetes.** The Helm chart deploys it with `ci.enabled=true`: a
container in the engine's pod and a Service, ready once the database answers
`/readyz`. It uses the release's database, in its own schema. Each secret is
a Secret you name under `ci` in the chart's values, mounted as the file
dagron-ci reads.

In the engine's pod, the engine asks it for a checkout's credential at the
pod's own address, which is the one address the engine takes over plain
http. Name `ci.checkoutTokenSecret`, and the chart sets the engine's
`DAGRON_CHECKOUT_TOKEN_URL` and mounts the secret in both containers.
`ci.placement=deployment` gives dagron-ci a Deployment of its own instead,
which the chart's engine is not told to ask: a private repository's
checkout then fails. The chart's `values.yaml` says what sharing the pod
means. The engine's two images are not set by the chart:
`DAGRON_CI_EXEC_IMAGE` and `DAGRON_CHECKOUT_IMAGE` go in
`engine.extraEnv`.

The chart makes no cell up. Its own dagron-api Service serves no TLS, and
dagron-ci starts on no cell over plain http, so with a token you name where
runs go: something that terminates TLS before dagron-api, such as the
ingress host where the ingress has TLS.

[Fleet mode](#fleet-mode)'s label map is the value `ci.fleet.labels`, which
the chart writes to the file dagron-ci reads, and `ci.fleet.prepull` pulls
its labels' images onto the runners' nodes.

```sh
helm upgrade --install dagron oci://registry-1.docker.io/mancube/dagron \
  --set ci.enabled=true --set ci.apiTokenSecret.name=dagron-ci-api \
  --set-string ci.cells=default=https://dagron.example.com
kubectl exec deploy/dagron-engine -c dagron-ci -- dagron-ci repo add github.com/acme/widgets
```

`crates/dagron-ci/tests/k3s.sh` installs the chart in k3s, with the image
built from this checkout, and checks that the pod turns ready: in a
Deployment of its own, and in the engine's pod.

The workflow is one saved in dagron-api; its id is in the console or in
`GET /api/workflows`. Each trusted event runs it with the event as parameters:

| Parameter | Value |
|---|---|
| `ci_event` | `push`, `tag` or `merge_group`; `pull_request` for a pull request opened, updated or reopened; `dispatch` for a run asked for through the run API |
| `ci_sha`, `ci_base_sha` | the commit (for an annotated tag, the commit the tag names), and for a pull request or GitHub merge group the commit it is measured against (else empty, and for a GitLab merge train's) |
| `ci_ref` | the ref: `refs/heads/…`, `refs/tags/…`, `refs/pull/{n}/head` |
| `ci_pr` | the pull request's number, else empty |
| `ci_repo`, `ci_forge_host` | `acme/widgets`, `github.com` |
| `ci_group` | `pr-{n}` for a pull request, else the ref with every byte outside `[A-Za-z0-9._/-]` replaced: fit for `concurrency_key` |
| `ci_trust` | `trusted`, or `untrusted` for a pull request from a fork or a non-member, where such are run (`CI_FORK_PRS=untrusted`) |
| a dispatch's `inputs` | under their own names, as the caller sent them |

`ci_ref` is a branch name its pusher chose: read it in a task's `env:`, never
in a command or a `when:`. Declare the parameters you use with an empty
default. A `notify.git` block with `sha: "{{ ci_sha }}"` puts the run on the
commit as a check, `pending` then its result. A `workspace.checkout` of the
event's repository at `ci_sha` starts the task in that commit, with a
credential when the repository needs one ([The checkout](#the-checkout)):

```yaml
name: widgets-ci
parameters: { ci_sha: "", ci_ref: "", ci_group: "", ci_repo: "", ci_forge_host: "" }
concurrency_key: "{{ ci_group }}"
max_active_runs: 1
notify:
  git: { provider: github, repo: acme/widgets, sha: "{{ ci_sha }}", context: dagron/ci }
tasks:
  - name: test
    docker_image: rust:1.86-bookworm
    env: [{ name: CI_REF, value: "{{ ci_ref }}" }]
    workspace:
      checkout: { repository: "https://{{ ci_forge_host }}/{{ ci_repo }}.git", commit: "{{ ci_sha }}" }
    command: ["cargo", "test"]
    timeout_secs: 1800
```

**Marked as CI's.** Give the token to a user of its own in dagron-api, in the
`ci:trigger` group (an admin's `POST /api/users` with
`groups: ["ci:trigger"]`). dagron-ci then submits each run with what it is for
(the repository, ref, commit, pull request, actor and its event's id).
dagron-api keeps that with the run, the run's trigger reads `ci` where it read
`manual`, and `GET /api/runs?trigger=ci` and the console's Runs page narrow to
such runs. An approval gate's `not_triggerer` reads the record's actor: for
a run asked for through `POST /ci/v1/runs` that is the caller, who then may
not approve it, and for a webhook's run it is the forge's login, which bars a
dagron-api user only where the two are the same text.
dagron-api takes the record from that group alone and refuses any
other caller's submit that carries one, so dagron-ci asks the cell who its
token is (`GET /api/me`, every five minutes) and sends none where the token's
user is not in the group. The runs are made all the same, read as manual, and
dagron-ci's log says why, once.

Then add a webhook to the repository (or a GitHub App) that points at
`https://<host>/hooks/github`, with content type `application/json`, the same
secret, and the events `push`, `pull_request` and `merge_group`. GitHub's
`ping` on creation answers `{"pong":true}` when the secret matches.

Without a webhook, the poll sweep alone picks up pushes, tags and pull
requests, within `CI_POLL_SECS`. A private repository on github.com needs a
credential for that: the App installed on it (`CI_GITHUB_APP_ID`), or
`CI_GITHUB_TOKEN_FILE`, a token that can read its contents and pull requests.

### GitLab

A GitLab project, gitlab.com's or a self-managed one's, is connected with
`dagron-ci repo add <host>/<group>/<project> --forge gitlab`. Its
pipelines are its `.dagron/ci` files, read through GitLab's REST API
(`https://<host>/api/v4`), and each job's outcome goes back as a commit
status (`pending`, `success`, `failed`: an `error` is a failure there).

- **Its token.** `CI_GITLAB_TOKENS_DIR` is a directory with a file for
  each GitLab host, named as the host (`gitlab.com`, or
  `gitlab.example:8443` for one on its own port), holding an access
  token with the `api` scope: refs, merge requests, pipelines and changed
  files are read with it, statuses posted, and each checkout's own token
  minted. To mint, it must be a personal access token, a user's or a
  service account's, that is each project's Maintainer: GitLab lets no
  group or project access token mint another, and with one, every
  checkout is fetched without a credential. Each goes to
  its own host and to no other, and to no job. A host without one is read
  as nobody, which reads a public project and posts no status. A
  Kubernetes Secret with a key per host mounts as such a directory.
- **Its checkouts.** Each attempt's checkout is given a project access
  token minted for it (`POST /projects/:id/access_tokens`): the one
  project, the Reporter role, `read_repository`, lapsing two days on
  (GitLab dates it by the day). dagron-ci revokes it by its id when the
  engine says the attempt is over, and its row keeps the id. Where GitLab
  refuses the mint (a host's token that is a group's or a project's access
  token, or only Developer), the checkout is fetched without a credential,
  which a public project needs alone.
  On gitlab.com, project access tokens are a paid tier's.
- **Its webhook.** In the project (or its group), Settings → Webhooks:
  the URL `https://<host>/hooks/gitlab`, the secret token
  `CI_GITLAB_WEBHOOK_SECRET_FILE` holds, and the triggers Push events,
  Tag push events and Merge request events, and Pipeline events for its
  merge trains (below). Without one, the poll sweep reads its branches,
  tags and open merge requests, and no merge train is seen.
- **Its statuses, and a note.** A job's status names the event as GitLab
  does (`(merge_request)`) and its ref, the branch or tag, or a merge
  request's source branch where it is the project's own (a fork's names
  none), so GitLab keeps a run's statuses as one external
  pipeline of that ref, which the merge request shows. With
  `CI_GITLAB_MR_NOTES=on`, a merge request has one note, as a pull request
  has a comment ([below](#a-pull-requests-comment)), written with the
  host's token.
- **Its `.gitlab-ci.yml`.** Where `CI_GITLAB_PIPELINES=on` and the project
  names it in `.dagron/ci.yaml` (`gitlab: pipeline: .gitlab-ci.yml`), it
  runs as a pipeline, `gitlab-ci`, with the files it includes from the
  project: [docs/CI_GITLAB.md](../../docs/CI_GITLAB.md). A job's artifacts
  go through the engine's artifact service, which its operator turns on.
- **Its merge trains** (GitLab Premium's merge queue). Each merge request
  on a train has a pipeline of GitLab's at `refs/merge-requests/<iid>/train`,
  of the commit that merges it with the target branch and the requests
  ahead of it, and GitLab merges it once that pipeline passes. A Pipeline
  event names the request's source branch for each of a merge request's
  pipelines, the train's among them, so dagron-ci asks GitLab's pipelines
  API, with the host's token, which ref the pipeline ran at: each step of a
  merge request's pipeline is asked about until its checks are asked for
  (a project with no merge trains pays that call too). GitLab is waited on
  for 5 seconds, well within the 10 it gives the delivery itself; one it
  did not answer is 502, to be told again at the pipeline's next step.
  GitLab disables a webhook whose deliveries keep failing, and says so in
  its Settings → Webhooks, where it is turned on again; such deliveries
  count in `dagron_ci_webhook_deliveries_total{forge="gitlab",outcome="error"}`.
  GitLab tells a pipeline's steps in no set order, so one told after GitLab
  says the pipeline ended asks for nothing. The train's pipeline's event,
  while it has yet to end, is a merge group's
  event, as GitHub's `merge_group` is: the pipelines whose `on:` takes
  `merge_group` (`branches`: the target branch) run at that commit,
  trusted, as only someone who may merge puts a request on the train, and
  once however many of its steps GitLab tells. Their statuses go to that
  commit under the train's ref, named as the merge request's
  (`(merge_request)`), so GitLab counts them in the train's pipeline and
  merges once they pass too. A train pipeline cancelled, as the train
  drops the request or makes its pipeline again behind one that failed,
  has its runs cancelled, and no other pipeline's: a train's ref is its
  merge request's, and a pipeline is its commit. **The train's pipeline
  is GitLab's own,** made from the project's `.gitlab-ci.yml`, which needs
  a job for it (GitLab makes no pipeline without one), and until
  dagron-ci's first status is in, that job is all it holds: a pipeline
  whose own jobs all end first has passed, and the request merges without
  dagron-ci's checks. Give the train's job a reason to outlast that
  (`rules: [{if: '$CI_MERGE_REQUEST_EVENT_TYPE == "merge_train"'}]`, and a
  script that takes longer than dagron-ci takes to submit, seconds). A
  project's `.gitlab-ci.yml` that dagron-ci runs itself (compat) is not
  run for a train's event.
- **Not yet.** An action's credential and a job's forge token are a GitHub
  App's. A job of the train's pipeline that waits on dagron-ci's checks,
  in place of one that outlasts their first status.

### Configuration

Environment variables only, read once at start. A value that is set but wrong
stops the process with a message naming it.

| Variable | Default | Meaning |
|---|---|---|
| `CI_ACTIONS_ALLOW` | — (any action, at any ref) | The actions of other repositories a workflow's steps may use, as `owner/*` and `owner/repo`, comma-separated. Set, a step that uses any other is refused, and each is pinned to a commit's full id |
| `CI_ACTIONS_MIRROR` | `https://github.com` | Where an action of another repository a workflow's step uses is fetched from, as `<this>/<owner>/<repo>.git`, with no credential, before the job starts: a mirror, for a site that does not reach github.com |
| `CI_ACTIONS_MIRROR_RAW` | — (github.com's raw files, where no mirror is named) | Where one file of an action's repository is read at a ref, with `{owner}`, `{repo}`, `{ref}` and `{path}` in it: how dagron-ci reads the `action.yml` of an action a step uses to fetch the actions it uses in turn. With a mirror named and this unset, an action's step that uses an action of another repository is refused |
| `CI_AGENT_RUNS` | `refuse` | What a run an agent asks for through the run API gets: a caller whose dagron-api subject is `agent:<name>`, as an agent's token presents. `refuse`, or `untrusted`, which runs it as an `agent` run, under the floor an untrusted run is held to, on a cell whose dagron-api takes the trigger record. An agent then reruns and cancels the events it asked for, and no other |
| `CI_API_TOKEN_FILE` | — (required with `CI_CELLS`) | File holding the dagron-api token dagron-ci runs workflows with |
| `CI_CELLS` | — (events stay queued) | `name=url` pairs, comma-separated: the dagron-api of each cell a repository's runs can go to (`dagron-ci repo add … --cell NAME`, default `default`). Each URL is `https://`, or `http://` to this machine (a sidecar that carries it on): `CI_API_TOKEN_FILE`'s token is sent there, and the credential of each caller of the run API, so a cell over plain http to another host stops start-up |
| `CI_CELLS_INSECURE` | — | `allow`: a cell in `CI_CELLS` may be plain `http://` to another host, on a closed network where that is accepted, such as a dagron-api beside dagron-ci behind a network policy. Its token, and each caller's credential, cross that network unencrypted |
| `CI_CHECKOUT_TOKEN_SECRETS_DIR` | — | Directory holding one file per cell, named as the cell, with the secret that cell's engines present: one holding it is answered for its own cell's runs alone |
| `CI_CHECKOUT_TOKEN_SECRET_FILE` | — (`/ci/v1/checkout-tokens` answers 404, unless `CI_CHECKOUT_TOKEN_SECRETS_DIR` is set) | File holding the secret engines present to ask for a checkout's credential, for a run on any cell: their `DAGRON_CHECKOUT_TOKEN_SECRET_FILE` holds the same. For one cell: dagron-ci does not start on it where `CI_CELLS` names two, since an engine of either could ask for the other's run |
| `CI_DATABASE_MAX_CONNECTIONS` | `8` | Pool size for `CI_DATABASE_URL` |
| `CI_DATABASE_SCHEMA` | `dagron_ci` | Postgres schema for dagron-ci's tables and migration history; created if missing |
| `CI_DATABASE_URL` | — (required) | `postgres://…` or `sqlite://…`; holds the event queue |
| `CI_DEFAULT_TIMEOUT_SECS` | `3600` | The timeout of a compiled pipeline's tasks when the pipeline sets none (1 to 604800); `dagron-ci compile` reads it too |
| `CI_EVENT_LEASE_SECS` | `60` | How long a worker holds an event before another may take it over. A paths filter's shallow fetch is given a third of it, and 20 s at most |
| `CI_EVENT_MAX_ATTEMPTS` | `8` | Attempts at an event before it is left failed |
| `CI_EVENT_WORKERS` | `8` | Event workers in this replica; `0` runs the receivers alone |
| `CI_FLAKE_DAYS` | `14` | How many days back a job's flakes are counted for `CI_FLAKE_ISSUES`, and kept (1 to 90) |
| `CI_FLAKE_ISSUES` | `off` | A number N files an issue of a job of a trusted run that passed only after an attempt of it failed (retried by the engine, or rerun by a person), N times within `CI_FLAKE_DAYS`, and writes it again in place as it flakes again: with the App's token for `issues: write` alone, or the GitLab host's (`CI_GITLAB_TOKENS_DIR`). Needs one of them. `off` keeps no flake ([A flaky job's issue](#a-flaky-jobs-issue)) |
| `CI_FLEET_ENVIRONMENT` | `dagron-ci-fleet` | The environment, in each cell's dagron-api, that a fleet runner's configuration is kept in as a secret while the runner is out. Made where a cell has none of the name ([Fleet mode](#fleet-mode)) |
| `CI_FLEET_UNTRUSTED_ENVIRONMENT` | `dagron-ci-fleet-untrusted` | The same, of a runner started for a fork's job (a label's `untrusted: floor`), whose run is not trusted. Made where a cell has none, and given a policy that admits such runs of the fleet's, where it has none and dagron-ci's token may write one. Not `CI_FLEET_ENVIRONMENT`'s ([Fleet mode](#fleet-mode)) |
| `CI_FORK_APPROVAL` | `first-time` | With `CI_FORK_PRS=untrusted`, which such pull requests wait for an approval before anything of them is run: `first-time` (their author has no commit in the repository), `always`, or `never` |
| `CI_FORK_APPROVAL_TIMEOUT_SECS` | `604800` | How long that approval waits before it is taken as refused: a minute to ninety days |
| `CI_FORK_APPROVERS` | — | Who may give it, comma-separated: an email, or `group:<name>`. Required with `CI_FORK_PRS=untrusted` unless `CI_FORK_APPROVAL=never` |
| `CI_FORK_PRS` | `refuse` | What a pull request from a fork or a non-member gets: `refuse`, or `untrusted`, which runs it as an untrusted run on a cell whose engine runs such work |
| `CI_GITHUB_API_FOR_JOBS` | — (a job reaches GitHub's API itself) | Where a workflow's job reaches dagron-ci's GitHub API door (`/ci/v1/github-api`, at a URL jobs can reach): its `GITHUB_API_URL`. The door answers `download-artifact`'s `run-id` for a run of dagron's, of the job's own repository, for that job's `GITHUB_TOKEN` in a trusted run, and passes every other call on to GitHub's API (`CI_GITHUB_API_URL`) as it came; one with no credential is refused. Needs `CI_CHECKOUT_TOKEN_SECRET_FILE` (or `_SECRETS_DIR`), which signs its links, and `CI_API_TOKEN_FILE` |
| `CI_GITHUB_API_URL` | `https://api.github.com` | Where github.com's REST API is reached: an egress proxy or a cache in front of it. `https://`, or `http://` to this machine, since `CI_GITHUB_TOKEN_FILE`'s token, and the App's, go there |
| `CI_GITHUB_APP_ID` | — (checkouts take no credential) | The GitHub App dagron-ci acts as on github.com. Each checkout's credential is minted as it: an installation token for the one repository, `contents: read`. So is the token dagron-ci's own calls for a repository are made with, where the App is installed on it (see [Whose authority a call carries](#whose-authority-a-call-carries)). With `CI_GITHUB_APP_KEY_FILE`, or `CI_GITHUB_APP_TOKEN_URL` |
| `CI_GITHUB_APP_KEY_FILE` | — (with `CI_GITHUB_APP_ID`) | File holding that App's private key, PEM, which signs its requests to GitHub; read and checked at start |
| `CI_GITHUB_APP_TOKEN_SECRET_FILE` | — (with `CI_GITHUB_APP_TOKEN_URL`) | File holding the secret dagron-ci asks `CI_GITHUB_APP_TOKEN_URL` for tokens under, as a bearer token |
| `CI_GITHUB_APP_TOKEN_URL` | — (dagron-ci holds the App's key) | Where the App's installation tokens are asked for, in place of `CI_GITHUB_APP_KEY_FILE`: a service that holds the key ([The App's key, held elsewhere](#the-apps-key-held-elsewhere)). `https://`, or `http://` to this machine |
| `CI_GITHUB_APP_TOKEN_URL_INSECURE` | — | `allow`: `CI_GITHUB_APP_TOKEN_URL` may be plain `http://` to another host, on a closed network where that is accepted. Its secret, and every token it gives, cross that network unencrypted |
| `CI_GITHUB_CHECKS` | `off` | `on` reports each job of a repository the App is installed on, with `checks: write`, as a check run and not a commit status, under the same name, with its test report and its steps' summaries, and takes a check's "Re-run", and a commit's "Re-run all checks", as reruns ([Check runs](#check-runs)). Needs `CI_GITHUB_APP_ID`. Any other repository's jobs are commit statuses still |
| `CI_GITHUB_DOCKER` | — (no job is given a Docker daemon) | The repositories whose workflow jobs may have a Docker daemon of their own, which is privileged on the engine's host (unless the engine runs it on Sysbox): `owner/*` and `owner/repo`, comma-separated. A listed repository's `github.docker: true` in `.dagron/ci.yaml` gives each job of its trusted runs one, where the engine offers it (`DAGRON_DOCKER_DAEMON=dind`); any other repository's asks for nothing ([docs/CI_RUNNERS.md](../../docs/CI_RUNNERS.md#a-jobs-docker-daemon)) |
| `CI_GITHUB_FLEET_FILE` | — (no job of GitHub Actions' own is served) | File holding the fleet's label map, YAML: each label a job's `runs-on` may ask for, and the runner image, runner class, pool, priority and time a job that asks for it is given, where its runner is registered, its quota, what a fork's job is given, the runners kept warm, and whether a job whose runner died is run again. Read and checked at start. Needs `CI_GITHUB_APP_ID`, and `CI_CELLS` with `CI_API_TOKEN_FILE`: dagron-ci does not start with a fleet it has nowhere to start a runner for. A job is heard of by its delivery alone, so some replica needs `CI_GITHUB_WEBHOOK_SECRET_FILE`: one without it says so as it starts ([Fleet mode](#fleet-mode)) |
| `CI_GITHUB_OIDC` | `off` | Whether a workflow's job that may ask for an OIDC token (`permissions: id-token: write`) is given the engine's. `on` gives a trusted run's such job the identity the engine gives a task (`DAGRON_OIDC_REQUEST_URL`), for the forge's own audience and those the repository names (`github.oidc_audiences`), with GitHub's two variables; `off` gives none, and the job runs without |
| `CI_GITHUB_PR_COMMENT` | `off` | `on` keeps one comment on each pull request of a repository the App is installed on, with `pull requests: write`: how the runs of its latest commit went, written again in place as they change. Needs `CI_GITHUB_APP_ID`. `off`, and for any other repository, none is written ([A pull request's comment](#a-pull-requests-comment)) |
| `CI_GITHUB_TOKEN` | `off` | Whether a workflow's job is given a `GITHUB_TOKEN`. `on` (it needs `CI_GITHUB_APP_ID`) asks the engine to mint each attempt one, from this dagron-ci's token service (`/ci/v1/checkout-tokens`), for the job's repository with what its `permissions` grant, `contents: read` where it says nothing; a run that is not trusted is given read alone. `off` gives none |
| `CI_GITHUB_TOKEN_FILE` | — (github.com is read without one, where the App is not installed) | File holding a GitHub token for the github.com repositories the App is not installed on or cannot have a token for, or for all of them without `CI_GITHUB_APP_ID`: refs, pull requests, pipelines and changed files are read with it, and commit statuses posted (`statuses: write`). Sent to github.com only (or `CI_GITHUB_API_URL`), never to another host |
| `CI_GITHUB_WEBHOOK_SECRET_FILE` | — (the GitHub receiver answers 404) | File holding the webhook secret |
| `CI_GITHUB_WORKFLOWS` | `off` | `on` runs the `.github/workflows` files a repository in compat mode lists in its `.dagron/ci.yaml`, as pipelines (level 1: [docs/CI_GITHUB.md](../../docs/CI_GITHUB.md)) |
| `CI_GITLAB_FLEET_FILE` | — (no job of GitLab CI's own is served) | File holding the GitLab fleet's label map, as `CI_GITHUB_FLEET_FILE`'s, for a job's tags; its runner is GitLab's own (`gitlab-runner run-single`). Read and checked at start. Needs `CI_CELLS` with `CI_API_TOKEN_FILE`, and `CI_GITLAB_TOKENS_DIR`'s token for each host whose projects it serves, which makes and removes the runners. A job is heard of by the project's Job Hook, so some replica needs `CI_GITLAB_WEBHOOK_SECRET_FILE` ([GitLab's fleet](#gitlabs-fleet)) |
| `CI_GITLAB_MR_NOTES` | `off` | `on` keeps one note on each merge request of a GitLab project on a host `CI_GITLAB_TOKENS_DIR` keeps a token for, written with that token: what a pull request's comment says ([A pull request's comment](#a-pull-requests-comment)). Needs a token. `off`, and for any other project, none is written |
| `CI_GITLAB_PIPELINES` | `off` | `on` runs the `.gitlab-ci.yml` a GitLab project in compat mode names in its `.dagron/ci.yaml` (`gitlab.pipeline`), with the files it includes, as a pipeline (level 1: [docs/CI_GITLAB.md](../../docs/CI_GITLAB.md)) |
| `CI_GITLAB_TOKENS_DIR` | — (GitLab is read without credentials, and given no statuses) | Directory of GitLab access tokens, one file per host, named as the host (`host:port` for one on its own port): that host's projects are read and given statuses with it, and each checkout's own project access token minted; sent to no other host, and to no job ([GitLab](#gitlab)) |
| `CI_GITLAB_WEBHOOK_SECRET_FILE` | — (the GitLab receiver answers 404) | File holding the secret token `POST /hooks/gitlab` compares `X-Gitlab-Token` with |
| `CI_LISTEN` | `0.0.0.0:8090` | HTTP listen address |
| `CI_OBJECT_STORE_URL` | — (a payload over 64 KiB is not kept, and no record is written out) | dagron-ci's object store: `s3://bucket/prefix`, `gs://bucket/prefix`, `az://container/prefix` (credentials from the environment, as the engine's stores read them) or `file:///path`. A webhook payload over 64 KiB is kept there, named by its event; and a day past `CI_RECORD_DAYS` is written there as segments, read back before it leaves the database ([A life for the records](#a-life-for-the-records)) |
| `CI_POLL_SECS` | `300` | How often the poll sweep reads a repository that sets no interval of its own (`dagron-ci repo set … --poll-secs`): 30 to 86400 seconds, or `0` for never |
| `CI_RECORD_DAYS` | — (every record is kept) | How many whole days before today dagron-ci's records are kept in the database, 1 to 3650: each hour the older days are written to `CI_OBJECT_STORE_URL`, where it is set, and dropped, with their events, runs' rows, run API requests, report log, deliveries and stored payloads ([A life for the records](#a-life-for-the-records)) |
| `CI_RUN_URL` | — (statuses link nowhere) | The page a commit status links to: an http(s) URL in which `{run_id}` and `{cell}` are the run's |
| `CI_SEGMENT_DAYS` | — (a segment is kept) | How many days after its day a segment written to `CI_OBJECT_STORE_URL` is kept, 1 to 36500; each hourly pass sets every segment's time from it. Needs `CI_OBJECT_STORE_URL` |
| `CI_URL_FOR_JOBS` | — (no patch bundles) | Where a job reaches dagron-ci, an https:// URL with no path (http:// to this machine, or with `CI_URL_FOR_JOBS_INSECURE`): where a patched checkout fetches its bundle (patch bundles, below) |
| `CI_URL_FOR_JOBS_INSECURE` | — (https only, or http:// to this machine) | `allow`: `CI_URL_FOR_JOBS` may be plain http:// to another host, on a closed network where that is accepted |

**Postgres.** dagron-ci can share a database with dagron-api. Its tables live
in their own schema (`CI_DATABASE_SCHEMA`), which it creates on first start if
its role may; otherwise create the schema beforehand and grant the role usage
and create on it. The schema also holds dagron-ci's own migration history, so
it never meets dagron-api's.

## HTTP surface

| Method and path | Auth | What it does |
|---|---|---|
| `POST /hooks/github` | the webhook signature | 202 `{"event_id": …}` once queued, or `{"duplicate": true}`, `{"ignored": …}`, `{"pong": true}`, `{"rerun": […], "left": […]}` for a check's "Re-run", or a commit's "Re-run all checks", that ran a run again ([Check runs](#check-runs)), or `{"fleet_job": …, "class": …, "state": …}` for a workflow job the fleet serves ([Fleet mode](#fleet-mode)); 401 bad signature; 400 unreadable; 404 repository not connected or receiver not configured; 500 database down (GitHub redelivers) |
| `POST /hooks/gitlab` | the webhook's secret token | 202 `{"event_id": …}` once queued, or `{"duplicate": true}`, `{"ignored": …}`, or `{"fleet_job": …, "class": …, "state": …}` for a job's Job Hook where the GitLab fleet serves any ([GitLab's fleet](#gitlabs-fleet)); 401 a token that is not the secret; 400 unreadable; 404 project not connected as a GitLab one, or receiver not configured; 500 database down (GitLab retries) |
| `POST /ci/v1/runs` | a dagron-api session or API token, not a viewer's | 202 with the run (below) once queued; 200 with the first request's run for a repeated `Idempotency-Key`; 400 invalid; 401 no credential or one dagron-api refused; 403 a viewer; 404 repository not connected, or no cell configured; 409 a key reused with another request; 422 no such ref; 502 the forge could not resolve the ref; 503 dagron-api could not be asked |
| `POST /ci/v1/bundles` | a dagron-api session or API token, not a viewer's | `?repo=HOST/OWNER/NAME&base=SHA` and a tar (or zstd tar) of up to 32 MiB as the body: 201 `{id, repo, base, size_bytes, sha256, expires_at}`. Or a JSON body `{repo, base, size_bytes}`, up to 256 MiB: 201 `{id, upload: {method, url, headers}, expires_at}`, an upload link the store signs, the bundle checked when a run first names it; 409 where the store signs none. 400 invalid; 401; 403 a viewer; 404 repository not connected; 413; 422 not a bundle (a link, a path outside the tree, `.git`); 503 bundles not taken here (patch bundles, below) |
| `GET /ci/v1/me` | a dagron-api session or API token | 200 `{"sub", "email", "groups", "trust"}`: who the `default` cell's dagron-api (else the first cell's) says presents the credential, and what a run they ask for is recorded as; 401 no credential or one dagron-api refused; 404 no cell configured; 503 dagron-api could not be asked |
| `GET /ci/v1/runs/{id}` | a dagron-api session or API token | 200 with the run: any event's id works, a webhook's or a poll's too, and so does an engine run's, which reads its event with that run alone; 401; 404 |
| `POST /ci/v1/runs/{id}/rerun` | a dagron-api session or API token, not a viewer's | 200 `{"id", "rerun": […], "left": […]}`: the event's failed and cancelled runs are running again, each from its failed jobs (`failed=true`, the default), or each of its runs that is over, whole (`failed=false`), and the rest are left, each with why; 400 a `failed` that is neither `true` nor `false`; 401; 403 a viewer, an agent, or a caller dagron-api does not let rerun; 404 no such event, or no dagron-api token configured; 409 nothing to rerun, with why for each run (among them a cell whose dagron-api runs no rerun of every job), or a repository in fleet mode; 503 dagron-api could not be asked |
| `POST /ci/v1/runs/{id}/cancel` | a dagron-api session or API token, not a viewer's | 200 `{"id", "event", "cancelled": […], "left": […]}`: each of the event's runs that was running is asked to stop, the rest are left, each with why, and `event` is `withdrawn` (it waited to be dispatched, and now is not), `dispatching` (a worker is on it: ask again) or `settled`; 401; 403 a viewer, an agent, or a caller dagron-api does not let cancel; 404 no such event; 409 nothing to cancel, each run saying why, with `Retry-After` when a worker is dispatching the event; 503 dagron-api could not be asked |
| `GET /ci/v1/runs/{id}/logs` | a dagron-api session or API token | 200 `{"id", "state", "reason", "runs": […]}`: each run with its `logs`, as its cell's dagron-api gives them, or the `error` that kept them from being read. `job`, `status`, `q`, `limit` and `tail` narrow it; `follow=true`: one run's log followed live, as its cell streams it (`text/event-stream`; `from`, `cursor`), 409 with no run yet, 400 for an event of several; 400 anything else, or what dagron-api refuses; 401; 403 and 503 when no run's log could be read; 404 |
| `GET /ci/v1/runs/{id}/statuses` | a dagron-api session or API token | 200 `{"event_id", "from", "statuses": […]}`: each status dagron-ci posted on the event's commit (an engine run's id: that run's alone), oldest first, with what came of it (`posted`, `refused`, `rate_limited`, `failed`, and why), from the database (`"from": "database"`) while the event's day is there and from its day's segments after (`"segments"`); 401; 404; 503 when the segments could not be read |
| `GET /ci/v1/repos` | a dagron-api session or API token of an admin, and no agent's; to change one, no viewer's either | 200 `{"repos": […]}`: the connected repositories, each with its id, forge, mode, cell, workflow and how its polling goes; 401; 403 not an admin |
| `GET /ci/v1/repos/{host}/{owner}/{name}`, or `/ci/v1/repos/{id}` | the same | 200 with the repository, connected or not (`connected`, `disconnected_at`), and its `changes`, oldest first: `{at, actor, action, set}`; 400 neither a repository's name nor an id; 401; 403; 404 never connected, or an id no repository has |
| `PUT /ci/v1/repos/{host}/{owner}/{name}`, or `/ci/v1/repos/{id}` | the same | 201 once connected (`"action": "connected"`, or `"reconnected"` for one that was disconnected); 200 for one connected already, `"changed"` or `"unchanged"`. The body, which may be empty, takes `forge`, `mode`, `cell`, `workflow` and `poll_secs`; 400 anything else, a cell that is not configured, or a forge named for a repository connected already; 401; 403; 404 an id no repository has; 409 connected by another request meanwhile |
| `DELETE /ci/v1/repos/{host}/{owner}/{name}`, or `/ci/v1/repos/{id}` | the same | 200 `{"id", "repo", "action": "disconnected"}`; 400 neither a repository's name nor an id; 401; 403; 404 not connected, or an id no repository has |
| `GET /ci/v1/fleet/usage` | a dagron-api session or API token of an admin, and no agent's | 200 `{"since", "until", "by", "rows": […], "total"}`: what fleet mode's runners did over the days, each row a `key` with its `jobs`, `job_seconds`, `runners` and `runner_seconds`. `since` and `until` (`YYYY-MM-DD`, UTC) and `by` (`repo`, `team` or `label`) ([Showback](#showback)); 400 anything else, or more than 366 days; 401; 403 not an admin |
| `POST /ci/v1/checkout-tokens` | a checkout secret as a bearer: every cell's, or the run's cell's own | 200 `{"username", "password", "expires_at"}`: a credential for one attempt's checkout; 204 for none (no App, a repository not on github.com, or one the App is not installed on); 400 unreadable; 401 not a secret; 403 not the run's cell, repository or commit; 204 also for a run dagron-ci did not submit, of a repository it does not serve; 404 not a run dagron-ci submitted, of a repository it serves (either said once the run has been looked for, for two seconds), or not configured; 409 the run is over, and its cell does not say it is running again; 502 GitHub could not mint one; 503 database down, not answering in time, or the cell of a run on record as over could not be asked whether it is running again |
| `DELETE /ci/v1/checkout-tokens` | the same | 204 once the credential sent (`{"password": …}`) is revoked, or when none could have been minted; 400; 401; 404 not configured; 502 GitHub could not be told |
| `GET /healthz` | none | `ok` while the process answers |
| `GET /readyz` | none | `ready` when the database answers within 1 s, else 503 |
| `GET /metrics` | none | Prometheus text (below) |

Errors are RFC 9457 problem documents (`application/problem+json`).

### Metrics

A build with the `otel` feature (the published images are) also sends these to an OpenTelemetry collector each `OTEL_METRIC_EXPORT_INTERVAL` (60 s), with its log records, once `OTEL_EXPORTER_OTLP_ENDPOINT` names one, or `OTEL_EXPORTER_OTLP_METRICS_ENDPOINT` for the metrics alone and `OTEL_EXPORTER_OTLP_LOGS_ENDPOINT` for the log records alone (`docs/CONFIG.md`). Their `service.name` is the binary's: `dagron-ci` for this one. dagron-ci opens no spans of its own yet.

| Series | Type | Meaning |
|---|---|---|
| `dagron_ci_build_info{version}` | gauge | always 1; `version` is the release the binary was built for, as `dagron-ci --version` prints it, and `dev` for a build no release made |
| `dagron_ci_webhook_deliveries_total{forge,outcome}` | counter | deliveries by outcome: `accepted`, `duplicate`, `ignored`, `unauthorized`, `bad_request`, `unknown_repo`, `not_configured`, `error` |
| `dagron_ci_events_pending` | gauge | events queued and not yet handled, retries waiting included |
| `dagron_ci_events_oldest_pending_age_seconds` | gauge | age of the oldest pending event |
| `dagron_ci_event_workers` | gauge | event workers this replica runs; 0 when no cell is configured |
| `dagron_ci_events_finished_total{outcome}` | counter | events workers finished: `done`, `skipped`, `failed`, `retried` later, or `waited` (a full concurrency key, a spent rate limit, or the build's limit on runs in flight; no attempt spent) |
| `dagron_ci_poll_sweeps_total{outcome}` | counter | poll sweeps by outcome: `baseline` (a first sweep, which queues nothing), `queued`, `unchanged`, `failed`, `rate_limited` |
| `dagron_ci_poll_events_total` | counter | events poll sweeps queued |
| `dagron_ci_api_requests_total{outcome}` | counter | `POST /ci/v1/runs`, `POST /ci/v1/runs/{id}/rerun` and `POST /ci/v1/runs/{id}/cancel` by outcome: `accepted`, `replayed`, `unauthorized`, `forbidden`, `bad_request`, `not_found`, `conflict`, `unavailable`, `not_configured`, `error` |
| `dagron_ci_commit_statuses_total{outcome}` | counter | commit statuses this replica meant to post, a run's jobs' and a refused pull request's, and the check runs it meant to post or bring up to date where a job is one: `posted`, `refused` (not retried), `failed` (retried), `rate_limited` (retried once GitHub allows) |
| `dagron_ci_runs_reopened_total{by}` | counter | records re-opened because their run was seen running again, by what occasioned the look: `rerun` (dagron-ci's own door), `credential` (a checkout's credential asked for a run on record as over), `sweep` (the look over each cell's running runs), `outbox` (the cell's outbox said the run is running again) |
| `dagron_ci_fleet_jobs_waiting{label}` | gauge | fleet mode's jobs that wait for a runner, every repository together; each label of the fleet's from the first scrape. Where fleet mode serves anything |
| `dagron_ci_fleet_runners{label,state}` | gauge | fleet mode's runners out: `wanted`, `launched` (waiting for a job, warm ones among them) and `busy` |
| `dagron_ci_checkout_tokens_total{outcome}` | counter | requests for a checkout's credential: `minted`, `anonymous` (none needed or to be had), `refused` (not a secret, not a run dagron-ci submitted though of a repository it serves, not its cell, repository or commit, or a run that is over), `failed` (GitHub could not mint one), `revoked` |

## The event queue

`ci_events` is the one place every door writes: the webhook receivers, the
poll sweep and the run API. Each event carries a commit-level key,
`ci:{repo}:{kind}:{ref}:{sha}`. A webhook and a poll that see the same commit
derive the same key and, from it, the same run idempotency key, so between
them they produce one run. A dispatch's key ends in its event id, since two
dispatches of one commit are two runs.

The raw payload is kept with its event when it is at most 64 KiB. A larger
one is kept in `CI_OBJECT_STORE_URL` where it is set (`s3://`, `gs://`,
`az://`, or a directory, `file:///path`): it is put there first, and its
event names it; a redelivery's copy, or that of an event that was not
recorded, is taken out again, and a store that does not take it is a `503`
the forge delivers again. Without a store, a larger payload is not kept; the
event's columns still hold everything the queue needs.

**Days.** On Postgres, `ci_events` is partitioned by day (UTC): a partition a
day, `ci_events_YYYYMMDD`, made a week ahead each hour, and a default one for
an event of a day whose partition was not made in time.

### A life for the records

Every record is kept unless `CI_RECORD_DAYS` says how many whole days before
today are kept in the database. Then each hour the older days go, oldest
first, each in one transaction: its events (a partition dropped whole on
Postgres, which waits for the queue's lock five seconds at most, a day not
dropped then being dropped at the next pass; rows deleted on SQLite), their
runs' rows, their run API requests, their report log, the day's deliveries,
and after them the payloads its events kept in the store.

Where `CI_OBJECT_STORE_URL` is set, a day is first written there: each of
`ci_deliveries`, `ci_events` (with each stored payload in its event's row),
`ci_runs`, `ci_api_requests` and `ci_report_log` a segment of that day's
rows, more than one past 50,000, JSON a row a line, compressed with zstd,
named `segments/<table>/<day>.instance.<part>.jsonl.zst`. Each is read back,
and its checksum and its count matched, before the day leaves the database;
a day that is not written stays, and the pass stops there. `ci_segments`
indexes them: the table, the day, the scope, the count, the size, the
checksum, when it was read back, and when it goes, `CI_SEGMENT_DAYS` after its
day, or never where that is unset. There is one retention scope yet, the
instance's; a repository's or an organisation's own waits for its policy.
What was posted for an event is read from either place
(`GET /ci/v1/runs/{id}/statuses`): as its day goes, each of its events and
runs is indexed in `ci_archived` (its event, its day, its repository's
cell), so that an event's id or an engine run's finds its day's segments,
and its reader is identified by that cell's dagron-api, as before the day
went. The index of a day goes once its segments have.

Who caused a run is also on the run's record in the engine's database, which
stays. `dagron_ci_records_oldest_age_seconds` is how old the oldest record in
the database is, and `dagron_ci_segments_written_total` and
`dagron_ci_events_dropped_total` count what was written out and dropped.

### Event workers

Each replica runs `CI_EVENT_WORKERS` workers once `CI_CELLS` names a cell. A
worker claims the oldest due event under a lease (`FOR UPDATE SKIP LOCKED` on
Postgres, so replicas share the queue) and, for a trusted event, reads the
repository's pipelines at the event's commit:

- **with pipelines**, it decides each one's `on:` against the event, compiles
  each that runs ([What a pipeline compiles to](#what-a-pipeline-compiles-to))
  and submits it as its own run (`POST /api/runs`). Every pipeline is decided
  before any is submitted;
- **without** (no `.dagron/ci/*.yaml` at the commit), it runs the workflow
  bound to the repository, with the event as its parameters.

Pipelines are read with GitHub's contents API, at the commit: github.com
(as the App, or with `CI_GITHUB_TOKEN_FILE`, for a private repository),
`*.ghe.com` and Enterprise Server; and with GitLab's repository API, for a
GitLab project (with its host's token in `CI_GITLAB_TOKENS_DIR`, for a
private one). A repository on another forge runs its bound workflow until dagron-ci
reads that forge's files. The worker leaves the event in one of these states,
its reason naming each pipeline and what became of it:

| State | When |
|---|---|
| `done` | each pipeline that runs was submitted (the reason names its run), now or for an earlier event of the commit |
| `skipped` | nothing to run: a pull request from a fork or a non-member where such are refused, or one that was not approved, a deleted ref, a closed pull request, no pipeline whose `on:` takes the event, or a commit without pipelines and a repository with no workflow bound |
| `failed` | something cannot run, once everything else has: a pipeline is invalid, a dispatch's inputs are refused, dagron-api refused a spec or a parameter (400) or has no such workflow (404), or the workflow is paused; or attempts ran out |
| `new`, retried | worth trying again: the forge or dagron-api was unreachable, busy (5xx) or refused a token (401, 403). Backoff: 5 s, doubling, at most 10 min |
| `new`, waiting | not yet, and no attempt spent: as many runs wait for a run's concurrency key as may (dagron-api's 429; a run over its key is otherwise made, and waits in the engine), or the build's limit on runs in flight is reached (see [Extending it](#extending-it)), so the event asks again every 30 s; or the forge's rate limit is spent, so it waits for the reset, at most an hour |

**Nothing is lost and nothing runs twice.** A worker that stops mid-event loses
nothing: its lease lapses and the next claim takes the event over. That claim
counts as an attempt, so an event that keeps killing its workers is given up
once attempts run out. Each run is submitted under an `Idempotency-Key` derived
from the event's commit key and the pipeline's path (or the bound workflow),
and recorded in `ci_runs` under it. A second event for the same commit (a
redelivery, a poll) finds the records and submits nothing; a worker that died
between submitting and recording gets the same run back from dagron-api.

**Paths filters** are decided with the files the event changed, asked of the
forge once per event, and only when a pipeline needs them: GitHub's compare
API between the event's base and its commit (GitLab's, for a GitLab project).
A pull request is compared with its base (what it adds); a push with the
commit its branch pointed at before, or for a polled push, the one the last
sweep saw.

A change the compare API cannot list (GitHub lists at most 300 files, GitLab
1000, and dagron-ci reads no comparison over 16 MiB) is listed by git itself:
dagron-ci fetches two commits at depth 1, over git's protocol v2 with the
repository's own credential, without their files' contents where the server
can leave them out (`filter blob:none`; github.com and GitLab can), and
compares their trees, as `git diff --name-only` does. The two are the
event's commit and the merge base the compare API compared from: the one
GitHub's comparison names (for a comparison too large to read, a page of it
past the first, which lists no files, names it), or GitLab's
`repository/merge_base`. The fetch then lists the files the compare API
would have: what the change added since it left its base, and not what the
base branch changed since. That holds for a push forced after a rebase too.
Its base, the branch's old tip, is not in its history, and a change from
there would leave out the branch's own files, the same in both tips.

The fetch is done in dagron-ci's own process (the image has no git) and is
bounded: 32 MiB of pack, 64 MiB inflated, 200,000 objects, 256 directories
deep, 64 MiB of trees read, 200,000 paths of at most 4,096 bytes and 16 MiB
in all, 20 s (a third of `CI_EVENT_LEASE_SECS` where that is under 60 s), and
one fetch at a time in a replica. A fetch then holds about 160 MiB at most,
inside the 256 MiB the charts give dagron-ci.

When the files still cannot be listed, the filter lets its pipeline run and
the reason says why, of both: a new branch has nothing to compare with; two
commits the forge cannot compare (no shared history, a commit it does not
have) are not fetched, nor is a change whose merge base the forge does not
name; a server that speaks only git's protocol v0, or fetches no shallow
history, cannot be fetched from; a server without `filter` sends the files'
contents too, so a large repository's answer there is over 32 MiB; and a
change past the fetch's other bounds, or not fetched in its time, is not
listed. (GitHub Actions decides on a change's first 300 files, and skips a
workflow on the rest; dagron-ci would rather run a check than skip one a
merge waits for.)

### Commit statuses

A run dagron-ci submits is recorded in `ci_runs`, due for a report at once.
Each replica's reporter claims a due run, as a worker claims an event (`FOR
UPDATE SKIP LOCKED` on Postgres, a lease that a stopped reporter's run lapses
from), reads the run's jobs from its cell's dagron-api (`GET
/api/runs/{id}/graph`, which carries no output), posts the statuses that
changed since its last look, and looks again in 5 s while the run runs. Once
every job has finished it reads the run's own status, settles the last
statuses, and leaves the run.

A reporter looks at up to eight due runs at once. When one look ends, it
and every other ended by then are written together: one statement for every
run's record, one for every status they posted, and one for each pull
request whose comment they made due. A look waits only for the write it is
in, and a run that falls due is looked at as soon as a look is free. A burst of ends is then a few statements, and a database that is slow
to take them holds that reporter back and nothing else
(`dagron_ci_report_writes_waiting`, `dagron_ci_report_write_oldest_wait_seconds`,
`dagron_ci_report_batches_total`, `dagron_ci_report_looks_written_total`).
A run is looked at when something of it changed: the reporter follows each
cell's outbox through its dagron-api (`POST /api/outbox/ci/claim`, a claim
that waits up to 25 s), and a task of a run beginning or ending, or the run
ending, makes its record due at once; a run running again re-opens its
record. While a cell's outbox answers, a running run with nothing new is
looked at once a minute, the net for a change lost, where it was every five
seconds; a cell whose dagron-api has none to hand (one older than this
release) or that does not answer has its runs looked at every five seconds,
until it does (`dagron_ci_report_notices_total`,
`dagron_ci_report_feed_failures_total`).
Each status posted, by the reporter or for an event that ran nothing, is a
row of `ci_report_log` with what the forge answered: what was posted when,
which the run's record, the last state of each status, does not keep.

One status per job, under `dagron-ci/<pipeline>/<job> (<event>)`: the
pipeline is its file's name in `.dagron/ci` (a bound workflow's id for a
repository without pipelines), so every run of it on that kind of event
updates the same checks, and a push and a pull request on one commit keep
their own. A merge group reports under its pull requests' names, since a
merge queue waits for the checks branch protection requires of them. Name the
`(pull_request)` ones in branch protection.

A group the queue lets go of without merging it, because a group ahead of it
failed or someone took a pull request out of the queue, is checked by nobody
any more. dagron-ci cancels its runs that are still going, as itself,
through each run's cell, and withdraws its events still waiting; each job's
check then reads `error`, "cancelled". A group that merged is left alone: its
checks are what let it.

| The job | Its status |
|---|---|
| waiting (pending, ready) | `pending`: queued |
| awaiting approval | `pending`: awaiting approval |
| running | `pending`: running |
| succeeded | `success` |
| failed | `failure` |
| cancelled | `error`: cancelled (GitHub's statuses have no cancelled) |
| skipped | `pending` until the run ends; then `success` if the run succeeded, else `error` |
| not finished when the run ended | `error`: did not finish |

A skipped job waits for the run because dagron skips a job whose upstream
failed as it skips one whose `when:` is false, and the first must not pass a
required check. `CI_RUN_URL` gives each status a link to the run.

Statuses go to repositories on github.com, as the App where it is installed
on the repository and otherwise with `CI_GITHUB_TOKEN_FILE`'s token, either
of which needs `statuses: write`. A repository dagron-ci cannot post to (no
token, another forge, a refusal) records why on the run (`ci_runs.report_error`)
and nothing more is posted for it; GitHub's rate limit is waited out, and a
failed post is retried in 30 s. A status GitHub refuses for itself alone (a
422 that is not about the commit: it keeps a thousand statuses under one
context on a commit, and then no more) stops that context for the run and
none of its others, and the run's record says which. Every run is followed to its end all the same,
every 30 s when nothing is posted for it, and its end is recorded in
`ci_runs.status`: the checkout-token endpoint reads it, and mints only for a
run that is not over. A run its cell's dagron-api no longer has is recorded
`gone`. Runs recorded before this existed are not reported.

A run that is run again reads `pending` again and then how it ends, under the
same names: a context never names an attempt (running a run again, below).

**A pipeline its paths skip says so.** A pipeline that takes the event, and
whose `paths:` name none of the files the event changed, does not run. A
branch that requires one of its checks would wait for ever on a pipeline
that said nothing, so each of its jobs gets a status that passes, under the
context its own would have had: `success`, reading "skipped: no changed
file matches on.push.paths" (or `on.pull_request.paths`). It is posted once
the event is settled, and not at each turn of one that is tried again. The
jobs are the ones the pipeline names: one that fans out when it runs has a
status for its name, and none for instances nobody made. A pipeline whose
`on:` does not take the event at all says nothing, as before. Nor does one
that has a run of its own at the commit, from an event that changed what
its paths name: a commit's statuses are read by every branch and pull
request it is the head of, and a passing "skipped" must not stand over a
job that failed there. A run of it that is recorded while the "skipped" is
on its way is told to post its own statuses again, after it, so they are
the last word whichever reached GitHub first. The event's reason says which
it was.

**A pipeline that cannot run says so.** An event's pipeline that is invalid
as written, that was asked for by name and does not take the event, or whose
run dagron-api refuses, makes no run, and the event ends `failed` with the
reason. The commit is told too, once the event has ended for good (or on the
last attempt of one that is still being retried, which the worker then ends): one
`error` status under the pipeline's own name, `dagron-ci/<pipeline>
(<event>)`, reading "not run:" and the reason, in GitHub's 140 characters.
That is no job's context, so it stands in for no job's check. An event that
is retried, or waits, says nothing until it ends. Where the post cannot be
made, the rules are a refused pull request's, below.

**A pull request from a fork is run where the operator says so**
(`CI_FORK_PRS=untrusted`), as an untrusted run: what holds it is dagron's
to do, by the run's record, and
[`docs/CI_PIPELINES.md`](../../docs/CI_PIPELINES.md) ("Pull requests from
forks") says what a fork's pipeline is and is not given. dagron-ci's part is
three things. It sends the record with every submit of such an event, and
refuses the pull request on a cell that would not take it. It asks for an
approval first where one is needed (`CI_FORK_APPROVAL`), as a run of its own
of one approval task, and submits no pipeline of the pull request until that
run has succeeded: its head shows `dagron-ci/approval (pull_request)`,
pending and then how it ended. The approval's run carries the pull request's
number and its commit, and one commit has one, whichever door saw it. And it
keeps nothing of the approval itself: the run is found again by its key,
each time the event is looked at.

**A refused pull request says so.** By default a pull request from a fork or
a non-member runs nothing (`CI_FORK_PRS=refuse`), and nobody should wait on
a check that is not coming. The worker that refuses it gives its head commit
one status:
`error`, under `dagron-ci (pull_request)`, reading "not run: pull requests
from forks and from non-members are refused here (CI_FORK_PRS=refuse)". It is
never one that passes. It names no pipeline and no job, since nothing of the
pull request is read to post it, so it stands in for no job's check: a check
branch protection requires of such a pull request still has no status of its
own. Each event of the pull request says it under that one context, so a
webhook and a sweep that both see a head post it twice and the commit shows
one. GitHub's rate limit is waited out and a failed post is tried again, as
any event's is; a post GitHub refuses (the token, the repository, the commit)
leaves the reason on the event instead. A 403 is read before it is taken
for a refusal. The wait its headers name is the wait; one that names none,
and whose body says it is a rate limit, waits a minute; one whose body
cannot be read is tried again. A run's statuses get the same. A closed pull
request is skipped for being closed, whoever opened it, and gets no status.

### Check runs

With `CI_GITHUB_CHECKS=on`, a job of a repository the App is installed on
is a **check run**, and no commit status. Off, which is the default, and for
any other repository, a job is a commit status as above.

**What the App needs.** The repository permission **Checks: Read and
write**, which each installation accepts once it is added, and, sent to
the webhook dagron-ci receives, the **Check run** event for a check's
"Re-run" and the **Check suite** event for "Re-run all checks". An App
that was not given the permission keeps its statuses:
dagron-ci asks GitHub for a token for the repository's check runs, is
refused, says so in its log, and posts the run's jobs as statuses. It asks
again for a run that starts a minute or more later, so the permission takes
effect once it is given, with no restart.

**One check run a job**, under the name its status would have as its
context, `dagron-ci/<pipeline>/<job> (<event>)`, so what branch protection
requires by name goes on being met. It is on the event's commit, links to
the run's page (`CI_RUN_URL`), and carries the engine run's id.

| The job | Its check run |
|---|---|
| waiting, or awaiting approval | `queued` |
| running | `in_progress` |
| succeeded | `completed`: `success` |
| failed | `completed`: `failure` |
| cancelled | `completed`: `cancelled` |
| skipped | `queued` until the run ends; then `skipped` if the run succeeded, else `cancelled` |
| not finished when the run ended | `completed`: `failure` |

A skipped job waits for the run, as its status does. Where the run did not
succeed it is not posted as GitHub's `skipped`, which a required check is
met by.

**With a token for check runs alone.** dagron-ci mints a second
installation token for the repository, with `checks: write` and nothing
else, and posts check runs with it. The token its reads and statuses are
made with is not given the permission.

**A check run is brought up to date while its job moves, and is not opened
again once it is over.** A job that runs again, after a rerun, is a new
check run of the same name, which is the one GitHub shows for that name
from then on. A run is check runs or statuses from its first post to its
last: turning `CI_GITHUB_CHECKS` on or off changes the runs that start
after.

**What a job said of itself goes with the end of its check run.** When a
job is over, dagron-ci reads the end of its log, from its cell, for the
lines dagron-ci-exec wrote there as the job ended
([its README](../dagron-ci-exec/README.md#what-a-job-says-of-itself)),
and posts them with the writing that completes the check run:

- **Annotations**, from the job's `report:`
  ([docs/CI_PIPELINES.md](../../docs/CI_PIPELINES.md#report)): each test
  that failed in a JUnit report, and each result of a SARIF log, that
  names a file of the repository, on that line of the commit. At most 50,
  which is what GitHub takes with one writing; how many were left out is
  in the check's text.
- **The check's text**: the report's counts, each failure by name with
  the first line of what it said, and any report that could not be read.
- **Under the check's summary**, what the job's steps wrote to their
  summaries (`CI_STEP_SUMMARY`; `GITHUB_STEP_SUMMARY` in a job that came
  from a GitHub workflow), as the Markdown it is.

These are the job's own word, on a stream its steps write to, so a step
can write any of them. They say where a job's tests failed and what it
wants read beside its check; how the job ended is the engine's to say,
and is never read from them. dagron-ci holds them to bounds whatever
wrote them: 50 annotations, a path inside the repository and no other, a
summary of 32 KiB. A check run that is over is not written to again, so
they go once: a log that cannot be read at that moment holds up nothing,
and the check run ends without them and says so. A job reported as a
commit status carries none of this, and its log has the lines.

**"Re-run", beside a check, reruns its run.** GitHub delivers it
(`check_run`, `rerequested`) and dagron-ci runs the check run's engine
run again from its failed jobs, as `POST /ci/v1/runs/{id}/rerun` does: all
of the run's failed jobs, and not that check's job alone.

- **Whose authority.** GitHub's, for who may ask: it offers "Re-run" to
  someone who can write to the repository, and the delivery's signature
  says GitHub sent it. dagron-ci's own, for the asking: the rerun is made
  with `CI_API_TOKEN_FILE`'s token, so dagron-api's record of it names
  dagron-ci, and who asked is in dagron-ci's log.
- **What is run again is the run as it was submitted**: the same commit,
  pipeline and trust.
- **Not taken:** a re-run a bot asked for; one of another App's check run;
  one whose check run names no run of that repository. Each is answered
  202 with why, and nothing is run.
- **A delivery is taken once**, as an event's is: its id is recorded before
  the rerun is asked for, and the same delivery sent again is answered
  `{"duplicate": true}`. One that could not be acted on (the run's cell
  did not answer) is not kept, so that one may be sent again.

**"Re-run all checks" reruns the commit's runs.** GitHub delivers it
(`check_suite`, `rerequested`) with the commit and no run, and dagron-ci
runs again, each from its failed jobs, the runs of that repository at
that commit that it posted check runs for. A run it reported as statuses
is of no suite, and is left. What holds for a check's "Re-run" holds for
it: whose authority it is, what is not taken, and a delivery taken once.
Where some of the runs are run again and one is not (its cell did not
answer or refused dagron-ci's token, or the time GitHub waits ran out
first), the answer has that one in `left` with why, dagron-ci's log has
a warning, and the delivery is taken all the same: the same delivery sent
again is a duplicate, and reruns nothing. A new "Re-run all checks", or
the check's own "Re-run", runs the one that was left again, and leaves
those that are running.

**A job that was not run** (its pipeline does not take the event, or a
workflow's `if:` left it out) is said as a check run too, over and
`skipped`, with why for its title: under one name a status and a check
run are two lines, and a run's own check run could not replace a status.
What dagron-ci says of a whole pipeline it did not run, and of a pull
request it refused, has a name no job has, and is a commit status still.

**Not built yet:** annotations from a workflow's commands (`::error
file=…::`), which are lines of the log; a report for a job that came from
a GitHub workflow, which has no `report:`; and more annotations than one
writing takes. A check run whose post succeeded and whose id dagron-ci
then failed
to record is posted again at the next look, as a second check run of the
name.

### A pull request's comment

With `CI_GITHUB_PR_COMMENT=on`, a pull request of a repository the App is
installed on is given **one comment**, which says how the runs of its
latest commit went; and with `CI_GITLAB_MR_NOTES=on`, a GitLab merge
request one note, which says the same, with its host's token:

```markdown
### dagron-ci: 1 job failed

Commit `4b4708e725f2`: 4 jobs, 3 passed, 1 failed.

| Pipeline | Job | Result | Tests |
|---|---|---|---|
| [`ci`](https://dagron.example.com/runs/…) | `build` | passed | 212 passed |
|  | `test` | **failed** | 3 of 120 failed |
| [`docs`](https://dagron.example.com/runs/…) | `check` | passed |  |

**Tests that failed**

- `ci` / `test`: `tests/test_size.py::test_round` and 2 more
```

**One comment, written again in place.** It is made when the first of the
pull request's runs is looked at, and brought up to date as its runs
change. A new commit's runs take the place of the last commit's in it, so
a pull request is not given a comment a push. One that someone deleted is
made again the next time there is something to say. It is written at most
once in twenty seconds: a run's jobs change one after another, and the
comment is not edited for each. One writer has a comment at a time, for
longer than GitHub is given to take it, so that two replicas do not make
two comments of one.

**What it says.** Each pipeline that ran for the commit, linked to its
run's page where `CI_RUN_URL` names one, and each of its jobs with how it
stands, in order of their names. A job that is over is asked, once, what it said of its tests (its
`report:`, as a check run is): how many ran and failed, and the first five
that failed by name. A run nobody has looked at yet reads `waiting`.

**Whose words are in it.** How a job ended is the engine's word. A
pipeline's name, a job's and a test's are the repository's to choose, and
a pull request's author's where the pull request changes them: each goes
in as code, cut to a length, with nothing that would end the code span or
a table's cell. None of them is read as Markdown, mentions a person, or
links anywhere.

**With a token of its own.** The comment is the App's, written with an
installation token for that one repository's `pull requests: write` and
nothing else. An App that was not given the permission writes no comment,
says so in dagron-ci's log, and its statuses and check runs go on as they
were. It is asked for again the next time a run of the pull request
changes.

A commit none of whose pipelines ran leaves the comment as it was: it
names the commit it speaks of.

**Not built yet:** a comment for a repository with a token and no App; a
count of how often a comment was written (a refusal is in dagron-ci's log,
and in `ci_pr_comments.error`); a comment on a merge group; and a link
from a failed test to its line, which a check run's annotations give. A
comment that GitHub made and whose answer never arrived, or whose id
dagron-ci then failed to record, is made again as a second comment:
nothing reads a pull request's comments back. So is one whose writer was
held up for longer than it holds the comment, where another has written
meanwhile.

### A flaky job's issue

With `CI_FLAKE_ISSUES=3`, a job that passes only after an attempt of it
failed, three times within `CI_FLAKE_DAYS` (14 days unset), is filed an
issue of its repository, which lists each time:

```markdown
**`test`** of `ci` passed only after an attempt of it failed, 3 times in the last 14 days.

| When | Commit | Failed attempts | Run |
|---|---|---|---|
| `2026-10-20 12:30` | `4b4708e725f2` | 1 | [run-…](https://dagron.example.com/runs/…) |
```

**What a flake is.** The look that sees a run end asks its cell, for each
job that passed on a later attempt, or passed in a run that was rerun, how
the attempts before it ended (`GET /api/runs/{id}/tasks/{tid}/attempts`).
Either of two makes the job a flake of that run: one the engine retried
after it failed (`max_attempts`, `retry_budgets`), or one that failed and a
person reran (`dagron-ci rerun`, a check's Re-run, the console). The same
commit failed and then passed, so neither result proved anything. A
`repeat:` task's earlier passes are not failures, nor is a job that passed
and was rerun whole, or one that was cancelled and rerun. A cell that keeps
no attempts (`DAGRON_ATTEMPT_LOG_BYTES=0`) says of none, and none is counted.

**Whose.** A trusted run's alone, and no patch bundle's: what a stranger
wrote never files an issue of the repository. Each job's flake is kept
once a run (`ci_flakes`), however often the run's end is looked at, and
forgotten after `CI_FLAKE_DAYS`.

**One issue, written again in place.** A job has one, of its pipeline,
opened at the Nth flake and brought up to date at each after. A person
closes it once the flake is dealt with: dagron-ci leaves it closed, and
counts the job's flakes again from when it was closed, opening another
once they come to N. One that was deleted is not opened again until the
job's flakes come to N anew. One writer has an issue at a time, as a
pull request's comment has.

**With a token of its own.** The App's, for that one repository's
`issues: write` and nothing else; or the token `CI_GITLAB_TOKENS_DIR` keeps
for a GitLab project's host. An App that was not given the permission
files none, says so in dagron-ci's log, and the rest goes on.

**Not built yet:** a flake of a test rather than a job (a job's tests are
read for a check run; a test's history across runs is not kept); a label
on the issue; and an issue for a failure that comes back without a pass
between.

### Fleet mode

A repository keeps GitHub Actions and its workflow files as they are, and
changes one thing: a job's `runs-on`. A job that asks for one of the
operator's labels is run by **GitHub's own runner**, which dagron-ci starts
for it as a dagron run, on the operator's capacity. GitHub Actions stays
the coordinator: it decides what runs, shows the logs and holds the
checks. dagron-ci posts nothing on the commit for such a job. A GitLab
project does the same with a job's `tags` ([below](#gitlabs-fleet)).

```yaml
# CI_GITHUB_FLEET_FILE
labels:
  dagron:
    image: ghcr.io/actions/actions-runner:2.321.0
  dagron-large:
    image: ghcr.io/actions/actions-runner:2.321.0
    runner_class: large      # the engine's runner class the task asks for
    pool: ci-large           # the engine's pool it is queued in
    priority: 5
    timeout_secs: 7200       # six hours unless said
    with: [self-hosted, linux, x64]
    scope: organization      # the repository's owner's runner (a repository's unless said)
    runner_group: 3          # in this runner group of the organization's (1, Default, unless said)
    max_runners: 20          # out at once, for every repository together
    max_per_repository: 5    # and for one
    untrusted: floor         # a fork's job: refuse (unless said), floor or serve
    untrusted_runner_class: sandbox
    docker: true             # a Docker daemon beside the runner
    warm: 2                  # runners kept ready beyond the jobs that wait
    warm_for_secs: 1800      # for a repository whose last job of it is this recent (an hour unless said)
    retry: true              # a job whose runner died is run again, once (unless said)
```

**The label map is the operator's**, read and checked when dagron-ci
starts. A label decides which image is run, on which runner class and in
which pool, how many may be out and what a fork's job is given, which is
capacity, and no repository's to say. A repository chooses among the labels
by writing one in `runs-on`.

**Which jobs are served.** GitHub gives a job to a runner that has every
label the job asks for. So a job is served when exactly one of its labels
is a label of the file's, and each of its others is one that label goes
`with` (`self-hosted`, `linux` and `x64` unless said). A job with any
other label, or with none of the file's (`ubuntu-latest`), is another
runner's and is left alone: nothing is kept of it. A label GitHub gives
every self-hosted runner may not be a label of the file's. GitHub gives a
job that asks for those labels alone (`runs-on: self-hosted`) to any
self-hosted runner, the fleet's among them: such a job, taken by a runner
of the fleet's, is kept as a job of that runner's label, told apart and
counted as the label's own are.

**What it needs.**

- The repository connected, as for anything else.
- The App, with the repository permissions **Administration: Read and
  write** and **Actions: Read**, and the **Workflow job** event sent to
  dagron-ci's webhook. GitHub puts registering a runner under a
  repository's administration, which is far more than the two calls
  dagron-ci makes with it. So the token for it is one of its own: minted
  for the one repository, used to register a runner and to remove one, and
  carried by nothing else dagron-ci does. **Actions: Read** is for telling
  a fork's job apart and for asking how a job stands; with an App without
  it, every job of a label that tells forks apart (every label, unless it
  says `untrusted: serve`) is taken for a fork's: given no runner, or a
  floor runner with `untrusted: floor`. **Actions: Read and write** lets a job whose
  runner died be run again; without it, none is. Each is a token of its
  own too.
- For `scope: organization`, the App installed on the organization with the
  organization permission **Self-hosted runners: Read and write**, again
  with a token of its own, minted for the installation and for no
  repository.
- A cell and dagron-ci's token for it (`CI_CELLS`, `CI_API_TOKEN_FILE`),
  and a cell whose dagron-api keeps secrets (`DAGRON_ENV_SECRET_KEY`).
- The webhook's secret (`CI_GITHUB_WEBHOOK_SECRET_FILE`) in the replica
  that takes deliveries: a job is heard of by its delivery. A replica
  without it starts the runners for the jobs another took, and says as it
  starts that it takes none itself.

dagron-ci does not start with a label map and no cell, no token or no App.
On the chart the map is `ci.fleet.labels`, and the render fails on a map
dagron-ci would not start on, and on one with no `ci.webhookSecret`.

**A runner is a dagron run of one task**, named `fleet-<label>`: the
label's image, started as GitHub's runner image starts
(`/home/runner/run.sh`, or the label's `command` for an image that starts
another way), with a just-in-time configuration GitHub mints for it. The
runner takes one job and exits, and GitHub forgets it. With `docker: true`
the task has a Docker daemon beside it (`docker_daemon`), where the engine
offers one (`DAGRON_DOCKER_DAEMON=dind`); an engine that does not fails the
runner's task, and says why.

**A runner is not one job's.** It is registered for labels, and GitHub
gives it whichever waiting job asks for them. dagron-ci keeps as many
runners out, and not yet busy, as jobs wait, for each label. It looks every
five seconds while a job waits or a runner is out, and starts at most eight
runners for a repository at a look.

- **An organization's runner** (`scope: organization`) is registered for the
  repository's owner, in the label's runner group, and GitHub may give it a
  job of any repository the group lets in: which repositories and
  workflows a group serves is the organization's to say, on GitHub. Which
  runner took which job is recorded by the runner's name, whichever
  repository the job is of, a repository dagron-ci does not serve
  included. A user's repository has no organization, and its runner is
  not registered, and says so.
- **A label's quota** holds its runners out to `max_runners` for every
  repository together, and to `max_per_repository` for one. A job past it
  waits for one of the label's runners to end. The count and the runner's
  row are one statement, so two replicas looking at once cannot both take
  the last place.

**A fork's job.** A job of a workflow run whose head is in another
repository, a fork's pull request, is told apart by asking GitHub for the
workflow run, once a run, and its label says what it is given:

- `untrusted: refuse`, unless said: no runner. The job waits on GitHub
  until someone cancels it, or another runner takes it.
- `untrusted: floor`: a runner submitted as a run that is **not trusted**,
  which the engine holds to its floor for such runs
  (`DAGRON_UNTRUSTED_ISOLATION_FLOOR`, `DAGRON_UNTRUSTED_LIMITS`): it is
  given none of its label's pool, priority or Docker daemon, asks for the
  label's `untrusted_runner_class` where it names one, and has its
  configuration in an environment of its own
  (`CI_FLEET_UNTRUSTED_ENVIRONMENT`, `dagron-ci-fleet-untrusted` unless
  said). dagron-ci makes that environment where the cell has none, and
  gives it a policy that admits such runs of the fleet's alone, where it
  has no policy and dagron-ci's token may write one; one an admin gave a
  policy is left as it is, and where it admits no such run, no runner is
  started, and the reason says what to set. The cell has to take a record
  of what started a run from dagron-ci's token (the `ci:trigger` group):
  without one nothing would say the run is not trusted, and no such runner
  is started.
- `untrusted: serve`: a runner as any job's, for a repository whose forks
  are all its own people's. GitHub is not asked.

A job not yet told apart is taken for a fork's. Floor runners and others
are counted apart.

**GitHub gives a waiting job to any runner with its labels**, so a fork's
job may be taken by a runner started for another job. An organization's
runner groups can keep that from happening: a group restricted to
selected workflows at a branch's ref is given no pull request's job.
Where it happens all the same, dagron-ci stops that runner's run as it
hears of it, and the job fails on GitHub. So it does where a job on a
trusted runner could not be told apart within thirty seconds (GitHub did
not answer, or the App has no **Actions: Read**): it is taken for a
fork's. What the job ran until then, it ran: on a repository whose forks'
workflows run without approval, the only runner that is sure to be the
floor's is the floor's own label.

**Warm capacity.** A label's `warm` runners are kept out beyond the jobs
that wait, for a repository that has had a job of the label within its
`warm_for_secs`: a job queued then is given one at once, and another is
started in its place. They are trusted runs, and count against the label's
quota. One that has waited half an hour, or a quarter of its label's time
where that is less, is taken back and started anew, one a look, so that a
job given it has most of its time. A warm runner is an idle trusted runner,
which GitHub would give a fork's job as soon as one waits: a label keeps
runners warm where it says `untrusted: serve`, or names a `runner_group`
restricted to selected workflows, which is given no pull request's job,
and dagron-ci does not start with a label that keeps them otherwise. On
the chart, `ci.fleet.prepull` pulls each label's image onto each node a
runner may start on, before one does.

**A job whose runner died is run again, once.** A job that failed on a
runner of the fleet's whose run did not succeed failed because its runner
died (its pod was evicted, its node lost, its process killed), and not
because of what it ran: GitHub's runner ends well after its one job,
however the job ended. It is run again through GitHub's API, as a new
attempt of its workflow run. GitHub runs a job again once its workflow run
is over, so the job is asked for each minute, for an hour. Not for a runner
that had its label's whole time, which the next would have too; not for one
dagron-ci stopped itself; and never twice for a job of one name of a
workflow run. Whether the runner died is its run's to say, whether GitHub
or the look heard first that the job had ended; a job of which a later
attempt is heard of was run again, whoever asked, and is not asked for
again. `retry: false` turns it off for a label.

**The configuration is a secret.** Whoever holds it is the runner, and is
handed the job's own secrets by GitHub. It is never in the run's spec, its
parameters or a task's `env`, which a reader of the run sees. dagron-ci
writes it as a secret of an environment of the cell's dagron-api
(`CI_FLEET_ENVIRONMENT`, `dagron-ci-fleet` unless said, made where the cell
has none; a floor runner's, `CI_FLEET_UNTRUSTED_ENVIRONMENT`), which keeps
it encrypted and gives it back to nobody; the run's task names it, and the
engine resolves it as the task starts. It is deleted once the runner is
over. Someone who may submit a run to that cell may name that environment
and that secret while a runner is out, as with any environment's secrets;
a run that is not trusted may name the floor's, whose runners are given
nothing a fork's pull request is not.

**Clearing up.**

- A runner whose job ended has its registration removed, where GitHub
  still has one, and its secret deleted.
- **A runner whose job has ended is left to exit by itself.** GitHub says
  a job has ended while its runner is still sending the last of the job's
  log. Its run is asked to stop only if it is still running thirty seconds
  on, so a run that hosted a job to its end reads as it ended.
- **A runner nothing waits for is taken back**: its job was cancelled as it
  waited, or another runner took it. After five seconds spare, its
  registration is removed first, so that GitHub can give it no job, and
  its run is then cancelled. Where GitHub answers that it is running a
  job, it has one dagron-ci had not heard of, and it is left to it.
- **What a delivery did not say is asked.** A job that has waited a minute,
  and one whose runner has been over a minute with no end of the job
  heard, is asked of GitHub (`GET …/actions/jobs/{id}`), four a look and
  each at most every five minutes, and its row is moved on to what GitHub
  says. A job GitHub no longer has is over.
- A runner that could not be started (the App lacks a permission, the cell
  refused the run) says why on its row, its job waits, and none other of
  its label and trust is started for a minute. The repository's other
  labels, and what the look does besides, go on.
- A repository that is disconnected is started no runner, and what it has
  out is taken back. Its jobs that waited are closed with the disconnect:
  no delivery of it is taken after, so nothing would say they had ended.
  A job whose delivery arrives as the disconnect is made is not kept
  waiting either.

#### GitLab's fleet

A GitLab project's job whose `tags` name one of the operator's labels is
given **GitLab's own runner**, `gitlab-runner run-single`, started as a
dagron run of one task in the same way. Its label map is a file of its own
(`CI_GITLAB_FLEET_FILE`), of the same keys:

```yaml
# CI_GITLAB_FLEET_FILE
labels:
  linux:
    image: gitlab/gitlab-runner:v17.5.0
  docker-big:
    image: registry.example.com/runners/gitlab-with-tools:1
    with: [docker]           # tags a job may name beside this one: none unless said
    scope: organization      # a runner of the project's group
    max_runners: 10
```

- GitLab gives a job to a runner that has every tag the job names, as
  GitHub does with labels, so a job is served as a workflow job is. GitLab
  gives a runner no tags of its own: a label goes `with` none unless it
  says, may be any name, and takes no `runner_group`. dagron-ci reads a
  job's tags in any case, and makes its runner with the label's in lower
  case: write a job's tags so.
- **Its runner** is made just before it starts (`POST /user/runners`), for
  the project, or for the project's group with `scope: organization`, with
  the label's tags and no untagged job, and removed once it is over. Its
  token is kept as a secret, as GitHub's configuration is. Its task is told
  by its environment where the instance is (`CI_SERVER_URL`), its token
  (`CI_SERVER_TOKEN`, named and not written), that it runs a job's script in
  its own container (`RUNNER_EXECUTOR=shell`), and its name; it runs
  `gitlab-runner run-single --max-builds 1` unless its label's `command`
  says otherwise. On the Docker executor an image's entrypoint is kept and
  the task's command given to it, and `gitlab/gitlab-runner`'s entrypoint
  is `gitlab-runner` itself: there, a label of that image says
  `command: [run-single, --max-builds, "1"]`.
- **A job is heard of by its Job Hook**, which names none of its tags: the
  project is looked over, and the look asks GitLab for the project's jobs
  that wait (`GET /projects/{path}/jobs?scope[]=pending`, five pages of a
  hundred at most, newest first: one past them is read once those before it
  are given runners or end, and the look says so in its log) and writes
  down those the fleet serves. A hook that
  comes while a look goes has another look follow it. What became of a job
  is the hook's to say after that, and GitLab's to answer where no hook
  came.
- **A merge request's job is told by its merge request**: its project's
  own where the merge request's source is the project, a fork's
  otherwise. A branch's or a tag's job is the project's own, and GitLab is
  not asked. A job a runner took before a look listed it, or one listed
  with no ref, is told by its pipeline's ref (`GET
  /projects/{path}/pipelines/{id}`); a pipeline with none is a fork's. A
  trusted runner that took a fork's is stopped, as on GitHub.
- **A runner that is running a job is not removed** (`GET
  /runners/{id}/jobs?status=running`): GitLab would remove it, and the job
  would fail with it.
- A job whose runner died is run again at once (`POST
  /projects/{path}/jobs/{id}/retry`): GitLab does not wait for the
  pipeline.
- **What it needs**: a token of `CI_GITLAB_TOKENS_DIR`'s for the host, of an
  Owner of the project (of the group, for its group's runners: GitLab
  makes a runner through its API for an Owner alone), on an instance that
  lets a project's and a group's members make runners; with the `api`
  scope, or `read_api`, `create_runner` and `manage_runner`, which serve
  the fleet alone (the host's own pipelines need `api`, as
  [its token](#gitlab) says);
  the project's webhook sending **Job events**, with
  `CI_GITLAB_WEBHOOK_SECRET_FILE`'s token; and a cell, as GitHub's fleet
  does. A label may not be a label of both files.

#### Showback

**What the fleet's runners did**, over whole days (UTC), by repository, by
team (a repository's owner: an organization, a user, a top-level group) or
by label: the jobs that started on them in the days and how long they ran,
to their end or to now, a job being its own repository's whichever
repository started the runner; and the runners submitted in the days and
how long they were out, the time kept warm or waiting for a job included,
a runner being the repository's it was started for. It is for showing who
used the capacity, and never to charge for it.

```sh
dagron-ci fleet usage --since 2026-10-01 --until 2026-11-01 --by team
curl -H "Authorization: Bearer $ADMIN_TOKEN" \
  "https://ci.example.com/ci/v1/fleet/usage?since=2026-10-01&by=label"
```

The thirty days to the end of today unless said, and a year and a day at
most; `GET /ci/v1/fleet/usage` is an admin's, as the repositories' door
is. What is counted is what is kept: with `CI_RECORD_DAYS` set, a job that
ended and a runner cleared up before its horizon leave the database, and
are written to no store. `/metrics` has the jobs that wait and the runners
out, by label and state ([Metrics](#metrics)).

**What is not built, and what to know before turning it on.**

- **A fork's job may be given another job's runner**, as above. On a public
  repository whose forks' workflows run without approval, GitHub's advice
  against self-hosted runners holds here, floor or not.
- **A VM's isolation** for a label: a runner class is the engine's, and
  none is a VM's.
- **A count of jobs by workflow run**, which an invoice would want, and which
  showback does not.
- **On the chart**, GitLab's map: the chart sets up no GitLab yet, so
  `CI_GITLAB_FLEET_FILE` is a file of the operator's own mounting.
- **A job GitHub never delivered at all** is not looked for: dagron-ci
  asks about the jobs it has heard of, and lists no repository's queue on
  GitHub.

### The checkout

A compiled task's workspace starts with the event's commit in it. The
workflow names it (`workspace.checkout`: the repository and the commit), and
the engine fetches it before the task runs: that one commit by its id, at
depth 1 and without tags, then checked out detached, so a branch that moved
on since the event changes nothing. On Kubernetes an init container fetches
it, after the one that installs `dagron-ci-exec`; on Docker a container of
its own, into the task's workspace volume; on the local executor, git on the
engine's machine. Docker and Kubernetes engines need `DAGRON_CHECKOUT_IMAGE`
for it, any image with `git` and `sh` (`alpine/git`). A fetch that fails
fails the attempt, with what git said. Git in the steps works on the
checkout as it is: the task's environment marks the workspace safe, since
the checkout may be another user's than the steps'.

**A credential for one attempt.** Before each attempt the engine asks
dagron-ci for one (`POST /ci/v1/checkout-tokens`, its
`DAGRON_CHECKOUT_TOKEN_URL`) with the secret both were given, naming the run,
the repository and the commit. dagron-ci answers only for a run it submitted
that is not over (or that its cell says is running again: below), and only
for that run's own repository
(`https://<forge host>/<owner>/<name>.git`, as the compiler writes it) and
commit, so the secret cannot be spent on anything else the App may read. The
credential is an installation token of dagron-ci's GitHub App for that one
repository, `contents: read`, which GitHub expires within the hour; the
engine says when the attempt is over (`DELETE`), and dagron-ci revokes it
then. A retry gets a token of its own.

**A run it has only just submitted.** dagron-api creates a run, and its
engine can start the run's first task, before dagron-ci has read the answer
that names it. An engine that asks in between is asking about a run that is
not on dagron-ci's record yet. Such a run is looked for again, for two
seconds, before the engine is told it is not one dagron-ci submitted (404).
The looks come out of the eight seconds an answer may take, so the engine is
still answered inside its ten.

**A run that is not dagron-ci's.** An engine that names dagron-ci as its token
service asks it about every checkout, a workflow's that has nothing to do with
CI included. What that engine is told, once the run has been looked for, turns
on whose repository the checkout names:
- **a repository dagron-ci does not serve:** 204, no credential. The commit is
  fetched without one, as an engine with no token service fetches it: enough
  for a public repository. Such a task waits out the two seconds first;
- **a repository dagron-ci serves:** refused (404), and its task fails. The
  runs of a connected repository are dagron-ci's to submit: one that is not on
  its record (a resubmit of a CI run, or a hand-written workflow over the same
  repository) would have no checks and no count.

Neither answer hands anything out. The repository is told by its host and
`owner/name`, without regard to case, to a trailing `.git` or to the scheme.

**Bound to its cell.** A secret is one cell's: a file in
`CI_CHECKOUT_TOKEN_SECRETS_DIR` named as the cell, the way a Kubernetes Secret
with a key per cell mounts. An engine holding one cell's secret is answered for
the runs dagron-ci submitted to that cell alone (403 for any other), so where
cells are tenants, one's engine cannot mint for another's run. A cell's secret
must be its own: one shared by two cells stops start-up, as does a file for
a cell `CI_CELLS` does not name. `CI_CHECKOUT_TOKEN_SECRET_FILE` is one secret
that answers for every cell, which is right where there is one cell.
dagron-ci does not start on it where `CI_CELLS` names two: an engine of
either could ask for the credential of the other's run.

**On record.** Each credential has a row (`ci_checkout_credentials`) that
names the run, the task and the attempt it was minted for, its repository
and commit, the cell that asked, when it was minted and when GitHub says it
lapses. The row is written before the credential is handed over. One that
cannot be recorded is revoked instead, and the attempt is told why (503).
The revoke marks the row with when; a row never marked is of a credential
that lapsed at its expiry. The row holds the credential's SHA-256, by which
the revoke finds it, and never the credential. Nothing drops these rows yet.

**The steps never hold it.** It reaches git as an HTTP header in git's
environment (`http.<repository>.extraHeader`, through `GIT_CONFIG_*`), never
in a URL or the repository's config, and only over HTTPS (or to the engine's
own machine). The fetch that carries it follows no redirect: git would send
the header on to wherever one moved the fetch, so a repository that answers
with a redirect fails its checkout instead. On Kubernetes it goes where the
task's other secrets go, its Secret (or inline, under
`DAGRON_TASK_SECRET_ENV=inline`), and only the checkout's init container reads
it. The engine redacts it from the task's log.

**Without one.** With no App, for a repository not on github.com, or one the
App is not installed on, dagron-ci answers 204 and the commit is fetched
without a credential: enough for a public repository, and a private one's
checkout fails, saying so. An engine without `DAGRON_CHECKOUT_TOKEN_URL`
fetches every checkout that way.

### Whose authority a call carries

dagron-ci's own calls for a github.com repository (its refs, its open pull
requests, a commit's pipelines and changed files, and the statuses it posts)
are made **as the App, where the App is installed on the repository**: with
an installation token minted for that repository alone, to read its contents
and pull requests and to post commit statuses, and nothing else. GitHub
refuses that token for any other repository, so what reads one repository
cannot read another. A token is used for fifty minutes and then minted
again; GitHub expires it at the hour. One GitHub answers 401 to is dropped at
once, and a status posted with one held a minute or more is tried again with
another.

`CI_GITHUB_TOKEN_FILE` is the fallback: for a repository the App is not
installed on (GitHub is asked again every five minutes whether it is), for
one the App cannot have a token for (it was not given those three
permissions; said in the log, and asked again in a minute), and for
everything where there is no App. With neither, a repository is read as
anyone reads it, which a public one allows, and no status is posted.

Where GitHub does not answer when the token is asked for (it is not reached,
it fails, or it is limiting requests), the fallback makes the call too, and
the token is asked for again five seconds on. Without a fallback the call
fails and is tried again. It is not made as nobody: GitHub answers an
anonymous caller as if a private repository held nothing, and dagron-ci
would take that for a commit with no pipelines.

Nothing of this goes to another host: a repository on GitHub Enterprise or
another forge is read without credentials, as before, and a GitLab project
with its host's token alone.

### The App's key, held elsewhere

One App's private key mints an installation token for every installation of
the App, whoever installed it. Where one App serves many dagron-ci instances,
each serving its own people, a key in each of them is a key that, leaked
from any one, reads every installation's repositories. So the key may be
held by one service instead, which each dagron-ci asks for the tokens it
needs: `CI_GITHUB_APP_TOKEN_URL`, in place of `CI_GITHUB_APP_KEY_FILE`, asked
under `CI_GITHUB_APP_TOKEN_SECRET_FILE`'s secret. That dagron-ci holds no
key, and asks GitHub nothing with one; it still revokes a token it was given,
as the token itself.

The request is GitHub's own, by repository:

```http
POST <CI_GITHUB_APP_TOKEN_URL>
Authorization: Bearer <the secret>

{"repository": "acme/widgets", "permissions": {"contents": "read"}, "one_repository": true}
```

`one_repository` is false for an organisation's token (fleet mode's runners
of an organisation). The service answers GitHub's `{"token", "expires_at"}`,
and 404 where the App is not installed on the repository, or not for this
caller: dagron-ci takes that as an App that is not installed there, and
falls back as it does then. Its other answers are read as GitHub's are: a
401, a 422, and a 403 that is no rate limit are its last word, and anything
else may be answered next time. It decides which installations each caller
may have tokens for, and keeps the record of each it gives; dagron-ci only
asks.

To give checkouts a credential, and dagron-ci's own calls the App's:
1. register a GitHub App with repository permissions **Contents: read**,
   **Pull requests: read** and **Commit statuses: read and write** (it needs
   no events or webhook), and install it on the repositories dagron-ci
   serves. With Contents alone it still mints checkouts' credentials, and
   dagron-ci's own calls fall back to `CI_GITHUB_TOKEN_FILE`;
2. give dagron-ci its id and private key: `CI_GITHUB_APP_ID`,
   `CI_GITHUB_APP_KEY_FILE`;
3. give each cell's engines a secret, `DAGRON_CHECKOUT_TOKEN_SECRET_FILE`,
   and dagron-ci the same: the cell's file in `CI_CHECKOUT_TOKEN_SECRETS_DIR`,
   or, where there is one cell, `CI_CHECKOUT_TOKEN_SECRET_FILE`; and
   give the engines
   `DAGRON_CHECKOUT_TOKEN_URL=https://<dagron-ci>/ci/v1/checkout-tokens` and
   `DAGRON_CHECKOUT_IMAGE`. The URL is over TLS: an engine starts on no
   token service over plain http, unless it is on the engine's own machine.

### The poll sweep

Webhooks are for latency; the sweep is what catches the commit a dropped
delivery missed, and it serves a forge that cannot reach dagron-ci at all.
Each replica runs four sweepers. A sweeper claims the most overdue repository
(`FOR UPDATE SKIP LOCKED` on Postgres, so replicas share the work) and reads:

- **its branches and tags**, with git's own ref listing over HTTPS, the
  request `git ls-remote --heads --tags` makes. dagron-ci speaks the protocol
  itself (protocol v2, which sends only the refs asked for, or v0 from an
  older server), so its image needs no git binary and a token never reaches a
  command line. Any forge that serves git over HTTPS answers this request.
  The answer has been read in three framings: a v2 answer that opens with
  `version 2`, as git's own server sends it; one that names the service
  first, as github.com does; and v0. Which of them another hosted forge
  sends has not been checked;
- **its open pull requests**, on GitHub, from the API. While they fit on one
  page the request carries the last answer's ETag: an unchanged list answers
  304, which GitHub does not count against the rate limit.

It compares them with what the last sweep saw (`ci_poll_refs`), and queues:

| What changed | Event |
|---|---|
| a branch is new or moved | `push`, `trusted` |
| a tag is new or moved | `tag`, `trusted` |
| a branch or tag is gone | `delete`, for the commit it last pointed at |
| a pull request opened or pushed to | `pull_request`, its trust decided as for a webhook's |
| a pull request closed | nothing: it is forgotten, as closing one runs nothing |

**An annotated tag runs at the commit it names.** Such a tag is an object of
its own, with an id of its own, and that id is what a push webhook reports.
Its event is queued under the tag's id by a webhook and by the sweep alike, so
the two make one run, and `dagron-ci events` lists it under that id. The
commit is kept beside it: the sweep asks git for it with the listing (`peel`),
and a GitHub push names it in `head_commit`. That commit is where the
pipelines are read, what `ci_sha` and the checkout say, and where the statuses
go. A tag that is a commit's own name (a lightweight one) has one id, and
nothing changes for it.

**The first sweep of a repository queues nothing.** It records what it finds,
so connecting a repository does not run every branch it has. After that, a
sweep queues what changed: one event per moved branch, for its newest commit,
however many pushes it took, measured against the commit the last sweep saw.

**A failed sweep is tried again next interval**, and `dagron-ci repo list`
(or `GET /ci/v1/repos`)
shows why (`poll_error=`). A spent GitHub rate limit holds the repository's
next sweep until the limit resets, at most an hour. A sweep commits its events
and what it saw in one transaction, fenced on its claim: a sweeper that
outlived its claim (five minutes) writes nothing, and one that stops mid-sweep
leaves the repository due again once the claim lapses. Each next sweep is
jittered by a tenth either way, so repositories connected together drift
apart.

`dagron-ci repo set HOST/NAME --poll-secs N` gives a repository its own
interval (`0` never polls it, `default` returns it to `CI_POLL_SECS`); the
change applies at once. The App's tokens and `CI_GITHUB_TOKEN_FILE` go to github.com only. A
GitLab project is polled with the token `CI_GITLAB_TOKENS_DIR` keeps for its
host, where it keeps one, so a private one can be ([GitLab](#gitlab)). A
repository on another host, GitHub Enterprise included, is polled without
credentials for now, so it must be public to its network.

### Connecting a repository

A repository is connected at a terminal, by whoever can reach the database
(`dagron-ci repo add`), or through the API, by an admin:

```sh
curl -X PUT https://ci.example/ci/v1/repos/github.com/acme/widgets \
  -H "Authorization: Bearer $DAGRON_API_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"workflow": null, "poll_secs": 600}'
```

**Who.** The route takes the credential the run API takes, a dagron-api
session or API token, and asks the `default` cell's dagron-api whose it
is (the first cell's by name, where none is called `default`). The caller
is in dagron-api's `admin` group, and is not an agent: an agent's token
connects nothing, whatever groups it carries. A viewer changes nothing, as
in dagron-api: an admin who is also in `viewer` may read the repositories,
and their `PUT` and `DELETE` are refused. An admin of that one cell says
which repositories run on every cell.

**What a `PUT` says.** The repository is the path: `host/owner/name` as
its forge names it, or the id dagron-ci gave it when it was first
connected, which every answer carries and which stays the same whatever
the forge calls it (an id has no `/`, and a name has two). An id names a
repository that was connected once, so its `PUT` changes it, or connects
it again. The body may be empty, and its keys are `forge` (`github` unless
said, or the forge it was on where it was connected before, and said once:
a connected repository's forge is not changed), `mode` (below), `cell`
(one of `CI_CELLS`), `workflow` (an id, or `null` for none) and
`poll_secs` (0 for never, 30 to 86400, or `null` for `CI_POLL_SECS`). For a
repository connected already a key that is absent leaves what it names as
it is. Any other key is refused. A `PUT` that says what is so already
changes nothing and records nothing (`"action": "unchanged"`), so one sent
on a schedule is safe.

**How it is served** (`mode`, `--mode`), one of three ways in:

| Mode | What dagron-ci runs of it |
|---|---|
| `native` (the default) | its `.dagron/ci` pipelines, and the workflow bound to it |
| `compat` | those, and the forge's own files its `.dagron/ci.yaml` names (`.github/workflows` with `CI_GITHUB_WORKFLOWS=on`, `.gitlab-ci.yml` with `CI_GITLAB_PIPELINES=on`): the files the team keeps, run in their forge's place; and a GitHub workflow's `on.schedule` |
| `fleet` | none of its pipelines, which its forge runs: dagron-ci runs the runners of the jobs that ask for one of its labels ([fleet mode](#fleet-mode)). A delivery of its pushes and merge requests is answered and dropped (one that lets go of a merge group is queued still, so that the group's runs from before are cancelled), it is not swept, a run asked of it, or asked again, is refused (409), and an event of it that waited when it was put in fleet mode is skipped |

A job asking for a fleet label is served whatever the repository's mode: a
label is the operator's, and a team in native mode may keep a workflow or
two on its forge. A repository connected before there were modes is in
`compat`, as it was served: the forge's files were read wherever the build
read them. One that leaves `fleet` is swept next from a baseline, so what
moved while it was in it is recorded and not run.

**Each change is recorded with who made it**, in the transaction that makes
it: the caller's email as dagron-api has it, or their subject where they
have none; `cli:` and the operator's name for a change made with
`dagron-ci repo` (`DAGRON_CI_ACTOR`, else the user the command runs as,
which is what the operator says and no more). `GET` on the repository, and
`dagron-ci repo show HOST/NAME`, list them oldest first. A repository
connected before this was kept has no change on record.

**A disconnect keeps what the repository had.** `DELETE`, and
`dagron-ci repo rm`, leave its events and runs to be read as before
(`dagron-ci events`, `GET /ci/v1/runs/{id}` and a run's logs). From then
on:

- A delivery for it is refused (404), a run asked for it is refused, and it
  is swept no more.
- Its events that waited are not run: each ends `skipped`, with
  `its repository was disconnected` for its reason. One a worker held at
  that moment is skipped when it would next be tried.
- **A run already going is not stopped.** Its statuses are still posted.
  A job of it that has yet to check out fails at its checkout: no
  credential is minted for a disconnected repository. Cancel the run to
  stop it at once (`POST /ci/v1/runs/{id}/cancel`).

**Connected again, it is the repository it was**: the same id, on the
same forge unless the connect names another, with its events, runs and
changes under it. It is set to what
that connect names, as a first time, so a workflow, an interval or a mode
it had is gone unless named again. Its next sweep records where each branch is and runs nothing, so
what moved while it was disconnected is not run.

### The run API

"Run directly": a developer, a script or a release job asks for a run instead
of pushing for one.

```sh
curl -X POST https://ci.example/ci/v1/runs \
  -H "Authorization: Bearer $DAGRON_API_TOKEN" \
  -H "Idempotency-Key: release-2026-10-04" \
  -d '{"repo": "github.com/acme/widgets", "ref": "refs/heads/main",
       "inputs": {"channel": "beta"}}'
```

| Field | |
|---|---|
| `repo` | `HOST/OWNER/NAME`, a connected repository, in whatever case: `Acme/Widgets` is `acme/widgets` |
| `ref` | a full branch or tag ref: `refs/heads/main`, `refs/tags/v1.2.0` |
| `sha` | optional: the commit to run, which is one that is on `ref`: the ref's own, or one it has behind it. dagron-ci asks the forge (GitHub's API; on another forge it takes the ref's own commit alone), and answers `422` for a commit the ref never had: any commit of a fork is reachable in the repository it forks. Without `sha`, dagron-ci reads where `ref` points now, as the poll sweep reads refs, and pins that commit: for an annotated tag, the commit the tag names |
| `inputs` | optional: up to 64 parameters, names a dagron parameter can have and not starting `ci_`, values up to 4 KiB |
| `pipeline` | optional: the one pipeline to run, a file in `.dagron/ci` (`deploy` or `deploy.yaml`). Without it, every pipeline whose `on:` takes `dispatch` runs |

It answers with the run, as `GET /ci/v1/runs/{id}` does, listing the engine
runs the event submitted:

```json
{"id": "01a1…", "repo": "github.com/acme/widgets", "kind": "dispatch",
 "ref": "refs/heads/main", "sha": "4b47…", "source": "api", "trust": "trusted",
 "actor": "dev@example.com", "state": "done", "attempts": 1,
 "reason": "deploy.yaml: submitted run 7c1e…",
 "engine_runs": [{"id": "7c1e…", "cell": "default", "pipeline": ".dagron/ci/deploy.yaml",
                  "status": "submitted"}]}
```

- **Who may call it.** dagron-ci keeps no users. The caller's dagron-api
  credential, a session or a personal access token, goes to the dagron-api of
  the repository's cell, the one its runs go to, which says who the caller is
  (`GET /api/me`). Its rule is the rule here: a viewer may read runs and start
  none, and anyone else may run. The credential goes nowhere else and is never
  stored. Nothing about a repository is told to a caller dagron-api has not
  identified: an unknown repository answers 401 to them, not 404.
- **What runs.** A dispatch is `trusted`, as a writer's push is. It runs the
  pipeline it names, or every pipeline at the commit whose `on:` takes
  `dispatch`, each with the inputs it declares (one sent an input it does not
  declare is refused); or, naming none at a commit without pipelines, the
  bound workflow, with `ci_event=dispatch` and the inputs. A pipeline named
  that the commit does not hold, or whose `on:` does not take `dispatch`,
  fails the event, and nothing runs in its place. A caller whose dagron-api subject is `agent:<name>` is recorded
  `agent`. Its runs are refused unless `CI_AGENT_RUNS=untrusted`, which runs
  them as `agent` runs, under the floor of an untrusted one, on a cell whose
  dagron-api takes the trigger record; and it reruns and cancels the events
  it asked for, and no other.
- **Once.** A repeat of a request under the same `Idempotency-Key` answers 200
  with the first request's run, even after the branch moved on; the same key
  with another request is a 409. Keys belong to the caller: another caller's
  identical key is theirs. Without a key, every request is a new run, and two
  dispatches of one commit are two runs.

### Running a run again

A check is run again on the same commit, from its failed jobs or whole, and
the commit shows it:

```sh
curl -X POST https://ci.example/ci/v1/runs/$EVENT_ID/rerun \
  -H "Authorization: Bearer $DAGRON_API_TOKEN"
```

It reruns the event's failed and cancelled runs, each from its failed jobs:
what succeeded is kept. With `?failed=false` it reruns each of the event's
runs that is over, one that succeeded too, whole: every job. Either way they
are the same engine runs, so the same checks on the same commit. Where a
build counts runs, a rerun of the failed jobs is no new count, and a rerun of
every job is counted as the run run again (`run_meter`, below).

```json
{"id": "01a1…",
 "rerun": [{"id": "7c1e…", "cell": "default", "pipeline": ".dagron/ci/ci.yaml", "jobs": 2}],
 "left": [{"id": "9d02…", "pipeline": ".dagron/ci/docs.yaml",
           "why": "it succeeded: none of its jobs failed"}]}
```

- **Who may.** The cell's dagron-api is asked for each rerun
  (`POST /api/runs/{id}/rerun`) with the caller's own credential, so its rule
  on who may rerun is the rule here, and its record names the caller. A viewer
  and an agent are refused before it is asked. A caller the cell refuses gets
  the cell's own 401 or 403; refused for a later run of the event, the answer
  says what was rerun, and leaves that run with why.
- **Under the limit.** A rerun passes a build's limit on runs in flight like a
  new run: over it, the run is left, and says it is waiting. So is a run
  whose pipeline's own concurrency key is held by another run: dagron-api
  refuses that rerun (429), as it would refuse a new run of the key.
- **From its failed jobs, or every job.** `?failed=true` is the default.
  `?failed=false` asks the cell's dagron-api for a rerun of every job
  (`{"from": "all"}`): each task of the run is put back, what succeeded
  included, and the run starts again from its first jobs. An approval gate
  asks again, from nothing, and whoever asked for the rerun may not approve
  it where it is `not_triggerer`. A job that resumes from a checkpoint
  starts over: its first attempt resumes nothing, and the `latest` its last
  round left in its checkpoint directory is removed then, so that no retry
  of it resumes from that either. A runtime fan-out is made again from its
  producer's next output, and its last instances' attempts and logs go with
  them. The run's `run_timeout_secs` is counted from the rerun. A cell
  whose dagron-api is older than this answers 400, and that run is left
  with why. A check's "Re-run" and a commit's "Re-run all checks" on GitHub
  are from the failed jobs.
- **One run.** `{id}` may be an engine run's id in place of the event's:
  that run is rerun, and no other of its event.
- `GET /ci/v1/runs/{id}` says how dagron-ci has each of the event's runs
  (`engine_runs[].status`): `submitted` while it is followed, then how it
  ended, and `submitted` again once it runs again.

**The record follows the run.** dagron-ci's record of a run
(`ci_runs.status`) ends when the run does. The engine can move the run after
that: by this door, and by dagron-api's own rerun, retry and clear, which the
console uses. The record re-opens when dagron-ci itself sees the run running
in its cell's dagron-api (`GET /api/runs/{id}`, with `CI_API_TOKEN_FILE`'s
token), and on nothing less: not for a run that reads over, one dagron-api no
longer has, or a dagron-api that could not be asked. It looks in three places:
- this door, which re-opens the record with dagron-api's answer;
- a checkout credential asked for a run on record as over. An engine is
  starting a job of it, so dagron-ci asks the cell before it answers, and
  mints for a run the cell says is running. The request's own word is not
  taken for it;
- every 30 s, each cell's running runs (`GET /api/runs?status=running`),
  for one on record as over: a run moved through dagron-api whose jobs check
  nothing out. It asks for every running run, marked as CI's or not, and
  keeps the ones on its own record. This look runs
  beside the reporter's, so a dagron-api that is slow to list holds no status
  back.

A re-opened record is a run in flight again. Its jobs' checks read `pending`
and then how they end, under the names they had, and a job that did not
succeed is posted again even where it reads as before. Its checkouts are
given their credentials. It holds a place under a build's limit. And its next
end is told to a build's meter again: a run is one run however often its
failed jobs are run again, and the meter takes such an end as a repeat. An
end after a rerun of every job is told with how many there have been
(`whole_reruns`), and a meter counts each such round as a run.
`dagron_ci_runs_reopened_total{by}` counts the records re-opened.

What it does not do:
- A resubmit (`POST /api/runs/{id}/resubmit`, the console's "Re-run
  unchanged") is not a rerun. It makes a new run, which dagron-ci did not
  submit: it has no checks, and its checkout is refused, the repository being
  one dagron-ci serves. To run a check again, rerun it.
- A run moved through dagron-api whose jobs check nothing out, and that is
  over again before dagron-ci looks, is not seen: its checks keep the
  earlier result. The cell's outbox tells the reporter of a rerun, a retry
  or a clear as it is made, which narrows that to a run that is over again
  before its notice is handled; where the cell has no outbox to hand, or it
  does not answer, it is a run over again before the next look over its
  cell. A rerun through this door is followed from the start, and so is one
  with a checkout, from its first job.
- That look reads up to 10,000 running runs a cell, newest first.
- A rerun that is over before dagron-ci's next look at it leaves no `pending`
  on the commit between its two results.

### Acting on a run: stop it, read it, from a terminal

**Stop it.**

```sh
curl -X POST https://ci.example/ci/v1/runs/$EVENT_ID/cancel \
  -H "Authorization: Bearer $DAGRON_API_TOKEN"
```

```json
{"id": "01a1…", "event": "settled",
 "cancelled": [{"id": "7c1e…", "cell": "default", "pipeline": ".dagron/ci/ci.yaml",
                "jobs": 3, "stopping": 1}],
 "left": [{"id": "9d02…", "pipeline": ".dagron/ci/docs.yaml", "why": "it is over: succeeded"}]}
```

- **Each run is asked of its cell** (`POST /api/runs/{id}/cancel`) with the
  caller's own credential, as a rerun is, so who may cancel is dagron-api's
  rule. A viewer and an agent are refused before it is asked. Every run of
  the event is asked about, whatever dagron-ci's record of it says: one on
  record as over may be running again, and a cancel of one that has ended
  changes nothing.
- **A cancel asks.** `jobs` is how many of the run's jobs were called off,
  and `stopping` how many of those a worker still holds. The run reads
  `running` until they have stopped, and then `cancelled`. dagron-ci
  writes nothing of the run itself: its record follows the run as it
  follows any other, and the commit is told when the run has ended.
- **An event that waits is withdrawn** (`"event": "withdrawn"`). One no
  worker holds, because it was just queued, or waits for a slot, a rate
  limit or another try, ends `skipped`, with who cancelled it, and
  submits nothing more.
- **An event a worker is dispatching** (`"event": "dispatching"`) has the
  runs it has submitted cancelled. One it submits a moment later is not:
  ask again. With nothing submitted yet the answer is a 409 with
  `Retry-After`.
- **One run.** With an engine run's id, that run is stopped and the rest of
  its event is left: its other runs, and the event itself.
- A 409 otherwise means nothing was there to cancel, and says why of each
  run.

**Read it.**

```sh
curl "https://ci.example/ci/v1/runs/$EVENT_ID/logs?job=test&status=failed&tail=true&limit=200" \
  -H "Authorization: Bearer $DAGRON_API_TOKEN"
```

dagron-ci keeps no log. Each run's is read from its cell's dagron-api
(`GET /api/runs/{id}/logs`) as the caller, and comes back as that gave it,
under the run: its jobs (there, `tasks`), its `lines`, and `total`,
`matched`, `truncated`, `eof` and `limit`. A viewer may read. A run
whose log could not be read has an `error` in its place.

| Parameter | |
|---|---|
| `job` | a job's name; may be repeated |
| `status` | the jobs that read so: `failed`, mostly |
| `q` | the lines that hold this text |
| `limit`, `tail` | at most this many lines; with `tail=true`, the last of them and not the first |

Anything else is refused (400), and so is what dagron-api refuses, in its
words. A job's steps write one log, so there is no `step`.

**Follow it.** With `follow=true` the door passes one run's log on as its
cell's dagron-api streams it (`GET /api/runs/{id}/logs/stream`, server-sent
events: `docs/API.md`), as the caller, for as long as the run's log lasts:
each job from where it is asked to begin, and then each line as it is
written, once. `from=start` begins at each job's first line (by default,
its last 256 KiB), and `cursor=` (each frame's `cursor`, one a job) goes on
exactly from where a stream that was cut off was. An event of several runs
is followed a run at a time, by each run's id (a 400 names them), and one
that has submitted nothing yet is a 409. The stream ends with an `eof`
event; any other end is a drop, to be followed again from the cursors.

**Which run.** The `{id}` of these doors is an event's: what `POST
/ci/v1/runs` answered with. It may also be an engine run's, the id in the
link of a commit status (`CI_RUN_URL`), which is the one a person looking at
a failed check has. It then means that run of its event and no other.

**Who.** `GET /ci/v1/me` answers who the `default` cell's dagron-api says
presents the credential, as the repositories' door asks it, and what a run
they ask for is recorded as: `{"sub": "u-1", "email": "dev@example.com",
"groups": [], "trust": "trusted"}`. A credential it refuses is a 401.
`dagron-ci login` asks it before it keeps a token.

**From a terminal.**

```sh
dagron-ci login --server https://ci.example    # paste a dagron-api API token; it is not shown
dagron-ci run --remote --repo github.com/acme/widgets --ref refs/heads/main --wait
dagron-ci logs 7c1e… --failed --tail 200
dagron-ci rerun 7c1e…
dagron-ci cancel 7c1e…
dagron-ci logout
```

| Command | |
|---|---|
| `run --remote --repo R --ref REF` | asks for a run (`POST /ci/v1/runs`) and prints its event's id. `--sha`, `--pipeline`, `--input NAME=VALUE` and `--key` (an `Idempotency-Key`) are the request's. `--wait` waits for what it runs, says each change, and prints each run and how it ended; `--timeout SECS` bounds the wait. `--bundle-from DIR` runs a change not pushed: the git working tree at DIR, as a patch bundle (below) |
| `logs ID` | prints each line as `[job] text`. `--job`, `--failed`, `--grep TEXT` and `--tail N` narrow it; `--json` prints the door's answer as it came. `--follow` follows each of the event's runs as it writes (`[pipeline job]` with more than one), new ones as they are submitted, from each job's last 256 KiB or `--from-start`; it goes on from its cursors after a drop, waits as long as a busy server asks, and ends once the event is over and every run's log has ended. With `--json`, each event as it came, a JSON object a line |
| `rerun ID` | runs its failed and cancelled runs again, each from its failed jobs. `--all` runs each of its runs that is over, whole: every job (`?failed=false`) |
| `cancel ID` | stops its runs. An event that is being dispatched is asked about again until it is not, so a run submitted a moment after the first cancel is stopped too |
| `login` | keeps the caller's token for the server, once the server has said whose it is (`GET /ci/v1/me`); a token it refuses is not kept, nor one it answers 200 for without naming a subject, or naming one (or an email) of more than 256 bytes or with a control character in it. The token is read from `--token-file`, `DAGRON_CI_TOKEN_FILE` or `DAGRON_CI_TOKEN`, else from standard input: typed at a terminal, which does not show it until it is read or Ctrl-C ends the command, or piped in |
| `logout` | forgets the token kept for the server (`--server`, `DAGRON_CI_URL`, or the one logged in to last). It works until it is revoked in dagron-api, where it was made: dagron-api revokes a token for a session, not for another token |

The server is `--server`, `DAGRON_CI_URL`, or the one logged in to last:
`https://`, or `http://` to this machine, since the token is sent there. A
redirect from it is not followed, and a server on this machine is asked
directly, whatever proxy the environment names. The token is the run
API's, a dagron-api session or API token, from `--token-file`,
`DAGRON_CI_TOKEN_FILE` or `DAGRON_CI_TOKEN`, in that order, else the one
`login` kept for that server; one kept for another server is never sent. No
flag takes the token itself, which a process list would show. The one to
keep is an API token (`POST /api/tokens`,
[HOWTO §5](../../docs/HOWTO.md#5-api-tokens-for-ci-and-scripts)): a
session's ends with the session.

`login` keeps tokens in `$XDG_CONFIG_HOME/dagron-ci/credentials.json`, else
`~/.config/dagron-ci/credentials.json` (`%APPDATA%\dagron-ci` where there is
no home), a token for each server and the server logged in to last. The
directory is the user's alone (0700) and so is the file (0600), written
whole beside the one it replaces and renamed over it. A file others may
read, or another user's, is refused, as ssh refuses such a key: `chmod 600`
it, or remove it and log in again. A `login` or `logout` reads the file
again under a lock (`credentials.lock`, beside it) before it writes it, so
neither undoes what another finished meanwhile. A `login` takes the lock
once its server has answered, not while the token is typed.

What was done is printed on standard output and the rest is said on
standard error. Each exits 0 when the server did what was asked, 1 when it
refused or could not be reached or, for `run --wait`, when a run did not
succeed, 2 for a usage error, and 124 when `--timeout` ran out.

## Pipelines: `.dagron/ci/*.yaml`

A pipeline is a dagron workflow with two additions, `on:` and `steps:`, and
one changed key, `concurrency:`. `dagron-ci validate` checks a checkout's
pipelines and `dagron-ci compile` shows what one runs for an event, offline,
needing no database; the event workers run them (above).

```yaml
# .dagron/ci/engine.yaml
name: engine-ci
on:
  pull_request: { branches: [main], paths: ["crates/**", "Cargo.*"] }
  push:         { branches: [main], tags: ["v*"] }
  merge_group:  {}
  dispatch:     { inputs: { channel: { default: beta } } }
concurrency: { key: "{{ ci_group }}" }
task_defaults: { timeout_secs: 2700 }
tasks:
  - name: lint
    docker_image: rust:1.86-bookworm
    steps:
      - run: cargo fmt --all -- --check
      - run: cargo clippy --workspace -- -D warnings
  - name: test
    depends_on: [lint]
    docker_image: rust:1.86-bookworm
    steps:
      - name: unit
        run: cargo test --workspace
        timeout_secs: 1800
      - run: scripts/flaky-suite.sh
        continue_on_error: true
```

- **`on:`** names the events that run the pipeline: `push` (branches and
  tags), `pull_request`, `merge_group`, `dispatch` (the run API, with its
  inputs). Each takes optional `branches`, `tags` and `paths` filters, or
  their `_ignore` forms, never both for one. `*` stays inside a path segment,
  `**` crosses them, `?` is one character, `[a-z]` a class; in a list, `!`
  excludes, and the last pattern that matches decides. With neither
  `branches` nor `tags`, a push of either runs it; with one, only that kind.
- **`on: failed`** runs the pipeline when a run of another of the
  repository's fails: `failed: [ci]`, the pipelines by their
  file's name in `.dagron/ci`, or `failed: { pipelines: [ci], branches:
  [main] }`, a push's branch or the branch a pull request merges into. The
  reporter raises a `failed` event as it records the run's end, once for
  each time the run fails (a rerun that fails again raises it again), at the
  failed run's commit and ref, as its actor and trust. Its run has the
  failed run's facts as `CI_FAILED_RUN`, `CI_FAILED_PIPELINE`,
  `CI_FAILED_JOB` (the job that failed first), `CI_FAILED_FAULT_CLASS` and
  `CI_FAILED_DISPOSITION`, where the engine said them, and its jobs' checks
  end `(after_failure)`. **Only a trusted run's failure raises one**: a
  fork's pull request's, an agent's and a patch bundle's raise nothing, so
  what a stranger wrote never sets a trusted pipeline going (GitHub's
  `workflow_run` makes that mistake easy), and the failed run's commit says
  nothing more of it. A failure's own run raises nothing, and a pipeline
  does not name itself.

  ```yaml
  # .dagron/ci/fix.yaml
  name: fix
  on: { failed: { pipelines: [ci], branches: [main] } }
  tasks:
    - name: triage
      steps:
        - run: echo "run $CI_FAILED_RUN failed at $CI_FAILED_JOB ($CI_FAILED_FAULT_CLASS)"
  ```

  `dagron-ci compile FILE --event failed --input ci_failed_pipeline=ci`
  shows what one compiles to. The agent recipe that fixes a failure in CI,
  and ends at an approval of the exact patch, is
  [`examples/ci/agents/fix.yaml`](../../examples/ci/agents/fix.yaml).
- **`steps:`** run in order inside a task, in one container and workspace,
  retried as a unit: `run`, and optionally `name`, `env`, `timeout_secs`,
  `continue_on_error`. The engine sees one task with one command. They
  travel in the task's environment, so a task takes at most 1000 steps,
  carrying at most 512 KiB of scripts and variables (64 KiB a script).
- **`concurrency:`** (`key`, `cancel_superseded`) compiles to dagron's
  `concurrency_key` and `when_full`, which a pipeline does not set itself,
  with `max_active_runs: 1` unless it sets that (at least 1). A newer run of
  the key is made and waits its turn in the engine (`when_full: queue`), so
  its commit is told that it waits; with `cancel_superseded: true` it takes
  the place of the runs before it (`when_full: supersede`, which wants the
  cap at 1). A key is the repository's own, never shared with another
  repository's pipeline.
- **Everything else is dagron's own** (`docker_image`, `image_pull_policy`, `depends_on`, `when`,
  `with_output_of`, approvals, `task_defaults`, `parameters`, `notify`, …) and
  passes through as written. A key that is neither is refused, with the
  nearest known key, since dagron would drop it without a word. A job's
  `identity:` (OIDC tokens for a cloud) is dagron's, checked as dagron
  checks it, and so is its `forge_token: { permissions: { contents: write
  } }`, a token for the repository it checks out, as a workflow's job's
  `GITHUB_TOKEN` (read alone for a run that is not trusted). A job's
  `report:` is the pipeline's own: the
  files its steps write of its tests, read when the steps are over
  ([Check runs](#check-runs)). A job's `services:` is
  dagron's key too: containers the job has beside it while it runs, each
  started before its first step and seen ready, and reached by its name. It
  is checked here as dagron checks it, and what a service runs may name
  what a command may. A job's own
  `environment:` is dagron's key: the environment that job's secrets are
  resolved from, where its policy admits the run. One of dagron's keys is
  checked for its shape: a task's `env:`, and
  `task_defaults.env`, which is a list of names and values
  (`- { name: X, value: "y" }`). The compiler adds a task's steps to that
  list, so one written as a map, the way a step's `env:` is, would be left
  out of the workflow without a word. It is refused, and so is an entry
  with no name.
- **`workspace:`** is dagron's: a directory the task starts in, made for each
  attempt and removed after it, with `dagron-ci-exec` beside it at
  `/dagron/bin` where the engine's operator installs it (see the engine's
  `DAGRON_CI_EXEC_IMAGE`), and the event's commit checked out in it
  ([The checkout](#the-checkout)). A task with `steps:` has one, `{}`, unless
  it sets its own: `{ size: 20Gi }` (a bound on Kubernetes),
  `{ checkout: false }` for one that starts empty,
  `{ checkout: { depth: 0 } }` for all of the repository's history (or
  `depth: N` for that many commits), or `null` for none.
  `cache: [{ prefix, key_files, paths }]` keeps directories between runs
  ([`docs/CI_PIPELINES.md`](../../docs/CI_PIPELINES.md)); the compiler
  gives each `restore_from: "{{ ci_base_ref }}"`, and a pipeline that names
  `restore_from` is refused.
- **A command names nothing an outsider chose.** Event data reaches steps as
  `CI_*` variables. A `{{ … }}` in a `run:`, a `command:` or a `when:` may name
  only `ci_event`, `ci_group`, `ci_sha`, `ci_base_sha`, `ci_pr`, `ci_trust`,
  `ci_repo`, `ci_forge_host`, the pipeline's own `parameters` (dispatch inputs
  excepted), and a matrix's `item` and `index`. A `when:` may name
  `tasks.X.output` as well: dagron reads a task's output there and
  substitutes it nowhere else, so it is refused in a `run:` or a
  `command:`. A branch name goes in a step's `env:`, or is read as
  `$CI_REF`.

`dagron-ci validate [FILE|DIR]…` (`.dagron/ci` by default) prints `ok` per
file, or every problem with where it is (`tasks[1] (test).steps[0].run: …`),
and exits 1 when there is one: fit for a pre-commit hook or a CI step.

### From another CI: `dagron-ci import`

```sh
dagron-ci import .github/workflows/ci.yml > .dagron/ci/ci.yaml    # one file, printed
dagron-ci import --write                                          # each the checkout holds
dagron-ci import Jenkinsfile --name build --image linux=docker.io/library/debian:12 --write
```

The importers of [`dagron-import`](../dagron-import) (GitHub Actions
workflows, GitLab CI pipelines, Jenkins Declarative Pipelines), with each
result held to this crate's own reader, and compiled for a push, a tag, a
pull request, a merge group and a dispatch, before it is called ok. What
each importer carries over, changes and leaves to do by hand is in
[docs/CI_MIGRATING.md](../../docs/CI_MIGRATING.md).

- **What.** Each FILE, or with none what the checkout holds: each
  workflow of `.github/workflows`, the `.gitlab-ci.yml` (or the file
  `.dagron/ci.yaml` names for GitLab) and the `Jenkinsfile`. A file's kind
  is told by where it is and what it is called, and `--from github|gitlab|jenkins`
  says it for one called something else. A pipeline is named for its file
  (a `.gitlab-ci.yml`'s is `gitlab-ci`, a `Jenkinsfile`'s `ci`), unless
  `--name`.
- **Where from.** The checkout is the directory this is run in. A GitLab
  pipeline's includes are read from it and from nowhere else: a path up
  out of it, or a link that leads out, is not read, and the report says it
  is left to do. As GitLab, it reads at most 150 of them, each path as it
  is written, and none over 2 MiB. A file found rather than named is read
  only where it is within it. The images `.dagron/ci.yaml` gives a
  `runs-on` label (`github.runs_on`) or a runner tag (`gitlab.tags`) are
  the importer's, under those `--image LABEL=IMAGE` gives;
  `--default-branch` is GitLab's `$CI_DEFAULT_BRANCH`.
- **What it writes.** Without `--write`, the one file's pipeline under its
  report on standard output, as `dagron-import` prints it, and a line on
  standard error; several files are refused. With it, each to
  `.dagron/ci/<name>.yaml` and a line on standard output, never over a file
  that is there unless `--force`, which writes a file beside it and renames
  it over; and never beside a `<name>.yml`, even with `--force` (`--name`
  names it another way). Two files that would be one
  pipeline are refused before anything is written. A `.dagron` or
  `.dagron/ci` that is a link out of the checkout is not written into, and
  nothing is made in it.
- **The line** says the pipeline's name, its tasks, the events it runs for,
  and how much of its report is to do by hand, was changed on the way and
  was not carried over: `.gitlab-ci.yml: ok, as .dagron/ci/gitlab-ci.yaml
  (gitlab-ci: 2 tasks; runs for push, tag, dispatch; 1 to do by hand, 3
  changed on the way, 0 not carried over)`. A job with something to do is
  a pipeline all the same: its first step fails, and says what.
- **Exit codes.** 0 when each file was imported as a pipeline dagron-ci
  reads and compiles (and written); 1 when an importer refused one (a
  Scripted Jenkinsfile, say), what it made is no such pipeline, or a file
  could not be read or written; 2 for a usage error, or nothing to import.

### A `.github/workflows` file

With `CI_GITHUB_WORKFLOWS=on`, the workflow files a repository lists in
its `.dagron/ci.yaml` run as pipelines beside the native ones
(`src/pipeline/github.rs`). It is level 1: the workflow file itself, with
its triggers, jobs, matrices, containers, services and `run:` steps, and
no action. A file is read into the same IR a native pipeline is, for the
event at hand: what is known when a run is made is evaluated then (a job's
`if:`, its matrix, its image), and everything of a step is left for the
step runner, which evaluates it as the job runs. Expressions are read by
[`dagron-ci-expr`](../dagron-ci-expr), GitHub's grammar as its
documentation gives it. What is taken, what differs and what is refused is
in [docs/CI_GITHUB.md](../../docs/CI_GITHUB.md).

### What a pipeline compiles to

```sh
dagron-ci compile .dagron/ci/engine.yaml --event push --ref refs/heads/main \
  --sha 4b4708e725f29891d22b4c96af9f0a403ff51c7c --repo github.com/acme/widgets
dagron-ci compile .dagron/ci/engine.yaml --event pull_request --ref refs/pull/7/head \
  --base-ref main --pr 7 --changed crates/dagron-ci/src/lib.rs
```

The compiler is a pure function of the pipeline, the event and
`CI_DEFAULT_TIMEOUT_SECS`: the same inputs give the same bytes. It prints the
workflow, headed by the run's parameters; `--json` prints the body
`POST /api/runs` takes. It exits 0 when the pipeline runs, 3 with the reason
when it does not (`branch "dev" is not in on.push.branches`), 1 when the
pipeline or a dispatch's inputs are refused, and 2 when a paths filter needs
`--changed`. A paths filter never guesses.

What the workflow holds:
- the pipeline's own keys, as written; `concurrency.key` as
  `concurrency_key`, with `max_active_runs: 1` unless the pipeline sets it,
  and `when_full`: `queue`, or `supersede` where the pipeline's newer run
  cancels. dagron counts a cap per workflow name and key, and two
  repositories on one cell may both have a pipeline called `ci`, so the key
  is prefixed with the repository (`{{ ci_forge_host }}/{{ ci_repo }}:`),
  and a `max_active_runs` set alone gets the repository as its key;
- `tags:` with `ci` added to the pipeline's own, so that
  `GET /api/workflows?include=ci&tag=ci` and the console's Workflows page list
  the pipelines CI compiled without their authors having to tag them;
- every `ci_*` parameter declared, with the event's values as the run's
  parameters. A dispatch's inputs are taken only when declared, a required
  one must be sent, and defaults fill the rest;
- `CI_EVENT`, `CI_REF`, `CI_SHA`, `CI_BASE_SHA`, `CI_BASE_REF`, `CI_PR`, `CI_REPO`,
  `CI_FORGE_HOST`, `CI_GROUP`, `CI_TRUST` in `task_defaults.env`, filled in
  by dagron from those parameters: event data reaches a task as variables,
  never as text in a command. The workflow is the same for every event that
  runs it; only the parameters differ;
- a timeout on every task: the pipeline's `task_defaults.timeout_secs`, else
  `CI_DEFAULT_TIMEOUT_SECS` (3600);
- for a task with `steps:`, one command, the compiler's alone, that runs
  each step in its own `sh -e`, in order. The steps reach it as variables,
  step protocol 1: `CI_STEP_COUNT`, then each step's script, name, `env:`,
  `timeout_secs` and `continue_on_error` as `CI_STEP_<n>_*`. The command
  hands them to [`dagron-ci-exec`](../dagron-ci-exec/README.md) where the
  image has it (at `/dagron/bin/dagron-ci-exec`, or on `PATH`), and runs
  them itself, a POSIX `sh` script, where it does not. Both end the task with
  a failing step's code unless it sets `continue_on_error`, and keep a step's
  `timeout_secs` (exit 124): dagron-ci-exec everywhere, the script where the
  image has `timeout(1)`. Both split the log with
  `::dagron-ci::step::<n>::start::<name>` and `::dagron-ci::step::<n>::end::<code>`
  lines on stderr, so a task's output is its steps' stdout: a steps task can
  feed `with_output_of`. dagron-ci-exec also passes a cancelled task's
  signal on to the running step, kills what the steps left running when the
  task ends, and logs each step's outcome as a last line,
  `::dagron-ci::result::{…}`. Each step starts in the task's workspace;
- `workspace: {}` on every task with `steps:` that sets none, so the job runs
  in a directory of its own with `dagron-ci-exec` installed beside it; and in
  every workspace that does not say `checkout: false`, a `checkout` of the
  event's commit: `repository: https://{{ ci_forge_host }}/{{ ci_repo }}.git`,
  `commit: "{{ ci_sha }}"`.

### Running them here: `dagron-ci run`

```sh
dagron-ci run                        # every pipeline, for a push of this branch at HEAD, on Docker
dagron-ci run .dagron/ci/engine.yaml --executor local
dagron-ci run --event pull_request --base-ref main --pr 7
dagron-ci run .github/workflows/build.yml --report run.json
```

The run a push would start, on this machine. The event is read from the
checkout, each pipeline compiles as the event workers compile it, and each one
that runs goes to a `dagron` engine started for it alone: a one-shot run on a
SQLite file of its own. The engine is `--engine PATH`, else the `dagron` beside
`dagron-ci`, else the one on `PATH`. A release's `dagron` and `dagron-ci`
archives, unpacked into one directory, are that pair (and on Linux
`dagron-ci-exec`'s beside them); from a checkout, `cargo build --release -p
dagron -p dagron-ci -p dagron-ci-exec` builds all three side by side. The tasks work
in the working tree, so they have no checkout of their own.

- **The pipelines** are the files named, else every file of `.dagron/ci`
  and then each workflow file `.dagron/ci.yaml` lists (`github.workflows`).
  A file of `.github/workflows` is read as the event workers read one, with
  the repository's settings (`runs_on`, `provided`): what it becomes runs
  like any pipeline, and the jobs its `if:` leaves out are skipped. Its
  actions of other repositories are not fetched here, so a step that uses
  one fails unless `provided` lists it.
- **`--report FILE`** writes what came of each pipeline, and of each of its
  jobs, as JSON, when the run is over: `{"event", "ref", "sha", "pipelines":
  [{"file", "pipeline", "result", "jobs": {"<task>": "<status>"}}]}`, with
  `why` for one that did not run.

- **The event** is a push of the branch HEAD is on, at HEAD, for `origin`'s
  repository (`github.com/acme/widgets` from either form of its URL;
  `localhost/local/checkout` without one), trusted. `compile`'s flags change any
  of it: `--event`, `--ref` (needed when HEAD is detached, and for a tag),
  `--sha`, `--base-ref`, `--base-sha`, `--pr`, `--repo`, `--input`.
- **A paths filter** reads the files the working tree changed: everything from
  where it parted from its upstream (or `--base-ref`, or `--base-sha`) to the
  working tree, committed or not, and the files git does not track yet. A
  branch with nothing to compare with lets the pipeline run, and says so;
  `--changed PATH` names the files instead.
- **On Docker** (the default) each task runs in its `docker_image`
  (`alpine:latest` when it names none), pulled first if it is not here, with the
  working tree mounted read-write at the path it has here, as its working
  directory: the tasks share it, and what they leave in it stays. A
  `dagron-ci-exec` beside `dagron-ci` or on `PATH` is mounted read-only at
  `/dagron/bin/dagron-ci-exec`, where the compiled command looks first; a
  static one runs in any image, and one that cannot run in the image is passed
  over for the step script. A container runs as its image's user, so on Linux
  what it writes is usually root's.
- **With `--executor local`** each task is a process in the working tree, with
  this environment, and dagron-ci-exec first on its `PATH`.
- **The engine's own settings stay out.** Every name `dagron config` lists, and
  every `DAGRON_` variable, is removed from the engine's environment, so a
  shell's `API_ADDR`, `DAGRON_CONFIG`, `RUNNER_CLASSES`, `GITHUB_TOKEN` or
  `DAGRON_INSTALLATION` can neither keep a run from ending nor reach past it. On
  the local executor a task inherits what is left.

Each task's log is printed as it comes, its step markers as what they say:

```text
dagron-ci run: push refs/heads/main at 4b4708e725f2 (github.com/acme/widgets), on docker, with /src/widgets mounted
.dagron/ci/engine.yaml: engine-ci
[lint] started
[lint] == step 1: cargo fmt --all -- --check
[lint] == step 1 ok
[lint] == step 2: cargo clippy --workspace -- -D warnings
[lint] == step 2 ok
[lint] succeeded in 41.2s
[test] started
[test] == step 1: unit
…
engine-ci: succeeded in 312.0s
```

It exits 0 when every pipeline that ran succeeded, 3 when none runs for the
event, 1 when one failed, could not run, or the engine refused it (with the
engine's reason), and 2 for a usage error. A run that did not succeed keeps its
directory (the workflow, the engine's database and its log) and says where;
`--keep` keeps one that did. ctrl-c, SIGTERM or SIGHUP stops the engine and
every task it runs, removes the run's containers, and exits 130, 143 or 129; a
second one kills them at once.

**A job that asks for a Docker daemon** (`docker_daemon: true`, or the
repository's `github.docker: true`) is given one only with
`--docker-daemon`, on the Docker executor. Its container is privileged on
this machine's Docker host, so a run of a repository you did not write is
not given one unless you say so: without the flag, the run stops before it
starts and says which job asked. Where this machine's Docker has Sysbox,
`DAGRON_DOCKER_DAEMON_RUNTIME=sysbox-runc` in the environment runs it
unprivileged instead.

Where it stops, for now: pipelines run one after another; a task waiting for an
approval waits until ctrl-c; a sub-workflow's tasks are not printed; and a
working tree whose path holds a `,` or a `:` cannot be mounted (`--executor
local` runs it).

## Extending it

`dagron-ci` is a library as well as a binary. An alternate build composes it
rather than patching it: its binary is `dagron_ci::cli::run` with its own
`dagron_ci::Seams`:
- `metrics`: extra series for `/metrics`;
- `admission`: a limit on runs in flight. Before each run it submits, a worker
  asks it with the number of runs dagron-ci submitted that have not ended, and
  takes a slot (`ci_admissions`) under the run's idempotency key in the same
  transaction. Recording the run lets the slot go, and the reporter seeing the
  run end frees its place. Over the limit the event waits, as behind a full
  concurrency key: no attempt is spent, it asks again every 30 s, and it never
  fails for it. The open build sets none, and writes nothing there. An
  admission may read the runs in flight by repository as well
  (`Admission::admit_run`, where `by_repository` asks for them): each slot
  names its repository, and each recorded run is its repository's. And by
  caller (`Admission::admit_caller`, where `by_caller` asks): each slot names
  its caller, the actor its event names (a forge's user, an agent's
  `agent:<name>`), and each run counts for its event's, so one caller can be
  held to fewer runs than the whole and hold back no one else's.
- `run_meter`: a count of the runs dagron-ci submits
  (`dagron_ci::RunMeter`). It is told each run once dagron-ci has recorded it,
  and each run's end once the reporter has recorded that: its status (or
  `gone`, when dagron-api no longer has it), whether any of its jobs
  started, which a run cancelled before a scheduler claimed any of its jobs
  has not, and how many times it was rerun whole (`whole_reruns`, as the
  cell's `GET /api/runs/{id}/wait` says it as the run ends; 0 from a cell
  that does not say). A rerun of every job runs on the same engine run, and
  `started` is then of that round. An end is told until the meter takes it:
  recording the end marks it due (`ci_runs.meter_due_at`) in the same write,
  and the reporter tells it again a minute later while the meter refuses it,
  so a meter must take a repeat as a no-op, keyed by the run's id and its
  `whole_reruns`. A submit is told once, at best. The
  open build sets none, and marks nothing due.
- `governance`: what a build holds the repositories it serves to
  (`dagron_ci::Governance`), each asked as an event is handled, at once and
  from memory:
  - `required`: pipelines the build requires of a repository, run beside its
    own as `.dagron/required/<name>.yaml`, a path none of the repository's
    files can have; a file of the repository's of the same name is refused,
    and says why on the commit;
  - `review`: its word on each run once it is compiled, before it is
    submitted. A refusal is a refused pipeline: the event fails, and once it
    has ended the commit says why, under the pipeline's name;
  - `actor`: the person a forge login is, which the run's trigger record then
    names in place of the login, so a gate its triggerer may not approve
    (`not_triggerer`) compares people.
  The open build sets none: a repository runs what it writes.
- `audit`: told of each act dagron-ci records (`dagron_ci::Audit`), where it
  is done: a delivery refused for its signature or token, a run submitted, a
  pipeline refused for good, a cancel and a rerun through dagron-ci's doors
  with the caller dagron-api named, a repository connected, changed or
  disconnected through the API (one changed at a terminal is in
  `ci_repo_changes`), and each checkout credential, by its SHA-256. It must
  not wait on a network: a build that sends a trail elsewhere keeps it first.
  The open build tells no one.

## What holds the compiler to its word

**A golden corpus** (`tests/corpus`). Each case is a file that says what
it shows, an event and a source: a pipeline, or a `.github/workflows`
file. Beside it is a golden file with all of what came of it: the
problems of a file that is no pipeline, why an event is refused or passed
over, or the workflow that is submitted, with its parameters. A change to
the compiler shows as a change to those files, to be read before it is
kept:

```sh
DAGRON_CI_BLESS=1 cargo test -p dagron-ci --test corpus
git diff crates/dagron-ci/tests/corpus/golden
```

Every case is compiled twice, for the same bytes, and every workflow that
comes of one is read by dagron's own parser.

**What the importers write** (`tests/import`) is held the same way:
[`dagron-import github`](../../docs/CI_MIGRATING.md#a-workflow-as-a-pipeline),
`gitlab` and `jenkins` are run over a set of workflows, `.gitlab-ci.yml`
files and Jenkinsfiles, and each pipeline they write has to be one this
crate reads, compiles and has an event run. `dagron-ci import` prints each
case's golden file too (`tests/import_cli.rs`), so the two commands never
write a pipeline two ways.

**Fuzz targets** (`fuzz/`), one for each parser that reads something
written outside dagron-ci: a pipeline, a workflow file, an expression, a
forge's deliveries and its git, a job's own lines and reports.
[`fuzz/README.md`](fuzz/README.md) says how to run them, and what to do
when one stops.

## What comes next

In order:
1. pipelines read from forges beyond GitHub, and their checkouts'
   credentials: GitHub Enterprise Server first;
2. each step of a job on its check run;
3. per-repository credentials for polling, kept by dagron-api, for private
   repositories beyond github.com;

## Patch bundles

A run of a change nobody pushed: a coding agent's work in progress, a
developer's before a push.

- **A bundle** is a tar, or one compressed with zstd, of the files a change
  touched since a commit of the repository's (its base), as they are in the
  change, and at its top a `.dagron-deleted` naming the files it deletes,
  one a line. Only files and directories at paths inside the tree: no links,
  no `..`, nothing under `.git`. `POST /ci/v1/bundles` takes one (up to
  32 MiB as the body, up to 256 MiB through an upload link the store signs)
  and keeps it in CI_OBJECT_STORE_URL for seven days, used or not.
- **A run on one** is `POST /ci/v1/runs` with `bundle`: the repository's
  pipelines at the bundle's base (a `sha` named must be it), the bundle's
  own where it changes them (`.dagron/ci`, a workflow file, a GitLab
  include) and none it deletes. Only the caller who uploaded it may run it,
  on its own repository.
- **It is untrusted** whoever asks (an agent's is an agent's): a change no
  one reviewed. It runs on a cell that records what started a run, as an
  agent's does, so the engine confines it: no cache written, no identity,
  an environment's secrets only where its policy names untrusted runs.
- **The forge is told nothing of it**: no commit status, check run or
  comment. Its base is a commit the change is not. The caller follows it
  through `GET /ci/v1/runs/{id}`.
- **A job's checkout** fetches the patch, then the base, and lays the patch
  over it, from a link of an hour that the checkout credential door hands
  the attempt (`POST /ci/v1/checkout-tokens` with `patch`), at the URL jobs
  reach dagron-ci at (`CI_URL_FOR_JOBS`). The link is a bearer's: it is
  https://, or http:// to this machine, unless `CI_URL_FOR_JOBS_INSECURE`
  allows plain http on a closed network, and the engine takes no other
  (`DAGRON_CHECKOUT_PATCH_INSECURE` is its own). The checkout's image needs
  `tar`, and `curl` or `wget` (alpine/git has both). A checkout that is to
  be patched and is given no patch fails: it never runs the base alone.
- **From a working tree**: `dagron-ci run --remote --bundle-from DIR`
  makes the bundle of the git working tree at DIR, uploads it and runs it,
  and dagron-mcp's `dagron_ci_run` does with `bundle_from`. The change is
  everything the tree has that the base has not, `--sha` (any revision git
  names) or its `HEAD`: commits since it, what is staged and what is not,
  and untracked files git does not ignore. Its entries are sorted, with no
  times or owners, so the same change is the same bytes. `--key` is not
  taken with it: each upload is a bundle of its own.
- **Which patch a run ran**: `GET /ci/v1/runs/{id}` says the bundle's id
  and the SHA-256 of what was uploaded (`bundle: {id, sha256}`).
- **Its bounds**: what a dispatch keeps in memory of a bundle (its
  pipelines, workflows and `.dagron-deleted`) is 16 MiB at most, and a zstd
  bundle expands to 1 GiB at most; the content a checkout fetches is
  decompressed as it is sent. An hourly pass deletes the bundles past their
  seven days.
- **It needs** CI_OBJECT_STORE_URL, CI_URL_FOR_JOBS and the credential
  door's secret (CI_CHECKOUT_TOKEN_SECRET_FILE or _SECRETS_DIR), which signs
  the links; and an engine that asks the door (`DAGRON_CHECKOUT_TOKEN_URL`).
  Without them `POST /ci/v1/bundles` answers 503.

Pull requests from forks and from non-members are refused unless the
operator has them run (`CI_FORK_PRS=untrusted`). A refused one's head says so
with an `error` status: the Statuses API has no neutral state, and a neutral
result waits for check runs.

The receiver's shape (an HMAC check over the raw body, a delivery id as the
replay key) follows AGAS's GitHub receiver (MIT); see [`NOTICE`](../../NOTICE).

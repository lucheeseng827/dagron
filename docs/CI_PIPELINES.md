# Pipelines: the reference

A pipeline is a YAML file in `.dagron/ci/` of a repository. dagron-ci reads
the files at the commit an event is for, and runs each one whose `on:` takes
the event, as a dagron run of its own.

A pipeline is a dagron workflow with two additions, `on:` and `steps:`, and
one changed key, `concurrency:`. Everything else in it is dagron's own, and
means what it means in any workflow ([`HOWTO.md`](HOWTO.md)).

New to it? [`CI_QUICKSTART.md`](CI_QUICKSTART.md) gets a first check green,
and [`examples/ci`](../examples/ci) has six pipelines to start from.

## A pipeline

```yaml
# .dagron/ci/ci.yaml
name: ci
on:
  push: { branches: [main] }
  pull_request: { paths: ["src/**", "Cargo.*"] }
  dispatch: { inputs: { suite: { default: quick } } }
task_defaults:
  timeout_secs: 1800
tasks:
  - name: lint
    docker_image: docker.io/library/rust:1-bookworm
    steps:
      - run: rustup component add rustfmt clippy
      - run: cargo fmt --all -- --check
      - run: cargo clippy --workspace -- -D warnings
  - name: test
    depends_on: [lint]
    docker_image: docker.io/library/rust:1-bookworm
    steps:
      - name: unit
        run: cargo test --workspace
        timeout_secs: 1200
      - name: slow
        env: { SUITE: "{{ suite }}" }
        run: scripts/suite.sh "$SUITE"
        continue_on_error: true
```

| Key | | |
|---|---|---|
| `name` | required | The pipeline's name: letters, digits, `.`, `_` or `-`, at most 100 |
| `on` | required | The events that run it. One left out never does |
| `tasks` | required | Its jobs |
| `concurrency` | | Runs of one key wait for one another |
| anything else | | dagron's own, as written: `task_defaults`, `parameters`, `environment`, `notify`, `tags`, `max_active_runs`, `run_timeout_secs`, … |

A key that is neither dagron-ci's nor dagron's is refused, with the nearest
key that exists: dagron would drop it without a word.

## `on:`: which events run it

```yaml
on:
  push:         { branches: [main, "release/*"], paths_ignore: ["docs/**"] }
  pull_request: { branches: [main] }
  merge_group:  {}
  dispatch:     { inputs: { channel: { default: beta }, version: { required: true } } }
```

| Event | It is | Filters |
|---|---|---|
| `push` | a push to a branch or a tag | `branches`, `tags`, `paths` |
| `pull_request` | a pull request opened, updated or reopened | `branches` (the branch it merges into), `paths` |
| `merge_group` | a merge queue's group asking for its checks. A group the queue lets go of without merging it has its runs cancelled | `branches` (the branch the queue merges into) |
| `dispatch` | someone asking for a run: `POST /ci/v1/runs`, or `dagron-ci run --remote` | none; `inputs` |

- **A filter is a pattern or a list of them.** `*` stays inside a path
  segment, `**` crosses them, `?` is one character, `[a-z]` is a class. In a
  list, a pattern that starts with `!` excludes, and the last pattern that
  matches decides.
- **Each filter has an `_ignore` form** (`branches_ignore`, `tags_ignore`,
  `paths_ignore`), and takes one or the other, never both.
- **A push with neither `branches` nor `tags` runs for both kinds.** With one
  of them, it runs for that kind alone. `paths` is not read for a tag.
- **`paths` asks which files the event changed.** The forge's compare says,
  and where a change is larger than it lists, a shallow fetch of the two
  commits does. Where neither can (a new branch has nothing to compare with,
  the forge cannot compare the two commits), the pipeline runs: skipping it
  could skip a check a merge waits for.
- **A pipeline its `paths` skip says so on the commit.** Each of its jobs
  gets a status that passes, reading "skipped: no changed file matches …",
  so a branch that requires the check is not left waiting. A pipeline whose
  `on:` does not take the event at all says nothing.
- **A dispatch's `inputs`** are the pipeline's to declare: each a name with
  an optional `default`, `required` and `description`. An input the pipeline
  does not declare is refused, and a required one must be sent. They reach a
  step as `{{ name }}`, which belongs in the step's `env:` (below).
- **A pull request runs when it comes from one of the repository's own
  branches, and its author is the repository's owner, a member of its
  organization or a collaborator.** One from a fork, or by anyone else,
  runs nothing by default: its head commit gets one status that says so
  ([`CI.md`](CI.md), "What it refuses"). An operator may have such pull
  requests run, as untrusted runs: "Pull requests from forks", below.

## `tasks:`: the jobs

A task is a job: one container, one workspace, retried as a unit.

```yaml
tasks:
  - name: test
    docker_image: docker.io/library/node:22-bookworm-slim
    depends_on: [build]
    env:
      - { name: CI, value: "true" }
    workspace: { size: 20Gi }
    timeout_secs: 900
    steps:
      - run: npm ci
      - run: npm test
```

| Key | |
|---|---|
| `name` | required: letters, digits, `.`, `_` or `-`, at most 100. It is the job's name on the commit |
| `steps` | commands run in order inside the job (below) |
| `docker_image` | the image the job runs in. Without one, the engine's default |
| `depends_on` | the jobs that must succeed first |
| `env` | **a list of names and values**, dagron's own: `- { name: X, value: "y" }`. A step's `env:` is a map; a job's is not, and one written as a map is refused |
| `workspace` | the directory the job starts in (below) |
| `services` | containers the job has beside it while it runs (below) |
| `docker_daemon` | `true`: a Docker daemon of the job's own beside it (below) |
| `report` | the files the job's steps write of its tests, read when the steps are over (below) |
| `timeout_secs` | how long the job may run. Without one: `task_defaults.timeout_secs`, else an hour |
| anything else | dagron's own task keys, as written: `when`, `with_items`, `with_output_of`, `env_from`, `resources`, `isolation`, `max_attempts`, `approvers`, `command`, … |

- A job has `steps`, or dagron's own `command`, or neither (an approval, a
  call to another workflow, a wait), never both.
- **dagron's keys are checked by dagron**, when the run is submitted, and
  not by `dagron-ci validate`: a value dagron refuses fails the event, and
  the commit says the pipeline was not run and why. `dagron-ci run` finds
  such a thing on your own machine, since it runs the pipeline on a real
  engine.
- `identity: { oidc: { audience: … } }` gives a job OpenID Connect tokens
  that a cloud's federation trades for credentials, so that the job holds no
  secret of the cloud's. The job is told where and how to ask for one
  (`DAGRON_OIDC_REQUEST_URL`, `DAGRON_OIDC_REQUEST_TOKEN`, and its first
  audience in `DAGRON_OIDC_AUDIENCE`). A token lasts ten minutes, so ask for
  it where it is used:

  ```yaml
  - name: publish
    environment: production
    identity: { oidc: { audience: sts.amazonaws.com } }
    steps:
      - run: |
          curl -fsS -H "Authorization: Bearer $DAGRON_OIDC_REQUEST_TOKEN" \
            "$DAGRON_OIDC_REQUEST_URL?audience=sts.amazonaws.com" \
            | jq -r .value > /tmp/web-identity
          export AWS_ROLE_ARN=arn:aws:iam::123456789012:role/deploy
          export AWS_WEB_IDENTITY_TOKEN_FILE=/tmp/web-identity
          aws s3 cp --recursive dist/ s3://releases/
  ```

  - One audience, or a list of up to 8; a request that names none gets the
    first. An audience the job was not given is refused.
  - Only a trusted run's job is given an identity: a run that is not trusted
    has the job refused, with the reason. A job whose `environment:` its run
    may not use is refused one too, since the token names the environment.
  - What the token says, and how a cloud's trust policy reads it:
    [`CI_SECURITY.md`](CI_SECURITY.md#a-jobs-identity). Where the operator
    has given dagron-api no issuer, such a job is refused and says so.
- `artifact_api: true` gives a job its run's artifacts over dagron-api's
  API, from its own container: `DAGRON_ARTIFACTS_API` and a credential of
  the attempt's own, `DAGRON_ARTIFACTS_TOKEN`, with which, while it runs, it
  reads any artifact of its run (`GET $DAGRON_ARTIFACTS_API/<job>/<name>`)
  and writes its own (`PUT`, under its own job's name alone). What an
  approval's `binds` reads is there. The engine's operator offers it
  (`DAGRON_ARTIFACT_API_URL`); where not, such a job is refused and says so.

  ```yaml
  - name: plan
    artifact_api: true
    steps:
      - run: |
          terraform plan -out plan.tfplan
          printf 'Authorization: Bearer %s\n' "$DAGRON_ARTIFACTS_TOKEN" |
            curl -fsS -H @- -T plan.tfplan "$DAGRON_ARTIFACTS_API/plan/plan.tfplan"
  ```
- `forge_token: { permissions: { contents: write, pull_requests: write } }`
  gives a job a token for the repository it checks out, as GitHub gives a
  workflow's job its `GITHUB_TOKEN`, and has it as `GITHUB_TOKEN`, masked in
  its log. The permissions are a GitHub App's names (`actions`, `checks`,
  `contents`, `deployments`, `discussions`, `issues`, `metadata`, `pages`,
  `pull_requests`, `security_events`, `statuses`), each `read` or `write`.
  The engine asks the token service for it as each attempt starts and has
  it revoked after; the service gives a run that is not trusted read alone.
  A job that checks out nothing is refused one.
- **A job that its own `when:` skips does not hold back the jobs after
  it.** A job that depends on it runs, as it would after a job that
  succeeded: one that must not run then is given the same `when:`. A job
  that comes after a job that *failed* is skipped.
- **A job its `when:` leaves out of the event has a passing status.** A
  `when:` that names only the event and the pipeline's parameters
  (`"{{ ci_event }} == tag"`) is decided as the run is made, and dagron makes
  no task of a job it leaves out. That job's status says so all the same:
  `success`, "skipped: its when: leaves it out of this event", as GitHub
  shows a job its `if:` leaves out, so a branch that requires its check is
  not left waiting on it. A run that leaves out every job ends at once, as
  one that succeeded.
- **A later job reads an earlier one's status, and not what it printed.**
  `env_from: [{ name: X, task: build, read: status }]` gives a job how
  `build` ended. `read: last_line` is for a task with a `command`: of a
  job with `steps` the last line is the step runner's own, so a value is
  not handed from one such job to the next yet.

### `steps:`

```yaml
steps:
  - name: unit                  # optional: how the step is named in the log
    run: cargo test --workspace # required: a shell script
    env: { RUST_BACKTRACE: "1" }  # optional: a map
    timeout_secs: 1200          # optional: the step is stopped past it, and fails
    continue_on_error: true     # optional: a failure does not end the job
```

- **Each step is a shell of its own** (`sh -e`), started in the workspace.
  A variable set in one step is not set in the next, and neither is a
  directory changed into. What a step leaves in the workspace is there for
  the next.
- **A step that fails ends its job**, with the step's exit code, unless it
  says `continue_on_error`. A step past its `timeout_secs` exits 124.
- **The job's output is its steps' output.** Each step's start and end are
  marked in the job's log, on standard error.
- A job takes at most 1000 steps, 64 KiB a script, and 512 KiB of scripts
  and variables in all: they travel to the job in its environment.

### `workspace:`

A job with `steps` starts in a workspace: a directory made for that attempt
and removed after it, with the event's commit checked out in it.

| | |
|---|---|
| left out | a workspace, with the commit checked out |
| `{ size: 20Gi }` | the same, with room for that much. A bound on Kubernetes |
| `{ checkout: false }` | a workspace that starts empty |
| `{ checkout: { depth: 0 } }` | the commit with all of the repository's history, every branch's and the tags; `depth: 30` for that many commits of its own |
| `null` | no workspace: the job starts where its image does |

- The checkout is that one commit and no history, unless the job says how
  deep: fetched by its id, at depth 1, without tags, and checked out
  detached. A job that asks git what changed (`git merge-base`,
  `git describe`) says `checkout: { depth: 0 }`.
- A private repository's checkout is given a credential for that attempt
  alone, which the job's steps never hold.
- `cache:` keeps directories between runs, where the engine's operator has
  given it a cache store (`DAGRON_CACHE_URL`,
  [`CI_RUNNERS.md`](CI_RUNNERS.md#caches)). Elsewhere a job starts cold, and
  says why in its log:

  ```yaml
  workspace:
    cache:
      - prefix: cargo
        key_files: [Cargo.lock]
        paths: [/usr/local/cargo/registry, /usr/local/cargo/git]
  ```

  - **The key** is the `prefix` and a hash of the files `key_files` names in
    the checkout, as globs (`**/package-lock.json`), read before the first
    step. With no `key_files` the key is the prefix alone.
  - **Restored** before the first step: the entry saved under that key, else
    the newest of the prefix, of the run's own ref and then of the branch a
    pull request or merge group merges into (`CI_BASE_REF`). Where a job
    restores from is dagron-ci's to say: a pipeline that names
    `restore_from` is refused.
  - **Saved** after every step has succeeded, unless the job restored that
    very key, and by a trusted run only. A run that is not trusted (a
    fork's pull request) restores, and never writes what a later run reads.
  - `paths`: 1 to 16 directories, absolute, `~/…` in the job's home, or
    relative to the workspace. `prefix`: letters, digits, `.`, `_` and `-`.
    At most 8 caches a job. Two caches of one prefix that keep other paths
    read none of each other's entries.
  - A cache never fails a job: one that is not restored or saved says why
    in the job's log (`cache cargo: …`), and the job runs on.

### `services:`

A job that needs a database, a cache or a queue beside it names each as a
service: a container of its own, started before the job's first step and
seen ready, and removed with the job.

```yaml
tasks:
  - name: test
    docker_image: rust:1.86-bookworm
    services:
      - name: db
        image: postgres:16-alpine
        env:
          - { name: POSTGRES_PASSWORD, value: pw }
        ready: pg_isready -h localhost -U postgres
    steps:
      - env: { DATABASE_URL: "postgres://postgres:pw@db:5432/postgres" }
        run: cargo test
```

- The job reaches a service at its `name`, on the port its image listens
  on (`db:5432`), and at `localhost`: the job and its services share one
  network.
- `ready` is a command run in the service's own container until it exits 0.
  The first step starts once every service is ready. A service that is not
  ready in `ready_timeout_secs` (60 unless set), or that stops first, fails
  the job with what the service said, and no step runs.
- A service's `env:` is a list of names and plain values. No secret is
  given to a service, and it is given none of the job's `CI_*` variables.
- `services` is dagron's own key, and `dagron-ci validate` checks it as
  dagron does. A `{{ … }}` in a service's `image`, `args` or `ready` may name
  what a command may, and nothing an outsider chose.
- The engine's executor has to be Docker or Kubernetes: one that runs a job
  as its own process fails a job with services, with the reason.
  `dagron-ci run` runs them where it runs jobs in containers.

Every key a service takes, and what differs between the two executors, is
in [`HOWTO.md`](HOWTO.md), "A container beside a task".

### `docker_daemon:`

A job that runs `docker build` or `docker run` names `docker_daemon: true`
and is given a daemon of its own: a service named `docker`, which the job
reaches as `DOCKER_HOST`, with the job's workspace at its own path.

```yaml
tasks:
  - name: image
    docker_image: docker.io/library/docker:27-cli
    docker_daemon: true
    steps:
      - run: docker build -t app:test .
```

- Its container is privileged (unless the engine runs it on Sysbox), so the
  engine's operator offers it (`DAGRON_DOCKER_DAEMON=dind`); on an engine
  that does not, the job fails
  with the reason. A run that is not trusted may not ask for one.
- On the Docker executor only. `dagron-ci run` gives one only with
  `--docker-daemon`: without it, a run with such a job stops before it
  starts and says so.
- `dagron-ci validate` checks it as dagron does: `true` or `false`, on a
  job that runs steps or a command, with no service named `docker` and at
  most seven others.
- What else holds of it is in [`HOWTO.md`](HOWTO.md), "A Docker daemon of
  the task's own".

### `report:`

```yaml
tasks:
  - name: test
    steps:
      - run: make test lint
    report:
      junit: target/junit.xml
      sarif: [lint.sarif, audit.sarif]
```

The files a job's steps write of its tests: **JUnit XML** (`junit`) and
**SARIF** (`sarif`), each a path in the workspace or a list of up to 8.
They are read when the steps are over, whether the steps succeeded or
not, and what they hold is said with the job.

- **On its check run**, where the repository's jobs are check runs
  ([how](../crates/dagron-ci/README.md#check-runs)): each test that
  failed, and each SARIF result, that names a file of the repository is
  an annotation on that line of the commit, up to 50. The check's page
  has the counts, and each failure by name with the first line of what
  it said.
- **In the pull request's comment**, where one is kept
  ([how](../crates/dagron-ci/README.md#a-pull-requests-comment)): how
  many of the job's tests ran and failed, and the first five that failed
  by name.
- **In its log** either way, as the lines that begin
  `::dagron-ci::report::`, `::dagron-ci::annotation::` and
  `::dagron-ci::failed::`. `dagron-ci run` prints them as the counts,
  each place with what was found there, and each failure by name.
- A JUnit test names its place with `file` and `line` on its
  `<testcase>`; a SARIF result with its first location. One that names
  no place, or a place outside the repository, is listed by name and
  annotates nothing. pytest writes them in the older of its two formats,
  and counts a line from 0, which is read as one more in a suite named
  `pytest`:
  `pytest --junitxml=junit.xml -o junit_family=xunit1 -o junit_suite_name=pytest`.
- A path is letters, digits, `.`, `_`, `-` and `/`, with no `..` and no
  leading `/`. A file that is not there, or is not what it is called, is
  said in the report and fails nothing: the job ends as its steps did.
- A report is the job's own word. It changes nothing about how the job
  ended, which is the engine's to say.
- It is read by dagron-ci-exec, which runs the steps where the image has
  it or the engine's operator provides it
  ([its README](../crates/dagron-ci-exec/README.md)). Where a shell runs
  the steps in its place, no report is read. A task with no `steps` has
  no `report`.

**A step's summary.** Each step is given `CI_STEP_SUMMARY`, the name of a
file, one for the job, which it may add Markdown to:
`echo "## Coverage: 91%" >> "$CI_STEP_SUMMARY"`. What the steps wrote is
shown on the job's check run, under the check's own summary, up to
24 KiB. It needs dagron-ci-exec as a report does.

## `concurrency:`

```yaml
concurrency: { key: "{{ ci_group }}" }
```

Runs of one key take turns: a newer run of the key starts when the one
running has ended. `{{ ci_group }}` is the branch, or the pull request, so
this is "one run a branch at a time". The key is the repository's own,
never shared with another repository's pipeline of the same name.

A run that waits is made at once, and waits in the engine: its jobs show
on the commit as queued, its run's page says it is queued, and it is
cancelled as any run is. Runs are let in in the order they came. A hundred
may wait for one key; an event past that waits in dagron-ci, and asks
again.

```yaml
concurrency: { key: "{{ ci_group }}", cancel_superseded: true }
```

With `cancel_superseded: true` a newer run **takes the place** of the ones
before it: a push to a pull request cancels the run of the push before.
They are cancelled as a person's cancel would, and their checks say so. A
job of theirs that is running is stopped, with its grace, and the newer
run starts when it has stopped; a run nothing was running for is over at
once. It is for a pipeline whose older runs nobody waits for. A pipeline
that deploys wants the other: every run in its turn.

It takes the place of one run, so `max_active_runs` is 1 beside it, or
left out. And a run that is not trusted, a pull request from a fork, takes
the place of the runs of its own pull request and of no other, whatever
key its pipeline writes: the key is its author's, and one that named
another pull request's would otherwise cancel that one's run.

`max_active_runs` set with no `concurrency:` caps the pipeline's runs for
the whole repository, and a run over it waits its turn the same way.

## What a step knows of its event

Event data reaches a step as variables, never as text in its script:

| Variable | |
|---|---|
| `CI_EVENT` | `push`, `tag` (a push of a tag), `pull_request`, `merge_group` or `dispatch` |
| `CI_REF` | the ref: `refs/heads/main`, `refs/pull/7/head`, `refs/tags/v1.2.0` |
| `CI_SHA` | the commit |
| `CI_BASE_SHA` | the commit it is compared with, where there is one |
| `CI_BASE_REF` | the branch a pull request or merge group merges into, as a ref (`refs/heads/main`); empty for any other event |
| `CI_PR` | the pull request's number, for one |
| `CI_REPO`, `CI_FORGE_HOST` | `owner/name`, and `github.com` |
| `CI_GROUP` | the branch or the pull request: what `concurrency` is keyed by |
| `CI_TRUST` | `trusted`, or `untrusted` for a pull request from a fork or a non-member, where such are run ("Pull requests from forks") |

**A script names nothing an outsider chose.** A `{{ … }}` in a `run:`, a
`command:` or a `when:` may name only `ci_event`, `ci_group`, `ci_sha`,
`ci_base_sha`, `ci_pr`, `ci_trust`, `ci_repo`, `ci_forge_host`, the
pipeline's own `parameters`, and a matrix's `item` and `index`. A
`when:` may name `tasks.X.output` as well: dagron reads a task's output
there, and substitutes it nowhere else, so `validate` refuses it in a
`run:` or a `command:`. A branch name is `$CI_REF`. A dispatch's input
goes through a step's `env:`, and is read as a variable:

```yaml
- env: { CHANNEL: "{{ channel }}" }
  run: scripts/publish.sh "$CHANNEL"
```

A variable whose name starts with `CI_` is dagron-ci's to set, and is
refused in an `env:`.

## Secrets

Secrets are dagron's own, as in any workflow ([`HOWTO.md`](HOWTO.md),
"Secrets & environment variables"). An environment holds them, encrypted; a
pipeline names the environment; and a job names each secret it is to be
given, in its `env:`:

```yaml
name: release
on: { push: { tags: ["v*"] } }
environment: release
tasks:
  - name: publish
    docker_image: docker.io/library/node:22-bookworm-slim
    env:
      - name: NPM_TOKEN
        value_from: { secret: npm_token }
    steps:
      - run: npm publish
```

The secret is decrypted when the job is dispatched and reaches that job as
a variable. It is not in the pipeline, the run or the engine's log, and what
the job prints of it is masked in its stored output. The job itself holds
it, and so does anything the job runs.

**A job may name an environment of its own.** Its secrets are then that
one's, and the pipeline's other jobs are given none of them:

```yaml
tasks:
  - name: test
    steps: [{ run: npm test }]
  - name: publish
    depends_on: [test]
    environment: release
    env:
      - name: NPM_TOKEN
        value_from: { secret: npm_token }
    steps: [{ run: npm publish }]
```

**Which runs may use an environment is its policy's to say**, and an admin
writes it ([`HOWTO.md`](HOWTO.md), "Who may use an environment"): by trust
class, by ref and by pipeline. A pipeline is read at the commit it runs for,
so a pull request's pipeline is the pull request's own, and may name any
environment. What it is given is another matter. An environment whose
policy names `refs/heads/main` and `refs/tags/v*` is read by a push to main
and by a release tag, and by no pull request, whoever opened it. Such a
run is not made: the commit gets one status under the pipeline's name that
says which environment refused it, and why.

## Pull requests from forks

A pull request from a fork, or from someone who is not a member of the
repository, is **not run** unless the operator says so: its head commit is
given one status that says it was not, and why. With
`CI_FORK_PRS=untrusted` it is run, as an **untrusted** run.

Its pipelines are read at its head, as every pull request's are: they are
its author's to write. What holds such a run is not what its pipeline says.
dagron goes by the run's record ([`HOWTO.md`](HOWTO.md), "Runs that are not
trusted"), which dagron-ci writes and the pipeline cannot:

- **It reads no secret** but an environment's whose policy an admin opened
  to `untrusted` runs, and none of the engine's own.
- **Its jobs run under the engine's floor for such runs**, on an engine
  that has one, and on no other. On Docker they are given none of the
  operator's mounts, and are held to limits.
- **The engine does nothing else for it.** A job with a `service_account`,
  a `defer`, a `produces`, or a `type:` other than an ordinary task's is not
  run: the run is refused, and the commit gets one status under the
  pipeline's name that says which key and why. So is one with a `repeat`
  or a `with_output_of`. A job's `cache` and `priority` are written for
  nothing: it is given neither, and runs. Its `pool` is not the one it
  named: every such job draws from the pool `untrusted`.
- **What it may spend is the operator's to say.** A job's `timeout_secs`
  and `max_attempts` stand where they are less than the operator allows,
  a run has only so many jobs, its jobs wait for one another in the pool
  `untrusted`, which the operator caps (`POOLS=untrusted:4`), and its
  `concurrency:` key is one among such runs: it holds
  no place in line that a run for main wanted. Where the operator lists the
  images such a run may name, a job in another is not run, nor one beside a
  service in another. A job has only so many services, each held to what
  the job's own container is.
- **It is never run without that record.** On a cell whose dagron-api takes
  no record from dagron-ci's token (the `ci:trigger` group), the pull
  request is not run, and its head says so.

**A first contribution waits for an approval** (`CI_FORK_APPROVAL`, default
`first-time`). No pipeline of the pull request is submitted until a person
named in `CI_FORK_APPROVERS` approves it, in the console or through the API,
as any approval is given. What is asked of them is a run of its own, which
carries the pull request's number and its commit, and nothing its author
wrote. Its head shows `dagron-ci/approval (pull_request)`:
pending while it waits, then how it ended. A refused approval, or one
nobody answered in a week (`CI_FORK_APPROVAL_TIMEOUT_SECS`), leaves nothing
run. Each new commit pushed to the pull request is asked for again, and an
approval asked for a commit is not withdrawn when the pull request is
closed or pushed to: it waits until someone answers it, or its time is up.
An author with a commit in the repository waits for none, unless the
setting is `always`.

A pipeline that both a push and a fork's pull request run is one to read
with this in mind. Keep what needs a secret, a service account or an
approval in a pipeline that takes no `pull_request`, or guard it:

```yaml
on:
  push: { branches: [main] }      # the deploy runs for main alone
```

## Checking and running a pipeline before it is pushed

```sh
dagron-ci validate                 # every file in .dagron/ci
dagron-ci compile .dagron/ci/ci.yaml --event push --ref refs/heads/main
dagron-ci run                      # run them here, for a push of this branch
```

- **`validate`** prints `ok` for a file, or every problem in it with where
  it is (`tasks[1] (test).steps[0].run: …`), and exits 1 when there is one.
  It needs no server and no database: fit for a pre-commit hook.
- **`compile`** prints the dagron workflow a pipeline is for an event, and
  says why when it would not run.
- **`run`** runs a checkout's pipelines on an engine it starts for the
  purpose, on Docker or as plain processes, in the working tree as it
  stands. It needs the `dagron` engine beside `dagron-ci`, or on `PATH`.

The [JSON Schema](../crates/dagron-ci/schema/pipeline.v1.json) is what
`dagron-ci schema` prints: point an editor at it for completion.

## Where the rest is

[`crates/dagron-ci/README.md`](../crates/dagron-ci/README.md) is the whole of
dagron-ci: what a pipeline compiles to, how events are taken, the checkout
and its credential, the commit statuses, the run API, and each setting.

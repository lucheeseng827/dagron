# GitHub Actions workflows on dagron-ci

dagron-ci can run a repository's `.github/workflows` files as pipelines.
What it takes of them is **level 1**: the workflow file itself. Its
triggers, its jobs, their matrices, containers and services, and their
`run:` steps.

An action (`uses:`) is level 2, and what an action leans on (the cache,
artifacts, a token) is level 3. Of level 2, composite and JavaScript
actions are built: one in the repository, `uses: ./dir`
([below](#a-local-action)), and one of another repository,
`uses: owner/repo@ref`, fetched before the job starts
([below](#an-action-of-another-repository)); a JavaScript one runs on a
Node.js the engine installs ([below](#a-javascript-action)); a Docker one,
and a step that uses an image, in a container the engine starts beside the
job ([below](#a-docker-action)). A workflow that needs what is not built is
refused, and the refusal says what it needs: nothing in a file is passed
over without a word.

This page is what is taken, what differs from GitHub, and what is not
taken. The native format is in [`CI_PIPELINES.md`](CI_PIPELINES.md).

## Turning it on

Three settings: the operator's, the repository's mode, and the repository's
own.

**The operator** sets `CI_GITHUB_WORKFLOWS=on` on dagron-ci. Off, which is
the default, no repository is asked for anything of this.

**The repository is in compat mode** (`dagron-ci repo add|set … --mode
compat`, or `"mode": "compat"` on `PUT /ci/v1/repos/…`). One in native
mode, the default for a repository connected now, is run as its
`.dagron/ci` says, and its `.dagron/ci.yaml` is not read; one connected
before there were modes is in compat mode already.

**The repository** lists the workflow files that run here, in
`.dagron/ci.yaml`:

```yaml
github:
  workflows: [ci.yml, lint.yml]
  runs_on:
    ubuntu-latest: docker.io/library/ubuntu:24.04
    ubuntu-24.04: docker.io/library/ubuntu:24.04
  provided: [dtolnay/rust-toolchain]
```

| Key | |
|---|---|
| `workflows` | the files of `.github/workflows` that run here, by name. None unless listed: a repository that has workflows has GitHub running them already, and listing one here does not stop it there |
| `runs_on` | the image a `runs-on` label means. A job whose label has none, and that names no `container`, is refused. The image is yours to choose: there is no copy of GitHub's hosted runners' tools |
| `provided` | actions whose work those images already hold, such as a toolchain's installer. A step that uses one is left out |
| `oidc_audiences` | the audiences a job's OIDC token may be asked for, beside the forge's own ([below](#an-oidc-token)): `sts.amazonaws.com` for AWS, `api://AzureADTokenExchange` for Azure. At most seven |
| `docker` | `true` gives each job of a trusted run a Docker daemon of its own, as a hosted runner has one: `docker build` and `docker run` work, with the job's workspace at its own path. Only where dagron-ci's operator lists the repository (`CI_GITHUB_DOCKER`) and the engine offers one, and each job fails, with the reason, on an engine that offers none ([CI_RUNNERS.md](CI_RUNNERS.md#a-jobs-docker-daemon)). A job with a service named `docker` keeps its own. A run that is not trusted is given none |

## What a workflow becomes

- **A file is a pipeline**, called by its file's name without its ending:
  `ci.yml` is `ci`. A commit's checks are named by it:
  `dagron-ci/ci/test (push)`. A workflow file and a pipeline of
  `.dagron/ci` may not share a name.
- **A job is a job.** A matrix is a job for each combination, named for
  its values: `test.linux-16`.
- **A job whose `if:` is false is left out**, with the jobs that needed it,
  and each gets a passing status that says it was skipped.

| `on:` | |
|---|---|
| `push` (`branches`, `tags`, `paths`, and each `-ignore`) | taken |
| `pull_request` (`branches`, `paths`, and each `-ignore`) | taken, for a pull request opened, pushed to or reopened. Another `types:` is refused |
| `merge_group` | taken |
| `workflow_dispatch` (`inputs`) | taken, through dagron-ci's run API. Every input is text, and one the request leaves out is its default |
| `schedule` (`- cron: '…'`) | taken: each cron fires at the head of the default branch ([below](#a-schedule)) |
| anything else (`release`, `workflow_call`, …) | no event is made for it here: the workflow never runs for it, and `validate` says so |

## A schedule

A workflow file's `on.schedule` runs it as GitHub runs it: each cron fires
at the head of the repository's default branch, and a run of the workflow
is made there.

- **A cron** is five fields (minute, hour, day of the month, month, day of
  the week), in UTC, as POSIX reads them: `*`, numbers, ranges, lists and
  steps, months and days by name, Sunday as 0 or 7, and a day that matches
  either the day of the month or of the week where both are given (a field
  that begins with `*`, as `*/2`, gives neither, as cron reads it). One
  that is not one is refused when the file is read.
- **dagron-ci's scheduler** looks at each GitHub repository in its turn,
  as its poll sweep does, where the operator has workflow files read
  (`CI_GITHUB_WORKFLOWS=on`). It reads the default branch (what the
  repository's `HEAD` names) from git's ref listing, and the crons of the
  workflow files `.dagron/ci.yaml` lists at its head once it has moved. A
  repository is looked at again when its next cron is due, and at least
  every ten minutes. It asks git for `HEAD` alone.
- **A cron that fires** queues one `schedule` event, trusted, at that head,
  for its workflow file alone: `github.event_name` is `schedule` and
  `github.event.schedule` is the cron. The workflow runs if it still has
  the cron; one that no longer has it is passed over, and nothing is said
  on the commit.
- **As GitHub**: one schedule fires at most every five minutes, a fire
  more than an hour late (dagron-ci was down, the forge would not answer)
  is passed over and not made up, and a cron fires once however many
  replicas run. A repository with no schedule is looked at every half
  hour.
- **Not as GitHub**: `github.actor` is empty (GitHub names the last to
  change the cron), and a schedule's event is recorded as come in by the
  poll's door (`ci_events.source = 'poll'`).

## What is taken

| Of a workflow | |
|---|---|
| `name`, `run-name`, `permissions` | read. What they grant is the job's `GITHUB_TOKEN`, where the operator gives workflows one ([below](#a-github_token)), and `id-token: write` (or `write-all`) gives it an OIDC token likewise ([below](#an-oidc-token)); a job's own `permissions` are in place of the workflow's |
| `env` | the workflow's variables, for every job |
| `defaults.run` | `shell` and `working-directory` |
| `concurrency` | `group`, as dagron's concurrency key: a newer run of the group waits for the older one, and every run that waits has its turn, where GitHub keeps the newest of them alone. With `cancel-in-progress: true` the newer run takes the older one's place: it is cancelled, and what it had running is stopped first. In the key a `{` of the group is written `%7B`, and a `%` is `%25`: dagron takes no key with a `{{` in it, and two groups stay two. A `cancel-in-progress` that is an expression is refused |

| Of a job | |
|---|---|
| `runs-on` | a label, looked up in the repository's `runs_on` |
| `container` | `image` and `env`. Its `options`, `volumes`, `ports` and `credentials` are refused |
| `services` | `image` and `env`, as dagron's `services:` ([`HOWTO.md`](HOWTO.md), "A container beside a task"). `options` may hold a health check (`--health-cmd`), which is what the service is waited for by; any other option is refused. `ports` is read and needs nothing: a job and its services share one network, so a service is at `localhost` and at its name |
| `needs` | the jobs that run first, and what each came to: `needs.<job>.result` and `needs.<job>.outputs.<name>`, read in the job's `if:`, its steps and its `outputs:` |
| `outputs` | what the job hands on to the jobs that need it, each a text or an expression, read as the job ends. Together they are at most 24 KiB |
| `if` | see below |
| `strategy.matrix` | axes, `include` and `exclude`, written out. An axis may be one expression of what an earlier job handed on (`module: ${{ fromJSON(needs.find.outputs.list) }}`), and so may the whole matrix: see below. `fail-fast` is read, and a failed job's siblings are not cancelled |
| `env`, `defaults.run`, `timeout-minutes`, `continue-on-error` | taken |

| Of a step | |
|---|---|
| `run`, `name`, `id` | taken |
| `shell` | `bash`, `sh`, `python`, `pwsh`, or a command with `{0}` where the script's file goes. With none, `bash` where the job's image has one, and `sh` where it does not |
| `working-directory`, `env` | taken, with expressions |
| `if` | taken, with `success()`, `failure()`, `always()` and `cancelled()` |
| `continue-on-error`, `timeout-minutes` | taken, written out. One that is an expression is refused |
| `uses: actions/checkout` | answered without running it: a job's workspace starts with the event's commit, with a credential the steps never hold. It is fetched alone, or as deep as `fetch-depth` says: 0 is every branch's history and the tags. A checkout that asks for more (`ref`, `submodules`, `fetch-tags`) is refused |
| `uses: ./dir`, `with` | a local action: its steps run in the step's place ([below](#a-local-action)). Each input of `with` is text, a number or `true`/`false`, and may hold expressions. Such a step takes `id`, `name`, `if`, `env`, `continue-on-error` and `timeout-minutes`; a `shell` or a `working-directory` is a run step's, and is refused |
| `uses: owner/repo@ref`, `owner/repo/dir@ref`, `with` | an action of another repository, fetched before the job starts and then run as a local one is ([below](#an-action-of-another-repository)). Where the operator lists the actions that may be used, one not listed is refused, and the ref is a commit's full id. A JavaScript one runs on Node.js ([below](#a-javascript-action)) |
| `uses: docker://image`, `with` | a step in an image of its own, run beside the job ([below](#a-docker-action)): `with.entrypoint` in place of its entrypoint, `with.args` its arguments, split as a shell splits words, and each other input `INPUT_<NAME>` |

A step's `GITHUB_ENV`, `GITHUB_PATH` and `GITHUB_OUTPUT` files are read as
GitHub's runner reads them: a variable for the steps after it, a directory
at the front of their `PATH`, and its outputs, which later steps read as
`steps.<id>.outputs.<name>`. The variables a script expects are set:
`CI`, `GITHUB_ACTIONS`, `GITHUB_WORKSPACE`, `GITHUB_SHA`, `GITHUB_REF`,
`GITHUB_REF_NAME`, `GITHUB_REPOSITORY`, `GITHUB_EVENT_NAME`, `GITHUB_JOB`,
`GITHUB_WORKFLOW`, `RUNNER_OS`, `RUNNER_ARCH`, `RUNNER_TEMP`.

## A local action

A step may use an action of the repository's own: `uses: ./` and a
directory of it, such as `./.github/actions/setup`. The action is read
from the job's workspace as the step starts, from `action.yml` in that
directory or `action.yaml`, as GitHub's runner reads it; so it is the
action at the commit being run. A link on the way to that file is
followed, and one that leads out of the workspace fails the step. A
composite action's steps then run in the step's place:

- **Its inputs** are what the step gives it with `with:`, and the
  `default` of each it is not given, evaluated as the step's own
  expressions are. They are its steps' `inputs`. An input it does not
  declare is warned of in the log, and given all the same.
- **Its steps** are `run:` steps, each with a `shell:` as GitHub requires,
  and steps that use another local action, ten deep at most. They take
  `id`, `name`, `if`, `env`, `working-directory` and `continue-on-error`.
  Their `steps` context is their own, and `success()` and `failure()` ask
  how they have gone, not how the job has. Each has the step's variables
  and `GITHUB_ACTION_PATH`, and `github.action_path` is the action's
  directory. What one writes to `GITHUB_ENV` and `GITHUB_PATH` is the
  job's.
- **Its outputs** (`outputs.<name>.value`) are evaluated when its steps
  are over, against them, and are the step's: `steps.<id>.outputs.<name>`.
- **The step's `timeout-minutes`** is the action's, all its steps
  together.
- **An expression in a script of it** is read as one in a workflow's
  ([below](#expressions)), and an input is a value an
  outsider may choose: a script for a shell that is not `sh` or `bash`
  reads an input through its step's `env:`, and one that pastes it fails.

Its steps are lines of the log of the step that uses it, each `-- ` and
its name, and have no status of their own. What the action gets wrong (no
`action.yml`, a run step with no shell) fails that step, and says what.

An action of a Docker image runs in a container of it
([below](#a-docker-action)).

## An action of another repository

A step may use an action of another repository, as GitHub writes one:
`uses: actions/setup-node@v4`, or `owner/repo/dir@ref` for one in a
directory of it. Each repository a job's steps use is fetched before the
job starts, once for that ref however many steps use it: by the engine,
with the operator's `DAGRON_CHECKOUT_IMAGE` (any image with `git` and
`sh`), into a directory of its own that the job reads and cannot change,
beside its workspace and never in it. The ref is fetched as it stands
when the job starts, as GitHub's runner fetches one, and the job's log
says which commit it was. Nothing is sent with the fetch, but for an
action of the repository's own owner's (a private action, below): an
action is a public repository's, or the operator's mirror's.

The action then runs as a local one does ([above](#a-local-action)),
from its own directory, with `GITHUB_ACTION_REPOSITORY` and
`GITHUB_ACTION_REF` saying which it is.

**An action's own actions.** A composite action's step may use an action
in turn, as on GitHub: one of another repository, a local one (`./dir`, of
the workspace), or an image (`docker://`). As dagron-ci handles the event,
it reads each action a job's steps use (its `action.yml`), and adds those
of other repositories their steps use to what the job fetches, held to the
same rules as a workflow's own step's, as deep as actions run (ten) and
no more than a job fetches in all (32). A local action is read at the
event's commit; another repository's from github.com's raw files, or,
where actions come from a mirror, where `CI_ACTIONS_MIRROR_RAW` says. One
whose file cannot be read (its host refuses it, or cannot be reached from
where dagron-ci runs) is left to the step runner, which says, as the step
starts, that what it uses was not fetched; a host that fails as it answers
(a 5xx, a connection lost) has the event handled again. No more than 64 action
files are read for one event, since anyone who writes a workflow decides
how many it names. `dagron-ci run` reads none: on one's own machine, an
action's own actions of other repositories are not fetched.

Three settings of the operator's say where actions come from, and which
may be used:

| Setting | |
|---|---|
| `CI_ACTIONS_MIRROR` | where an action is fetched from: `<mirror>/<owner>/<repo>.git`. Unset: `https://github.com`. A mirror of the actions a site uses, for one that does not reach github.com |
| `CI_ACTIONS_ALLOW` | `owner/*` and `owner/repo`, comma-separated. Set, a step that uses an action not listed is refused, and each that is listed is pinned to a commit's full id, which no one can move. Unset: any action, at any ref |
| `CI_ACTIONS_MIRROR_RAW` | where one file of an action's repository is read at a ref, with `{owner}`, `{repo}`, `{ref}` and `{path}` in it after its host (`https://git.example/{owner}/{repo}/raw/{ref}/{path}`): how dagron-ci reads an action's file to fetch the actions it uses. Unset with no mirror: github.com's raw files. Unset with a mirror: what an action of another repository uses is not fetched, and the step that runs it says so; a local action's step that uses one of another repository is refused |

**A private action.** An action of the repository's own owner's
(`uses: acme/tools/lint@v1` in a workflow of `acme/widgets`), in a
trusted run, may be in a private repository, as on GitHub, where an
organisation shares its own actions among its repositories. Its fetch is
given a credential of dagron-ci's GitHub App, as the checkout's is
(`DAGRON_CHECKOUT_TOKEN_URL`, [CONFIG.md](CONFIG.md)): minted for that
one attempt, for that one repository, to read it alone, revoked after,
and sent to its URL and no other. dagron-ci reads its `action.yml` with
the App too, to fetch the actions it uses. So the App is to be installed
on that repository as well; where it is not, the action is fetched with
nothing, as a public one is.

- **Only where it shares them.** As on GitHub, a private repository's
  actions are a workflow's to use only where that repository's own
  setting shares them (*Settings → Actions → General → Access*:
  accessible from the owner's repositories, `user`, `organization` or
  `enterprise`), which GitHub leaves off. dagron-ci asks GitHub for that
  setting before it reads such an action's file or gives its fetch a
  credential, and gives none where the answer is not that it shares
  them: its App's access is no way round a repository's own settings, so
  that a workflow cannot have a private repository read that its author
  could not. The App needs the repository permission **Administration:
  Read** to ask; without it, no private action is fetched. A public
  action is fetched with nothing.
- **Only the owner's own.** Another owner's private action is fetched with
  nothing, and fails the job before it starts, in git's words: GitHub, too,
  shares a private action within its owner alone.
- **Only a trusted run.** A pull request from a fork, or an agent's run, is
  given no credential for it, and a private action fails its job there;
  dagron-ci refuses one asked for such a run whatever the engine says.
- **Only from github.com.** Where actions come from a mirror
  (`CI_ACTIONS_MIRROR`), none is sent one: the mirror's are its own to
  give.

## A Docker action

A step that uses an image (`uses: docker://image`), and an action of
`runs.using: docker` whose `runs.image` is one (`docker://image`), run in a
container of that image, which the engine starts beside the job as the
step starts:

- **It has the job's workspace** at `/github/workspace`, where it starts,
  as on GitHub, and the job's network. The job's steps after it see what it
  left there.
- **It has the step's variables**: what a script of the step would have,
  each input as `INPUT_<NAME>`, an action's `runs.env`, and the runner's
  files of its own (`GITHUB_OUTPUT`, `GITHUB_ENV`, `GITHUB_PATH`,
  `GITHUB_STATE`, `GITHUB_STEP_SUMMARY`), which are the job's when it has
  ended. `GITHUB_EVENT_PATH`, `HOME` and `RUNNER_TEMP` are files of the
  step's own in the workspace, gone after.
- **An action's `runs.args`** are each one argument, read against its
  inputs; `runs.entrypoint` is in place of the image's entrypoint;
  `pre-entrypoint` runs before, and `post-entrypoint` when the job's steps
  are over, as a JavaScript action's `pre` and `post` do.
- **What it said** is the step's lines, once it has ended. It exits as the
  container exits, and the step's `timeout-minutes` is its time.

It needs the engine's results service (`DAGRON_ACTIONS_CACHE_LISTEN`,
[CI_RUNNERS.md](CI_RUNNERS.md#a-workflows-docker-actions)), which the job
reaches with a credential of its own, and the Docker or the Kubernetes
executor. On the local one, or a job given no service, the step fails, and
says so. A run that is not trusted names an image the operator lists for it
(`DAGRON_UNTRUSTED_IMAGES`), and its container is held as its job is; on
Kubernetes, where such a container is held to no limit, it runs none.

**An action whose image is a Dockerfile** (`runs.image: Dockerfile`, or
another path of its directory) has its image built first, as the step
starts, from the Dockerfile's directory, as GitHub's runner builds it: the
step runner sends that directory to the engine, less what its
`.dockerignore` leaves out (as Docker's command line reads one), whose
executor builds it on its daemon, and the step runs in the image as one
of `docker://` does. What the build said is the step's first lines, and
the build is of the step's time. The image is named for a digest of what
it was built from, by the engine (`dagron-ci-action:<digest>`), so a
second step of the same action, in the same job or another, has it
already from the engine that built it, while the daemon holds it as it
was built: an image another put under the name is built over, and one by
the name is never pulled. It is built:

- **only for a trusted run.** A build runs what its Dockerfile says on the
  daemon, with its network and none of a job's confinement, so a run that
  is not trusted has none built, and its step fails, saying so: such a run
  names an image a registry has;
- **only on the Docker executor, where its operator lets it**
  (`DAGRON_DOCKER_BUILD_ACTIONS=allow`, [CONFIG.md](CONFIG.md)). On
  Kubernetes, the local executor, or a Docker executor not let, the step
  fails, and says so. Where the job's image already holds what such an
  action does, list it under `github.provided` in `.dagron/ci.yaml`, and
  the step is left out;
- **from no more than 64 MiB**, with the memory the job is held to, in no
  longer than the step's time, with no registry credential: an image its
  `FROM` names is one a registry gives anyone, or the host holds.

## A JavaScript action

An action of `runs.using: node20` or `node24` runs its scripts on Node.js,
as GitHub's runner does (one of `node12` or `node16` too, as GitHub now
runs it, on a later Node.js). The engine installs that Node.js beside the
step runner, at `/dagron/bin/node`, for a job whose steps use an action
of any kind: one build, Node.js 24, made to run in a job image of any libc
(glibc, musl, or none, as a distroless image has). It is some 130 MB, so a
job that uses no action is not given it. Where a job runs on the local
executor, the `node` on its `PATH` runs it.

- **`pre`**, where its `pre-if` holds (`always()` where it says none),
  runs as the step starts. GitHub runs it as the job starts.
- **`main`** runs with each input as `INPUT_<NAME>` (the name upper-cased,
  each space `_`), and its default where the step gives it none, and what
  `pre` wrote to `GITHUB_STATE` as `STATE_<name>`. What it writes to
  `GITHUB_OUTPUT` is the step's outputs, `GITHUB_ENV` and `GITHUB_PATH`
  are the job's, and its exit is the step's.
- **`post`** runs when the job's steps are over, whatever became of them,
  the last action's first, where its `post-if` holds (`always()` where it
  says none), with what `main` wrote to `GITHUB_STATE`. One that fails
  fails the job. A cancelled job runs none.

Each has the runner's files and variables, as a `run:` step has, and
`GITHUB_ACTION`, `GITHUB_ACTION_PATH`, `GITHUB_EVENT_PATH` (what of the
event the job's context holds) and `RUNNER_TOOL_CACHE`, where a setup
action keeps what it downloads, for the job alone. A script is a file of
the action's own directory, found as its `action.yml` is: one whose path
leads out of the workspace, or out of the repository the action was
fetched from, fails the step.

What an action leans on, it does not find yet: no cache service
(`actions/cache` warns and goes on), no artifact service, no token, no
OIDC request endpoint. Those are level 3. Workflow commands it prints are
read as a script's are ([below](#workflow-commands)).

## Workflow commands

What a step prints is read, a line at a time, by the step runner before it
reaches the job's log, as GitHub's runner reads it:

- **`::add-mask::VALUE`** has the value printed `***` from then on, in
  that step and the ones after, wherever in a line it is, and the line
  itself is printed `::add-mask::***`. `%25`, `%0D` and `%0A` are read as
  GitHub escapes them, and a value of several lines is masked a line at a
  time. The job's own credentials are masked from the start: its
  `GITHUB_TOKEN`, the results service's token and the OIDC request token.
  A value is masked too as an annotation's message and properties escape
  it, and two that overlap are masked as one. What the job says of itself
  (its result, its summary, what its test reports say) is masked as well,
  and an output that holds a masked value is not handed on to the jobs
  after it, as on GitHub: the job warns that it is not.
- **`::error file=F,line=L,endLine=E,title=T::MESSAGE`**, and
  `::warning` and `::notice` likewise, is an annotation on the job's check
  run, where the operator has check runs on. `file` is the repository's
  path; one under the job's workspace (as a tool that prints absolute
  paths writes it) is read as the rest. One that names no file is the
  workflow's, as on GitHub. Fifty of a job's are kept, with what its test
  reports annotate.
- **`::stop-commands::TOKEN`** stops reading commands until a line
  `::TOKEN::`: what is printed between the two masks and annotates
  nothing, as on GitHub, so that a step can print what it did not write.
- **`::debug::MESSAGE`** is printed only where `ACTIONS_STEP_DEBUG` is
  `true`, which also sets `RUNNER_DEBUG=1` for the job's steps: in the
  workflow's or the job's `env:` (dagron has no `vars`, where GitHub reads
  it too), or as the repository's secret of that name, which is given to a
  job that is given any secret. Elsewhere the line is not printed, as on
  GitHub.
- **`::group::TITLE` and `::endgroup::`** are printed as they are, and the
  console folds the lines between them under the title, folded until it
  is opened, as GitHub's log does.
- **What is not read**: a line longer than 64 KiB is printed as it is,
  and a value cut in two there is not masked. A process a step left
  running that prints after the job's steps are over is not printed, and
  the job does not wait on it: the job's own last line is its last.

## A `GITHUB_TOKEN`

A job is given a `GITHUB_TOKEN` where the operator gives workflows one
(`CI_GITHUB_TOKEN=on`, with dagron-ci's GitHub App and the engine's token
service, `DAGRON_CHECKOUT_TOKEN_URL`). The engine asks dagron-ci for it as
each attempt starts, and it is revoked once the attempt is over:

- **What it may do** is what the job's `permissions` grant, a job's own in
  place of the workflow's, by a GitHub App's names (`pull-requests` is
  `pull_requests`), with `metadata: read`. Where nothing says, it is
  `contents: read`, GitHub's restricted default. `read-all` and
  `write-all` are each of them. What GitHub has and an App does not
  (`packages`, `attestations`) gives the token nothing, and a job granted
  nothing (`permissions: {}`) is given none.
- **A run that is not trusted is given read alone**, whatever its
  `permissions` say, as GitHub gives a fork's pull request a token that
  reads.
- **It is for the job's own repository**, and no other.
- **A step reads it** as `secrets.GITHUB_TOKEN` or `github.token`, each a
  variable of the script and never its text, and the job has it as
  `GITHUB_TOKEN`. No step may set it for the steps after it. It is masked
  in the job's log, and on Kubernetes it is in the task's Secret, not its
  pod.

Off (the default), a job is given none, and `secrets.GITHUB_TOKEN` reads as
empty.

## A repository's secrets

`${{ secrets.NAME }}` in a step (its script, `env:` or `with:`) is a secret
of the repository's, which dagron keeps: GitHub gives no one a secret's
value back, so a repository's secrets are put in dagron, in an environment
of the repository's own, named by its address in lower case,
`<host>/<owner>/<repo>`:

```sh
# An environment for acme/widgets, then each secret its workflows read.
curl -X POST "$API/api/environments" -H "$AUTH" \
  -d '{"name":"github.com/acme/widgets"}'
curl -X PUT "$API/api/environments/$ID/secrets/NPM_TOKEN" -H "$AUTH" \
  -d '{"value":"…"}'
```

- **The job is given each secret its steps read**, and no other: as it is
  dispatched, the engine decrypts each from the environment and gives the
  job it as a variable of its own (`CI_SECRET_NAME`), which the step
  runner reads `secrets.NAME` from. A name is read in capitals, as GitHub
  keeps its secrets: `secrets.npm_token` is `NPM_TOKEN`.
- **An admin makes the environment**, as an admin writes a policy: its
  name alone says whose secrets are in it. Anyone who may write
  environments may then set its secrets, as for any environment.
- **One the environment does not hold reads as empty**, as GitHub's
  undefined secret does, and so does every one where the repository has no
  environment: the run is made all the same. The engine's own store
  (`DAGRON_SECRET_<NAME>`) is never read for one.
- **A repository reads no secret but its own.** The engine gives a
  repository's environment to that repository's runs alone, as dagron-ci
  recorded them: not to another repository's, not to one of the same name
  on another forge (whose address is another), and not to a run of any
  other kind that names it (a native pipeline's task, a run on the API).
  The environment is the address's, not the repository's: one renamed
  leaves it to whoever registers the old name next, so move or remove it
  when a repository is renamed.
- **A step has the secrets its workflow reads**, and no process inherits
  them: the step runner takes them out of its own environment as it
  starts, and is not dumpable, so that the environment it began with is
  no step's to read under `/proc`. A step, or an action one uses, sees a
  secret only where the workflow put it (its script, `env:` or `with:`).
  An action's own steps, and its inputs' defaults, have no `secrets`, as
  on GitHub.
- **A run that is not trusted is given none**, as GitHub gives a fork's
  pull request none: its `secrets.NAME` is empty. Which trusted runs may
  read them is the environment's policy (by default, every trusted run):
  an admin can hold them to a branch, `allowed_refs: ["refs/heads/main"]`,
  or to some of its workflows. A run the policy does not admit fails as
  its job starts, where GitHub would give it empty secrets: a job that
  must run without them reads none.
- **It is masked** in the job's log from the start, wherever it is
  printed, as on GitHub. No step sets `CI_SECRET_*` for the steps after
  it.
- **A job's or the workflow's `env:` may read one**, as on GitHub: the
  step runner reads such a variable as the job starts, with the secrets,
  and every step has it, as the rest of the job's `env:`. The task holds
  only the variable's text as written (`CI_JOB_ENVX_<NAME>`), never its
  value. A workflow may not name a variable `CI_JOB_ENVX_*` or
  `CI_SECRET_*` itself, and one such variable that is the runner's own
  (`GITHUB_*`, `RUNNER_*`) is not set, as a step's `GITHUB_ENV` may not set
  it.
- **Not taken yet**: `secrets` in a job's `outputs:` or its `if:`, its
  container's or a service's `env:` (each read before any secret is given,
  or by the engine), and `secrets` read whole (`toJSON(secrets)`).

## An OIDC token

A job whose `permissions` give it `id-token: write` may ask for an OIDC
token, as a step does to reach a cloud with no stored secret
(`aws-actions/configure-aws-credentials`, `azure/login`), where the
operator gives workflows one (`CI_GITHUB_OIDC=on`, with the engine's
`DAGRON_OIDC_REQUEST_URL` and dagron-api's issuer set up). The job is given
the identity the engine gives a task, and GitHub's two variables for it:
`ACTIONS_ID_TOKEN_REQUEST_URL` and `ACTIONS_ID_TOKEN_REQUEST_TOKEN`, which
`@actions/core`'s `getIDToken` reads. The answer is GitHub's,
`{"value": "<token>"}`.

- **Its audiences**: the forge's own, `https://github.com/<owner>`, which
  a request that names none is given, as GitHub gives it; then those the
  repository names in `.dagron/ci.yaml` (`github.oidc_audiences`). A
  request for any other is refused.
- **Its issuer is dagron's, not GitHub's.** A cloud's trust policy names
  dagron-api's issuer, and its subject is dagron's:
  `repo:github.com/<owner>/<repo>:ref:<ref>` (or `:pull_request`), with the
  forge's host in it, where GitHub's has none.
- **A run that is not trusted is given none**, as GitHub gives a fork's
  pull request none: the job runs, and a step that asks for one fails.
- **Off** (the default), `id-token: write` gives nothing, and the job runs
  without.

No step may set `ACTIONS_*` for the steps after it through `GITHUB_ENV`:
one that moved the request URL would have the next step send its request
credential elsewhere.

## Expressions

`${{ … }}` is read by GitHub's documented grammar: literals, the operators
in their order, contexts, `.*`, the loose comparison (strings compare
ignoring case), and `contains`, `startsWith`, `endsWith`, `format`, `join`,
`toJSON`, `fromJSON` and, in a step, `hashFiles` (below). An expression
may nest fifty deep, and stand a hundred operators, properties and calls
one on another (`a || b || c` is two): past either it is refused when the
file is read.

| Context | |
|---|---|
| `github` | `event_name`, `ref`, `ref_name`, `ref_type`, `sha`, `repository`, `repository_owner`, `base_ref`, `head_ref`, `actor`, `triggering_actor`, `workflow`, `job`, `run_id`, `run_number`, `run_attempt`, `server_url`, `api_url`, `graphql_url`, `workspace`, and of `event`: `number`, `pull_request.number`, `pull_request.base`, `pull_request.head.ref` and `.sha`, `sender.login`, `inputs`, and a push's `before` and `after`. The rest of `event` is the forge's own payload, as its webhook delivered it (a pull request's `title`, `labels` and `user`, a push's `commits` and `head_commit`, `repository`, `action`, …), where dagron-ci kept it (at most 64 KiB), with the fields above over it; an event dagron-ci made itself (a poll, a schedule, a request of its run API) has the fields above alone, and anything else of it is `null`. Each is a step's `GITHUB_*` variable too, as on GitHub; `api_url` is dagron-ci's GitHub API door where the operator serves one ([CI_RUNNERS.md](CI_RUNNERS.md#a-workflows-artifacts)) |
| `inputs`, `matrix`, `strategy`, `env` | as on GitHub |
| `steps`, `job`, `runner` | in a step: `steps.<id>.outputs`, `.outcome`, `.conclusion`; `job.status`; `runner.os`, `.arch`, `.temp` |
| `vars` | empty |
| `needs` | in a job's `if:`, its steps and its `outputs:`: each job it names, with its `result` (`success`, `failure`, `cancelled`, `skipped`) and its `outputs`. A job with a matrix is one job here: a failure if one of its instances failed, and their outputs one map |
| `secrets` | in a step, and in a job's or the workflow's `env:`: the repository's, from its environment ([above](#a-repositorys-secrets)), and `GITHUB_TOKEN` |

**The run's numbers.** dagron-ci gives a workflow's run its numbers as it
makes the run: `github.run_id`, a count of every run of the
repository's, and `github.run_number`, a count of that workflow file's
runs on it. Both are text, as GitHub's are; `run_id` is no count across
repositories, as GitHub's is, so that none says how much another runs. A
workflow an event does not run is given none, so a workflow's count is of
its runs, and an event handled again gives its run the numbers it had. A
concurrency group made of them (`${{ github.head_ref || github.run_id }}`)
is the run's own. `github.run_attempt` is `1`: a rerun, of a run's failed
jobs or of every job, runs what was made for its first attempt.
`github.api_url` is GitHub's API, a GitHub server's (`/api/v3`), or
Gitea's and Forgejo's (`/api/v1`, with no `graphql_url`).
`github.actor` and `github.triggering_actor` are who caused the event, as
the forge said, and `github.head_ref` a pull request's head branch: each
is read in a script as a variable, never pasted into it, as its owner
chose it.

**`hashFiles(…)`**, in a step's script, its `with:`, its `env:` and its
`if:`, is the hash a cache's key is most often made of
(`hashFiles('**/Cargo.lock')`):

- Its patterns are globs read against each file's path under the job's
  workspace, `/` between its parts, as GitHub's runner reads them: an
  argument may hold several, a line each, trimmed, blank lines and `#`
  comments passed over; an odd number of `!` before one leaves out what it
  matches; and the last that matches a path decides. A pattern written
  under the workspace's own path, or after `./`, reads as the rest.
- A pattern names what is under it too: `src` is all of `src/`, and
  `!vendor` leaves out all of `vendor/`. A list with nothing to include
  matches nothing.
- It is SHA-256's, in hex, of each matched file's own SHA-256, one after
  another in their paths' order; the empty string where nothing matched.
  GitHub's runner hashes the same way, in the order its search finds the
  files, so a key may differ from GitHub's for the same files: dagron-ci's
  caches are its own, and a key needs only to change when the files do.
- A symbolic link to a file is hashed as that file. One to a directory is
  looked into only where the first argument is `--follow-symbolic-links`,
  and a link back up is not followed round. A broken link is passed over,
  and so are a pipe, a socket and a device.
- It is a step's: a job's `if:`, `runs-on` or `env:` that calls it is
  refused when the file is read, as on GitHub.

**A job's `if:`** is decided twice. When the run is made, from the event:
a job whose `if:` is false then is left out, with the jobs that need it,
and `always()` and `failure()` become the engine's own rules (run whatever
became of the jobs before, run where one of them failed). And as the job
starts, by the step runner, from what the jobs it needs came to. An `if:`
that reads `needs` is decided only then: the job is started whatever
became of the jobs it needs, and one whose `if:` is false runs no step.
With no status function a job needs every job it names to have succeeded,
and one of them skipped is not that: a job that needs a skipped job is
skipped with it, as on GitHub.

**How one job tells another.** A job's last line is its own:
`::dagron-ci::job::` and its result and outputs, as JSON. The engine gives
each job that needs it that line and the task's status as it starts
(dagron's `env_from:`, [`HOWTO.md`](HOWTO.md) §11), and the step runner
reads them. So a job writes everything it prints to one stream, its steps'
standard error with it.

**A matrix an earlier job computes.** A matrix that is written out is
written out into jobs when the run is made. One with an axis that is an
expression, or that is one expression whole, cannot be: no job has run.
Such a job is two tasks. The first, `<job>.strategy.matrix`, is started as
the job would be: it reads what the jobs it needs came to, decides the
job's `if:`, and prints the job's instances. The engine then makes a task
of each, `<job>.<label>`, under the job's own task, which is what a job
that needs it waits for. A job whose `if:` is false has no instance, and
reads as skipped to the jobs after it. An axis that comes to no value fails
the job, as on GitHub. What such a job's `runs-on`, container, services
and `env:` read is decided when the run is made, so they cannot read its
`matrix`; its steps can. A matrix that is one expression whole has its
axes in the order of their names, and not the order the JSON wrote them
in: its jobs are the same ones, made and named in that order.

**A step's expressions** are decided as the job runs, by the step runner.

**An expression in a script.** GitHub pastes `${{ … }}` into a `run:`
before any shell reads it, which is how a pull request's title becomes a
command. Here an expression that reads only what no outsider chooses (a
matrix value, `github.sha`, `github.event_name`, `runner.os`) is pasted as
GitHub pastes it. Any other is replaced by a shell variable that holds its
value: `echo "${{ github.ref_name }}"` prints the branch's name and runs
nothing of it.

- Inside single quotes a variable is not expanded. Pass such a value
  through the step's `env:` and read the variable.
- In a script for a shell that is not `sh` or `bash`, such an expression is
  refused, with the same advice.

## What differs from GitHub

- **The image is yours.** A label means the image you gave it. Nothing of
  GitHub's hosted runners' tools is there unless your image holds it.
- **A job's workspace is the event's commit**, detached, with no
  credential left in it. A job cannot push with it. It has the commit's
  history where a checkout step asked for it (`fetch-depth`), and the
  commit alone where none did.
- **A `GITHUB_TOKEN` is an installation token of the operator's App**,
  where the operator gives one: it lapses within the hour, as GitHub's, or
  when the job is over.
- **Of the workflow commands**, `::add-mask::`, `::error::`,
  `::warning::`, `::notice::` and `::stop-commands::` are read
  ([below](#workflow-commands)), and `::debug::` is printed where debug is
  on and `::group::` folds in the console. `::echo::` is a line of the log,
  and `::set-output` and `::save-state`, which GitHub has turned off, do
  nothing.
- **A step's summary is on its job's check run**, where the operator has
  check runs on ([how](../crates/dagron-ci/README.md#check-runs)): what
  the job's steps wrote to `GITHUB_STEP_SUMMARY`, in their order, up to
  24 KiB a job. Where a job is a commit status, nothing shows it.
- **A cancelled job starts no further step**, whatever its `if:` says.
- **A failed job's matrix siblings run on**: `fail-fast` cancels nothing.
- **A job skipped as it starts has started.** Where a job's `if:` reads
  `needs`, or a job it needs was skipped so, a container is started for it
  to find that out, and its task succeeds having run no step. Its status on
  the commit reads `succeeded`; the jobs after it read `skipped`.
- **A job's status functions look at the jobs it names.** `failure()` in a
  job's `if:` is true where a job in its own `needs:` failed, and not for
  one further back that a job between them was skipped for.
- **A failed job's outputs may not arrive.** A job that failed hands on its
  result; its outputs are read from its last line, and where the executor
  writes why the task failed after it they are not.
- **A matrix an earlier job computes is two more checks on the commit.**
  The task that plans the instances and the job's own task each have a
  status, beside the instances'.
- **A job's log is one stream.** What a step wrote to its standard error is
  in order with what it wrote to its standard output, and is not told apart.
- **A `{{` is text, as on GitHub, and is written another way on its
  way.** dagron fills `{{ name }}` in a native pipeline's text with the
  run's parameter of that name. Nothing of a workflow file is read so: not
  a script's `{{ version }}` or `{{.Names}}`, and not what a caller sends
  as an input. In the workflow a run is submitted as, each `{{` of the
  file is written `{{ ci_braces }}`, a parameter of the run whose value is
  the two braces: dagron fills that in, and the job gets what was written.

## How close it comes: the conformance suite

GitHub's own workflow templates for Rust, Go, Node.js, Python and a
Docker image are run on dagron-ci and on GitHub, and what each
of their jobs came to is compared ([`conformance/`](../conformance/README.md)).
On dagron-ci each job runs in an image of its toolchain, which its `runs-on`
label names, with the setup action that image makes unneeded `provided`.

- On dagron-ci all five build and test, the Node.js template's three
  matrix rows among them, and the Docker template's `docker build` with a
  daemon of its job's own (`github.docker: true`, [above](#turning-it-on)).
- On GitHub all five succeed. **The score (2026-10-16): 5 of 5 jobs
  agree, 100%.** The first, before a job could have a daemon, was 4 of 5.

`dagron-ci run` runs a repository's workflow files as it runs its
pipelines, and `--report` writes what each of their jobs came to.

## What is not taken

Each is refused when the file is read, with its name: an action the
operator does not allow, or one not
pinned where the operator lists the actions it allows; a job that calls a
reusable workflow; `needs` read anywhere but a job's `if:`, its steps and
its `outputs:` (a job's `env:`, its image, its matrix); `secrets` anywhere
but a step and a job's or the workflow's `env:`; a job's `environment`; an expression among a matrix's values or in a row of
it; the `outputs` of a job whose matrix an earlier job computes;
`max-parallel`; a runner group; a `cancel-in-progress` that is an
expression; a job's own `concurrency:`; a container's or a service's
`credentials:` and `volumes:`.

## Checking a file before it is pushed

```sh
dagron-ci validate .github/workflows .dagron/ci.yaml
```

Each workflow file is read by the front-end that would run it, and then
read for an event of each kind it takes, so that a label with no image, or
an expression that does not evaluate, is found here. It says how many jobs
the workflow becomes, which events it runs for, and whether it is listed to
run at all.

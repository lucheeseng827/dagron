# Moving a repository's CI to dagron-ci

From GitHub Actions, from Jenkins and from GitLab CI: what each thing a
team has becomes, and what has no place to go yet. The pipeline format is
in [`CI_PIPELINES.md`](CI_PIPELINES.md), and what dagron-ci refuses and
does not do is in [`CI.md`](CI.md). This says how to get from there to
here.

## Before any of them

- **The repository is on github.com, or on GitLab.** Those are the forges
  dagron-ci reads, reports to and checks out from. A repository on Gitea
  or on GitHub Enterprise Server is not served yet,
  whatever its CI is written in.
- **A job is a container of an image you name.** Nothing of a hosted
  runner's tools is there unless the image holds it.
- **Check a pipeline before it is pushed**: `dagron-ci validate` reads it
  with no server, `dagron-ci compile` prints what it becomes for an
  event, and `dagron-ci run` runs it on your own machine
  ([how](CI_PIPELINES.md#checking-and-running-a-pipeline-before-it-is-pushed)).
- **Import first, then edit.** `dagron-ci import --write`, at the
  checkout's root, writes each workflow of `.github/workflows`, the
  `.gitlab-ci.yml` and the `Jenkinsfile` as a pipeline of `.dagron/ci`,
  under a report of what it changed, did not carry over and leaves to do by
  hand; and it reads and compiles each before it says ok
  ([how](../crates/dagron-ci/README.md#from-another-ci-dagron-ci-import)).
  `dagron-import github`, `gitlab` and `jenkins` are the same importers, one
  file at a time.
- **Run both for a while.** dagron-ci's checks have names of their own,
  `dagron-ci/<pipeline>/<job> (<event>)`, beside whatever the old CI
  posts. Move branch protection to them, naming the `(pull_request)`
  ones, when they have agreed for long enough, and take the old pipeline
  out after.

What a team gives up today, whichever CI it comes from:

| Had | Here |
|---|---|
| A cache between runs | `workspace.cache`, where the operator gives the engine a cache store: a prefix and the files that make its key; restored from the run's ref, then a pull request's base branch; saved by trusted runs |
| Files handed from one job to the next (artifacts, a stash) | not built. A job's workspace is its own and goes with it: steps that share files are steps of one job |
| A value one job prints, read by a later job | a later job reads an earlier one's *status* (`env_from`), and not, for a job with `steps`, a value it printed |
| A schedule | not built for a pipeline. A run can be asked for from a scheduler of your own, through the run API. A GitHub workflow file run as it is keeps its `on.schedule` ([CI_GITHUB.md](CI_GITHUB.md#a-schedule)) |
| A newer push cancelling the older run | `concurrency: { key: ..., cancel_superseded: true }`. Without `cancel_superseded` a newer run of the key waits its turn |
| A token for the job to call its forge | not built |
| Cloud identity (OIDC, `id-token: write`) | `identity: { oidc: { audience: … } }`; the cloud's trust policy names dagron's issuer and not GitHub's, and matches `repo:<host>/<owner>/<name>:…` |
| A step's own condition, shell or retry | a step is a shell script: the condition is in the script. A *job* has `when:` and `max_attempts` |
| A following log | a log is read, and by step in the console |

## From GitHub Actions

There are three ways, and they can be taken one after another.

| | What changes in the repository | What it is for |
|---|---|---|
| **Keep Actions, move the machines** ([fleet mode](../crates/dagron-ci/README.md#fleet-mode)) | a job's `runs-on` | GitHub Actions stays the coordinator, with its actions, its secrets and its checks. The job runs on your capacity |
| **Run the workflow file as it is** ([`CI_GITHUB.md`](CI_GITHUB.md)) | one file, `.dagron/ci.yaml`, that lists which workflows run here and what image each label means | a workflow of `run:` steps. An action other than the checkout is refused, and so is a workflow that reads `secrets` |
| **Write a pipeline** | a file in `.dagron/ci` | everything a pipeline has: environments' secrets, approvals, test reports |

A workflow listed in `.dagron/ci.yaml` still runs on GitHub too, until it
is turned off there.

### A workflow as a pipeline

`dagron-ci import` writes the pipeline for a workflow file, by the table
below, says what it could not carry over, and reads and compiles what it
wrote (`dagron-import github` is the same importer, printing it):

```sh
dagron-ci import .github/workflows/ci.yml --write    # .dagron/ci/ci.yaml, read and compiled
# or, with the importer alone:
dagron-import github .github/workflows/ci.yml > .dagron/ci/ci.yaml
dagron-ci validate .dagron/ci/ci.yaml
```

The file it writes begins with its report, as a comment, in three parts:

- **To do by hand**: what the workflow had that the pipeline has no place
  for, and is not right without. An action other than the checkout and
  the ones that install a toolchain, a job's outputs, a condition that
  could not be read, a secret with no environment named for it, a key
  the importer does not know.
- **Changed on the way**: what is there and reads differently. A job's
  image where it had a runner's label or a setup action, a variable where
  a script had an expression, a test at the head of a script where a step
  had an `if:`.
- **Not carried over**: what the pipeline does without. A cache whose key
  or path the importer cannot read, a `permissions:` block, a schedule, a
  pull request's `types`.

Each line names every place it is said of, and a heading counts places.

**A job with something left to do does not pass.** It starts with a step
that says what is left and fails, an action that was not imported is a
step that fails in its place, and an expression with nothing to stand for
it stops its script where it is read. What is left of the workflow as a
whole (an event with no like here, a key that is not imported) is no one
job's, and every job's first step says it. A job that may fail
(`continue-on-error`) is not written as one while something of it is
left, or that step would fail nothing. So what is written is a pipeline
`dagron-ci validate` takes and an event runs, and one nobody takes for
finished before it is.

A label GitHub hosts (`ubuntu-latest`) is read as an image of that
operating system, which holds none of a hosted runner's tools: the report
says so for each job. Say what your own labels mean with
`--image LABEL=IMAGE`, and what to call the pipeline with `--name`
(its file's name unless said, where the workflow's own name cannot be
one). An image given for a label is the job's, whatever its steps
install. A label no image stands for (`macos-latest`, one of your own
with none given) leaves its job to do.

What reads differently here, and what the importer does about each:

- **A pattern.** GitHub's `+` and `?` after a character say how many of
  it (`v[0-9]+.[0-9]+`), and a pattern here has no such word: a `?` is
  any one character, and a `+` is itself. Such a pattern is written to
  let in what it did and more (`v[0-9]*.[0-9]*`), and is left to do, so
  that what runs for more than it should is yours to see first.
- **Two braces.** A pipeline reads `{{ name }}` as a template of its own.
  A `{{` the workflow held for a reason of its own (a Go template, a
  Jinja one) is written `{ {`, and left to do: nothing of the workflow's
  is read as a template.
- **An expression in a script.** GitHub wrote its value into the script
  before a shell read it. Here it is a variable (`${CI_SHA}`), which the
  shell fills in where a shell does. One that stood inside single quotes,
  or in a here-document whose mark is quoted, is left to do.
- **The shell.** A step that named none ran under bash on a hosted
  runner, and a step here is `sh -e`. A script that is plainly bash's
  (`[[`, `pipefail`, an array, `==` in a test) is left to do. That is not
  all there is to find.
- **A secret.** It is the job's here, and so every step's of it, where
  the workflow gave it to the steps that read it. The report says which.
- **A matrix's version.** A setup action whose version is the matrix's
  (`node-version: ${{ matrix.node }}`) gives each instance its own image:
  each item is an object with the image's name beside the version, and
  the job's `docker_image` reads it. A version that names no image
  (`nightly`) leaves the job to do.
- **An input, on an event that is no dispatch.** It is nothing there, as
  on GitHub: a script that reads one begins by saying so. A job's
  variable or image that holds one is left to do where the pipeline runs
  for other events too, since dagron would leave the input's name there
  in braces.
- **A pull request's `types`.** One that names none of opened,
  synchronize and reopened (`[closed]`, `[labeled]`) is a workflow for
  another moment: the event is left out, and left to do.
- **A step's `if:` on a yes or a no.** GitHub reads `matrix.x == 'true'`
  against a matrix's `true` as never the same, which is seldom what was
  meant: it is left to do. Text is compared as text, letter for letter.

| In a workflow | In a pipeline |
|---|---|
| `on.push` with `branches`, `tags`, `paths` | the same, under `on.push`. An `-ignore` is `_ignore`. A `+` or a `?` in a pattern has no like here |
| `on.pull_request` | `on.pull_request`. No `types`: it is opened, updated and reopened |
| `on.workflow_dispatch.inputs` | `on.dispatch.inputs`. Every input is text, with a `default`, `required` and a `description` |
| `on.merge_group` | `on.merge_group` |
| `on.schedule` | none: see above |
| `jobs.<id>` | an entry of `tasks:`, with that `name` |
| `runs-on`, `container.image` | `docker_image` |
| `needs` | `depends_on` |
| `steps[].run` | `steps[].run`. Each is its own `sh -e`: no `shell:`, and a `cd` does not carry over |
| `steps[].uses: actions/checkout` | nothing: every job with steps starts with the commit checked out. `workspace: { checkout: { depth: 0 } }` for all history |
| Any other `uses:` | the commands it stood for, in a step |
| `env:` on a job | `env:`, as a **list**: `- { name: X, value: "y" }`. A step's stays a map |
| `${{ secrets.X }}` | `environment:` on the pipeline or the job, and `- { name: X, value_from: { secret: x } }` in the job's `env:` ([secrets](CI_PIPELINES.md#secrets)) |
| `environment:` with required reviewers | an environment's policy says which runs may read it. A person's sign-off is a job of `type: approval` |
| `strategy.matrix` on one axis | `with_items: [...]` on the job, read as `{{ item }}`. Its instances are named `<job>.0`, `<job>.1`, or by `instance_key` |
| A setup action's version from the matrix | each item an object, with the image that holds its version: `docker_image: "{{ item.node_image }}"` |
| A matrix of several axes | `with_items` over the combinations, written out as objects: `{{ item.os }}` |
| `if:` on a job | `when:` on the job |
| `if:` on a step | a test in the step's script |
| `timeout-minutes` | `timeout_secs`, on a job or a step |
| `continue-on-error` | `continue_on_error` on a step; `allow_failure: true` on a job |
| `services` | `services`, each with an `image`, an `env:` list and a `ready` command |
| `concurrency.group` | `concurrency: { key: ... }` |
| `concurrency.cancel-in-progress: true` | `concurrency: { cancel_superseded: true }`: a newer run takes the older one's place. Without it a newer run waits its turn, and every run that waits has one, where GitHub keeps the newest alone |
| `outputs`, `needs.<job>.outputs` | not built for a pipeline's jobs: see above |
| `actions/cache`, `Swatinem/rust-cache`, a setup action's `cache:` | `workspace.cache`: `dagron-import` takes the prefix from the key's own words and the key's files from its `hashFiles(…)`; `restore-keys` read as "the newest of the prefix" |
| `actions/upload-artifact` | not built |
| `GITHUB_TOKEN`, `permissions` | none: the job is given no token |
| `GITHUB_SHA`, `GITHUB_REF`, `github.event.number` | `CI_SHA`, `CI_REF`, `CI_PR` ([all of them](CI_PIPELINES.md#what-a-step-knows-of-its-event)) |
| `${{ github.head_ref }}` pasted into a script | `$CI_REF` read in the script. A `{{ ... }}` that names something an outsider chose is refused in a script |
| `$GITHUB_STEP_SUMMARY` | `$CI_STEP_SUMMARY` |
| An action that reports tests | `report: { junit: ..., sarif: ... }` on the job ([`report:`](CI_PIPELINES.md#report)) |

A workflow of three jobs, and the pipeline `dagron-import github` writes
for it:

```yaml
# .github/workflows/ci.yml
name: ci
on:
  push: { branches: [main] }
  pull_request: { branches: [main] }
concurrency: { group: "${{ github.ref }}" }
jobs:
  lint:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-node@v4
        with: { node-version: 22 }
      - run: npm ci
      - run: npm run lint
  test:
    needs: lint
    runs-on: ubuntu-latest
    strategy:
      matrix: { node: [18, 20, 22] }
    timeout-minutes: 15
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-node@v4
        with: { node-version: "${{ matrix.node }}" }
      - run: npm ci
      - run: npm test
  publish:
    needs: test
    if: github.event_name == 'push'
    runs-on: ubuntu-latest
    environment: release
    steps:
      - uses: actions/checkout@v4
      - run: npm publish
        env: { NPM_TOKEN: "${{ secrets.NPM_TOKEN }}" }
```

```yaml
# .dagron/ci/ci.yaml, under its report of what was changed on the way
name: ci
on:
  push:
    branches:
    - main
  pull_request:
    branches:
    - main
concurrency:
  key: '{{ ci_group }}'
tasks:
- name: lint
  docker_image: docker.io/library/node:22-bookworm-slim
  steps:
  - run: npm ci
  - run: npm run lint
- name: test
  docker_image: '{{ item.node_image }}'
  depends_on:
  - lint
  with_items:
  - node: 18
    node_image: docker.io/library/node:18-bookworm-slim
  - node: 20
    node_image: docker.io/library/node:20-bookworm-slim
  - node: 22
    node_image: docker.io/library/node:22-bookworm-slim
  timeout_secs: 900
  steps:
  - run: npm ci
  - run: npm test
- name: publish
  docker_image: docker.io/library/ubuntu:24.04
  depends_on:
  - test
  when: '{{ ci_event }} in [push, tag]'
  environment: release
  env:
  - name: NPM_TOKEN
    value_from:
      secret: npm_token
  steps:
  - run: npm publish
```

What changed besides the keys, each a line of its report:

- **The image does what `setup-node` did.** Each instance of the matrix
  has the image that holds its own version, named in its item. Written by
  hand, the same is `docker_image: "node:{{ item }}-bookworm-slim"` over
  `with_items: ["18", "20", "22"]`.
- **`publish` has no Node.** Its label is an operating system's image,
  with none of a hosted runner's tools, and the report says so: name the
  image that holds what the job runs.
- **A push of a tag is a `tag` here**, where GitHub says `push`: the job's
  condition is written to run for both.
- **The checkout is gone from the steps**: a job has it before its first.
- **The secret is an environment's**, in dagron-api, with a policy that
  says which runs may read it. Close `release` to pull requests there
  (`allowed_refs`), and the `when:` is no longer what keeps a pull
  request from it.
- **The checks have new names**: `dagron-ci/ci/lint (push)`,
  `dagron-ci/ci/test.0 (pull_request)`.

## From Jenkins

`dagron-ci import` (or `dagron-import jenkins`) writes the pipeline for a
declarative `Jenkinsfile`, under the same report the GitHub importer
writes, as the points below say: the table after them is the map for
doing it by hand, and says where the importer does otherwise. A Scripted Pipeline
(`node { … }`) is Groovy run as a program, and is refused by name: write
it as a pipeline by hand.

```sh
dagron-ci import Jenkinsfile --image linux=docker.io/library/debian:12 --write    # .dagron/ci/ci.yaml
# or, with the importer alone:
dagron-import jenkins Jenkinsfile --image linux=docker.io/library/debian:12 > .dagron/ci/ci.yaml
dagron-ci validate .dagron/ci/ci.yaml
```

What it does that a person might not expect:

- **The stages of one agent are the steps of one job.** Jenkins runs them
  in one workspace, so a stage reads what the stage before it built, and a
  job's workspace here is its own. A stage with an agent of its own is a
  job of its own, as it had a workspace of its own there. Parallel stages
  on the pipeline's agent are steps too, one after another, and the report
  says so: make one a job of its own where it needs nothing of the stages
  before it.
- **A stage's `when` is a test at the head of its steps**, and of its
  `post`'s, read as Jenkins read it (`branch`, `tag`, `buildingTag()`,
  `changeRequest()`, `environment`, a plain `expression`, and `not`,
  `allOf`, `anyOf` of them). Where it is false the steps end there, and
  the job passes. What it cannot read before the stage runs (`changeset`,
  an `expression` of Groovy) is taken to hold, or not, whichever runs the
  stage, and is left to do. An `input` is a gate (`type: approval`) before
  its stage, given the stage's `when` where dagron can read it; the jobs
  after it wait for what it waited for too, as a run its `when` does not
  hold for leaves the gate out.
- **A stage's secrets are its steps' alone on Jenkins.** Here the job
  holds each by a name of its own (`TOKEN__prod_token`), and the stage's
  steps set the stage's name (`$TOKEN`) from it at their head, as a
  `withCredentials` block's steps do: every step of the job has the secret
  in its environment under the name of its own. That is left to do: the
  job starts with a step that fails until someone accepts it, or gives the
  stage a job of its own.
- **A `catchError` block is one step that may fail**
  (`continue_on_error`): it ends at the first of its steps that fails, as
  the block did, and the stage goes on after it.
- **An agent's label is an image you name**: `--image LABEL=IMAGE` gives
  the image for `agent { label 'LABEL' }`, and `--image any=IMAGE` for
  `agent any`.
- **Jenkins's variables a step reads are set at its head** from
  dagron-ci's own (`$BRANCH_NAME`, `$GIT_COMMIT`, `$CHANGE_ID`, …).
  `$BUILD_NUMBER`, and the others no step is given, are left to do.

A declarative `Jenkinsfile`, key by key.

| In a `Jenkinsfile` | In a pipeline |
|---|---|
| `agent { docker { image '...' } }` | `docker_image`, on the job or in `task_defaults` |
| `agent { label '...' }` | `runner_class` or `pool`, which name capacity the operator set up. The importer writes the `docker_image` `--image LABEL=IMAGE` gives instead |
| A `stage` | a job, where it should have a container of its own; a step, where it shares files with the stage before it |
| `steps { sh '...' }` | `steps: [{ run: ... }]` |
| `parallel` | jobs with the same `depends_on` |
| `environment { X = 'y' }` | `env:`, a list on a job and a map on a step |
| `credentials('id')`, `withCredentials` | an environment's secret, named in the job's `env:` with `value_from` |
| `parameters { string(...) }` | `on.dispatch.inputs`, sent with a run asked for through the run API |
| `triggers { pollSCM(...) }` | nothing to write: dagron-ci looks at a connected repository itself, and takes a webhook |
| `triggers { cron(...) }` | none: see above |
| `when { branch 'main' }` | `on.push.branches` for the pipeline; `when:` for one job |
| `when { changeset 'docs/**' }` | `on.push.paths`. The importer runs the stage, and leaves it to do |
| `options { timeout(time: 30, unit: 'MINUTES') }` | `timeout_secs: 1800` |
| `options { retry(3) }` | `max_attempts: 3`, on a job. It counts attempts |
| `options { disableConcurrentBuilds() }` | `concurrency: { key: ... }` |
| `lock` | `concurrency: { key: ... }`, by hand: the importer leaves it to do |
| `input` | a job of `type: approval`, with `approvers` |
| `post { always { junit '**/*.xml' } }` | `report: { junit: ... }`, of up to eight paths written out: a `*` is not read |
| `post { failure { ... } }` | a job with `trigger_rule: one_failed`, or dagron's `notify` |
| `stash`, `unstash`, `archiveArtifacts` | not built: see above |
| `matrix { axes { ... } }` | `with_items` over the combinations |
| A shared library | none. What several pipelines share is in their images |
| A multibranch pipeline | nothing to set up: a pipeline is read at each commit it runs for, on any branch its `on:` takes |

A build that deploys when someone asks, after a sign-off:

```yaml
# .dagron/ci/build.yaml
name: build
on:
  push: { branches: [main] }
  pull_request: {}
  dispatch:
    inputs:
      channel: { default: stable, description: "where it is deployed to" }
task_defaults:
  docker_image: docker.io/library/maven:3.9-eclipse-temurin-21
  timeout_secs: 1800
tasks:
  - name: build
    max_attempts: 2
    steps:
      - run: mvn --batch-mode --no-transfer-progress verify
  - name: approve
    type: approval
    depends_on: [build]
    when: "{{ ci_event }} == dispatch"
    approvers: ["group:release"]
    approval_message: "Deploy this build?"
  - name: deploy
    depends_on: [build, approve]
    when: "{{ ci_event }} == dispatch"
    environment: production
    env:
      - { name: DEPLOY_KEY, value_from: { secret: deploy_key } }
    steps:
      - env: { CHANNEL: "{{ channel }}" }
        run: ./deploy.sh "$CHANNEL"
```

Two things in it are not Jenkins':

- **A job that its own `when:` skips does not hold back the jobs after
  it.** `deploy` depends on `approve`, and on a push `approve` is skipped:
  without a `when:` of its own, `deploy` would run. A job that a *failed*
  job comes before is skipped.
- **An input reaches a step through its `env:`**, never as text in its
  script.

## From GitLab CI

**A `.gitlab-ci.yml` can run as it is.** Where the operator turns the
front-end on and the project names the file in `.dagron/ci.yaml`, dagron-ci
reads it, with what it includes from the project, and runs it as one
pipeline: [`CI_GITLAB.md`](CI_GITLAB.md) says what is taken of it. What
follows is how one is written as a pipeline of `.dagron/ci` instead, for a
project that moves its CI here for good.

`dagron-ci import` (or `dagron-import gitlab`) writes it under the same
report the GitHub importer writes, with the files the pipeline includes
from the project: from the checkout `dagron-ci import` is run in (or
`dagron-import`'s `--root`, else the file's own directory), and never from
outside it, through a `../` or a link. As GitLab, it reads at most 150 of
them, and none over 2 MiB. The table after the list is the map for a
person doing it by hand. The importer follows it, but where the list says
otherwise, and where a row says *by hand*, which it leaves to do:

```sh
dagron-ci import .gitlab-ci.yml --default-branch main --name pipeline --write
# or, with the importer alone:
dagron-import gitlab .gitlab-ci.yml --default-branch main > .dagron/ci/pipeline.yaml
dagron-ci validate .dagron/ci/pipeline.yaml
```

What it does that a person might not expect:

- **`workflow:rules` is the pipeline's `on:`, and a job's `rules` its
  `when:`**, written for every event at once. A rule on the event's kind,
  a branch, a tag or the branch a merge request merges into is read
  exactly. One on what is not known before a run (`$CI_COMMIT_MESSAGE`,
  `changes:`, a variable of the project's settings, or one of the file's
  in a run asked for, which may set it otherwise) is written to run
  wherever it might have, and left to do. With `workflow:rules`, a job
  without rules of its own is in every pipeline they let be, a merge
  request's too, as on GitLab. A pipeline for schedules alone is a run
  asked for (`dispatch`), which a schedule here asks for through the run
  API.
- **The order is GitLab's**: a job waits for the stages before its own,
  or for its `needs`, and a job left out of a run takes its place in the
  order with it. A job that runs `on_failure` is waited for by none that
  run where the jobs before them succeeded. A manual job that may be left
  unrun holds back none of the stages after it; one that may fail is
  waited for, and its failure fails nothing, as on GitLab: its steps go
  on past one that fails (`continue_on_error`).
- **A job's `before_script` and `script` are one step**, with GitLab's
  variables it reads (`$CI_COMMIT_SHA`, `$CI_COMMIT_REF_NAME`, …) set at
  its head from dagron-ci's own; `after_script` is a step after it.
- **`extends`, `!reference`, anchors and `default:` are written out** in
  each job. `when: manual` is a gate (`type: approval`) before the job, and
  a `docker:dind` service is the job's own daemon (`docker_daemon`).
- **Files a job kept (`artifacts:paths`) are not handed on**: a job that
  took them is left to do.

| In `.gitlab-ci.yml` | In a pipeline |
|---|---|
| `stages`, and a job's `stage` | `depends_on`: there are no stages, and a job says which jobs come before it |
| `needs` | `depends_on` |
| `image` | `docker_image` |
| `script` | `steps`, a step a command or a group of them |
| `before_script` | the job's first step |
| `variables` | `env:` |
| A masked or protected CI/CD variable | an environment's secret, with a policy by ref, by hand |
| `rules`, `only`, `except` on branches, tags and changes | `on:` for the pipeline (`branches`, `tags`; `changes` as `paths`, by hand); `when:` for one job |
| `services` | `services`, reached at their `name` |
| `artifacts: reports: junit` | `report: { junit: ... }` |
| `artifacts: paths`, `dependencies` | none in a pipeline of `.dagron/ci`; a `.gitlab-ci.yml` read as it is has them ([`CI_GITLAB.md`](CI_GITLAB.md#artifacts)) |
| `cache` | `workspace.cache`: `key: files:` as `key_files`, `paths` as `paths` |
| `parallel: matrix` | `with_items` over the combinations |
| `when: manual` | a job of `type: approval` before it; or a pipeline that takes `dispatch` alone |
| `environment` | `environment:`, for its secrets and its policy. There is no page of deployments |
| `timeout` | `timeout_secs` |
| `retry` | `max_attempts`, which counts attempts: `retry: 2` is `max_attempts: 3` |
| `allow_failure: true` | `continue_on_error: true` on its steps: the job ends as a success, and the jobs after it wait for it, as on GitLab. dagron's own `allow_failure: true` would skip them where it failed |
| `allow_failure: { exit_codes: [...] }` | its script in a subshell, ended as a success on those codes |
| `interruptible`, auto-cancel of a redundant pipeline | `concurrency: { key: ..., cancel_superseded: true }` on the pipeline: a newer run of the key takes the older one's place |
| `resource_group` | `concurrency: { key: ... }`, by hand: runs of one key run one at a time, so a job that must not run twice at once is the only job of such a pipeline |
| `include`, `extends`, anchors shared across files | none in a pipeline of `.dagron/ci`: one file is one pipeline, and YAML anchors within a file are YAML's. A `.gitlab-ci.yml` read as it is has `include: local`, `extends` and `!reference` |
| `CI_COMMIT_SHA`, `CI_COMMIT_REF_NAME`, `CI_MERGE_REQUEST_IID` | `CI_SHA`, `CI_REF` (the whole ref, `refs/heads/main`), `CI_PR` |
| A merge request pipeline | `on.pull_request` |
| A scheduled pipeline | a run asked for (`on.dispatch`), which a schedule asks for through the run API |

A test job with a database beside it, and a release on a tag:

```yaml
# .dagron/ci/pipeline.yaml
name: pipeline
on:
  push: { branches: [main], tags: ["v*"] }
  pull_request: {}
task_defaults:
  docker_image: docker.io/library/python:3.12-slim
tasks:
  - name: test
    services:
      - name: db
        image: docker.io/library/postgres:16-alpine
        env:
          - { name: POSTGRES_PASSWORD, value: ci }
        ready: pg_isready -h localhost -U postgres
    steps:
      - run: pip install -r requirements.txt
      - env: { DATABASE_URL: "postgres://postgres:ci@db:5432/postgres" }
        run: pytest --junitxml=junit.xml -o junit_family=xunit1 -o junit_suite_name=pytest
    report:
      junit: junit.xml
  - name: docs
    steps:
      - run: pip install mkdocs && mkdocs build --strict
        continue_on_error: true
  - name: release
    depends_on: [test]
    when: "{{ ci_event }} == tag"
    steps:
      - run: python -m build
```

A push of a tag is the event `tag`, and of a branch `push`: that is what
`CI_EVENT` and `{{ ci_event }}` read.

## What this was checked against

Each pipeline shown here is read by `dagron-ci validate`, compiled for a
push and a pull request, and taken by the engine as a run. Their steps
were not run: they are another project's commands. The rule about `when:`
was seen in a run made for it.

The importers are held to a corpus each (`crates/dagron-ci/tests/import`):
twenty GitHub workflows, eighteen GitLab pipelines and eighteen
Jenkinsfiles, each beside the pipeline it is imported as, and each of
those read by `dagron-ci`'s reader, compiled for every kind of event, and
taken by the engine's parser.

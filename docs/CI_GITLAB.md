# A `.gitlab-ci.yml` on dagron-ci

dagron-ci reads a GitLab project's `.gitlab-ci.yml` and runs it as one
pipeline, its jobs as tasks. This is level 1: the file itself, with the
files it includes from the project. What it takes is listed below, and so
is what it refuses. Nothing in a file is passed over without a word: a key
that is not taken is refused, with what to do instead.

## Turning it on

Three things, one of them the operator's:

1. **The operator** turns the front-end on: `CI_GITLAB_PIPELINES=on`. Off, no
   project's `.gitlab-ci.yml` is read. And the project is in compat mode
   (`dagron-ci repo add|set … --mode compat`): one in native mode, the
   default for a project connected now, is run as its `.dagron/ci` says.
2. **The project** names its file in `.dagron/ci.yaml`. A project that has a
   `.gitlab-ci.yml` may have GitLab running it already, so none runs here
   until the project says so:

   ```yaml
   gitlab:
     pipeline: .gitlab-ci.yml
     # The image a job with no `image:` runs in, by its runner tag.
     tags:
       docker: docker.io/library/alpine:3.20
   ```

3. **The project is connected** as a GitLab one (`dagron-ci repo add
   gitlab.com/group/project --forge gitlab`), with its webhook or the poll
   sweep bringing its events, and a token for its host in
   `CI_GITLAB_TOKENS_DIR` where it is private. See the
   [GitLab section of dagron-ci's README](../crates/dagron-ci/README.md#gitlab).

## What a pipeline becomes

One pipeline, named `gitlab-ci` (a file of another name is named for it:
`ci/main.yml` is `main`). Each job becomes a task of that name, letters,
digits, `.`, `_` and `-` (`build:linux` becomes `build-linux`). A job's
`parallel` makes a task of each instance, `shard.1` to `shard.3`, or one
for each combination of a matrix, its values joined by `-`
(`PROFILE: release` and `TARGET: aarch64` make `build.release-aarch64`).
The variables of a matrix's row are read in the order of their names.

Each job is a commit status, `dagron-ci/gitlab-ci/<task> (<event>)`, the
event being `push`, `tag` or `merge_request`. Each status names its ref: the
branch or the tag, and for a merge request its source branch, where that is
the project's own (a fork's merge request names none). GitLab keeps a
commit's statuses of one ref in one pipeline, an external one where GitLab
ran none, and a merge request shows its source branch's: a run's jobs read
there as a pipeline of their own, which "Pipelines must succeed" waits for.

With `CI_GITLAB_MR_NOTES=on`, a merge request is also given **one note**,
which says how the runs of its latest commit went, and is written again in
place as they change: the note a pull request on GitHub is given as a
comment ([`crates/dagron-ci`](../crates/dagron-ci/README.md#a-pull-requests-comment)).
It is written with the host's token, so it reads as that token's user's.

**When a pipeline is made**, everything GitLab decides before a job runs
is decided: the predefined variables, `workflow:rules`, each job's `rules`,
its stage, `needs` and `parallel`. A job its rules leave out has no task.
**While a job runs**, its scripts are its shell's: a `$NAME` in a script is
expanded by the shell, from the job's variables.

## What is taken

| In `.gitlab-ci.yml` | What it becomes |
|---|---|
| `stages`, a job's `stage` | A job waits for every job of the stages before its own. `.pre` and `.post` are first and last |
| `needs` | It waits for those jobs alone, and fetches their artifacts (`artifacts: false` does not). `optional: true` passes over a job the pipeline does not have |
| `dependencies` | Whose artifacts it fetches, of the stages before its own |
| `script`, `before_script` | One shell: `before_script` then `script`, which stops at the first command that fails |
| `after_script` | A shell of its own, after, whatever became of the script. Its failure fails nothing |
| `image` (or `name:` and the image) | The task's image. Without one, the image its first mapped `tags:` names, else refused |
| `services` (`name`, `alias`, `variables`) | A service, reached at its alias, or a name made of its image (`postgres:16` is `postgres`, `team/db` is `team-db`). It has the job's variables |
| `variables` (and `value:`, `description:`, `expand: false`) | The job's variables, expanded with each other, the job's over the file's; a variable with `expand: false` keeps its text as written |
| `rules` (`if`, `changes`, `when`, `allow_failure`, `variables`) | Read in order; the first that holds decides, and one that says no `when` takes the job's. None holds: the job is not in the pipeline |
| `workflow: rules` | Whether there is a pipeline at all, and variables for every job |
| `when` | `on_success`; `on_failure` and `always` as the trigger rules that say the same; `manual` a job of its own that waits for an approval first; `never`. A job of a stage after an `on_failure` job that did not run runs all the same |
| `allow_failure` | A job that may fail, and fails, ends as succeeded, with a warning that says it failed (not one whose artifacts could not be fetched or uploaded): the stages after it run, and an `on_failure` job does not, as on GitLab. A manual job may fail unless it says otherwise, and the stages after it do not wait for it; one a rule makes manual may not |
| `parallel`, `parallel: matrix` | An instance of each, `CI_NODE_INDEX` and `CI_NODE_TOTAL` set |
| `artifacts` (`paths`, `exclude`, `expire_in`, `when`) | Uploaded when the job is over, as `when` says ([below](#artifacts)) |
| `artifacts: reports: junit` | The job's test report: a file's path, with no wildcard |
| `cache` (`key`, `key: files`, `key: prefix`, `paths`) | A workspace cache: restored before the job, saved after it succeeds |
| `environment` (`name:`) | dagron's environment of that name: its secrets, and its policy |
| `tags` | The image the project maps it to, where the job names none |
| `timeout` | The task's timeout: `1h 30m`, `90 minutes`, `3600` |
| `retry` (`max:` too) | `retry: 2` is three attempts |
| `default` | What every job has unless it says otherwise; the older top-level `image`, `services`, `before_script`, `after_script` and `cache` too |
| `extends` | The jobs and templates named merged under it, as GitLab merges them, eleven deep |
| `!reference [job, key, …]` | What it names, ten deep. A list of them in a script is one script |
| `include: local` (`- path`, `- local: path`) | Each file merged under the one that includes it, at the commit; their own includes too, 150 files at most |
| YAML anchors and `<<:` merge keys | As YAML has them |
| Hidden jobs (`.name`) | Templates: no task of their own |

### Rules

A `rules:if` is GitLab's expression language: a variable (`$NAME`,
`${NAME}`), alone or compared with `==` and `!=` to another, a string or
`null`; `=~` and `!~` against a `/regular expression/`, with `i` after it
for no case, `m`, `s` and `U` as RE2 has them, or a variable holding one;
`&&` before `||`, and parentheses. The regular expressions are RE2's
syntax. A variable that is not set is matched as empty text.

A job **without rules** is in a branch's and a tag's pipeline, and not in a
merge request's, as GitLab has it. To run one in a merge request:

```yaml
test:
  script: [make test]
  rules:
    - if: $CI_PIPELINE_SOURCE == "merge_request_event"
    - if: $CI_COMMIT_BRANCH == $CI_DEFAULT_BRANCH
```

`rules:changes` holds where the event changed a file it names, which
dagron-ci asks the forge for before it makes the pipeline: a push's files
since the commit before it, a merge request's since its target branch. For
a tag, a new branch and a dispatch, as on GitLab, it holds.

### Variables

The variables GitLab predefines that a pipeline reads to decide:
`CI`, `GITLAB_CI`, `CI_PIPELINE_SOURCE` (`push`, `merge_request_event`,
`api` for a run asked for), `CI_COMMIT_SHA`, `CI_COMMIT_SHORT_SHA`,
`CI_COMMIT_BRANCH`, `CI_COMMIT_TAG`, `CI_COMMIT_REF_NAME`,
`CI_COMMIT_REF_SLUG`, `CI_COMMIT_BEFORE_SHA`, `CI_DEFAULT_BRANCH`,
`CI_MERGE_REQUEST_IID`, `CI_MERGE_REQUEST_SOURCE_BRANCH_NAME`,
`CI_MERGE_REQUEST_TARGET_BRANCH_NAME`, `CI_MERGE_REQUEST_DIFF_BASE_SHA`,
`CI_PROJECT_PATH`, `CI_PROJECT_NAME`, `CI_PROJECT_NAMESPACE`,
`CI_PROJECT_URL`, `CI_PROJECT_DIR` (where the job starts),
`CI_SERVER_HOST`, `CI_SERVER_URL`, `CI_PIPELINE_ID`, `CI_PIPELINE_IID`,
`CI_JOB_NAME`, `CI_JOB_STAGE`, `GITLAB_USER_LOGIN`; in `after_script`,
`CI_JOB_STATUS` (`success`, `failed`, `canceled`).

A run asked for through the run API (`POST /ci/v1/runs`) takes its inputs
as the pipeline's variables, over the file's.

A pipeline may not set dagron's own variables, which its steps and the step
runner read: `CI_STEP_*`, `CI_EVENT`, `CI_REF`, `CI_SHA`, `CI_PR`,
`CI_REPORT_*`, `CI_ARTIFACT_*` and their like, `ACTIONS_*` and `DAGRON_*`.

## Artifacts

A job's `artifacts:paths` (a file, a directory and all under it, or a
glob, less what `exclude` names) are packed into a zip when its scripts
are over, where `when` says so of how they went, and uploaded to the
engine's artifact service: the one GitHub's `upload-artifact` speaks to,
so a run's GitLab artifacts are listed and downloaded in the console as a
workflow's are. Each is named for its task, and kept as `expire_in` says,
to the operator's longest (`DAGRON_ACTIONS_ARTIFACT_RETENTION_DAYS`).

Before a job's scripts start, the artifacts of the jobs it waits for (its
`needs`, else every job of the stages before its own, else its
`dependencies`) are unpacked into its workspace.

The service is the operator's to turn on, on the engine:
`DAGRON_ACTIONS_CACHE_LISTEN` and `DAGRON_ACTIONS_CACHE_URL`, over a cache
store that signs URLs (S3, GCS or Azure). A job whose artifacts cannot be
fetched fails before its scripts run; one whose artifacts cannot be
uploaded fails, as on GitLab. A job that uploaded none (it failed, and
uploads on success) has none to fetch: the job after it is told so, and
runs.

## What differs from GitLab

- **One shell, stopped at a failure.** A script runs with `set -e`, and
  `pipefail` where the shell has it, in `bash` where the image has one and
  `sh` where it does not. Each command is printed before it runs.
- **A manual job** is an approval in dagron's console, of the run, before
  the job. The run stays open until it is approved or turned down; one
  that may fail and is turned down fails nothing.
- **A job that may fail, and fails,** is a succeeded task with a warning,
  where GitLab shows a warning beside a failed job.
- **`$CI_PROJECT_DIR` in a variable** (`PIP_CACHE_DIR:
  $CI_PROJECT_DIR/.cache/pip`) is the directory the job starts in, set by
  its shell as the job starts. In a service's variables it is not.
- **Variables are expanded when the pipeline is made**, with each other,
  the job's, and GitLab's predefined ones. One that names a variable none
  of them has (`$PATH`) is left as written.
- **`environment`** is dagron's: its secrets, and the policy that says
  which runs may have them. There is no page of deployments.
- **Artifacts are a zip of regular files**, each with its permissions. A
  link is not packed, nor a file whose name is not UTF-8. A zip is under
  4 GiB and 65,535 files.
- **A matrix's instances** are named, and their variables read, in the
  order of the variables' names.
- **Read, and nothing done with them:** `interruptible`, `coverage`, an
  artifact's `name` and `expose_as`, an environment's `url`, `action` and
  `deployment_tier`, a variable's `options`, and `workflow: name`. dagron
  cancels no run of a
  pipeline for a newer one, and its console names a run's artifacts by
  their jobs.

## What is not taken

`only` and `except` (write them as `rules`); `include` from another
project, a remote URL or a template (level 2); `trigger`, child and
downstream pipelines (level 2); `id_tokens` and `secrets` (level 3);
`rules: exists`, `rules: needs`, `changes: compare_to`; `when: delayed`
and `start_in`; `resource_group`; `inherit`; `release`; `pages`;
`artifacts: untracked`, `public` and reports other than `junit`;
`cache: policy` other than `pull-push`, `cache: when`, `fallback_keys`;
`allow_failure: exit_codes`; `retry: when`; an image's or a service's
`entrypoint`, `command` and `pull_policy`; `workflow: auto_cancel`. A merge train's
pipeline is not made: a train's event runs the `.dagron/ci` pipelines that
take `on.merge_group`, beside the train's own pipeline, which GitLab makes
(the [README](../crates/dagron-ci/README.md#gitlab)).

## Checking a file before it is pushed

```console
$ dagron-ci validate .gitlab-ci.yml
.gitlab-ci.yml: ok (GitLab pipeline: 4 jobs, 1 file included; a push to main runs 4 tasks)
```

It reads the files the pipeline includes from the directory it is run in,
and the project's `.dagron/ci.yaml` for its tags, and makes the pipeline
for a push to `main`, so that a job with no image is said.

# dagron-ci-exec

The step runner of a dagron-ci task. dagron-ci compiles a pipeline's `steps:`
into environment variables, the step protocol below, and a command that runs
them. That command hands the steps to `dagron-ci-exec` where the image has it,
and runs them itself, as a POSIX `sh` script, where it does not. Both write the
same markers, give the same output and exit with the same code (a conformance
test holds them to it); the binary adds what a script cannot do in every image:

- **per-step timeouts** without `timeout(1)` in the image;
- **cancellation**: a cancelled task's signal reaches the running step, which
  is killed if it has not stopped once the grace has passed, and no further
  step starts;
- **cleanup**: what a step leaves running is killed when the task ends;
- **a result**: each step's outcome, as the last log line and in
  `CI_RESULT_FILE`.

It is a static binary (musl; x86_64 and arm64) that needs nothing at run time
but `sh`.

## Getting it into a task

The compiled command looks for it at `/dagron/bin/dagron-ci-exec`, then on
`PATH`, and asks it `--speaks 1` first: a copy that speaks another protocol is
passed over, and the steps run under the fallback instead. (A job that came
from a GitHub workflow asks `--speaks 2`, and has no fallback: see "The step
protocol, version 2".)

- **In a CI image:** release assets carry `dagron-ci-exec-<version>-<target>.tar.gz`
  for `x86_64-unknown-linux-musl` and `aarch64-unknown-linux-musl`. Put the
  binary at `/dagron/bin/dagron-ci-exec` (`COPY dagron-ci-exec /dagron/bin/` in
  the image's Dockerfile).
- **Built here:** `cargo build --release -p dagron-ci-exec --target x86_64-unknown-linux-musl`.
- **On the local executor:** on the engine host's `PATH`.
- **Under `dagron-ci run`:** found beside `dagron-ci` or on `PATH`, it is put
  first on a local task's `PATH`, and mounted read-only at
  `/dagron/bin/dagron-ci-exec` in a Docker one.
- **By an init container**, the usual way on Kubernetes and Docker: set the
  engine's `DAGRON_CI_EXEC_IMAGE` to the `dagron-ci-exec` image
  (`mancube/dagron-ci-exec`, or one built from this crate's `Dockerfile`).
  Every task with a workspace, which is every dagron-ci job, then starts with
  an init container from it that runs `dagron-ci-exec install /dagron/bin`
  into a volume of its own, and the task mounts that volume read-only at
  `/dagron/bin`. The image is the static binary on scratch, run as uid 65532,
  its entrypoint the binary; any image whose entrypoint takes `install DIR`
  will do. On Docker the init container also makes the task's workspace
  volume, which starts as a copy of the image's `/workspace`: this image's is
  open to every user (0777), so that a task whose image runs as any user can
  write in it, and an image of your own needs one too
  (`mkdir -m 0777 /workspace`).

## The step protocol, version 1

| Variable | |
|---|---|
| `CI_STEP_PROTOCOL` | `1` |
| `CI_STEP_COUNT` | how many steps; each `n` from 1 has the variables below |
| `CI_STEP_<n>_RUN` | the step's script, run as `sh -ec SCRIPT` |
| `CI_STEP_<n>_NAME` | its name, for the markers (`step <n>` when unset) |
| `CI_STEP_<n>_TIMEOUT_SECS` | optional: how long it may run, 1 to 604800 |
| `CI_STEP_<n>_CONTINUE_ON_ERROR` | optional: `true` lets the task go on when it fails |
| `CI_STEP_<n>_ENV_<NAME>` | each of its own variables, set as `NAME` for it alone |
| `CI_REPORT_JUNIT`, `CI_REPORT_SARIF` | optional: the reports the steps write, a path a line, from the task's directory ([below](#what-a-job-says-of-itself)) |

Scripts, names and values are values, never command text: nothing in them is
parsed by what runs the steps. dagron-ci sets these itself and refuses `CI_`
names in a pipeline's own `env:`.

dagron-ci-exec also reads two settings of its own:

| Variable | Default | |
|---|---|---|
| `CI_RESULT_FILE` | — (logged only) | Where the result is written, its directories created; written whole, by a rename |
| `CI_STEP_GRACE_SECS` | `10` | How long a step asked to stop (a timeout, a cancelled task) has before SIGKILL, 0 to 600: `timeout -k 10`, as the fallback uses |

### How a step runs

As `sh -ec SCRIPT`, with the task's environment and its own variables on top,
in the task's working directory, with stdin closed, as the leader of its own
process group. Its `cd`, its exports and its variables do not reach the next
step. A step that fails ends the task, unless it sets `continue_on_error`.
Each step is given `CI_STEP_SUMMARY`, the name of a file, one for the
task, which it may add Markdown to.

### Markers

Lines on stderr, the log, beside the steps' own stderr:

| Marker | |
|---|---|
| `::dagron-ci::step::<n>::start::<name>` | a step starts |
| `::dagron-ci::step::<n>::end::<code>` | it ended, with this code |
| `::dagron-ci::step::<n>::continued` | it failed, and `continue_on_error` let the task go on |
| `::dagron-ci::warning::<text>` | something a reader should know: a step whose `sh` could not start; under the fallback, a timeout the image has no `timeout(1)` for |
| `::dagron-ci::cancelled::<signal>` | the task was cancelled by this signal (dagron-ci-exec only) |
| `::dagron-ci::report::<json>`, `::dagron-ci::annotation::<json>`, `::dagron-ci::failed::<json>` | what the task's reports held (dagron-ci-exec only; [below](#what-a-job-says-of-itself)) |
| `::dagron-ci::summary::<json string>` | a piece of what the steps wrote to their summaries (dagron-ci-exec only) |
| `::dagron-ci::result::<json>` | the result, the last line (dagron-ci-exec only) |

A reader ignores the markers it does not know. Markers go to stderr so that a
task's output, its stdout, is what its steps print: `with_output_of` and
`when:` read it as they read any task's. (The Docker and Kubernetes executors
store stderr in a task's output as well; the local executor does not.)

### Exit codes

| Code | |
|---|---|
| `0` | every step succeeded, or failed under `continue_on_error` |
| the step's code | a step failed and ended the task |
| `124` | a step ran past its `timeout_secs` and stopped; `137` when it had to be killed (`timeout(1)`'s codes) |
| `128` + signal | a step a signal ended; or the task was cancelled, `143` for SIGTERM |
| `125` | dagron-ci-exec itself failed: an environment that is not protocol 1, signals it could not catch |
| `127` | a step's `sh` could not start |

### The result

```json
{"protocol": 1, "exit_code": 3, "cancelled_by": null, "steps": [
  {"n": 1, "name": "build", "outcome": "success", "conclusion": "success",
   "exit_code": 0, "started_at_ms": 1791173262341, "duration_ms": 4120},
  {"n": 2, "name": "lint", "outcome": "failure", "conclusion": "success",
   "exit_code": 1, "started_at_ms": 1791173266461, "duration_ms": 950},
  {"n": 3, "name": "test", "outcome": "timed_out", "conclusion": "timed_out",
   "exit_code": 124, "signal": 15, "timeout_secs": 600, "started_at_ms": 1791173267411, "duration_ms": 600031},
  {"n": 4, "name": "publish", "outcome": "skipped", "conclusion": "skipped"}]}
```

`outcome` is what happened: `success`, `failure`, `timed_out`, `cancelled` or
`skipped`. `conclusion` is what it counts as: `success` for a step that failed
under `continue_on_error`, as GitHub reports one.

### What a job says of itself

When its steps are over, whatever became of them, and before the result:

- **Its reports.** Each file `CI_REPORT_JUNIT` and `CI_REPORT_SARIF` name
  is read, up to 32 MiB of it. Then a line of counts,
  `::dagron-ci::report::{"tests", "failures", "errors", "skipped",
  "findings", "more_annotations", "more_failed", "problems"}`; a line for
  each failed test and each SARIF result that names a file under the
  task's directory, `::dagron-ci::annotation::{"path", "start_line",
  "end_line", "level", "title", "message"}`, its level one of `failure`,
  `warning` and `notice`; and a line for each failure by name,
  `::dagron-ci::failed::{"name", "message"}`. At most 50 of each: the
  counts say how many more there were. A report that is not there, is no
  file, or is not what it is called, is one of the `problems`, and changes
  nothing about how the task ends. A task that names no report says none
  of these.
- **Its steps' summaries.** What the steps wrote to `CI_STEP_SUMMARY`
  (under version 2, to each step's `GITHUB_STEP_SUMMARY`, in their
  order), up to 24 KiB, as lines of `::dagron-ci::summary::` and a JSON
  string each, to be put back end to end.

A JUnit test names its place with `file` and `line` on its `<testcase>`,
and a SARIF result with its first location. (pytest counts a line from 0:
in a suite it names `pytest`, which it does unless told otherwise, a line
is read as one more.) A place outside the task's
directory is no annotation: the failure is still counted, and listed by
name.

dagron-ci reads these lines from the job's log when the job is over, and
posts them with its check run
([its README](../dagron-ci/README.md#check-runs)). They are lines of the
log as a step's own are: one a step prints is read as the task's would
be, which is why they say nothing of how the task ended.

## The step protocol, version 2

For the steps of a job that came from a GitHub workflow
([docs/CI_GITHUB.md](../../docs/CI_GITHUB.md)). Version 2 has everything
version 1 has, and these:

| Variable | |
|---|---|
| `CI_STEP_PROTOCOL` | `2` |
| `CI_CONTEXT` | the job's contexts as JSON: `github`, `inputs`, `matrix`, `strategy`, and `env` (the workflow's and the job's variables) |
| `CI_STEP_<n>_ID` | optional: its id, which later steps read its outputs by |
| `CI_STEP_<n>_SHELL` | optional: `bash`, `sh`, `python`, `pwsh`, a command with `{0}` for the script's file, or `auto`, which unset also means: `bash` where the image has one, else `sh` |
| `CI_STEP_<n>_WORKDIR` | optional: where it starts, in the task's directory |
| `CI_STEP_<n>_IF` | optional: its condition |
| `CI_STEP_<n>_ENVX_<NAME>` | a variable whose value holds expressions, evaluated as the step starts |
| `CI_STEP_<n>_EXPR_<k>` | from 1: the expression the script's `__CI_E<k>` variable is set to the value of |

A step's `RUN`, `NAME`, `WORKDIR` and `ENVX` values may hold `${{ … }}`,
GitHub's expressions, which this binary evaluates as the job runs.

Version 3 has everything version 2 has, and what a job is beside its
steps:

| Variable | |
|---|---|
| `CI_STEP_PROTOCOL` | `3` |
| `CI_JOB_IF` | optional: the job's own condition, decided before its first step from the jobs it needs |
| `CI_NEED_COUNT` | how many tasks the job needs; 0 where unset |
| `CI_NEED_<k>_JOB` | from 1: the id of the job that task is, or is an instance of |
| `CI_NEED_<k>_STATUS`, `CI_NEED_<k>_SAID` | the task's status, and the last line it printed. The engine sets both as the job starts (`env_from`) |
| `CI_OUTPUT_COUNT` | how many outputs the job hands on; 0 where unset |
| `CI_OUTPUT_<k>_NAME`, `CI_OUTPUT_<k>_VALUE` | from 1: an output's name, and its value, which may hold `${{ … }}` |

Under it a job writes everything to its standard output, its steps'
standard error and the markers with it, and ends with a line of its own
after the result's: `::dagron-ci::job::{"outputs":{…},"result":"…"}`, the
result one of `success`, `failure`, `cancelled` and `skipped`. That
line is what the jobs after it read. A job whose `CI_JOB_IF` is false, or
that has none where a job it needs did not succeed, runs no step, says
`::dagron-ci::job::skipped`, and exits 0.

An instance of a job whose matrix an earlier job computed is also handed
`CI_MATRIX` and `CI_STRATEGY`, JSON objects that are its `matrix` and
`strategy` contexts: the engine fills them in as it makes the instance,
after `CI_CONTEXT` was written.

`dagron-ci-exec matrix` is what plans such a job's instances. It reads
`CI_CONTEXT`, `CI_JOB_IF` and the `CI_NEED_<k>_…` variables as a job
does, and `CI_MATRIX_PLAN`, the matrix as the workflow writes it:
`{"whole": EXPR}`, or `{"axes": [[NAME, VALUES or {"expr": EXPR}], …],
"include": […], "exclude": […]}`, a row a list of `[KEY, VALUE]` pairs. It
prints a JSON array and nothing else, an element for each instance:
`{"label", "matrix", "strategy", "b"}`, where `b` is two opening braces
(what the instances' own text is written with, so that the engine's
filling of `{{ item… }}` hands it on). A job whose `if:` is false has
none: `[]`. An axis that comes to no value, or to what is no list, is an
error, and exits 1.

- **A step runs** where its `IF` holds, or, with none, while no step
  before it has failed. So a step can run after a failure (`failure()`,
  `always()`); the task still exits with the failing step's code.
- **Its script** is written to a file and run by its shell: `bash
  --noprofile --norc -e -o pipefail FILE`, `sh -e FILE`, or as its shell
  says.
- **It is given** the files `GITHUB_ENV`, `GITHUB_PATH`, `GITHUB_OUTPUT`,
  `GITHUB_STEP_SUMMARY` and `GITHUB_STATE` name, each new for it. What it
  wrote to the first three is read when it ends: a variable for the steps
  after it (but for `GITHUB_*`, `RUNNER_*` and `CI_STEP_*`, which are the
  runner's own), a directory at the front of their `PATH`, and its
  outputs. A file that cannot be read as GitHub writes one fails the step.
  What it wrote to `GITHUB_STEP_SUMMARY` is said when the job is over
  ([above](#what-a-job-says-of-itself)). What a step leaves where one of
  these files was that is no file (a pipe, a directory) is read as
  holding nothing, and is not waited on.
- **A cancelled task** starts no further step, whatever a step's `IF` says.

There is no shell fallback for it: a task that asks for protocol 2 where
no dagron-ci-exec speaks it exits 125 and says so.

### A GitLab job: version 5

Version 5 is version 2, and what is done around a GitLab job's steps. It
is not version 3 or 4, which are a GitHub job's: its steps run as
version 2's. dagron-ci hands a job on in it only where it has one of
these, so a dagron-ci-exec of before it refuses the job rather than run it
without them.

| Variable | |
|---|---|
| `CI_STEP_PROTOCOL` | `5` |
| `CI_ARTIFACT_DOWNLOAD` | optional: the artifacts of the run, by name, one to a line, fetched from the engine's artifact service and unpacked into the workspace before the first step. One its job never uploaded is warned of and passed over; one that cannot be fetched fails the job, which runs no step |
| `CI_ARTIFACT_UPLOAD_NAME`, `_PATHS`, `_EXCLUDE`, `_WHEN`, `_EXPIRE_DAYS` | optional: the job's artifacts, packed into a zip (each file's permissions kept) and uploaded under that name when the steps are over, where `_WHEN` (`on_success`, `on_failure`, `always`) says so of how they went. An upload that fails fails the job |
| `CI_JOB_ALLOW_FAILURE` | optional: `true`, and a job that fails ends as succeeded, with a warning that says it failed |

### A step that uses an action: version 4

Version 4 has everything version 3 has, and a step that uses an action in
place of a script: one of the repository's own, or one of another
repository that the executor fetched before the job started. dagron-ci
hands a job on in it only where one of its steps does, so a job with no
such step runs where an older dagron-ci-exec is installed.

| Variable | |
|---|---|
| `CI_STEP_PROTOCOL` | `4` |
| `CI_STEP_<n>_USES` | optional: the action, as `uses:` names it. `./` and a directory of the workspace with no `..` in it; or `owner/repo[/dir]@ref`, which is read from `$DAGRON_ACTIONS_DIR/<owner>/<repo>/<ref, each / written %2F>[/dir]` (`/dagron/actions` where that is unset). Such a step has no `RUN` |
| `CI_STEP_<n>_WITH_<k>_NAME`, `CI_STEP_<n>_WITH_<k>_VALUE` | from 1: an input the step gives the action, and its value, which may hold `${{ … }}` |

The action is read as the step starts, from `action.yml` in its directory,
or `action.yaml`, as GitHub's runner reads it. A link on the way to that
file is followed, and the file is read only where it resolves to a file of
the workspace, or of the repository the action was fetched from. An action
of another repository that was not fetched fails its step, and says so. A
composite action's steps run in the step's place:

- **Its inputs** are what the step gives it, and the default of each it is
  not given, evaluated as the step's own expressions are. They are its
  steps' `inputs` context. An input it does not declare is warned of, and
  given all the same.
- **Its steps** are `run:` steps, each with a `shell:`, and steps that use
  another local action, ten deep at most. Their `steps` context is their
  own, and `success()` and `failure()` ask how they have gone, not how the
  job has. Each has the step's variables, `GITHUB_ACTION_PATH`, and
  `github.action_path`, and an action of another repository
  `GITHUB_ACTION_REPOSITORY` and `GITHUB_ACTION_REF`. What one writes to
  `GITHUB_ENV` and `GITHUB_PATH` is the job's, as on GitHub. A step of it
  that uses another repository's action fails: what a job fetches is
  decided from the workflow's own steps.
- **Its outputs** are evaluated when its steps are over, against them, and
  are the step's, which later steps read as `steps.<id>.outputs.<name>`.
- **Its time** is the step's `TIMEOUT_SECS`: all of its steps together.
- **Its script's expressions**: one whose value no outsider chooses (a
  matrix value, the commit's id) is pasted, as in a workflow's script, and
  any other is a variable the script reads. An input's value is one an
  outsider may choose, so a script for a shell that is not `sh` or `bash`
  reads an input through its step's `env:`, and one that pastes it fails.

Its steps are no steps of the job's: they have no markers of their own.
Each is a line of the log, `-- ` and its name, between the markers of the
step that uses the action, and what one gets wrong is said as that step's
error. The step's outcome is the action's: a failure where a step of it
failed and did not continue on error.

A JavaScript action (`runs.using: node20`, `node24`, and `node12` and
`node16`, as GitHub now runs them, on a later Node.js) runs its scripts on
the Node.js installed beside this binary (`install --with node`, which
copies the `node` directory beside the image's binary), else on the
`node` on `PATH`. `pre` runs as the step starts, `main` with each input as
`INPUT_<NAME>` and what `pre` kept as `STATE_<name>`, its `GITHUB_OUTPUT`
the step's outputs; `post` runs when the job's steps are over, the last
action's first, with what `main` kept, and one that fails fails the job.
Each has `GITHUB_ACTION`, `GITHUB_EVENT_PATH` and `RUNNER_TOOL_CACHE` too.

An action of a Docker image, a step of an action that uses another
repository's, and an action that is not in the workspace, fail the step,
and say so.

## Command line

```
dagron-ci-exec              run the steps this environment describes
dagron-ci-exec --speaks N   exit 0 if this binary speaks step protocol N
dagron-ci-exec install DIR [--with node]
                            copy this binary to DIR/dagron-ci-exec, and the
                            Node.js beside it to DIR/node where asked
dagron-ci-exec wait-no-network SECS
                            exit 0 once this pod's network is seen to be gone,
                            and 125 if it still has one after SECS seconds
dagron-ci-exec wait-allowlist SECS
                            exit 0 once this pod is seen to be held to its
                            allowlist, and 125 if it is not after SECS seconds
dagron-ci-exec --version
```

`wait-no-network` is no part of the step protocol. The Kubernetes executor
runs it as the last init container of a task that declared
`isolation: { network: none }`, so that the task's container starts only once
its pod reaches nothing. It asks for a connection at the addresses every pod
is given, the cluster's API server and the name servers of the pod's
`/etc/resolv.conf`, all at once, and ends once none has been made at any of
them three times running. Each attempt is given a second, and a round follows
the last by a fifth of one: a few rounds a second where a path refuses, one
where it drops. A refusal is no connection: a CNI that rejects a packet it
will not carry looks exactly so. An attempt that could not be made at all (no
descriptor, no memory) is not silence, and ends the wait as a failure of its
own. It checks those addresses, as far as a connection goes, and cannot tell
silence from an outage; `docs/FAMILIES.md` says what that does and does not
show.

`wait-allowlist` is the same wait for a task that named an allowlist
(`isolation: { network: allow:<name> }`). It asks the API server alone: a
name server is what an allowlist most often allows, so its answer would say
nothing. So an allowlist must not allow the API server, and with one address
asked this is the weaker wait of the two.

## Where it stops

- When the engine's own task timeout fires first, the engine kills the task.
  In a container that ends everything; on the local executor the kill
  (SIGKILL) reaches the runner alone. The running step's shell dies with it
  (its parent-death signal), and that shell's own children may not.
- Cleanup goes by process group. A process a step starts in a session of its
  own survives the task on the local executor.
- Of GitHub's workflow commands, `::add-mask::`, `::error::`,
  `::warning::`, `::notice::`, `::stop-commands::` and `::debug::` are
  read, under step protocol 3, as the job's stream is copied
  (`src/commands.rs`): a `::debug::` line is printed only where
  `ACTIONS_STEP_DEBUG` is `true`. `::group::` is a line of the log, which
  the console folds.
- A local action's `pre:` and `post:` steps, and its `runs.steps[*]`
  `timeout-minutes`, are not run or read: a composite action has neither
  on GitHub either, but for the second, which GitHub does not document.

# CI on dagron

dagron-ci is CI for a team's own code on GitHub, run by dagron. A
repository keeps its pipelines in `.dagron/ci/`. A push, a pull request by
one of the repository's own people, a tag or a call starts them. Each job
runs in a container, on Docker or on Kubernetes, in a workspace with the
commit checked out, and its outcome is a status on the commit. A run can be
run again, and stopped.

It is one service beside the engine and dagron-api, with a database schema
of its own. It keeps no users: who may do what is dagron-api's rule.

| To | Read |
|---|---|
| get a first check green | [`CI_QUICKSTART.md`](CI_QUICKSTART.md) |
| write a pipeline | [`CI_PIPELINES.md`](CI_PIPELINES.md), and the starters in [`examples/ci`](../examples/ci) |
| have an agent review, triage or fix | the recipes in [`examples/ci/agents`](../examples/ci/agents) |
| run a `.github/workflows` file | [`CI_GITHUB.md`](CI_GITHUB.md): what is taken of one, and what is not |
| run a `.gitlab-ci.yml` | [`CI_GITLAB.md`](CI_GITLAB.md): what is taken of one, and what is not |
| move a repository's CI here | [`CI_MIGRATING.md`](CI_MIGRATING.md): from GitHub Actions, Jenkins and GitLab CI, key by key |
| review what it holds | [`CI_SECURITY.md`](CI_SECURITY.md): whose word is taken for what, and what is not held |
| run its jobs | [`CI_RUNNERS.md`](CI_RUNNERS.md): setting up, bounding, watching and repairing what runs them |
| run and operate it | [`crates/dagron-ci/README.md`](../crates/dagron-ci/README.md): every setting, door and metric |

## What it does

- **Takes a repository's events**: a GitHub webhook, or a sweep that looks
  at each repository on a timer and needs nothing to reach it, or both. One
  commit seen both ways runs once.
- **Runs its pipelines** at the event's commit, each as a dagron run: jobs
  in order of what they depend on, each in a container, each with a
  workspace of its own.
- **Checks the commit out** for each job, with a credential minted for that
  attempt and that one repository, which the job's steps never hold, and
  which is revoked after. Each credential has a row that says so.
- **Says how each job went** on the commit: a status a job, pending and
  then its outcome, with a link to the run.
- **Lets a run be acted on**: run again from its failed jobs or whole
  (`dagron-ci rerun ID --all`), stopped, its log read, and asked for by
  hand, through its API or from a terminal.
- **Shows it in the console**: a repository's branches and pull requests,
  each with the runs for its latest commit; a run's jobs as a grid, where a
  job that ran as several instances is a row of them; and a job's log by
  step, with what the job said of its tests above it
  ([in the console](#in-the-console)).
- **Runs GitHub Actions' own jobs on your capacity**, where an operator
  says which: a job whose `runs-on` asks for one of the operator's labels
  is given GitHub's own runner, started as a dagron run for that one job,
  and the repository changes nothing else
  ([fleet mode](../crates/dagron-ci/README.md#fleet-mode)).
- **Runs a checkout's pipelines on your own machine**, before a push:
  `dagron-ci validate`, `compile` and `run`.

## In the console

dagron's console is where a run's page already was. Three things in it are
for CI, and all three read dagron-api: the console's session is
dagron-api's, and a browser does not ask dagron-ci's own API.

- **Repositories**, in the menu, lists each repository CI has run for, the
  one with the latest run first. A repository's page has its pull
  requests, branches and tags, each with the runs for its latest commit:
  the latest run of each pipeline for that commit, whether a push started
  it or someone asked for it by hand. A run that was run again is the same
  run, and reads as it is now.
- **A run's page** has a **Jobs** view beside its graph. A job is a row,
  and a job that ran as several instances (a matrix, a fan-out) is a row
  of them, each a cell that opens its task. The header links the
  repository's page, and the same event's other pipelines.
- **A job's log reads by step**, in its task's drawer: each step with how
  it ended, the failed and running ones open, and the rest a click away.
  Where the job's task has `report:`
  ([`CI_PIPELINES.md`](CI_PIPELINES.md#report)), what its reports held is
  above the log: the counts, each failure by name, and each place a
  failure names. So with what its steps wrote to their summaries. **Raw**
  shows the log as it was written.

What these are read from, and so what they do not show:

- A repository's page is made of the runs' own records. A branch CI has
  not run for is not on it, a pull request's title is not kept, and a
  branch or an author is the name the event gave: text, and no link.
- A job is told from its task's name, `<job>.<instance>`: nothing else
  says which job a task is an instance of. A matrix's values are not
  shown beyond that name.
- A report on a run's page is the job's own word, as on a check run, and
  is held to the same bounds. How a job ended is its task's status.

## What it refuses, and why

dagron-ci refuses what it cannot hold safely, and says so where the person
waiting will see it. A refusal on a commit is a status that never passes.

| It refuses | Because | Where it says so |
|---|---|---|
| **A pull request from a fork, or by anyone who is not the repository's owner, a member of its organization or a collaborator**, unless the operator has such run (`CI_FORK_PRS=untrusted`) | Its code is a stranger's. Run as the team's own it would have the team's secrets, network and cache. Run as untrusted it has none of them, on an engine whose operator gave it a floor for such runs ([`CI_PIPELINES.md`](CI_PIPELINES.md), "Pull requests from forks") | One status on its head commit: "not run: pull requests from forks and from non-members are refused here" |
| **A caller that is an agent** (`agent:<name>`), unless the operator has its runs run (`CI_AGENT_RUNS=untrusted`), and then any event but its own | Its code is a machine's, and asked for in a person's name. Run, it is held to what an untrusted run is held to ([`CI_SECURITY.md`](CI_SECURITY.md#who-may-ask-dagron-ci-for-what)) | Its dispatch is queued as an agent's and skipped, with the reason; a rerun or a cancel is a 403 |
| **A viewer who asks for a run, a rerun or a cancel** | A viewer may read | 403 |
| **A pipeline key that is not one** | dagron would drop it without a word, and the pipeline would not do what it says | `validate`, and a "not run" status on the commit, name the key and the nearest that exists |
| **A job's `env:` written as a map** | A job's is dagron's list of names and values. The map would be left out of the job | `validate`: "is a list of names and values" |
| **A `{{ … }}` in a script that names something an outsider chose** | A branch name pasted into a shell script is the script's author | `validate`; a branch is `$CI_REF`, an input goes through a step's `env:` |
| **A variable named `CI_…` in an `env:`** | Those are dagron-ci's, from the event | `validate` |
| **A dispatch input the pipeline does not declare**, or a required one left out | The pipeline says what it takes | 400, or the event fails with the reason |
| **A cell over plain http**, unless it is its own machine | dagron-ci's token for dagron-api goes there, and each caller's credential | It does not start, and says which cell |
| **A token service over plain http** (the engine), unless it is its own machine | The engine's secret goes there, and every credential comes back | The engine does not start |
| **One checkout secret for two cells** | An engine of either could ask for the credential of the other's run | dagron-ci does not start; the chart does not render |
| **A checkout credential for a run it did not submit**, of a repository it serves | That repository's runs are dagron-ci's to submit: one that is not on its record has no checks and no count | 404 to the engine, and the job fails at its checkout |
| **A checkout that answers with a redirect** | git would send the credential wherever it led | The job fails at its checkout |
| **A network allowlist the engine's operator has not made** (`isolation.network: allow:<name>`) | What a list allows is the operator's: a network of theirs on Docker, a NetworkPolicy of theirs on Kubernetes. A job names one and cannot write one | The job is refused when it is dispatched |
| **A log followed for an event of several runs** (`follow=true`) | A stream is one run's, as its cell streams it: each run is followed by its own id | 400, which names them |
| **A redirect from a server a token is sent to** | The token is for the server that was named | The command, or the engine, says the answer it got |

## What 0.11.1 does not do

Said here so that nobody finds out by trying.

- **Two forges, and GitHub's more than GitLab's.** GitHub, at github.com.
  GitLab, gitlab.com's or your own: its pushes, tags and merge requests
  run a project's `.dagron/ci` pipelines and are given commit statuses,
  and a private project is read with a token the operator keeps for its
  host, and checked out with a token of its own minted for each attempt.
  Not Gitea or GitHub Enterprise Server.
- **Its own format.** A `.github/workflows` file is read at level 1, the
  file itself with no action in it, where the operator and the repository
  both say so ([`CI_GITHUB.md`](CI_GITHUB.md)); and a `.gitlab-ci.yml` at
  level 1, the file and what it includes from the project
  ([`CI_GITLAB.md`](CI_GITLAB.md)).
- **A team's own code, unless the operator says otherwise.** A fork's pull
  request is refused by default. Where an operator has such run, they run
  as untrusted, behind an approval for a first contribution, and that was
  seen on Docker alone.
- **A job keeps caches where the operator gives the engine a store**
  (`workspace.cache`, `DAGRON_CACHE_URL`); elsewhere it starts cold. There is
  no artifact store of CI's own. A job may have services beside it
  (`services:`), where the engine runs jobs on Docker or Kubernetes.
- **Statuses, unless the operator turns check runs on.** With
  `CI_GITHUB_CHECKS=on`, and the App given the permission, a job is a check
  run, GitHub's own "Re-run" reruns its run, and a job's test report and
  its steps' summaries are on its check
  ([how](../crates/dagron-ci/README.md#check-runs)). Without it the button
  does nothing, a rerun is asked of dagron-ci, and a report is lines of
  the job's log.
- **A pull request's comment, where the operator turns it on.** With
  `CI_GITHUB_PR_COMMENT=on`, and the App given the permission, a pull
  request has one comment that says how the runs of its latest commit
  went, written again in place as they change
  ([how](../crates/dagron-ci/README.md#a-pull-requests-comment)).
- **GitHub's buttons rerun a run's failed jobs.** "Re-run" and "Re-run all
  checks" leave a run that succeeded as it is. A run that succeeded is run
  again whole with `?failed=false` or `dagron-ci rerun ID --all`.
- **A log is followed from a terminal and the API, not in the console**
  (`dagron-ci logs --follow`, the logs door's `follow=true`), and it is kept
  up to a cap for each job: in the scheduling database, unless the engine is
  given a store for logs (`DAGRON_LOG_STORE`).
- **`validate` checks dagron-ci's own keys.** dagron's are checked by dagron
  when the run is submitted. `dagron-ci run` finds a mistake in one on your
  machine.
- **On the Helm chart, a private repository is checked out only where
  dagron-ci shares the engine's pod**, which is where the chart puts it.
  Given a Deployment of its own (`ci.placement=deployment`), the engine is
  not told to ask it for the credential.
  [`CI_QUICKSTART.md`](CI_QUICKSTART.md) says why.
- **No figure for capacity or timing** is claimed for it.

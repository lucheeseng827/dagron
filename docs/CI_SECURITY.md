# The security model of dagron-ci

What dagron-ci holds, on whose word, and what it does not hold. It is
written for the person who operates it and the person who reviews it, and
each part points at where the detail is: the settings and doors in
[`crates/dagron-ci/README.md`](../crates/dagron-ci/README.md), the engine's
side in [`HOWTO.md`](HOWTO.md), [`CONFIG.md`](CONFIG.md),
[`FAMILIES.md`](FAMILIES.md) and [`HARDENING.md`](HARDENING.md).

It describes what is built. It is no certification, and it claims nothing
about a deployment: what holds in yours depends on how its engine and its
cluster are set.

## Whose word is taken for what

| Who | Is taken at their word for | And not for |
|---|---|---|
| **GitHub**, by a signed delivery | what happened in a repository, and who did it | anything in a delivery whose signature does not check |
| **The operator**, by dagron-ci's and the engine's settings | which repositories are served, what an untrusted run is held to, which images and classes a fleet label means | |
| **An admin** of dagron-api | which runs may read an environment's secrets, which repositories are connected | |
| **Someone who may submit runs** to dagron-api | what a `trusted` run can read: against them a policy guards a mistake, and is no boundary ([`HOWTO.md`](HOWTO.md), "Who may use an environment") | |
| **A pipeline's author** | what the jobs do | the run's trust class, its limits, where its checkout comes from, how its jobs are said to have ended |
| **A job** | where its tests failed, and what it wants read beside its check | how it ended, which is the engine's to say |
| **Someone outside the repository** | nothing: by default their pull request runs nothing | |

## An event's trust

Every event is recorded with one of three classes, decided at the door from
what GitHub vouches for, after the delivery's signature has checked.

- **`trusted`**: a push or a tag (only someone who can write to the
  repository can push to it), a merge queue's group, a run asked for
  through the run API, and a pull request whose head is the repository's
  own branch **and** whose author GitHub says is its owner, a member of
  its organization or a collaborator.
- **`untrusted`**: any other pull request. One from a fork, one whose fork
  is gone, one by anyone else. A member's pull request from a fork is
  untrusted too.
- **`agent`**: a run asked for by a caller dagron-api knows as an agent,
  whose subject is `agent:<name>` (an agent's token, below). It is refused
  unless the operator sets `CI_AGENT_RUNS=untrusted`. Then it is held to
  what an untrusted run is held to, on a cell whose dagron-api records it
  so, and is never run as anything else.

Who pushed or opened is kept for the record. It never decides trust on its
own, and it never reaches a shell.

**A pull request from outside runs nothing unless the operator says so.**
With `CI_FORK_PRS=refuse`, which is the default, such a pull request's head
commit gets one status, `dagron-ci (pull_request)`, that says it was not
run and why. It is an `error` and never a pass, and nothing of the pull
request is read to post it.

**A pull request's pipeline is its own.** A pipeline is read at the commit
it runs for, so a trusted author's pull request may change what its jobs
do, and may name any environment. What it is then given is the
environment's policy to decide (below): an environment whose secrets a
pull request must not reach is closed to pull requests by `allowed_refs`,
and what needs a secret is best kept in a pipeline that takes no
`pull_request` at all
([`CI_PIPELINES.md`](CI_PIPELINES.md#pull-requests-from-forks)).

## What an untrusted run is held to

An operator who wants outside pull requests run sets `CI_FORK_PRS=untrusted`.
They are then run as `untrusted`, and it is the engine that holds them to
it, going by the run's own record, which dagron-ci writes and the pipeline
cannot.

What has to be in place, and what happens where it is not:

| What | Without it |
|---|---|
| `CI_FORK_PRS=untrusted` | such pull requests are refused, as above |
| Approvers for a first contribution (`CI_FORK_APPROVERS`), unless `CI_FORK_APPROVAL=never` | dagron-ci does not start |
| dagron-ci's token belonging to dagron-api's `ci:trigger` group | the pull request is not run, and its commit says the cell records no run as untrusted |
| A floor for such runs on the engine (`DAGRON_UNTRUSTED_ISOLATION_FLOOR`) | each of the run's jobs fails with the reason |

With a floor set, the engine starts only where it can hold such a task:
never with `EXECUTOR=local`, on Docker only with a jobs network
(`DAGRON_DOCKER_NETWORK`), on Kubernetes only with a network rule in the
floor. An untrusted run then:

- **Reads no secret** except those of an environment whose policy an
  admin opened to `untrusted` runs. It reads none of the engine's own.
- **Is refused outright** if it uses what reaches beyond itself: a
  notification, a dataset, an approval or a call to another workflow, a
  service account, and the rest of the list in [`HOWTO.md`](HOWTO.md),
  "Runs that are not trusted".
- **Is given no cache** of a task's output, no pool but `untrusted`, no
  raised priority, and none of the operator's mounts on Docker.
- **Writes no path cache.** It restores what trusted runs of its own ref and
  of its base branch saved, and is given no URL to save with, so nothing it
  does is read by a later run. Where it restores from is dagron-ci's to say,
  not its pipeline's.
- **Runs inside limits** it cannot turn off (`DAGRON_UNTRUSTED_LIMITS`):
  how long, how many attempts, tasks and services, and on Docker how much
  memory, how many processes and how much CPU.
- **Names only the images** the operator lists, where one is listed
  (`DAGRON_UNTRUSTED_IMAGES`). Without a list, on Docker, it may name any
  image the host already holds.
- **Waits for a person** where it is its author's first contribution: an
  approval that is a run of its own, written by dagron-ci, carrying the
  pull request's number and commit and nothing its author wrote. Each new
  commit is asked for again.

What this does not hold, in the engine's own words: the size of a pod on
Kubernetes, the host's kernel (a container shares it unless the floor names
a sandbox runtime), and who may run such a run again. And it was seen on
Docker alone.

## The repository's code

A job gets its commit from a checkout the engine makes before the job's
steps, with a credential the steps never hold.

- **Minted for one attempt.** Before each attempt the engine asks
  dagron-ci, and dagron-ci mints, as its GitHub App, an installation token
  for that one repository with `contents: read`. GitHub expires it within
  the hour, and dagron-ci revokes it when the attempt is over. A retry
  gets one of its own.
- **Only for what dagron-ci submitted.** It is given for a run dagron-ci
  submitted that is not over, and for that run's own repository and
  commit, so the engine's secret for asking cannot be spent on anything
  else the App may read.
- **Never in the steps' hands.** It reaches git as a header in git's own
  environment, never in a URL or the repository's configuration, over
  HTTPS or to the engine's own machine and no other way, following no
  redirect. On Docker it is in the settings of the container that fetches
  the commit alone, which is seen removed before the job's container
  starts: one that is not fails the attempt, and is left until the task's
  next dispatch, a stop's teardown or the fleet sweep removes it (by hand
  for a task that has ended where the sweep does not run), the
  credential still in its settings: revoked once the attempt is over, in
  the background, or, where that revoke fails, valid until it expires. The
  workspace the steps start in has no credential left in it: a job cannot
  push with it.
- **On record.** Each one has a row (`ci_checkout_credentials`): its run,
  task and attempt, its repository and commit, when it was minted, when it
  expires and when it was revoked, and its digest, never the credential.
  Nothing drops these rows yet.
- **The asking is authenticated.** An engine asks with a secret of its
  cell's, and dagron-ci starts on no arrangement that lets one cell spend
  another's.

Without an App there is no such credential: a public repository is fetched
as anyone would fetch it, and a private one's checkout fails.

## Secrets

- **An environment holds them**, encrypted at rest by dagron-api; a
  pipeline names the environment, and a job names each secret it is to be
  given. A job that names none is given none
  ([`CI_PIPELINES.md`](CI_PIPELINES.md#secrets)).
- **A policy says which runs may read an environment**: by trust class, by
  ref and by pipeline name. Only an admin writes one. An environment with
  no policy admits `trusted` runs and no others. A run that names an
  environment it may not use is not made, and its commit says so.
- **What a job prints of a secret is masked** in its stored output. The
  job itself holds the value, and so does anything the job runs.
- **A service is given no secret**, and none of the job's `CI_*` variables.
- **A person who may submit runs can read what a trusted run can.** A
  policy guards against a mistake there. It is not a boundary against
  them.
- **Where the value can be seen** by someone with access to the machinery
  (a Secret in the task's namespace, `docker inspect`, a process's
  environment) is in [`HOWTO.md`](HOWTO.md), "Where a secret's plaintext
  can be seen".
- **Masking, as GitHub's runner masks.** A value a step names with
  `::add-mask::` is printed `***` from then on, and the job's own
  credentials (its `GITHUB_TOKEN` where it has one, the results service's
  token, the OIDC request token) from the start, wherever in a line they
  are ([CI_GITHUB.md](CI_GITHUB.md#workflow-commands)). It is the log's
  word, not a boundary: a step that prints a value a piece at a time, or
  encoded, prints it. The step runner's own lines are not in the steps'
  stream, so no step forges or cuts one, and a step that reads that
  stream itself takes only the steps' lines from it. The runner is not
  dumpable, so a step running as its user cannot open its descriptors or
  read its memory under `/proc`; a step that is root (`sudo`) can, and is
  held by nothing of the runner's. A job's identity for a cloud is below.

## A job's identity

A job with `identity: { oidc: { audience: … } }` asks dagron-api's issuer
for OpenID Connect tokens with a request credential of its own, and trades
them with a cloud for credentials. It holds no secret of the cloud's: the
cloud trusts the issuer (`DAGRON_OIDC_ISSUER`), and its trust policy says
which runs may assume which role.

- **Who is given one:** a trusted run's job, while it runs. A run that is not
  trusted has the job refused, and the issuer refuses one too. A job whose
  `environment:` its run may not use is refused one, since the token names
  the environment. The request credential is the job's alone, its attempt's
  alone, and nothing is signed for it once the job has ended. Its variable is
  named as a token is (`DAGRON_OIDC_REQUEST_TOKEN`), so the job's log shows
  `***` where a step prints it, and its base64 forms likewise.
- **What it says:** every claim is the run record's, which dagron-ci wrote
  from the verified event, never the pipeline's.

| Claim | |
|---|---|
| `iss` | the issuer's URL |
| `aud` | the audience asked for, one of the job's |
| `sub` | `repo:<host>/<owner>/<name>`, then `:environment:<name>` where the job names one, else `:pull_request` for a pull request, else `:ref:<ref>` |
| `repository`, `forge_host`, `ref`, `sha`, `pr` | the run's repository, forge, ref, commit and pull request |
| `trust` | the run's trust class: always `trusted` in a token |
| `pipeline`, `run_id`, `task`, `attempt` | the pipeline, the engine's run, the job and its attempt |
| `environment`, `actor` | the job's environment and who caused the event, where there are |
| `iat`, `nbf`, `exp`, `jti` | when it was made; it lasts `DAGRON_OIDC_TOKEN_SECS` (600) |

- **A trust policy** names the issuer and the audience, and matches the
  subject: on AWS, a condition on `<issuer host>:sub` such as
  `repo:github.com/acme/widgets:environment:production`; on GCP, an
  attribute mapping of `google.subject=assertion.sub` and a condition on
  `assertion.repository`; on Azure, a federated credential whose subject is
  the `sub` above. Match the environment, or the ref, rather than the
  repository alone: a pull request of the repository's own is trusted too.
- Tokens are RS256, signed with the newest of the issuer's keys; every key is
  published ([`CI_RUNNERS.md`](CI_RUNNERS.md#a-jobs-identity)).

## What a pipeline cannot inject

A branch name, a pull request's title and an input are text someone else
chose. They reach a job as values, never as text in a command.

- **A `{{ … }}` in a script, a command or a `when:` may name only what no
  outsider chose**: the event's kind, its commit, its pull request's
  number, the pipeline's own parameters, a matrix's item. `{{ ci_ref }}`
  there is refused: a step reads `$CI_REF`.
- **Event data is in variables.** `CI_REF`, `CI_SHA` and the rest are set
  for every job, and a name beginning `CI_` is refused in any `env:`, so a
  pipeline cannot stand in for them.
- **A step's script is a value too.** Scripts, names and variables are
  handed to the step runner as variables, and nothing in them is parsed by
  what runs the steps.
- **In a GitHub workflow file**, an expression that reads only what no
  outsider chooses is pasted into a script, as GitHub pastes it. Any other
  becomes a shell variable that holds its value
  ([`CI_GITHUB.md`](CI_GITHUB.md)).

`dagron-ci validate` checks all of this before a push, and the same check
refuses the pipeline when its event arrives.

## dagron-ci's own credentials

Each is a file dagron-ci is given the path of, read once at start. None is
taken from a flag, and none is logged. (`CI_DATABASE_URL` is the one value
that may hold a password, and it is redacted where it is logged.)

| Credential | What it can do | Kept to |
|---|---|---|
| The webhook secret | prove a delivery is GitHub's: an HMAC over the raw body, checked before anything parses it | a delivery with a bad signature writes nothing; each delivery id is taken once, in a table, so a restart forgets none |
| The GitHub App's key | sign a short-lived request for an installation token, and nothing else | |
| A token to read and report with | read a repository's pipelines and pull requests, post commit statuses | one repository; contents and pull requests read, statuses write |
| A token for check runs | post check runs | one repository; `checks: write` and nothing else. The token above is not given the permission |
| A token for a pull request's comment | write the one comment dagron-ci keeps on a pull request, where that is turned on | one repository; `pull requests: write` and nothing else, which is also what would let it edit a pull request's title or request a review: dagron-ci makes the two calls a comment takes with it |
| A token for a checkout | fetch | one repository; `contents: read`; one attempt |
| A token for a private action | fetch an action of the repository's own owner's, in a trusted run | that one repository; `contents: read`; one attempt; only where the repository's Actions access shares its actions |
| A token to ask whether a repository shares its actions | read a private action's repository's visibility and Actions access | that one repository; `metadata: read` and `administration: read`; revoked once it is answered |
| A token for the fleet | register a runner and remove one | one repository; **Administration: Read and write**, which is far more than the two calls made with it |
| A token for an organization's fleet | register an organization's runner and remove one | the installation, and no repository; the organization permission **Self-hosted runners: Read and write** |
| A token for a fleet's jobs | read a workflow run's head and a job's state | one repository; `actions: read` |
| A token to run a fleet's job again | run again a job whose runner died | one repository; `actions: write`, which may also cancel and re-run any of its workflows |
| dagron-api's token (`CI_API_TOKEN_FILE`) | submit runs with a trigger record, rerun them at GitHub's asking, keep a fleet runner's secret | a user of its own in the `ci:trigger` group. dagron-api's tokens are not scoped: it carries its owner's permissions |
| A cell's checkout secret | ask dagron-ci for a checkout's credential | one cell; compared by digest |

A token of the operator's own (`CI_GITHUB_TOKEN_FILE`) stands in where there
is no App. It is sent to github.com alone, and to no other host a
repository names.

A cell is reached over `https`, or over plain `http` on dagron-ci's own
machine and nowhere else.

## Who may ask dagron-ci for what

dagron-ci keeps no users. A caller of its run API presents a dagron-api
credential, and the dagron-api of the repository's cell says who it is.
The credential goes there and nowhere else, and is never stored.

| | A viewer | An agent | Anyone else dagron-api knows |
|---|---|---|---|
| Ask for a run | no | with `CI_AGENT_RUNS=untrusted`, as an `agent` run; else queued as an agent's, and skipped | yes |
| Read a run, or its log | yes | yes | yes |
| Rerun, cancel | no | with `CI_AGENT_RUNS=untrusted`, the events it asked for and no other | yes, as themselves: dagron-api's rule on who may |
| Connect or change a repository | no | no | an admin |

**An agent's token.** A person signed in mints one in dagron-api
(`POST /api/tokens` with `agent: "<name>"`). It presents as
`agent:<name>`, never as the person, with the person's groups less `admin`
and `ci:trigger`. Through dagron-api it reads, and reruns or cancels the
runs recorded as its own, and changes nothing else: no run is made there
in its name, no approval is its, and no token is minted with it. An agent's
name stays its first owner's, so a run recorded as one agent's never passes
to another person's.

An agent's run is confined as an untrusted run is. It has no approval in
it, since a confined run asks no decision of the installation's people, and
it reads an environment only where the environment's policy names `agent`.
A deploy after an agent's change is a person's. Nothing of an agent's
dispatch that did not run is said on the commit, which others build on:
why is on its event, for the agent to read.

- Someone not identified is told nothing: an unknown repository answers as
  an unknown caller does.
- A commit asked for by hand must be on the ref it is asked for on, since
  any commit of a fork can be reached in the repository it forks.
- Each change to a repository's connection is recorded with who made it.
- The engine's checkout door takes its cell's secret, and no person's
  credential.
- `/healthz`, `/readyz` and `/metrics` take none.

A browser asks dagron-ci nothing. The console reads dagron-api, and on
Kubernetes the chart's ingress sends dagron-ci one path, the webhook's.

**On a caller's own machine.** `dagron-ci login` keeps the caller's
dagron-api token for one server, once that server has said whose it is
(`GET /ci/v1/me`), in `~/.config/dagron-ci/credentials.json`: a file of the
user's alone (0600, in a directory that is 0700), which the commands refuse
to read when others may. It is read from a file, a variable or standard
input, never from a command line, and sent to that server alone, which is
`https://` or this machine. `dagron-ci logout` forgets it there; it works
until it is revoked in dagron-api.

## What a job says, and what it cannot say

- **How a job ended is the engine's to say.** A commit status and a check
  run's conclusion are read from the task's status in dagron-api, which a
  job's output is no part of.
- **A job that did not run does not pass.** A skipped job reads as a pass
  only where its run succeeded; where it did not, its check run is
  `cancelled`, since GitHub's `skipped` meets a required check.
- **A passing "skipped" never stands over a run's result.** Two events of
  one commit can share a job's check, as two pull requests with the same
  head do, and one can leave the job out (its `if:` or `when:`, or its
  pipeline's `paths:`) while the other runs it. The run owns the check from
  its first post, and nothing is said skipped under a check a run owns; a
  skipped status goes up only under a lease on the check, which a run waits
  out before its own first post. Whichever order the two are written in,
  what the run says is the later, so a failure is never covered by a pass,
  not even for the seconds until the run posted again.
- **A report is the job's own word.** The lines a job's tests are reported
  in are on a stream its steps write to, so a step can print any of them.
  They are taken for where tests failed and what to read beside the check,
  and held to bounds whatever wrote them: fifty annotations, a path inside
  the repository and no other, a summary of bounded size.
- **A name is not Markdown.** A pull request's comment holds names a pull
  request's author may have chosen: a pipeline's, a job's, a test's. Each
  is written as code, cut to a length, with what would end the code span
  or a table's cell taken out, so none is read as Markdown, mentions a
  person, or links anywhere. How a job ended, in that comment too, is the
  engine's word.
- **GitHub's "Re-run" is taken from GitHub alone**: a signed delivery, of
  dagron-ci's own App's check run, naming a run of that repository, asked
  for by a person. A bot's asking is not taken. What is run again is the
  run as it was submitted, with its commit, its pipeline and its trust: a
  re-run widens nothing.

## Fleet mode

In fleet mode dagron-ci starts GitHub's own runner for a job of GitHub
Actions' own, and GitLab's for a GitLab CI job
([how](../crates/dagron-ci/README.md#fleet-mode)). What that changes:

- **The runner's configuration is a secret.** Whoever holds it is the
  runner, and is handed the job's own secrets by GitHub. It is never in
  the run's spec, its parameters or a task's `env`: it is kept as a secret
  of an environment in the cell's dagron-api while the runner is out, and
  deleted after. Someone who may submit a run to that cell may name that
  environment and that secret meanwhile, as with any environment's.
- **A runner is registered for one repository**, with a token for that
  repository alone, or for its organization with a token for the
  organization's runners alone, takes one job and exits.
- **A fork's job is told apart**, by its workflow run's head (a GitLab
  merge request's job, by its merge request), and is given no runner
  unless its label says. `untrusted: floor` gives it a runner submitted
  as a run that is not trusted: the engine holds it to its floor for such
  runs, gives it no pool, priority or Docker daemon, and resolves its
  configuration from an environment that admits such runs and holds no
  other runner's. A job not yet told apart is taken for a fork's.
- **What is started for a fork's job is not the only runner that may take
  it.** GitHub gives a waiting job to any runner with its labels: a
  trusted runner that is out may be given a fork's job, and dagron-ci
  stops its run when it hears so, as it does where the job could not be
  told apart within thirty seconds. What the job ran in between, it ran. A
  warm runner is idle by design, so a label that tells a fork's job apart
  keeps none warm unless it names a runner group, and dagron-ci does not
  start with one that does. An organization's runner group restricted to selected
  workflows keeps a pull request's job from a trusted label's runners;
  short of that, GitHub's advice against self-hosted runners on a public
  repository whose forks' workflows run without approval holds here.
- **What a label means is the operator's**: its image, its class, its
  pool. No repository says.

## A job's isolation

A job is a container, or a pod, of its own, with a workspace made for the
attempt and removed after it. What separates it from its neighbours and
its host is what the engine was set to ask for.

- **By default little is asked.** A job runs as its image's user, on the
  network the executor gives it, and on Kubernetes without the hardening
  the engine can ask for (a read-only root, dropped capabilities, seccomp,
  a runtime class). One default is on: a job that did not ask for a
  service account's token is given none.
- **An operator sets a floor** no pipeline can go under
  (`DAGRON_TASK_ISOLATION_FLOOR`), and a stricter one for untrusted runs.
  What cannot be held where the engine runs is refused, never ignored.
- **A network of none, or of a named allowlist,** on Kubernetes needs
  the cluster's network policies to be enforced, and has a gate in the job
  that waits until they are. Both were run on k3s alone. On Docker an
  allowlist is an internal network of the operator's that the host has no
  address on, read before each job, with only what they put on it; without a jobs network every other
  container is on the daemon's default one, beside every other on the host.
- **A list by hostname is a proxy of the operator's**, which the list's
  network or policy names and the job is told of (`HTTP_PROXY` and the
  rest). What it lets through is the proxy's; dagron-ci runs none.
- **A sandbox runtime** (a runtime class, such as gVisor's) is Kubernetes'
  alone.

What is not claimed:

- A container on Docker is what the daemon was asked for, and not a
  sandbox: it shares its host's kernel.
- The local executor holds nothing: a job there runs as the engine does.
- Task pods share the engine's namespace and are not isolated from one
  another by the chart, whose network policies say nothing of egress.
- The engine's own API takes no credential. dagron-ci in the engine's pod
  reaches it there, which no network policy closes inside a pod.
- The quickstart's compose file is a way to meet dagron-ci and not a
  deployment: its credentials are in the file, and its engine holds the
  container runtime's socket.

## What is not built

Each is said where it is met. Together:

- **Forges other than github.com and GitLab**: credentials, statuses and
  checkouts are GitHub's and GitLab's. The token the operator keeps for a
  GitLab host is long-lived and reaches every project it can, so it goes
  to no job: a checkout is given a project access token of its own, for
  the one project, to read it, revoked once the attempt is over. GitLab
  dates its expiry by the day, so one whose revoke failed lives up to two
  days, where GitHub's installation token lives an hour.
- **Untrusted runs anywhere but Docker** have not been seen, and the
  `agent` class is refused unless an operator has it run.
- **An approval** that an agent's run waits on before a deploy. (A run of a
  patch that was never pushed, a patch bundle, is untrusted whoever asks,
  is its uploader's alone, and tells the forge nothing: dagron-ci's README,
  patch bundles.)
- **Provenance** for what a job builds, and an issuer's keys in a KMS.

- **A neutral result.** A refused pull request reads `error`, since a
  commit status has no neutral state.
- **A fork's job held from every trusted runner in fleet mode**, short of an
  organization's runner group restricted to selected workflows: what
  dagron-ci starts for one is held to the floor, and what GitHub gives
  one is stopped once it is heard of, which is after it started.
- **Scoped tokens in dagron-api**, and a boundary between people who may
  all submit runs to one cell.
- **Limits on asking**: dagron-ci bounds a request's size and keeps no
  count of requests.
- **Rotation without a restart.** Each credential is read once, at start.
- **Dropping old rows**: events, their payloads (kept up to 64 KiB each)
  and credential records are kept.
- **An audit trail of its own** beyond a repository's changes and the
  credential records. An approval and a rerun are dagron-api's to record.

## Reporting a vulnerability

[`SECURITY.md`](../SECURITY.md) says how.

# Agents in CI: four recipes

Four ways an agent takes part in a repository's CI with what dagron-ci
has today:

- **reviewing** a pull request;
- **triaging** a failure;
- **fixing** one, from your own agent;
- **fixing** one in CI, up to a maintainer's approval of the patch.

Each says what it rests on, and what is not built yet. None of them gives
an agent more than the person or the run it acts for.

| Recipe | What it is | Where it runs |
|---|---|---|
| [Review](#review-a-pull-request) | a job that asks a model about a pull request's diff and annotates the lines it finds fault with | a pipeline of the repository's, [`review.yaml`](review.yaml) |
| [Triage](#triage-a-failure) | each failure's class, which decides whether it is run again, and which an agent reads | the engine, and the run's failure summary |
| [Fix on failure](#fix-a-failure-from-your-agent) | your coding agent runs the pipelines on its change, reads what failed, fixes it and runs them again | your agent, through dagron-mcp, as itself |
| [Fix in CI](#fix-a-failure-in-ci) | a pipeline that runs when `ci` fails on main: a model's patch, run as an agent until it passes, approved by its exact bytes, then a pull request | a pipeline of the repository's, [`fix.yaml`](fix.yaml) |

## Review a pull request

[`review.yaml`](review.yaml) is a pipeline that runs one job on a pull
request:

1. **The diff.** The job takes the pull request's diff, from where it left
   its base to its head.
2. **The question.** It sends the diff to a model, and asks for the defects
   in the lines the pull request adds. A finding is kept only where it names
   one of those lines.
3. **The answer.** It writes what comes back as SARIF, which `report:` reads.
   - Where the repository's jobs are check runs, each finding is an
     annotation on its line of the commit.
   - The job's log and its summary say how many there were.

The review is advisory. Its findings never fail the job, so a branch that
requires the check is not held by a model's opinion. A model that cannot
be asked does fail the job, so a review that never happened does not read
as a clean one.

**The model** is any endpoint that answers OpenAI's chat completions API: a
hosted one, a gateway in front of one, or one served on your own machines.
The pipeline names none. An admin makes an environment that says where it
is:

```console
curl -sS -b cookies.txt -X POST localhost:8080/api/environments \
  -H 'content-type: application/json' \
  -d '{"name":"agent-review","variables":{"MODEL_URL":"https://models.example.com/v1","MODEL":"reviewer-large"}}'
# -> {"id":"<env-id>", ...}
curl -sS -b cookies.txt -X PUT localhost:8080/api/environments/<env-id>/secrets/model_api_key \
  -H 'content-type: application/json' -d '{"value":"<the endpoint key>"}'
```

Then copy the pipeline, and check it:

```sh
cp path/to/dagron/examples/ci/agents/review.yaml .dagron/ci/agent-review.yaml
dagron-ci validate
```

**Who is given the key** is the environment's policy's to say. With none,
it is trusted runs alone: a member's pull request from a branch of the
repository is reviewed. A pull request from a fork, where an operator has
such run at all (`CI_FORK_PRS=untrusted`), is refused the environment, so
no review runs for it, and its commit says which environment refused it.
An untrusted job never holds a provider's key. To review such pull requests
too, an admin would open the environment to `untrusted` runs: the key is
then within reach of code the pull request's author wrote, which is the
reason it is not by default.

**What the model reads is the author's text.** The job tells the model to
read the diff as data and never as instructions to it, and keeps a finding
only where it names a line the pull request adds. A pull
request can still try to talk a model out of a finding. Read a review as
what one more reviewer said, and not as a gate.

The job sends at most 60000 bytes of diff, and says so when it cut one.
Change `LIMIT` in the script for a model that reads more, or less.

## Triage a failure

**The engine classifies each failure** from what the failed attempt
printed: `user-code`, `config`, `storage`, `preemption`, `node-fail`, a
timeout, and more ([`HPC_AUTOPSY.md`](../../../docs/HPC_AUTOPSY.md) lists
them). **The class decides how many attempts the job is given**, so a
runner that ran out of disk is tried again and a failing test is not:

```yaml
task_defaults:
  max_attempts: 1           # a failure the engine cannot place: once
  retry_budgets:            # by what broke, whatever max_attempts says
    storage: 3              # a full disk, a stale mount
    preemption: 3           # a spot machine taken back
    node-fail: 3            # the machine under the job went away
    user-code: 1            # the job's own code: running it again proves nothing
    config: 1               # a missing variable or file
```

A class the pipeline does not name has its disposition's default: five
attempts for infrastructure, three for platform, one for application. A run
that is not trusted is given no `retry_budgets`, and has its `max_attempts`.

**An agent reads the class** on the run's failure summary, which every
run's detail carries (`GET /api/runs/{id}`, and dagron-mcp's
`dagron_get_run`):

```json
"failure": {
  "task_name": "test", "attempt": 1, "failed_tasks": 1,
  "message": "…the last 20 lines of what the task printed…",
  "fault_class": "user-code", "disposition": "application"
}
```

- **`disposition`** is the field to act on. `infrastructure` is worth
  another attempt elsewhere. `application` is the code's, and another
  attempt reproduces it: read `message`. `platform` is a question of policy
  (the deadline, the price). `unknown` says nothing either way.
- **What breaks most** is counted by class in the engine's
  `scheduler_task_faults_total`.

**A job that keeps flaking is filed an issue.** Where its operator sets
`CI_FLAKE_ISSUES`, a job of a trusted run that passed only after an
attempt of it failed is kept, and one that does so `CI_FLAKE_ISSUES` times
within `CI_FLAKE_DAYS` is filed one issue of it, written again in place as it
flakes again, for a person (or an agent) to take up
([dagron-ci's README](../../../crates/dagron-ci/README.md#a-flaky-jobs-issue)).

**Not built:** a history of a test's flakes across runs, rather than a
job's; an issue for a failure that comes back without a pass between.

## Fix a failure, from your agent

A coding agent drives the pipelines itself, through dagron-mcp's CI tools,
with a token of its own:

1. **A token for the agent.** Mint one in dagron-api, signed in as yourself,
   with the agent's name. It presents as `agent:<name>`, not as you, with
   your groups less `admin` and `ci:trigger`.

   ```console
   curl -sS -b cookies.txt -X POST localhost:8080/api/tokens \
     -H 'content-type: application/json' \
     -d '{"name":"fixer","expires_in_days":7,"agent":"fixer"}'
   ```

2. **dagron-mcp, with dagron-ci's address.** The agent's MCP client runs it
   with that token:

   ```json
   {
     "mcpServers": {
       "dagron": {
         "command": "dagron-mcp",
         "env": {
           "DAGRON_API_URL": "https://dagron-api.example.com",
           "DAGRON_CI_URL": "https://dagron-ci.example.com",
           "DAGRON_MCP_TOKEN": "<the agent's token>"
         }
       }
     }
   }
   ```

3. **The loop.**
   1. The agent runs the pipelines on its change without pushing it
      (`dagron_ci_run` with `bundle_from`: its working tree, as a patch
      bundle laid over the commit it started from). Or it pushes the change
      to a branch of its own, one that no pipeline's `push:` names (the
      starters' name `main` alone), and runs them there. A push is the
      pusher's, as the forge says who that is.
   2. It waits for them (`dagron_ci_wait`), which hands it the first failed
      job and its last lines.
   3. It reads more where it needs to (`dagron_ci_logs`, and
      `dagron_get_run` for the failure's `disposition`).
   4. It fixes what failed, and runs them again.

   An infrastructure failure it runs again (`dagron_ci_rerun_failed`), as
   is.
4. **A person merges.** The agent opens a pull request with what passed,
   and a maintainer reviews and merges it as anyone else's.

What holds it:

- **dagron-ci runs an agent's work only where its operator says so.**
  - With `CI_AGENT_RUNS=untrusted`, the agent's runs are `agent` runs,
    confined as a fork's are: the engine's floor and limits, no approval of
    its own, no cache written, and an environment's secrets only where its
    policy names `agent`.
  - With the default, `refuse`, they are queued and skipped, saying why.
- **An environment's policy gives it more, there alone.** An identity
  (OIDC tokens for a cloud) where the policy says `"allowed_identity":
  ["agent"]`, its tokens saying `"trust": "agent"`. And a protected
  environment, `"approvers": ["group:release"]`: each task of the agent's
  run that names it waits on a gate of those people first, which the
  engine puts in as the run is made ([HOWTO
  §6](../../../docs/HOWTO.md#who-may-use-an-environment)).
- **Each agent can be held to so many runs at once.** dagron-ci's
  admission counts a run for its caller, and the enterprise build holds
  each `agent:<name>` to `CI_AGENT_MAX_RUNS`: an agent at its cap waits for
  one of its own runs to end, and holds back no one else's.
- **The agent changes only its own runs.** It may rerun and cancel the
  runs it asked for, and nothing else of anybody's.
- **dagron-api refuses its other changes.** It makes no run there, gives
  no approval, and mints no token.

- **A bundle's run tells the forge nothing.** Its base is a commit the
  change is not, so no status or check is posted for it; the agent follows
  it through dagron-ci, and `GET /ci/v1/runs/{id}` says the bundle's
  SHA-256.

## Fix a failure, in CI

[`fix.yaml`](fix.yaml) is a pipeline that runs when a run of `ci` fails on
main (`on: { failed: { pipelines: [ci], branches: [main] } }`), on the
commit that failed. Its three jobs:

1. **`fix`, the loop.** One turn an iteration (`repeat:`, up to four):
   1. It asks a model for a patch, showing it what the failed jobs printed,
      the files they name and the repository's files, and on a later turn
      its last patch and what the run of it said.
   2. It applies the patch to the commit. A patch that does not apply, or
      changes `.dagron/` or `.github/`, is refused, and the turn fails.
   3. It runs `ci` on the commit with the patch laid over it, as the agent
      (a patch bundle, `POST /ci/v1/bundles`, then `POST /ci/v1/runs` with
      `bundle`), and waits for it.
   4. A run that passes ends the loop, and the patch, as git has it, is
      kept as `fix/patch.diff`. One that fails is the next turn's to fix.

   Each turn is kept in the run's artifacts, with its bundle and its run,
   so a turn the engine stopped is taken up where it was, and
   `fix/turns.txt` says how each went.
2. **`review`, the approval.** A maintainer (`approvers:
   [group:maintainers]`) reads the patch and the turns, and approves the
   patch's exact bytes (`binds: [fix/patch.diff]`): approving needs their
   SHA-256, and a patch changed after it was read cannot be approved. No
   one answers in three days, and it is rejected.
3. **`open-pr`, the change.** It checks the approved digest
   (`review/approved.sha256`), applies the patch, pushes it to a branch of
   its own (`agent-fix/<the failed run>`) and opens a pull request of it,
   with a token for the repository that pushes and opens pull requests
   (`forge_token`). A person reviews and merges it as anyone's.

**What it needs.**

- **An agent's token**, minted as above, and kept as the secret
  `agent_token` of an environment `agent-fix`, with the model's variables
  and key as the review's environment has them, and `DAGRON_CI_URL`, where a
  job reaches dagron-ci's run API. With no policy, the environment is a
  trusted run's alone, which a run of `on: failed` is: only a trusted run's
  failure raises one.

  ```console
  curl -sS -b cookies.txt -X POST localhost:8080/api/environments \
    -H 'content-type: application/json' \
    -d '{"name":"agent-fix","variables":{"MODEL_URL":"https://models.example.com/v1","MODEL":"fixer-large","DAGRON_CI_URL":"https://dagron-ci.example.com"}}'
  curl -sS -b cookies.txt -X PUT localhost:8080/api/environments/<env-id>/secrets/model_api_key \
    -H 'content-type: application/json' -d '{"value":"<the endpoint key>"}'
  curl -sS -b cookies.txt -X PUT localhost:8080/api/environments/<env-id>/secrets/agent_token \
    -H 'content-type: application/json' -d '{"value":"<the agent token>"}'
  ```
- **The agent's runs run** (`CI_AGENT_RUNS=untrusted`), and bundles are
  taken (CI_OBJECT_STORE_URL, CI_URL_FOR_JOBS and the checkout credential
  door's secret).
- **`ci` takes dispatch** (`dispatch: {}` in its `on:`): the agent's run of
  its patch is a dispatch of `ci` alone.
- **dagron's artifact store, which the jobs reach over dagron-api's API.**
  The turns and the patch the approval binds are kept in the run's
  artifacts, in dagron-api's store (`DAGRON_ARTIFACT_DIR`), which `fix` and
  `open-pr` reach from their containers (`artifact_api: true`) where the
  engine's operator sets `DAGRON_ARTIFACT_API_URL`, dagron-api's
  `/task/v1/artifacts` as the jobs reach it. `fix` writes under `fix/`
  alone, and `open-pr` reads the digests the approval published and the
  patch they name. `binds` is checked by dagron-api's Postgres build.
- **A token service that gives a trusted run's job a token that pushes**
  (`contents: write`, `pull_requests: write`), as dagron-ci's does.

**What holds it.**

- **The model never holds more than the turn.** The patch it writes is
  applied in the `fix` job, and never run there: it is run in the agent's
  run, confined as a fork's pull request is, with no secret of the
  environment and no token that writes.
- **The agent's token is the agent's.** It runs its own runs, reads, and
  reruns and cancels what it ran; it approves nothing and pushes nothing.
- **A person approves the bytes.** The gate binds the patch, and `open-pr`
  checks the digest before it applies it: what is pushed is what was read.
- **A person merges.** The pull request is reviewed as anyone's, and its
  own CI runs on it.

**The model through eggrd, with no key in the environment.** The `fix`
job need hold no model key: with `identity` uncommented in `fix.yaml`, it
asks for its run's OIDC token for eggrd's audience at each turn, and sends
that. eggrd checks it against this installation's issuer, injects the
provider key whose entry the token's claims match, and caps each run with a
budget of its own (`claim:run_id`). A hijacked job can spend its run's
budget, while its token lasts, and cannot take the provider key.

```toml
# eggrd.toml
[auth]
mode = "jwt"

[auth.jwt]
algorithm = "RS256"
jwks_url = "https://dagron.example.com/.well-known/jwks"   # DAGRON_OIDC_ISSUER's
issuer = "https://dagron.example.com"
audience = "eggrd"

[[llm.keys]]
claims = { trust = ["trusted"], repository = ["acme/*"], pipeline = ["agent-fix"] }
provider_key = "sk-real-..."
allowed_models = ["fixer-large"]

[[llm.budgets]]
name = "per-run"
scope = "claim:run_id"
unit = "tokens"
limit = 200000
window = "24h"
```

The environment's `MODEL_URL` is then eggrd's (`https://eggrd.example.com/v1`),
and its `model_api_key` is left out. A run of `on: failed` is trusted, so its
task is given a token with no policy; an agent's own run is given one only
where its environment's policy has `allowed_identity: ["agent"]`.

**What each turn spent.** Each turn of `fix` prints what it spent of the
model's tokens, as its API counted them, on a line of its own
(`dagron::tokens=<n>`), which the engine takes out of the output before
`until` reads it. dagron's enterprise build keeps it, per run and for the
runs above it, and holds a run to a `budget: { tokens }` at dispatch
([AI_WORKLOADS.md](../../../docs/AI_WORKLOADS.md#what-a-model-call-spent)).

**Not built:** a budget in a pipeline of `.dagron/ci`. Its jobs may say
`tokens:`, and a pipeline's run is metered, but it is held by eggrd's budget
above, not by one of dagron's.

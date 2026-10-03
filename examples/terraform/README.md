# Terraform reference workflows

Six workflows, each a complete Dagron spec you copy and adapt. All of them assume
`terraform` is on the worker's PATH, the root module lives in `tf_dir`, and every task
shares one filesystem (a single worker or a shared volume). `tflint` and `checkov` are
the lint and compliance steps; swap in your own. `lifecycle.yaml` and `promote_environments.yaml` also need `jq`
(their destroy guards read the plan's JSON).

| File | Use it for | Gate | Changes infrastructure |
|---|---|---|---|
| `pr_check.yaml` | every pull request: fmt, validate, lint, compliance, speculative plan | none | no |
| `plan_review_apply.yaml` | one environment: checks, plan, human review, apply the reviewed plan | 1 | yes |
| `promote_environments.yaml` | dev, then staging, then prod, gated after dev; each stage is one call of a `stage` template | 1 per stage after dev | yes |
| `lifecycle.yaml` | the whole life of an environment in one run: plan, approve, apply, then plan the teardown, approve, destroy; both steps are one call of a `stage` template | 2 | yes (creates, then destroys) |
| `plan_destroy_review.yaml` | tear an environment down, reviewed | 1 | yes (destroys) |
| `drift_check.yaml` | scheduled: has the real world moved away from state? | none | no |

## Standard conditions and where each is expressed

| Condition | How | Where |
|---|---|---|
| Formatting, syntax, lint, policy must pass before a plan | plan depends on all four; they run in parallel, so one run reports every problem | all |
| Nothing to change: skip review and apply, still succeed | `plan -detailed-exitcode` (0 none, 2 changes, 1 error) writes a marker; a tiny `plan_result` (`changed` in the stage template) task echoes `changes`/`no_changes`; the gate has `when: "{{ tasks.plan_result.output }} == changes"` | `plan_review_apply`, `plan_destroy_review`, `promote_environments` |
| Apply exactly what was reviewed | plan saved with `-out`; `binds` pins the approval to the sha256 of the plan and its text; `apply` runs `sha256sum -c review/approved.sha256` and applies the saved plan, never a fresh one | apply and destroy flows |
| Human approval, with the plan in front of them | `type: approval`, `approval_message`, `approval_show: [plan/plan.txt]` | apply and destroy flows |
| Who may approve | `approvers: ["group:release"]`, `not_triggerer: true` (gateway-enforced; commented out until your login carries the group) | apply flows |
| Nobody answers | `approval_timeout_secs` with `approval_on_timeout: reject` | apply flows |
| Stand up, prove, and tear down in one run | `deploy` and `teardown` are two calls of one `stage` template; the teardown plans with `-destroy` after the apply finished and has its own gate, so the run stops twice. `destroy_after: "false"` removes the teardown call at expansion. Each stage ends in `done`, which runs only when the stage applied or had nothing to apply, and the teardown plans only after the deploy's `done`: rejecting the apply, or a failed check or plan, skips the teardown | `lifecycle` |
| Refuse destructive plans unless asked | plan step counts resource `delete` actions in `terraform show -json` (with `jq`), fails unless the run has `allow_destroy: "true"` | `promote_environments` |
| Stop part-way through a promotion | the staging call has `when: "{{ promote_through }} in [staging, prod]"` and the prod call `when: "{{ promote_through }} == prod"` (dev always runs); a parameter-only condition is decided when the run is created, so a stage outside it never exists | `promote_environments` |
| One stage ungated, the rest gated, from one template | the review's `when: "{{ gated }} == true and {{ tasks.changed.output }} == changes"`: the parameter part is decided at creation (no review task when `gated` is false), the output part at runtime | `promote_environments` |
| A skipped stage must not skip the next | `trigger_rule: none_failed` on the template's `plan` | `promote_environments` |
| Only valid inputs | `param_schema` (enums and patterns, checked before any task exists; `tf_dir` is spliced into shell so its pattern is a security control) | all |
| One run per stack at a time | `max_active_runs: 1` with `concurrency_key` naming the stack (or PR) | all |
| State lock held by someone else | `-lock-timeout=5m` on plan and apply; a check that must not block a deploy uses `-lock=false` | promotion, PR, drift |
| Flaky network at init | `max_attempts: 2`, `retry_delay_secs` on `init` only; plan and apply are never retried | all |
| Runaway runs | `timeout_secs` per task, `run_timeout_secs` for the run | all |
| Tell someone it failed | `hook: on_failure` task posting to `$NOTIFY_WEBHOOK_URL`, `allow_failure` so the alert cannot mask the real failure | promotion, drift |
| Drift is an alert, not a change | `plan -refresh-only`; exit 2 fails the run on purpose | `drift_check` |
| The log is the whole output | the run log keeps stdout and stderr; `exec 2>&1` at the top of each step keeps them in terraform's order, `-no-color` drops escape codes | all |

## Running them

- Submit from the console (Workflows, New run) or `POST /api/runs`; set parameters there.
- Schedule `drift_check` from the Schedules drawer, e.g. `0 0 6 * * *` (cron is
  `sec min hour dom mon dow`).
- Trigger `pr_check` from CI with `tf_dir`, `environment` and `pr` set.
- Approvers open **Approvals**, read the plan through the link, and approve or reject; the
  decision, who made it and the comment are recorded on the gate.

## What they do not do

- They do not pick a Terraform version, configure a backend, or authenticate to a cloud.
  Put those in the worker image or environment.
- A saved plan applies only to the state it was made from: if something else changes the
  stack between plan and approval, Terraform refuses the stale plan and the apply fails.
  That is the safe outcome; rerun.
- `approvers`, `not_triggerer` and `binds` are enforced by the `dagron-api` gateway. The
  engine API on port 8787 has no caller identity, so it answers 403 instead: to both
  approve and reject on a gate with `approvers`, and to approve on a gate with
  `not_triggerer` or `binds` (rejecting those still works there).

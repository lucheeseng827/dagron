# Infrastructure as code, from a template library

`examples/terraform/` shows what a reviewed Terraform workflow looks like, written
out in full: a `promote_environments.yaml` of about 200 lines, almost all of it a
`templates:` block and five check tasks that the next Terraform workflow would copy
again. This directory is the same thing with the repeated part moved into a
**template library** and the workflow reduced to what is particular to it.

| File | What it is |
|------|------------|
| `iac-library.yaml` | The library. Templates `tf_checks`, `tf_stage` (Terraform **or OpenTofu**) and `pulumi_stage`. Not a workflow you run. |
| `promote_terraform.yaml` | dev → staging → prod. `tool=tofu` switches every command to OpenTofu. 74 lines. |
| `promote_pulumi.yaml` | The same promotion for Pulumi stacks. 49 lines. |

## Use it

```bash
# 1. Save the library once, under the name its spec declares (`iac-library`).
curl -X POST $DAGRON/api/workflows -H 'Content-Type: application/json' \
  -d "$(jq -n --rawfile spec examples/iac/iac-library.yaml '{spec: $spec}')"

# 2. Save and run workflows that import it.
curl -X POST $DAGRON/api/workflows -H 'Content-Type: application/json' \
  -d "$(jq -n --rawfile spec examples/iac/promote_terraform.yaml '{spec: $spec}')"
```

The console's workflow editor takes the same two YAML files (the **YAML** view).

## How `use:` works

```yaml
use:
  - iac-library               # every template the library declares
  - ci-library/build          # one template, plus any template it calls
tasks:
  - { name: prod, template: tf_stage, arguments: { env: prod, tf_dir: ./infra } }
```

- **A library is any saved workflow that declares `templates:`.** `tasks: []` is
  required because every workflow has a `tasks:` key; a library has none. Tag it
  `library` so it is easy to tell from workflows people run.
- **`use:` appends the library's templates to the spec's own `templates:`**, and
  the engine never sees the key. `template:` calls and `arguments:` then work
  exactly as for a template written in the spec, including `when:`,
  `depends_on` and fan-out on the call.
- **By name, as saved now.** Editing the library changes every workflow that
  imports it on its next run, the same as `workflow_ref`. To pin a workflow to a
  fixed copy, save that copy under a new name (`iac-library-2026-10`) and import it.
- **Name clashes are an error**, not a silent override: a template defined both in
  the spec and in a library, or in two libraries, is refused with both names. Import
  `library/template` to take only what you need.
- **One level.** A library may not itself `use:` another; import both from the
  workflow. A workflow chained with `workflow_ref` may not declare `use:` for the
  same reason it may not declare `templates:`.
- **Checked on save.** A missing library, a missing template or a clash is a `400`
  when the workflow is saved, not at the first run.
- **Resolved by `dagron-api`.** Paths that parse a spec without the gateway (a bare
  engine, a file ingest) refuse an unresolved `use:` with a message saying so.

## What the stage templates promise

`tf_stage` and `pulumi_stage` share a contract; the details are in the header of
`iac-library.yaml`. In short: name the call after the environment; `gated: "false"`
removes the review; a plan that deletes or replaces anything fails unless
`allow_destroy: "true"`; nothing to change skips the review and apply and the stage
still completes; the approver reads the plan, `binds` pins the approval to the exact
bytes shown, and `apply` re-checks them first. Each stage ends in a `done` task that
succeeds only when the plan was applied or changed nothing, and the next stage waits
for it, so a failed plan, a rejected review or a failed apply stops the promotion
instead of letting the next stage plan.

**OpenTofu** needs 1.6 or newer; the templates use only CLI surface it shares with
Terraform.

**`jq`** must be on the worker's PATH for `tf_stage` as well as `pulumi_stage`: the
destroy guard reads the plan's JSON.

**Pulumi is weaker than Terraform here, and the template says so.** Terraform
applies the plan file that was reviewed. Pulumi's saved plans are experimental, so
`pulumi_stage` pins the reviewed preview and a summary of its changes, and `apply`
re-previews and refuses if the summary moved. That catches drift between review and
apply; it does not catch a different change with the same counts. The template also
needs `jq`, a logged-in backend and a secrets passphrase or KMS provider in the task
environment, and it has not been run against a live Pulumi stack in this
repository's tests — try it on a scratch stack first.

# Sub-workflows & templating (Argo-style)

A dagron workflow can **call other workflows** inline. Declare reusable sub-DAGs
under `templates:` and invoke them from a task with `template:`. At run-creation
the engine **expands** every call into leaf tasks (one flat DAG), so the
reconcile loop / executors run it unchanged — a sub-workflow is just a bigger DAG.

Implemented in [`src/expand.rs`](../../src/expand.rs); expansion runs inside
`DagGraph::from_yaml`, so it works on every submit path (API, cron, file ingest).

## Schema additions

```yaml
name: my-workflow
parameters:                 # top-level defaults, referenced as {{ name }}
  env: prod

templates:                  # reusable sub-DAGs
  - name: deploy
    parameters:             # template-scoped defaults (isolated scope)
      region: us-east-1
    tasks:                  # same TaskSpec as the main DAG (can call templates too)
      - { name: push, command: ["sh", "-c", "echo {{ region }}"] }

tasks:
  - name: call-it
    template: deploy        # CALL task: runs the template instead of a container
    arguments:             # override template parameters (resolved in caller scope)
      region: "{{ env }}-east"
    with_items: [a, b]     # optional fan-out: one expansion per item ({{ item }})
    with_param: "{{ xs }}" #   …or fan-out from a JSON-array parameter
    when: "{{ env }} == prod"   # optional guard; false ⇒ task (and its sub-DAG) skipped
    depends_on: [other]
```

A task is **either** a leaf (`command:`) **or** a call (`template:`) — exactly one.

### Substitution & expressions
- `{{ name }}` — parameter / argument / `item` / `index` lookup. Unknown keys are
  left verbatim.
- `{{ item }}`, `{{ item.key }}`, `{{ index }}` — fan-out bindings.
- `{{ a OP b }}` — minimal arithmetic (`+ - * / %`, whitespace-separated), e.g.
  `{{ n - 1 }}`. This is what lets a recursive template decrement and terminate.
- `when:` — `LHS OP RHS` (`== != < > <= >=`) or a bare truthy value.

### Scope
Template scope is **isolated**: inside a template only its own `parameters`
(filled by the caller's `arguments`) are visible. `arguments` values are resolved
in the **caller's** scope first, then passed in — the standard Argo model.

### Safety
- **Depth cap** (64) — a recursive template without a correct `when:` base case
  fails loudly instead of looping forever.
- **Task cap** (100k) — a fan-out × recursion blow-up errors rather than OOMs.
- Post-expansion validation rejects cycles, duplicate names, and any task left
  without a `command`.

## Pattern catalog

| # | File | Pattern |
|---|------|---------|
| 01 | `01_dag_of_dags.yaml` | Workflow calls another workflow (basic sub-DAG) |
| 02 | `02_parameters.yaml` | Parameterized template + dynamic `arguments` |
| 03 | `03_fanout_map.yaml` | Fan-out / map over a literal list (`with_items`) |
| 04 | `04_scatter_gather.yaml` | Scatter→gather (map-reduce, fan-out then fan-in) |
| 05 | `05_dynamic_shards_withparam.yaml` | Data-driven width (`with_param` JSON array) |
| 06 | `06_recursion_countdown.yaml` | Recursion + `when` base case + `{{ n - 1 }}` |
| 07 | `07_recursion_divide_conquer.yaml` | Binary recursion / divide & conquer tree |
| 08 | `08_conditional_branch.yaml` | Conditional execution (`when`) of steps & sub-DAGs |
| 09 | `09_nested_templates.yaml` | Templates calling templates (composition) |
| 10 | `10_matrix_nested_fanout.yaml` | Nested fan-out = parameter matrix (2-D) |
| 11 | `11_diamond_of_subdags.yaml` | Diamond where each branch is a sub-DAG |
| 12 | `12_retry_wrapper.yaml` | Reusable retry/timeout resilience wrapper |
| 13 | `13_fanout_then_subdag.yaml` | Fan-out where each item runs a multi-step sub-DAG |

Every file in this directory is checked by a unit test
(`expand::tests::every_example_template_expands_and_builds`) — they all expand,
build a valid acyclic graph, and are runnable.

## Try one

```bash
# submit to a running engine (management API):
curl -X POST localhost:8080/runs -H 'Content-Type: application/x-yaml' \
  --data-binary @examples/templates/04_scatter_gather.yaml
# inspect the EXPANDED graph that ran:
curl -s localhost:8080/runs/<run_id> | jq '.tasks[].name'
```

## Notes / limits (v1)
- Templates are **inline** (same file). Cross-file/stored-workflow `templateRef`
  is a planned follow-up (it needs a DB lookup during expansion).
- A skipped task (`when` false) contributes no nodes; a dependent that depended
  *only* on it becomes a root (doesn't block) — matching Argo's skip semantics.
- Arithmetic is single-binary-op and whitespace-separated; no nested expressions.

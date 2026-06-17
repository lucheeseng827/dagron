# Argo Workflows equivalents of the dagron templating patterns

dagron's sub-workflow / templating model is **modeled on Argo Workflows**, so the
13 patterns in [`../`](../) translate near one-for-one into native Argo. Each
file here is a single `kind: Workflow` (`apiVersion: argoproj.io/v1alpha1`) using
`metadata.generateName`, `spec.entrypoint`, and `spec.templates`. Leaf work is an
`alpine:3.20` `container` running `sh -c "echo ..."`, mirroring the dagron tasks.

## Concept mapping (dagron -> Argo)

| dagron | Argo |
|--------|------|
| reusable `template` (sub-DAG) | a `dag:` (or `steps:`) template |
| `template:` call task | a dag task's `template:` reference |
| top-level `parameters` | `spec.arguments.parameters` (`{{workflow.parameters.x}}`) |
| template `parameters` (defaults) | `inputs.parameters` with `default:`/`value:` |
| caller `arguments` | task `arguments.parameters` |
| `{{ item }}` / `with_items` | `{{item}}` / `withItems:` |
| `with_param: "{{ xs }}"` (JSON array) | `withParam:` (JSON array) |
| scatter -> gather | `withItems`/`withParam` task listed in a downstream task's `dependencies` |
| recursion (`{{ n - 1 }}` + `when`) | self-referencing dag task + `when:` guard + `{{=asInt(...) - 1}}` expression |
| `when:` guard | `when:` on a dag task |
| `depends_on` | `dependencies` |
| `max_attempts` / `retry_delay_secs` / `timeout_secs` | `retryStrategy` (`limit` + `backoff`) / `activeDeadlineSeconds` |

## Pattern catalog

| # | File | Pattern | Primary Argo feature |
|---|------|---------|----------------------|
| 01 | `01_dag_of_dags.yaml` | Workflow calls another workflow (basic sub-DAG) | nested `dag` templates |
| 02 | `02_parameters.yaml` | Parameterized template + dynamic `arguments` | `inputs.parameters` + `arguments.parameters` |
| 03 | `03_fanout_map.yaml` | Fan-out / map over a literal list | `withItems` + `{{item}}` |
| 04 | `04_scatter_gather.yaml` | Scatter -> gather (map-reduce) | `withItems` + `dependencies` fan-in |
| 05 | `05_dynamic_shards_withparam.yaml` | Data-driven width (JSON array) | `withParam` over `{{workflow.parameters.shards}}` |
| 06 | `06_recursion_countdown.yaml` | Recursion + `when` base case + decrement | self-call dag task + `when` + `{{=asInt(n)-1}}` |
| 07 | `07_recursion_divide_conquer.yaml` | Binary recursion / divide & conquer tree | two self-call tasks + `when` + `{{=asInt(size)/2}}` |
| 08 | `08_conditional_branch.yaml` | Conditional execution of steps & sub-DAGs | `when:` on dag tasks |
| 09 | `09_nested_templates.yaml` | Templates calling templates (composition) | multi-level `dag` template references |
| 10 | `10_matrix_nested_fanout.yaml` | Nested fan-out = parameter matrix (2-D) | outer `withItems` -> inner `withParam` |
| 11 | `11_diamond_of_subdags.yaml` | Diamond where each branch is a sub-DAG | diamond `dependencies` over `dag` templates |
| 12 | `12_retry_wrapper.yaml` | Reusable retry/timeout resilience wrapper | `retryStrategy` + `backoff` + `activeDeadlineSeconds` |
| 13 | `13_fanout_then_subdag.yaml` | Fan-out where each item runs a multi-step sub-DAG | `withParam` -> `dag` template + fan-in |

## Notes

- **Recursion base case (06, 07):** the `when:` guard on the self-referencing
  task IS the terminator. When the decremented/halved parameter no longer
  satisfies the guard, Argo skips the self-call and the recursion unwinds. Argo
  also has its own depth/parallelism controls as a safety net, mirroring dagron's
  depth cap.
- **Expressions:** decrement/halve use Argo's expression syntax
  `{{=asInt(inputs.parameters.n) - 1}}`, the analogue of dagron's `{{ n - 1 }}`.
- **withParam JSON:** `withParam` consumes a JSON array string; override the
  workflow parameter at submit (e.g. `argo submit -p shards='[1,2,3,4,5]'`) to
  change fan-out width without editing the workflow — same as dagron `with_param`.
- These are YAML-parse + structurally validated. The `argo` CLI was not available
  in the authoring environment, so `argo lint` / cluster admission were not run.

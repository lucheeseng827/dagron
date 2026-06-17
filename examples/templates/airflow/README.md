# Airflow analogues of the dagron sub-workflow / templating patterns

This directory mirrors, one-for-one, the 13 dagron sub-workflow & templating
examples in [`../`](../) as **Apache Airflow DAGs**. Each `NN_*.py` here is the
faithful Airflow translation of the corresponding `NN_*.yaml` dagron template.

These are **illustrative**: every file parses with **core `apache-airflow`
(>= 2.5)** only — no extra providers. The check applied is `python -m py_compile`
(syntactic validity), since Airflow may not be installed where you read this.

> Note: all files *parse* on 2.5+, but pattern **13** (`13_fanout_then_subdag.py`)
> requires Airflow **>= 2.6** at *runtime* for mapped task groups (`@task_group`
> + `.expand()`).

## How dagron templates map to Airflow

dagron is an Argo-style engine: you declare reusable sub-DAGs under `templates:`
and a task *calls* one via `template:`. At run-creation the engine **expands**
every call inline into one flat DAG. Airflow has no runtime "call another
workflow" primitive in core, so the idiomatic translation is:

| dagron concept | Airflow analogue |
|---|---|
| `template` (reusable sub-DAG) | a Python **factory function** that builds a `TaskGroup` |
| calling a template (`template:`) | invoking that factory inside the DAG |
| `parameters:` / `arguments:` | factory **function args** + DAG `params` / Jinja `{{ params.x }}` |
| `with_items` fan-out | dynamic task mapping `.partial().expand(...)` |
| `with_param` (JSON-array param) | mapping over a list produced at run time (parse `param` -> `.expand()`) |
| `{{ item }}` / `{{ index }}` | the per-instance mapped value |
| recursion (`{{ n - 1 }}` + `when` base case) | a **recursive parse-time builder** of nested `TaskGroup`s (base case terminates) |
| conditional `when:` | `ShortCircuitOperator` (or `BranchPythonOperator`) driven by a `param` |
| nested templates (template calls template) | nested factories / nested `TaskGroup`s |
| retry/timeout policy template | a factory stamping `retries` / `retry_delay` / `execution_timeout` |
| "each item runs a sub-DAG" | a `@task_group` expanded with `.expand()` (Airflow 2.6+) |

### Important caveat — parse-time vs runtime recursion

dagron expands recursion **at run-creation** (the `{{ n - 1 }}` decrement plus a
`when:` base case terminate it). Airflow **cannot self-trigger a DAG recursively
at run time**, so patterns 06 and 07 use **parse-time recursion**: a recursive
Python builder materializes the nested `TaskGroup` tree when the DAG file is
parsed. The base case (`n <= 0` / `size <= 1`) is the terminator. This is the
faithful structural analogue, but the graph shape is fixed at parse time rather
than computed per run from a parameter.

## Pattern catalog (dagron <-> Airflow)

| # | dagron file | Airflow file | Pattern | Airflow primitive used |
|---|---|---|---|---|
| 01 | `01_dag_of_dags.yaml` | `01_dag_of_dags.py` | Workflow calls another workflow | `TaskGroup` factory |
| 02 | `02_parameters.yaml` | `02_parameters.py` | Parameterized template + dynamic arguments | factory args + DAG `params` / `{{ params.env }}` |
| 03 | `03_fanout_map.yaml` | `03_fanout_map.py` | Fan-out / map over a literal list | `.partial().expand()` |
| 04 | `04_scatter_gather.yaml` | `04_scatter_gather.py` | Scatter -> gather (map-reduce) | `.expand()` + `split >> mapped >> reduce` fan-in |
| 05 | `05_dynamic_shards_withparam.yaml` | `05_dynamic_shards_withparam.py` | Data-driven width (JSON-array param) | `PythonOperator` -> `.expand()` over XCom list |
| 06 | `06_recursion_countdown.yaml` | `06_recursion_countdown.py` | Recursion + base case + decrement | recursive parse-time `TaskGroup` builder |
| 07 | `07_recursion_divide_conquer.yaml` | `07_recursion_divide_conquer.py` | Binary recursion / divide & conquer | recursive parse-time `TaskGroup` builder (2 children/level) |
| 08 | `08_conditional_branch.yaml` | `08_conditional_branch.py` | Conditional execution (`when`) | `ShortCircuitOperator` driven by `params` |
| 09 | `09_nested_templates.yaml` | `09_nested_templates.py` | Templates calling templates | nested `TaskGroup` factories |
| 10 | `10_matrix_nested_fanout.yaml` | `10_matrix_nested_fanout.py` | Nested fan-out = parameter matrix | `.expand()` over `itertools.product` |
| 11 | `11_diamond_of_subdags.yaml` | `11_diamond_of_subdags.py` | Diamond where each branch is a sub-DAG | `TaskGroup` branches + `split >> [a,b] >> join` |
| 12 | `12_retry_wrapper.yaml` | `12_retry_wrapper.py` | Reusable retry/timeout wrapper | factory applying `retries`/`retry_delay`/`retry_exponential_backoff`/`execution_timeout` |
| 13 | `13_fanout_then_subdag.yaml` | `13_fanout_then_subdag.py` | Fan-out where each item runs a sub-DAG | `@task_group` + `.expand()` (Airflow 2.6+) |

## Notes

- Every DAG uses `schedule=None`, `catchup=False`, `start_date=datetime(2024, 1, 1)`,
  and a `dag_id` of the form `pattern_NN_<name>`.
- Tasks shell out with `BashOperator` running `echo ...`, mirroring the dagron
  `sh -c echo` leaf tasks.
- Patterns 05 and 13 make width/sub-DAG fan-out **run-time dynamic** (via XCom
  mapping / mapped task groups), which is actually a touch closer to dagron's
  `with_param` intent than a hard-coded list; the structure still mirrors the YAML.

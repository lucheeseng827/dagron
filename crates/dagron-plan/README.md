# dagron-plan — show what a workflow change does before you merge it

`dagron-plan` diffs two workflow specs **the way the engine sees them**. Both
sides are resolved through the exact parse → template-expansion → validation
pipeline every submit path uses (`DagGraph::from_yaml`), so the plan reflects
what would actually run, not a textual YAML diff: template calls and `with_items`
/ `with_param` fan-outs are flattened to concrete leaf tasks before comparison.
The output is a PR-friendly GitHub-flavored markdown summary plus a Mermaid graph
of the resulting DAG with added/changed tasks flagged.

It ships as both a `dagron-plan` binary and a `dagron_plan` library.

## What it does

- `plan(base_yaml, head_yaml) -> Plan` — resolves both specs and computes the
  difference. Either side failing to parse/expand/validate is an error (a plan on
  an invalid DAG is meaningless).
- `Plan` — the computed diff: `added` / `removed` / `changed` tasks, a run-level
  `run_timeout` change, and root-level `root_changes` (`deadline`, `notify`,
  `result_from`). `has_changes()` reports whether anything differs.
  - `to_markdown()` — summary line, per-category sections, and a Mermaid graph.
  - `to_mermaid()` — a Mermaid `flowchart TD` of the head DAG, with added tasks
    in the `added` class and changed tasks in the `changed` class.
- `TaskView` — the normalized, post-expansion view of a leaf task (command,
  sorted `depends_on`, image, env + `value_from` secret refs, retry/timeout
  fields, trigger rule, hook, approval settings) that field-level diffs compare.

## Quickstart

```sh
dagron-plan <base.yaml> <head.yaml>          # diff two files
dagron-plan --git <base>..<head> <path>      # diff a file across two refs
dagron-plan --git <base> <path>              # diff a ref against the working tree
```

Options:

- `--exit-code` — return `2` (not `0`) when the plan has changes (CI drift gate)
- `--mermaid` — print only the Mermaid graph
- `-h`, `--help` — usage

Exit codes follow `git diff`: `0` no changes, `1` error, and `2` on a non-empty
plan when `--exit-code` is passed. Pipe the markdown into a PR comment.

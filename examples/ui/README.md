# UI starter workflows

The same specs offered behind **Workflows → New → “Start from an example…”** in
the console, as standalone files you can also submit by API or file source.
Walkthrough: [`../../docs/WORKFLOW_UI_GUIDE.md`](../../docs/WORKFLOW_UI_GUIDE.md).

| File | Pattern |
|---|---|
| `01_hello.yaml` | One task — the smallest runnable workflow. |
| `02_diamond.yaml` | Fan-out / fan-in (`a → {b,c} → d`) with retries on `d`. |
| `03_etl.yaml` | Reusable `build → process → publish` pipeline (the **child** below). |
| `04_chained_parent.yaml` | **Chaining**: `prepare → [runs the saved `etl`] → notify` via `workflow_ref`. |

To run the chained example end-to-end, save `03_etl.yaml` as the workflow `etl`
first, then run `04_chained_parent.yaml` — dagron inlines `etl`’s tasks where the
`run-etl` step sits.

> `workflow_ref` (chaining a **saved** workflow) is resolved by dagron-api on the
> UI / management-API run path. It is distinct from the engine’s inline
> `template:` mechanism in [`../templates/`](../templates/README.md), which
> expands at run-creation on the engine’s file/queue path.

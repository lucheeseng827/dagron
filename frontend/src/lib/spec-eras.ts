// Group a workflow's runs into the definitions they actually ran.
//
// Every run snapshots its own `workflow_definitions` row at submit — so
// `definition_id` is unique per run and says nothing about whether the spec
// changed. Two runs of an untouched workflow have different definition ids and
// byte-identical specs. Grouping therefore has to be on the **spec text**.
//
// An *era* is a maximal run of chronologically adjacent runs sharing one spec.
// Adjacency matters: if a spec is edited and later reverted, that is two eras
// with the same text, not one interrupted era — the workflow really did run
// something else in between, and collapsing them would hide it.
//
// Runs whose spec could not be fetched are left out rather than guessed at;
// the caller reports the count so a partial view never passes for a complete
// one.

import type { RunSummary } from "@/types/dagron";

export interface SpecEra {
  /// The YAML every run in this era was created from.
  spec: string;
  /// The runs, newest first — the order the history table shows them in.
  runs: RunSummary[];
  /// The oldest run in this era: where this definition started being used.
  firstRun: RunSummary;
  /// The newest run in this era.
  lastRun: RunSummary;
}

export interface EraGrouping {
  /// Oldest first, so index 0 is the baseline for the runs in hand and a diff
  /// reads forward from it.
  eras: SpecEra[];
  /// Runs left out because their spec could not be read.
  unresolved: number;
}

/// Group `runs` (newest first, as the API returns them) into spec eras.
export function groupEras(runs: RunSummary[], specByRun: Map<string, string>): EraGrouping {
  const eras: SpecEra[] = [];
  let unresolved = 0;
  // Walk oldest → newest so an era's `runs` can be built forward and reversed
  // once at the end, and so "the era before this one" means the earlier one.
  for (let i = runs.length - 1; i >= 0; i--) {
    const run = runs[i];
    const spec = specByRun.get(run.id);
    if (spec === undefined) {
      unresolved++;
      continue;
    }
    const last = eras[eras.length - 1];
    if (last && last.spec === spec) {
      last.runs.push(run);
      last.lastRun = run;
    } else {
      eras.push({ spec, runs: [run], firstRun: run, lastRun: run });
    }
  }
  // Each era's runs were collected oldest → newest; the table reads the other
  // way, so hand them back newest first like every other run list in the UI.
  for (const e of eras) e.runs.reverse();
  return { eras, unresolved };
}

/// Index of the era a run belongs to, for marking the run table. Runs whose
/// spec never resolved are absent.
export function eraIndexByRun(eras: SpecEra[]): Map<string, number> {
  const out = new Map<string, number>();
  eras.forEach((era, i) => {
    for (const r of era.runs) out.set(r.id, i);
  });
  return out;
}

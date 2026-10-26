/// A run's tasks as jobs: each job a row, and a job that ran as several
/// instances (a matrix, a fan-out) a row of them.
///
/// The engine names an instance `<job>.<label>`: a matrix's row by its
/// values (`test.linux-18`), a fan-out's by its item or its index
/// (`shard.0`). Nothing else on a task says which job it is an instance of,
/// so the name is what is read. A name is taken for an instance where
/// another task shares what stands before its last `.`: one task alone with
/// a `.` in its name is a job of that name.

export interface JobTask {
  id: string;
  name: string;
  status: string;
}

export interface JobCell extends JobTask {
  /// What tells the instance from its job's others: its matrix row, its
  /// item, its index. Empty for a job that is one task.
  label: string;
}

export interface JobRow {
  /// The job's name: what stands before an instance's label.
  job: string;
  /// The task that bears the job's own name, where there is one beside its
  /// instances: the one that waits for them all.
  head: JobTask | null;
  cells: JobCell[];
}

function split(name: string): [string, string] | null {
  const at = name.lastIndexOf(".");
  return at <= 0 || at === name.length - 1 ? null : [name.slice(0, at), name.slice(at + 1)];
}

/// The jobs of `tasks`, in the order their first task is in.
export function jobRows(tasks: JobTask[]): JobRow[] {
  // How many tasks share each prefix: two or more are instances of it.
  const shared = new Map<string, number>();
  for (const task of tasks) {
    const parts = split(task.name);
    if (parts) shared.set(parts[0], (shared.get(parts[0]) ?? 0) + 1);
  }
  const instancesOf = (name: string): string | null => {
    const parts = split(name);
    return parts && (shared.get(parts[0]) ?? 0) >= 2 ? parts[0] : null;
  };
  const rows: JobRow[] = [];
  const byJob = new Map<string, JobRow>();
  const row = (job: string): JobRow => {
    let found = byJob.get(job);
    if (!found) {
      found = { job, head: null, cells: [] };
      byJob.set(job, found);
      rows.push(found);
    }
    return found;
  };
  for (const task of tasks) {
    const job = instancesOf(task.name);
    if (job != null) {
      row(job).cells.push({ ...task, label: task.name.slice(job.length + 1) });
    } else if ((shared.get(task.name) ?? 0) >= 2) {
      // The task that bears the name its instances share.
      row(task.name).head = task;
    } else {
      row(task.name).cells.push({ ...task, label: "" });
    }
  }
  return rows;
}

/// Whether any job of `rows` ran as more than one instance: whether a grid
/// says more than the graph does.
export function hasInstances(rows: JobRow[]): boolean {
  return rows.some((r) => r.cells.length > 1);
}

/// How a job reads as a whole, from its instances: what is worst of them.
export function jobStatus(row: JobRow): string {
  const order = ["failed", "cancelled", "running", "awaiting_approval", "ready", "pending", "skipped", "succeeded"];
  const statuses = row.cells.map((c) => c.status);
  if (row.head && statuses.length === 0) return row.head.status;
  for (const status of order) if (statuses.includes(status)) return status;
  return row.head?.status ?? statuses[0] ?? "pending";
}

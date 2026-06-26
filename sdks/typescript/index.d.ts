// Type declarations for the dagron SDK (hand-written so TS consumers get types
// without a build step).

export interface TaskOptions {
  /** Container image (maps to dagron `docker_image`). */
  image?: string;
  /** argv to run. */
  command?: string[];
  /** Names of upstream tasks this one depends on. */
  dependsOn?: string[];
}

export interface DagSpec {
  name: string;
  tasks: Array<{
    name: string;
    docker_image?: string;
    command?: string[];
    depends_on?: string[];
  }>;
}

export declare class Dag {
  readonly name: string;
  constructor(name: string);
  /** Add a task; returns its name (use it in a later task's `dependsOn`). */
  task(name: string, opts?: TaskOptions): string;
  /** Build the dagron spec (validates dependency references). */
  toSpec(): DagSpec;
  /** dagron spec as JSON (valid dagron input). */
  toJSON(): string;
  /** POST the DAG to dagron-api `/api/runs` (wrapped as `{yaml}`); resolves to the new run id. */
  submit(apiUrl: string, opts?: { token?: string }): Promise<string>;
}

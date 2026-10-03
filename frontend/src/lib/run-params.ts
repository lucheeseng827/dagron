import type { ParamRule } from "@/types/dagron";

export interface ParamField {
  name: string;
  default: string;
  required: boolean;
  choices: string[];
  pattern?: string;
  description?: string;
}

/// One field per declared parameter, in name order. A schema entry without a
/// `parameters:` default is refused when the spec is saved, but it is kept here
/// anyway so a hand-edited spec still gets a field.
export function paramFields(
  parameters: Record<string, string> | undefined,
  schema: Record<string, ParamRule> | undefined,
): ParamField[] {
  const names = new Set([...Object.keys(parameters ?? {}), ...Object.keys(schema ?? {})]);
  return [...names].sort().map((name) => {
    const r = schema?.[name] ?? {};
    return {
      name,
      default: parameters?.[name] ?? "",
      required: !!r.required,
      choices: r.enum ?? [],
      pattern: r.pattern,
      description: r.description,
    };
  });
}

/// The same rules the engine applies when the run is triggered (an empty value
/// counts as not supplied), so the form can say what is wrong before a round
/// trip. The server stays authoritative: a pattern JavaScript cannot compile is
/// skipped here and left to it.
export function fieldError(f: ParamField, value: string): string | null {
  if (value === "") return f.required ? "required" : null;
  if (f.choices.length && !f.choices.includes(value)) return `must be one of: ${f.choices.join(", ")}`;
  if (f.pattern) {
    let re: RegExp | null = null;
    try {
      re = new RegExp(`^(?:${f.pattern})$`);
    } catch {
      re = null;
    }
    if (re && !re.test(value)) return `must match ${f.pattern}`;
  }
  return null;
}

/// Only the values the caller changed. Unchanged fields are left out so the
/// spec's own defaults (and anything the server layers on top) still apply.
export function changedValues(fields: ParamField[], values: Record<string, string>): Record<string, string> {
  const out: Record<string, string> = {};
  for (const f of fields) {
    const v = values[f.name] ?? f.default;
    if (v !== f.default) out[f.name] = v;
  }
  return out;
}

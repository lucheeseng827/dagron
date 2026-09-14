//! dagron-state — explain and compile a backfill planner's state plan.
//!
//!   dagron-state explain <plan.json>     # markdown + Mermaid, for a PR comment
//!   dagron-state plan    <plan.json>     # the dagron workflow YAML
//!   dagron-state contract                # the wire contract revision this build reads
//!
//! `-` reads the plan from stdin, so it composes with the planner directly:
//!
//!   freshet plan --project ./models --json | dagron-state explain -
//!
//! The input is either a bare planner `PlanResponse` or a full envelope
//! (`{"plan": …, "graph": …, "options": …}`) — the two are told apart by the
//! presence of a `plan` key, so the planner's own output works unwrapped.
//!
//! This binary is **offline**: it never talks to a dagron. Piping the YAML to
//! `POST /api/state/plans/submit` (or to `dagron`'s run submit) is the caller's
//! step, exactly as `dagron-plan` leaves posting the comment to the caller.
//!
//! Exit codes follow `dagron-plan`, which follows `git diff`: `0` success, `1`
//! error. Pass `--exit-code` to also return `2` when the plan is non-empty — the
//! CI gate shape ("this PR rebuilds something, say so").

use std::process::ExitCode;

use dagron_state::compile::{compile, CompileError, CompileOptions, Ordering, PlanEnvelope};
use dagron_state::explain::explain;
use dagron_state::wire::{PlanResponse, WIRE_CONTRACT_VERSION};

/// Stand-in command for `explain`, which never renders the command. Only ever
/// used when no `--command` was given and the envelope carried none either.
const PLACEHOLDER_COMMAND: &str = "echo {{ model }}";

const USAGE: &str = "usage:\n  \
    dagron-state explain <plan.json|->\n  \
    dagron-state plan    <plan.json|-> [--command '<shell>']\n  \
    dagron-state contract\n\
    \noptions:\n  \
    --command <shell>  per-model argv as `sh -c <shell>`; `{{ model }}`,\n                     \
    `{{ unit }}` and `{{ partitions }}` substitute. `plan` needs this\n                     \
    unless the envelope carries `options.command_template`.\n  \
    --sequential       chain the plan instead of deriving parallel edges\n  \
    --mermaid          print only the Mermaid graph (explain)\n  \
    --json             print the explanation as JSON (explain)\n  \
    --exit-code        return 2 (not 0) when the plan is non-empty\n  \
    -h, --help         this help";

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    match run(&args) {
        Ok(code) => code,
        Err(e) => {
            eprintln!("dagron-state: {e:#}");
            ExitCode::FAILURE
        }
    }
}

fn run(args: &[String]) -> anyhow::Result<ExitCode> {
    let mut positional: Vec<&str> = Vec::new();
    let mut command: Option<String> = None;
    let (mut sequential, mut mermaid_only, mut as_json, mut exit_code) = (false, false, false, false);

    let mut it = args.iter();
    while let Some(a) = it.next() {
        match a.as_str() {
            "-h" | "--help" => {
                println!("{USAGE}");
                return Ok(ExitCode::SUCCESS);
            }
            "--command" => {
                command = Some(
                    it.next().ok_or_else(|| anyhow::anyhow!("--command needs a value\n{USAGE}"))?.clone(),
                );
            }
            "--sequential" => sequential = true,
            "--mermaid" => mermaid_only = true,
            "--json" => as_json = true,
            "--exit-code" => exit_code = true,
            // `-` is the stdin sentinel, not a flag. Every other dash-prefixed
            // token is a typo and must error rather than silently becoming a path.
            "-" => positional.push("-"),
            flag if flag.starts_with('-') => anyhow::bail!("unknown flag '{flag}'\n{USAGE}"),
            other => positional.push(other),
        }
    }

    let Some((&subcommand, rest)) = positional.split_first() else {
        anyhow::bail!("missing subcommand\n{USAGE}");
    };

    match subcommand {
        "contract" => {
            println!("{WIRE_CONTRACT_VERSION}");
            Ok(ExitCode::SUCCESS)
        }
        "explain" | "plan" => {
            let [path] = rest else {
                anyhow::bail!("`{subcommand}` takes exactly one plan file (or `-`)\n{USAGE}");
            };
            let mut envelope = read_envelope(path, command.as_deref(), sequential)?;

            // A command has to come from somewhere the caller actually chose:
            // the flag, or the envelope's own `command_template`. Only when
            // neither exists do the two subcommands diverge — `plan` emits YAML
            // someone will run, so it refuses to guess; `explain` never renders
            // the command, so a placeholder is harmless and keeps the report
            // readable without a flag.
            if envelope.options.command_template.is_empty() {
                if subcommand == "plan" {
                    anyhow::bail!(
                        "`plan` needs a command: pass --command, or give the envelope an \
                         `options.command_template`\n{USAGE}"
                    );
                }
                envelope.options.command_template =
                    vec!["sh".into(), "-c".into(), PLACEHOLDER_COMMAND.into()];
            }
            emit(subcommand, &envelope, mermaid_only, as_json, exit_code)
        }
        other => anyhow::bail!("unknown subcommand '{other}'\n{USAGE}"),
    }
}

/// Read the plan, accepting either a bare `PlanResponse` or a full envelope.
///
/// `command` is `None` when the flag was not given. It stays an `Option` all the
/// way down on purpose: collapsing "absent" into a placeholder string made the
/// override test a string comparison, so passing `--command` with *exactly* the
/// placeholder text against an envelope that carried its own `command_template`
/// silently compiled the envelope's command instead of the one the user typed.
fn read_envelope(
    path: &str,
    command: Option<&str>,
    sequential: bool,
) -> anyhow::Result<PlanEnvelope> {
    let raw = if path == "-" {
        std::io::read_to_string(std::io::stdin())?
    } else {
        std::fs::read_to_string(path).map_err(|e| anyhow::anyhow!("reading {path}: {e}"))?
    };

    let value: serde_json::Value = serde_json::from_str(&raw)
        .map_err(|e| anyhow::anyhow!("{path} is not valid JSON: {e}"))?;

    // An envelope has a `plan`; the planner's own `--json` output does not. This
    // is what lets `freshet plan --json | dagron-state explain -` work unwrapped.
    let mut envelope: PlanEnvelope = if value.get("plan").is_some() {
        serde_json::from_value(value).map_err(|e| anyhow::anyhow!("{path} is not a plan envelope: {e}"))?
    } else {
        let plan: PlanResponse = serde_json::from_value(value)
            .map_err(|e| anyhow::anyhow!("{path} is not a planner PlanResponse: {e}"))?;
        PlanEnvelope { plan, graph: None, options: CompileOptions::default() }
    };

    // CLI flags win over whatever the envelope carried: the person typing the
    // command is more current than the file they are pointing at. Presence of the
    // flag decides — never the value, which can legitimately equal the fallback.
    // With no flag the envelope's own command stands untouched; supplying the
    // fallback is the caller's decision, made after this returns.
    if let Some(c) = command {
        envelope.options.command_template = vec!["sh".into(), "-c".into(), c.to_string()];
    }
    if sequential {
        envelope.options.ordering = Ordering::Sequential;
    }
    Ok(envelope)
}

fn emit(
    subcommand: &str,
    envelope: &PlanEnvelope,
    mermaid_only: bool,
    as_json: bool,
    exit_code: bool,
) -> anyhow::Result<ExitCode> {
    let spec = match compile(envelope) {
        Ok(spec) => spec,
        // "Nothing to rebuild" is a successful outcome, not a failure — a CI gate
        // that treated it as an error would fail every no-op commit.
        Err(CompileError::EmptyPlan) => {
            println!("No models to rebuild.");
            return Ok(ExitCode::SUCCESS);
        }
        Err(e) => anyhow::bail!(e),
    };

    match subcommand {
        "plan" => print!("{}", spec.to_yaml()?),
        _ => {
            let ex = explain(envelope, &spec);
            if mermaid_only {
                print!("{}", ex.mermaid);
            } else if as_json {
                println!("{}", serde_json::to_string_pretty(&ex)?);
            } else {
                print!("{}", ex.markdown);
            }
        }
    }

    Ok(if exit_code { ExitCode::from(2) } else { ExitCode::SUCCESS })
}

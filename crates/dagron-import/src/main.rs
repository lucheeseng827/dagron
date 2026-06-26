//! dagron-import — convert another orchestrator's workflow to dagron DAG YAML.
//!
//!   dagron-import argo <workflow.yaml>   # prints dagron YAML to stdout
//!
//! Pipe it into a file or `POST /api/runs` to migrate. Today only `argo` is
//! supported (Argo Workflows); more importers land behind the same CLI.

use std::process::ExitCode;

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().collect();
    if args.len() != 3 {
        eprintln!("usage: dagron-import <argo> <workflow.yaml>");
        return ExitCode::from(2);
    }
    let kind = args[1].as_str();
    let path = &args[2];

    let input = match std::fs::read_to_string(path) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("error: cannot read '{path}': {e}");
            return ExitCode::FAILURE;
        }
    };

    let result = match kind {
        "argo" => dagron_import::argo_to_dagron(&input),
        other => {
            eprintln!("error: unknown importer '{other}' (supported: argo)");
            return ExitCode::from(2);
        }
    };

    match result {
        Ok(yaml) => {
            print!("{yaml}");
            ExitCode::SUCCESS
        }
        Err(e) => {
            eprintln!("error: {e:#}");
            ExitCode::FAILURE
        }
    }
}

//! `dagron-ci-cache restore` and `dagron-ci-cache save`: what dagron-ci-exec
//! runs before a task's steps and after they succeed, where the engine gave
//! the task caches (`DAGRON_CACHE`). See the library for what each does.
//! Both always exit 0: a cache never fails a task.

use std::process::ExitCode;

use dagron_ci_cache::{restore, save, state_path, Given, Here, State, ENV};

const USAGE: &str = "usage: dagron-ci-cache restore|save\n\
    \n\
    Restores a task's path caches before its steps (restore) and saves them after\n\
    they succeed (save), with the signed URLs the engine gives it in DAGRON_CACHE.";

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let what = match args.first().map(String::as_str) {
        Some("--version") => {
            println!(
                "dagron-ci-cache {}",
                option_env!("APP_VERSION").unwrap_or(env!("CARGO_PKG_VERSION"))
            );
            return ExitCode::SUCCESS;
        }
        Some("--help") | Some("-h") => {
            println!("{USAGE}");
            return ExitCode::SUCCESS;
        }
        Some(w @ ("restore" | "save")) if args.len() == 1 => w,
        _ => {
            eprintln!("{USAGE}");
            return ExitCode::from(2);
        }
    };
    let Some(raw) = std::env::var(ENV).ok().filter(|v| !v.trim().is_empty()) else {
        return ExitCode::SUCCESS;
    };
    let mut log = std::io::stdout();
    let given = match Given::parse(&raw) {
        Ok(given) => given,
        Err(why) => {
            println!("caches: {why}: none restored or saved");
            return ExitCode::SUCCESS;
        }
    };
    let here = Here::now();
    let path = state_path();
    if what == "restore" {
        let state = restore(&given, &here, &mut log);
        if let Err(e) = std::fs::write(&path, serde_json::to_vec(&state).unwrap_or_default()) {
            println!("caches: what was restored could not be noted ({e}): each is saved anew");
        }
    } else {
        let state: State = std::fs::read(&path)
            .ok()
            .and_then(|b| serde_json::from_slice(&b).ok())
            .unwrap_or_default();
        save(&given, &state, &here, &mut log);
        let _ = std::fs::remove_file(&path);
    }
    ExitCode::SUCCESS
}

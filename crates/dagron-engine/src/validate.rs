//! `dagron validate` — offline workflow-spec validation (QW: fast-fail lint).
//!
//! Validates one or more YAML files (or directories, walked recursively for
//! `*.yaml` / `*.yml`) through the exact pipeline every submit path uses —
//! [`DagGraph::from_yaml`]: parse → template expansion → duplicate/cycle/leaf
//! checks — so "validate passed" means "the server would accept it". No
//! database, no network, no daemon: wire it into pre-commit or CI next to
//! `dagron plan` to gate merges on spec validity.
//!
//! Output is human-readable by default; `--json` emits one JSON object per
//! file (`{"file", "ok", "tasks"?, "error"?}`) on stdout for CI annotation.
//! Exit is non-zero (an `Err` from [`run_cli`]) when any file fails.

use anyhow::{bail, Context, Result};
use std::path::{Path, PathBuf};

use crate::dag::DagGraph;

/// One file's validation outcome.
struct FileResult {
    file: PathBuf,
    /// `Ok(task_count)` after expansion, or the validation error.
    outcome: Result<usize>,
}

/// Entry point for `dagron validate <file|dir>... [--json]`. Returns `Err` when
/// any file is invalid (or no YAML file was found), so the process exits
/// non-zero for CI.
pub fn run_cli(args: &[String]) -> Result<()> {
    let mut json = false;
    let mut paths: Vec<PathBuf> = Vec::new();
    for a in args {
        match a.as_str() {
            "--json" => json = true,
            "-h" | "--help" => {
                println!("{USAGE}");
                return Ok(());
            }
            other if other.starts_with('-') => bail!("unknown flag '{other}'\n{USAGE}"),
            other => paths.push(PathBuf::from(other)),
        }
    }
    if paths.is_empty() {
        bail!("no files or directories given\n{USAGE}");
    }

    let files = collect_yaml_files(&paths)?;
    if files.is_empty() {
        bail!("no .yaml/.yml files found under the given paths");
    }

    let results: Vec<FileResult> = files
        .into_iter()
        .map(|file| {
            let outcome = std::fs::read_to_string(&file)
                .with_context(|| format!("reading {}", file.display()))
                .and_then(|yaml| DagGraph::from_yaml(&yaml).map(|g| g.spec.tasks.len()));
            FileResult { file, outcome }
        })
        .collect();

    let mut failed = 0usize;
    for r in &results {
        match (&r.outcome, json) {
            (Ok(tasks), false) => println!("{}: OK ({tasks} tasks)", r.file.display()),
            (Err(e), false) => {
                failed += 1;
                // `{:#}` renders the whole anyhow context chain on one line
                // (serde_yaml errors already carry line/column positions).
                println!("{}: ERROR {:#}", r.file.display(), e);
            }
            (outcome, true) => {
                let obj = match outcome {
                    Ok(tasks) => serde_json::json!({
                        "file": r.file.display().to_string(), "ok": true, "tasks": tasks,
                    }),
                    Err(e) => {
                        failed += 1;
                        serde_json::json!({
                            "file": r.file.display().to_string(), "ok": false, "error": format!("{e:#}"),
                        })
                    }
                };
                println!("{obj}");
            }
        }
    }

    if failed > 0 {
        bail!("{failed} of {} file(s) invalid", results.len());
    }
    Ok(())
}

const USAGE: &str = "usage: dagron validate <file|dir>... [--json]\n\
  Validate workflow YAML offline (parse, template expansion, duplicate/cycle/leaf checks).\n\
  Directories are walked recursively for *.yaml / *.yml. Exits non-zero if any file fails.";

/// Expand the given paths into a sorted, de-duplicated list of YAML files.
/// Directories are walked recursively; hidden directories (`.git`, …) are
/// skipped. A path that exists but is neither a YAML file nor a directory is an
/// error, as is a path that does not exist.
fn collect_yaml_files(paths: &[PathBuf]) -> Result<Vec<PathBuf>> {
    let mut out: Vec<PathBuf> = Vec::new();
    for p in paths {
        if p.is_dir() {
            walk_dir(p, &mut out)?;
        } else if p.is_file() {
            if !is_yaml(p) {
                bail!("{} is not a .yaml/.yml file", p.display());
            }
            out.push(p.clone());
        } else {
            bail!("{}: no such file or directory", p.display());
        }
    }
    out.sort();
    out.dedup();
    Ok(out)
}

fn walk_dir(dir: &Path, out: &mut Vec<PathBuf>) -> Result<()> {
    let entries =
        std::fs::read_dir(dir).with_context(|| format!("reading directory {}", dir.display()))?;
    for entry in entries {
        let entry = entry?;
        // Skip symlinks (file_type does not follow them) so a symlinked-dir cycle
        // under the scan root can't recurse forever / overflow the stack.
        if entry.file_type()?.is_symlink() {
            continue;
        }
        let path = entry.path();
        let hidden = path
            .file_name()
            .and_then(|n| n.to_str())
            .map(|n| n.starts_with('.'))
            .unwrap_or(false);
        if hidden {
            continue;
        }
        if path.is_dir() {
            walk_dir(&path, out)?;
        } else if is_yaml(&path) {
            out.push(path);
        }
    }
    Ok(())
}

fn is_yaml(p: &Path) -> bool {
    matches!(
        p.extension().and_then(|s| s.to_str()),
        Some("yaml") | Some("yml")
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn write(dir: &Path, name: &str, contents: &str) -> PathBuf {
        let p = dir.join(name);
        if let Some(parent) = p.parent() {
            std::fs::create_dir_all(parent).unwrap();
        }
        std::fs::write(&p, contents).unwrap();
        p
    }

    const GOOD: &str = "name: ok\ntasks:\n  - { name: a, command: [\"true\"] }\n";
    const BAD_CYCLE: &str = "name: cyc\ntasks:\n  - { name: a, command: [\"true\"], depends_on: [b] }\n  - { name: b, command: [\"true\"], depends_on: [a] }\n";

    #[test]
    fn validates_good_and_bad_files() {
        let tmp = std::env::temp_dir().join(format!("dagron-validate-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&tmp).unwrap();
        let good = write(&tmp, "good.yaml", GOOD);
        let bad = write(&tmp, "sub/bad.yml", BAD_CYCLE);
        write(&tmp, ".hidden/skipped.yaml", BAD_CYCLE); // hidden dir: not walked
        write(&tmp, "notes.txt", "not yaml");

        // Directory walk finds exactly the two visible YAML files.
        let files = collect_yaml_files(&[tmp.clone()]).unwrap();
        assert_eq!(files, {
            let mut v = vec![good.clone(), bad.clone()];
            v.sort();
            v
        });

        // Good alone passes; the directory (containing the cycle) fails.
        run_cli(&[good.display().to_string()]).unwrap();
        let err = run_cli(&[tmp.display().to_string()])
            .unwrap_err()
            .to_string();
        assert!(err.contains("1 of 2"), "got: {err}");

        // Missing path and non-YAML file are hard errors.
        assert!(run_cli(&["/nonexistent/x.yaml".to_string()]).is_err());
        let txt = tmp.join("notes.txt").display().to_string();
        assert!(run_cli(&[txt]).is_err());

        std::fs::remove_dir_all(&tmp).ok();
    }
}

//! dagron-plan — show what a workflow change does before you merge it.
//!
//!   dagron-plan <base.yaml> <head.yaml>         # diff two files
//!   dagron-plan --git <base>..<head> <path>     # diff a file across two refs
//!   dagron-plan --git <base> <path>             # diff <base>:<path> vs the worktree
//!
//! Both specs are resolved through the real dagron parser + expander, so the
//! plan reflects what would actually run. Output is GitHub-flavored markdown
//! (summary + per-task diff + a Mermaid graph of the resulting DAG) — pipe it
//! into a PR comment.
//!
//! Exit codes follow `git diff`: `0` no changes, `1` error. Pass `--exit-code`
//! to also return `2` when the plan is non-empty (CI drift gate).

use std::process::{Command, ExitCode};

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    match run(&args) {
        Ok(code) => code,
        Err(e) => {
            eprintln!("dagron-plan: {e:#}");
            ExitCode::FAILURE
        }
    }
}

const USAGE: &str = "usage:\n  \
    dagron-plan <base.yaml> <head.yaml>\n  \
    dagron-plan --git <base-ref>..<head-ref> <path>\n  \
    dagron-plan --git <base-ref> <path>          # compare against the working tree\n\
    \noptions:\n  \
    --exit-code   return 2 (not 0) when the plan has changes\n  \
    --mermaid     print only the Mermaid graph\n  \
    -h, --help    this help";

fn run(args: &[String]) -> anyhow::Result<ExitCode> {
    let mut positional: Vec<&str> = Vec::new();
    let mut git = false;
    let mut exit_code = false;
    let mut mermaid_only = false;
    for a in args {
        match a.as_str() {
            "-h" | "--help" => {
                println!("{USAGE}");
                return Ok(ExitCode::SUCCESS);
            }
            "--git" => git = true,
            "--exit-code" => exit_code = true,
            "--mermaid" => mermaid_only = true,
            // Any unrecognized dash-prefixed arg is a bad flag (a single-dash typo
            // like `-x` should error, not silently become a positional).
            flag if flag.starts_with('-') => anyhow::bail!("unknown flag '{flag}'\n{USAGE}"),
            other => positional.push(other),
        }
    }

    let (base_yaml, head_yaml) = if git {
        read_git(&positional)?
    } else {
        anyhow::ensure!(positional.len() == 2, "expected two files\n{USAGE}");
        (read_file(positional[0])?, read_file(positional[1])?)
    };

    let plan = dagron_plan::plan(&base_yaml, &head_yaml)?;
    if mermaid_only {
        print!("{}", plan.to_mermaid());
    } else {
        print!("{}", plan.to_markdown());
    }

    Ok(if exit_code && plan.has_changes() {
        ExitCode::from(2)
    } else {
        ExitCode::SUCCESS
    })
}

/// `--git` mode inputs: `<base-ref>..<head-ref> <path>` compares the file at two
/// refs; `<base-ref> <path>` compares the ref against the working-tree file.
fn read_git(positional: &[&str]) -> anyhow::Result<(String, String)> {
    anyhow::ensure!(
        positional.len() == 2,
        "--git needs '<base>..<head> <path>' or '<base> <path>'\n{USAGE}"
    );
    let (refspec, path) = (positional[0], positional[1]);
    match refspec.split_once("..") {
        Some((base_ref, head_ref)) => Ok((git_show(base_ref, path)?, git_show(head_ref, path)?)),
        None => Ok((git_show(refspec, path)?, read_file(path)?)),
    }
}

/// `git show <ref>:<path>` — the file's contents at a ref.
fn git_show(git_ref: &str, path: &str) -> anyhow::Result<String> {
    let spec = format!("{git_ref}:{path}");
    let out = Command::new("git")
        .args(["show", &spec])
        .output()
        .map_err(|e| anyhow::anyhow!("running `git show {spec}`: {e}"))?;
    if !out.status.success() {
        anyhow::bail!(
            "`git show {spec}` failed: {}",
            String::from_utf8_lossy(&out.stderr).trim()
        );
    }
    String::from_utf8(out.stdout)
        .map_err(|e| anyhow::anyhow!("`git show {spec}` output is not UTF-8: {e}"))
}

fn read_file(path: &str) -> anyhow::Result<String> {
    std::fs::read_to_string(path).map_err(|e| anyhow::anyhow!("reading '{path}': {e}"))
}

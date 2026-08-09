//! Hyper-scale exposure E1: what does one BIG workflow cost the engine, in memory?
//!
//! This is the headline scalability risk: dagron's measured 16.4 MiB control
//! plane was taken with 7-task workflows whose backlog
//! lives as Postgres rows. It has never been measured against a workflow whose
//! *expanded graph* must exist in engine memory all at once.
//!
//! [`dagron_core::expand::expand`] is a pure function — no database, no network,
//! no cluster. So the risk is measurable on a laptop for the price of nothing,
//! and there is no reason to rent a node to answer it. That is the entire point
//! of this example: it moves E1 out of the "needs infrastructure" column.
//!
//! What it measures, in the order the submission path pays it:
//!
//!   1. **serialize** — the YAML body a client must ship (exposure E2).
//!   2. **parse**     — `serde_yaml` → `DagSpec`, the cost of accepting it.
//!   3. **expand**    — the flattening that materialises every leaf task.
//!
//! Peak RSS comes from `VmHWM` in `/proc/self/status` — the kernel's own
//! high-water mark, which counts allocator behaviour a Rust-side counter would
//! miss. Because a high-water mark only ever rises, **one process measures one
//! task count**; sweeping N in a loop inside one process would report the
//! largest N for every row. `bench_expand.py` drives the sweep and forks per N.
//!
//!   cargo run -p dagron-core --release --example expand_bench -- --tasks 10000
//!   cargo run -p dagron-core --release --example expand_bench -- --tasks 512 --shape chain
//!
//! Build in **release**: a debug build measures `serde`'s unoptimised layout, not
//! the engine's.

use std::time::Instant;

/// Upper bound on `--tasks`, one past `expand()`'s own `MAX_TASKS` (100,000).
///
/// Deliberately `MAX_TASKS + 1` rather than `MAX_TASKS`: the whole point of the
/// top tier is to observe the *refusal*, so the first rejected value must remain
/// reachable. `MAX_TASKS` itself is private to `expand.rs`, so this mirrors it —
/// if that constant moves, this one has to follow.
const MAX_BENCH_TASKS: usize = 100_001;

/// Peak resident set size for this process, in KiB, straight from the kernel.
///
/// `VmHWM` ("high water mark") is monotonic for the life of the process, which
/// is exactly why this example measures a single task count and exits.
fn peak_rss_kib() -> u64 {
    read_status_field("VmHWM:")
}

/// Current resident set size in KiB — used for the baseline before any work.
fn current_rss_kib() -> u64 {
    read_status_field("VmRSS:")
}

fn read_status_field(field: &str) -> u64 {
    let status = match std::fs::read_to_string("/proc/self/status") {
        Ok(s) => s,
        // Non-Linux (no procfs): report 0 rather than failing the run. The
        // timings and byte counts below are still meaningful.
        Err(_) => return 0,
    };
    status
        .lines()
        .find(|l| l.starts_with(field))
        .and_then(|l| l.split_whitespace().nth(1))
        .and_then(|v| v.parse().ok())
        .unwrap_or(0)
}

/// A fan-out: one extract, `width` independent transforms, one load that joins
/// them all. The load task's `depends_on` is what makes this interesting — it is
/// a single task holding `width` edges, so the edge count grows with the graph.
fn yaml_fanout(width: usize) -> String {
    let mut s = String::with_capacity(width * 128);
    s.push_str("name: bench-fanout\ntasks:\n");
    s.push_str("  - name: extract\n    command: [\"/bin/etl\", \"extract\"]\n");
    for i in 0..width {
        s.push_str(&format!(
            "  - name: transform-{i}\n    command: [\"/bin/etl\", \"transform\"]\n    depends_on: [extract]\n"
        ));
    }
    s.push_str("  - name: load\n    command: [\"/bin/etl\", \"load\"]\n    depends_on: [");
    for i in 0..width {
        if i > 0 {
            s.push_str(", ");
        }
        s.push_str(&format!("transform-{i}"));
    }
    s.push_str("]\n");
    s
}

/// A linear chain `depth` tasks long. The benchmark's deepest shape was **6**;
/// this is the axis that tests per-hop scheduling rather than fan-out width.
fn yaml_chain(depth: usize) -> String {
    let mut s = String::with_capacity(depth * 128);
    s.push_str("name: bench-chain\ntasks:\n");
    s.push_str("  - name: step-0\n    command: [\"/bin/etl\", \"extract\"]\n");
    for i in 1..=depth {
        s.push_str(&format!(
            "  - name: step-{i}\n    command: [\"/bin/etl\", \"transform\"]\n    depends_on: [step-{}]\n",
            i - 1
        ));
    }
    s
}

/// A `with_items` fan-out routed through a template — the path that actually
/// calls [`expand`]'s recursion and budget accounting. The flat shapes above are
/// already leaf-only, so they exercise parse and validation but never the
/// expander's interesting half; this one does.
fn yaml_template_fanout(width: usize) -> String {
    let items: Vec<String> = (0..width).map(|i| format!("\"{i}\"")).collect();
    format!(
        "name: bench-template-fanout\ntemplates:\n  - name: unit\n    parameters:\n      shard: \"0\"\n    tasks:\n      - name: work\n        command: [\"/bin/etl\", \"transform\", \"{{{{ shard }}}}\"]\ntasks:\n  - name: extract\n    command: [\"/bin/etl\", \"extract\"]\n  - name: shard\n    template: unit\n    depends_on: [extract]\n    with_items: [{}]\n    arguments:\n      shard: \"{{{{ item }}}}\"\n",
        items.join(", ")
    )
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let mut tasks = 1000usize;
    let mut shape = "fanout".to_string();

    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--tasks" | "-n" => {
                i += 1;
                // Reject 0 as well as junk: an empty graph produces a row of
                // timings that measure nothing and pollute the sweep table.
                //
                // Also bound the top: the generator and `serde_yaml` both run to
                // completion BEFORE `expand()` gets to apply its own MAX_TASKS
                // refusal, so `--tasks 1000000000` would build and parse a
                // multi-gigabyte document and die of memory exhaustion instead of
                // producing the typed refusal this bench exists to record.
                tasks = args
                    .get(i)
                    .and_then(|v| v.parse().ok())
                    .filter(|n: &usize| (1..=MAX_BENCH_TASKS).contains(n))
                    .unwrap_or_else(|| {
                        eprintln!("--tasks needs an integer in 1..={MAX_BENCH_TASKS}");
                        std::process::exit(2);
                    });
            }
            "--shape" | "-s" => {
                i += 1;
                shape = args.get(i).cloned().unwrap_or_default();
            }
            "--help" | "-h" => {
                eprintln!(
                    "usage: expand_bench --tasks N [--shape fanout|chain|template]\n\
                     \n\
                     Measures serialize / parse / expand cost for ONE task count and exits.\n\
                     Peak RSS is a per-process high-water mark, so sweep by forking\n\
                     (see loadtest/harness/bench_expand.py)."
                );
                std::process::exit(0);
            }
            other => {
                eprintln!("unknown argument: {other}");
                std::process::exit(2);
            }
        }
        i += 1;
    }

    let rss_baseline = current_rss_kib();

    // ── 1. serialize — exposure E2: the body every submission ships ──────────
    let t0 = Instant::now();
    let yaml = match shape.as_str() {
        "fanout" => yaml_fanout(tasks),
        "chain" => yaml_chain(tasks),
        "template" => yaml_template_fanout(tasks),
        other => {
            eprintln!("unknown shape '{other}' (want fanout|chain|template)");
            std::process::exit(2);
        }
    };
    let serialize_ms = t0.elapsed().as_secs_f64() * 1000.0;
    let yaml_bytes = yaml.len();
    let rss_after_serialize = peak_rss_kib();

    // ── 2. parse — the cost of accepting the body ────────────────────────────
    let t1 = Instant::now();
    let spec: dagron_core::dag::DagSpec = match serde_yaml::from_str(&yaml) {
        Ok(s) => s,
        Err(e) => {
            emit_failure(tasks, &shape, yaml_bytes, "parse", &e.to_string());
            std::process::exit(1);
        }
    };
    let parse_ms = t1.elapsed().as_secs_f64() * 1000.0;
    let rss_after_parse = peak_rss_kib();

    // ── 3. expand — the flattening E1 is about ───────────────────────────────
    let t2 = Instant::now();
    let expanded = dagron_core::expand::expand(spec);
    let expand_ms = t2.elapsed().as_secs_f64() * 1000.0;
    let rss_peak = peak_rss_kib();

    match expanded {
        Ok(spec) => {
            // Emitted as one JSON object per process so the sweep driver can
            // collect stdout without parsing prose.
            println!(
                r#"{{"shape":"{shape}","requested_tasks":{tasks},"expanded_tasks":{},"yaml_bytes":{yaml_bytes},"serialize_ms":{serialize_ms:.3},"parse_ms":{parse_ms:.3},"expand_ms":{expand_ms:.3},"rss_baseline_kib":{rss_baseline},"rss_after_serialize_kib":{rss_after_serialize},"rss_after_parse_kib":{rss_after_parse},"rss_peak_kib":{rss_peak},"ok":true}}"#,
                spec.tasks.len()
            );
        }
        Err(e) => {
            // A refusal is a RESULT, not a crash: `expand()` caps a run at
            // MAX_TASKS (100_000) and bails deliberately rather than exhausting
            // memory. Recording the message is the point of the T5 tier.
            emit_failure(tasks, &shape, yaml_bytes, "expand", &e.to_string());
        }
    }
}

fn emit_failure(tasks: usize, shape: &str, yaml_bytes: usize, stage: &str, msg: &str) {
    let escaped = msg.replace('\\', "\\\\").replace('"', "\\\"").replace('\n', " ");
    println!(
        r#"{{"shape":"{shape}","requested_tasks":{tasks},"yaml_bytes":{yaml_bytes},"rss_peak_kib":{},"ok":false,"failed_stage":"{stage}","error":"{escaped}"}}"#,
        peak_rss_kib()
    );
}

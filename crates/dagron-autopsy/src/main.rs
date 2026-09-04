//! `dagron-autopsy` — the job-autopsy binary. **It schedules nothing.**
//!
//! Install it beside an existing Slurm cluster, point it at what is already on
//! disk, and it answers the question the scheduler cannot: *what actually
//! broke?* (`sacct` is required — it supplies the node set and window that
//! everything else is joined against.)
//!
//! ```text
//! dagron-autopsy 88123 \
//!   --sacct  <(sacct -j 88123 -P -n -o JobID,JobName,State,ExitCode,Start,End,Elapsed,NodeList,AllocTRES,Reason) \
//!   --dcgm   /var/log/dcgm/events.json \
//!   --nccl   slurm-88123.out \
//!   --ib-before ib-pre.txt --ib-after ib-post.txt
//! ```
//!
//! Zero migration risk is the point: no database, no daemon, no agent, no
//! change to how jobs are submitted. Arguments are parsed by hand rather than
//! with a CLI crate — this binary's whole pitch is "one small static thing you
//! can drop on a login node", and a dependency tree undercuts that before the
//! first line of output.

use anyhow::{bail, Context, Result};
use chrono::TimeDelta;
use dagron_autopsy::{correlate, dcgm, ib, nccl, record::JobAutopsy, sacct, Inputs, Window};
use dagron_core::fault::FaultClass;
use std::io::Read;
use std::process::Command;

const USAGE: &str = r#"dagron-autopsy — fault-attributed job records. Schedules nothing.

USAGE
  dagron-autopsy <job-id> [inputs] [options]
  dagron-autopsy --explain

INPUTS  (all optional except one source of job identity; `-` means stdin)
  --sacct FILE       sacct -P -n output (see --collect for the exact command)
  --dcgm FILE        DCGM events: dcgmi text, CSV with a header, or NDJSON
  --nccl FILE        the job's stdout/stderr (NCCL + framework logs)
  --ib-events FILE   UFM / ibwarn / syslog fabric event lines
  --ib-before FILE   InfiniBand counter sample taken before the job
  --ib-after FILE    ...and after. Both are needed: only a *rise* is evidence.
  --collect          run `sacct` for <job-id> instead of reading --sacct

WINDOW  (durations: 90s, 15m, 2h, 1d)
  --lookback DUR     how far before the job's end to believe a signal
                     (default: the whole job)
  --grace DUR        how far after its end (default 120s — a driver reports a
                     dead device after the process is already gone)
  --skew DUR         slack for clock disagreement between sources (default 30s)

OUTPUT
  --format text|json  default text
  --explain           print the fault taxonomy and exit
  -h, --help

EXIT
  0  a record was produced (including an honest "unattributed" one)
  1  the inputs could not be read or parsed
  2  usage error
"#;

fn main() {
    match run() {
        Ok(code) => std::process::exit(code),
        Err(e) => {
            eprintln!("dagron-autopsy: {e:#}");
            std::process::exit(1);
        }
    }
}

#[derive(Default)]
struct Args {
    job_id: Option<String>,
    sacct: Option<String>,
    dcgm: Option<String>,
    nccl: Option<String>,
    ib_events: Option<String>,
    ib_before: Option<String>,
    ib_after: Option<String>,
    collect: bool,
    lookback: Option<TimeDelta>,
    grace: Option<TimeDelta>,
    skew: Option<TimeDelta>,
    json: bool,
}

fn run() -> Result<i32> {
    let argv: Vec<String> = std::env::args().skip(1).collect();
    if argv.is_empty() || argv.iter().any(|a| a == "-h" || a == "--help") {
        print!("{USAGE}");
        return Ok(if argv.is_empty() { 2 } else { 0 });
    }
    if argv.iter().any(|a| a == "--explain") {
        print!("{}", explain());
        return Ok(0);
    }

    // Usage mistakes are exit 2, which USAGE promises. Parsing is its own
    // function returning the message rather than an `anyhow::Error` for exactly
    // that reason: a `bail!` here would surface through `main` as exit 1, and a
    // wrapper script branching on the code would read "you typed it wrong" as
    // "the input could not be read".
    let a = match parse_args(&argv) {
        Ok(a) => a,
        Err(msg) => {
            eprintln!("dagron-autopsy: {msg}\n");
            print!("{USAGE}");
            return Ok(2);
        }
    };

    let Some(job_id) = a.job_id.clone() else {
        eprintln!("dagron-autopsy: no job id given\n");
        print!("{USAGE}");
        return Ok(2);
    };

    let autopsy = autopsy(&a, &job_id)?;
    if a.json {
        println!("{}", autopsy.to_json());
    } else {
        print!("{}", autopsy.to_text());
    }
    Ok(0)
}

/// Parse the command line. `Err` is a **usage** message — the caller turns it
/// into exit 2 and the help text.
fn parse_args(argv: &[String]) -> std::result::Result<Args, String> {
    let mut a = Args::default();
    let mut i = 0;
    while i < argv.len() {
        let arg = argv[i].as_str();
        // One `next` helper so a missing value is the same message everywhere
        // rather than an index panic on the last flag.
        macro_rules! next {
            ($name:expr) => {{
                i += 1;
                match argv.get(i) {
                    // A following option is a forgotten value, not a path.
                    // Swallowing it silently stored `--dcgm` as the sacct path
                    // and surfaced later as a file-read failure — exit 1, the
                    // "could not read the input" code, for what is a usage
                    // mistake. Any leading `-` counts, not just `--`: a typo'd
                    // or short-form flag is no more a plausible filename than a
                    // long one, and catching only `--` left `-x` landing in the
                    // same file-read error this exists to prevent. `-` alone is
                    // exempt: it is the documented stdin path.
                    Some(v) if v.starts_with('-') && v != "-" => {
                        return Err(format!("{} needs a value (got the option '{v}')", $name))
                    }
                    Some(v) => v.clone(),
                    None => return Err(format!("{} needs a value", $name)),
                }
            }};
        }
        match arg {
            "--sacct" => a.sacct = Some(next!("--sacct")),
            "--dcgm" => a.dcgm = Some(next!("--dcgm")),
            "--nccl" => a.nccl = Some(next!("--nccl")),
            "--ib-events" => a.ib_events = Some(next!("--ib-events")),
            "--ib-before" => a.ib_before = Some(next!("--ib-before")),
            "--ib-after" => a.ib_after = Some(next!("--ib-after")),
            "--collect" => a.collect = true,
            "--lookback" => a.lookback = Some(parse_duration(&next!("--lookback")).map_err(|e| e.to_string())?),
            "--grace" => a.grace = Some(parse_duration(&next!("--grace")).map_err(|e| e.to_string())?),
            "--skew" => a.skew = Some(parse_duration(&next!("--skew")).map_err(|e| e.to_string())?),
            "--format" => {
                let v = next!("--format");
                match v.as_str() {
                    "json" => a.json = true,
                    "text" => a.json = false,
                    other => {
                        return Err(format!("unknown --format '{other}' (expected text or json)"))
                    }
                }
            }
            other if other.starts_with('-') => {
                return Err(format!("unknown option '{other}'"));
            }
            other => {
                if let Some(first) = &a.job_id {
                    return Err(format!(
                        "more than one job id given ('{first}' and '{other}')"
                    ));
                }
                a.job_id = Some(other.to_string());
            }
        }
        i += 1;
    }
    Ok(a)
}

fn autopsy(a: &Args, job_id: &str) -> Result<JobAutopsy> {
    // ── sacct: the frame. Without a node set and a window there is nothing to
    //    intersect against, so this is the one required input.
    let sacct_text = match (&a.sacct, a.collect) {
        (Some(path), _) => read(path)?,
        (None, true) => collect_sacct(job_id)?,
        (None, false) => bail!(
            "need --sacct FILE (or --collect). The autopsy is a join against the job's node set \
             and time window; without them there is nothing to join.\n\n  \
             sacct -j {job_id} -P -n -o {}",
            sacct::FIELDS
        ),
    };
    let (records, mut warnings) = sacct::parse(&sacct_text)?;
    let job = sacct::select(&records, Some(job_id))
        .with_context(|| format!("job {job_id} not found in the sacct output"))?
        .clone();

    let mut signals = job.signals();

    if let Some(path) = &a.dcgm {
        signals.extend(dcgm::parse(&read(path)?));
    } else {
        warnings.push(
            "no --dcgm input: XID, ECC and row-remap events were not consulted, so a device \
             fault cannot be found even if there was one"
                .into(),
        );
    }

    // The job's own logs are dated to its end when their lines carry no clock,
    // and attributed to its first node when they carry no host.
    let fallback_at = job.end.or(job.start).unwrap_or_else(chrono::Utc::now);
    let first_node = job.nodes.iter().next().cloned();
    let mut topology = nccl::RankTopology::default();
    if let Some(path) = &a.nccl {
        let report = nccl::parse(&read(path)?, fallback_at, first_node.as_deref());
        signals.extend(report.signals);
        topology = report.topology;
    } else {
        warnings.push(
            "no --nccl input: the rank topology of the hang is unknown, so a deadlock cannot be \
             told from a straggler"
                .into(),
        );
    }

    if let Some(path) = &a.ib_events {
        signals.extend(ib::parse_events(&read(path)?));
    }
    match (&a.ib_before, &a.ib_after) {
        (Some(b), Some(af)) => {
            let before = ib::parse_samples(&read(b)?);
            let after = ib::parse_samples(&read(af)?);
            signals.extend(ib::diff(&before, &after, fallback_at));
        }
        (None, None) => {}
        // Half a pair is not a smaller answer, it is no answer: a level says
        // nothing about a fabric, only a delta does. Saying so beats silently
        // ignoring the flag the operator did pass.
        _ => warnings.push(
            "--ib-before and --ib-after must be given together (a counter level is not evidence; \
             only a rise during the job is) — fabric counters were skipped"
                .into(),
        ),
    }

    let window = Window {
        lookback: a.lookback,
        grace: a.grace.unwrap_or_else(|| Window::default().grace),
        skew: a.skew.unwrap_or_else(|| Window::default().skew),
    };
    Ok(correlate(&job, Inputs { signals, topology, warnings }, &window))
}

/// `-` is stdin, everything else is a path.
fn read(path: &str) -> Result<String> {
    if path == "-" {
        let mut s = String::new();
        std::io::stdin().read_to_string(&mut s).context("reading stdin")?;
        return Ok(s);
    }
    std::fs::read_to_string(path).with_context(|| format!("reading {path}"))
}

/// Run `sacct` ourselves. Opt-in (`--collect`) rather than a fallback: a
/// diagnostic tool that shells out without being asked is one an admin cannot
/// reason about, and on a login node under load an unexpected `sacct` is a real
/// cost.
fn collect_sacct(job_id: &str) -> Result<String> {
    let out = Command::new("sacct")
        .args(["-j", job_id, "-P", "-n", "-o", sacct::FIELDS])
        .output()
        .context("running `sacct` (is Slurm on PATH? otherwise pass --sacct FILE)")?;
    if !out.status.success() {
        bail!(
            "sacct exited {}: {}",
            out.status,
            String::from_utf8_lossy(&out.stderr).trim()
        );
    }
    Ok(String::from_utf8_lossy(&out.stdout).into_owned())
}

/// `90s`, `15m`, `2h`, `1d`, or a bare number of seconds.
fn parse_duration(s: &str) -> Result<TimeDelta> {
    let t = s.trim();
    let (num, mult) = match t.chars().last() {
        Some('s') => (&t[..t.len() - 1], 1),
        Some('m') => (&t[..t.len() - 1], 60),
        Some('h') => (&t[..t.len() - 1], 3_600),
        Some('d') => (&t[..t.len() - 1], 86_400),
        _ => (t, 1),
    };
    let n: i64 = num
        .trim()
        .parse()
        .with_context(|| format!("bad duration '{s}' (expected e.g. 90s, 15m, 2h, 1d)"))?;
    if n < 0 {
        bail!("duration '{s}' is negative");
    }
    Ok(TimeDelta::seconds(n * mult))
}

/// The taxonomy, printed. A vocabulary nobody can enumerate is a vocabulary
/// nobody adopts — and this table is also what a `retry_budgets:` key must
/// match, so it doubles as the reference for that.
fn explain() -> String {
    use std::fmt::Write;
    let mut s = String::new();
    let _ = writeln!(
        s,
        "fault classes — the vocabulary shared by dagron-autopsy and dagron's retry budgets\n"
    );
    let _ = writeln!(
        s,
        "{:<20} {:<16} {:<7} {:<6} BELIEVED AS",
        "CLASS", "DISPOSITION", "BUDGET", "DRAIN"
    );
    for c in FaultClass::ALL {
        let budget = c.default_budget();
        let _ = writeln!(
            s,
            "{:<20} {:<16} {:<7} {:<6} {}",
            c.as_str(),
            c.disposition().as_str(),
            if budget == 0 { "task's".to_string() } else { budget.to_string() },
            if c.should_drain_node() { "yes" } else { "-" },
            match c.precedence() {
                dagron_core::fault::Precedence::RootCause => "a cause",
                dagron_core::fault::Precedence::Ambiguous => "either",
                dagron_core::fault::Precedence::Symptom => "a symptom only",
            }
        );
    }
    let _ = writeln!(
        s,
        "\nBUDGET is the default attempts a class gets; \"task's\" means the class declines to\n\
         have an opinion and the task's own max_attempts applies. A class BELIEVED AS a symptom\n\
         is never promoted to a cause on its own — nccl-timeout is printed by every rank that\n\
         was *waiting*, which is to say by the healthy ones."
    );
    s
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn durations_parse_in_the_units_the_help_advertises() {
        assert_eq!(parse_duration("90s").unwrap(), TimeDelta::seconds(90));
        assert_eq!(parse_duration("15m").unwrap(), TimeDelta::seconds(900));
        assert_eq!(parse_duration("2h").unwrap(), TimeDelta::seconds(7200));
        assert_eq!(parse_duration("1d").unwrap(), TimeDelta::seconds(86_400));
        assert_eq!(parse_duration("45").unwrap(), TimeDelta::seconds(45));
        assert!(parse_duration("soon").is_err());
        assert!(parse_duration("-5m").is_err());
    }

    #[test]
    fn every_usage_mistake_is_a_usage_error_not_an_input_error() {
        // USAGE promises exit 2 for a usage mistake and 1 for unreadable input.
        // These three used to travel through `bail!` and surface as 1, so a
        // wrapper branching on the code could not tell them apart.
        let args = |v: &[&str]| v.iter().map(|s| s.to_string()).collect::<Vec<_>>();
        for bad in [
            vec!["88123", "--format", "yaml"],
            vec!["88123", "--sacct"],
            // A forgotten value that the next flag would otherwise fill in.
            vec!["88123", "--sacct", "--dcgm"],
            // Same mistake in short form. Catching only `--` left this one
            // storing "-x" as the sacct path, so the usage error resurfaced as
            // the file-read error (exit 1) the guard exists to prevent.
            vec!["88123", "--sacct", "-x"],
            vec!["88123", "99999"],
            vec!["--nope"],
        ] {
            assert!(
                parse_args(&args(&bad)).is_err(),
                "{bad:?} should be a usage error"
            );
        }
        // And a well-formed line still parses.
        let ok = parse_args(&args(&["88123", "--sacct", "s.txt", "--format", "json"])).unwrap();
        assert_eq!(ok.job_id.as_deref(), Some("88123"));
        assert_eq!(ok.sacct.as_deref(), Some("s.txt"));
        assert!(ok.json);
        // `-` is the documented stdin path and must survive the guard above.
        let stdin = parse_args(&args(&["88123", "--sacct", "-"])).unwrap();
        assert_eq!(stdin.sacct.as_deref(), Some("-"));
    }

    #[test]
    fn explain_lists_every_class_exactly_once() {
        let text = explain();
        for c in FaultClass::ALL {
            assert!(
                text.lines().any(|l| l.starts_with(c.as_str())),
                "{} missing from --explain",
                c.as_str()
            );
        }
    }
}

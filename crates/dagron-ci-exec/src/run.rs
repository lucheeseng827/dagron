//! Running a job's steps, in order, each in its own `sh -ec` and its own
//! process group.
//!
//! - **Markers go to stderr.** A task's output is its stdout, which `when:`,
//!   `with_output_of` and the task cache read: it is the steps' stdout alone.
//! - **A timeout** sends the step's process group SIGTERM, then SIGKILL once
//!   the grace has passed. The step ends with `timeout(1)`'s codes, 124, or
//!   137 when it had to be killed, as it does under the shell fallback.
//! - **A cancelled task** (SIGTERM, SIGINT, SIGHUP or SIGQUIT to this
//!   process) has the signal passed on to the running step, SIGKILL after
//!   the grace, and no further step started. The task exits 128 plus the
//!   signal.
//! - **What a step leaves running** is killed when the task ends: the
//!   process groups of its steps, while they still hold a child of this
//!   process. On Linux this process is a child subreaper, so what a step
//!   leaves behind is reaped here, and a step's shell dies with it.

use std::fmt;
use std::io::{self, Write};
use std::os::unix::process::CommandExt;
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicI32, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use crate::protocol::{Job, Step};
use crate::result::{JobResult, Outcome, StepResult};

/// The signals that cancel a task. Each is passed on to the running step.
pub const CANCEL_SIGNALS: [libc::c_int; 4] =
    [libc::SIGTERM, libc::SIGINT, libc::SIGHUP, libc::SIGQUIT];

/// How often a step is looked at in its first moments, so a short step ends
/// promptly, and after them.
const FIRST_POLL: Duration = Duration::from_millis(2);
const FIRST: Duration = Duration::from_millis(100);
const POLL: Duration = Duration::from_millis(25);

/// The longest the end of a task waits for what was killed to be reaped.
const REAP_LEFTOVERS: Duration = Duration::from_millis(500);

/// The last cancelling signal this process received and has not acted on.
static PENDING: AtomicI32 = AtomicI32::new(0);

extern "C" fn on_signal(signal: libc::c_int) {
    PENDING.store(signal, Ordering::SeqCst);
}

/// Catch the cancelling signals. From here on they stop the task cleanly,
/// instead of ending this process and leaving its step running.
pub fn catch_cancel_signals() -> io::Result<()> {
    for signal in CANCEL_SIGNALS {
        // SAFETY: a zeroed sigaction is valid, and the handler only stores
        // to an atomic, which is async-signal-safe.
        unsafe {
            let mut action: libc::sigaction = std::mem::zeroed();
            action.sa_sigaction = on_signal as extern "C" fn(libc::c_int) as libc::sighandler_t;
            action.sa_flags = libc::SA_RESTART;
            libc::sigemptyset(&mut action.sa_mask);
            if libc::sigaction(signal, &action, std::ptr::null_mut()) != 0 {
                return Err(io::Error::last_os_error());
            }
        }
    }
    Ok(())
}

/// Adopt what the steps leave behind (Linux): an orphaned process is
/// reparented to this one, which reaps it, rather than to init. Elsewhere,
/// and where the kernel refuses, nothing changes.
pub fn become_subreaper() {
    #[cfg(target_os = "linux")]
    // SAFETY: prctl with an integer argument; its result is advisory here.
    unsafe {
        libc::prctl(libc::PR_SET_CHILD_SUBREAPER, 1 as libc::c_ulong);
    }
}

/// The cancelling signal this process has received and not acted on, taken.
pub(crate) fn take_cancel() -> Option<i32> {
    Some(PENDING.swap(0, Ordering::SeqCst)).filter(|signal| *signal != 0)
}

/// Run `job`'s steps, writing markers to `log`, and say what became of them.
pub fn run(job: &Job, log: &mut impl Write) -> JobResult {
    if job.protocol >= 2 {
        return crate::workflow::run(job, log);
    }
    let mut steps = Vec::with_capacity(job.steps.len());
    let mut groups = Vec::new();
    let mut exit_code = 0;
    let mut cancelled_by = None;
    // One file for the job, which each step may add to (`CI_STEP_SUMMARY`):
    // what it holds when the steps are over is the job's summary. It is
    // made new and this user's alone, so that a name another left there is
    // not written through. A job that cannot be given one runs all the
    // same.
    let summary_file =
        std::env::temp_dir().join(format!("dagron-ci-{}-summary.md", std::process::id()));
    let _ = std::fs::remove_file(&summary_file);
    let summarised = {
        use std::os::unix::fs::OpenOptionsExt;
        std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&summary_file)
            .is_ok()
    };
    if summarised {
        // This process starts no thread: its environment is its own to set.
        std::env::set_var("CI_STEP_SUMMARY", &summary_file);
    }
    for step in &job.steps {
        if cancelled_by.is_none() {
            let signal = PENDING.swap(0, Ordering::SeqCst);
            if signal != 0 {
                cancelled_by = Some(signal);
            }
        }
        if exit_code != 0 || cancelled_by.is_some() {
            steps.push(StepResult::skipped(step));
            continue;
        }

        marker(
            log,
            format_args!("step::{}::start::{}", step.n, Printable(&step.name)),
        );
        let started_at_ms = now_ms();
        let started = Instant::now();
        let ran = match spawn(step) {
            Ok(group) => {
                groups.push(group);
                wait(group, step, job.grace)
            }
            Err(e) => {
                marker(
                    log,
                    format_args!("warning::step {} could not start sh: {e}", step.n),
                );
                Ran {
                    code: 127,
                    signal: None,
                    timed_out: false,
                    cancelled: None,
                }
            }
        };
        marker(log, format_args!("step::{}::end::{}", step.n, ran.code));

        let outcome = if ran.cancelled.is_some() {
            Outcome::Cancelled
        } else if ran.code == 0 {
            Outcome::Success
        } else if ran.timed_out {
            Outcome::TimedOut
        } else {
            Outcome::Failure
        };
        let mut conclusion = outcome;
        match outcome {
            Outcome::Cancelled => cancelled_by = ran.cancelled,
            Outcome::Failure | Outcome::TimedOut if step.continue_on_error => {
                marker(log, format_args!("step::{}::continued", step.n));
                conclusion = Outcome::Success;
            }
            Outcome::Failure | Outcome::TimedOut => exit_code = ran.code,
            Outcome::Success | Outcome::Skipped => {}
        }
        steps.push(StepResult {
            n: step.n,
            name: step.name.clone(),
            outcome,
            conclusion,
            exit_code: Some(ran.code),
            signal: ran.signal,
            started_at_ms: Some(started_at_ms),
            duration_ms: Some(u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX)),
            timeout_secs: step.timeout.map(|t| t.as_secs()),
        });
    }
    if let Some(signal) = cancelled_by {
        marker(log, format_args!("cancelled::{signal}"));
        exit_code = 128 + signal;
    }
    end_leftovers(&groups);
    let summary = match summarised {
        true => {
            let text = crate::workflow::read_summary(&summary_file);
            let _ = std::fs::remove_file(&summary_file);
            text
        }
        false => String::new(),
    };
    JobResult {
        protocol: job.protocol,
        exit_code,
        cancelled_by,
        steps,
        said: None,
        summary,
    }
}

/// What became of one step's process.
#[derive(Debug)]
pub(crate) struct Ran {
    pub(crate) code: i32,
    pub(crate) signal: Option<i32>,
    pub(crate) timed_out: bool,
    pub(crate) cancelled: Option<i32>,
}

/// Start `sh -ec SCRIPT` with the step's own variables, as the leader of a
/// new process group. Its pid, which is also its group's id.
fn spawn(step: &Step) -> io::Result<libc::pid_t> {
    let mut command = Command::new("sh");
    command.arg("-ec").arg(&step.run);
    for (name, value) in &step.env {
        command.env(name, value);
    }
    spawn_command(command)
}

/// Start `command` as a step: with no input, as the leader of a new process
/// group, to die with this process. Its pid, which is also its group's id.
pub(crate) fn spawn_command(mut command: Command) -> io::Result<libc::pid_t> {
    command
        // A step reads no input. A terminal's would stop it with SIGTTIN, as
        // it is not in the foreground group.
        .stdin(Stdio::null())
        .process_group(0);
    #[cfg(target_os = "linux")]
    {
        // SAFETY: getpid is always safe.
        let parent = unsafe { libc::getpid() };
        // SAFETY: between fork and exec the closure makes only
        // async-signal-safe calls (prctl, getppid).
        unsafe {
            command.pre_exec(move || {
                // Should this process be killed, the step's shell goes too.
                if libc::prctl(libc::PR_SET_PDEATHSIG, libc::SIGKILL as libc::c_ulong) != 0 {
                    return Err(io::Error::last_os_error());
                }
                // This process may have died before the setting took, when
                // nothing would ever send it.
                if libc::getppid() != parent {
                    return Err(io::Error::from_raw_os_error(libc::ESRCH));
                }
                Ok(())
            });
        }
    }
    let child = command.spawn()?;
    // Reaped by `wait`, with waitpid on any child, so the orphans this
    // process adopts are reaped too; the handle is not needed for that.
    libc::pid_t::try_from(child.id()).map_err(|_| io::Error::from_raw_os_error(libc::ERANGE))
}

/// Wait for the step whose group is `group` to end, stopping it at its
/// timeout and passing a cancelling signal on to it.
pub(crate) fn wait(group: libc::pid_t, step: &Step, grace: Duration) -> Ran {
    let started = Instant::now();
    let deadline = step.timeout.map(|t| started + t);
    let mut stop_sent: Option<Instant> = None;
    let mut killed = false;
    let mut timed_out = false;
    let mut cancelled = None;
    loop {
        match reap(group) {
            Some(Some(status)) => {
                let (code, signal) = decode(status);
                // timeout(1)'s codes, which the shell fallback reports: 124
                // for a step stopped at its timeout, 137 when it was killed.
                let code = if timed_out && signal != Some(libc::SIGKILL) {
                    124
                } else {
                    code
                };
                return Ran {
                    code,
                    signal,
                    timed_out,
                    cancelled,
                };
            }
            // Not this process's child any more: nothing can say how it
            // ended.
            Some(None) => {
                return Ran {
                    code: 125,
                    signal: None,
                    timed_out,
                    cancelled,
                }
            }
            None => {}
        }
        // The step's shell is unreaped here, so its group id is still its own
        // and every signal below reaches the step and nothing else.
        let now = Instant::now();
        let signal = PENDING.swap(0, Ordering::SeqCst);
        if signal != 0 {
            signal_group(group, signal);
            cancelled = Some(signal);
            stop_sent.get_or_insert(now);
        }
        if cancelled.is_none() && !timed_out && deadline.is_some_and(|d| now >= d) {
            signal_group(group, libc::SIGTERM);
            timed_out = true;
            stop_sent = Some(now);
        }
        if !killed && stop_sent.is_some_and(|sent| now.duration_since(sent) >= grace) {
            signal_group(group, libc::SIGKILL);
            killed = true;
        }
        std::thread::sleep(if now.duration_since(started) < FIRST {
            FIRST_POLL
        } else {
            POLL
        });
    }
}

/// Reap whatever children have ended. `Some(Some(status))` once `step` has,
/// `Some(None)` when this process has no children at all (the step was
/// reaped elsewhere), `None` while it runs.
fn reap(step: libc::pid_t) -> Option<Option<libc::c_int>> {
    loop {
        let mut status: libc::c_int = 0;
        // SAFETY: waitpid writes only to `status`.
        let got = unsafe { libc::waitpid(-1, &mut status, libc::WNOHANG) };
        if got == step {
            return Some(Some(status));
        }
        if got > 0 {
            // An orphan this process adopted, reaped. Nothing more to do.
            continue;
        }
        if got == -1 && io::Error::last_os_error().raw_os_error() == Some(libc::ECHILD) {
            return Some(None);
        }
        return None;
    }
}

/// A wait status as a shell reports it: the exit code, or 128 plus the
/// signal that ended the process, with that signal.
fn decode(status: libc::c_int) -> (i32, Option<i32>) {
    if libc::WIFEXITED(status) {
        (libc::WEXITSTATUS(status), None)
    } else if libc::WIFSIGNALED(status) {
        let signal = libc::WTERMSIG(status);
        (128 + signal, Some(signal))
    } else {
        (125, None)
    }
}

fn signal_group(group: libc::pid_t, signal: libc::c_int) {
    // SAFETY: kill takes plain integers. A group that is already gone
    // (ESRCH) is not an error here.
    unsafe {
        libc::kill(-group, signal);
    }
}

/// Kill what the steps left running, and reap it. Only a group that still
/// holds a child of this process is signalled: that proves it is the step's
/// group, and not a number the system has since given to someone else.
pub(crate) fn end_leftovers(groups: &[libc::pid_t]) {
    for &group in groups {
        if holds_a_child(group) {
            signal_group(group, libc::SIGKILL);
        }
    }
    let until = Instant::now() + REAP_LEFTOVERS;
    loop {
        let mut status: libc::c_int = 0;
        // SAFETY: waitpid writes only to `status`.
        let got = unsafe { libc::waitpid(-1, &mut status, libc::WNOHANG) };
        if got > 0 {
            continue;
        }
        // -1 is ECHILD: no children left.
        if got == -1 || Instant::now() >= until {
            return;
        }
        std::thread::sleep(FIRST_POLL);
    }
}

/// Whether a child of this process is still in process group `group`.
fn holds_a_child(group: libc::pid_t) -> bool {
    loop {
        let mut status: libc::c_int = 0;
        // SAFETY: waitpid writes only to `status`.
        match unsafe { libc::waitpid(-group, &mut status, libc::WNOHANG) } {
            0 => return true,
            got if got > 0 => continue,
            _ => return false,
        }
    }
}

/// One marker line: `::dagron-ci::…`. A log that cannot be written to does
/// not stop the steps.
pub(crate) fn marker(log: &mut impl Write, args: fmt::Arguments) {
    let _ = writeln!(log, "::dagron-ci::{args}");
    let _ = log.flush();
}

/// A name with its control characters as spaces, so a marker is one line.
pub(crate) struct Printable<'a>(pub(crate) &'a str);

impl fmt::Display for Printable<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for c in self.0.chars() {
            write!(f, "{}", if c.is_control() { ' ' } else { c })?;
        }
        Ok(())
    }
}

pub(crate) fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_wait_status_reads_as_a_shell_reports_it() {
        // Exited 3; killed by SIGKILL; killed by SIGTERM.
        assert_eq!(decode(3 << 8), (3, None));
        assert_eq!(decode(libc::SIGKILL), (137, Some(libc::SIGKILL)));
        assert_eq!(decode(libc::SIGTERM), (143, Some(libc::SIGTERM)));
    }

    #[test]
    fn a_marker_is_one_line_whatever_a_name_holds() {
        let mut log = Vec::new();
        marker(
            &mut log,
            format_args!("step::1::start::{}", Printable("a\nb\tc")),
        );
        assert_eq!(
            String::from_utf8(log).unwrap(),
            "::dagron-ci::step::1::start::a b c\n"
        );
    }
}

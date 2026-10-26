//! GitHub's workflow commands, read as a job prints them: what a step asks
//! to have masked (`::add-mask::`), with the job's own credentials masked
//! from the start, and what it asks to have annotated (`::error::`,
//! `::warning::`, `::notice::`), as GitHub's runner reads them.
//!
//! **How.** The job is one stream under step protocol 3 (standard error is
//! standard output). Before the first step this process makes that stream a
//! pipe ([`Relay::start`]), which every step's shell, and whatever it
//! starts, inherits; a thread of this process copies the pipe to the stream
//! it was, a line at a time. What a step's container said goes the same
//! way. What this process says itself, its markers and its result, does
//! not: each of its lines waits in a queue, and this process asks the
//! relay, on a pipe of its own that no step holds, to print it ([`Own`]).
//! The relay first reads what is in the steps' pipe as the ask comes, which
//! holds every byte a step that has ended wrote, so that the line is in
//! its place; and nothing a step prints, or reads of the steps' pipe
//! itself, is an ask or keeps one waiting. This process is not dumpable:
//! no step opens its descriptors under `/proc`.
//!
//! - **A mask** is any value a line `::add-mask::VALUE` names, `%25`,
//!   `%0D` and `%0A` read as GitHub escapes them, from that line on; each
//!   line of a value of several is one. The line itself is printed
//!   `::add-mask::***`. And from the start, the job's own credentials
//!   ([`CREDENTIALS`]). A masked value is printed `***`, wherever in a line
//!   it is, the longest first.
//! - **An annotation**: a line `::error file=F,line=L,endLine=E,title=T::M`
//!   (or `::warning`, `::notice`) is printed as it is, and after it the
//!   annotation in the line dagron-ci reads a job's annotations from
//!   (`::dagron-ci::annotation::`), so that a check run has it. `file` is
//!   the repository's path, and one under the job's workspace is read as
//!   the rest; one named by no file is the workflow's (`.github`), as on
//!   GitHub. [`ANNOTATIONS_MAX`] of a job's are said; dagron-ci holds them
//!   to a path inside the repository and to its own bounds.
//! - **`::stop-commands::TOKEN`** stops reading commands until a line
//!   `::TOKEN::`: an `::add-mask::` or an `::error::` between the two is
//!   printed as it is, and does nothing, as on GitHub.
//! - **A line** ends at `\n` or `\r` (a progress bar's), or at
//!   [`LINE_MAX`] bytes, which is read and printed as it is. A value cut in
//!   two there is not masked: GitHub's runner cuts its lines too.
//!
//! When the steps are over ([`Relay::settle`]) only this process's lines
//! are printed, and when it has said its last ([`Relay::finish`]) the
//! stream is given back: what a process a step left prints after is not
//! printed, and is not waited on.

use std::collections::VecDeque;
use std::io::{self, Write};
use std::os::fd::{FromRawFd, RawFd};
use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::Duration;

/// The longest line read before it is printed whole.
pub const LINE_MAX: usize = 64 * 1024;

/// How long the relay is waited for, where the job ends in a panic.
pub const DRAIN: Duration = Duration::from_secs(2);

/// The annotations of a job's steps said for its check run: what a forge
/// takes in one request.
pub const ANNOTATIONS_MAX: usize = crate::report::ANNOTATIONS_MAX;

/// Where a step's container sees the job's workspace
/// (`crate::container::WORKSPACE`): a `file` under it is read as the rest.
const CONTAINER_WORKSPACE: &str = "/github/workspace";

/// The variables whose values are the job's own credentials, masked from
/// the start.
pub const CREDENTIALS: &[&str] = &[
    "GITHUB_TOKEN",
    "ACTIONS_RUNTIME_TOKEN",
    "ACTIONS_ID_TOKEN_REQUEST_TOKEN",
    "DAGRON_OIDC_REQUEST_TOKEN",
];

/// What the commands read so far have asked for.
#[derive(Debug, Default)]
pub struct Commands {
    masks: Vec<Vec<u8>>,
    /// The job's workspace, as a `file` under it is written.
    workspace: Option<String>,
    /// The token that ends `::stop-commands::`, while commands are stopped.
    stopped: Option<String>,
    annotations: usize,
    /// Whether a step's `::debug::` lines are printed.
    debug: bool,
}

impl Commands {
    /// Commands of a job in `workspace`, with `masks` masked from the start
    /// (each line of each one).
    pub fn new<I: IntoIterator<Item = String>>(workspace: Option<String>, masks: I) -> Commands {
        let mut commands = Commands {
            workspace: workspace.map(|w| w.trim_end_matches('/').to_string()),
            ..Commands::default()
        };
        for value in masks {
            commands.mask(&value);
        }
        commands
    }

    fn mask(&mut self, value: &str) {
        for line in value.split(['\n', '\r']) {
            let line = line.trim();
            if line.is_empty() {
                continue;
            }
            // And as a workflow command's data and its properties escape it
            // (`%25`, `%3A`, `%2C`), where a step's `::error::` writes it so.
            let data = line.replace('%', "%25");
            let property = data.replace(':', "%3A").replace(',', "%2C");
            for form in [line.to_string(), data, property] {
                if !self.masks.iter().any(|v| v == form.as_bytes()) {
                    self.masks.push(form.into_bytes());
                }
            }
        }
    }

    /// `text` with every masked value `***`: each place a value matches,
    /// overlapping ones made one, so that no part of one is left showing.
    fn masked(&self, text: &[u8]) -> Vec<u8> {
        let mut spans: Vec<(usize, usize)> = Vec::new();
        for value in self.masks.iter().filter(|v| !v.is_empty()) {
            let mut at = 0;
            while at + value.len() <= text.len() {
                if text[at..].starts_with(value) {
                    spans.push((at, at + value.len()));
                    at += 1;
                } else {
                    at += 1;
                }
            }
        }
        if spans.is_empty() {
            return text.to_vec();
        }
        spans.sort_unstable();
        let mut out = Vec::with_capacity(text.len());
        let mut from = 0;
        let mut i = 0;
        while i < spans.len() {
            let (start, mut end) = spans[i];
            while i + 1 < spans.len() && spans[i + 1].0 <= end {
                end = end.max(spans[i + 1].1);
                i += 1;
            }
            out.extend_from_slice(&text[from..start]);
            out.extend_from_slice(b"***");
            from = end;
            i += 1;
        }
        out.extend_from_slice(&text[from..]);
        out
    }

    /// These commands, with a step's `::debug::` lines printed where `on`
    /// (`ACTIONS_STEP_DEBUG`), and passed over where not, as on GitHub.
    pub fn with_debug(mut self, on: bool) -> Commands {
        self.debug = on;
        self
    }

    /// Whether `line` is printed at all: a `::debug::` line is not, where
    /// debug is off and commands are not stopped.
    pub fn hides(&self, line: &[u8]) -> bool {
        !self.debug && self.stopped.is_none() && line.trim_ascii_start().starts_with(b"::debug::")
    }

    /// `text` with every masked value `***`.
    pub fn mask_text(&self, text: &str) -> String {
        String::from_utf8_lossy(&self.masked(text.as_bytes())).into_owned()
    }

    /// A value of `::add-mask::` that is not UTF-8, as its bytes came: each
    /// of its lines a mask, as a value of text is.
    fn mask_bytes(&mut self, value: &[u8]) {
        let value = unescape_bytes(value);
        for line in value.split(|b| *b == b'\n' || *b == b'\r') {
            let line = line.trim_ascii();
            if !line.is_empty() && !self.masks.iter().any(|v| v == line) {
                self.masks.push(line.to_vec());
            }
        }
    }

    /// One of the runner's own lines, as it is printed: masked only where
    /// a step's words are in it (a step's name, what is said of a step),
    /// so that a mask as short as `1` breaks no marker. What the runner
    /// said as JSON was masked where it was made, a value at a time.
    pub fn own_line(&self, line: &[u8]) -> Vec<u8> {
        const PROTOCOL: &[u8] = b"::dagron-ci::";
        const AS_THEY_ARE: [&[u8]; 7] = [
            b"result",
            b"job",
            b"report",
            b"annotation",
            b"failed",
            b"summary",
            b"cancelled",
        ];
        let Some(rest) = line.strip_prefix(PROTOCOL) else {
            return self.masked(line);
        };
        let Some(at) = find(rest, b"::") else {
            return line.to_vec();
        };
        let (kind, words) = (&rest[..at], &rest[at + 2..]);
        let kept = PROTOCOL.len() + at + 2;
        if AS_THEY_ARE.contains(&kind) {
            return line.to_vec();
        }
        let from = if kind == b"step" {
            // `step::N::start::NAME`: the name is the workflow's words.
            match find(words, b"::start::") {
                Some(at) => kept + at + b"::start::".len(),
                None => return line.to_vec(),
            }
        } else {
            kept
        };
        let mut out = line[..from].to_vec();
        out.extend_from_slice(&self.masked(&line[from..]));
        out
    }

    /// `line` (without its end), as it is printed, and the annotation line
    /// to print after it where it asked for one: both masked, the
    /// annotation in each of its fields, so that its JSON stays JSON.
    pub fn line(&mut self, line: &[u8]) -> (Vec<u8>, Option<Vec<u8>>) {
        let text = String::from_utf8_lossy(line);
        let command = text.trim();
        let mut shown: Option<Vec<u8>> = None;
        let mut annotation = None;
        match &self.stopped {
            Some(token) => {
                if command == format!("::{token}::") {
                    self.stopped = None;
                }
            }
            None => {
                if let Some(value) = command.strip_prefix("::add-mask::") {
                    match std::str::from_utf8(line) {
                        Ok(_) => self.mask(&unescape(value, false)),
                        Err(_) => {
                            let at = find(line, b"::add-mask::").unwrap_or(0);
                            self.mask_bytes(&line[at + b"::add-mask::".len()..]);
                        }
                    }
                    shown = Some(b"::add-mask::***".to_vec());
                } else if let Some(token) = command.strip_prefix("::stop-commands::") {
                    let token = token.trim();
                    if !token.is_empty() {
                        self.stopped = Some(token.to_string());
                    }
                } else if let Some(said) = self.annotation(command) {
                    annotation = Some(said);
                }
            }
        }
        let shown = self.masked(&shown.unwrap_or_else(|| line.to_vec()));
        (shown, annotation.map(String::into_bytes))
    }

    /// The `::dagron-ci::annotation::` line a command `::error …::` (or
    /// `::warning`, `::notice`) asks for; `None` for any other line, and
    /// past [`ANNOTATIONS_MAX`].
    fn annotation(&mut self, command: &str) -> Option<String> {
        let rest = command.strip_prefix("::")?;
        let (head, message) = rest.split_once("::")?;
        let (name, properties) = head.split_once(' ').unwrap_or((head, ""));
        let level = match name {
            "error" => "failure",
            "warning" => "warning",
            "notice" => "notice",
            _ => return None,
        };
        if self.annotations >= ANNOTATIONS_MAX {
            return None;
        }
        self.annotations += 1;
        let mut file = None;
        let mut start: Option<u64> = None;
        let mut end: Option<u64> = None;
        let mut title = String::new();
        for property in properties.split(',') {
            let Some((key, value)) = property.split_once('=') else {
                continue;
            };
            let value = unescape(value.trim(), true);
            match key.trim() {
                "file" => file = Some(value),
                "line" => start = value.parse().ok(),
                "endLine" => end = value.parse().ok(),
                "title" => title = value,
                _ => {}
            }
        }
        let path = match file {
            None => ".github".to_string(),
            Some(file) => {
                // Under the job's workspace, or under a step container's,
                // which sees it at `/github/workspace`: the rest.
                let under = [self.workspace.as_deref(), Some(CONTAINER_WORKSPACE)]
                    .into_iter()
                    .flatten()
                    .find_map(|ws| {
                        file.strip_prefix(ws)
                            .and_then(|rest| rest.strip_prefix('/'))
                    });
                let file = under.map(str::to_string).unwrap_or(file);
                file.strip_prefix("./").map(str::to_string).unwrap_or(file)
            }
        };
        let text = |t: &str| String::from_utf8_lossy(&self.masked(t.as_bytes())).into_owned();
        let start = start.filter(|l| *l >= 1).unwrap_or(1);
        Some(format!(
            "::dagron-ci::annotation::{}",
            serde_json::json!({
                "path": text(&path),
                "start_line": start,
                "end_line": end.filter(|e| *e >= start).unwrap_or(start),
                "level": level,
                "title": text(&title),
                "message": text(&unescape(message, false)),
            })
        ))
    }
}

/// A command's data as GitHub escapes it: `%0D`, `%0A`, then `%25`; and in
/// a property's value `%3A` and `%2C` too.
fn unescape(value: &str, property: bool) -> String {
    let mut value = value.replace("%0D", "\r").replace("%0A", "\n");
    if property {
        value = value.replace("%3A", ":").replace("%2C", ",");
    }
    value.replace("%25", "%")
}

/// [`unescape`] of a command's data, on its bytes.
fn unescape_bytes(value: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(value.len());
    let mut i = 0;
    while i < value.len() {
        let three = value.get(i..i + 3);
        let byte = match three {
            Some(b"%0D") => Some(b'\r'),
            Some(b"%0A") => Some(b'\n'),
            Some(b"%25") => Some(b'%'),
            _ => None,
        };
        match byte {
            Some(b) => {
                out.push(b);
                i += 3;
            }
            None => {
                out.push(value[i]);
                i += 1;
            }
        }
    }
    out
}

/// How the relay prints, as the job goes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Stage {
    /// The steps run: their lines are read and masked, and the runner's
    /// own printed where it asks.
    Steps,
    /// The steps are over: every mask is known, and the runner's own lines
    /// alone are printed. What a process a step left behind prints now is
    /// not, so that the job's last line stays its last.
    Own,
    /// The runner's last line is printed: nothing more is.
    Closed,
}

/// What the runner asks of the relay: print its next own line; the steps
/// are over; its last line is printed.
pub const ASK_LINE: u8 = b'l';
pub const ASK_END: u8 = b'e';
pub const ASK_CLOSE: u8 = b'c';

/// Where the relay prints: `None` once the stream is given back.
pub type Output = Mutex<Option<Box<dyn Write + Send>>>;

/// What the relay's thread and the runner share.
#[derive(Default)]
pub struct Shared {
    pub output: Output,
    pub commands: Mutex<Commands>,
    /// The runner's own lines, in their order, each printed when the runner
    /// asks: after every line of the steps' before it, and never in their
    /// stream, so that none is cut, bounded, or read as a step's.
    pub queue: Mutex<VecDeque<Vec<u8>>>,
}

impl Shared {
    fn write(&self, bytes: &[u8]) {
        if let Ok(mut out) = self.output.lock() {
            if let Some(out) = out.as_mut() {
                let _ = out.write_all(bytes);
                let _ = out.flush();
            }
        }
    }
}

/// The relay's reading of the job's stream: the steps' lines so far, and
/// how the job stands.
pub struct Reader<'a> {
    shared: &'a Shared,
    pending: Vec<u8>,
    /// How much of `pending` holds no end: each byte is looked through
    /// once, however a long line comes.
    scanned: usize,
    /// Whether a step's line was printed cut at its bound, with no end.
    open: bool,
    /// Whether the line being read is one not printed (a `::debug::`
    /// line where debug is off), cut at its bound: the rest of it is not
    /// printed either.
    hiding: bool,
    stage: Stage,
}

impl<'a> Reader<'a> {
    pub fn new(shared: &'a Shared) -> Reader<'a> {
        Reader {
            shared,
            pending: Vec::new(),
            scanned: 0,
            open: false,
            hiding: false,
            stage: Stage::Steps,
        }
    }

    /// A step's line, and how it ended: `\n`, `\r`, `\r\n`, or nothing for
    /// a line cut at its bound, or the last.
    fn line(&mut self, text: &[u8], end: &[u8]) {
        if self.stage != Stage::Steps {
            return;
        }
        // The rest of a line not printed, until its end.
        if self.hiding {
            self.hiding = end.is_empty();
            return;
        }
        let (mut shown, annotation) = match self.shared.commands.lock() {
            Ok(commands) if commands.hides(text) => {
                self.hiding = end.is_empty();
                return;
            }
            Ok(mut commands) => commands.line(text),
            Err(_) => (b"***".to_vec(), None),
        };
        shown.extend_from_slice(end);
        if let Some(annotation) = annotation {
            // On a line of its own, after the line that asked for it.
            if !end.ends_with(b"\n") {
                shown.push(b'\n');
            }
            shown.extend_from_slice(&annotation);
            shown.push(b'\n');
        }
        self.shared.write(&shown);
    }

    /// What the steps printed next: each whole line read through the
    /// commands while the steps run, and passed over after.
    pub fn feed(&mut self, bytes: &[u8]) {
        self.pending.extend_from_slice(bytes);
        loop {
            let Some(at) = self.pending[self.scanned..]
                .iter()
                .position(|b| *b == b'\n' || *b == b'\r')
                .map(|at| at + self.scanned)
            else {
                self.scanned = self.pending.len();
                break;
            };
            // `\r\n` is one end: a `\r` last is held until what follows it
            // has come.
            let end = match (self.pending[at], self.pending.get(at + 1)) {
                (b'\r', Some(b'\n')) => 2,
                (b'\r', None) => {
                    self.scanned = at;
                    break;
                }
                _ => 1,
            };
            self.scanned = 0;
            let taken: Vec<u8> = self.pending.drain(..at + end).collect();
            self.line(&taken[..at], &taken[at..]);
            self.open = false;
        }
        if self.pending.len() >= LINE_MAX {
            let text = std::mem::take(&mut self.pending);
            self.scanned = 0;
            self.line(&text, b"");
            self.open = true;
        }
    }

    /// The steps' stream is over: what is left of it is printed.
    pub fn end(&mut self) {
        if !self.pending.is_empty() {
            let text = std::mem::take(&mut self.pending);
            self.scanned = 0;
            self.line(&text, b"");
            self.open = true;
        }
    }

    /// The runner asks, once every byte of the steps' before it is fed. A
    /// step's line without its end (the last a step printed) ends here: the
    /// runner's line is one of its own.
    pub fn ask(&mut self, word: u8) {
        if !self.pending.is_empty() || self.open {
            let mut text = std::mem::take(&mut self.pending);
            let end: &[u8] = if text.last() == Some(&b'\r') {
                text.pop();
                b"\r\n"
            } else {
                b"\n"
            };
            self.scanned = 0;
            self.line(&text, end);
            self.open = false;
        }
        match word {
            ASK_LINE => {
                let line = self
                    .shared
                    .queue
                    .lock()
                    .ok()
                    .and_then(|mut q| q.pop_front());
                if let (Some(line), false) = (line, self.stage == Stage::Closed) {
                    let shown = match self.shared.commands.lock() {
                        Ok(commands) => commands.own_line(&line),
                        Err(_) => line,
                    };
                    self.shared.write(&shown);
                }
            }
            ASK_END if self.stage == Stage::Steps => self.stage = Stage::Own,
            ASK_CLOSE => self.stage = Stage::Closed,
            _ => {}
        }
    }
}

/// The relay's thread: the steps' stream (`steps`, the pipe's read end,
/// not blocking) and the runner's asks (`asks`, a pipe of its own no step
/// holds), read until the runner has none left. Each ask is answered, on
/// `told`, once the bytes of the steps' in the pipe as it came are read:
/// every byte a step that has ended wrote is among them, and what a step
/// that reads the pipe itself takes of them is not waited for.
fn serve(mut reader: Reader, steps: RawFd, asks: RawFd, told: mpsc::Sender<()>) {
    let mut buf = [0u8; 8192];
    let mut open = true;
    loop {
        let mut fds = [
            libc::pollfd {
                fd: asks,
                events: libc::POLLIN,
                revents: 0,
            },
            libc::pollfd {
                fd: if open { steps } else { -1 },
                events: libc::POLLIN,
                revents: 0,
            },
        ];
        // SAFETY: `fds` is two pollfds, as said.
        if unsafe { libc::poll(fds.as_mut_ptr(), 2, -1) } < 0 {
            if io::Error::last_os_error().kind() == io::ErrorKind::Interrupted {
                continue;
            }
            return;
        }
        if fds[0].revents != 0 {
            let mut word = [0u8; 1];
            // SAFETY: one byte into `word`.
            match unsafe { libc::read(asks, word.as_mut_ptr().cast(), 1) } {
                1 => {
                    if open {
                        let mut waiting: libc::c_int = 0;
                        // SAFETY: FIONREAD writes the bytes in the pipe.
                        unsafe { libc::ioctl(steps, libc::FIONREAD, &mut waiting) };
                        let mut left = usize::try_from(waiting).unwrap_or(0);
                        while left > 0 {
                            let want = left.min(buf.len());
                            // SAFETY: at most `want` bytes into `buf`.
                            let n = unsafe { libc::read(steps, buf.as_mut_ptr().cast(), want) };
                            match n {
                                n if n > 0 => {
                                    let n = n as usize;
                                    reader.feed(&buf[..n]);
                                    left -= n;
                                }
                                0 => {
                                    open = false;
                                    reader.end();
                                    break;
                                }
                                _ if io::Error::last_os_error().kind()
                                    == io::ErrorKind::Interrupted => {}
                                // Taken by another reader: not waited for.
                                _ => break,
                            }
                        }
                    }
                    reader.ask(word[0]);
                    let _ = told.send(());
                }
                // The runner is gone.
                0 => return,
                _ => {
                    let kind = io::Error::last_os_error().kind();
                    if kind != io::ErrorKind::Interrupted && kind != io::ErrorKind::WouldBlock {
                        return;
                    }
                }
            }
            continue;
        }
        if fds[1].revents != 0 {
            // SAFETY: at most `buf.len()` bytes into `buf`.
            match unsafe { libc::read(steps, buf.as_mut_ptr().cast(), buf.len()) } {
                n if n > 0 => reader.feed(&buf[..n as usize]),
                0 => {
                    open = false;
                    reader.end();
                }
                _ => {
                    let kind = io::Error::last_os_error().kind();
                    if kind != io::ErrorKind::Interrupted && kind != io::ErrorKind::WouldBlock {
                        open = false;
                        reader.end();
                    }
                }
            }
        }
    }
}

/// What the relay and the runner's own writers hold.
struct Inner {
    shared: Arc<Shared>,
    /// The asks pipe's write end: this process's alone.
    asks: RawFd,
    /// Told as the relay answers each ask.
    told: Mutex<mpsc::Receiver<()>>,
}

impl Inner {
    /// Ask, and wait for the answer (for as long as `bound`, where there is
    /// one): whether it was asked.
    fn ask(&self, word: u8, bound: Option<Duration>) -> bool {
        // One waiting at a time: each answer is its own ask's.
        let Ok(told) = self.told.lock() else {
            return false;
        };
        loop {
            // SAFETY: one byte of this process's own pipe.
            let n = unsafe { libc::write(self.asks, [word].as_ptr().cast(), 1) };
            if n == 1 {
                break;
            }
            if n < 0 && io::Error::last_os_error().kind() == io::ErrorKind::Interrupted {
                continue;
            }
            return false;
        }
        // A relay that is gone answers nothing: not asked, so that the
        // runner prints its line itself.
        match bound {
            Some(bound) => told.recv_timeout(bound).is_ok(),
            None => told.recv().is_ok(),
        }
    }

    /// The runner's own line, printed in its place.
    fn own(&self, line: Vec<u8>) {
        if let Ok(mut queue) = self.shared.queue.lock() {
            queue.push_back(line);
        }
        if !self.ask(ASK_LINE, None) {
            // Not asked: printed now, rather than not at all.
            let line = self.shared.queue.lock().ok().and_then(|mut q| q.pop_back());
            if let Some(line) = line {
                let shown = match self.shared.commands.lock() {
                    Ok(commands) => commands.own_line(&line),
                    Err(_) => line,
                };
                self.shared.write(&shown);
            }
        }
    }
}

/// A writer of the step runner's own lines: each, once whole, printed by
/// the relay after every line of the steps' before it, and masked only
/// where a step's words are in it ([`Commands::own_line`]).
pub struct Own {
    inner: Arc<Inner>,
    line: Vec<u8>,
}

impl Write for Own {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        for &b in buf {
            self.line.push(b);
            if b == b'\n' {
                self.inner.own(std::mem::take(&mut self.line));
            }
        }
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl Drop for Own {
    fn drop(&mut self) {
        if !self.line.is_empty() {
            let mut line = std::mem::take(&mut self.line);
            line.push(b'\n');
            self.inner.own(line);
        }
    }
}

/// The job's stream, made a pipe a thread copies, read, to what it was.
pub struct Relay {
    inner: Arc<Inner>,
    /// The stream as it was: given back to this process's standard output
    /// and error when the job is over.
    original: RawFd,
    finished: bool,
    _thread: JoinHandle<()>,
}

impl Relay {
    /// Make this process's standard output and error one pipe, copied by a
    /// thread to what standard output was, through `commands`. Called
    /// before any step starts, by a process whose standard error is its
    /// standard output (step protocol 3).
    pub fn start(commands: Commands) -> io::Result<Relay> {
        // No step, which runs as this process's user, opens this process's
        // descriptors or reads its memory under `/proc`: not the stream as
        // it was, nor the runner's asks.
        #[cfg(target_os = "linux")]
        // SAFETY: a flag of this process's own.
        if unsafe { libc::prctl(libc::PR_SET_DUMPABLE, 0, 0, 0, 0) } != 0 {
            return Err(io::Error::last_os_error());
        }
        let pipe = || -> io::Result<(RawFd, RawFd)> {
            let mut fds = [0 as RawFd; 2];
            // SAFETY: `fds` has room for the two descriptors pipe makes.
            // Not handed to the steps, from the start.
            if unsafe { libc::pipe2(fds.as_mut_ptr(), libc::O_CLOEXEC) } != 0 {
                return Err(io::Error::last_os_error());
            }
            Ok((fds[0], fds[1]))
        };
        let (read_end, write_end) = pipe()?;
        let (asks_read, asks_write) = pipe()?;
        // SAFETY: plain descriptor calls on this process's own descriptors.
        let (original, copy, panics) = unsafe {
            let original = libc::dup(1);
            (original, libc::dup(original), libc::dup(original))
        };
        if [original, copy, panics].iter().any(|fd| *fd < 0) {
            return Err(io::Error::last_os_error());
        }
        // Not handed to the steps: only the pipe's write end is, as their
        // standard output and error.
        for fd in [original, copy, panics] {
            // SAFETY: as above.
            unsafe { libc::fcntl(fd, libc::F_SETFD, libc::FD_CLOEXEC) };
        }
        // The read end is the relay's own description: what a step sets on
        // its end, or on one it opens itself, is not the relay's.
        // SAFETY: as above.
        unsafe {
            let flags = libc::fcntl(read_end, libc::F_GETFL);
            libc::fcntl(read_end, libc::F_SETFL, flags | libc::O_NONBLOCK);
        }
        // SAFETY: `copy` is a descriptor this process owns and gives here.
        let out: Box<dyn Write + Send> = Box::new(unsafe { std::fs::File::from_raw_fd(copy) });
        let shared = Arc::new(Shared {
            output: Mutex::new(Some(out)),
            commands: Mutex::new(commands),
            queue: Mutex::default(),
        });
        let (tell, told) = mpsc::channel();
        let thread_shared = Arc::clone(&shared);
        let thread = std::thread::Builder::new()
            .name("commands".into())
            .spawn(move || serve(Reader::new(&thread_shared), read_end, asks_read, tell))?;
        // SAFETY: as above; the write end becomes standard output and error.
        unsafe {
            if libc::dup2(write_end, 1) < 0 || libc::dup2(write_end, 2) < 0 {
                return Err(io::Error::last_os_error());
            }
            libc::close(write_end);
        }
        // A panic's words go to the stream as it was: after the steps, a
        // line of the steps' stream is not printed.
        let panic_shared = Arc::clone(&shared);
        std::panic::set_hook(Box::new(move |info| {
            let words = format!("{info}");
            let words = match panic_shared.commands.try_lock() {
                Ok(commands) => commands.mask_text(&words),
                Err(_) => String::new(),
            };
            let said = format!("\n::dagron-ci::error::the step runner failed: {words}\n");
            // SAFETY: `panics` is this process's, and `said` as long as said.
            unsafe { libc::write(panics, said.as_ptr().cast(), said.len()) };
        }));
        Ok(Relay {
            inner: Arc::new(Inner {
                shared,
                asks: asks_write,
                told: Mutex::new(told),
            }),
            original,
            finished: false,
            _thread: thread,
        })
    }

    /// A writer of the runner's own lines ([`Own`]).
    pub fn own(&self) -> Own {
        Own {
            inner: Arc::clone(&self.inner),
            line: Vec::new(),
        }
    }

    /// Say the steps are over, and wait until the relay has read every line
    /// of theirs before: from then on every mask is known, and only the
    /// runner's own lines are printed.
    pub fn settle(&self) {
        self.inner.ask(ASK_END, None);
    }

    /// Whether `text` holds a masked value: an output the jobs after this
    /// one would read it in is not handed on, as on GitHub.
    pub fn masks(&self, text: &str) -> bool {
        self.inner
            .shared
            .commands
            .lock()
            .map(|c| c.masked(text.as_bytes()) != text.as_bytes())
            .unwrap_or(true)
    }

    /// `text` with every masked value `***`: for what the runner says of
    /// the job itself, masked before it is cut, split or made JSON.
    pub fn mask(&self, text: &str) -> String {
        self.inner
            .shared
            .commands
            .lock()
            .map(|c| c.mask_text(text))
            .unwrap_or_else(|_| "***".to_string())
    }

    /// Give the stream back: every line of the runner's printed, and nothing
    /// after; then this process's standard output and error are what they
    /// were. The relay is waited for however long the stream takes, as this
    /// process's own writes to it would have been.
    pub fn finish(mut self) {
        self.close(None);
    }

    fn close(&mut self, bound: Option<Duration>) {
        if self.finished {
            return;
        }
        self.finished = true;
        self.inner.ask(ASK_CLOSE, bound);
        // SAFETY: plain descriptor calls on this process's own descriptors.
        unsafe {
            libc::dup2(self.original, 1);
            libc::dup2(self.original, 2);
        }
        if let Ok(mut out) = self.inner.shared.output.lock() {
            *out = None;
        }
    }
}

impl Drop for Relay {
    /// A job that ended another way than [`Relay::finish`] (a panic) has
    /// its lines printed, for as long as [`DRAIN`] allows.
    fn drop(&mut self) {
        self.close(Some(DRAIN));
    }
}

/// Where `needle` first is in `haystack`.
fn find(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    if needle.is_empty() || haystack.len() < needle.len() {
        return None;
    }
    haystack.windows(needle.len()).position(|w| w == needle)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn shown(commands: &mut Commands, line: &str) -> String {
        String::from_utf8(commands.line(line.as_bytes()).0).unwrap()
    }

    fn annotated(commands: &mut Commands, line: &str) -> Option<serde_json::Value> {
        let (_, said) = commands.line(line.as_bytes());
        said.map(|said| {
            let said = String::from_utf8(said).unwrap();
            let json = said.strip_prefix("::dagron-ci::annotation::").unwrap();
            serde_json::from_str(json).unwrap()
        })
    }

    #[test]
    fn a_value_asked_for_is_masked_from_that_line_on() {
        let mut c = Commands::default();
        assert_eq!(shown(&mut c, "token s3cret"), "token s3cret");
        assert_eq!(shown(&mut c, "::add-mask::s3cret"), "::add-mask::***");
        assert_eq!(
            shown(&mut c, "token s3cret, again s3cret"),
            "token ***, again ***"
        );
        assert_eq!(shown(&mut c, "  ::add-mask::two words"), "::add-mask::***");
        assert_eq!(shown(&mut c, "said two words"), "said ***");
    }

    #[test]
    fn credentials_and_lines_of_a_value_are_masked_the_longest_first() {
        let mut c = Commands::new(None, ["ghs_abc".to_string(), "ghs_abcdef".to_string()]);
        assert_eq!(shown(&mut c, "x ghs_abcdef y ghs_abc"), "x *** y ***");
        // A value of several lines is masked a line at a time; %0A is one.
        assert_eq!(
            shown(&mut c, "::add-mask::line1%0Aline2"),
            "::add-mask::***"
        );
        assert_eq!(shown(&mut c, "line2 then line1"), "*** then ***");
        // %25 is a %.
        shown(&mut c, "::add-mask::100%25");
        assert_eq!(shown(&mut c, "it is 100%"), "it is ***");
    }

    #[test]
    fn stopped_commands_do_nothing_until_their_token() {
        let mut c = Commands::default();
        shown(&mut c, "::stop-commands::tok1");
        assert_eq!(shown(&mut c, "::add-mask::leaked"), "::add-mask::leaked");
        assert_eq!(shown(&mut c, "leaked"), "leaked");
        assert_eq!(annotated(&mut c, "::error::not one"), None);
        shown(&mut c, "::tok1::");
        assert_eq!(shown(&mut c, "::add-mask::kept"), "::add-mask::***");
        assert_eq!(shown(&mut c, "kept"), "***");
        assert!(annotated(&mut c, "::error::one").is_some());
    }

    #[test]
    fn an_error_warning_or_notice_is_an_annotation_as_github_reads_it() {
        let mut c = Commands::new(Some("/workspace".into()), Vec::<String>::new());
        let a = annotated(
            &mut c,
            "::error file=/workspace/src/lib.rs,line=7,endLine=9,title=Bad%3A thing::left %25 right%0Aat 7",
        )
        .unwrap();
        assert_eq!(a["path"], "src/lib.rs", "under the workspace, the rest");
        assert_eq!(
            (a["start_line"].as_u64(), a["end_line"].as_u64()),
            (Some(7), Some(9))
        );
        assert_eq!(a["level"], "failure");
        assert_eq!(a["title"], "Bad: thing");
        assert_eq!(a["message"], "left % right\nat 7");
        let a = annotated(&mut c, "::warning file=./a.rs,line=2::careful").unwrap();
        assert_eq!(
            (a["path"].as_str(), a["level"].as_str()),
            (Some("a.rs"), Some("warning"))
        );
        // No file: the workflow's, at its first line.
        let a = annotated(&mut c, "::notice::done").unwrap();
        assert_eq!(
            (a["path"].as_str(), a["start_line"].as_u64()),
            (Some(".github"), Some(1))
        );
        // Not one: another command, a dagron-ci marker, plain text.
        for line in [
            "::debug::x",
            "::group::g",
            "::dagron-ci::error::x",
            "error: x",
        ] {
            assert_eq!(annotated(&mut c, line), None, "{line}");
        }
    }

    #[test]
    fn an_annotation_is_masked_and_a_job_says_so_many() {
        let mut c = Commands::new(None, ["s3cret".to_string()]);
        let a = annotated(&mut c, "::error title=s3cret::the s3cret").unwrap();
        assert_eq!(
            (a["title"].as_str(), a["message"].as_str()),
            (Some("***"), Some("the ***"))
        );
        for _ in 1..ANNOTATIONS_MAX {
            assert!(annotated(&mut c, "::warning::w").is_some());
        }
        assert_eq!(annotated(&mut c, "::warning::one too many"), None);
        assert_eq!(
            shown(&mut c, "::warning::one too many"),
            "::warning::one too many"
        );
    }

    struct Into(Arc<Mutex<Vec<u8>>>);
    impl Write for Into {
        fn write(&mut self, b: &[u8]) -> io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(b);
            Ok(b.len())
        }
        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    /// A job's stream as the relay reads it: what the steps printed, and
    /// where the runner asked.
    enum Part<'a> {
        Steps(&'a [u8]),
        Ask(u8),
    }
    use Part::{Ask, Steps};

    /// What the relay prints of `stream`, with `own` the runner's lines its
    /// asks print, fed whole and a byte at a time (which must print the
    /// same).
    fn relayed(stream: &[Part], own: &[&str]) -> String {
        let mut got = Vec::new();
        for trickle in [false, true] {
            let out: Arc<Mutex<Vec<u8>>> = Arc::default();
            let shared = Shared {
                output: Mutex::new(Some(Box::new(Into(Arc::clone(&out))))),
                commands: Mutex::default(),
                queue: Mutex::new(own.iter().map(|l| format!("{l}\n").into_bytes()).collect()),
            };
            let mut reader = Reader::new(&shared);
            for read in stream {
                match read {
                    Steps(bytes) if trickle => bytes.chunks(1).for_each(|b| reader.feed(b)),
                    Steps(bytes) => reader.feed(bytes),
                    Ask(word) => reader.ask(*word),
                }
            }
            reader.end();
            got.push(String::from_utf8(out.lock().unwrap().clone()).unwrap());
        }
        assert_eq!(got[0], got[1], "fed whole and a byte at a time");
        got.remove(0)
    }

    #[test]
    fn the_relay_reads_whole_lines_and_ends_them_as_they_ended() {
        let got = relayed(
            &[Steps(
                b"a s3cret\n::add-mask::s3cret\nb s3cret\rprogress s3cret\r\n::error::bad\r\nlast s3cret",
            )],
            &[],
        );
        let lines: Vec<&str> = got.split('\n').collect();
        assert_eq!(
            lines[..4],
            [
                "a s3cret",
                "::add-mask::***",
                "b ***\rprogress ***\r",
                "::error::bad\r"
            ],
            "\\r\\n as it came, and no blank line after a command's"
        );
        let json = lines[4].strip_prefix("::dagron-ci::annotation::").unwrap();
        let a: serde_json::Value = serde_json::from_str(json).unwrap();
        assert_eq!(
            (a["message"].as_str(), a["path"].as_str()),
            (Some("bad"), Some(".github"))
        );
        assert_eq!(lines[5..], ["last ***"]);
    }

    /// The runner's own lines are printed where it asks, whatever is
    /// masked: they are the job's protocol, which a mask must not break. A
    /// step's line that only looks like one is a step's.
    #[test]
    fn the_runner_s_own_lines_are_printed_where_it_asks() {
        let got = relayed(
            &[
                Steps(b"::add-mask::1\n"),
                Ask(ASK_LINE),
                Steps(b"::dagron-ci::job::{\"result\":\"success\"}\nstep 1\n"),
                Ask(ASK_LINE),
            ],
            &[
                "::dagron-ci::step::1::start::build 1",
                "::dagron-ci::result::{\"n\":1}",
            ],
        );
        assert_eq!(
            got,
            "::add-mask::***\n::dagron-ci::step::1::start::build ***\n\
             ::dagron-ci::job::{\"result\":\"success\"}\nstep ***\n::dagron-ci::result::{\"n\":1}\n"
        );
    }

    /// A step's last line without its end ends where the runner asks: the
    /// runner's line is printed whole, on its own.
    #[test]
    fn a_step_s_line_without_its_end_ends_where_the_runner_asks() {
        let got = relayed(
            &[
                Steps(b"done"),
                Ask(ASK_LINE),
                Steps(b"after\nhalf\r"),
                Ask(ASK_END),
                Steps(b"way\n"),
            ],
            &["::dagron-ci::step::1::end::0"],
        );
        assert_eq!(got, "done\n::dagron-ci::step::1::end::0\nafter\nhalf\r\n");
    }

    /// After the runner says the steps are over, a step's line is not
    /// printed (a process one left behind), and after it closes, nothing
    /// is: the job's last line stays its last, and no timer decides it.
    #[test]
    fn after_the_steps_only_the_runner_s_lines_and_after_its_last_nothing() {
        let got = relayed(
            &[
                Steps(b"step\n"),
                Ask(ASK_END),
                Steps(b"late, from a process a step left\n"),
                Ask(ASK_LINE),
                Ask(ASK_CLOSE),
                Ask(ASK_LINE),
                Steps(b"after the close\n"),
            ],
            &["::dagron-ci::job::{}", "::dagron-ci::job::{\"after\":1}"],
        );
        assert_eq!(got, "step\n::dagron-ci::job::{}\n");
    }

    /// A step's words in the runner's lines are masked; the runner's are
    /// not, however short a mask.
    #[test]
    fn the_runner_s_lines_are_masked_where_a_step_s_words_are() {
        let c = Commands::new(None, ["hunter2".to_string(), "1".to_string()]);
        let own = |line: &str| String::from_utf8(c.own_line(line.as_bytes())).unwrap();
        assert_eq!(
            own("::dagron-ci::error::step 2: \"pw hunter2\" is neither NAME=value\n"),
            "::dagron-ci::error::step 2: \"pw ***\" is neither NAME=value\n"
        );
        assert_eq!(
            own("::dagron-ci::step::1::start::deploy 1 hunter2\n"),
            "::dagron-ci::step::1::start::deploy *** ***\n"
        );
        assert_eq!(
            own("::dagron-ci::step::1::end::1\n"),
            "::dagron-ci::step::1::end::1\n"
        );
        assert_eq!(
            own("::dagron-ci::result::{\"n\":1}\n"),
            "::dagron-ci::result::{\"n\":1}\n"
        );
        assert_eq!(own("-- hunter2 step\n"), "-- *** step\n");
    }

    #[test]
    fn masks_overlapping_are_masked_as_one_and_escaped_forms_too() {
        let c = Commands::new(None, ["abcdef".to_string(), "xyzab".to_string()]);
        assert_eq!(c.mask_text("xyzabcdef!"), "***!", "no part of either shows");
        // As a workflow command's data and properties escape it.
        let c = Commands::new(None, ["p%s:s,".to_string()]);
        assert_eq!(c.mask_text("a p%25s:s, b"), "a *** b");
        assert_eq!(c.mask_text("a p%25s%3As%2C b"), "a *** b");
        // An annotation is masked in its fields, so its JSON stays JSON.
        let mut c = Commands::new(None, ["\"q\\".to_string(), "1".to_string()]);
        let a = annotated(&mut c, "::error file=a.rs,line=10::say \"q\\ now").unwrap();
        assert_eq!(a["message"], "say *** now");
        assert_eq!(a["start_line"], 10, "a mask is no number's");
    }

    #[test]
    fn a_value_that_is_not_utf_8_is_masked_as_its_bytes_came() {
        let mut c = Commands::default();
        assert_eq!(c.line(b"::add-mask::\xffab%25\n").0, b"::add-mask::***");
        assert_eq!(c.line(b"x \xffab% y").0, b"x *** y");
    }

    /// A line not printed (`::debug::` where debug is off) is not printed
    /// in any part, however long: the part past its bound neither.
    #[test]
    fn a_hidden_line_past_its_bound_is_hidden_whole() {
        let mut long = b"::debug::".to_vec();
        long.extend(vec![b'x'; LINE_MAX + 10]);
        long.extend_from_slice(b"\nshown\n");
        assert_eq!(relayed(&[Steps(&long)], &[]), "shown\n");
    }

    #[test]
    fn a_line_past_its_bound_is_printed_as_it_is() {
        let long = vec![b'x'; LINE_MAX + 10];
        assert_eq!(relayed(&[Steps(&long)], &[]).len(), LINE_MAX + 10);
        // And the runner's line after it is on a line of its own.
        let got = relayed(&[Steps(&long), Ask(ASK_LINE)], &["own"]);
        assert_eq!(got.len(), LINE_MAX + 10 + "\nown\n".len());
        assert!(got.ends_with("x\nown\n"));
    }

    /// The relay's thread on pipes: an ask is answered once what the steps
    /// wrote before it is printed, a step that reads the steps' pipe itself
    /// keeps no ask waiting, and with the runner gone, the thread is.
    #[test]
    fn an_ask_is_answered_after_what_came_before_it() {
        let pipe = || {
            let mut fds = [0 as RawFd; 2];
            assert_eq!(unsafe { libc::pipe(fds.as_mut_ptr()) }, 0);
            (fds[0], fds[1])
        };
        let ((steps_read, steps_write), (asks_read, asks_write)) = (pipe(), pipe());
        unsafe {
            let flags = libc::fcntl(steps_read, libc::F_GETFL);
            libc::fcntl(steps_read, libc::F_SETFL, flags | libc::O_NONBLOCK);
        }
        let out: Arc<Mutex<Vec<u8>>> = Arc::default();
        let shared = Arc::new(Shared {
            output: Mutex::new(Some(Box::new(Into(Arc::clone(&out))))),
            commands: Mutex::default(),
            queue: Mutex::new(["own\n".as_bytes().to_vec()].into()),
        });
        let (tell, told) = mpsc::channel();
        let thread_shared = Arc::clone(&shared);
        let thread = std::thread::spawn(move || {
            serve(Reader::new(&thread_shared), steps_read, asks_read, tell)
        });
        let write = |fd: RawFd, bytes: &[u8]| {
            assert_eq!(
                unsafe { libc::write(fd, bytes.as_ptr().cast(), bytes.len()) },
                bytes.len() as isize
            );
        };
        write(steps_write, b"one\ntwo");
        write(asks_write, &[ASK_LINE]);
        told.recv_timeout(Duration::from_secs(5)).unwrap();
        assert_eq!(*out.lock().unwrap(), b"one\ntwo\nown\n");
        // A thief takes what is in the pipe: the ask is answered all the same.
        write(steps_write, b"taken\n");
        let mut buf = [0u8; 16];
        // Taken here or by the relay, whichever reads first.
        let _ = unsafe { libc::read(steps_read, buf.as_mut_ptr().cast(), 16) };
        write(asks_write, &[ASK_END]);
        told.recv_timeout(Duration::from_secs(5)).unwrap();
        unsafe {
            libc::close(asks_write);
            libc::close(steps_write);
        }
        thread.join().unwrap();
        unsafe {
            libc::close(asks_read);
            libc::close(steps_read);
        }
    }
}

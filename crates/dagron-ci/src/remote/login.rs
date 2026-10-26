//! `dagron-ci login` and `logout`: the caller's token for a server, kept for
//! the commands that speak to it once the server has taken it.
//!
//! **Where.** `$XDG_CONFIG_HOME/dagron-ci/credentials.json`, else
//! `~/.config/dagron-ci/credentials.json` (`%APPDATA%` where there is no
//! home): a token for each server, and the server logged in to last, which a
//! command speaks to when none is named. The directory is the user's alone
//! (0700), and so is the file (0600), which is written whole beside the one
//! it replaces and renamed over it. A file others may read, or another
//! user's, is refused, as ssh refuses such a key. A login or a logout reads
//! it, changes it and writes it under a lock (`credentials.lock`, beside
//! it), so that neither undoes what another finished meanwhile.
//!
//! **Which token.** `--token-file`, DAGRON_CI_TOKEN_FILE or DAGRON_CI_TOKEN,
//! else what is typed at the terminal, which does not show it, or piped in.
//! Never a flag: a process list shows those. The one to keep is a dagron-api
//! API token (`dgp_…`, `POST /api/tokens`); a session's ends with the
//! session. A signal that ends the process while the token is typed (Ctrl-C)
//! gives the terminal back as it was first.
//!
//! **Only what the server took.** The server is held to the rule every
//! command here is (`https://`, or `http://` to this machine), and the token
//! is kept only once the server has said whose it is (`GET /ci/v1/me`): a
//! 200 that names no subject keeps nothing, and nor does one whose subject
//! or email is more than [`MAX_NAME`] bytes or holds a control character.
//!
//! **A logout forgets.** The token is no longer kept here, and works until
//! it is revoked in dagron-api, where it was made: dagron-api revokes a token
//! for a session, and not for another token.

use std::collections::BTreeMap;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

use reqwest::Method;
use serde::{Deserialize, Serialize};

use super::{checked, text, usage, Env, Out, Server, Stop, Target};

/// Where `login` reads a token that no flag or variable gives: told what to
/// prompt with, it answers what was typed or piped in.
pub(super) type Ask<'a> = &'a mut (dyn FnMut(&str) -> std::io::Result<String> + Send);

/// The largest credentials file read, and so written.
const MAX_FILE: u64 = 1 << 20;

/// The longest subject or email kept: an OpenID subject is at most 255
/// bytes, and an email 254.
const MAX_NAME: usize = 256;

/// What `dagron-ci login` keeps.
#[derive(Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub(super) struct Stored {
    /// The server logged in to last.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default: Option<String>,
    /// A token for each server, by its URL as the commands use it.
    #[serde(default)]
    pub servers: BTreeMap<String, Kept>,
}

/// A server's token, and whose the server said it is.
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
pub(super) struct Kept {
    pub token: String,
    /// The subject the server named, after its email where it gave one:
    /// `dev@example.com (u-1)`.
    #[serde(rename = "as", default)]
    pub who: String,
}

/// The file the tokens are kept in, where the environment gives it a home.
pub(super) fn path(env: Env<'_>) -> Option<PathBuf> {
    let config = env("XDG_CONFIG_HOME")
        .map(PathBuf::from)
        .filter(|dir| dir.is_absolute())
        .or_else(|| env("HOME").map(|home| PathBuf::from(home).join(".config")))
        .or_else(|| env("APPDATA").map(PathBuf::from))?;
    Some(config.join("dagron-ci").join("credentials.json"))
}

/// What is kept, if anything: `None` with no file. A file others may read,
/// another user's, or one dagron-ci did not write is refused.
pub(super) fn read(env: Env<'_>) -> Result<Option<Stored>, Stop> {
    let Some(path) = path(env) else {
        return Ok(None);
    };
    let shown = path.display();
    let file = match std::fs::File::open(&path) {
        Ok(file) => file,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return usage(format!("{shown}: {e}")),
    };
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        let meta = file
            .metadata()
            .map_err(|e| Stop::Usage(format!("{shown}: {e}")))?;
        if meta.mode() & 0o077 != 0 {
            return usage(format!(
                "{shown} holds tokens, and others may read it (mode {:03o}): `chmod 600` it, or \
                 remove it and log in again",
                meta.mode() & 0o777
            ));
        }
        // SAFETY: geteuid(2) takes nothing and cannot fail.
        if meta.uid() != unsafe { libc::geteuid() } {
            return usage(format!("{shown} is not yours: remove it and log in again"));
        }
    }
    let mut text = String::new();
    file.take(MAX_FILE)
        .read_to_string(&mut text)
        .map_err(|e| Stop::Usage(format!("{shown}: {e}")))?;
    serde_json::from_str(&text).map(Some).map_err(|e| {
        Stop::Usage(format!(
            "{shown} is not a file `dagron-ci login` wrote ({e}): remove it and log in again"
        ))
    })
}

/// Keep `stored`: written whole to a file of the user's alone beside the one
/// it replaces, and renamed over it. With no server left, the file goes. One
/// larger than is read back is not written.
pub(super) fn write(env: Env<'_>, stored: &Stored) -> Result<PathBuf, Stop> {
    let Some(path) = path(env) else {
        return usage("there is no home to keep a token in: set HOME or XDG_CONFIG_HOME");
    };
    let failed = |e: std::io::Error| Stop::Failed(format!("{}: {e}", path.display()));
    if stored.servers.is_empty() {
        return match std::fs::remove_file(&path) {
            Ok(()) => Ok(path),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(path),
            Err(e) => Err(failed(e)),
        };
    }
    let body = serde_json::to_string_pretty(stored).map_err(|e| Stop::Failed(e.to_string()))?;
    if body.len() as u64 >= MAX_FILE {
        return Err(Stop::Failed(format!(
            "{} is not written: it would be more than 1 MiB, more than is read back",
            path.display()
        )));
    }
    let dir = home_dir(&path).map_err(failed)?;
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    // One a write that stopped half way left is this user's, in a directory
    // of theirs alone.
    let temp = dir.join(format!(".credentials.json.{}.tmp", std::process::id()));
    let _ = std::fs::remove_file(&temp);
    let done = options.open(&temp).and_then(|mut file| {
        file.write_all(format!("{body}\n").as_bytes())?;
        file.sync_all()?;
        std::fs::rename(&temp, &path)
    });
    if let Err(e) = done {
        let _ = std::fs::remove_file(&temp);
        return Err(failed(e));
    }
    Ok(path)
}

/// The directory of the credentials file at `path`, made where it is not
/// there, and the user's alone.
fn home_dir(path: &Path) -> std::io::Result<&Path> {
    let dir = path.parent().unwrap_or(Path::new("."));
    if let Some(config) = dir.parent() {
        std::fs::create_dir_all(config)?;
    }
    private_dir(dir)?;
    Ok(dir)
}

/// Hold what is kept against another `login` or `logout` until what this
/// gives is dropped: an advisory lock on `credentials.lock`, beside it. Each
/// reads what is kept once it holds it, and has written what it changed
/// before it lets go, so neither writes back a copy older than what the
/// other kept. `make` makes the directory where it is not there; without it,
/// there is nothing kept to hold. Where there are no such locks, nothing is
/// held.
pub(super) fn hold(env: Env<'_>, make: bool) -> Result<Option<std::fs::File>, Stop> {
    let Some(path) = path(env) else {
        return Ok(None);
    };
    let lock = path.with_file_name("credentials.lock");
    let failed = |e: std::io::Error| Stop::Failed(format!("{}: {e}", lock.display()));
    if make {
        home_dir(&path).map_err(failed)?;
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        use std::os::unix::io::AsRawFd;
        let file = match std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .mode(0o600)
            .custom_flags(libc::O_NOFOLLOW)
            .open(&lock)
        {
            Ok(file) => file,
            Err(e) if !make && e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(failed(e)),
        };
        loop {
            // SAFETY: flock(2) on a descriptor `file` owns; the lock goes
            // with it when it is closed.
            if unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX) } == 0 {
                return Ok(Some(file));
            }
            let e = std::io::Error::last_os_error();
            if e.kind() != std::io::ErrorKind::Interrupted {
                return Err(failed(e));
            }
        }
    }
    #[cfg(not(unix))]
    Ok(None)
}

/// `dir`, made where it is not there, and the user's alone (0700) where
/// modes are had: it is dagron-ci's own.
fn private_dir(dir: &std::path::Path) -> std::io::Result<()> {
    #[cfg(unix)]
    let made = {
        use std::os::unix::fs::DirBuilderExt;
        std::fs::DirBuilder::new().mode(0o700).create(dir)
    };
    #[cfg(not(unix))]
    let made = std::fs::create_dir(dir);
    match made {
        Err(e) if e.kind() != std::io::ErrorKind::AlreadyExists => return Err(e),
        _ => {}
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(dir, std::fs::Permissions::from_mode(0o700))?;
    }
    Ok(())
}

/// A token typed at the terminal, which does not show it; or the first line
/// of what is piped in.
pub(super) fn typed(prompt: &str) -> std::io::Result<String> {
    use std::io::{BufRead, IsTerminal};
    let stdin = std::io::stdin();
    let mut line = String::new();
    if !stdin.is_terminal() {
        stdin.lock().read_line(&mut line)?;
        return Ok(line);
    }
    let quiet = Quiet::new()?;
    eprint!("{prompt}");
    let read = stdin.lock().read_line(&mut line);
    drop(quiet);
    eprintln!();
    read.map(|_| line)
}

/// The terminal on standard input, not showing what is typed until this is
/// dropped, or until a signal that ends the process meanwhile (Ctrl-C) puts
/// it back.
#[cfg(unix)]
struct Quiet {
    /// The terminal as it was, which `WAS` points at while this lives.
    was: Box<libc::termios>,
    /// Each signal given to `put_back_and_end`, with what it did before.
    caught: Vec<(libc::c_int, libc::sigaction)>,
}

/// The signals a terminal or `kill` ends a process with by default: while
/// the token is typed, each puts the terminal back first.
#[cfg(unix)]
const ENDING: [libc::c_int; 3] = [libc::SIGINT, libc::SIGQUIT, libc::SIGTERM];

/// The terminal as it was before the token was typed, while a `Quiet` holds
/// it; null otherwise.
#[cfg(unix)]
static WAS: std::sync::atomic::AtomicPtr<libc::termios> =
    std::sync::atomic::AtomicPtr::new(std::ptr::null_mut());

/// A signal that would have ended the process: the terminal is put back as
/// it was, and the signal ends it as it would have.
#[cfg(unix)]
extern "C" fn put_back_and_end(signal: libc::c_int) {
    // SAFETY: tcsetattr(3), signal(2) and raise(3) may be called in a signal
    // handler. `WAS` points at the termios of the `Quiet` that set this
    // handler, which puts the handler back before that termios goes. The
    // signal is blocked while this runs, so the one raised ends the process
    // as this returns.
    unsafe {
        let was = WAS.load(std::sync::atomic::Ordering::SeqCst);
        if !was.is_null() {
            libc::tcsetattr(libc::STDIN_FILENO, libc::TCSANOW, was);
        }
        libc::signal(signal, libc::SIG_DFL);
        libc::raise(signal);
    }
}

#[cfg(unix)]
impl Quiet {
    fn new() -> std::io::Result<Quiet> {
        // SAFETY: tcgetattr(3), tcsetattr(3) and sigaction(2), each with
        // structures this function owns; `WAS` is set before a handler that
        // reads it is, and the `Quiet` made puts both back when dropped, on
        // the way out of here too.
        unsafe {
            let mut was: Box<libc::termios> = Box::new(std::mem::zeroed());
            if libc::tcgetattr(libc::STDIN_FILENO, &mut *was) != 0 {
                return Err(std::io::Error::last_os_error());
            }
            let mut quiet = *was;
            quiet.c_lflag &= !libc::ECHO;
            let mut held = Quiet {
                was,
                caught: Vec::new(),
            };
            WAS.store(&mut *held.was, std::sync::atomic::Ordering::SeqCst);
            // A signal ignored, or handled already, is left as it is.
            let mut action: libc::sigaction = std::mem::zeroed();
            action.sa_sigaction =
                put_back_and_end as extern "C" fn(libc::c_int) as libc::sighandler_t;
            libc::sigemptyset(&mut action.sa_mask);
            for signal in ENDING {
                let mut before: libc::sigaction = std::mem::zeroed();
                if libc::sigaction(signal, std::ptr::null(), &mut before) == 0
                    && before.sa_sigaction == libc::SIG_DFL
                    && libc::sigaction(signal, &action, std::ptr::null_mut()) == 0
                {
                    held.caught.push((signal, before));
                }
            }
            if libc::tcsetattr(libc::STDIN_FILENO, libc::TCSANOW, &quiet) != 0 {
                return Err(std::io::Error::last_os_error());
            }
            Ok(held)
        }
    }
}

#[cfg(unix)]
impl Drop for Quiet {
    fn drop(&mut self) {
        // SAFETY: as in `new`: the terminal is put back, then each signal's
        // handler, and only then is `WAS` let go of.
        unsafe {
            libc::tcsetattr(libc::STDIN_FILENO, libc::TCSANOW, &*self.was);
            for (signal, before) in &self.caught {
                libc::sigaction(*signal, before, std::ptr::null_mut());
            }
        }
        WAS.store(std::ptr::null_mut(), std::sync::atomic::Ordering::SeqCst);
    }
}

/// Elsewhere, a terminal that would show the token is not read from.
#[cfg(not(unix))]
struct Quiet;

#[cfg(not(unix))]
impl Quiet {
    fn new() -> std::io::Result<Quiet> {
        Err(std::io::Error::other(
            "the terminal would show it: give it with --token-file, or pipe it in",
        ))
    }
}

/// `dagron-ci login`: ask the server whose the token is (`GET /ci/v1/me`),
/// and keep it for that server, which is then the one a command speaks to
/// when none is named. A token the server does not take is not kept.
pub(super) async fn login(
    args: &[&str],
    env: Env<'_>,
    ask: Ask<'_>,
    out: Out<'_>,
    err: Out<'_>,
) -> Result<u8, Stop> {
    let mut target = Target::default();
    let mut rest = args;
    loop {
        if let Some(tail) = target.take(rest) {
            rest = tail;
            continue;
        }
        match rest {
            [] => break,
            [other, ..] => return usage(format!("{other:?} is not something login takes")),
        }
    }
    // The server logged in to last, from what is kept; that is read again
    // before it is written.
    let last = read(env)?.and_then(|stored| stored.default);
    let url = target
        .server
        .map(str::to_string)
        .or_else(|| env("DAGRON_CI_URL"))
        .or(last);
    let Some(url) = url else {
        return usage("name the server to log in to with --server or DAGRON_CI_URL");
    };
    // The server is held to the rule before a token is asked for.
    let base = checked(&url)?;
    let token = match target.given(env)? {
        Some(token) => token,
        None => ask(&format!("dagron-api API token for {base}: "))
            .map_err(|e| Stop::Usage(format!("no token was read: {e}")))?,
    };
    let server = Server::new(base, &token)?;
    let answer = server
        .ask(Method::GET, "/ci/v1/me", &[], None, None)
        .await?;
    if answer.status != 200 {
        let why = match answer.status {
            404 if !answer.body["title"].is_string() => format!(
                "{}: the server has no GET /ci/v1/me, so is older than this dagron-ci; give the \
                 token with DAGRON_CI_TOKEN_FILE instead",
                answer.refusal()
            ),
            _ => answer.refusal(),
        };
        return Err(Stop::Failed(format!("nothing was kept: {why}")));
    }
    // A 200 that names no one is not the run API's answer (a console's page,
    // say, served for every path), and keeps nothing.
    let sub = text(&answer.body["sub"]);
    if sub.is_empty() {
        return Err(Stop::Failed(format!(
            "nothing was kept: {} answered GET /ci/v1/me without saying whose the token is: \
             name a dagron-ci server",
            server.base
        )));
    }
    // What is written to the terminal and kept is a name: no longer than one
    // is, and with nothing in it that the terminal would act on.
    let email = text(&answer.body["email"]);
    if [sub, email]
        .iter()
        .any(|name| name.len() > MAX_NAME || name.contains(char::is_control))
    {
        return Err(Stop::Failed(format!(
            "nothing was kept: {} answered GET /ci/v1/me with a subject or an email of more than \
             {MAX_NAME} bytes, or with a control character in it: name a dagron-ci server",
            server.base
        )));
    }
    let who = match email {
        "" => sub.to_string(),
        email => format!("{email} ({sub})"),
    };
    // What is kept is read again, under the lock, now the server has
    // answered: a login or a logout that finished while the token was typed
    // or the server asked is kept as it left it.
    let held = hold(env, true)?;
    let mut stored = read(env)?.unwrap_or_default();
    stored.servers.insert(
        server.base.clone(),
        Kept {
            token: server.token.clone(),
            who: who.clone(),
        },
    );
    stored.default = Some(server.base.clone());
    let path = write(env, &stored)?;
    drop(held);
    let _ = writeln!(out, "logged in to {} as {who}", server.base);
    let viewer = answer.body["groups"]
        .as_array()
        .is_some_and(|groups| groups.iter().any(|g| g == "viewer"));
    if viewer {
        let _ = writeln!(err, "a viewer: runs are read, and none is started");
    }
    let _ = writeln!(err, "the token is kept in {}", path.display());
    Ok(0)
}

/// `dagron-ci logout`: forget the token kept for the server named, or for
/// the one logged in to last.
pub(super) fn logout(args: &[&str], env: Env<'_>, out: Out<'_>, err: Out<'_>) -> Result<u8, Stop> {
    let named = match args {
        [] => None,
        ["--server", url] => Some(url.to_string()),
        [other, ..] => return usage(format!("{other:?} is not something logout takes")),
    };
    let _held = hold(env, false)?;
    let Some(mut stored) = read(env)? else {
        let _ = writeln!(err, "no token is kept here");
        return Ok(0);
    };
    let url = named
        .or_else(|| env("DAGRON_CI_URL"))
        .or_else(|| stored.default.clone());
    let Some(url) = url else {
        return usage("name the server to log out of with --server");
    };
    let base = checked(&url)?;
    let Some(kept) = stored.servers.remove(&base) else {
        let _ = writeln!(err, "no token is kept for {base}");
        return Ok(0);
    };
    if stored.default.as_deref() == Some(base.as_str()) {
        stored.default = None;
    }
    write(env, &stored)?;
    let _ = writeln!(out, "logged out of {base}");
    let _ = writeln!(
        err,
        "the token of {} is no longer kept here; it works until it is revoked in dagron-api",
        kept.who
    );
    Ok(0)
}

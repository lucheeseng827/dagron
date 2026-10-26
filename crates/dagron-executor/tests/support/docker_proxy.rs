//! A proxy of the Docker API, for tests: a socket of its own in front of the
//! daemon's, one thread per connection, that passes each request on as it
//! came, and refuses, holds or delays those a test names. It records each
//! request it was sent, in order.
//!
//! It reads the head of a connection's first request only, and passes the
//! rest on as bytes, both ways: a wait, a log and an exec's hijacked stream
//! included. So each request must come on a connection of its own. bollard
//! keeps no idle connection to a socket (`pool_max_idle_per_host(0)`), and
//! the daemon is told to close each one after its answer, but for an
//! upgraded one, which is its request's to the end. The self-test in
//! `docker_workspace.rs` sees that this holds.
//!
//! The daemon is the one [`bollard::Docker::connect_with_local_defaults`]
//! reaches: the socket `DOCKER_HOST` names (`unix://`), else Docker's own.
//!
//! Whoever reaches its socket has the daemon's API, so the socket is in a
//! directory no other user may enter: `/tmp/dgp-<id>/s`, short, since a
//! path under a long `TMPDIR` would not fit a socket's address (108 bytes
//! on Linux, 104 on macOS).

use std::io::{BufRead, BufReader, Read, Write};
use std::net::Shutdown;
use std::os::unix::fs::DirBuilderExt;
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::{Path, PathBuf};
use std::sync::mpsc::{channel, Sender};
use std::sync::{Arc, Mutex};

pub struct DockerProxy {
    socket: PathBuf,
    state: Arc<Mutex<State>>,
    _root: TempRoot,
}

/// What a rule does to a request it names.
#[derive(Debug, Clone, Copy)]
enum Act {
    /// Answered with this status, and a message, and never passed on.
    Refuse(u16),
    /// Never answered nor passed on: kept open until [`DockerProxy::release`],
    /// and then closed.
    HoldThenDrop,
    /// Kept, unanswered, until [`DockerProxy::release`], and then passed on
    /// as it came: a request the daemon is slow to carry out.
    Delay,
}

/// A request a rule keeps from the daemon until [`DockerProxy::release`].
#[derive(Debug)]
enum Held {
    /// Closed then, unanswered.
    ToClose(UnixStream),
    /// Passed on then, by the thread of its connection, which waits for this.
    ToPassOn(Sender<()>),
}

/// Requests by `method` whose path, with no API version and no query, ends
/// with `tail`: the `nth` of them (from 1), or each where it is `None`.
#[derive(Debug)]
struct Rule {
    method: String,
    tail: String,
    nth: Option<u32>,
    act: Act,
    matched: u32,
}

#[derive(Debug, Default)]
struct State {
    rules: Vec<Rule>,
    seen: Vec<String>,
    held: Vec<Held>,
}

/// A directory of the proxy's own, which no other user may enter, removed
/// with it.
struct TempRoot(PathBuf);

impl Drop for TempRoot {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

impl DockerProxy {
    /// A proxy in front of the daemon, passing everything on.
    pub fn start() -> DockerProxy {
        let upstream = std::env::var("DOCKER_HOST")
            .ok()
            .and_then(|host| host.strip_prefix("unix://").map(PathBuf::from))
            .unwrap_or_else(|| PathBuf::from("/var/run/docker.sock"));
        let id = uuid::Uuid::new_v4().simple().to_string();
        let root = TempRoot(PathBuf::from(format!("/tmp/dgp-{}", &id[..12])));
        std::fs::DirBuilder::new()
            .mode(0o700)
            .create(&root.0)
            .unwrap();
        let socket = root.0.join("s");
        let listener = UnixListener::bind(&socket).unwrap();
        let state = Arc::new(Mutex::new(State::default()));
        let shared = Arc::clone(&state);
        std::thread::spawn(move || {
            for client in listener.incoming().flatten() {
                let (state, upstream) = (Arc::clone(&shared), upstream.clone());
                std::thread::spawn(move || {
                    let _ = serve(client, &upstream, &state);
                });
            }
        });
        DockerProxy {
            socket,
            state,
            _root: root,
        }
    }

    /// A client of the daemon through this proxy.
    pub fn client(&self) -> bollard::Docker {
        bollard::Docker::connect_with_unix(
            self.socket.to_str().unwrap(),
            120,
            bollard::API_DEFAULT_VERSION,
        )
        .unwrap()
    }

    /// Answer the `nth` request by `method` whose path ends with `tail` (each
    /// of them, where `nth` is `None`) with `status`, as the daemon answers
    /// one it refuses.
    pub fn refuse(&self, method: &str, tail: &str, nth: Option<u32>, status: u16) {
        self.rule(method, tail, nth, Act::Refuse(status));
    }

    /// Hold the `nth` request by `method` whose path ends with `tail` (each of
    /// them, where `nth` is `None`): never answered, never passed on, and
    /// closed by [`Self::release`].
    pub fn hold(&self, method: &str, tail: &str, nth: Option<u32>) {
        self.rule(method, tail, nth, Act::HoldThenDrop);
    }

    /// Delay the `nth` request by `method` whose path ends with `tail` (each
    /// of them, where `nth` is `None`): kept from the daemon, unanswered,
    /// and passed on as it came by [`Self::release`], as a request the
    /// daemon is slow to carry out. Its answer is then the daemon's.
    pub fn delay(&self, method: &str, tail: &str, nth: Option<u32>) {
        self.rule(method, tail, nth, Act::Delay);
    }

    fn rule(&self, method: &str, tail: &str, nth: Option<u32>, act: Act) {
        self.state.lock().unwrap().rules.push(Rule {
            method: method.to_string(),
            tail: tail.to_string(),
            nth,
            act,
            matched: 0,
        });
    }

    /// Once a request is held or delayed: panics where none is within
    /// `within`.
    pub async fn wait_held(&self, within: std::time::Duration) {
        let until = std::time::Instant::now() + within;
        loop {
            if !self.state.lock().unwrap().held.is_empty() {
                return;
            }
            if std::time::Instant::now() >= until {
                panic!("no request was held within {within:?}: {:?}", self.seen());
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
    }

    /// Close each request held, unanswered, and pass each one delayed on.
    pub fn release(&self) {
        for held in self.state.lock().unwrap().held.drain(..) {
            match held {
                Held::ToClose(client) => {
                    let _ = client.shutdown(Shutdown::Both);
                }
                Held::ToPassOn(go) => {
                    let _ = go.send(());
                }
            }
        }
    }

    /// Pass everything on from now: no rule is left. What is held or
    /// delayed stays so until [`Self::release`].
    pub fn clear(&self) {
        self.state.lock().unwrap().rules.clear();
    }

    /// Each request sent here so far, in order: its method and its path with
    /// its query and with no API version (`DELETE /containers/x?force=true`).
    pub fn seen(&self) -> Vec<String> {
        self.state.lock().unwrap().seen.clone()
    }
}

/// `target` with no API version in front (`/v1.44/containers/x` is
/// `/containers/x`).
fn unversioned(target: &str) -> &str {
    let Some(rest) = target.strip_prefix("/v") else {
        return target;
    };
    let end = rest.find('/').unwrap_or(rest.len());
    if end > 0 && rest[..end].bytes().all(|b| b.is_ascii_digit() || b == b'.') {
        &rest[end..]
    } else {
        target
    }
}

/// One connection: its first request's head read, recorded and judged by
/// the rules, and then refused, held, or passed on with the rest of the
/// connection, at once or once released.
fn serve(client: UnixStream, upstream: &Path, state: &Mutex<State>) -> std::io::Result<()> {
    let mut reader = BufReader::new(client.try_clone()?);
    let mut line = String::new();
    reader.read_line(&mut line)?;
    let mut parts = line.split_whitespace();
    let (method, target, version) = (
        parts.next().unwrap_or_default().to_string(),
        parts.next().unwrap_or_default().to_string(),
        parts.next().unwrap_or("HTTP/1.1").to_string(),
    );
    let mut headers = Vec::new();
    loop {
        let mut h = String::new();
        if reader.read_line(&mut h)? == 0 {
            break;
        }
        let h = h.trim_end();
        if h.is_empty() {
            break;
        }
        headers.push(h.to_string());
    }
    let header = |name: &str| {
        headers.iter().find_map(|h| {
            h.split_once(':')
                .filter(|(n, _)| n.trim().eq_ignore_ascii_case(name))
                .map(|(_, v)| v.trim().to_string())
        })
    };

    let asked = unversioned(&target);
    let path = asked.split('?').next().unwrap_or_default();
    let act = {
        let mut state = state.lock().unwrap();
        state.seen.push(format!("{method} {asked}"));
        let mut act = None;
        for rule in state
            .rules
            .iter_mut()
            .filter(|r| r.method == method && path.ends_with(&r.tail))
        {
            rule.matched += 1;
            if act.is_none() && rule.nth.is_none_or(|n| n == rule.matched) {
                act = Some(rule.act);
            }
        }
        act
    };

    match act {
        Some(Act::Refuse(status)) => {
            if let Some(len) = header("content-length").and_then(|l| l.parse::<u64>().ok()) {
                std::io::copy(&mut (&mut reader).take(len), &mut std::io::sink())?;
            }
            let body = format!("{{\"message\":\"refused by the test's proxy: {method} {path}\"}}");
            let mut out = client;
            out.write_all(
                format!(
                    "HTTP/1.1 {status} Refused\r\nContent-Type: application/json\r\n\
                     Content-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                )
                .as_bytes(),
            )
        }
        Some(Act::HoldThenDrop) => {
            state.lock().unwrap().held.push(Held::ToClose(client));
            Ok(())
        }
        // Passed on: at once, or once released.
        Some(Act::Delay) | None => {
            if matches!(act, Some(Act::Delay)) {
                let (go, released) = channel();
                state.lock().unwrap().held.push(Held::ToPassOn(go));
                let _ = released.recv();
            }
            let mut up = UnixStream::connect(upstream)?;
            // An upgraded connection (an exec's stream) is the request's own
            // to the end; any other is closed after its answer.
            let upgrade = header("upgrade").is_some();
            let mut head = format!("{method} {target} {version}\r\n");
            for h in &headers {
                let name = h.split(':').next().unwrap_or_default().trim();
                if upgrade || !name.eq_ignore_ascii_case("connection") {
                    head.push_str(&format!("{h}\r\n"));
                }
            }
            if !upgrade {
                head.push_str("Connection: close\r\n");
            }
            head.push_str("\r\n");
            up.write_all(head.as_bytes())?;
            // What was read past the head: the start of the body.
            up.write_all(reader.buffer())?;
            let mut from_client = reader.into_inner();
            let mut to_upstream = up.try_clone()?;
            let pump = std::thread::spawn(move || {
                let _ = std::io::copy(&mut from_client, &mut to_upstream);
                let _ = to_upstream.shutdown(Shutdown::Write);
            });
            let mut down = client;
            let _ = std::io::copy(&mut up, &mut down);
            // The answer is whole: the client's side is done too.
            let _ = down.shutdown(Shutdown::Both);
            let _ = up.shutdown(Shutdown::Both);
            let _ = pump.join();
            Ok(())
        }
    }
}

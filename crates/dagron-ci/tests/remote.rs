//! The commands that speak to a server, through the shipped binary, in a
//! process environment of their own: a server on this machine is asked
//! directly, whatever proxy the environment names.
//!
//! In a process of its own because a proxy is read from the environment
//! once, by the first client a process builds.

use std::io::{Read, Write};
use std::net::TcpListener;
use std::process::{Command, Stdio};
use std::sync::{Arc, Mutex};

const BIN: &str = env!("CARGO_BIN_EXE_dagron-ci");

/// Something listening on this machine that answers every request with
/// `body`, and keeps each request's first line and its credential.
fn listener(body: &'static str) -> (String, Arc<Mutex<Vec<String>>>) {
    let socket = TcpListener::bind("127.0.0.1:0").unwrap();
    let base = format!("http://{}", socket.local_addr().unwrap());
    let seen = Arc::new(Mutex::new(Vec::new()));
    let kept = seen.clone();
    std::thread::spawn(move || {
        for stream in socket.incoming() {
            let Ok(mut stream) = stream else { continue };
            let mut head = Vec::new();
            let mut byte = [0u8; 1];
            while !head.ends_with(b"\r\n\r\n") {
                match stream.read(&mut byte) {
                    Ok(1) => head.push(byte[0]),
                    _ => break,
                }
            }
            let head = String::from_utf8_lossy(&head);
            let line = head.lines().next().unwrap_or_default();
            let credential = head
                .lines()
                .find_map(|l| l.strip_prefix("authorization: "))
                .unwrap_or_default();
            kept.lock()
                .unwrap()
                .push(format!("{line} {credential}").trim().to_string());
            let _ = write!(
                stream,
                "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\n\
                 content-length: {}\r\nconnection: close\r\n\r\n{body}",
                body.len()
            );
        }
    });
    (base, seen)
}

/// A server over plain http is this machine, and the caller's token goes to
/// it and to nothing between. A proxy the environment names (HTTP_PROXY,
/// ALL_PROXY) would be handed the request and the token in it, and carry
/// both off the machine in the clear.
#[test]
fn a_server_on_this_machine_is_asked_past_the_environment_s_proxy() {
    let (server, asked) = listener(r#"{"id":"ev-1","state":"done","reason":null,"runs":[]}"#);
    let (proxy, proxied) = listener("{}");
    let out = Command::new(BIN)
        .args(["logs", "ev-1", "--json"])
        .env("DAGRON_CI_URL", &server)
        .env("DAGRON_CI_TOKEN", "dgr_dev")
        .env("HTTP_PROXY", &proxy)
        .env("http_proxy", &proxy)
        .env("ALL_PROXY", &proxy)
        .env("all_proxy", &proxy)
        .env_remove("NO_PROXY")
        .env_remove("no_proxy")
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert_eq!(
        *asked.lock().unwrap(),
        ["GET /ci/v1/runs/ev-1/logs HTTP/1.1 Bearer dgr_dev"],
        "the server was asked, and directly"
    );
    assert!(
        proxied.lock().unwrap().is_empty(),
        "the request, and the token in it, went to the proxy: {:?}",
        proxied.lock().unwrap()
    );
}

/// `login` reads the token from standard input, never from an argument,
/// keeps it once the server took it, and the next command sends it, to
/// that server, with nothing in the environment to say either.
#[test]
fn login_keeps_the_token_the_next_command_sends() {
    let (server, asked) = listener(
        r#"{"sub":"u-1","email":"dev@example.com","groups":[],"id":"ev-1","state":"done","reason":null,"runs":[]}"#,
    );
    let home = tempfile::tempdir().unwrap();
    let command = |args: &[&str]| {
        let mut command = Command::new(BIN);
        command
            .args(args)
            .env("XDG_CONFIG_HOME", home.path())
            .env_remove("DAGRON_CI_URL")
            .env_remove("DAGRON_CI_TOKEN")
            .env_remove("DAGRON_CI_TOKEN_FILE");
        command
    };
    let mut login = command(&["login", "--server", &server])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    login
        .stdin
        .take()
        .unwrap()
        .write_all(b"dgp_piped\n")
        .unwrap();
    let out = login.wait_with_output().unwrap();
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert_eq!(
        String::from_utf8_lossy(&out.stdout),
        format!("logged in to {server} as dev@example.com (u-1)\n")
    );

    let out = command(&["logs", "ev-1", "--json"]).output().unwrap();
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert_eq!(
        *asked.lock().unwrap(),
        [
            "GET /ci/v1/me HTTP/1.1 Bearer dgp_piped",
            "GET /ci/v1/runs/ev-1/logs HTTP/1.1 Bearer dgp_piped"
        ]
    );
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let kept = home.path().join("dagron-ci/credentials.json");
        assert_eq!(
            std::fs::metadata(kept).unwrap().permissions().mode() & 0o777,
            0o600
        );
    }
}

/// Ended by Ctrl-C while the token is typed at a terminal, `login` gives the
/// terminal back as it found it: what is typed next is shown again.
#[cfg(target_os = "linux")]
#[test]
fn login_ended_at_its_prompt_gives_the_terminal_back() {
    use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
    use std::os::unix::process::ExitStatusExt;

    let (mut master, mut terminal) = (-1, -1);
    // SAFETY: openpty(3), into two descriptors this test then owns.
    let made = unsafe {
        libc::openpty(
            &mut master,
            &mut terminal,
            std::ptr::null_mut(),
            std::ptr::null(),
            std::ptr::null(),
        )
    };
    assert_eq!(made, 0, "{}", std::io::Error::last_os_error());
    // SAFETY: both were just opened, and nothing else holds them.
    let (_master, terminal) =
        unsafe { (OwnedFd::from_raw_fd(master), OwnedFd::from_raw_fd(terminal)) };
    let shows_what_is_typed = || {
        // SAFETY: tcgetattr(3) into a termios of this closure's.
        unsafe {
            let mut settings: libc::termios = std::mem::zeroed();
            assert_eq!(libc::tcgetattr(terminal.as_raw_fd(), &mut settings), 0);
            settings.c_lflag & libc::ECHO != 0
        }
    };
    assert!(shows_what_is_typed());

    let home = tempfile::tempdir().unwrap();
    let mut login = Command::new(BIN)
        .args(["login", "--server", "https://ci.example"])
        .env("XDG_CONFIG_HOME", home.path())
        .env_remove("DAGRON_CI_URL")
        .env_remove("DAGRON_CI_TOKEN")
        .env_remove("DAGRON_CI_TOKEN_FILE")
        .stdin(Stdio::from(terminal.try_clone().unwrap()))
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    // The prompt is said once the terminal no longer shows what is typed.
    let mut stderr = login.stderr.take().unwrap();
    let mut said = Vec::new();
    let mut chunk = [0u8; 256];
    while !String::from_utf8_lossy(&said).contains("API token for https://ci.example: ") {
        let n = stderr.read(&mut chunk).unwrap();
        assert!(
            n > 0,
            "login ended before it asked: {}",
            String::from_utf8_lossy(&said)
        );
        said.extend_from_slice(&chunk[..n]);
    }
    assert!(!shows_what_is_typed());

    // SAFETY: kill(2) of the child this test started, which has not been
    // waited for.
    unsafe { libc::kill(login.id() as libc::pid_t, libc::SIGINT) };
    let status = login.wait().unwrap();
    assert_eq!(status.signal(), Some(libc::SIGINT), "{status:?}");
    assert!(
        shows_what_is_typed(),
        "the terminal was left not showing what is typed"
    );
    assert!(!home.path().join("dagron-ci/credentials.json").exists());
}

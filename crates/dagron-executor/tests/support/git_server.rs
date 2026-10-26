//! A git server over smart HTTP, for tests: `git http-backend` behind a
//! listener of its own, one thread per connection. It serves one bare
//! repository holding one commit, can require a credential, and records the
//! `Authorization` each request carried.
//!
//! Needs `git`, with its `http-backend`. [`GitServer::start`] says so and
//! returns `None` where either is missing.

use std::io::{BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::{Arc, Mutex};

pub struct GitServer {
    /// The repository's URL, as a clone takes it.
    pub url: String,
    /// The commit a checkout asks for: the tip of `main`, whose files are
    /// the ones the server was started with.
    pub commit: String,
    /// The commit before it, which holds no file: its history.
    pub earlier: String,
    /// The tip of a branch beside it, `side`, one commit off the earlier
    /// one: what only a checkout with every branch's history has. The tag
    /// `v1`, annotated, is on it.
    pub side: String,
    /// The `Authorization` of each request, in order (`None`: it had none).
    pub seen: Arc<Mutex<Vec<Option<String>>>>,
    _root: TempRoot,
}

/// A directory of the server's own, removed with it.
struct TempRoot(PathBuf);

impl Drop for TempRoot {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn git(dir: &Path, args: &[&str]) -> String {
    let out = Command::new("git")
        .current_dir(dir)
        .args(["-c", "init.defaultBranch=main"])
        .args(args)
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .env("GIT_CONFIG_GLOBAL", "/dev/null")
        .output()
        .expect("git");
    assert!(
        out.status.success(),
        "git {args:?}: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8_lossy(&out.stdout).trim().to_string()
}

impl GitServer {
    /// A repository whose one commit holds `files`, served on `bind` (a
    /// loopback address, or `0.0.0.0` for containers to reach), under
    /// `host` in its URL. With `credential`, a request without exactly that
    /// basic credential is answered 401.
    pub fn start(
        files: &[(&str, &str)],
        bind: &str,
        host: &str,
        credential: Option<(&str, &str)>,
    ) -> Option<GitServer> {
        Self::start_with_links(files, &[], bind, host, credential)
    }

    /// As [`GitServer::start`], and the commit holds `links` too: each a
    /// symbolic link at its path to its target.
    #[allow(dead_code)]
    pub fn start_with_links(
        files: &[(&str, &str)],
        links: &[(&str, &str)],
        bind: &str,
        host: &str,
        credential: Option<(&str, &str)>,
    ) -> Option<GitServer> {
        let backend = Command::new("git")
            .arg("--exec-path")
            .output()
            .ok()
            .filter(|o| o.status.success())
            .map(|o| {
                PathBuf::from(String::from_utf8_lossy(&o.stdout).trim()).join("git-http-backend")
            })
            .filter(|p| p.exists());
        let Some(backend) = backend else {
            eprintln!("no git, or no git http-backend, here - skipping");
            return None;
        };

        let root = TempRoot(std::env::temp_dir().join(format!(
            "dagron-git-server-{}",
            uuid::Uuid::new_v4().simple()
        )));
        std::fs::create_dir(&root.0).unwrap();
        let work = root.0.join("work");
        std::fs::create_dir(&work).unwrap();
        git(&work, &["init", "-q"]);
        let commit_as = |message: &str, empty: bool| {
            let mut args = vec![
                "-c",
                "user.name=dagron",
                "-c",
                "user.email=dagron@example.invalid",
                "commit",
                "-q",
                "-m",
                message,
            ];
            if empty {
                args.push("--allow-empty");
            }
            git(&work, &args);
            git(&work, &["rev-parse", "HEAD"])
        };
        // A commit before the one that is asked for, and a branch off it.
        let earlier = commit_as("an earlier commit", true);
        git(&work, &["checkout", "-q", "-b", "side"]);
        std::fs::write(work.join("SIDE"), "on a branch beside main\n").unwrap();
        git(&work, &["add", "-A"]);
        let side = commit_as("on a branch beside main", false);
        git(
            &work,
            &[
                "-c",
                "user.name=dagron",
                "-c",
                "user.email=dagron@example.invalid",
                "tag",
                "-a",
                "-m",
                "the first release",
                "v1",
            ],
        );
        git(&work, &["checkout", "-q", "main"]);
        for (path, content) in files {
            let at = work.join(path);
            std::fs::create_dir_all(at.parent().unwrap()).unwrap();
            std::fs::write(at, content).unwrap();
        }
        #[cfg(unix)]
        for (path, target) in links {
            std::os::unix::fs::symlink(target, work.join(path)).unwrap();
        }
        #[cfg(not(unix))]
        let _ = links;
        git(&work, &["add", "-A"]);
        git(
            &work,
            &[
                "-c",
                "user.name=dagron",
                "-c",
                "user.email=dagron@example.invalid",
                "commit",
                "-q",
                "-m",
                "the one commit",
            ],
        );
        let commit = git(&work, &["rev-parse", "HEAD"]);
        let bare = root.0.join("repo.git");
        git(
            &root.0,
            &["clone", "-q", "--bare", work.to_str().unwrap(), "repo.git"],
        );
        // A fetch asks for the commit by its id, as a checkout does.
        git(&bare, &["config", "uploadpack.allowAnySHA1InWant", "true"]);
        git(&bare, &["config", "http.receivepack", "false"]);

        let listener = TcpListener::bind((bind, 0)).unwrap();
        let port = listener.local_addr().unwrap().port();
        let seen = Arc::new(Mutex::new(Vec::new()));
        let expected = credential
            .map(|(user, password)| format!("Basic {}", base64(&format!("{user}:{password}"))));
        let (project_root, record) = (root.0.clone(), seen.clone());
        std::thread::spawn(move || {
            for stream in listener.incoming().flatten() {
                let (backend, project_root, record, expected) = (
                    backend.clone(),
                    project_root.clone(),
                    record.clone(),
                    expected.clone(),
                );
                std::thread::spawn(move || {
                    let _ = serve(
                        stream,
                        &backend,
                        &project_root,
                        &record,
                        expected.as_deref(),
                    );
                });
            }
        });
        Some(GitServer {
            url: format!("http://{host}:{port}/repo.git"),
            commit,
            earlier,
            side,
            seen,
            _root: root,
        })
    }
}

/// One request: parsed, checked for the credential, handed to the backend
/// as CGI, and its answer written back.
fn serve(
    stream: TcpStream,
    backend: &Path,
    project_root: &Path,
    seen: &Mutex<Vec<Option<String>>>,
    expected: Option<&str>,
) -> std::io::Result<()> {
    let mut reader = BufReader::new(stream.try_clone()?);
    let mut line = String::new();
    reader.read_line(&mut line)?;
    let mut parts = line.split_whitespace();
    let (method, target) = (
        parts.next().unwrap_or_default().to_string(),
        parts.next().unwrap_or_default().to_string(),
    );
    let mut headers = Vec::new();
    loop {
        let mut h = String::new();
        reader.read_line(&mut h)?;
        let h = h.trim_end();
        if h.is_empty() {
            break;
        }
        if let Some((name, value)) = h.split_once(':') {
            headers.push((name.trim().to_ascii_lowercase(), value.trim().to_string()));
        }
    }
    let header = |name: &str| {
        headers
            .iter()
            .find(|(n, _)| n == name)
            .map(|(_, v)| v.clone())
    };
    let authorization = header("authorization");
    seen.lock().unwrap().push(authorization.clone());

    let mut out = stream;
    if let Some(expected) = expected {
        if authorization.as_deref() != Some(expected) {
            return out.write_all(
                b"HTTP/1.1 401 Unauthorized\r\nWWW-Authenticate: Basic realm=\"git\"\r\n\
                  Content-Length: 0\r\nConnection: close\r\n\r\n",
            );
        }
    }

    let mut body = Vec::new();
    if let Some(len) = header("content-length").and_then(|l| l.parse::<usize>().ok()) {
        body.resize(len, 0);
        reader.read_exact(&mut body)?;
    } else if header("transfer-encoding").is_some_and(|t| t.eq_ignore_ascii_case("chunked")) {
        loop {
            let mut size = String::new();
            reader.read_line(&mut size)?;
            let size = usize::from_str_radix(size.trim(), 16).unwrap_or(0);
            let mut chunk = vec![0; size + 2];
            reader.read_exact(&mut chunk)?;
            if size == 0 {
                break;
            }
            body.extend_from_slice(&chunk[..size]);
        }
    }

    let (path, query) = target.split_once('?').unwrap_or((target.as_str(), ""));
    let mut cgi = Command::new(backend);
    cgi.env_clear()
        .env("GIT_PROJECT_ROOT", project_root)
        .env("GIT_HTTP_EXPORT_ALL", "1")
        .env("REQUEST_METHOD", &method)
        .env("PATH_INFO", path)
        .env("QUERY_STRING", query)
        .env("CONTENT_LENGTH", body.len().to_string())
        .env("REMOTE_ADDR", "127.0.0.1")
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .env("GIT_CONFIG_GLOBAL", "/dev/null")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null());
    if let Some(v) = header("content-type") {
        cgi.env("CONTENT_TYPE", v);
    }
    if let Some(v) = header("content-encoding") {
        cgi.env("HTTP_CONTENT_ENCODING", v);
    }
    if let Some(v) = header("git-protocol") {
        cgi.env("GIT_PROTOCOL", v);
    }
    let mut child = cgi.spawn()?;
    child.stdin.take().unwrap().write_all(&body)?;
    let answer = child.wait_with_output()?.stdout;

    // A CGI answer: its headers, a blank line, its body.
    let split = answer
        .windows(4)
        .position(|w| w == b"\r\n\r\n")
        .map(|i| (i, 4))
        .or_else(|| answer.windows(2).position(|w| w == b"\n\n").map(|i| (i, 2)));
    let (head, rest) = match split {
        Some((i, n)) => (
            String::from_utf8_lossy(&answer[..i]).to_string(),
            &answer[i + n..],
        ),
        None => (String::new(), &answer[..]),
    };
    let mut status = "200 OK".to_string();
    let mut response = Vec::new();
    for h in head.lines() {
        match h.split_once(':') {
            Some((name, value)) if name.eq_ignore_ascii_case("status") => {
                status = value.trim().to_string();
            }
            Some(_) => response.extend_from_slice(format!("{h}\r\n").as_bytes()),
            None => {}
        }
    }
    out.write_all(format!("HTTP/1.1 {status}\r\n").as_bytes())?;
    out.write_all(&response)?;
    out.write_all(
        format!(
            "Content-Length: {}\r\nConnection: close\r\n\r\n",
            rest.len()
        )
        .as_bytes(),
    )?;
    out.write_all(rest)
}

/// Standard base64, padded: what a basic credential is written in.
pub fn base64(s: &str) -> String {
    const ALPHABET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let bytes = s.as_bytes();
    let mut out = String::new();
    for chunk in bytes.chunks(3) {
        let b = [
            chunk[0],
            *chunk.get(1).unwrap_or(&0),
            *chunk.get(2).unwrap_or(&0),
        ];
        let n = (u32::from(b[0]) << 16) | (u32::from(b[1]) << 8) | u32::from(b[2]);
        for i in 0..4 {
            if i <= chunk.len() {
                out.push(ALPHABET[((n >> (18 - 6 * i)) & 63) as usize] as char);
            } else {
                out.push('=');
            }
        }
    }
    out
}

//! `dagron-ci repo`, through the shipped binary: a repository is connected,
//! changed and disconnected at a terminal, and each of those is on record
//! with who the operator said they were.

use std::process::Command;

const BIN: &str = env!("CARGO_BIN_EXE_dagron-ci");
const REPO: &str = "github.com/acme/widgets";

/// `dagron-ci repo ARGS` on the database in `dir`, as `actor`: whether it
/// succeeded, and what it printed (its errors, where it did not).
fn repo(dir: &std::path::Path, actor: Option<&str>, args: &[&str]) -> (bool, String) {
    let secret = dir.join("secret");
    std::fs::write(&secret, "s3cret\n").unwrap();
    let token = dir.join("token");
    std::fs::write(&token, "dgr_ci_token\n").unwrap();
    let mut cmd = Command::new(BIN);
    cmd.arg("repo")
        .args(args)
        .env(
            "CI_DATABASE_URL",
            format!("sqlite://{}", dir.join("ci.db").display()),
        )
        .env("CI_GITHUB_WEBHOOK_SECRET_FILE", &secret)
        .env("CI_CELLS", "default=http://127.0.0.1:9")
        .env("CI_API_TOKEN_FILE", &token)
        .env("CI_POLL_SECS", "0")
        .env("RUST_LOG", "warn");
    for name in ["DAGRON_CI_ACTOR", "USER", "LOGNAME", "USERNAME"] {
        cmd.env_remove(name);
    }
    if let Some(actor) = actor {
        cmd.env("DAGRON_CI_ACTOR", actor);
    }
    let out = cmd.output().unwrap();
    let said = if out.status.success() {
        &out.stdout
    } else {
        &out.stderr
    };
    (
        out.status.success(),
        String::from_utf8_lossy(said).into_owned(),
    )
}

#[test]
fn a_repository_changed_at_a_terminal_says_who_changed_it() {
    let dir = tempfile::tempdir().unwrap();
    let at = dir.path();

    let (ok, said) = repo(at, Some("alice"), &["show", REPO]);
    assert!(!ok && said.contains("was never connected"), "{said}");

    let (ok, said) = repo(at, Some("alice"), &["add", REPO, "--poll-secs", "120"]);
    assert!(
        ok && said.starts_with("connected github.com/acme/widgets ("),
        "{said}"
    );
    let id = said
        .trim()
        .rsplit_once('(')
        .unwrap()
        .1
        .trim_end_matches(')')
        .to_string();
    let (ok, said) = repo(at, Some("alice"), &["add", REPO]);
    assert!(ok && said.contains("is connected already"), "{said}");
    let (ok, said) = repo(at, Some("bob"), &["set", REPO, "--workflow", "wf-1"]);
    assert!(ok && said.starts_with("updated "), "{said}");
    // Said again, it is so already, and nothing more is on record.
    let (ok, said) = repo(at, Some("dave"), &["set", REPO, "--workflow", "wf-1"]);
    assert!(ok && said.contains("is as it was"), "{said}");

    // Disconnected, it is in no listing, and what was done to it is there to
    // read still.
    let (ok, said) = repo(at, None, &["rm", REPO]);
    assert!(
        ok && said.contains("its events and runs are kept"),
        "{said}"
    );
    let (ok, said) = repo(at, None, &["rm", REPO]);
    assert!(!ok && said.contains("is not connected"), "{said}");
    let (ok, said) = repo(at, None, &["list"]);
    assert!(ok && said.is_empty(), "{said}");
    let (ok, said) = repo(at, Some("bob"), &["set", REPO, "--cell", "default"]);
    assert!(!ok && said.contains("is not connected"), "{said}");

    // Connected again, it is the repository it was, set to what this `add`
    // says and no more, as a first time.
    let (ok, said) = repo(at, Some("  carol\n"), &["add", REPO]);
    assert!(ok, "{said}");
    assert_eq!(
        said.trim(),
        format!("connected again github.com/acme/widgets ({id})")
    );

    let (ok, said) = repo(at, None, &["show", REPO]);
    assert!(ok, "{said}");
    let mut lines = said.lines();
    let first: Vec<&str> = lines.next().unwrap().split('\t').collect();
    assert_eq!(
        first,
        [
            REPO,
            "github",
            "mode=native",
            "workflow=-",
            "cell=default",
            "connected",
            id.as_str()
        ]
    );
    let changes: Vec<(String, String, String)> = lines
        .map(|line| {
            let mut fields = line.split('\t').skip(1).map(str::to_string);
            (
                fields.next().unwrap(),
                fields.next().unwrap(),
                fields.next().unwrap(),
            )
        })
        .collect();
    let said: Vec<(&str, &str)> = changes
        .iter()
        .map(|(actor, action, _)| (actor.as_str(), action.as_str()))
        .collect();
    assert_eq!(
        said,
        [
            ("cli:alice", "connected"),
            ("cli:bob", "changed"),
            // Nobody was named: the change is a terminal's, and no one's.
            ("cli", "disconnected"),
            ("cli:carol", "reconnected"),
        ]
    );
    let connected: serde_json::Value = serde_json::from_str(&changes[0].2).unwrap();
    assert_eq!(connected["poll_secs"], 120, "{connected}");
    assert_eq!(connected["mode"], "native", "{connected}");
    assert_eq!(changes[1].2, r#"{"workflow":"wf-1"}"#);

    // How it is served is changed as the rest is, and recorded.
    let (ok, said) = repo(at, Some("erin"), &["set", REPO, "--mode", "fleet"]);
    assert!(ok && said.starts_with("updated "), "{said}");
    let (ok, said) = repo(at, Some("erin"), &["set", REPO, "--mode", "push"]);
    assert!(!ok && said.contains("usage:"), "{said}");
    let (ok, said) = repo(at, None, &["list"]);
    assert!(ok && said.contains("\tmode=fleet\t"), "{said}");
    let (_, said) = repo(at, None, &["show", REPO]);
    let last = said.lines().last().unwrap();
    assert!(
        last.contains("cli:erin\tchanged\t{\"mode\":\"fleet\"}"),
        "{last}"
    );

    // Connected again without `--forge`, a repository is on the forge it
    // was on.
    const PROJECT: &str = "gitlab.example/group/app";
    let forge_of = |at: &std::path::Path| {
        let (ok, said) = repo(at, None, &["show", PROJECT]);
        assert!(ok, "{said}");
        said.lines()
            .next()
            .unwrap()
            .split('\t')
            .nth(1)
            .unwrap()
            .to_string()
    };
    let (ok, said) = repo(at, None, &["add", PROJECT, "--forge", "gitlab"]);
    assert!(ok, "{said}");
    let (ok, said) = repo(at, None, &["rm", PROJECT]);
    assert!(ok, "{said}");
    let (ok, said) = repo(at, None, &["add", PROJECT]);
    assert!(ok && said.starts_with("connected again "), "{said}");
    assert_eq!(forge_of(at), "gitlab");
}

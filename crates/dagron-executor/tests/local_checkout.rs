//! A workspace's checkout on the local executor: the commit fetched with this
//! host's git, over smart HTTP, from a server of the test's own, with the
//! attempt's credential where the server wants one, before the task runs in
//! the workspace. Skips, saying so, where git or its http-backend is missing.

mod support;

use dagron_core::workspace::{ActionSpec, CheckoutSpec, WorkspaceSpec};
use dagron_executor::checkout::Credential;
use dagron_executor::executor::{ExecContext, Executor, LocalExecutor};
use support::git_server::{base64, GitServer};

const FILES: &[(&str, &str)] = &[
    ("README", "hello from the commit\n"),
    ("src/build.sh", "echo built\n"),
];

fn task(server: &GitServer, script: &str) -> ExecContext {
    let mut ctx = ExecContext::new(
        vec!["sh".into(), "-c".into(), script.into()],
        Some(60),
        None,
    );
    ctx.workspace = Some(WorkspaceSpec {
        checkout: Some(CheckoutSpec {
            repository: server.url.clone(),
            commit: server.commit.clone(),
            depth: None,
            patched: false,
        }),
        ..WorkspaceSpec::default()
    });
    ctx
}

/// A task's actions: each fetched with this host's git at the ref it names
/// (a branch, a tag, a commit's id) into a directory of its own beside the
/// workspace, which the task is told of and which goes with the attempt.
/// What is there is the action's files, and nothing of git's. A ref the
/// repository does not have fails the attempt before the task runs, and
/// says which.
#[tokio::test]
async fn a_task_s_actions_are_fetched_at_the_refs_they_name() {
    let Some(server) = GitServer::start(FILES, "127.0.0.1", "127.0.0.1", None) else {
        return;
    };
    let action = |git_ref: &str, path: &str| ActionSpec {
        repository: server.url.clone(),
        git_ref: git_ref.into(),
        path: path.into(),
        credential: false,
    };
    let script = format!(
        "cd \"$DAGRON_ACTIONS_DIR\" && pwd; \
         cat acme/repo/main/README; cat acme/repo/v1/SIDE; \
         test -e acme/repo/{earlier}/README && echo files || echo none; \
         test -e acme/repo/main/.git && echo git || echo no-git",
        earlier = server.earlier,
    );
    let mut ctx = ExecContext::new(vec!["sh".into(), "-c".into(), script], Some(60), None);
    ctx.workspace = Some(WorkspaceSpec {
        actions: vec![
            action("main", "acme/repo/main"),
            action("v1", "acme/repo/v1"),
            action(&server.earlier, &format!("acme/repo/{}", server.earlier)),
        ],
        ..WorkspaceSpec::default()
    });
    let out = LocalExecutor::default()
        .execute(&ctx)
        .await
        .expect("the task ran");
    assert!(out.success, "{}", out.output);
    let lines: Vec<&str> = out.output.lines().collect();
    let dir = lines
        .iter()
        .find(|l| l.contains("dagron-ws-"))
        .expect("where the actions were");
    assert_eq!(
        lines
            .iter()
            .filter(|l| !l.contains("dagron-ws-"))
            .collect::<Vec<_>>(),
        [
            &"hello from the commit",
            &"on a branch beside main",
            &"none",
            &"no-git"
        ],
        "a branch, a tag, an empty commit by its id; and no .git"
    );
    assert!(
        !std::path::Path::new(dir.trim()).exists(),
        "the actions go with the attempt"
    );

    let mut missing = ExecContext::new(
        vec!["sh".into(), "-c".into(), "echo ran".into()],
        Some(60),
        None,
    );
    missing.workspace = Some(WorkspaceSpec {
        actions: vec![action("v9", "acme/repo/v9")],
        ..WorkspaceSpec::default()
    });
    let err = LocalExecutor::default()
        .execute(&missing)
        .await
        .expect_err("no such ref");
    let err = format!("{err:#}");
    assert!(err.contains("has no v9 to fetch"), "{err}");
}

/// A workspace's commit comes alone unless the checkout says how deep its
/// history goes. Alone, the commit before it is not there to ask about. At
/// 0 every branch's history is: the commit's own, and the branch beside it,
/// which is what `git merge-base` needs of a pull request and its base.
#[tokio::test]
async fn a_checkout_comes_with_as_much_history_as_it_asks_for() {
    let Some(server) = GitServer::start(FILES, "127.0.0.1", "127.0.0.1", None) else {
        return;
    };
    let script = format!(
        "git rev-parse HEAD; git rev-list --count HEAD; \
         git cat-file -e {earlier} 2>/dev/null && echo earlier || echo no-earlier; \
         git cat-file -e {side} 2>/dev/null && echo side || echo no-side; \
         git merge-base HEAD {side} 2>/dev/null || echo no-base; cat README",
        earlier = server.earlier,
        side = server.side,
    );
    let ran = |depth: Option<u32>| {
        let mut ctx = task(&server, &script);
        ctx.workspace.as_mut().unwrap().checkout.as_mut().unwrap().depth = depth;
        async move {
            let out = LocalExecutor::default()
                .execute(&ctx)
                .await
                .expect("the task ran");
            assert!(out.success, "{}", out.output);
            out.output
        }
    };
    let alone = format!(
        "{}\n1\nno-earlier\nno-side\nno-base\nhello from the commit\n",
        server.commit
    );
    assert_eq!(ran(None).await, alone);
    assert_eq!(ran(Some(1)).await, alone);
    // Two deep: the commit before it, and still not the branch beside it.
    assert_eq!(
        ran(Some(2)).await,
        format!(
            "{}\n2\nearlier\nno-side\nno-base\nhello from the commit\n",
            server.commit
        )
    );
    // All of it: both, and where the two branches part.
    assert_eq!(
        ran(Some(0)).await,
        format!(
            "{}\n2\nearlier\nside\n{}\nhello from the commit\n",
            server.commit, server.earlier
        )
    );
}

fn credential(password: &str) -> Credential {
    Credential {
        username: "x-access-token".into(),
        password: password.into(),
    }
}

#[tokio::test]
async fn a_workspace_starts_with_its_commit_checked_out() {
    let Some(server) = GitServer::start(FILES, "127.0.0.1", "127.0.0.1", None) else {
        return;
    };
    let ctx = task(
        &server,
        "cat README; sh src/build.sh; git rev-parse HEAD; git status --porcelain | wc -l",
    );
    let out = LocalExecutor::default()
        .execute(&ctx)
        .await
        .expect("the task ran");
    assert!(out.success, "{}", out.output);
    assert_eq!(
        out.output,
        format!("hello from the commit\nbuilt\n{}\n0\n", server.commit),
        "the commit, alone and clean"
    );
    assert!(server.seen.lock().unwrap().iter().all(Option::is_none));
}

#[tokio::test]
async fn a_private_repository_is_fetched_with_the_attempt_s_credential_and_the_task_never_sees_it()
{
    let Some(server) = GitServer::start(
        FILES,
        "127.0.0.1",
        "127.0.0.1",
        Some(("x-access-token", "ghs_for_this_attempt")),
    ) else {
        return;
    };
    let mut ctx = task(
        &server,
        "cat README; env | grep -c -i -e ghs_for_this_attempt -e extraheader || true; \
         grep -rli -e ghs_for_this_attempt -e extraheader . | wc -l",
    );
    ctx.checkout_credential = Some(credential("ghs_for_this_attempt"));
    let out = LocalExecutor::default()
        .execute(&ctx)
        .await
        .expect("the task ran");
    assert!(out.success, "{}", out.output);
    assert_eq!(
        out.output, "hello from the commit\n0\n0\n",
        "the task's environment and its checkout hold no credential"
    );
    let expected = format!("Basic {}", base64("x-access-token:ghs_for_this_attempt"));
    let seen = server.seen.lock().unwrap();
    assert!(
        !seen.is_empty() && seen.iter().all(|a| a.as_deref() == Some(expected.as_str())),
        "{seen:?}"
    );
}

#[tokio::test]
async fn without_its_credential_a_private_checkout_fails_in_git_s_words_and_the_task_never_runs() {
    let Some(server) = GitServer::start(
        FILES,
        "127.0.0.1",
        "127.0.0.1",
        Some(("x-access-token", "right")),
    ) else {
        return;
    };
    for given in [None, Some(credential("wrong"))] {
        let marker =
            std::env::temp_dir().join(format!("dagron-ran-{}", uuid::Uuid::new_v4().simple()));
        let mut ctx = task(&server, &format!("touch {}", marker.display()));
        ctx.checkout_credential = given.clone();
        let err = LocalExecutor::default()
            .execute(&ctx)
            .await
            .expect_err("no checkout, no task")
            .to_string();
        assert!(
            err.contains(&format!(
                "the checkout of {} at {} failed",
                server.url, server.commit
            )),
            "{err}"
        );
        assert!(
            err.contains("terminal prompts disabled")
                || err.contains("Authentication failed")
                || err.contains("401"),
            "git says why: {err}"
        );
        assert!(!err.contains("right") && !err.contains("wrong\n"), "{err}");
        assert!(!marker.exists(), "{given:?}: the task ran");
    }
}

/// A server that answers every request with a redirect to the same path under
/// `to`, the way a repository that moved, or a host that wants its fetches
/// elsewhere, answers. Its own URL for the repository.
fn redirecting_to(to: &str) -> String {
    use std::io::{Read, Write};
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let to = to.to_string();
    std::thread::spawn(move || {
        for mut stream in listener.incoming().flatten() {
            let mut head = Vec::new();
            let mut byte = [0u8; 1];
            while !head.ends_with(b"\r\n\r\n") && matches!(stream.read(&mut byte), Ok(1)) {
                head.push(byte[0]);
            }
            let head = String::from_utf8_lossy(&head);
            let path = head.split_whitespace().nth(1).unwrap_or("/");
            let rest = path.strip_prefix("/repo.git").unwrap_or(path);
            let _ = write!(
                stream,
                "HTTP/1.1 302 Found\r\nLocation: {to}{rest}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
            );
        }
    });
    format!("http://127.0.0.1:{port}/repo.git")
}

/// A fetch that carries a credential follows no redirect: the repository's
/// own URL is the one place the credential goes, so a redirect fails the
/// checkout, and wherever it pointed never hears from the fetch. Without a
/// credential there is nothing to give away, and a redirect is followed.
#[tokio::test]
async fn a_fetch_with_a_credential_follows_no_redirect() {
    let Some(server) = GitServer::start(FILES, "127.0.0.1", "127.0.0.1", None) else {
        return;
    };
    let moved = redirecting_to(&server.url);
    let checkout_of = |repository: &str| {
        let mut ctx = task(&server, "cat README");
        ctx.workspace
            .as_mut()
            .unwrap()
            .checkout
            .as_mut()
            .unwrap()
            .repository = repository.to_string();
        ctx
    };

    let mut ctx = checkout_of(&moved);
    ctx.checkout_credential = Some(credential("ghs_for_this_attempt"));
    let err = LocalExecutor::default()
        .execute(&ctx)
        .await
        .expect_err("a redirect fails a fetch with a credential")
        .to_string();
    assert!(
        err.contains(&format!(
            "the checkout of {moved} at {} failed",
            server.commit
        )),
        "{err}"
    );
    assert!(!err.contains("ghs_for_this_attempt"), "{err}");
    assert!(
        server.seen.lock().unwrap().is_empty(),
        "the redirect's target heard from the fetch: {:?}",
        server.seen.lock().unwrap()
    );

    let out = LocalExecutor::default()
        .execute(&checkout_of(&moved))
        .await
        .expect("without a credential, the redirect is followed");
    assert!(out.success, "{}", out.output);
    assert_eq!(out.output, "hello from the commit\n");
    assert!(server.seen.lock().unwrap().iter().all(Option::is_none));
}

/// An action of a private repository that asks for a credential is fetched
/// with the one minted for it, which the task never sees; without it, the
/// fetch fails in git's words and the task never runs.
#[tokio::test]
async fn a_private_action_is_fetched_with_its_credential_and_the_task_never_sees_it() {
    let Some(server) = GitServer::start(
        FILES,
        "127.0.0.1",
        "127.0.0.1",
        Some(("x-access-token", "ghs_for_the_action")),
    ) else {
        return;
    };
    let action = ActionSpec {
        repository: server.url.clone(),
        git_ref: "main".into(),
        path: "acme/repo/main".into(),
        credential: true,
    };
    let task_of = |script: &str| {
        let mut ctx = ExecContext::new(
            vec!["sh".into(), "-c".into(), script.into()],
            Some(60),
            None,
        );
        ctx.workspace = Some(WorkspaceSpec {
            actions: vec![action.clone()],
            ..WorkspaceSpec::default()
        });
        ctx
    };
    let mut ctx = task_of(
        "cat \"$DAGRON_ACTIONS_DIR/acme/repo/main/README\"; \
         env | grep -c -i -e ghs_for_the_action -e extraheader || true",
    );
    ctx.action_credentials = vec![(server.url.clone(), credential("ghs_for_the_action"))];
    let out = LocalExecutor::default()
        .execute(&ctx)
        .await
        .expect("the task ran");
    assert!(out.success, "{}", out.output);
    assert_eq!(out.output, "hello from the commit\n0\n");
    let expected = format!("Basic {}", base64("x-access-token:ghs_for_the_action"));
    assert!(
        server
            .seen
            .lock()
            .unwrap()
            .iter()
            .any(|a| a.as_deref() == Some(expected.as_str())),
        "the fetch was sent its credential"
    );

    let marker = std::env::temp_dir().join(format!("dagron-ran-{}", uuid::Uuid::new_v4().simple()));
    let ctx = task_of(&format!("touch {}", marker.display()));
    let err = LocalExecutor::default()
        .execute(&ctx)
        .await
        .expect_err("no credential, no action, no task");
    let err = format!("{err:#}");
    assert!(
        err.contains("the fetch of the task's actions failed"),
        "{err}"
    );
    assert!(!marker.exists(), "the task ran");
}

/// A server of one file over plain HTTP: what a bundle's link serves.
fn serve_once_each(body: Vec<u8>) -> String {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let url = format!("http://{}/patch.tar", listener.local_addr().unwrap());
    std::thread::spawn(move || {
        for stream in listener.incoming() {
            let Ok(mut stream) = stream else { continue };
            let mut buf = [0u8; 4096];
            let _ = std::io::Read::read(&mut stream, &mut buf);
            let head = format!(
                "HTTP/1.1 200 OK\r\ncontent-type: application/x-tar\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
                body.len()
            );
            let _ = std::io::Write::write_all(&mut stream, head.as_bytes());
            let _ = std::io::Write::write_all(&mut stream, &body);
        }
    });
    url
}

/// 1.6: a `.dagron-deleted` the commit has of its own deletes nothing: only
/// the patch's says what it deletes, and the commit's is left as it was.
#[tokio::test]
async fn a_commit_s_own_dagron_deleted_deletes_nothing() {
    let has = |tool: &str| {
        std::process::Command::new(tool)
            .arg("--version")
            .output()
            .is_ok()
    };
    if !has("tar") || !(has("curl") || has("wget")) {
        eprintln!("skipped: a patched checkout needs tar, and curl or wget, which this host lacks");
        return;
    }
    let mut files = FILES.to_vec();
    files.push((".dagron-deleted", "README\n"));
    let Some(server) = GitServer::start(&files, "127.0.0.1", "127.0.0.1", None) else {
        return;
    };
    let patch = std::env::temp_dir().join(format!(
        "dagron-patch-{}",
        uuid::Uuid::new_v4().simple()
    ));
    std::fs::create_dir_all(patch.join("new")).unwrap();
    std::fs::write(patch.join("new/file"), "new\n").unwrap();
    let tar = std::process::Command::new("tar")
        .args(["-cf", "-", "-C"])
        .arg(&patch)
        .arg("new/file")
        .output()
        .unwrap();
    assert!(tar.status.success());
    let mut ctx = task(&server, "cat README; cat new/file; cat .dagron-deleted");
    if let Some(checkout) = ctx.workspace.as_mut().and_then(|w| w.checkout.as_mut()) {
        checkout.patched = true;
    }
    ctx.checkout_patch = Some(serve_once_each(tar.stdout));
    let out = LocalExecutor::default()
        .execute(&ctx)
        .await
        .expect("the task ran");
    assert!(out.success, "{}", out.output);
    for line in ["hello from the commit", "new", "README"] {
        assert!(
            out.output.lines().any(|l| l == line),
            "{line:?} in {}",
            out.output
        );
    }
    let _ = std::fs::remove_dir_all(&patch);
}

/// 1.6: a patch is laid over its commit through no symbolic link of the
/// commit's: a file it carries under one, or one it deletes under one, fails
/// the checkout, and what the link points to is untouched.
#[cfg(unix)]
#[tokio::test]
async fn a_patch_is_laid_through_no_symbolic_link_of_the_commit() {
    let has = |tool: &str| {
        std::process::Command::new(tool)
            .arg("--version")
            .output()
            .is_ok()
    };
    if !has("tar") || !(has("curl") || has("wget")) {
        eprintln!("skipped: a patched checkout needs tar, and curl or wget, which this host lacks");
        return;
    }
    let outside =
        std::env::temp_dir().join(format!("dagron-outside-{}", uuid::Uuid::new_v4().simple()));
    std::fs::create_dir_all(&outside).unwrap();
    std::fs::write(outside.join("victim"), "kept\n").unwrap();
    let target = outside.to_str().unwrap().to_string();
    let Some(server) = GitServer::start_with_links(
        FILES,
        &[("out", target.as_str())],
        "127.0.0.1",
        "127.0.0.1",
        None,
    ) else {
        return;
    };
    let patch =
        std::env::temp_dir().join(format!("dagron-patch-{}", uuid::Uuid::new_v4().simple()));
    for (entries, what) in [
        (vec![("out/evil", "written\n")], "a file under the link"),
        (
            vec![(".dagron-deleted", "out/victim")],
            "a deletion under the link",
        ),
    ] {
        let _ = std::fs::remove_dir_all(&patch);
        std::fs::create_dir_all(&patch).unwrap();
        for (path, content) in &entries {
            let at = patch.join(path);
            std::fs::create_dir_all(at.parent().unwrap()).unwrap();
            std::fs::write(at, content).unwrap();
        }
        let tar = std::process::Command::new("tar")
            .args(["-cf", "-", "-C"])
            .arg(&patch)
            .args(entries.iter().map(|(path, _)| *path))
            .output()
            .unwrap();
        assert!(tar.status.success());
        let mut ctx = task(&server, "echo ran");
        if let Some(checkout) = ctx.workspace.as_mut().and_then(|w| w.checkout.as_mut()) {
            checkout.patched = true;
        }
        ctx.checkout_patch = Some(serve_once_each(tar.stdout));
        let failed = match LocalExecutor::default().execute(&ctx).await {
            Ok(out) => !out.success && !out.output.lines().any(|l| l == "ran"),
            Err(_) => true,
        };
        assert!(failed, "{what}: the checkout went on");
        assert!(
            !outside.join("evil").exists(),
            "{what}: written through the link"
        );
        assert_eq!(
            std::fs::read_to_string(outside.join("victim")).unwrap(),
            "kept\n",
            "{what}: deleted through the link"
        );
    }
    let _ = std::fs::remove_dir_all(&patch);
    let _ = std::fs::remove_dir_all(&outside);
}

/// 1.6: a patched checkout fetches its commit, then lays the patch over it:
/// the files it carries in place of the commit's, the ones it deletes gone,
/// and nothing of it left behind. Without a link to the patch it fails, and
/// the task never runs the commit alone.
#[tokio::test]
async fn a_patched_checkout_lays_its_patch_over_the_commit() {
    let Some(server) = GitServer::start(FILES, "127.0.0.1", "127.0.0.1", None) else {
        return;
    };
    let has = |tool: &str| {
        std::process::Command::new(tool)
            .arg("--version")
            .output()
            .is_ok()
    };
    if !has("tar") || !(has("curl") || has("wget")) {
        eprintln!("skipped: a patched checkout needs tar, and curl or wget, which this host lacks");
        return;
    }
    let patch = std::env::temp_dir().join(format!(
        "dagron-patch-{}-{}",
        std::process::id(),
        server.commit
    ));
    let _ = std::fs::remove_dir_all(&patch);
    std::fs::create_dir_all(&patch).unwrap();
    std::fs::write(
        patch.as_path().join("README"),
        "patched
",
    )
    .unwrap();
    std::fs::create_dir(patch.as_path().join("new")).unwrap();
    std::fs::write(
        patch.as_path().join("new/file"),
        "new
",
    )
    .unwrap();
    std::fs::write(
        patch.as_path().join(".dagron-deleted"),
        // Its last line ends with no newline, and is deleted all the same.
        "no/such/file
src/build.sh",
    )
    .unwrap();
    let tar = std::process::Command::new("tar")
        .args(["-cf", "-", "-C"])
        .arg(patch.as_path())
        .args(["README", "new/file", ".dagron-deleted"])
        .output()
        .unwrap();
    assert!(tar.status.success());
    let link = serve_once_each(tar.stdout);
    let script = "cat README; cat new/file; test -e src/build.sh && echo kept || echo deleted; \
                  test -e .dagron-deleted && echo manifest || echo clean; ls -a | grep -c patch.tar || true";
    let mut ctx = task(&server, script);
    if let Some(checkout) = ctx.workspace.as_mut().and_then(|w| w.checkout.as_mut()) {
        checkout.patched = true;
    }
    ctx.checkout_patch = Some(link);
    let out = LocalExecutor::default()
        .execute(&ctx)
        .await
        .expect("the task ran");
    assert!(out.success, "{}", out.output);
    for line in ["patched", "new", "deleted", "clean"] {
        assert!(
            out.output.lines().any(|l| l == line),
            "{line:?} in {}",
            out.output
        );
    }

    let mut ctx = task(&server, "echo ran");
    if let Some(checkout) = ctx.workspace.as_mut().and_then(|w| w.checkout.as_mut()) {
        checkout.patched = true;
    }
    let out = LocalExecutor::default().execute(&ctx).await;
    let failed = match out {
        Ok(out) => !out.success && !out.output.lines().any(|l| l == "ran"),
        Err(_) => true,
    };
    assert!(
        failed,
        "a patched checkout with no patch never runs the commit alone"
    );
}

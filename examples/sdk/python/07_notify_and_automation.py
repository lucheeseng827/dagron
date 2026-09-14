"""New in 0.9: a CI-shaped run — a revocable token instead of a password, a
git commit-status check that says *what the run produced*, and the
account-wide event feed instead of polling every list.

    python 07_notify_and_automation.py

Three things a CI job wants that a human at the console does not:

1. `Client.from_env()` + `create_token()` — mint a token once, store it, and
   no job ever holds the password that would mint another.
2. `notify.git.description` — a commit-status check that names what the run
   produced, not just that it passed. Four `run.*` names resolve in it
   besides the workflow's own parameters, because those are the things only
   the engine knows: `run.id`, `run.workflow`, `run.status`, `run.images`.
3. `Client.stream_events()` — one account-wide SSE connection instead of
   polling `list_runs` on a timer.
"""
from __future__ import annotations

import os
import socket

from _config import TOKEN, connect
from dagron import Client, Dag

# The commit-status update in step 2 is best-effort: without GITHUB_TOKEN (or
# GITLAB_TOKEN) configured on the server it is a documented no-op, not an
# error — docs/CONFIG.md calls this "unset = forge feedback off" — so this
# example runs to completion either way. Set one on your dagron deployment to
# see the check actually land on a real commit.


def main() -> None:
    api = connect()

    # 1. The token dance a CI job actually wants: mint once with a password
    #    session, then only ever read it back from the environment. Minting
    #    requires a password session — a token cannot mint another, which is
    #    what keeps a leaked one from outrunning revocation.
    #
    #    Which means: if DAGRON_TOKEN is already set, `connect()` handed back a
    #    token-authed client and there is no password session to mint from, so
    #    `create_token` would 403. That is not a problem to work around — it is
    #    the steady state this example is teaching. Skip straight to using it.
    os.environ.setdefault("DAGRON_API_URL", api.base_url)
    if TOKEN:
        print("DAGRON_TOKEN already set — using it (minting needs a password session)")
    else:
        minted = api.create_token("sdk-example-ci", expires_in_days=1)
        print("minted token:", minted["token"][:12] + "…", "(shown once, never again)")
        os.environ["DAGRON_TOKEN"] = minted["token"]
    ci = Client.from_env()  # what the job itself would call — no password in sight

    # 2. A DAG whose commit-status check reports what it built, not just
    #    pass/fail. `description` and the other GitNotify fields are
    #    `{{ param }}`-templated, plus four `run.*` names the caller cannot
    #    supply itself.
    dag = Dag(
        "sdk-ci-build",
        parameters={"commit_sha": "0" * 40},
        notify={
            "git": {
                "provider": "github",
                "repo": "your-org/your-repo",
                "sha": "{{ commit_sha }}",
                "context": "dagron/ci",
                "description": "ran {{ run.workflow }} as {{ run.id }}",
            }
        },
    )
    dag.task("build", command=["echo", "build artifact"])
    print("spec:", dag.to_json())

    run_id = ci.submit_run(dag, parameters={"commit_sha": "a1b2c3d4e5f6"})
    print("submitted run:", run_id)

    # 3. The account-wide feed: one connection sees every run's task-state
    #    changes, which is what the console's live mode is built on. Filter to
    #    the run just submitted and stop as soon as it says something.
    try:
        for ev in ci.stream_events(timeout=15):
            if ev.get("data", {}).get("run_id") == run_id:
                print("  event ->", ev["event"], ev["data"])
                break
    # `socket.timeout`, not `TimeoutError`: this package supports Python 3.8, and
    # the two only became the same object in 3.10. An idle read on 3.8/3.9 raises
    # a `socket.timeout` that a bare `except TimeoutError` walks straight past.
    except socket.timeout:
        print("  (feed idle — falling back to a direct wait)")

    result = ci.wait_run(run_id, timeout_secs=30)
    print("run status:", result["status"], "result:", result.get("result"))


if __name__ == "__main__":
    main()

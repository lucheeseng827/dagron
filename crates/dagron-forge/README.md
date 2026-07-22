# dagron-forge — forge commit-status feedback

`dagron-forge` posts a commit status / PR check to **GitHub or GitLab** when a
dagron run finishes, so a workflow's result surfaces as a green/red check on the
commit that triggered it. When a workflow declares a `notify.git` block, the
engine calls [`ForgeClient::post_status`] on run finalization. It is
**best-effort**, mirroring the OpenLineage emitter: a forge being unreachable
never affects run execution.

## What it does

- `ForgeClient` — posts commit statuses to whichever forge(s) have a token
  configured. `ForgeClient::from_env()` returns `None` when no forge token is
  set, so the engine skips the call entirely; a bounded 10s HTTP timeout keeps a
  slow/hung forge from stalling finalization.
- `CommitState` — the outcome to publish (`Pending` / `Success` / `Failure`).
  `CommitState::from_run_status` maps a dagron run status string (`succeeded` →
  success, `running`/`pending` → pending, everything else → failure). `Pending`
  is reserved for a future run-started hook; the engine posts `Success`/`Failure`
  on finalize today.
- `GitTarget` — a resolved `notify.git` target (provider, repo, sha, context,
  optional `target_url` back to the run in the dagron UI).
- `github_request` / `gitlab_request` — build the provider-specific URL + JSON
  body (GitHub Statuses API vs GitLab Commit Status API, including project-path
  percent-encoding and GitLab's `failed` spelling of failure).

## Quickstart

Wired into the engine's run-finalization path:

```rust
if let Some(client) = dagron_forge::ForgeClient::from_env() {
    let target = dagron_forge::GitTarget { /* from the resolved notify.git block */ };
    let state = dagron_forge::CommitState::from_run_status(run_status);
    // best-effort: log, don't fail the run
    let _ = client.post_status(&target, state).await;
}
```

GitHub uses `Authorization: token <t>`; GitLab uses a `PRIVATE-TOKEN` header.

## Config

A client is returned only if at least one token is set.

| Env | Purpose |
|-----|---------|
| `GITHUB_TOKEN` | GitHub token; enables the GitHub provider |
| `GITHUB_API_BASE` | GitHub API base (default `https://api.github.com`; set for GHE) |
| `GITLAB_TOKEN` | GitLab token; enables the GitLab provider |
| `GITLAB_API_BASE` | GitLab API base (default `https://gitlab.com/api/v4`) |

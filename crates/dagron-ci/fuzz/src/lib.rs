//! What the fuzz targets share: the events a pipeline is compiled for, and
//! a repository's settings for its workflow files.

use std::collections::BTreeMap;

use dagron_ci::pipeline::compile::{Changes, Event};
use dagron_ci::pipeline::github;

/// One event of each kind, with its changed files known, not to be had, and
/// not looked up: each is a road through the compiler.
pub fn events() -> Vec<Event> {
    let event = |kind: &str, git_ref: &str, changed: Changes| Event {
        kind: kind.into(),
        git_ref: git_ref.into(),
        sha: "4b4708e725f29891d22b4c96af9f0a403ff51c7c".into(),
        base_sha: Some("1111111111111111111111111111111111111111".into()),
        base_ref: matches!(kind, "pull_request" | "merge_group").then(|| "main".to_string()),
        pr: (kind == "pull_request").then_some(7),
        trust: "trusted".into(),
        repo: "acme/widgets".into(),
        forge_host: "github.com".into(),
        inputs: BTreeMap::from([("suite".to_string(), "quick".to_string())]),
        changed,
        ..Event::default()
    };
    let listed = || Changes::Listed(vec!["src/lib.rs".into(), "docs/guide.md".into()]);
    vec![
        event("push", "refs/heads/main", listed()),
        event("push", "refs/heads/feature/x", Changes::Unknown),
        event("tag", "refs/tags/v1.2.3", Changes::Unknown),
        event("pull_request", "refs/pull/7/head", listed()),
        event(
            "pull_request",
            "refs/pull/7/head",
            Changes::Unlisted("too large".into()),
        ),
        event(
            "merge_group",
            "refs/heads/gh-readonly-queue/main/pr-7",
            listed(),
        ),
        event("dispatch", "refs/heads/main", Changes::Unknown),
        event("delete", "refs/heads/gone", Changes::Unknown),
    ]
}

/// A repository's settings that give the usual labels an image, so that a
/// workflow file gets past its `runs-on`.
pub fn settings() -> github::Settings {
    let image = "docker.io/library/ubuntu:24.04".to_string();
    github::Settings {
        workflows: vec!["ci.yml".into()],
        runs_on: ["ubuntu-latest", "ubuntu-24.04", "linux", "self-hosted"]
            .into_iter()
            .map(|label| (label.to_string(), image.clone()))
            .collect(),
        provided: ["actions/setup-node", "dtolnay/rust-toolchain"]
            .into_iter()
            .map(str::to_string)
            .collect(),
        ..github::Settings::default()
    }
}

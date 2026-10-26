//! The acts dagron-ci records, told to a build that keeps them or sends them
//! on.
//!
//! The open build tells no one: what it records is in its own tables
//! (`ci_events`, `ci_runs`, `ci_repo_changes`, the checkout credentials) and
//! its log. A build that sends an audit trail elsewhere passes an [`Audit`]
//! in its [`crate::Seams`], and dagron-ci tells it each act of [`kind`] where
//! the act is done:
//! - a delivery refused for its signature or its token;
//! - a run submitted, with the event's actor and trust class;
//! - a pipeline that was not run because it was refused, by the build's
//!   rules or by dagron-api, once its event has ended;
//! - a run cancelled or run again through dagron-ci's doors, with the caller
//!   dagron-api named;
//! - a repository connected, changed or disconnected through the API, with
//!   its caller (one changed at a terminal is in `ci_repo_changes`);
//! - a checkout credential handed to an engine, by its SHA-256.
//!
//! An act is told once, as it is done. A build that must not lose one keeps
//! it before it sends it anywhere: [`Audit::record`] is asked on the request
//! or the event that does the act, and must not wait on a network.

/// What an act was.
pub mod kind {
    /// A webhook delivery refused before it was read: its signature, or its
    /// token, is not the secret's.
    pub const DELIVERY_REFUSED: &str = "ci.delivery.refused";
    /// A run submitted to dagron-api for an event.
    pub const RUN_SUBMITTED: &str = "ci.run.submitted";
    /// A pipeline not run, for good, and why.
    pub const PIPELINE_REFUSED: &str = "ci.pipeline.refused";
    /// A caller asked dagron-ci to cancel an event's runs.
    pub const RUN_CANCEL: &str = "ci.run.cancel";
    /// A caller asked dagron-ci to run an event's failed runs again.
    pub const RUN_RERUN: &str = "ci.run.rerun";
    /// A repository connected, changed or disconnected.
    pub const REPO_CHANGED: &str = "ci.repo.changed";
    /// A checkout credential handed to an engine for a run's job.
    pub const CHECKOUT_CREDENTIAL: &str = "ci.checkout.credential";
}

/// One act.
#[derive(Clone, Debug, PartialEq)]
pub struct Entry {
    /// One of [`kind`].
    pub kind: &'static str,
    /// Who did it: the caller dagron-api named, the forge login an event is
    /// of, or none where dagron-ci did it of itself.
    pub actor: Option<String>,
    /// The repository it was done to: `forge host/owner/name`.
    pub repo: Option<String>,
    /// What else says what it was: a run, an event, a reason.
    pub detail: serde_json::Value,
}

impl Entry {
    pub fn new(kind: &'static str) -> Entry {
        Entry {
            kind,
            actor: None,
            repo: None,
            detail: serde_json::Value::Object(Default::default()),
        }
    }

    pub fn actor(mut self, actor: Option<&str>) -> Entry {
        self.actor = actor.filter(|a| !a.is_empty()).map(str::to_string);
        self
    }

    pub fn repo(mut self, forge_host: &str, full_name: &str) -> Entry {
        self.repo = Some(format!("{forge_host}/{full_name}"));
        self
    }

    /// The same entry, with `name` in its detail.
    pub fn with(mut self, name: &str, value: impl Into<serde_json::Value>) -> Entry {
        if let serde_json::Value::Object(map) = &mut self.detail {
            map.insert(name.to_string(), value.into());
        }
        self
    }
}

/// Where a build keeps the acts dagron-ci records.
pub trait Audit: Send + Sync + 'static {
    /// Keep `entry`. Asked where the act is done: it must not wait on a
    /// network, and a failure to keep it is the build's to say.
    fn record(&self, entry: &Entry);
}

/// Tell `audit`, where there is one.
pub(crate) fn record(audit: Option<&std::sync::Arc<dyn Audit>>, entry: impl FnOnce() -> Entry) {
    if let Some(audit) = audit {
        audit.record(&entry());
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use std::sync::{Arc, Mutex};

    use super::*;

    /// An audit that keeps what it is told, for a test to read.
    #[derive(Default)]
    pub(crate) struct Kept(pub Mutex<Vec<Entry>>);

    impl Audit for Kept {
        fn record(&self, entry: &Entry) {
            self.0.lock().unwrap().push(entry.clone());
        }
    }

    impl Kept {
        pub(crate) fn kinds(&self) -> Vec<&'static str> {
            self.0.lock().unwrap().iter().map(|e| e.kind).collect()
        }
    }

    #[test]
    fn an_entry_says_who_where_and_what() {
        let entry = Entry::new(kind::RUN_SUBMITTED)
            .actor(Some("octocat"))
            .repo("github.com", "acme/widgets")
            .with("run_id", "r1");
        assert_eq!(entry.actor.as_deref(), Some("octocat"));
        assert_eq!(entry.repo.as_deref(), Some("github.com/acme/widgets"));
        assert_eq!(entry.detail["run_id"], "r1");
        assert_eq!(Entry::new(kind::RUN_CANCEL).actor(Some("")).actor, None);
    }

    #[test]
    fn nothing_is_told_without_an_audit() {
        let kept = Arc::new(Kept::default());
        record(None, || panic!("no entry is made without an audit"));
        let audit: Arc<dyn Audit> = kept.clone();
        record(Some(&audit), || Entry::new(kind::RUN_RERUN));
        assert_eq!(kept.kinds(), [kind::RUN_RERUN]);
    }
}

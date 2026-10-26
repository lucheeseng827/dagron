//! One watch of the namespace's task pods, for every task an executor runs.
//!
//! A task's pod was waited for by asking the API server for it twice a
//! second, for as long as the task ran: a thousand jobs were two thousand
//! requests a second that mostly answered "still running". One watch tells
//! of every change to every task pod, and a task that waits is told of its
//! own.
//!
//! **What a waiter may take from it.** A pod's status as the watch last saw
//! it, once the watch has seen the pod at all, and for as long as the watch
//! is following the namespace ([`PodWatch::live`]). In every other case the
//! waiter asks for the pod itself, as it always did: where the watch is not
//! following (it could not list, it was refused, its connection went quiet),
//! where it has seen nothing of the pod yet, and where it saw the pod go. So
//! a watch that fails costs nothing but what it was saving, and a pod it
//! never tells of (one made without the label it selects on) is found by
//! asking.
//!
//! **How the watch keeps following.** It lists the task pods, then watches
//! from where the list ended. The server ends a watch after
//! [`WATCH_SECS`], and it is begun again from the last change seen. A watch
//! that says nothing for [`QUIET`] is taken for dead, which bounds what a
//! connection that hangs can cost: the waiters go back to asking, and the
//! pods are listed again. Any error does the same.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures_util::TryStreamExt;
use k8s_openapi::api::core::v1::{Pod, PodStatus};
use kube::api::{Api, ListParams, WatchEvent, WatchParams};
use tokio::sync::watch;
use tokio::time::timeout;

/// The label every task pod is made with, as a selector: what the watch
/// follows. `build_pod` sets it, and a test there holds the two together.
pub(crate) const TASK_PODS: &str = "app=module-54-scheduler";

/// How long the server is asked to keep one watch open. Short, so that a
/// connection that has died is found out soon: the next is begun from the
/// last change seen, and costs the server one request.
const WATCH_SECS: u32 = 10;

/// How long a watch may say nothing before it is taken for dead. Longer
/// than [`WATCH_SECS`], within which the server ends a quiet one itself.
const QUIET: Duration = Duration::from_secs(15);

/// How long the watch waits before it lists again after a failure, at
/// first and at most. It doubles between.
const RETRY: Duration = Duration::from_secs(1);
const MAX_RETRY: Duration = Duration::from_secs(30);

/// What the watch has seen of one pod.
#[derive(Debug, Clone, Default)]
pub(crate) enum Seen {
    /// Nothing yet: it has not been listed, and no change to it has come.
    #[default]
    Nothing,
    /// Its status as of the last change.
    Status(Option<Box<PodStatus>>),
    /// It was seen, and then was deleted, or was not there when the
    /// namespace was listed again.
    Gone,
}

struct Inner {
    /// The pods a task is waiting for, each with what was last seen of it.
    /// A pod nobody waits for is not kept: the map is as large as the work
    /// in flight.
    pods: Mutex<HashMap<String, watch::Sender<Seen>>>,
    /// Whether the watch is following the namespace now.
    live: AtomicBool,
}

impl Inner {
    fn pods(&self) -> std::sync::MutexGuard<'_, HashMap<String, watch::Sender<Seen>>> {
        self.pods.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// A pod as the watch now has it, told to whoever waits for it.
    fn saw(&self, pod: Pod) {
        let Some(name) = pod.metadata.name else {
            return;
        };
        if let Some(waiter) = self.pods().get(&name) {
            waiter.send_replace(Seen::Status(pod.status.map(Box::new)));
        }
    }

    fn gone(&self, name: &str) {
        if let Some(waiter) = self.pods().get(name) {
            waiter.send_replace(Seen::Gone);
        }
    }

    /// The namespace as it was listed. A pod that was seen before and is not
    /// in the list has gone. One that was never seen is left as it is: it
    /// may not have been made yet.
    fn listed(&self, pods: Vec<Pod>) {
        let mut there: HashMap<String, Option<Box<PodStatus>>> = HashMap::new();
        let waited_for = self.pods();
        for pod in pods {
            if let Some(name) = pod.metadata.name {
                if waited_for.contains_key(&name) {
                    there.insert(name, pod.status.map(Box::new));
                }
            }
        }
        for (name, waiter) in waited_for.iter() {
            match there.remove(name) {
                Some(status) => {
                    waiter.send_replace(Seen::Status(status));
                }
                None => {
                    waiter.send_if_modified(|seen| {
                        let was_seen = matches!(seen, Seen::Status(_));
                        if was_seen {
                            *seen = Seen::Gone;
                        }
                        was_seen
                    });
                }
            }
        }
    }
}

/// The watch, and the task that keeps it. Dropped, the task is stopped.
pub(crate) struct PodWatch {
    inner: Arc<Inner>,
    task: tokio::task::JoinHandle<()>,
}

impl Drop for PodWatch {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl PodWatch {
    /// Begin watching the task pods `api` reaches.
    pub(crate) fn start(api: Api<Pod>) -> Self {
        let inner = Arc::new(Inner {
            pods: Mutex::new(HashMap::new()),
            live: AtomicBool::new(false),
        });
        let task = tokio::spawn(keep_following(api, Arc::clone(&inner)));
        PodWatch { inner, task }
    }

    /// Whether the watch is following the namespace now.
    pub(crate) fn live(&self) -> bool {
        self.inner.live.load(Ordering::SeqCst)
    }

    /// Wait for `name`, from before it is made: what the watch sees of it
    /// from here on is kept for the waiter, and forgotten when the waiter
    /// is done.
    pub(crate) fn follow(&self, name: &str) -> Following {
        let (tx, rx) = watch::channel(Seen::Nothing);
        self.inner.pods().insert(name.to_string(), tx);
        Following {
            inner: Arc::clone(&self.inner),
            name: name.to_string(),
            rx,
        }
    }
}

/// One task's wait for its pod.
pub(crate) struct Following {
    inner: Arc<Inner>,
    name: String,
    rx: watch::Receiver<Seen>,
}

impl Drop for Following {
    fn drop(&mut self) {
        self.inner.pods().remove(&self.name);
    }
}

impl Following {
    /// The pod's status as the watch has it, where the waiter may take it
    /// from there: the watch is following, and has seen the pod. `None`
    /// where the waiter must ask for the pod itself.
    pub(crate) fn status(&mut self) -> Option<Option<PodStatus>> {
        if !self.inner.live.load(Ordering::SeqCst) {
            return None;
        }
        match &*self.rx.borrow_and_update() {
            Seen::Status(status) => Some(status.as_deref().cloned()),
            Seen::Nothing | Seen::Gone => None,
        }
    }

    /// Wait until the watch sees the pod change, or `at_most` has passed.
    pub(crate) async fn changed(&mut self, at_most: Duration) {
        let _ = timeout(at_most, self.rx.changed()).await;
    }
}

/// Follow the namespace for as long as the executor lives, beginning again
/// after each failure.
async fn keep_following(api: Api<Pod>, inner: Arc<Inner>) {
    let mut wait = RETRY;
    let mut said = false;
    loop {
        let began = tokio::time::Instant::now();
        let why = follow(&api, &inner).await;
        let was_live = inner.live.swap(false, Ordering::SeqCst);
        // Said once for each time it stops following, and once if it never
        // began: a watch that is refused (no `watch` on pods) would
        // otherwise say so every half minute for ever.
        if was_live || !said {
            tracing::warn!(
                error = %why,
                "the watch of task pods is not following: each task asks for its own pod until it is"
            );
            said = true;
        }
        if was_live && began.elapsed() > MAX_RETRY {
            wait = RETRY;
        }
        tokio::time::sleep(wait).await;
        wait = (wait * 2).min(MAX_RETRY);
    }
}

/// List the task pods, then watch them. Returns only with why it stopped.
async fn follow(api: &Api<Pod>, inner: &Inner) -> anyhow::Error {
    let listed = match timeout(QUIET, api.list(&ListParams::default().labels(TASK_PODS))).await {
        Ok(Ok(listed)) => listed,
        Ok(Err(e)) => return anyhow::anyhow!("listing them: {e}"),
        Err(_) => return anyhow::anyhow!("listing them took more than {QUIET:?}"),
    };
    let mut version = listed.metadata.resource_version.unwrap_or_default();
    inner.listed(listed.items);
    inner.live.store(true, Ordering::SeqCst);
    let params = WatchParams::default().labels(TASK_PODS).timeout(WATCH_SECS);
    loop {
        let events = match timeout(QUIET, api.watch(&params, &version)).await {
            Ok(Ok(events)) => events,
            Ok(Err(e)) => return anyhow::anyhow!("beginning the watch: {e}"),
            Err(_) => return anyhow::anyhow!("beginning the watch took more than {QUIET:?}"),
        };
        let mut events = std::pin::pin!(events);
        loop {
            let event = match timeout(QUIET, events.try_next()).await {
                Ok(Ok(Some(event))) => event,
                // The server ended it, as it does every WATCH_SECS: begin
                // the next from the last change seen.
                Ok(Ok(None)) => break,
                Ok(Err(e)) => return anyhow::anyhow!("the watch failed: {e}"),
                Err(_) => return anyhow::anyhow!("the watch said nothing for {QUIET:?}"),
            };
            match event {
                WatchEvent::Added(pod) | WatchEvent::Modified(pod) => {
                    if let Some(at) = &pod.metadata.resource_version {
                        version.clone_from(at);
                    }
                    inner.saw(pod);
                }
                WatchEvent::Deleted(pod) => {
                    if let Some(at) = &pod.metadata.resource_version {
                        version.clone_from(at);
                    }
                    if let Some(name) = &pod.metadata.name {
                        inner.gone(name);
                    }
                }
                WatchEvent::Bookmark(mark) => version = mark.metadata.resource_version,
                // Most often that the version asked from is too old to
                // watch from: the pods are listed again.
                WatchEvent::Error(status) => {
                    return anyhow::anyhow!(
                        "the server refused the watch: {} {}",
                        status.code,
                        status.message
                    )
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pod(name: &str, phase: &str) -> Pod {
        serde_json::from_value(serde_json::json!({
            "metadata": { "name": name },
            "status": { "phase": phase },
        }))
        .unwrap()
    }

    fn rig() -> (Arc<Inner>, impl Fn(&str) -> Following) {
        let inner = Arc::new(Inner {
            pods: Mutex::new(HashMap::new()),
            live: AtomicBool::new(true),
        });
        let follow = {
            let inner = Arc::clone(&inner);
            move |name: &str| {
                let (tx, rx) = watch::channel(Seen::Nothing);
                inner.pods().insert(name.to_string(), tx);
                Following {
                    inner: Arc::clone(&inner),
                    name: name.to_string(),
                    rx,
                }
            }
        };
        (inner, follow)
    }

    fn phase(seen: Option<Option<PodStatus>>) -> Option<String> {
        seen.flatten().and_then(|status| status.phase)
    }

    #[tokio::test]
    async fn a_waiter_takes_from_the_watch_only_what_the_watch_has_seen() {
        let (inner, follow) = rig();
        let mut mine = follow("sched-a");
        // Nothing has been seen of it: the waiter asks for it itself.
        assert!(mine.status().is_none());
        // A change to another pod is not this waiter's, and is kept nowhere.
        inner.saw(pod("sched-b", "Running"));
        assert!(mine.status().is_none());
        assert_eq!(inner.pods().len(), 1, "a pod nobody waits for is not kept");

        inner.saw(pod("sched-a", "Pending"));
        assert_eq!(phase(mine.status()).as_deref(), Some("Pending"));
        // A change wakes the waiter, well before its time is up.
        inner.saw(pod("sched-a", "Succeeded"));
        tokio::time::timeout(
            Duration::from_secs(5),
            mine.changed(Duration::from_secs(3600)),
        )
        .await
        .expect("woken by the change");
        assert_eq!(phase(mine.status()).as_deref(), Some("Succeeded"));

        // A watch that is not following is not believed, whatever it last saw.
        inner.live.store(false, Ordering::SeqCst);
        assert!(mine.status().is_none());
        inner.live.store(true, Ordering::SeqCst);
        // A pod that went is asked for: the answer is the API server's.
        inner.gone("sched-a");
        assert!(mine.status().is_none());

        // Done waiting, it is forgotten.
        drop(mine);
        assert!(inner.pods().is_empty());
    }

    #[tokio::test]
    async fn a_list_says_a_pod_has_gone_only_if_it_was_seen() {
        let (inner, follow) = rig();
        let mut made = follow("sched-made");
        let mut not_yet = follow("sched-not-yet");
        inner.saw(pod("sched-made", "Running"));

        // Listed with both there: each is as the list has it.
        inner.listed(vec![
            pod("sched-made", "Running"),
            pod("sched-not-yet", "Pending"),
        ]);
        assert_eq!(phase(made.status()).as_deref(), Some("Running"));
        assert_eq!(phase(not_yet.status()).as_deref(), Some("Pending"));

        // Listed with neither. The one that was seen has gone. A pod that
        // was never seen is not said to have gone: it may not be made yet.
        let (inner, follow) = rig();
        let mut made = follow("sched-made");
        let mut not_yet = follow("sched-not-yet");
        inner.saw(pod("sched-made", "Running"));
        inner.listed(Vec::new());
        assert!(matches!(&*made.rx.borrow_and_update(), Seen::Gone));
        assert!(matches!(&*not_yet.rx.borrow_and_update(), Seen::Nothing));
        assert!(made.status().is_none() && not_yet.status().is_none());
    }
}

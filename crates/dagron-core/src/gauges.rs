//! The datastore gauges `/metrics` serves, in two tiers (dagron-ci plan
//! 10.10), each cached on its own: what has not finished, read for a scrape
//! and cached briefly, and the history, which grows with every run and is
//! read far less often.
//!
//! The engine's `GET /metrics` and its OTLP export share one cache, and
//! dagron-api's `GET /api/metrics` has one of its own. The reads are the
//! caller's ([`crate::db::status_counts_live`] and
//! [`crate::db::status_counts_history`]), handed in as closures, so the cache
//! is the same whichever process holds it, and is tested with no datastore.

use std::future::Future;
use std::sync::{Arc, RwLock};
use std::time::Duration;

use tokio::sync::Mutex;
use tokio::time::Instant;

use crate::models::{MetricsHistory, MetricsSnapshot};

/// A whole-seconds TTL as a setting gives it: `0` is off, and unset or what
/// is no whole number is `default`.
pub fn ttl_from(raw: Option<String>, default: Duration) -> Duration {
    raw.and_then(|v| v.trim().parse::<u64>().ok())
        .map(Duration::from_secs)
        .unwrap_or(default)
}

/// The history's TTL as its setting gives it, beside the live tier's `live`:
/// as [`ttl_from`], but where the setting is not given and `live` is 0, it is
/// 0 too. A live TTL of 0 turned the whole cache off before there were two
/// tiers, and a harness that changes state and then scrapes sets only that.
/// A history TTL that is set is its own, whatever `live` is.
pub fn history_ttl_from(raw: Option<String>, live: Duration, default: Duration) -> Duration {
    if live.is_zero() {
        return ttl_from(raw, Duration::ZERO);
    }
    ttl_from(raw, default)
}

/// The longest a read of what has not finished is waited for. A read that
/// never returns (a connection a failover left half open) would hold the
/// live tier's lock, and every call behind it, for good: it is given up
/// after this, and its call answers an error.
const LIVE_READ_MOST: Duration = Duration::from_secs(30);

/// The least a read of the history is waited for: its TTL, where that is
/// longer. Given up after it, as a live read is.
const HISTORY_READ_LEAST: Duration = Duration::from_secs(60);

/// `read`, or an error once it has taken `most`: a read of `what`.
async fn bounded<T>(
    most: Duration,
    what: &str,
    read: impl Future<Output = anyhow::Result<T>>,
) -> anyhow::Result<T> {
    tokio::time::timeout(most, read).await.unwrap_or_else(|_| {
        Err(anyhow::anyhow!(
            "a read of {what} of the datastore gauges did not return in {most:?}, and is given up"
        ))
    })
}

/// The two tiers of the datastore gauges and when each was read.
pub struct GaugeCache {
    live_ttl: Duration,
    history_ttl: Duration,
    /// How long a read of each tier is waited for ([`LIVE_READ_MOST`],
    /// [`HISTORY_READ_LEAST`]).
    live_most: Duration,
    history_most: Duration,
    /// Held across a read, so the scrapes that come as it expires wait for
    /// one read and do not each make their own.
    live: Mutex<Option<(Instant, MetricsSnapshot)>>,
    /// Never held across a read: a scrape that finds the history stale
    /// serves what it has. Shared with the read that refreshes it, which
    /// runs on its own and outlives the scrape that began it.
    history: Arc<RwLock<Option<(Instant, MetricsHistory)>>>,
    /// Held by the one read of the history in flight.
    history_refresh: Arc<Mutex<()>>,
}

impl GaugeCache {
    /// A cache that serves what has not finished for up to `live_ttl` and the
    /// history for up to `history_ttl`. A TTL of zero reads its tier on every
    /// call, as a scrape read the whole datastore before there was a cache.
    pub fn new(live_ttl: Duration, history_ttl: Duration) -> Self {
        Self {
            live_ttl,
            history_ttl,
            live_most: LIVE_READ_MOST,
            history_most: history_ttl.max(HISTORY_READ_LEAST),
            live: Mutex::new(None),
            history: Arc::new(RwLock::new(None)),
            history_refresh: Arc::new(Mutex::new(())),
        }
    }

    /// The gauges: what has not finished, from `live` or its cache, with the
    /// history, from `history` or its cache, and how old that is.
    ///
    /// The live tier is read first: with the datastore gone, that read fails
    /// and the call with it. A failed read of either tier is never cached,
    /// because `/metrics` answering an error while the datastore is down is
    /// what an operator alerts on.
    ///
    /// A call that finds the history stale serves the history it has at
    /// once, and has it read again behind it, one read at a time: a slow
    /// read of what has finished never holds up a call, the one that found
    /// it stale included, and a call given up on (a scrape's timeout) does
    /// not take the read with it. A read behind a call that fails is logged
    /// and leaves the last history, and the next call to find it stale reads
    /// again. Only a call that finds no history at all, the first after a
    /// start, waits for it, and a failed read is its error. That read runs
    /// on its own as well, so a call given up on while it waits leaves it
    /// to be cached.
    ///
    /// A read of what has not finished is given up after 30 s, and one of
    /// the history after its TTL or 60 s, whichever is longer: a read that
    /// never returns is an error for its call, never a lock held for good.
    pub async fn read<L, LF, H, HF>(&self, live: L, history: H) -> anyhow::Result<MetricsSnapshot>
    where
        L: FnOnce() -> LF,
        LF: Future<Output = anyhow::Result<MetricsSnapshot>>,
        H: FnOnce() -> HF,
        HF: Future<Output = anyhow::Result<MetricsHistory>> + Send + 'static,
    {
        let snap = self.live(live).await?;
        if self.history_ttl.is_zero() {
            let past = bounded(self.history_most, "the history", history()).await?;
            return Ok(snap.with_history(&past, 0.0));
        }
        let (at, past) = self.history(history).await?;
        Ok(snap.with_history(&past, at.elapsed().as_secs_f64()))
    }

    async fn live<L, LF>(&self, live: L) -> anyhow::Result<MetricsSnapshot>
    where
        L: FnOnce() -> LF,
        LF: Future<Output = anyhow::Result<MetricsSnapshot>>,
    {
        if self.live_ttl.is_zero() {
            return bounded(self.live_most, "what has not finished", live()).await;
        }
        let mut cached = self.live.lock().await;
        if let Some((at, snap)) = &*cached {
            if at.elapsed() < self.live_ttl {
                return Ok(snap.clone());
            }
        }
        // Stamped before the read: the snapshot is as old as the moment it
        // was taken, however long the read then ran.
        let at = Instant::now();
        let snap = bounded(self.live_most, "what has not finished", live()).await?;
        *cached = Some((at, snap.clone()));
        Ok(snap)
    }

    async fn history<H, HF>(&self, history: H) -> anyhow::Result<(Instant, MetricsHistory)>
    where
        H: FnOnce() -> HF,
        HF: Future<Output = anyhow::Result<MetricsHistory>> + Send + 'static,
    {
        if let Some(fresh) = self.cached_history(true) {
            return Ok(fresh);
        }
        if let Some(stale) = self.cached_history(false) {
            // Stale: what there is will do, and it is read again behind
            // this call, unless a read is in flight already.
            if let Ok(refresh) = Arc::clone(&self.history_refresh).try_lock_owned() {
                let read = history();
                let cached = Arc::clone(&self.history);
                // A read that does not return (a connection a failover left
                // half open, a lock it waits behind) is given up, so that the
                // next can be made: the scrape that dropped it did so before.
                let most = self.history_most;
                tokio::spawn(async move {
                    // Stamped before the read, as the live tier is.
                    let at = Instant::now();
                    match tokio::time::timeout(most, read).await {
                        Ok(Ok(past)) => {
                            *cached.write().unwrap_or_else(|e| e.into_inner()) = Some((at, past));
                        }
                        Ok(Err(e)) => tracing::warn!(
                            error = %e,
                            "could not read the history of the datastore gauges; the last is \
                             served until a read of it succeeds"
                        ),
                        Err(_) => tracing::warn!(
                            after_secs = most.as_secs(),
                            "a read of the history of the datastore gauges did not return, and \
                             is given up; the last is served until a read of it succeeds"
                        ),
                    }
                    drop(refresh);
                });
            }
            return Ok(stale);
        }
        // Nothing yet: this call has it read, or waits for the read in
        // flight and serves what it brought in. Read on its own, as a stale
        // one is, and holding the lock until it is in, so that a call given
        // up on (a scrape's timeout) does not take the read with it.
        let refresh = Arc::clone(&self.history_refresh).lock_owned().await;
        if let Some(read) = self.cached_history(false) {
            return Ok(read);
        }
        let (read, cached, most) = (history(), Arc::clone(&self.history), self.history_most);
        let first = tokio::spawn(async move {
            let at = Instant::now();
            let past = bounded(most, "the history", read).await;
            if let Ok(past) = &past {
                *cached.write().unwrap_or_else(|e| e.into_inner()) = Some((at, past.clone()));
            }
            drop(refresh);
            past.map(|past| (at, past))
        });
        first.await.unwrap_or_else(|e| {
            Err(anyhow::anyhow!(
                "the read of the history of the datastore gauges failed: {e}"
            ))
        })
    }

    /// The cached history, when it is younger than its TTL or `fresh` is not
    /// asked for.
    fn cached_history(&self, fresh: bool) -> Option<(Instant, MetricsHistory)> {
        let cached = self.history.read().unwrap_or_else(|e| e.into_inner());
        cached
            .as_ref()
            .filter(|(at, _)| !fresh || at.elapsed() < self.history_ttl)
            .cloned()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    use super::*;

    const HOUR: Duration = Duration::from_secs(3600);

    fn live_of(dead_letters: i64) -> MetricsSnapshot {
        MetricsSnapshot {
            runs_by_status: vec![("running".to_string(), 1)],
            dead_letters,
            ..Default::default()
        }
    }

    fn history_of(succeeded: i64) -> MetricsHistory {
        MetricsHistory {
            runs_by_status: vec![("succeeded".to_string(), succeeded)],
            ..Default::default()
        }
    }

    fn succeeded(snap: &MetricsSnapshot) -> i64 {
        snap.runs_by_status
            .iter()
            .find(|(status, _)| status == "succeeded")
            .map_or(0, |(_, n)| *n)
    }

    /// A read that counts the calls it was asked for.
    fn counted<'a, T: 'a>(
        calls: &'a AtomicUsize,
        value: T,
    ) -> impl FnOnce() -> std::future::Ready<anyhow::Result<T>> + 'a {
        move || {
            calls.fetch_add(1, Ordering::SeqCst);
            std::future::ready(Ok(value))
        }
    }

    /// The gauges of what is running are read again once their TTL is past,
    /// and the history is not: it is younger than its own.
    #[tokio::test]
    async fn live_gauges_refresh_while_history_is_cached() {
        let cache = GaugeCache::new(Duration::from_millis(1), HOUR);
        let (live, past) = (AtomicUsize::new(0), AtomicUsize::new(0));
        let first = cache
            .read(counted(&live, live_of(1)), counted(&past, history_of(7)))
            .await
            .unwrap();
        assert_eq!((first.dead_letters, succeeded(&first)), (1, 7));
        tokio::time::sleep(Duration::from_millis(5)).await;
        let second = cache
            .read(counted(&live, live_of(2)), counted(&past, history_of(8)))
            .await
            .unwrap();
        assert_eq!(second.dead_letters, 2, "the live tier was read again");
        assert_eq!(
            succeeded(&second),
            7,
            "the history was served from its cache"
        );
        assert_eq!(live.load(Ordering::SeqCst), 2);
        assert_eq!(past.load(Ordering::SeqCst), 1);
        assert!(second.history_age_secs > 0.0);
        assert_eq!(
            second.runs_by_status.len(),
            2,
            "{:?}",
            second.runs_by_status
        );
    }

    /// With both TTLs zero, every call reads both tiers, as every scrape read
    /// the whole datastore before.
    #[tokio::test]
    async fn a_zero_history_ttl_reads_both_every_time() {
        let cache = GaugeCache::new(Duration::ZERO, Duration::ZERO);
        let (live, past) = (AtomicUsize::new(0), AtomicUsize::new(0));
        for n in 1..=3 {
            let snap = cache
                .read(counted(&live, live_of(n)), counted(&past, history_of(n)))
                .await
                .unwrap();
            assert_eq!((snap.dead_letters, succeeded(&snap)), (n, n));
            assert_eq!(snap.history_age_secs, 0.0);
        }
        assert_eq!(live.load(Ordering::SeqCst), 3);
        assert_eq!(past.load(Ordering::SeqCst), 3);
    }

    /// A call long enough for any read here that is not held up.
    const PROMPT: Duration = Duration::from_secs(5);

    /// The history `read` serves, once it is `want`: what a read behind an
    /// earlier call brought in.
    async fn history_becomes(cache: &GaugeCache, want: i64) {
        let deadline = Instant::now() + PROMPT;
        loop {
            let read = cache
                .read(
                    || async { Ok(live_of(0)) },
                    move || async move { Ok(history_of(want)) },
                )
                .await
                .unwrap();
            if succeeded(&read) == want {
                return;
            }
            assert!(Instant::now() < deadline, "the history stayed {read:?}");
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }

    /// One caller alone, as a single Prometheus or the OTLP export is: a call
    /// that finds the history stale is answered at once with the last one,
    /// and the read it began goes on behind it. While that read is in
    /// flight, no call begins another, and once it is in, it is served.
    #[tokio::test]
    async fn a_stale_history_is_served_at_once_and_read_again_behind() {
        let cache = GaugeCache::new(Duration::ZERO, Duration::from_millis(1));
        cache
            .read(|| async { Ok(live_of(0)) }, || async { Ok(history_of(1)) })
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(5)).await;

        let (go, held) = tokio::sync::oneshot::channel::<()>();
        let (started, reading) = tokio::sync::oneshot::channel::<()>();
        let stale = tokio::time::timeout(
            PROMPT,
            cache.read(
                || async { Ok(live_of(0)) },
                || async move {
                    started.send(()).unwrap();
                    held.await.unwrap();
                    Ok(history_of(2))
                },
            ),
        )
        .await
        .expect("the call that found the history stale did not wait for its read")
        .unwrap();
        assert_eq!(succeeded(&stale), 1, "the last history was served");
        assert!(stale.history_age_secs > 0.0);
        reading.await.unwrap();

        let reads = AtomicUsize::new(0);
        let meanwhile = cache
            .read(|| async { Ok(live_of(0)) }, counted(&reads, history_of(9)))
            .await
            .unwrap();
        assert_eq!(succeeded(&meanwhile), 1, "the last history, again");
        assert_eq!(
            reads.load(Ordering::SeqCst),
            0,
            "no second read while one is in flight"
        );

        go.send(()).unwrap();
        history_becomes(&cache, 2).await;
    }

    /// A read behind a call that fails leaves the last history to be served,
    /// and the next call that finds it stale reads it again.
    #[tokio::test]
    async fn a_failed_read_behind_a_call_leaves_the_last_history() {
        let cache = GaugeCache::new(Duration::ZERO, Duration::from_millis(1));
        cache
            .read(|| async { Ok(live_of(0)) }, || async { Ok(history_of(1)) })
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(5)).await;
        let read = cache
            .read(
                || async { Ok(live_of(0)) },
                || async { Err(anyhow::anyhow!("the datastore is slow to answer")) },
            )
            .await
            .unwrap();
        assert_eq!(succeeded(&read), 1);
        // The read behind that call holds the refresh lock until it has
        // failed and been logged.
        drop(cache.history_refresh.lock().await);

        let reads = AtomicUsize::new(0);
        let after = cache
            .read(|| async { Ok(live_of(0)) }, counted(&reads, history_of(3)))
            .await
            .unwrap();
        assert_eq!(
            succeeded(&after),
            1,
            "the last history is served once the read failed"
        );
        assert_eq!(
            reads.load(Ordering::SeqCst),
            1,
            "the next call to find it stale read it again"
        );
        drop(cache.history_refresh.lock().await);
        history_becomes(&cache, 3).await;
    }

    /// A call given up on while it waits for the first history (a scrape's
    /// timeout) does not take the read with it: the read goes on, and once
    /// it is in, the next call serves it and reads nothing of its own.
    #[tokio::test]
    async fn a_call_given_up_on_leaves_the_first_history_to_be_cached() {
        let cache = GaugeCache::new(Duration::ZERO, HOUR);
        let (go, held) = tokio::sync::oneshot::channel::<()>();
        let given_up = tokio::time::timeout(
            Duration::from_millis(20),
            cache.read(
                || async { Ok(live_of(0)) },
                || async move {
                    held.await.unwrap();
                    Ok(history_of(4))
                },
            ),
        )
        .await;
        assert!(given_up.is_err(), "the call was answered before its read");
        assert!(
            go.send(()).is_ok(),
            "the read went with the call given up on"
        );

        let reads = AtomicUsize::new(0);
        let next = tokio::time::timeout(
            PROMPT,
            cache.read(|| async { Ok(live_of(0)) }, counted(&reads, history_of(5))),
        )
        .await
        .expect("the next call was held")
        .unwrap();
        assert_eq!(
            succeeded(&next),
            4,
            "the read of the call given up on was cached"
        );
        assert_eq!(reads.load(Ordering::SeqCst), 0);
    }

    /// A read that never returns (a connection a failover left half open)
    /// is given up after its bound and is its call's error, and the call
    /// behind it, which waited on the lock that read held, reads for itself:
    /// neither tier is held for good. A cache given bounds of 50 ms here.
    #[tokio::test]
    async fn a_read_that_never_returns_is_given_up() {
        let most = Duration::from_millis(50);
        let bounded = |live_ttl, history_ttl| {
            Arc::new(GaugeCache {
                live_most: most,
                history_most: most,
                ..GaugeCache::new(live_ttl, history_ttl)
            })
        };

        // The live tier: the read holds its lock.
        let cache = bounded(HOUR, HOUR);
        let (started, reading) = tokio::sync::oneshot::channel::<()>();
        let hung = {
            let cache = Arc::clone(&cache);
            tokio::spawn(async move {
                cache
                    .read(
                        || async move {
                            started.send(()).unwrap();
                            std::future::pending().await
                        },
                        || async { Ok(history_of(1)) },
                    )
                    .await
            })
        };
        reading.await.unwrap();
        let behind = tokio::time::timeout(
            PROMPT,
            cache.read(|| async { Ok(live_of(2)) }, || async { Ok(history_of(1)) }),
        )
        .await
        .expect("a call behind a live read that never returns was held")
        .unwrap();
        assert_eq!(behind.dead_letters, 2);
        assert!(
            hung.await.unwrap().is_err(),
            "the hung read is its call's error"
        );

        // The first history: the read holds the refresh lock.
        let cache = bounded(Duration::ZERO, HOUR);
        let (started, reading) = tokio::sync::oneshot::channel::<()>();
        let hung = {
            let cache = Arc::clone(&cache);
            tokio::spawn(async move {
                cache
                    .read(
                        || async { Ok(live_of(0)) },
                        || async move {
                            started.send(()).unwrap();
                            std::future::pending().await
                        },
                    )
                    .await
            })
        };
        reading.await.unwrap();
        let behind = tokio::time::timeout(
            PROMPT,
            cache.read(|| async { Ok(live_of(0)) }, || async { Ok(history_of(3)) }),
        )
        .await
        .expect("a call behind a history read that never returns was held")
        .unwrap();
        assert_eq!(succeeded(&behind), 3);
        assert!(
            hung.await.unwrap().is_err(),
            "the hung read is its call's error"
        );

        // With both TTLs zero, nothing is locked, and each read is bounded.
        let cache = bounded(Duration::ZERO, Duration::ZERO);
        let hung = tokio::time::timeout(
            PROMPT,
            cache.read(std::future::pending, || async { Ok(history_of(1)) }),
        )
        .await
        .expect("a live read that never returns was waited for");
        assert!(hung.is_err());
        let hung = tokio::time::timeout(
            PROMPT,
            cache.read(|| async { Ok(live_of(0)) }, std::future::pending),
        )
        .await
        .expect("a history read that never returns was waited for");
        assert!(hung.is_err());
    }

    /// With no history yet, the first call waits for its read, and a call
    /// that comes while it reads waits for that read and does not make its
    /// own.
    #[tokio::test]
    async fn the_first_history_is_waited_for_and_read_once() {
        let cache = Arc::new(GaugeCache::new(Duration::ZERO, HOUR));
        let (go, held) = tokio::sync::oneshot::channel::<()>();
        let (started, reading) = tokio::sync::oneshot::channel::<()>();
        let first = {
            let cache = Arc::clone(&cache);
            tokio::spawn(async move {
                cache
                    .read(
                        || async { Ok(live_of(0)) },
                        || async move {
                            started.send(()).unwrap();
                            held.await.unwrap();
                            Ok(history_of(4))
                        },
                    )
                    .await
                    .unwrap()
            })
        };
        reading.await.unwrap();
        let reads = Arc::new(AtomicUsize::new(0));
        let second = {
            let (cache, reads) = (Arc::clone(&cache), Arc::clone(&reads));
            tokio::spawn(async move {
                cache
                    .read(
                        || async { Ok(live_of(0)) },
                        move || {
                            reads.fetch_add(1, Ordering::SeqCst);
                            std::future::ready(Ok(history_of(5)))
                        },
                    )
                    .await
                    .unwrap()
            })
        };
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(!second.is_finished(), "it waits for the first history");
        go.send(()).unwrap();
        assert_eq!(succeeded(&first.await.unwrap()), 4);
        assert_eq!(succeeded(&second.await.unwrap()), 4);
        assert_eq!(reads.load(Ordering::SeqCst), 0, "read once");
    }

    /// A failed read of either tier is an error for its call, and the next
    /// call reads again: nothing stale stands in for it.
    #[tokio::test]
    async fn an_error_is_never_served_from_the_cache() {
        let cache = GaugeCache::new(HOUR, HOUR);
        let failed = cache
            .read(
                || async { Err(anyhow::anyhow!("the datastore is gone")) },
                || async { panic!("the history is read after the live tier") },
            )
            .await;
        assert!(failed.is_err());
        let failed = cache
            .read(
                || async { Ok(live_of(4)) },
                || async { Err(anyhow::anyhow!("the datastore is gone")) },
            )
            .await;
        assert!(failed.is_err());
        let read = cache
            .read(|| async { Ok(live_of(5)) }, || async { Ok(history_of(6)) })
            .await
            .unwrap();
        assert_eq!(
            read.dead_letters, 4,
            "the live read that did not fail was kept"
        );
        assert_eq!(succeeded(&read), 6, "the failed history was not");
    }

    #[test]
    fn a_ttl_is_whole_seconds_with_zero_meaning_off() {
        let ten = Duration::from_secs(10);
        assert_eq!(ttl_from(None, ten), ten);
        assert_eq!(ttl_from(Some(" 30 ".into()), ten), Duration::from_secs(30));
        assert_eq!(ttl_from(Some("0".into()), ten), Duration::ZERO);
        assert_eq!(ttl_from(Some("soon".into()), ten), ten);
        assert_eq!(ttl_from(Some("-1".into()), ten), ten);
    }

    /// A live TTL of 0 with the history's not set turns the whole cache off,
    /// as it did before there were two tiers; a history TTL set is its own.
    #[test]
    fn an_unset_history_ttl_follows_a_live_ttl_of_zero() {
        let five = Duration::from_secs(300);
        let ten = Duration::from_secs(10);
        assert_eq!(history_ttl_from(None, Duration::ZERO, five), Duration::ZERO);
        assert_eq!(
            history_ttl_from(Some("300".into()), Duration::ZERO, five),
            five
        );
        assert_eq!(history_ttl_from(None, ten, five), five);
        assert_eq!(
            history_ttl_from(Some("0".into()), ten, five),
            Duration::ZERO
        );
    }
}

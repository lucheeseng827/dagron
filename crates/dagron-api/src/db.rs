//! The datastore: Postgres, or SQLite for one node.
//!
//! Every route is handed a [`Pool`], dagron-core's, for whichever backend this
//! binary was built with (`postgres`, the default, or `sqlite`), and opens it
//! here. The two dialects differ in a few places the routes meet, and those
//! are named here rather than spelled at each query.

#[cfg(feature = "postgres")]
use std::time::Duration;

use anyhow::{Context, Result};
use tracing::info;

pub use dagron_core::db::Pool;

/// The sqlx database of this build's backend.
#[cfg(feature = "postgres")]
pub type Db = sqlx::Postgres;
#[cfg(feature = "sqlite")]
pub type Db = sqlx::Sqlite;

/// What a routine `SELECT … FOR UPDATE` appends. Postgres takes the row lock;
/// SQLite has no row locks, and one writer at a time already serializes what
/// the lock is for.
#[cfg(feature = "postgres")]
pub const FOR_UPDATE: &str = " FOR UPDATE";
#[cfg(feature = "sqlite")]
pub const FOR_UPDATE: &str = "";

/// `column IN` a list bound as one parameter (`param`, as `$1`): Postgres
/// takes the list as an array, SQLite as a JSON array it unpacks. Bind it
/// with [`list`].
#[cfg(feature = "postgres")]
pub fn in_list(param: &str) -> String {
    format!("= ANY({param})")
}
#[cfg(feature = "sqlite")]
pub fn in_list(param: &str) -> String {
    format!("IN (SELECT value FROM json_each({param}))")
}

/// The list for an [`in_list`] parameter, as this backend binds it.
#[cfg(feature = "postgres")]
pub fn list(items: &[String]) -> &[String] {
    items
}
#[cfg(feature = "sqlite")]
pub fn list(items: &[String]) -> String {
    serde_json::to_string(items).expect("a list of strings is JSON")
}

#[cfg_attr(feature = "sqlite", allow(dead_code))]
fn env_parse<T: std::str::FromStr>(key: &str, default: T) -> T {
    std::env::var(key)
        .ok()
        .and_then(|v| v.trim().parse().ok())
        .unwrap_or(default)
}

/// The pool's ceiling: `configured`, and at least 2 where the live-events
/// listener shares the pool (no `DATABASE_LISTEN_URL`). That listener holds
/// one of the pool's connections for as long as it listens, so a pool of one
/// would leave none for requests.
#[cfg(feature = "postgres")]
fn pool_max(configured: u32, listen_url: Option<&str>) -> u32 {
    let listens_apart = listen_url.is_some_and(|url| !url.trim().is_empty());
    configured.max(if listens_apart { 1 } else { 2 })
}

/// Open the pool, as `DATABASE_URL` names it.
///
/// Pool discipline (LOW_LATENCY R-2). Every knob defaults to the previous
/// hard-coded behaviour; the low-latency profile is what changes them:
///  - DAGRON_DB_MAX_CONNECTIONS (default 8; at least 2 without
///    DATABASE_LISTEN_URL, as `pool_max` says)
///  - DAGRON_DB_MIN_CONNECTIONS (default 0) — a floor of warm connections;
///    also acquired once at startup below, so a replica is never marked
///    Ready with a cold pool (market open must not pay TLS + auth
///    handshakes on the first N requests).
///  - DAGRON_DB_ACQUIRE_TIMEOUT_MS (default 10000) — the profile sets
///    ~250 so a saturated pool fails fast (503 + Retry-After via /readyz
///    flipping) instead of queueing requests for ten seconds.
///  - DAGRON_DB_TEST_BEFORE_ACQUIRE (default true) — sqlx's per-checkout
///    liveness round trip; the profile turns it off and lets the query
///    itself surface a dead connection.
#[cfg(feature = "postgres")]
pub async fn connect(database_url: &str) -> Result<Pool> {
    let db_max = pool_max(
        env_parse("DAGRON_DB_MAX_CONNECTIONS", 8),
        std::env::var("DATABASE_LISTEN_URL").ok().as_deref(),
    );
    let db_min: u32 = env_parse("DAGRON_DB_MIN_CONNECTIONS", 0).min(db_max);
    let db_acquire_ms: u64 = env_parse("DAGRON_DB_ACQUIRE_TIMEOUT_MS", 10_000).max(50);
    let db_test_before_acquire = std::env::var("DAGRON_DB_TEST_BEFORE_ACQUIRE")
        .map(|v| !matches!(v.trim().to_ascii_lowercase().as_str(), "false" | "0" | "no"))
        .unwrap_or(true);
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(db_max)
        .min_connections(db_min)
        .acquire_timeout(Duration::from_millis(db_acquire_ms))
        .test_before_acquire(db_test_before_acquire)
        .connect(database_url)
        .await
        .context("connecting to Postgres")?;
    info!(
        max = db_max,
        min = db_min,
        acquire_timeout_ms = db_acquire_ms,
        test_before_acquire = db_test_before_acquire,
        "db pool configured"
    );
    // Warm the floor eagerly: hold min_connections checkouts at once so the
    // pool actually opens them, then release. Best-effort — a partially warmed
    // pool is not a startup failure, and /readyz still gates on a live acquire.
    if db_min > 0 {
        let held: Vec<_> = futures::future::join_all((0..db_min).map(|_| pool.acquire())).await;
        let warmed = held.iter().filter(|c| c.is_ok()).count();
        info!(warmed, target = db_min, "db pool warmed");
    }
    Ok(pool)
}

/// The limits over this dagron-api's sessions, as it last read them
/// (`dagron_core::connections`): what `GET /api/metrics` answers as
/// `db_connections`, beside the counts. Per replica. A read that fails
/// leaves the last one, which goes on being answered until one succeeds.
#[cfg(feature = "postgres")]
#[derive(Default)]
pub struct ConnectionGauge {
    /// The sessions this process may hold at once: its pool, and one for its
    /// live events' LISTEN.
    holds: std::sync::atomic::AtomicU32,
    last: std::sync::Mutex<Option<dagron_core::connections::Limits>>,
}

#[cfg(feature = "postgres")]
impl ConnectionGauge {
    /// What was last read, with what this process may hold; `None` until a
    /// read has succeeded.
    pub fn snapshot(&self) -> Option<(u32, dagron_core::connections::Limits)> {
        let last = *self.last.lock().unwrap_or_else(|e| e.into_inner());
        last.map(|limits| {
            (
                self.holds.load(std::sync::atomic::Ordering::Relaxed),
                limits,
            )
        })
    }

    /// Keep `limits`, and say where `holds` is past one of them, or past
    /// half of one, unless the limits are the ones last kept.
    fn record(&self, holds: u32, limits: dagron_core::connections::Limits) -> Vec<String> {
        self.holds
            .store(holds, std::sync::atomic::Ordering::Relaxed);
        let key = |l: &dagron_core::connections::Limits| (l.database, l.login, l.server);
        let before = self
            .last
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .replace(limits);
        if before.as_ref().map(key) == Some(key(&limits)) {
            return Vec::new();
        }
        dagron_core::connections::warnings(
            &limits,
            holds,
            &format!(
                "dagron-api (its pool of {}, DAGRON_DB_MAX_CONNECTIONS, and a LISTEN session)",
                holds.saturating_sub(1)
            ),
        )
    }
}

/// Read the limits over this process's sessions now, and every few minutes
/// after, into `gauge`, saying where its pool and LISTEN are past one, or
/// past half of one. Never a stop: a failed read is said, and changes
/// nothing.
#[cfg(feature = "postgres")]
pub fn spawn_connection_check(pool: Pool, gauge: std::sync::Arc<ConnectionGauge>) {
    let holds = pool.options().get_max_connections() + 1;
    tokio::spawn(async move {
        loop {
            match dagron_core::connections::read(&pool).await {
                Ok(limits) => {
                    for line in gauge.record(holds, limits) {
                        tracing::warn!("{line}");
                    }
                }
                Err(e) => tracing::warn!(
                    error = %e,
                    "could not read the database's connection limits"
                ),
            }
            tokio::time::sleep(dagron_core::connections::CHECK_EVERY).await;
        }
    });
}

/// Open the SQLite file the engine runs on, as the engine opens it
/// (dagron-core's `init_pool`): WAL, a busy timeout, its migrations applied
/// (the open tried again past an engine opening or migrating it at once),
/// and one connection, so a transaction's read-then-write never loses its
/// lock to a sibling connection. `DATABASE_URL` is the file's path, or
/// `sqlite://<path>`.
#[cfg(feature = "sqlite")]
pub async fn connect(database_url: &str) -> Result<Pool> {
    let path = sqlite_path(database_url);
    let pool = dagron_core::db::init_pool(path)
        .await
        .with_context(|| format!("opening the SQLite datastore {path}"))?;
    info!(path, "db pool configured (SQLite, one node)");
    Ok(pool)
}

/// `sqlite://path`, `sqlite:path`, or the path itself.
#[cfg(feature = "sqlite")]
fn sqlite_path(url: &str) -> &str {
    url.strip_prefix("sqlite://")
        .or_else(|| url.strip_prefix("sqlite:"))
        .unwrap_or(url)
}

#[cfg(all(test, feature = "sqlite"))]
mod tests {
    #[test]
    fn a_sqlite_url_is_its_path() {
        assert_eq!(super::sqlite_path("sqlite:///var/lib/dagron/workflow.db"), "/var/lib/dagron/workflow.db");
        assert_eq!(super::sqlite_path("sqlite:workflow.db"), "workflow.db");
        assert_eq!(super::sqlite_path("/data/workflow.db"), "/data/workflow.db");
    }
}

#[cfg(all(test, feature = "postgres"))]
mod pg_tests {
    use super::pool_max;

    /// The listener shares the pool unless it has an endpoint of its own:
    /// sharing it, a pool of one would leave no connection for a request.
    #[test]
    fn a_pool_the_listener_shares_is_never_one() {
        assert_eq!(pool_max(8, None), 8);
        assert_eq!(pool_max(1, None), 2);
        assert_eq!(pool_max(0, None), 2);
        assert_eq!(pool_max(1, Some(" ")), 2);
        assert_eq!(pool_max(1, Some("postgres://db:5432/dagron")), 1);
        assert_eq!(pool_max(0, Some("postgres://db:5432/dagron")), 1);
    }
}

#[cfg(all(test, feature = "postgres"))]
mod connection_tests {
    use super::ConnectionGauge;
    use dagron_core::connections::Limits;

    fn limits(database: Option<i64>) -> Limits {
        Limits {
            database,
            login: None,
            server: 97,
            database_sessions: 5,
            login_sessions: 5,
            server_sessions: 12,
        }
    }

    /// The gauge keeps what was last read, and says where the pool
    /// and LISTEN are past a limit, or half of one, once for each set of
    /// limits.
    #[test]
    fn the_gauge_keeps_the_last_read_and_says_each_set_once() {
        let gauge = ConnectionGauge::default();
        assert_eq!(gauge.snapshot(), None, "nothing read yet");
        let said = gauge.record(9, limits(Some(16)));
        assert_eq!(said.len(), 1, "{said:?}");
        assert!(
            said[0].starts_with("dagron-api (its pool of 8"),
            "{}",
            said[0]
        );
        assert!(gauge.record(9, limits(Some(16))).is_empty(), "said once");
        assert!(gauge.record(9, limits(Some(40))).is_empty(), "within half");
        assert_eq!(gauge.snapshot(), Some((9, limits(Some(40)))));
        let answer = crate::routes::ops::DbConnections::new(9, &limits(None));
        assert_eq!(
            serde_json::to_value(&answer).unwrap(),
            serde_json::json!({
                "holds": 9,
                "database": { "limit": null, "sessions": 5 },
                "login": { "limit": null, "sessions": 5 },
                "server": { "limit": 97, "sessions": 12 },
            })
        );
    }

    /// Live, read-only: the check fills the gauge from the shared test
    /// database as it starts, its own sessions among those it counts.
    #[tokio::test]
    async fn the_check_reads_the_limits_as_it_starts() {
        let Ok(url) = std::env::var("TEST_DATABASE_URL") else {
            eprintln!("TEST_DATABASE_URL unset - skipping a live-datastore test");
            return;
        };
        let pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(2)
            .connect(&url)
            .await
            .unwrap();
        let gauge = std::sync::Arc::new(ConnectionGauge::default());
        super::spawn_connection_check(pool.clone(), std::sync::Arc::clone(&gauge));
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        let (holds, read) = loop {
            if let Some(read) = gauge.snapshot() {
                break read;
            }
            assert!(std::time::Instant::now() < deadline, "no read within 10s");
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        };
        assert_eq!(holds, 3, "a pool of 2 and a LISTEN");
        assert!(read.server > 0 && read.server_sessions >= 1, "{read:?}");
        assert!(
            read.database_sessions >= 1 && read.login_sessions >= 1,
            "{read:?}"
        );
        pool.close().await;
    }
}

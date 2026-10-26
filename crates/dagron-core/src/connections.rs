//! A process's connections against the limits Postgres holds it to.
//!
//! Three limits bound the sessions a process opens: its database's
//! `CONNECTION LIMIT` (`datconnlimit`), its login's (`rolconnlimit`), and the
//! server's `max_connections`, less the slots kept for superusers and, from
//! Postgres 16, `reserved_connections`. A session past any of them is refused
//! (`53300`), and the process waits on its pool, or fails what it was doing.
//! Nothing tells the process its limits, and a pool sized past them shows only
//! when the sessions run out, so the engine and dagron-api read them as they
//! start and every [`CHECK_EVERY`] after, with their sessions under each,
//! publish them, and say where what they may hold ([`warnings`]) is past one,
//! or past half of one: a rollout runs an old pod beside each new, each
//! holding as much. They never fail on it: the limits are an operator's, and
//! a process refused a session already says so.

use std::time::Duration;

/// How often a process reads its limits again: a cap raised by hand shows at
/// the next read, with no restart.
pub const CHECK_EVERY: Duration = Duration::from_secs(300);

/// The limits over one process's sessions, and the sessions under each, as
/// the server reported them. A limit of `None` is none (Postgres's `-1`).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Limits {
    /// `datconnlimit` of `current_database()`.
    pub database: Option<i64>,
    /// `rolconnlimit` of `current_user`.
    pub login: Option<i64>,
    /// `max_connections` less the reserved slots: what an ordinary login may
    /// open on the server.
    pub server: i64,
    /// Sessions on the database, every login's, with its autovacuum and
    /// parallel workers ([`READ`]).
    pub database_sessions: i64,
    /// Sessions of the login, on any database.
    pub login_sessions: i64,
    /// Client sessions on the server: every login's where its login has
    /// `pg_read_all_stats`, else its own ([`READ`]).
    pub server_sessions: i64,
}

impl Limits {
    /// Each scope: its name, its limit, and its sessions.
    pub fn scopes(&self) -> [(&'static str, Option<i64>, i64); 3] {
        [
            ("database", self.database, self.database_sessions),
            ("login", self.login, self.login_sessions),
            ("server", Some(self.server), self.server_sessions),
        ]
    }
}

/// What a process that may hold `holds` sessions at once (its pool, and one
/// for LISTEN where it listens) should be told of `limits`: a line for each
/// scope whose limit is below `holds`, where it will be refused sessions,
/// or below twice `holds`, where it will be while a rollout runs a second pod
/// beside it. `what` names the process and its pool's setting.
pub fn warnings(limits: &Limits, holds: u32, what: &str) -> Vec<String> {
    let holds = i64::from(holds);
    limits
        .scopes()
        .into_iter()
        .filter_map(|(scope, limit, sessions)| {
            let limit = limit?;
            let of = match scope {
                "database" => "its database's CONNECTION LIMIT",
                "login" => "its login's CONNECTION LIMIT",
                _ => "the server's max_connections, less its reserved connections,",
            };
            if holds > limit {
                Some(format!(
                    "{what} may hold {holds} sessions at once, past {of} of {limit} \
                     ({sessions} open now): it will be refused sessions. Raise the limit, \
                     or give it a smaller pool"
                ))
            } else if holds * 2 > limit {
                Some(format!(
                    "{what} may hold {holds} sessions at once, over half of {of} of {limit} \
                     ({sessions} open now): while a rollout runs a second pod beside this one, \
                     the two will be refused sessions. Raise the limit, or give it a smaller \
                     pool"
                ))
            } else {
                None
            }
        })
        .collect()
}

/// The statement that reads [`Limits`]: one round trip, readable by any
/// login. The sessions are those `pg_stat_activity` shows it. Any login sees
/// every session's database and login (`datname`, `usename`), so the
/// database and login counts are every login's; the database's also counts
/// its autovacuum and parallel workers, which its `CONNECTION LIMIT` does
/// not, so it may read a little high. A login without `pg_read_all_stats`
/// does not see another login's `backend_type`, so the server count, of
/// client backends, is its own sessions alone.
#[cfg(feature = "postgres")]
pub const READ: &str = "SELECT
    (SELECT datconnlimit FROM pg_database WHERE datname = current_database())::bigint
        AS database_limit,
    (SELECT rolconnlimit FROM pg_roles WHERE rolname = current_user)::bigint AS login_limit,
    (current_setting('max_connections')::bigint
        - current_setting('superuser_reserved_connections')::bigint
        - COALESCE(NULLIF(current_setting('reserved_connections', true), '')::bigint, 0))::bigint
        AS server_limit,
    (SELECT count(*) FROM pg_stat_activity WHERE datname = current_database())::bigint
        AS database_sessions,
    (SELECT count(*) FROM pg_stat_activity WHERE usename = current_user)::bigint
        AS login_sessions,
    (SELECT count(*) FROM pg_stat_activity WHERE backend_type = 'client backend')::bigint
        AS server_sessions";

/// [`Limits`], as the server `conn` is on reports them.
#[cfg(feature = "postgres")]
pub async fn read<'e>(conn: impl sqlx::PgExecutor<'e>) -> Result<Limits, sqlx::Error> {
    use sqlx::Row as _;
    let row = sqlx::query(READ).fetch_one(conn).await?;
    let limit = |v: Option<i64>| v.filter(|n| *n >= 0);
    Ok(Limits {
        database: limit(row.try_get("database_limit")?),
        login: limit(row.try_get("login_limit")?),
        server: row.try_get("server_limit")?,
        database_sessions: row.try_get("database_sessions")?,
        login_sessions: row.try_get("login_sessions")?,
        server_sessions: row.try_get("server_sessions")?,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn limits(database: Option<i64>, login: Option<i64>, server: i64) -> Limits {
        Limits {
            database,
            login,
            server,
            database_sessions: 3,
            login_sessions: 2,
            server_sessions: 9,
        }
    }

    /// Within half of every limit, or with no limit, nothing is said.
    #[test]
    fn a_pool_within_half_of_every_limit_is_not_said() {
        assert!(warnings(&limits(Some(40), Some(18), 97), 9, "the engine").is_empty());
        assert!(warnings(&limits(None, None, 97), 9, "the engine").is_empty());
        assert!(warnings(&limits(Some(18), None, 97), 9, "the engine").is_empty());
    }

    /// Past a limit, the process will be refused sessions; past half of one,
    /// it will be in a rollout. Each scope is said on its own.
    #[test]
    fn a_pool_past_a_limit_or_half_of_one_is_said() {
        let said = warnings(
            &limits(Some(16), Some(8), 97),
            9,
            "the engine (DB_MAX_CONNECTIONS 8)",
        );
        assert_eq!(said.len(), 2, "{said:?}");
        assert!(
            said[0].contains("over half of its database's CONNECTION LIMIT of 16 (3 open now)")
                && said[0].starts_with("the engine (DB_MAX_CONNECTIONS 8) may hold 9 sessions"),
            "{}",
            said[0]
        );
        assert!(
            said[1].contains("past its login's CONNECTION LIMIT of 8 (2 open now)")
                && said[1].contains("refused sessions"),
            "{}",
            said[1]
        );
        let said = warnings(&limits(None, None, 5), 9, "dagron-api");
        assert_eq!(said.len(), 1);
        assert!(
            said[0].contains("the server's max_connections"),
            "{}",
            said[0]
        );
    }
}

#[cfg(all(test, feature = "postgres"))]
mod live {
    use sqlx::postgres::PgConnectOptions;
    use sqlx::{Connection, PgConnection};

    /// The limits as the server has them, on a database and a login of the
    /// test's own, made with their caps and dropped after: the shared test
    /// database's own cap is never changed.
    #[tokio::test]
    async fn the_limits_are_read_as_the_server_has_them() {
        let Ok(admin_url) = std::env::var("TEST_DATABASE_URL") else {
            eprintln!("TEST_DATABASE_URL unset - skipping a live-datastore test");
            return;
        };
        let tag = uuid::Uuid::new_v4().simple().to_string();
        let name = format!("conn_limits_{tag}");
        let password = format!("pw{tag}");
        let mut admin = PgConnection::connect(&admin_url).await.unwrap();
        for ddl in [
            format!("CREATE DATABASE {name} CONNECTION LIMIT 7"),
            format!("CREATE ROLE {name} LOGIN PASSWORD '{password}' CONNECTION LIMIT 5"),
        ] {
            sqlx::query(&ddl).execute(&mut admin).await.unwrap();
        }
        let opts = admin_url
            .parse::<PgConnectOptions>()
            .unwrap()
            .database(&name)
            .username(&name)
            .password(&password);
        let read = async {
            let mut conn = PgConnection::connect_with(&opts).await?;
            let limits = super::read(&mut conn).await;
            conn.close().await?;
            limits
        }
        .await;
        for ddl in [
            format!("DROP DATABASE IF EXISTS {name} WITH (FORCE)"),
            format!("DROP ROLE IF EXISTS {name}"),
        ] {
            sqlx::query(&ddl).execute(&mut admin).await.unwrap();
        }
        admin.close().await.unwrap();
        let limits = read.unwrap();
        assert_eq!((limits.database, limits.login), (Some(7), Some(5)));
        // Its one session; the database's count may add an autovacuum or
        // parallel worker there.
        assert_eq!(limits.login_sessions, 1, "{limits:?}");
        assert!(limits.database_sessions >= 1, "{limits:?}");
        assert!(limits.server > 0, "{limits:?}");
        // Its own client sessions at least: another login's backend_type is
        // hidden from one without pg_read_all_stats.
        assert!(limits.server_sessions >= 1, "{limits:?}");
        assert!(super::warnings(&limits, 2, "a test").is_empty());
        assert_eq!(
            super::warnings(&limits, 3, "a test").len(),
            1,
            "over half of 5"
        );
        assert_eq!(
            super::warnings(&limits, 4, "a test").len(),
            2,
            "over half of 7 and of 5"
        );
    }
}

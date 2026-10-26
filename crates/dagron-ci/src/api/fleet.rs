//! The fleet's door: `GET /ci/v1/fleet/usage`, what fleet mode's runners did
//! over whole days, by repository, team or label ([`crate::fleet::usage`]).
//!
//! **Who.** An admin, as the repositories' door has it ([`super::repos`]):
//! the report is of every repository this dagron-ci serves.
//!
//! **What it is asked.** `since` and `until`, each a day (`YYYY-MM-DD`,
//! UTC), from the start of one to the start of the other, the thirty days to
//! the end of today unless said, and a year and a day at most; and `by`,
//! `repo` unless said, or `team` or `label`.

use axum::extract::{Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde::Deserialize;

use crate::db::Day;
use crate::fleet::usage::{self, By};
use crate::problem::problem;
use crate::server::AppState;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Asked {
    since: Option<String>,
    until: Option<String>,
    by: Option<String>,
}

/// `GET /ci/v1/fleet/usage`.
pub async fn usage(
    State(st): State<AppState>,
    headers: HeaderMap,
    Query(asked): Query<Asked>,
) -> Response {
    if let Err(response) = super::repos::admin(&st, &headers, false).await {
        return response;
    }
    let by = match asked.by.as_deref().map(By::parse) {
        None => By::Repository,
        Some(Some(by)) => by,
        Some(None) => {
            return problem(
                StatusCode::BAD_REQUEST,
                "no such report",
                "by is repo, team or label",
            )
        }
    };
    let (since, until) =
        match usage::window(asked.since.as_deref(), asked.until.as_deref(), Day::today()) {
            Ok(window) => window,
            Err(why) => return problem(StatusCode::BAD_REQUEST, "no such days", why),
        };
    match usage::usage(&st.db, since, until, by).await {
        Ok(report) => (StatusCode::OK, Json(report.json())).into_response(),
        Err(e) => super::database_error(e).1,
    }
}

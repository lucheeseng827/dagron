//! Live SSE streams (per-run and account-wide).
//!
//! Both endpoints subscribe a receiver to the shared broadcast channel; the
//! per-run stream emits only the events for the requested run, while the
//! account-wide stream forwards every event so list pages (Runs, Workflows,
//! Overview) can refresh on activity instead of polling. On broadcast lag
//! (client too slow) both emit a `resync` event so the client refetches rather
//! than silently missing a transition. Auth is the standard header bearer
//! (`AuthUser`) — the frontend uses fetch-event-source so it can send the
//! Authorization header (decision 01-02).

use std::convert::Infallible;

use axum::extract::{Path, State};
use axum::response::sse::{Event, KeepAlive, Sse};
use futures::Stream;
use tokio_stream::wrappers::BroadcastStream;
use tokio_stream::StreamExt;

use crate::auth::AuthUser;
use crate::state::AppState;

/// `GET /api/runs/:id/stream` — SSE of this run's task-state changes.
pub async fn stream_run(
    _auth: AuthUser,
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Sse<impl Stream<Item = Result<Event, Infallible>>> {
    let rx = state.tx.subscribe();

    let stream = BroadcastStream::new(rx).filter_map(move |item| match item {
        // Event for the run this client is watching → forward as JSON.
        Ok(ev) if ev.run_id == id => Some(Ok(event_json(&ev))),
        // Event for a different run → ignore.
        Ok(_) => None,
        // Client lagged behind the buffer → tell it to refetch (don't drop silently).
        Err(_lagged) => Some(Ok(Event::default().event("resync").data("lagged"))),
    });

    Sse::new(stream).keep_alive(KeepAlive::default())
}

/// `GET /api/events/stream` — SSE of task-state changes across *all* runs.
///
/// Feeds the list pages' live mode: each event carries the affected run_id and
/// the client coalesces bursts into a debounced list refetch, so idle sessions
/// cost nothing (no polling) and busy ones are bounded by the client throttle.
pub async fn stream_events(
    _auth: AuthUser,
    State(state): State<AppState>,
) -> Sse<impl Stream<Item = Result<Event, Infallible>>> {
    let rx = state.tx.subscribe();

    let stream = BroadcastStream::new(rx).map(|item| match item {
        Ok(ev) => Ok(event_json(&ev)),
        Err(_lagged) => Ok(Event::default().event("resync").data("lagged")),
    });

    Sse::new(stream).keep_alive(KeepAlive::default())
}

/// Serialize a TaskEvent to an SSE data event; fall back to an empty object on
/// the (practically impossible) serialization error so the stream never breaks.
fn event_json(ev: &crate::state::TaskEvent) -> Event {
    Event::default()
        .json_data(ev)
        .unwrap_or_else(|_| Event::default().data("{}"))
}

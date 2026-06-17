//! Per-run live SSE stream.
//!
//! Subscribes a receiver to the shared broadcast channel and emits only the
//! events for the requested run. On broadcast lag (client too slow) it emits a
//! `resync` event so the client refetches the full graph rather than silently
//! missing a transition. Auth is the standard header bearer (`AuthUser`) — the
//! frontend uses fetch-event-source so it can send the Authorization header
//! (decision 01-02).

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

/// Serialize a TaskEvent to an SSE data event; fall back to an empty object on
/// the (practically impossible) serialization error so the stream never breaks.
fn event_json(ev: &crate::state::TaskEvent) -> Event {
    Event::default()
        .json_data(ev)
        .unwrap_or_else(|_| Event::default().data("{}"))
}

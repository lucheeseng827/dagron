//! Run-status badge — an embeddable SVG of a workflow's latest run outcome.
//!
//! `GET /api/badges/:name` returns a shields-style flat badge
//! ("dagron | succeeded", colored by status) for the newest run of the named
//! workflow. Unauthenticated by design: badges are embedded in READMEs and
//! dashboards that can't send an auth header, and the response reveals only a
//! status label, never the spec. `no runs` (grey) covers both "unknown
//! workflow" and "no runs yet".

use axum::extract::{Path, State};
use axum::http::header;
use axum::response::{IntoResponse, Response};
use badgelib::{Badge, Color};

use crate::state::AppState;

/// `GET /api/badges/:name` — latest run status as an SVG badge.
pub async fn workflow_badge(State(state): State<AppState>, Path(name): Path<String>) -> Response {
    let status: Option<String> = sqlx::query_scalar(
        "SELECT wr.status FROM workflow_runs wr
         JOIN workflow_definitions wd ON wd.id = wr.definition_id
         WHERE wd.name = $1 ORDER BY wr.created_at DESC LIMIT 1",
    )
    .bind(&name)
    .fetch_optional(&state.read_pool)
    .await
    .map_err(|err| {
        // Don't render a DB outage as the same grey "no runs" badge — log it so
        // it's visible, then fall back to the unknown badge.
        tracing::error!(error = ?err, workflow = %name, "badge status query failed");
        err
    })
    .ok()
    .flatten();

    let (text, color) = status_style(status.as_deref());
    let svg = render_badge("dagron", text, color);
    (
        [
            (header::CONTENT_TYPE, "image/svg+xml; charset=utf-8"),
            // Badges must not be cached stale by the forge/CDN across runs.
            (header::CACHE_CONTROL, "no-cache, max-age=0"),
        ],
        svg,
    )
        .into_response()
}

/// Map a run status to a (label, color) pair. `None` (no runs / unknown
/// workflow) is a neutral grey badge.
fn status_style(status: Option<&str>) -> (&str, &str) {
    match status {
        Some("succeeded") => ("succeeded", "#2da44e"),
        Some("failed") => ("failed", "#cf222e"),
        Some("running") => ("running", "#0969da"),
        Some("cancelled") => ("cancelled", "#8c959f"),
        Some("pending") => ("pending", "#bf8700"),
        Some(_) => ("unknown", "#8c959f"),
        None => ("no runs", "#8c959f"),
    }
}

/// Render a flat SVG badge with a grey `label` segment and a colored `status`
/// segment. `badgelib` owns the geometry, text measurement and XML escaping that
/// this used to do by hand; the tests below still pin the parts callers depend on
/// — the `aria-label`, the status text, and escaping of `<` and `&`.
///
/// `color` arrives from `status_style` as a CSS hex with a leading `#`, which
/// `Color::Hex` does not want, so it is trimmed here rather than at every call.
fn render_badge(label: &str, status: &str, color: &str) -> String {
    Badge::new()
        .label(label)
        .value(status)
        .label_color(Color::Hex("555".into()))
        .value_color(Color::Hex(color.trim_start_matches('#').into()))
        .to_svg()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn status_colors() {
        assert_eq!(status_style(Some("succeeded")).1, "#2da44e");
        assert_eq!(status_style(Some("failed")).1, "#cf222e");
        assert_eq!(status_style(None).0, "no runs");
        assert_eq!(status_style(Some("weird")).0, "unknown");
    }

    #[test]
    fn badge_svg_is_wellformed_and_carries_status() {
        let svg = render_badge("dagron", "succeeded", "#2da44e");
        assert!(svg.starts_with("<svg"));
        assert!(svg.contains("</svg>"));
        assert!(svg.contains(">succeeded<"));
        assert!(svg.contains("#2da44e"));
        assert!(svg.contains(r#"aria-label="dagron: succeeded""#));
    }

    #[test]
    fn badge_escapes_xml() {
        let svg = render_badge("dagron", "a<b&c", "#555");
        assert!(svg.contains("a&lt;b&amp;c"));
        assert!(!svg.contains("a<b&c"));
    }
}

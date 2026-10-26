//! Errors as RFC 9457 problem documents (`application/problem+json`).

use axum::http::{header, StatusCode};
use axum::response::{IntoResponse, Response};

pub const CONTENT_TYPE: &str = "application/problem+json";

pub fn problem(status: StatusCode, title: &str, detail: impl Into<String>) -> Response {
    let body = serde_json::json!({
        "type": "about:blank",
        "title": title,
        "status": status.as_u16(),
        "detail": detail.into(),
    });
    (
        status,
        [(header::CONTENT_TYPE, CONTENT_TYPE)],
        body.to_string(),
    )
        .into_response()
}

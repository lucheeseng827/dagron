//! CloudEvents 1.0 for the webhooks dagron sends.
//!
//! An event goes out in one of the HTTP binding's two content modes:
//!
//! * **binary**: the body is the event's data, as it always was, and its
//!   attributes ride in `ce-*` headers. A receiver that knows nothing of
//!   CloudEvents reads the same body it read before; one that does reads an
//!   event. So binary is what every dagron webhook sends unless told otherwise.
//! * **structured**: the body is the whole event, `application/cloudevents+json`,
//!   with the data under `data`. A receiver built on a CloudEvents SDK takes it
//!   from the body alone, which a queue or a log keeps whole.
//!
//! The event's `type` is dagron's own name for what happened, under
//! [`TYPE_PREFIX`]: `run.completed` goes out as `dev.dagron.run.completed`. Its
//! `source` is the installation's ([`ENV_SOURCE`], `/dagron` by default), its
//! `subject` the run, and its `time` when the occurrence was recorded, not when
//! it was sent, so a retried event keeps its time as it keeps its `id`.
//!
//! Spec: <https://github.com/cloudevents/spec/blob/v1.0.2/cloudevents/spec.md>,
//! and its HTTP binding: <https://github.com/cloudevents/spec/blob/v1.0.2/cloudevents/bindings/http-protocol-binding.md>.

use anyhow::{bail, Result};
use serde_json::{json, Value};

/// The CloudEvents version every event dagron sends is.
pub const SPECVERSION: &str = "1.0";
/// Put before dagron's own name for what happened to give an event its `type`:
/// reverse DNS of the project's domain, as the spec asks.
pub const TYPE_PREFIX: &str = "dev.dagron.";
/// The installation's `source`, a URI reference: what a receiver that hears
/// from more than one dagron tells them apart by. `/dagron` when unset.
pub const ENV_SOURCE: &str = "DAGRON_EVENTS_SOURCE";
/// What a structured-mode body is.
pub const STRUCTURED_CONTENT_TYPE: &str = "application/cloudevents+json; charset=utf-8";

/// Which of the HTTP binding's content modes a sender uses.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Mode {
    /// The data is the body; the attributes are `ce-*` headers.
    #[default]
    Binary,
    /// The body is the whole event.
    Structured,
}

impl Mode {
    /// `binary` or `structured`; empty is binary.
    pub fn parse(s: &str) -> Result<Self> {
        match s.trim() {
            "" | "binary" => Ok(Self::Binary),
            "structured" => Ok(Self::Structured),
            other => bail!("`{other}` is no CloudEvents content mode: binary or structured"),
        }
    }
}

/// The installation's `source`: [`ENV_SOURCE`], or `/dagron`.
pub fn source_from_env() -> String {
    std::env::var(ENV_SOURCE)
        .ok()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "/dagron".to_string())
}

/// One event's attributes. Its data is the sender's, a JSON body.
#[derive(Debug, Clone)]
pub struct Event {
    /// Unique within `source`, and the same each time the event is sent again,
    /// so a receiver can drop a repeat.
    pub id: String,
    pub source: String,
    /// dagron's own name for what happened (`run.completed`), without the
    /// prefix.
    pub kind: String,
    /// What the event is about: a run's id.
    pub subject: Option<String>,
    /// When it happened, RFC 3339.
    pub time: Option<String>,
}

impl Event {
    /// The event's `type`: [`TYPE_PREFIX`] and its kind.
    pub fn event_type(&self) -> String {
        format!("{TYPE_PREFIX}{}", self.kind)
    }

    /// The binary mode's headers, the body's content type among them (the
    /// event's `datacontenttype`, which the binding carries as `Content-Type`).
    pub fn binary_headers(&self) -> Vec<(&'static str, String)> {
        let mut headers = vec![
            ("content-type", "application/json".to_string()),
            ("ce-specversion", SPECVERSION.to_string()),
            ("ce-id", header_value(&self.id)),
            ("ce-source", header_value(&self.source)),
            ("ce-type", header_value(&self.event_type())),
        ];
        if let Some(subject) = &self.subject {
            headers.push(("ce-subject", header_value(subject)));
        }
        if let Some(time) = &self.time {
            headers.push(("ce-time", header_value(time)));
        }
        headers
    }

    /// The structured mode's body: the event, its data under `data`. `data` is
    /// a JSON text; one that does not parse goes as a string, so nothing a
    /// sender gives is lost.
    pub fn structured(&self, data: &str) -> String {
        let data: Value =
            serde_json::from_str(data).unwrap_or_else(|_| Value::String(data.to_string()));
        let mut event = json!({
            "specversion": SPECVERSION,
            "id": self.id,
            "source": self.source,
            "type": self.event_type(),
            "datacontenttype": "application/json",
            "data": data,
        });
        if let Some(subject) = &self.subject {
            event["subject"] = json!(subject);
        }
        if let Some(time) = &self.time {
            event["time"] = json!(time);
        }
        event.to_string()
    }

    /// What goes out in `mode`: the headers and the body for `data`.
    pub fn encode(&self, mode: Mode, data: &str) -> (Vec<(&'static str, String)>, String) {
        match mode {
            Mode::Binary => (self.binary_headers(), data.to_string()),
            Mode::Structured => (
                vec![("content-type", STRUCTURED_CONTENT_TYPE.to_string())],
                self.structured(data),
            ),
        }
    }
}

/// A `ce-*` header's value, percent-encoded as the binding asks: a space, a
/// double quote, a percent sign and anything outside printable ASCII, each by
/// its UTF-8 bytes.
fn header_value(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        if (0x21..=0x7e).contains(&b) && b != b'"' && b != b'%' {
            out.push(b as char);
        } else {
            out.push_str(&format!("%{b:02X}"));
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn event() -> Event {
        Event {
            id: "ev-1".into(),
            source: "/dagron".into(),
            kind: "run.completed".into(),
            subject: Some("run-42".into()),
            time: Some("2026-10-19T10:00:00+00:00".into()),
        }
    }

    fn header<'a>(headers: &'a [(&'static str, String)], name: &str) -> Option<&'a str> {
        headers
            .iter()
            .find(|(k, _)| *k == name)
            .map(|(_, v)| v.as_str())
    }

    #[test]
    fn binary_mode_keeps_the_body_and_puts_the_attributes_in_headers() {
        let (headers, body) = event().encode(Mode::Binary, r#"{"run_id":"run-42"}"#);
        assert_eq!(
            body, r#"{"run_id":"run-42"}"#,
            "the body is the data, unchanged"
        );
        assert_eq!(header(&headers, "ce-specversion"), Some("1.0"));
        assert_eq!(header(&headers, "ce-id"), Some("ev-1"));
        assert_eq!(header(&headers, "ce-source"), Some("/dagron"));
        assert_eq!(
            header(&headers, "ce-type"),
            Some("dev.dagron.run.completed")
        );
        assert_eq!(header(&headers, "ce-subject"), Some("run-42"));
        assert_eq!(
            header(&headers, "ce-time"),
            Some("2026-10-19T10:00:00+00:00")
        );
        assert_eq!(header(&headers, "content-type"), Some("application/json"));
    }

    #[test]
    fn optional_attributes_are_left_out_when_absent() {
        let ev = Event {
            subject: None,
            time: None,
            ..event()
        };
        let headers = ev.binary_headers();
        assert!(header(&headers, "ce-subject").is_none());
        assert!(header(&headers, "ce-time").is_none());
        let body: Value = serde_json::from_str(&ev.structured("{}")).unwrap();
        assert!(body.get("subject").is_none() && body.get("time").is_none());
    }

    #[test]
    fn structured_mode_carries_the_whole_event() {
        let (headers, body) = event().encode(Mode::Structured, r#"{"run_id":"run-42"}"#);
        assert_eq!(
            header(&headers, "content-type"),
            Some("application/cloudevents+json; charset=utf-8")
        );
        assert!(
            header(&headers, "ce-id").is_none(),
            "no attribute headers in structured mode"
        );
        let ev: Value = serde_json::from_str(&body).unwrap();
        assert_eq!(ev["specversion"], "1.0");
        assert_eq!(ev["id"], "ev-1");
        assert_eq!(ev["type"], "dev.dagron.run.completed");
        assert_eq!(ev["subject"], "run-42");
        assert_eq!(ev["datacontenttype"], "application/json");
        assert_eq!(
            ev["data"]["run_id"], "run-42",
            "the data is JSON, not a string of it"
        );
    }

    #[test]
    fn data_that_is_not_json_goes_as_a_string() {
        let ev: Value = serde_json::from_str(&event().structured("not json")).unwrap();
        assert_eq!(ev["data"], "not json");
    }

    #[test]
    fn header_values_are_percent_encoded_as_the_binding_asks() {
        assert_eq!(header_value("plain-ASCII_1.0/x:y"), "plain-ASCII_1.0/x:y");
        assert_eq!(header_value("a b"), "a%20b");
        assert_eq!(header_value("say \"hi\""), "say%20%22hi%22");
        assert_eq!(header_value("100%"), "100%25");
        assert_eq!(header_value("é"), "%C3%A9", "non-ASCII by its UTF-8 bytes");
        assert_eq!(header_value("a\nb"), "a%0Ab");
    }

    #[test]
    fn content_modes_parse() {
        assert_eq!(Mode::parse("").unwrap(), Mode::Binary);
        assert_eq!(Mode::parse("binary").unwrap(), Mode::Binary);
        assert_eq!(Mode::parse(" structured ").unwrap(), Mode::Structured);
        assert!(Mode::parse("batched").is_err());
    }

    #[test]
    fn the_source_defaults_and_is_set_by_the_environment() {
        let _env = crate::env_lock();
        std::env::remove_var(ENV_SOURCE);
        assert_eq!(source_from_env(), "/dagron");
        std::env::set_var(ENV_SOURCE, " https://ci.example.com/dagron ");
        assert_eq!(source_from_env(), "https://ci.example.com/dagron");
        std::env::set_var(ENV_SOURCE, "  ");
        assert_eq!(source_from_env(), "/dagron");
        std::env::remove_var(ENV_SOURCE);
    }
}

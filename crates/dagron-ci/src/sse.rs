//! Server-sent events read as they come: a followed log, whose
//! frames arrive in whatever pieces the connection gives them.
//!
//! The format is the HTML standard's: lines of `field: value`, a blank line
//! ending each event; `event` names it, `data` lines make its data (joined by
//! newlines), `id` is its id, and a line beginning `:` is a comment (a
//! keep-alive). An event with no data is no event.

/// One event.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Frame {
    pub(crate) event: String,
    pub(crate) data: String,
    pub(crate) id: Option<String>,
}

/// What has come of a stream so far, not yet an event.
#[derive(Debug, Default)]
pub(crate) struct Parser {
    buf: Vec<u8>,
    event: String,
    data: String,
    id: Option<String>,
}

/// The longest line an event of ours has: a frame's data is a step of a log
/// (a MiB), as JSON.
const MAX_LINE: usize = 16 << 20;

impl Parser {
    /// The events `bytes` finish. `Err` where a line is longer than any event
    /// of ours has: what is sent is not a log stream.
    pub(crate) fn push(&mut self, bytes: &[u8]) -> Result<Vec<Frame>, String> {
        self.buf.extend_from_slice(bytes);
        let mut out = Vec::new();
        let mut start = 0;
        while let Some(at) = self.buf[start..].iter().position(|b| *b == b'\n') {
            let end = start + at;
            let mut line = &self.buf[start..end];
            if line.last() == Some(&b'\r') {
                line = &line[..line.len() - 1];
            }
            let line = String::from_utf8_lossy(line).into_owned();
            start = end + 1;
            if line.is_empty() {
                if !self.data.is_empty() {
                    self.data.pop();
                    out.push(Frame {
                        event: match std::mem::take(&mut self.event) {
                            e if e.is_empty() => "message".to_string(),
                            e => e,
                        },
                        data: std::mem::take(&mut self.data),
                        id: self.id.clone(),
                    });
                }
                self.event.clear();
                continue;
            }
            if line.starts_with(':') {
                continue;
            }
            let (field, value) = match line.split_once(':') {
                Some((field, value)) => (field, value.strip_prefix(' ').unwrap_or(value)),
                None => (line.as_str(), ""),
            };
            match field {
                "event" => self.event = value.to_string(),
                "data" => {
                    self.data.push_str(value);
                    self.data.push('\n');
                }
                "id" => self.id = Some(value.to_string()),
                _ => {}
            }
        }
        self.buf.drain(..start);
        if self.buf.len() > MAX_LINE {
            return Err("a line longer than any event of a log stream".to_string());
        }
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn events_are_read_from_whatever_pieces_they_come_in() {
        let stream = "event: start\ndata: {\"a\":1}\n\n: keep-alive\n\nevent: log\r\nid: t:1:0:5\r\ndata: one\ndata: two\n\ndata: plain\n\n";
        let whole = Parser::default().push(stream.as_bytes()).unwrap();
        let mut pieces = Parser::default();
        let mut each = Vec::new();
        for byte in stream.as_bytes() {
            each.extend(pieces.push(&[*byte]).unwrap());
        }
        assert_eq!(whole, each);
        assert_eq!(
            whole,
            [
                Frame {
                    event: "start".into(),
                    data: "{\"a\":1}".into(),
                    id: None
                },
                Frame {
                    event: "log".into(),
                    data: "one\ntwo".into(),
                    id: Some("t:1:0:5".into())
                },
                Frame {
                    event: "message".into(),
                    data: "plain".into(),
                    id: Some("t:1:0:5".into())
                },
            ]
        );
    }
}

//! dagron-api's runner protocol, as a runner calls it (`/runner/v1`).
//!
//! The runner dials out, and nothing dials it: it works behind NAT, with no
//! port open. Each call says what came of it in the terms the run loop acts
//! on ([`CallError`]): a session that is gone, a word that came too late, a
//! credential that is refused, or a failure that is worth trying again.

use std::time::Duration;

use anyhow::Context;
use dagron_core::runner::{
    Claimed, Heartbeat, HeartbeatAck, Held, LogSegment, OpenSession, Queue, RegisterRequest,
    Registered, Report, Session,
};
use serde::de::DeserializeOwned;
use serde::Serialize;

/// What came of a call that did not succeed.
#[derive(Debug)]
pub enum CallError {
    /// 410: the session has expired or was closed. Its tasks are, or are about
    /// to be, taken back: stop them and open another.
    Gone,
    /// 409: the task is not the session's under that fence any more.
    Conflict(String),
    /// 426: the two ends share no protocol, or this runner is older than
    /// dagron-api takes.
    Upgrade(String),
    /// 401: the credential is refused (revoked), or the join token is.
    Unauthorized(String),
    /// Any other answer, with what dagron-api said of it.
    Status(u16, String),
    /// No answer: the network, a timeout, a body that could not be read.
    Transport(String),
}

impl std::fmt::Display for CallError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CallError::Gone => f.write_str("the session is gone"),
            CallError::Conflict(d) => write!(f, "conflict: {d}"),
            CallError::Upgrade(d) => write!(f, "upgrade required: {d}"),
            CallError::Unauthorized(d) => write!(f, "unauthorized: {d}"),
            CallError::Status(s, d) => write!(f, "dagron-api answered {s}: {d}"),
            CallError::Transport(d) => write!(f, "dagron-api could not be reached: {d}"),
        }
    }
}

impl std::error::Error for CallError {}

impl CallError {
    /// Whether asking again later may go otherwise: no answer, or one that
    /// says dagron-api could not answer now.
    pub fn is_transient(&self) -> bool {
        match self {
            CallError::Transport(_) => true,
            CallError::Status(s, _) => *s >= 500 || *s == 429 || *s == 408,
            _ => false,
        }
    }
}

/// How long a call may take, past a claim's own wait.
const CALL_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Clone)]
pub struct Client {
    http: reqwest::Client,
    base: String,
    token: Option<String>,
}

impl Client {
    /// A client of dagron-api at `base` (checked by [`crate::config::check_url`]).
    pub fn new(base: &str) -> anyhow::Result<Client> {
        let http = reqwest::Client::builder()
            .user_agent(format!("dagron-runner/{}", dagron_logging::VERSION))
            .connect_timeout(Duration::from_secs(10))
            .pool_idle_timeout(Duration::from_secs(90))
            .build()
            .context("building the HTTP client")?;
        Ok(Client {
            http,
            base: base.trim_end_matches('/').to_string(),
            token: None,
        })
    }

    /// The same client, presenting the runner's credential.
    pub fn with_token(mut self, token: impl Into<String>) -> Client {
        self.token = Some(token.into());
        self
    }

    /// The path under the base, joined as text: a base with a path (an
    /// ingress's prefix) keeps it.
    fn url(&self, path: &str) -> String {
        format!("{}{path}", self.base)
    }

    async fn call<B: Serialize, T: DeserializeOwned>(
        &self,
        method: reqwest::Method,
        path: &str,
        body: Option<&B>,
        timeout: Duration,
    ) -> Result<(u16, Option<T>), CallError> {
        let mut req = self.http.request(method, self.url(path)).timeout(timeout);
        if let Some(token) = &self.token {
            req = req.bearer_auth(token);
        }
        if let Some(body) = body {
            req = req.json(body);
        }
        let resp = req
            .send()
            .await
            .map_err(|e| CallError::Transport(e.to_string()))?;
        let status = resp.status().as_u16();
        let bytes = resp
            .bytes()
            .await
            .map_err(|e| CallError::Transport(e.to_string()))?;
        if (200..300).contains(&status) {
            if bytes.is_empty() {
                return Ok((status, None));
            }
            return serde_json::from_slice(&bytes)
                .map(|v| (status, Some(v)))
                .map_err(|e| {
                    CallError::Transport(format!("an answer that could not be read: {e}"))
                });
        }
        // A problem document's `detail`, or the body as it is.
        let detail = serde_json::from_slice::<serde_json::Value>(&bytes)
            .ok()
            .and_then(|v| v.get("detail").and_then(|d| d.as_str()).map(str::to_string))
            .unwrap_or_else(|| String::from_utf8_lossy(&bytes).chars().take(512).collect());
        Err(match status {
            401 => CallError::Unauthorized(detail),
            409 => CallError::Conflict(detail),
            410 => CallError::Gone,
            426 => CallError::Upgrade(detail),
            _ => CallError::Status(status, detail),
        })
    }

    async fn post<B: Serialize, T: DeserializeOwned>(
        &self,
        path: &str,
        body: &B,
        timeout: Duration,
    ) -> Result<T, CallError> {
        match self
            .call(reqwest::Method::POST, path, Some(body), timeout)
            .await?
        {
            (_, Some(v)) => Ok(v),
            (status, None) => Err(CallError::Status(status, "an empty answer".into())),
        }
    }

    /// `GET /runner/v1`: the protocols dagron-api speaks.
    pub async fn about(&self) -> Result<serde_json::Value, CallError> {
        match self
            .call::<(), serde_json::Value>(reqwest::Method::GET, "/runner/v1", None, CALL_TIMEOUT)
            .await?
        {
            (_, Some(v)) => Ok(v),
            (status, None) => Err(CallError::Status(status, "an empty answer".into())),
        }
    }

    pub async fn register(&self, req: &RegisterRequest) -> Result<Registered, CallError> {
        self.post("/runner/v1/register", req, CALL_TIMEOUT).await
    }

    pub async fn open_session(&self, req: &OpenSession) -> Result<Session, CallError> {
        self.post("/runner/v1/sessions", req, CALL_TIMEOUT).await
    }

    /// Long-polls up to `wait` seconds for tasks.
    pub async fn claim(&self, session_id: &str, wait: u64) -> Result<Claimed, CallError> {
        self.post(
            &format!("/runner/v1/sessions/{session_id}/claim?wait={wait}"),
            &serde_json::json!({}),
            CALL_TIMEOUT + Duration::from_secs(wait),
        )
        .await
    }

    pub async fn heartbeat(
        &self,
        session_id: &str,
        running: Vec<Held>,
    ) -> Result<HeartbeatAck, CallError> {
        self.post(
            &format!("/runner/v1/sessions/{session_id}/heartbeat"),
            &Heartbeat { running },
            CALL_TIMEOUT,
        )
        .await
    }

    /// `DELETE /runner/v1/sessions/{id}`: whether the session has ended
    /// (`true`), or drains while what it runs is over.
    pub async fn close_session(&self, session_id: &str) -> Result<bool, CallError> {
        let (_, body) = self
            .call::<(), serde_json::Value>(
                reqwest::Method::DELETE,
                &format!("/runner/v1/sessions/{session_id}"),
                None,
                CALL_TIMEOUT,
            )
            .await?;
        Ok(body
            .and_then(|b| b.get("ended").and_then(|e| e.as_bool()))
            .unwrap_or(false))
    }

    pub async fn log(
        &self,
        task_id: &str,
        seq: u64,
        segment: &LogSegment,
    ) -> Result<(), CallError> {
        self.call::<_, serde_json::Value>(
            reqwest::Method::POST,
            &format!("/runner/v1/tasks/{task_id}/logs/{seq}"),
            Some(segment),
            CALL_TIMEOUT,
        )
        .await
        .map(|_| ())
    }

    pub async fn result(&self, task_id: &str, report: &Report) -> Result<(), CallError> {
        self.call::<_, serde_json::Value>(
            reqwest::Method::POST,
            &format!("/runner/v1/tasks/{task_id}/result"),
            Some(report),
            CALL_TIMEOUT,
        )
        .await
        .map(|_| ())
    }

    /// `GET /runner/v1/queue`: the work there is for `labels` (all the
    /// runner serves, where empty).
    pub async fn queue(&self, labels: &[String]) -> Result<Queue, CallError> {
        let path = if labels.is_empty() {
            "/runner/v1/queue".to_string()
        } else {
            format!("/runner/v1/queue?labels={}", labels.join(","))
        };
        match self
            .call::<(), Queue>(reqwest::Method::GET, &path, None, CALL_TIMEOUT)
            .await?
        {
            (_, Some(q)) => Ok(q),
            (status, None) => Err(CallError::Status(status, "an empty answer".into())),
        }
    }
}

/// Wait before trying again: from a second, doubling to a minute.
pub struct Backoff {
    next: Duration,
}

impl Default for Backoff {
    fn default() -> Self {
        Backoff {
            next: Duration::from_secs(1),
        }
    }
}

impl Backoff {
    pub async fn wait(&mut self) {
        tokio::time::sleep(self.next).await;
        self.next = (self.next * 2).min(Duration::from_secs(60));
    }

    pub fn reset(&mut self) {
        self.next = Duration::from_secs(1);
    }
}

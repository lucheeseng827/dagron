//! MQTT ingestion source (`SOURCE=mqtt`, feature `mqtt`).
//!
//! Subscribe to a topic on a plant, gateway or robot broker and turn each
//! message (a workflow spec, YAML or JSON) into a run. This is the open
//! protocol adapter for the physical edge — MQTT is the industrial default
//! (Sparkplug B rides on it), and "a run starts when the PLC, the gateway or
//! the robot says so" is the on-ramp a controls engineer installs without
//! asking anyone. It has the shape of the managed broker connectors: one file
//! behind the [`WorkflowSource`] trait, nothing changes in the engine.
//!
//! ```text
//! PLC / gateway ──publish──▶ broker ──subscribe──▶ MqttSource ──recv──▶ IngestActor ──▶ run
//!                                       ▲               │
//!                                    PUBACK ◀────── ack ┘   (manual acks: only after the
//!                                                            run is durably created)
//! ```
//!
//! **Delivery semantics.** Manual acknowledgements, at-least-once by default:
//! the PUBACK for a QoS 1 message (PUBREC for QoS 2) goes out only after the
//! ingest actor has durably created the run — or parked the message as a dead
//! letter — so a crash between receive and commit leaves the message unacked
//! and the broker redelivers it when the session resumes
//! (`MQTT_CLEAN_SESSION=false`, the default, needs a stable `MQTT_CLIENT_ID`
//! for that to span a restart). A `nack` (a transient `create_run` failure, a
//! workflow at its `max_active_runs` cap) redelivers the same message
//! in-process on the next `recv` — nothing else would, because a broker only
//! redelivers on reconnect. `recv` never returns `Ok(None)`: a subscription is
//! never exhausted, and a broker that is down or drops the connection is a
//! `recv` *error* that the ingest actor retries after its 1 s backoff (rumqttc
//! reconnects on the next poll and this source re-subscribes on every
//! CONNACK) — never a reason to stop ingesting.
//!
//! **Exactly-once (opt-in).** `MQTT_POSITION_FIELD` names a top-level field of
//! the payload carrying a monotonic per-topic id — Sparkplug's `seq`, a
//! CloudEvents `id`, a gateway's own counter. The value becomes the message's
//! [`PendingPosition`] (substream = the topic), which the ingest actor commits
//! in the same datastore transaction as the run (`source_offsets`, keyed
//! `mqtt/<topic>`). On receipt the source compares the id with the committed
//! one for that topic — numerically when both parse as `u64` (`<=` is a
//! duplicate), else by string equality — and a duplicate is acked and skipped
//! without becoming a run. That closes the crash-redelivery window MQTT itself
//! leaves open. Sparkplug's `seq` wraps at 256, so pair it with a
//! session-scoped topic or use a non-wrapping id.
//!
//! **Dead letters.** A poison message becomes a durable `dead_letters` row
//! through the ingest actor and, with `MQTT_DLQ_TOPIC` set, is mirrored to
//! that topic as `{"error", "topic", "payload"}` (QoS 1) so broker-side
//! tooling sees it too. The DLQ topic must lie outside the subscription
//! filter: a configuration that would consume its own dead letters is refused
//! at startup instead of re-parking them forever.
//!
//! **Backpressure.** The ingest actor pulls one message at a time and stops
//! calling `recv` while the engine sits at `MAX_INFLIGHT_RUNS`; the broker's
//! queue absorbs the burst. While throttled the event loop is not polled, so a
//! pause longer than the keepalive lets the broker drop the session; the next
//! poll reconnects, resumes it, and unacked messages are redelivered —
//! at-least-once doing its job, and the position field (above) making it
//! invisible.
//!
//! **TLS.** `mqtts://` uses rustls with the platform root store. The crate is
//! built with rumqttc's `use-rustls-no-provider` and installs the `ring`
//! provider itself, so the process carries exactly one provider (the one
//! reqwest already links) and never the aws-lc one. Client certificates are
//! not wired; a broker that requires them is fronted by a local bridge today.
//!
//! | Env var | Default | Meaning |
//! |---|---|---|
//! | `MQTT_URL` | `mqtt://127.0.0.1:1883` | broker endpoint; `mqtts://host[:8883]` for TLS |
//! | `MQTT_TOPIC` | `dagron/workflows` | subscription filter (`+` / trailing `#` allowed) |
//! | `MQTT_CLIENT_ID` | `dagron-<uuid>` | a stable id resumes the broker session across restarts |
//! | `MQTT_QOS` | `1` | `0` fire-and-forget, `1` at-least-once, `2` exactly-once handshake |
//! | `MQTT_USERNAME` / `MQTT_PASSWORD` | — | broker credentials |
//! | `MQTT_CLEAN_SESSION` | `false` | `true` discards broker-side session state on connect |
//! | `MQTT_DLQ_TOPIC` | — | dead-letter mirror topic; must not match `MQTT_TOPIC` |
//! | `MQTT_KEEPALIVE_SECS` | `30` | ping interval; `0` disables pings |
//! | `MQTT_POSITION_FIELD` | — | top-level payload field with a monotonic id (exactly-once) |

use std::collections::{HashMap, HashSet};
use std::time::Duration;

use anyhow::{anyhow, bail, Context, Result};
use async_trait::async_trait;
use rumqttc::{
    AsyncClient, Event, EventLoop, MqttOptions, Packet, Publish, QoS, SubscribeReasonCode,
};

use crate::source::{AckHandle, PendingPosition, WorkflowMessage, WorkflowSource};

/// Default subscription filter.
pub const DEFAULT_TOPIC: &str = "dagron/workflows";
/// Default broker endpoint.
pub const DEFAULT_URL: &str = "mqtt://127.0.0.1:1883";

/// Largest inbound packet accepted. rumqttc's default is 10 KiB, which is
/// smaller than a real workflow spec with a few templated tasks; 1 MiB is the
/// same ceiling the fleet uplink uses for a batch.
const MAX_INCOMING_BYTES: usize = 1024 * 1024;
/// Outbound ceiling: twice the inbound one. That is *not* enough for a verbatim
/// mirror of the largest accepted message — lossy UTF-8 conversion can treble a
/// binary payload and JSON escaping of control bytes can sextuple it — so
/// [`MqttSource::dead_letter`] truncates the mirrored payload to
/// [`MAX_DLQ_PAYLOAD_BYTES`] instead of letting an oversize packet be encoded.
/// An outgoing packet over this ceiling is not a rejected publish: rumqttc fails
/// to encode it, tears the connection down, and discards the queued acks with
/// it, so the poison message is redelivered and the source makes no progress
/// ever again.
const MAX_OUTGOING_BYTES: usize = 2 * MAX_INCOMING_BYTES;
/// Payload bytes mirrored into a dead-letter envelope. Escaping is worst-case
/// 6× (`\u00XX` per control byte), so this bound holds the encoded envelope
/// inside [`MAX_OUTGOING_BYTES`] with room for the error and topic fields.
const MAX_DLQ_PAYLOAD_BYTES: usize = 256 * 1024;
/// Error bytes mirrored into a dead-letter envelope. The text comes from the
/// ingest actor (a parse-error chain can be arbitrarily long), so it is bounded
/// for the same reason the payload is: an envelope over [`MAX_OUTGOING_BYTES`]
/// is not a rejected publish, it is a torn-down connection and a stalled source.
const MAX_DLQ_ERROR_BYTES: usize = 8 * 1024;

/// Largest prefix of `s` that is at most `max` bytes and ends on a char
/// boundary, so a truncated mirror is still valid UTF-8.
fn truncate_on_boundary(s: &str, max: usize) -> &str {
    let mut end = s.len().min(max);
    while end > 0 && !s.is_char_boundary(end) {
        end -= 1;
    }
    &s[..end]
}
/// Capacity of the client → event-loop request channel. Acks, the one
/// subscribe and the odd dead-letter publish are all that travel on it.
const REQUEST_CAP: usize = 16;

// ── Config ────────────────────────────────────────────────────────────────────

/// Where `MQTT_URL` points, split the way [`MqttOptions::new`] wants it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Endpoint {
    pub host: String,
    pub port: u16,
    pub tls: bool,
}

/// Parse `mqtt://host[:port]` / `mqtts://host[:port]` by hand. rumqttc's own
/// `parse_url` needs its `url` feature, which is not enabled — and this accepts
/// exactly the shape the docs promise, no more: credentials belong in
/// `MQTT_USERNAME` / `MQTT_PASSWORD` (a password inside `MQTT_URL` would land
/// in every "scheduler starting" log line), and MQTT has no path or query.
pub fn parse_url(url: &str) -> Result<Endpoint> {
    let url = url.trim();
    let (scheme, rest) = url.split_once("://").ok_or_else(|| {
        anyhow!("MQTT_URL '{url}' has no scheme — expected mqtt://host[:port] or mqtts://host[:port]")
    })?;
    let tls = match scheme.to_ascii_lowercase().as_str() {
        "mqtt" | "tcp" => false,
        "mqtts" | "ssl" => true,
        other => bail!(
            "MQTT_URL scheme '{other}' is not supported — use mqtt:// (plain TCP) or \
             mqtts:// (TLS); websocket transports are not built in"
        ),
    };
    let authority = rest.split(['/', '?', '#']).next().unwrap_or_default();
    let tail = &rest[authority.len()..];
    if !tail.is_empty() && tail != "/" {
        bail!(
            "MQTT_URL '{url}' carries a path or query ('{tail}') — MQTT has neither; \
             the topic is MQTT_TOPIC"
        );
    }
    if authority.contains('@') {
        bail!(
            "MQTT_URL '{url}' embeds credentials — use MQTT_USERNAME / MQTT_PASSWORD so \
             the password never reaches a log line"
        );
    }
    let (host, port) = if let Some(bracketed) = authority.strip_prefix('[') {
        // IPv6 literal: `[::1]:1883`.
        let (host, after) = bracketed
            .split_once(']')
            .ok_or_else(|| anyhow!("MQTT_URL '{url}': unterminated IPv6 literal"))?;
        let port = match after {
            "" => None,
            p => Some(p.strip_prefix(':').ok_or_else(|| {
                anyhow!("MQTT_URL '{url}': expected ':port' after the IPv6 literal, got '{p}'")
            })?),
        };
        (host.to_string(), port)
    } else {
        match authority.rsplit_once(':') {
            Some((h, p)) => (h.to_string(), Some(p)),
            None => (authority.to_string(), None),
        }
    };
    if host.is_empty() {
        bail!("MQTT_URL '{url}' has no host");
    }
    let port = match port {
        Some(p) => p
            .parse::<u16>()
            .ok()
            .filter(|p| *p != 0)
            .ok_or_else(|| anyhow!("MQTT_URL '{url}': port '{p}' is not a number in 1..=65535"))?,
        None if tls => 8883,
        None => 1883,
    };
    Ok(Endpoint { host, port, tls })
}

/// MQTT 3.1.1 §4.7 subscription filter: `+` must be a whole level, `#` only
/// the last level, never inside a level; no NUL, never empty.
pub fn valid_filter(filter: &str) -> bool {
    if filter.is_empty() || filter.contains('\0') {
        return false;
    }
    let levels: Vec<&str> = filter.split('/').collect();
    levels.iter().enumerate().all(|(i, level)| {
        let hash_ok = !level.contains('#') || (*level == "#" && i + 1 == levels.len());
        let plus_ok = !level.contains('+') || *level == "+";
        hash_ok && plus_ok
    })
}

/// A concrete topic (a publish destination): no wildcards, no NUL, never empty.
pub fn valid_topic(topic: &str) -> bool {
    !topic.is_empty() && !topic.contains(['#', '+', '\0'])
}

/// Does `filter` match `topic` (MQTT 3.1.1 §4.7)? `+` matches exactly one
/// level, a trailing `#` matches the rest including the parent itself
/// (`a/#` matches `a`).
pub fn topic_matches(filter: &str, topic: &str) -> bool {
    let mut f = filter.split('/');
    let mut t = topic.split('/');
    loop {
        match (f.next(), t.next()) {
            (Some("#"), _) => return true,
            (Some("+"), Some(_)) => continue,
            (Some(a), Some(b)) if a == b => continue,
            (None, None) => return true,
            _ => return false,
        }
    }
}

/// Configuration for [`MqttSource`] (the module table gives the env mapping).
#[derive(Clone, Debug)]
pub struct MqttConfig {
    /// As given, for log lines.
    pub url: String,
    pub host: String,
    pub port: u16,
    pub tls: bool,
    /// Subscription filter.
    pub topic: String,
    pub client_id: String,
    pub qos: QoS,
    pub username: Option<String>,
    pub password: Option<String>,
    pub clean_session: bool,
    pub dlq_topic: Option<String>,
    /// `Duration::ZERO` disables pings.
    pub keepalive: Duration,
    /// Top-level payload field carrying the exactly-once id.
    pub position_field: Option<String>,
}

/// Trimmed, non-empty env value — an exported-but-blank variable means "unset".
fn env(name: &str) -> Option<String> {
    std::env::var(name).ok().map(|v| v.trim().to_string()).filter(|v| !v.is_empty())
}

fn parse_bool(name: &str, raw: &str) -> Result<bool> {
    match raw.trim().to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => Ok(true),
        "0" | "false" | "no" | "off" => Ok(false),
        other => bail!("{name} '{other}' — expected true or false"),
    }
}

impl MqttConfig {
    /// Defaults for a broker at `url` (the module table's second column).
    pub fn new(url: impl Into<String>) -> Result<Self> {
        let url = url.into();
        let Endpoint { host, port, tls } = parse_url(&url)?;
        Ok(Self {
            url,
            host,
            port,
            tls,
            topic: DEFAULT_TOPIC.to_string(),
            client_id: format!("dagron-{}", uuid::Uuid::new_v4()),
            qos: QoS::AtLeastOnce,
            username: None,
            password: None,
            // A *generated* client id and a persistent session are never right
            // together: the broker keys the session on the id, so a restart with
            // a fresh uuid orphans the old session and every message queued
            // against it — the redelivery the persistent session exists for goes
            // to a client that will never reconnect, and each boot leaks another
            // session. `from_env` turns this on when the operator names a stable
            // `MQTT_CLIENT_ID`, which is what makes the guarantee real.
            clean_session: true,
            dlq_topic: None,
            keepalive: Duration::from_secs(30),
            position_field: None,
        })
    }

    /// Read the `MQTT_*` family (all registered in the engine's knob registry).
    /// Every malformed value is a startup error: a source that quietly ran with
    /// a default in place of a typo would ingest from the wrong broker.
    pub fn from_env() -> Result<Self> {
        let mut cfg = Self::new(env("MQTT_URL").unwrap_or_else(|| DEFAULT_URL.to_string()))?;
        if let Some(topic) = env("MQTT_TOPIC") {
            cfg.topic = topic;
        }
        if !valid_filter(&cfg.topic) {
            bail!(
                "MQTT_TOPIC '{}' is not a valid subscription filter ('+' must be a whole \
                 level, '#' only the last one)",
                cfg.topic
            );
        }
        // A stable id is what makes a persistent session mean anything, so it
        // also decides the default: named ⇒ resume the session and collect what
        // the broker held while this unit was away; generated ⇒ start clean,
        // because a session nobody will ever reconnect to only strands messages.
        // An explicit MQTT_CLEAN_SESSION below still wins either way.
        let stable_id = env("MQTT_CLIENT_ID");
        if let Some(id) = stable_id.clone() {
            cfg.client_id = id;
            cfg.clean_session = false;
        }
        cfg.qos = match env("MQTT_QOS").as_deref() {
            None | Some("1") => QoS::AtLeastOnce,
            Some("0") => QoS::AtMostOnce,
            Some("2") => QoS::ExactlyOnce,
            Some(other) => bail!("MQTT_QOS '{other}' — expected 0, 1 or 2"),
        };
        cfg.username = env("MQTT_USERNAME");
        // Not trimmed: a password is bytes, and a leading space in one is legal.
        cfg.password = std::env::var("MQTT_PASSWORD").ok().filter(|p| !p.is_empty());
        if cfg.password.is_some() && cfg.username.is_none() {
            bail!("MQTT_PASSWORD is set but MQTT_USERNAME is not — the broker needs both");
        }
        if let Some(raw) = env("MQTT_CLEAN_SESSION") {
            let clean = parse_bool("MQTT_CLEAN_SESSION", &raw)?;
            if !clean && stable_id.is_none() {
                bail!(
                    "MQTT_CLEAN_SESSION=false needs a stable MQTT_CLIENT_ID: the broker keys the \
                     session on the id, so a generated one orphans the session — and every message \
                     queued against it — on every restart"
                );
            }
            cfg.clean_session = clean;
        }
        cfg.dlq_topic = env("MQTT_DLQ_TOPIC");
        if let Some(dlq) = &cfg.dlq_topic {
            if !valid_topic(dlq) {
                bail!("MQTT_DLQ_TOPIC '{dlq}' must be a concrete topic (no '+' or '#')");
            }
            if topic_matches(&cfg.topic, dlq) {
                bail!(
                    "MQTT_DLQ_TOPIC '{dlq}' is matched by MQTT_TOPIC '{}' — the source would \
                     consume its own dead letters and re-park them forever; pick a topic \
                     outside the subscription",
                    cfg.topic
                );
            }
        }
        if let Some(raw) = env("MQTT_KEEPALIVE_SECS") {
            let secs: u64 = raw
                .parse()
                .with_context(|| format!("MQTT_KEEPALIVE_SECS '{raw}' is not a whole number of seconds"))?;
            cfg.keepalive = Duration::from_secs(secs);
        }
        cfg.position_field = env("MQTT_POSITION_FIELD");
        Ok(cfg)
    }

    /// The rumqttc options this config resolves to. Connection is lazy — the
    /// first `poll` dials — so this never touches the network.
    fn mqtt_options(&self) -> MqttOptions {
        let mut opts = MqttOptions::new(self.client_id.clone(), self.host.clone(), self.port);
        opts.set_manual_acks(true)
            .set_clean_session(self.clean_session)
            .set_keep_alive(self.keepalive)
            .set_max_packet_size(MAX_INCOMING_BYTES, MAX_OUTGOING_BYTES);
        if let Some(user) = &self.username {
            opts.set_credentials(user.clone(), self.password.clone().unwrap_or_default());
        }
        if self.tls {
            // rustls 0.23 wants one process-level CryptoProvider before a
            // ClientConfig is built. Idempotent: a second install just reports
            // the one already there (the kube executor installs the same ring
            // provider), so the result is deliberately ignored.
            let _ = rustls::crypto::ring::default_provider().install_default();
            opts.set_transport(rumqttc::Transport::tls_with_default_config());
        }
        opts
    }
}

// ── Position helpers (pure) ───────────────────────────────────────────────────

/// The exactly-once id of a payload: the top-level `field` of the JSON — or,
/// since every spec is YAML and JSON is a subset of it, of the YAML — document.
/// Strings are taken as-is, numbers in their canonical text; anything else
/// (missing, nested, boolean, object) is `None`, which keeps the message on the
/// at-least-once path rather than inventing a coordinate.
pub fn extract_position(payload: &str, field: &str) -> Option<String> {
    let value: serde_json::Value = serde_json::from_str(payload)
        .or_else(|_| serde_yaml::from_str(payload))
        .ok()?;
    match value.get(field)? {
        serde_json::Value::String(s) => Some(s.trim().to_string()).filter(|s| !s.is_empty()),
        serde_json::Value::Number(n) => Some(n.to_string()),
        _ => None,
    }
}

/// Has `position` already been accounted for, given the topic's committed
/// cursor? Numeric when both sides parse as `u64` (a redelivered or replayed
/// message is at or below the cursor), string equality otherwise (a CloudEvents
/// id catches the immediate redelivery, which is the crash window that matters).
pub fn is_duplicate(position: &str, committed: Option<&str>) -> bool {
    let Some(committed) = committed else { return false };
    match (position.trim().parse::<u64>(), committed.trim().parse::<u64>()) {
        (Ok(p), Ok(c)) => p <= c,
        _ => position == committed,
    }
}

// ── MqttSource ────────────────────────────────────────────────────────────────

/// The message handed out and not yet acked, with what the ack needs.
struct Inflight {
    publish: Publish,
    payload: String,
    position: Option<String>,
}

/// Broker subscription as a workflow source — see the module docs for semantics.
pub struct MqttSource {
    cfg: MqttConfig,
    client: AsyncClient,
    eventloop: EventLoop,
    /// The message delivered by the last `recv` and not yet acked. `nack` moves
    /// it to `redeliver`; `ack` sends the broker its PUBACK.
    inflight: Option<Inflight>,
    /// A nacked message, handed back first by the next `recv`.
    redeliver: Option<Inflight>,
    /// A subscribe is owed: set on every CONNACK (a resumed session keeps its
    /// subscriptions, a fresh one does not — subscribing again is idempotent
    /// and the only way to be sure) and after a refused SUBACK (retried on the
    /// next `recv`, i.e. after the ingest actor's backoff).
    subscribe_pending: bool,
    /// The last poll failed; the next CONNACK is a recovery, logged as such.
    was_down: bool,
    /// Per-topic committed cursor, read lazily from `source_offsets` the first
    /// time a topic is seen and advanced on every ack. The value is what the
    /// ingest actor committed with the run, so a restart consults the datastore
    /// once per topic and then answers from memory.
    committed: HashMap<String, Option<String>>,
    /// Topics already warned about for a missing position field — once, not
    /// once per message.
    warned_no_position: HashSet<String>,
    /// Datastore + the `source_offsets` namespace (the configured `SOURCE`
    /// value, which is the ingest actor's `source_name`).
    store: Option<(dagron_core::db::Pool, String)>,
}

impl MqttSource {
    /// Build the client. Nothing is dialled here — rumqttc connects on the
    /// first poll, so a broker that is down at boot is a `recv` error with a
    /// retry, not a failed start.
    pub fn new(cfg: MqttConfig) -> Self {
        let (client, eventloop) = AsyncClient::new(cfg.mqtt_options(), REQUEST_CAP);
        Self {
            cfg,
            client,
            eventloop,
            inflight: None,
            redeliver: None,
            subscribe_pending: false,
            was_down: false,
            committed: HashMap::new(),
            warned_no_position: HashSet::new(),
            store: None,
        }
    }

    pub fn from_env() -> Result<Self> {
        Ok(Self::new(MqttConfig::from_env()?))
    }

    /// Exactly-once across restarts: read each topic's committed cursor back
    /// from `source_offsets`. `source_name` must be the ingest actor's (the
    /// `SOURCE` value) or the row written with the run and the row read here
    /// would differ.
    pub fn with_datastore(
        mut self,
        pool: dagron_core::db::Pool,
        source_name: impl Into<String>,
    ) -> Self {
        self.store = Some((pool, source_name.into()));
        self
    }

    pub fn config(&self) -> &MqttConfig {
        &self.cfg
    }

    /// The committed cursor for `topic`: the in-memory answer when there is
    /// one, else one indexed datastore read, cached. A failed read is *not*
    /// cached — the next message on the topic asks again — and answers
    /// `None`, because a duplicate run is visible and recoverable while a
    /// message that silently never runs is neither.
    async fn committed_for(&mut self, topic: &str) -> Option<String> {
        if let Some(cached) = self.committed.get(topic) {
            return cached.clone();
        }
        let Some((pool, source_name)) = self.store.as_ref() else {
            self.committed.insert(topic.to_string(), None);
            return None;
        };
        let key = format!("{source_name}/{topic}");
        match dagron_core::db::source_offset(pool, &key).await {
            Ok(committed) => {
                self.committed.insert(topic.to_string(), committed.clone());
                committed
            }
            Err(e) => {
                tracing::warn!(topic, error = %e,
                    "could not read the committed position for this topic — treating the message as new");
                None
            }
        }
    }

    /// Decide what to do with a publish: skip it as a duplicate (acking it so
    /// the broker stops redelivering), or hand it out and hold it in flight.
    async fn admit(&mut self, publish: Publish) -> Option<WorkflowMessage> {
        let payload = String::from_utf8_lossy(&publish.payload).into_owned();
        let position = match &self.cfg.position_field {
            None => None,
            Some(field) => {
                let found = extract_position(&payload, field);
                if found.is_none() && self.warned_no_position.insert(publish.topic.clone()) {
                    tracing::warn!(topic = %publish.topic, field,
                        "MQTT_POSITION_FIELD not found at the top level of a payload on this topic — \
                         delivering at-least-once (warned once per topic)");
                }
                found
            }
        };
        if let Some(pos) = &position {
            let committed = self.committed_for(&publish.topic).await;
            if is_duplicate(pos, committed.as_deref()) {
                tracing::info!(topic = %publish.topic, position = %pos,
                    committed = committed.as_deref().unwrap_or("-"),
                    "duplicate MQTT message (at or below the committed position) — acked and skipped");
                if let Err(e) = self.client.ack(&publish).await {
                    tracing::warn!(error = %e, "ack of a duplicate failed — it may redeliver");
                }
                return None;
            }
        }
        if publish.retain {
            tracing::warn!(topic = %publish.topic,
                "retained MQTT message received — a retained spec is redelivered on every \
                 (re)subscribe; publish specs without retain, or set MQTT_POSITION_FIELD");
        }
        tracing::debug!(topic = %publish.topic, pkid = publish.pkid, dup = publish.dup,
            bytes = publish.payload.len(), position = position.as_deref().unwrap_or("-"),
            "MQTT message received");
        self.inflight = Some(Inflight { publish, payload: payload.clone(), position });
        Some(WorkflowMessage { payload, handle: AckHandle::None })
    }
}

#[async_trait]
impl WorkflowSource for MqttSource {
    async fn recv(&mut self) -> Result<Option<WorkflowMessage>> {
        // A nacked message goes back out before the broker is asked for
        // anything new — the in-process analog of a broker redelivery.
        if let Some(msg) = self.redeliver.take() {
            tracing::debug!(topic = %msg.publish.topic, pkid = msg.publish.pkid,
                "redelivering nacked MQTT message");
            let payload = msg.payload.clone();
            self.inflight = Some(msg);
            return Ok(Some(WorkflowMessage { payload, handle: AckHandle::None }));
        }
        loop {
            if self.subscribe_pending {
                // Queued on the request channel; the poll below writes it.
                self.client
                    .subscribe(self.cfg.topic.clone(), self.cfg.qos)
                    .await
                    .map_err(|e| anyhow!("queue MQTT subscribe for '{}': {e}", self.cfg.topic))?;
                self.subscribe_pending = false;
            }
            let event = match self.eventloop.poll().await {
                Ok(event) => event,
                // rumqttc has already torn the connection down and will redial
                // on the next poll; the ingest actor logs this chain and backs
                // off 1 s before that next poll. A fresh CONNACK re-arms the
                // subscribe, so nothing is owed here.
                Err(e) => {
                    self.was_down = true;
                    self.subscribe_pending = false;
                    return Err(anyhow::Error::new(e).context(format!(
                        "MQTT broker {}:{} unreachable or dropped the connection (client '{}') — retrying",
                        self.cfg.host, self.cfg.port, self.cfg.client_id
                    )));
                }
            };
            match event {
                Event::Incoming(Packet::ConnAck(ack)) => {
                    if std::mem::take(&mut self.was_down) {
                        tracing::info!(url = %self.cfg.url, client_id = %self.cfg.client_id,
                            session_present = ack.session_present, "MQTT broker reconnected");
                    } else {
                        tracing::info!(url = %self.cfg.url, client_id = %self.cfg.client_id,
                            session_present = ack.session_present, "MQTT broker connected");
                    }
                    self.subscribe_pending = true;
                }
                Event::Incoming(Packet::SubAck(ack)) => {
                    if ack.return_codes.iter().any(|c| matches!(c, SubscribeReasonCode::Failure)) {
                        // Loud on purpose: a refused subscription (an ACL, a
                        // broker policy) would otherwise be silent idleness.
                        // The retry recovers without a restart once the ACL
                        // is fixed.
                        self.subscribe_pending = true;
                        bail!(
                            "MQTT broker refused the subscription to '{}' for client '{}' \
                             (check the broker ACL) — retrying",
                            self.cfg.topic,
                            self.cfg.client_id
                        );
                    }
                    tracing::info!(topic = %self.cfg.topic, granted = ?ack.return_codes,
                        "MQTT subscription active");
                }
                Event::Incoming(Packet::Publish(publish)) => {
                    if let Some(msg) = self.admit(publish).await {
                        return Ok(Some(msg));
                    }
                }
                // Acks, pings, our own outgoing traffic: nothing to hand out.
                _ => {}
            }
        }
    }

    /// The run (or dead letter) is durable: remember the position it committed
    /// and release the message at the broker. The cursor cache advances FIRST,
    /// so a failed ack request (logged by the actor, message may redeliver)
    /// still leaves the redelivery recognisable as a duplicate.
    async fn ack(&mut self, _handle: &AckHandle) -> Result<()> {
        let Some(msg) = self.inflight.take() else { return Ok(()) };
        if let Some(pos) = msg.position {
            self.committed.insert(msg.publish.topic.clone(), Some(pos));
        }
        self.client
            .ack(&msg.publish)
            .await
            .map_err(|e| anyhow!("MQTT ack (pkid {}, topic '{}'): {e}", msg.publish.pkid, msg.publish.topic))
    }

    /// Keep the message so the next `recv` hands it back.
    async fn nack(&mut self, _handle: &AckHandle) -> Result<()> {
        if let Some(msg) = self.inflight.take() {
            self.redeliver = Some(msg);
        }
        Ok(())
    }

    /// Mirror a poison payload to the DLQ topic (QoS 1, never retained). The
    /// durable `dead_letters` row is the ingest actor's; this is the broker-side
    /// copy for whatever consumes dead letters on the bus.
    ///
    /// `payload` is the message as the source read it — lossy UTF-8, so a binary
    /// spec is not byte-identical — and is **truncated** past
    /// [`MAX_DLQ_PAYLOAD_BYTES`], with `truncated: true` on the envelope when it
    /// was. Both are deliberate: the durable row is the record of what happened,
    /// and an envelope that cannot be encoded would kill the connection and
    /// stall the subscription rather than mirror anything.
    async fn dead_letter(&mut self, payload: &str, error: &str) -> Result<()> {
        let Some(dlq) = &self.cfg.dlq_topic else { return Ok(()) };
        let origin = self
            .inflight
            .as_ref()
            .map(|m| m.publish.topic.as_str())
            .unwrap_or(self.cfg.topic.as_str());
        let kept = truncate_on_boundary(payload, MAX_DLQ_PAYLOAD_BYTES);
        let truncated = kept.len() < payload.len();
        let entry = serde_json::json!({
            "error": truncate_on_boundary(error, MAX_DLQ_ERROR_BYTES),
            "topic": origin,
            "payload": kept,
            "truncated": truncated,
        });
        self.client
            .publish(dlq.clone(), QoS::AtLeastOnce, false, entry.to_string())
            .await
            .map_err(|e| anyhow!("MQTT dead-letter publish to '{dlq}': {e}"))?;
        Ok(())
    }

    /// Exactly-once: the in-flight message's id, namespaced by its topic, for
    /// the ingest actor to commit with the run it becomes. `None` (no position
    /// field configured, or the payload lacks it) keeps the at-least-once path.
    fn pending_position(&self) -> Option<PendingPosition> {
        let msg = self.inflight.as_ref()?;
        let position = msg.position.clone()?;
        Some(PendingPosition { substream: Some(msg.publish.topic.clone()), position })
    }

    /// Cursors are per topic and read lazily (`committed_for`); the
    /// whole-source row the actor offers does not apply to a subscription.
    async fn set_committed_position(&mut self, _position: Option<String>) -> Result<()> {
        Ok(())
    }
}

// ── Test support ──────────────────────────────────────────────────────────────

/// Shared by every test that touches the `MQTT_*` environment, in this module
/// and in `source.rs`: the process environment is global, and cargo runs test
/// threads in parallel.
#[cfg(test)]
pub(crate) mod test_support {
    use std::sync::{Mutex, MutexGuard};

    pub(crate) const VARS: &[&str] = &[
        "MQTT_URL",
        "MQTT_TOPIC",
        "MQTT_CLIENT_ID",
        "MQTT_QOS",
        "MQTT_USERNAME",
        "MQTT_PASSWORD",
        "MQTT_CLEAN_SESSION",
        "MQTT_DLQ_TOPIC",
        "MQTT_KEEPALIVE_SECS",
        "MQTT_POSITION_FIELD",
    ];

    /// Hold this for the whole test; it also clears the family on acquisition
    /// so a test starts from "nothing set" whatever ran before it.
    pub(crate) fn env_lock() -> MutexGuard<'static, ()> {
        static LOCK: Mutex<()> = Mutex::new(());
        let guard = LOCK.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        for v in VARS {
            std::env::remove_var(v);
        }
        guard
    }
}

#[cfg(test)]
mod tests {
    use super::test_support::env_lock;
    use super::*;
    use tokio::time::timeout;

    // ── URL parsing ──────────────────────────────────────────────────────────

    #[test]
    fn parse_url_accepts_the_documented_shapes() {
        let ep = |h: &str, p: u16, tls: bool| Endpoint { host: h.to_string(), port: p, tls };
        assert_eq!(parse_url("mqtt://127.0.0.1:1883").unwrap(), ep("127.0.0.1", 1883, false));
        assert_eq!(parse_url("mqtt://broker.plant").unwrap(), ep("broker.plant", 1883, false), "default plain port");
        assert_eq!(parse_url("mqtts://broker.plant").unwrap(), ep("broker.plant", 8883, true), "default TLS port");
        assert_eq!(parse_url("mqtts://broker.plant:9443/").unwrap(), ep("broker.plant", 9443, true), "trailing slash tolerated");
        assert_eq!(parse_url("tcp://gw:1884").unwrap(), ep("gw", 1884, false), "tcp:// alias");
        assert_eq!(parse_url("ssl://gw").unwrap(), ep("gw", 8883, true), "ssl:// alias");
        assert_eq!(parse_url("  MQTT://GW:1883  ").unwrap(), ep("GW", 1883, false), "scheme case-insensitive, whitespace trimmed");
        assert_eq!(parse_url("mqtt://[::1]:1883").unwrap(), ep("::1", 1883, false), "bracketed IPv6");
        assert_eq!(parse_url("mqtts://[fe80::1]").unwrap(), ep("fe80::1", 8883, true), "bracketed IPv6, default port");
    }

    #[test]
    fn parse_url_rejects_what_it_cannot_honour() {
        let err = |u: &str| parse_url(u).unwrap_err().to_string();
        assert!(err("127.0.0.1:1883").contains("no scheme"));
        assert!(err("ws://broker").contains("not supported"));
        assert!(err("mqtt://").contains("no host"));
        assert!(err("mqtt://broker:0").contains("1..=65535"));
        assert!(err("mqtt://broker:notaport").contains("1..=65535"));
        assert!(err("mqtt://broker:70000").contains("1..=65535"));
        assert!(err("mqtt://user:pw@broker").contains("embeds credentials"));
        assert!(err("mqtt://broker/topic").contains("path or query"));
        assert!(err("mqtt://broker?client_id=x").contains("path or query"));
        assert!(err("mqtt://[::1").contains("unterminated IPv6"));
        assert!(err("mqtt://[::1]1883").contains("expected ':port'"));
    }

    // ── Topic filters ────────────────────────────────────────────────────────

    #[test]
    fn filters_and_topics_follow_the_spec() {
        for ok in ["a", "a/b", "+", "#", "a/+/c", "a/#", "+/+", "$SYS/#", "/", "a//b"] {
            assert!(valid_filter(ok), "{ok} is a valid filter");
        }
        for bad in ["", "a#", "#/a", "a/#/b", "a+", "a/b+", "a\0b"] {
            assert!(!valid_filter(bad), "{bad} is not a valid filter");
        }
        assert!(valid_topic("plant/line1/jobs"));
        for bad in ["", "plant/#", "plant/+/x", "a\0"] {
            assert!(!valid_topic(bad), "{bad} is not a publishable topic");
        }
    }

    #[test]
    fn topic_matching_handles_both_wildcards() {
        assert!(topic_matches("a/b", "a/b"));
        assert!(!topic_matches("a/b", "a/b/c"));
        assert!(!topic_matches("a/b/c", "a/b"));
        assert!(topic_matches("a/+/c", "a/x/c"));
        assert!(!topic_matches("a/+/c", "a/x/y/c"));
        assert!(!topic_matches("a/+", "a"));
        assert!(topic_matches("a/#", "a/b/c/d"));
        assert!(topic_matches("a/#", "a"), "a trailing # matches the parent level too");
        assert!(!topic_matches("a/#", "b/a"));
        assert!(topic_matches("#", "anything/at/all"));
        assert!(topic_matches("+/+", "a/b"));
        assert!(!topic_matches("+", "a/b"));
    }

    // ── Position helpers ─────────────────────────────────────────────────────

    #[test]
    fn extract_position_reads_top_level_strings_and_numbers() {
        assert_eq!(extract_position(r#"{"seq": 12, "name": "x"}"#, "seq").as_deref(), Some("12"));
        assert_eq!(extract_position(r#"{"id": "evt-7"}"#, "id").as_deref(), Some("evt-7"));
        assert_eq!(extract_position(r#"{"seq": 1.5}"#, "seq").as_deref(), Some("1.5"));
        assert_eq!(extract_position(r#"{"seq": "  9 "}"#, "seq").as_deref(), Some("9"), "trimmed");
        // YAML is the spec language; a `seq:` key on a YAML spec works too.
        assert_eq!(extract_position("seq: 42\nname: x\ntasks: []\n", "seq").as_deref(), Some("42"));
        assert_eq!(extract_position(r#"{"name": "x"}"#, "seq"), None, "missing");
        assert_eq!(extract_position(r#"{"meta": {"seq": 3}}"#, "seq"), None, "nested is not top-level");
        assert_eq!(extract_position(r#"{"seq": true}"#, "seq"), None, "bool");
        assert_eq!(extract_position(r#"{"seq": null}"#, "seq"), None, "null");
        assert_eq!(extract_position(r#"{"seq": [1]}"#, "seq"), None, "array");
        assert_eq!(extract_position(r#"{"seq": ""}"#, "seq"), None, "empty string");
        assert_eq!(extract_position("not: [valid", "seq"), None, "unparsable payload");
        assert_eq!(extract_position("just a scalar", "seq"), None, "scalar document");
    }

    #[test]
    fn is_duplicate_is_numeric_when_it_can_be_and_equality_otherwise() {
        assert!(!is_duplicate("5", None), "nothing committed yet");
        assert!(is_duplicate("5", Some("5")));
        assert!(is_duplicate("4", Some("5")), "below the cursor: a replay");
        assert!(!is_duplicate("6", Some("5")));
        assert!(is_duplicate("05", Some("5")), "numeric compare, not textual");
        assert!(is_duplicate("evt-7", Some("evt-7")));
        assert!(!is_duplicate("evt-6", Some("evt-7")), "strings are not ordered");
        assert!(!is_duplicate("7", Some("evt-7")), "mixed falls back to equality");
        assert!(!is_duplicate("evt-7", Some("7")));
    }

    // ── Config from env ──────────────────────────────────────────────────────

    #[test]
    fn from_env_defaults_match_the_registry() {
        let _g = env_lock();
        let cfg = MqttConfig::from_env().unwrap();
        assert_eq!(cfg.url, DEFAULT_URL);
        assert_eq!((cfg.host.as_str(), cfg.port, cfg.tls), ("127.0.0.1", 1883, false));
        assert_eq!(cfg.topic, DEFAULT_TOPIC);
        assert!(cfg.client_id.starts_with("dagron-"), "{}", cfg.client_id);
        assert!(cfg.client_id.len() > "dagron-".len() + 30, "a uuid follows the prefix");
        assert_eq!(cfg.qos, QoS::AtLeastOnce);
        assert_eq!((cfg.username.as_deref(), cfg.password.as_deref()), (None, None));
        assert!(
            cfg.clean_session,
            "a generated client id starts clean — a persistent session keyed to an id that is \
             never reused only strands the messages it holds"
        );
        assert_eq!(cfg.dlq_topic, None);
        assert_eq!(cfg.keepalive, Duration::from_secs(30));
        assert_eq!(cfg.position_field, None);
        // Two sources built from defaults never share a client id.
        assert_ne!(MqttConfig::from_env().unwrap().client_id, cfg.client_id);
    }

    #[test]
    fn from_env_reads_every_knob() {
        let _g = env_lock();
        std::env::set_var("MQTT_URL", "mqtts://broker.plant:9443");
        std::env::set_var("MQTT_TOPIC", "plant/+/jobs");
        std::env::set_var("MQTT_CLIENT_ID", "cell-7");
        std::env::set_var("MQTT_QOS", "2");
        std::env::set_var("MQTT_USERNAME", "dagron");
        std::env::set_var("MQTT_PASSWORD", " s3cret ");
        std::env::set_var("MQTT_CLEAN_SESSION", "yes");
        std::env::set_var("MQTT_DLQ_TOPIC", "plant-dlq/jobs");
        std::env::set_var("MQTT_KEEPALIVE_SECS", "0");
        std::env::set_var("MQTT_POSITION_FIELD", " seq ");
        let cfg = MqttConfig::from_env().unwrap();
        assert_eq!((cfg.host.as_str(), cfg.port, cfg.tls), ("broker.plant", 9443, true));
        assert_eq!(cfg.topic, "plant/+/jobs");
        assert_eq!(cfg.client_id, "cell-7");
        assert_eq!(cfg.qos, QoS::ExactlyOnce);
        assert_eq!(cfg.username.as_deref(), Some("dagron"));
        assert_eq!(cfg.password.as_deref(), Some(" s3cret "), "passwords are not trimmed");
        assert!(cfg.clean_session);
        assert_eq!(cfg.dlq_topic.as_deref(), Some("plant-dlq/jobs"), "outside the subscription");
        assert_eq!(cfg.keepalive, Duration::ZERO, "0 disables pings");
        assert_eq!(cfg.position_field.as_deref(), Some("seq"), "trimmed");
        // Building the options for a TLS endpoint must not panic (provider
        // install + default TLS config) — this is the mqtts:// boot path.
        let opts = cfg.mqtt_options();
        assert_eq!(opts.broker_address(), ("broker.plant".to_string(), 9443));
        assert!(opts.manual_acks());
        assert!(opts.clean_session());
        assert_eq!(opts.max_packet_size(), MAX_INCOMING_BYTES);
        assert_eq!(opts.credentials().map(|l| l.username), Some("dagron".to_string()));
        assert!(matches!(opts.transport(), rumqttc::Transport::Tls(_)));
    }

    #[test]
    fn from_env_treats_blank_as_unset_and_accepts_qos_zero() {
        let _g = env_lock();
        std::env::set_var("MQTT_TOPIC", "   ");
        std::env::set_var("MQTT_QOS", "0");
        std::env::set_var("MQTT_CLIENT_ID", "cell-7");
        std::env::set_var("MQTT_CLEAN_SESSION", "0");
        std::env::set_var("MQTT_USERNAME", "u");
        let cfg = MqttConfig::from_env().unwrap();
        assert_eq!(cfg.topic, DEFAULT_TOPIC, "a blank export is the default, not an empty filter");
        assert_eq!(cfg.qos, QoS::AtMostOnce);
        assert!(!cfg.clean_session);
        // A username without a password is legal in MQTT; the password sent is empty.
        assert_eq!(cfg.mqtt_options().credentials().map(|l| l.password), Some(String::new()));
    }

    /// A persistent session is only meaningful with an id the broker will see
    /// again, so the id decides the default and the broken pairing is refused
    /// outright rather than silently losing whatever the broker queued.
    #[test]
    fn a_persistent_session_requires_a_stable_client_id() {
        let _g = env_lock();
        std::env::set_var("MQTT_CLIENT_ID", "line-3");
        let named = MqttConfig::from_env().unwrap();
        assert!(!named.clean_session, "a named id opts into resuming the session");
        assert_eq!(named.client_id, "line-3");

        std::env::remove_var("MQTT_CLIENT_ID");
        std::env::set_var("MQTT_CLEAN_SESSION", "false");
        let err = MqttConfig::from_env().expect_err("generated id + persistent session is refused");
        assert!(format!("{err:#}").contains("stable MQTT_CLIENT_ID"), "{err:#}");
        std::env::remove_var("MQTT_CLEAN_SESSION");
    }

    #[test]
    fn from_env_rejects_malformed_values() {
        let _g = env_lock();
        let expect = |var: &str, value: &str, needle: &str| {
            std::env::set_var(var, value);
            let err = MqttConfig::from_env().expect_err(&format!("{var}={value} must fail")).to_string();
            std::env::remove_var(var);
            assert!(err.contains(needle), "{var}={value}: {err}");
        };
        expect("MQTT_URL", "ws://broker", "not supported");
        expect("MQTT_TOPIC", "plant/#/jobs", "not a valid subscription filter");
        expect("MQTT_QOS", "3", "expected 0, 1 or 2");
        expect("MQTT_PASSWORD", "pw", "MQTT_USERNAME is not");
        expect("MQTT_CLEAN_SESSION", "maybe", "expected true or false");
        expect("MQTT_KEEPALIVE_SECS", "30s", "whole number of seconds");
        expect("MQTT_DLQ_TOPIC", "dagron/dlq/#", "concrete topic");
        // The default filter is `dagron/workflows`; a DLQ inside it would loop.
        expect("MQTT_DLQ_TOPIC", "dagron/workflows", "consume its own dead letters");
        std::env::set_var("MQTT_TOPIC", "plant/#");
        expect("MQTT_DLQ_TOPIC", "plant/dlq", "consume its own dead letters");
        std::env::set_var("MQTT_DLQ_TOPIC", "dlq/plant");
        assert!(MqttConfig::from_env().is_ok(), "a DLQ outside the filter is fine");
    }

    // ── A minimal in-process MQTT 3.1.1 broker ───────────────────────────────
    //
    // Just enough of the wire protocol to stand in for one: CONNACK, SUBACK,
    // PINGRESP, deliver QoS 1 PUBLISHes on command, and report what the client
    // sent (PUBACKs, its own PUBLISHes). Framed by hand — the `bytes` crate is
    // not a dependency of this crate, so rumqttc's codec cannot be named here.
    mod fake_broker {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::tcp::OwnedReadHalf;
        use tokio::net::{TcpListener, TcpStream};
        use tokio::sync::mpsc;

        #[derive(Debug, PartialEq, Eq)]
        pub enum Seen {
            Connect,
            Subscribe(Vec<String>),
            PubAck(u16),
            Publish { topic: String, payload: String },
            Disconnected,
        }

        pub enum Cmd {
            Publish { topic: String, pkid: u16, payload: String, dup: bool },
            Drop,
        }

        pub struct Broker {
            pub url: String,
            pub cmd: mpsc::UnboundedSender<Cmd>,
        }

        /// Start listening; accepts one client at a time, forever (so a client
        /// that reconnects after `Cmd::Drop` is served again).
        pub async fn start(refuse_subscribe: bool) -> (Broker, mpsc::UnboundedReceiver<Seen>) {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();
            let (cmd_tx, mut cmd_rx) = mpsc::unbounded_channel();
            let (seen_tx, seen_rx) = mpsc::unbounded_channel();
            tokio::spawn(async move {
                loop {
                    let Ok((stream, _)) = listener.accept().await else { return };
                    serve(stream, &mut cmd_rx, &seen_tx, refuse_subscribe).await;
                    let _ = seen_tx.send(Seen::Disconnected);
                }
            });
            (Broker { url: format!("mqtt://{addr}"), cmd: cmd_tx }, seen_rx)
        }

        async fn read_frame(rd: &mut OwnedReadHalf) -> Option<(u8, Vec<u8>)> {
            let mut b = [0u8; 1];
            rd.read_exact(&mut b).await.ok()?;
            let first = b[0];
            let (mut len, mut mult) = (0usize, 1usize);
            loop {
                rd.read_exact(&mut b).await.ok()?;
                len += (b[0] & 0x7F) as usize * mult;
                if b[0] & 0x80 == 0 {
                    break;
                }
                mult *= 128;
            }
            let mut body = vec![0u8; len];
            rd.read_exact(&mut body).await.ok()?;
            Some((first, body))
        }

        fn frame(first: u8, body: &[u8]) -> Vec<u8> {
            let mut out = vec![first];
            let mut len = body.len();
            loop {
                let mut byte = (len % 128) as u8;
                len /= 128;
                if len > 0 {
                    byte |= 0x80;
                }
                out.push(byte);
                if len == 0 {
                    break;
                }
            }
            out.extend_from_slice(body);
            out
        }

        fn publish_frame(topic: &str, pkid: u16, payload: &[u8], dup: bool) -> Vec<u8> {
            let mut body = Vec::new();
            body.extend_from_slice(&(topic.len() as u16).to_be_bytes());
            body.extend_from_slice(topic.as_bytes());
            body.extend_from_slice(&pkid.to_be_bytes());
            body.extend_from_slice(payload);
            frame(0x30 | 0x02 | if dup { 0x08 } else { 0 }, &body) // QoS 1
        }

        async fn serve(
            stream: TcpStream,
            cmd: &mut mpsc::UnboundedReceiver<Cmd>,
            seen: &mpsc::UnboundedSender<Seen>,
            refuse_subscribe: bool,
        ) {
            let (mut rd, mut wr) = stream.into_split();
            loop {
                tokio::select! {
                    frame_in = read_frame(&mut rd) => {
                        let Some((first, body)) = frame_in else { return };
                        match first >> 4 {
                            1 => {
                                wr.write_all(&[0x20, 0x02, 0x00, 0x00]).await.ok();
                                let _ = seen.send(Seen::Connect);
                            }
                            8 => {
                                let pkid = [body[0], body[1]];
                                let mut filters = Vec::new();
                                let mut i = 2;
                                while i + 2 <= body.len() {
                                    let n = u16::from_be_bytes([body[i], body[i + 1]]) as usize;
                                    i += 2;
                                    filters.push(String::from_utf8_lossy(&body[i..i + n]).into_owned());
                                    i += n + 1; // + the requested QoS byte
                                }
                                let code = if refuse_subscribe { 0x80 } else { 0x01 };
                                let mut ack = vec![pkid[0], pkid[1]];
                                ack.extend(std::iter::repeat(code).take(filters.len()));
                                wr.write_all(&frame(0x90, &ack)).await.ok();
                                let _ = seen.send(Seen::Subscribe(filters));
                            }
                            12 => {
                                wr.write_all(&[0xD0, 0x00]).await.ok();
                            }
                            4 => {
                                let _ = seen.send(Seen::PubAck(u16::from_be_bytes([body[0], body[1]])));
                            }
                            3 => {
                                let qos = (first >> 1) & 0x03;
                                let n = u16::from_be_bytes([body[0], body[1]]) as usize;
                                let topic = String::from_utf8_lossy(&body[2..2 + n]).into_owned();
                                let mut i = 2 + n;
                                if qos > 0 {
                                    let pkid = [body[i], body[i + 1]];
                                    i += 2;
                                    wr.write_all(&[0x40, 0x02, pkid[0], pkid[1]]).await.ok();
                                }
                                let payload = String::from_utf8_lossy(&body[i..]).into_owned();
                                let _ = seen.send(Seen::Publish { topic, payload });
                            }
                            14 => return,
                            _ => {}
                        }
                    }
                    c = cmd.recv() => match c {
                        Some(Cmd::Publish { topic, pkid, payload, dup }) => {
                            wr.write_all(&publish_frame(&topic, pkid, payload.as_bytes(), dup)).await.ok();
                        }
                        Some(Cmd::Drop) | None => return,
                    },
                }
            }
        }
    }

    use fake_broker::{Cmd, Seen};

    const SPEC_A: &str = "name: a\ntasks:\n  - name: t\n    command: [\"true\"]\n";
    const SPEC_B: &str = "name: b\ntasks:\n  - name: t\n    command: [\"true\"]\n";

    fn source_for(broker: &fake_broker::Broker) -> MqttSource {
        let mut cfg = MqttConfig::new(&broker.url).unwrap();
        cfg.topic = "plant/#".to_string();
        cfg.client_id = "test-client".to_string();
        MqttSource::new(cfg)
    }

    /// The next event of the wanted kind, skipping others; a hard timeout so a
    /// broken handshake fails the test instead of hanging it.
    async fn next_where(
        seen: &mut tokio::sync::mpsc::UnboundedReceiver<Seen>,
        want: impl Fn(&Seen) -> bool,
    ) -> Seen {
        timeout(Duration::from_secs(5), async {
            loop {
                let ev = seen.recv().await.expect("broker task alive");
                if want(&ev) {
                    return ev;
                }
            }
        })
        .await
        .expect("expected broker event within 5 s")
    }

    /// Every `PubAck` the broker has seen so far. Acks are queued by
    /// `client.ack` and only written by a later event-loop poll, so *which*
    /// poll flushes a given ack is an implementation detail of the client —
    /// drain what has arrived once the traffic is over rather than asserting a
    /// flush order the source does not promise.
    async fn drain_pubacks(seen: &mut tokio::sync::mpsc::UnboundedReceiver<Seen>) -> Vec<u16> {
        let mut acked = Vec::new();
        while let Ok(Some(ev)) = timeout(Duration::from_millis(500), seen.recv()).await {
            if let Seen::PubAck(p) = ev {
                acked.push(p);
            }
        }
        acked.sort();
        acked
    }

    /// Turn the event loop a little with nothing to deliver: the pending acks
    /// and publishes are flushed on the first poll, then the wait is idle and
    /// safe to cancel (the reader accumulates into its buffer between polls,
    /// so a cancelled idle read loses nothing).
    async fn drive(src: &mut MqttSource) {
        let _ = timeout(Duration::from_millis(300), src.recv()).await;
    }

    /// Wait for the subscription, then publish `frames` in order.
    async fn publish_after_subscribe(
        seen: &mut tokio::sync::mpsc::UnboundedReceiver<Seen>,
        broker: &fake_broker::Broker,
        frames: Vec<(&str, u16, &str, bool)>,
    ) {
        let sub = next_where(seen, |e| matches!(e, Seen::Subscribe(_))).await;
        assert_eq!(sub, Seen::Subscribe(vec!["plant/#".to_string()]));
        for (topic, pkid, payload, dup) in frames {
            broker
                .cmd
                .send(Cmd::Publish { topic: topic.into(), pkid, payload: payload.into(), dup })
                .unwrap();
        }
    }

    /// Full at-least-once cycle: a published spec is handed out; a nack hands
    /// the same message back without any broker traffic; an ack produces the
    /// PUBACK the broker is waiting for.
    #[tokio::test]
    async fn delivers_redelivers_on_nack_and_acks_the_broker() {
        let (broker, mut seen) = fake_broker::start(false).await;
        let mut src = source_for(&broker);

        let recv = src.recv();
        let feed = publish_after_subscribe(&mut seen, &broker, vec![("plant/line1/jobs", 7, SPEC_A, false)]);
        let (msg, _) = tokio::join!(timeout(Duration::from_secs(5), recv), feed);
        let msg = msg.expect("delivered within 5 s").unwrap().expect("never Ok(None)");
        assert_eq!(msg.payload, SPEC_A);
        assert!(src.pending_position().is_none(), "no position field configured");

        src.nack(&msg.handle).await.unwrap();
        let again = timeout(Duration::from_millis(500), src.recv())
            .await
            .expect("a nacked message redelivers immediately")
            .unwrap()
            .unwrap();
        assert_eq!(again.payload, SPEC_A);

        src.ack(&again.handle).await.unwrap();
        drive(&mut src).await;
        assert_eq!(next_where(&mut seen, |e| matches!(e, Seen::PubAck(_))).await, Seen::PubAck(7));

        // Acked: nothing in flight, nothing to redeliver.
        assert!(timeout(Duration::from_millis(300), src.recv()).await.is_err(), "idle after ack");
    }

    /// Exactly-once: with a position field, a message at or below the topic's
    /// committed cursor (read from `source_offsets` under `mqtt/<topic>`) is
    /// acked and skipped; the next one is delivered with its coordinate; an ack
    /// advances the in-memory cursor so a broker redelivery (dup) is skipped too.
    #[tokio::test]
    async fn duplicate_positions_are_acked_and_skipped() {
        let db = std::env::temp_dir().join(format!("m54-mqtt-{}.db", uuid::Uuid::new_v4()));
        let pool = dagron_core::db::init_pool(db.to_str().unwrap()).await.unwrap();
        // An earlier process committed seq 3 on this topic with the run it created.
        let dag = dagron_core::dag::DagGraph::from_yaml(SPEC_A).unwrap();
        dagron_core::db::create_run_with_offset(&pool, &dag, SPEC_A, "mqtt/plant/line1/jobs", "3")
            .await
            .unwrap();

        let (broker, mut seen) = fake_broker::start(false).await;
        let mut cfg = MqttConfig::new(&broker.url).unwrap();
        cfg.topic = "plant/#".to_string();
        cfg.client_id = "test-eo".to_string();
        cfg.position_field = Some("seq".to_string());
        let mut src = MqttSource::new(cfg).with_datastore(pool.clone(), "mqtt");

        let spec3 = format!("seq: 3\n{SPEC_A}");
        let spec4 = format!("seq: 4\n{SPEC_B}");
        let recv = src.recv();
        let feed = publish_after_subscribe(
            &mut seen,
            &broker,
            vec![("plant/line1/jobs", 1, &spec3, true), ("plant/line1/jobs", 2, &spec4, false)],
        );
        let (msg, _) = tokio::join!(timeout(Duration::from_secs(5), recv), feed);
        let msg = msg.expect("delivered").unwrap().unwrap();
        assert_eq!(msg.payload, spec4, "seq 3 is at the committed cursor: skipped");
        assert_eq!(
            src.pending_position(),
            Some(PendingPosition { substream: Some("plant/line1/jobs".to_string()), position: "4".to_string() })
        );
        // Commit like the actor does, then ack: the cursor cache moves to 4.
        let pp = src.pending_position().unwrap();
        let dag = dagron_core::dag::DagGraph::from_yaml(&msg.payload).unwrap();
        dagron_core::db::create_run_with_offset(&pool, &dag, &msg.payload, &pp.offset_key("mqtt"), &pp.position)
            .await
            .unwrap();
        src.ack(&msg.handle).await.unwrap();

        // The broker redelivers seq 4 (a crash-window replay), then sends seq 5.
        let spec5 = format!("seq: 5\n{SPEC_A}");
        broker.cmd.send(Cmd::Publish { topic: "plant/line1/jobs".into(), pkid: 3, payload: spec4.clone(), dup: true }).unwrap();
        broker.cmd.send(Cmd::Publish { topic: "plant/line1/jobs".into(), pkid: 4, payload: spec5.clone(), dup: false }).unwrap();
        let msg = timeout(Duration::from_secs(5), src.recv()).await.expect("delivered").unwrap().unwrap();
        assert_eq!(msg.payload, spec5, "the redelivered seq 4 is skipped from the in-memory cursor");


        // A different topic has its own cursor: seq 1 there is new.
        let spec1 = format!("seq: 1\n{SPEC_B}");
        src.ack(&msg.handle).await.unwrap();
        broker.cmd.send(Cmd::Publish { topic: "plant/line2/jobs".into(), pkid: 5, payload: spec1.clone(), dup: false }).unwrap();
        let msg = timeout(Duration::from_secs(5), src.recv()).await.expect("delivered").unwrap().unwrap();
        assert_eq!(msg.payload, spec1);
        assert_eq!(src.pending_position().unwrap().substream.as_deref(), Some("plant/line2/jobs"));

        // Nothing the source consumed is left unacknowledged: the skipped
        // duplicate (1), the delivered seq 4 (2), its replay (3) and seq 5 (4).
        // Only pkid 5 is still in flight, deliberately — the actor has not
        // committed it yet.
        assert_eq!(
            drain_pubacks(&mut seen).await,
            vec![1, 2, 3, 4],
            "every duplicate and every consumed message was acked"
        );

        pool.close().await;
        let _ = std::fs::remove_file(&db);
    }

    /// A payload without the position field stays at-least-once (no
    /// coordinate offered) rather than being refused.
    #[tokio::test]
    async fn payload_without_the_position_field_is_delivered_at_least_once() {
        let (broker, mut seen) = fake_broker::start(false).await;
        let mut cfg = MqttConfig::new(&broker.url).unwrap();
        cfg.topic = "plant/#".to_string();
        cfg.client_id = "test-nopos".to_string();
        cfg.position_field = Some("seq".to_string());
        let mut src = MqttSource::new(cfg);

        let recv = src.recv();
        let feed = publish_after_subscribe(&mut seen, &broker, vec![("plant/x", 9, SPEC_A, false)]);
        let (msg, _) = tokio::join!(timeout(Duration::from_secs(5), recv), feed);
        let msg = msg.expect("delivered").unwrap().unwrap();
        assert_eq!(msg.payload, SPEC_A);
        assert!(src.pending_position().is_none(), "no id, no coordinate");
    }

    /// The broker-native DLQ mirror: the envelope lands on the DLQ topic and
    /// the poison message itself is still acked off the subscription.
    #[tokio::test]
    async fn dead_letter_publishes_the_envelope_to_the_dlq_topic() {
        let (broker, mut seen) = fake_broker::start(false).await;
        let mut cfg = MqttConfig::new(&broker.url).unwrap();
        cfg.topic = "plant/#".to_string();
        cfg.client_id = "test-dlq".to_string();
        cfg.dlq_topic = Some("dagron/dlq".to_string());
        let mut src = MqttSource::new(cfg);

        let recv = src.recv();
        let feed = publish_after_subscribe(&mut seen, &broker, vec![("plant/line1/jobs", 11, "not: [a spec", false)]);
        let (msg, _) = tokio::join!(timeout(Duration::from_secs(5), recv), feed);
        let msg = msg.expect("delivered").unwrap().unwrap();

        // What the ingest actor does with a poison message: DLQ mirror, then ack.
        src.dead_letter(&msg.payload, "invalid workflow spec: boom").await.unwrap();
        src.ack(&msg.handle).await.unwrap();
        drive(&mut src).await;

        let published = next_where(&mut seen, |e| matches!(e, Seen::Publish { .. })).await;
        let Seen::Publish { topic, payload } = published else { unreachable!() };
        assert_eq!(topic, "dagron/dlq");
        let entry: serde_json::Value = serde_json::from_str(&payload).unwrap();
        assert_eq!(entry["error"], "invalid workflow spec: boom");
        assert_eq!(entry["topic"], "plant/line1/jobs");
        assert_eq!(entry["payload"], "not: [a spec");
        assert_eq!(next_where(&mut seen, |e| matches!(e, Seen::PubAck(_))).await, Seen::PubAck(11));
    }

    /// Without a DLQ topic `dead_letter` is a no-op (the datastore row is the
    /// record), and never an error that would stall the actor.
    #[tokio::test]
    async fn dead_letter_without_a_dlq_topic_is_a_no_op() {
        let (broker, _seen) = fake_broker::start(false).await;
        let mut src = source_for(&broker);
        src.dead_letter("x", "y").await.unwrap();
    }

    /// A refused SUBACK is a loud `recv` error — and the subscribe is retried
    /// on the next `recv`, so fixing the ACL needs no restart.
    #[tokio::test]
    async fn refused_subscription_is_an_error_and_retries() {
        let (broker, mut seen) = fake_broker::start(true).await;
        let mut src = source_for(&broker);

        let err = match timeout(Duration::from_secs(5), src.recv()).await.expect("answered") {
            Ok(_) => panic!("expected an error, got a message"),
            Err(e) => e,
        };
        assert!(err.to_string().contains("refused the subscription"), "{err:#}");
        assert!(matches!(next_where(&mut seen, |e| matches!(e, Seen::Subscribe(_))).await, Seen::Subscribe(_)));

        let err = match timeout(Duration::from_secs(5), src.recv()).await.expect("answered") {
            Ok(_) => panic!("expected an error, got a message"),
            Err(e) => e,
        };
        assert!(err.to_string().contains("refused the subscription"), "{err:#}");
        assert!(matches!(next_where(&mut seen, |e| matches!(e, Seen::Subscribe(_))).await, Seen::Subscribe(_)),
            "subscribed again on the next recv");
    }

    /// A dropped connection is a `recv` error (the actor backs off), and the
    /// next `recv` reconnects, re-subscribes, and delivers.
    #[tokio::test]
    async fn reconnects_and_resubscribes_after_the_broker_drops() {
        let (broker, mut seen) = fake_broker::start(false).await;
        let mut src = source_for(&broker);

        // First session: connect, subscribe, then the broker goes away.
        let recv = src.recv();
        let drop_after_subscribe = async {
            next_where(&mut seen, |e| matches!(e, Seen::Subscribe(_))).await;
            broker.cmd.send(Cmd::Drop).unwrap();
        };
        let (first, _) = tokio::join!(timeout(Duration::from_secs(5), recv), drop_after_subscribe);
        let err = match first.expect("the drop surfaces promptly") {
            Ok(_) => panic!("a dropped connection is an error, not Ok(None)"),
            Err(e) => e,
        };
        assert!(err.to_string().contains("dropped the connection"), "{err:#}");
        assert_eq!(next_where(&mut seen, |e| matches!(e, Seen::Disconnected)).await, Seen::Disconnected);

        // Second session: a new CONNECT, a new SUBSCRIBE, then traffic flows.
        let recv = src.recv();
        let feed = async {
            assert_eq!(next_where(&mut seen, |e| matches!(e, Seen::Connect)).await, Seen::Connect, "reconnected");
            publish_after_subscribe(&mut seen, &broker, vec![("plant/line1/jobs", 2, SPEC_B, false)]).await;
        };
        let (msg, _) = tokio::join!(timeout(Duration::from_secs(5), recv), feed);
        assert_eq!(msg.expect("delivered after reconnect").unwrap().unwrap().payload, SPEC_B);
    }

    /// A broker that is down is a retryable `recv` error — the ingest actor
    /// logs and backs off — never `Ok(None)`, which would stop ingestion for
    /// the life of the process.
    #[tokio::test]
    async fn a_down_broker_is_an_error_not_an_exit() {
        let mut cfg = MqttConfig::new("mqtt://127.0.0.1:1").unwrap();
        cfg.client_id = "test-down".to_string();
        let mut src = MqttSource::new(cfg);
        for _ in 0..2 {
            let res = timeout(Duration::from_secs(10), src.recv()).await.expect("refused promptly");
            let err = match res {
                Ok(_) => panic!("connection refused is Err, not Ok"),
                Err(e) => e,
            };
            assert!(format!("{err:#}").contains("unreachable"), "{err:#}");
        }
    }

    /// Against a real broker: `MQTT_TEST_URL=mqtt://127.0.0.1:1883 cargo test
    /// -p dagron-source --features mqtt,sqlite -- --ignored live_broker`.
    /// Publishes with a second rumqttc client and checks the round trip + ack.
    #[tokio::test]
    #[ignore = "needs a reachable MQTT broker in MQTT_TEST_URL"]
    async fn live_broker_round_trip() {
        let url = std::env::var("MQTT_TEST_URL").expect("MQTT_TEST_URL");
        let topic = format!("dagron/test/{}", uuid::Uuid::new_v4());
        let mut cfg = MqttConfig::new(&url).unwrap();
        cfg.topic = topic.clone();
        cfg.client_id = format!("dagron-test-{}", uuid::Uuid::new_v4());
        cfg.clean_session = true;
        let ep = parse_url(&url).unwrap();
        let mut src = MqttSource::new(cfg);

        // Producer: its own client, event loop driven in the background.
        let mut popts = MqttOptions::new(format!("dagron-pub-{}", uuid::Uuid::new_v4()), ep.host, ep.port);
        popts.set_keep_alive(Duration::from_secs(5));
        let (producer, mut ploop) = AsyncClient::new(popts, 8);
        tokio::spawn(async move {
            loop {
                if ploop.poll().await.is_err() {
                    tokio::time::sleep(Duration::from_millis(200)).await;
                }
            }
        });

        // Subscribe first (drive until the SUBACK has been processed), then publish.
        drive(&mut src).await;
        tokio::time::sleep(Duration::from_millis(300)).await;
        producer.publish(topic.clone(), QoS::AtLeastOnce, false, SPEC_A).await.unwrap();

        let msg = timeout(Duration::from_secs(10), src.recv()).await.expect("delivered").unwrap().unwrap();
        assert_eq!(msg.payload, SPEC_A);
        src.ack(&msg.handle).await.unwrap();
        drive(&mut src).await;
    }
}

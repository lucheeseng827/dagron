# MQTT on the plant floor — a run per message on `SOURCE=mqtt`

A gateway on each production line publishes a **sensor-window job** every time
a batch of readings closes. One dagron engine on the cell PC subscribes to
`plant/+/jobs` and turns every message into a durable run — the aggregation
happens on the machine, and what an operator inspects afterwards is the run
(retries, logs, outcome), not the bus. Nothing here needs more than the dagron
binary, a broker and `mosquitto_pub`. Semantics reference:
[`docs/STREAMING.md`](../../../docs/STREAMING.md) — the MQTT adapter speaks the
same contract as `SOURCE=stream` and the managed broker connectors
(ack-after-durable-run, dead-lettering, exactly-once via a committed position).

| File | What it is |
|---|---|
| [`01_sensor_window.yaml`](01_sensor_window.yaml) | the job a gateway publishes: readings in, `n/min/max/avg` out, idempotent by (line, window) |
| [`publish.sh`](publish.sh) | the gateway stand-in: fills the parameters and publishes to `plant/<line>/jobs`, QoS 1 |

## One-time setup

```bash
# The MQTT client is not linked into a stock build — opt in:
cargo build --release --features mqtt              # → ./target/release/dagron
cd examples/edge/mqtt
export DAGRON=../../../target/release/dagron

# A broker. Any MQTT 3.1.1 broker works; Mosquitto is the usual one:
docker run -d --name mosquitto -p 1883:1883 eclipse-mosquitto:2 \
  sh -c 'printf "listener 1883\nallow_anonymous true\n" > /mosquitto/config/mosquitto.conf && mosquitto -c /mosquitto/config/mosquitto.conf'
# …and the CLI publish.sh uses: apt install mosquitto-clients  (brew/apk: mosquitto)

$DAGRON validate .                                   # both specs parse offline
```

Every walkthrough uses two terminals: **T1** runs the engine subscribed to the
broker, **T2** publishes as the gateway would.

## 01 — A window per message

```bash
# T1 — engine on the cell PC, subscribed to every line's job topic:
SOURCE=mqtt MQTT_URL=mqtt://127.0.0.1:1883 MQTT_TOPIC='plant/+/jobs' \
  MQTT_CLIENT_ID=cell-7 API_ADDR=127.0.0.1:8787 $DAGRON

# T2 — the gateway closes a batch on line1, then one on line2:
./publish.sh line1
./publish.sh line2

curl -s 'localhost:8787/runs?limit=5' | head -c 600; echo
cat data/sensor_windows.csv                          # line,window_end,n,min,max,avg
```

What the engine logs on the way: `MQTT broker connected`, `MQTT subscription
active`, then `run created from queue` per message. The PUBACK for each
message goes out **after** its run is durably created — kill the engine
(Ctrl-C) between publish and run and restart it with the same
`MQTT_CLIENT_ID`: the broker redelivers the unacked message when the session
resumes (`MQTT_CLEAN_SESSION=false` is the default; a stable client id is what
makes the session survive the restart).

Start the engine **before** the broker is up, or stop the broker while it
runs: the log shows `MQTT broker … unreachable or dropped the connection —
retrying` once a second and ingestion resumes by itself when the broker is
back. A broker outage is never a reason for the engine to exit.

## 02 — Exactly-once with a position field

At-least-once means a redelivery after a crash *can* run the same window
twice (the job above is idempotent by window, so that is harmless here — but
not every job is). Give the engine a monotonic id to commit with the run and
the duplicate is acked and skipped instead:

```bash
# T1 — the id lives at the top level of the payload, under `seq`:
SOURCE=mqtt MQTT_TOPIC='plant/+/jobs' MQTT_CLIENT_ID=cell-7 \
  MQTT_POSITION_FIELD=seq API_ADDR=127.0.0.1:8787 $DAGRON

# T2 — publish seq 41, then 42, then 42 again (a gateway retry):
./publish.sh line1 --seq 41
./publish.sh line1 --seq 42
./publish.sh line1 --seq 42       # → "duplicate MQTT message … acked and skipped"
curl -s 'localhost:8787/runs?limit=5' | grep -c sensor_window   # 2, not 3
```

The cursor is per topic (`source_offsets`, keyed `mqtt/plant/line1/jobs`) and
commits in the **same transaction** as the run, so it survives a restart: the
engine reads it back the first time it sees the topic. Numeric ids compare
numerically (anything at or below the cursor is a duplicate — a replayed
backlog is skipped wholesale); other ids compare by equality (a CloudEvents
`id` catches the immediate redelivery, which is the crash window that
matters). **Sparkplug B's `seq` wraps at 256** — pair it with a
session-scoped topic or use a non-wrapping id.

## 03 — Poison messages and the broker-side dead-letter mirror

```bash
# T1 — mirror dead letters to a topic outside the subscription:
SOURCE=mqtt MQTT_TOPIC='plant/+/jobs' MQTT_DLQ_TOPIC=plant-dlq/jobs \
  API_ADDR=127.0.0.1:8787 $DAGRON
# T2 — watch the mirror, then publish something that is not a spec:
mosquitto_sub -t 'plant-dlq/#' -v &
./publish.sh line1 --poison

curl -s localhost:8787/dead-letters | head -c 400; echo    # the durable row
# the mirror carries {"error","topic","payload"}; redrive by re-publishing the payload:
mosquitto_sub -C 1 -t 'plant-dlq/jobs' | jq -r .payload | mosquitto_pub -t plant/line1/jobs -q 1 -s
```

The datastore row is the source of truth (inspect / redrive / delete via API
and console — [`docs/DEAD_LETTERS.md`](../../../docs/DEAD_LETTERS.md)); the
topic is for whatever already watches dead letters on the bus. A DLQ topic
that the subscription filter would match is refused at startup — the engine
would otherwise consume its own dead letters forever.

## Knobs that matter on a real floor

| Setting | Why |
|---|---|
| `MQTT_CLIENT_ID=<stable>` | the default `dagron-<uuid>` changes per boot, so the broker cannot resume the session; set one per engine |
| `MQTT_QOS=1` (default) | `0` is fire-and-forget (a crash loses the message); `2` adds the broker handshake, still one run per message |
| `mqtts://broker:8883` | TLS with the platform root store; credentials via `MQTT_USERNAME` / `MQTT_PASSWORD`, never in the URL |
| `MQTT_KEEPALIVE_SECS` | keep it above the longest admission pause: while the engine sits at `MAX_INFLIGHT_RUNS` it does not poll the broker, and a session that outlives the keepalive is dropped and resumed (redelivering unacked messages — at-least-once at work) |
| publish **without** `retain` | a retained spec is redelivered on every (re)subscribe; the engine warns when it sees one |
| `MQTT_TOPIC='plant/+/jobs'` | one engine per cell subscribes to every line; per-topic cursors keep exactly-once per line |

Full knob table: [`docs/CONFIG.md`](../../../docs/CONFIG.md) (`MQTT_*`).

---

**Beyond one machine.** The adapter is the on-ramp; managing it across two
hundred cells — credential rotation per unit, topic and position-field config
pushed as a signed bundle, dead-letter rollups per unit — is the fleet plane,
which is [not in this build](../../../README.md#what-this-build-does-not-do)
behind the same `SourceFactory` seam.

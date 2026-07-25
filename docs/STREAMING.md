# Streaming ingestion — events in, workflows out

dagron treats streaming as **micro-batch, exactly-once-shaped orchestration**:
an event lands on a stream, and a durable DAG run — retries, fan-out,
dead-lettering, observability included — processes it. It is not a
sub-millisecond stream processor (that lane belongs to Flink/Ray, which dagron
happily schedules *around*); it is the layer that turns streamed events into
**reliable, inspectable work**, with the same operational surface as the batch
jobs next to it.

```text
producers                     dagron                                sinks
─────────           ────────────────────────────                 ─────────
app logs ─┐                                                   ┌─ database
CDC feed ─┼─▶ stream ─▶ IngestActor ─▶ run per event ─▶ tasks ─┼─ webhook
queue    ─┘   (source)   │ backpressure   │ retries/backoff    └─ next queue
                         └ dead-letters   └ offset committed after the
                                            run is durably created
```

## What ships in this build (open source)

| Capability | Where |
|---|---|
| `SOURCE=stream` — follow an NDJSON event file / named pipe, one workflow per line | `dagron-source` [`stream.rs`](../crates/dagron-source/src/stream.rs) |
| **Exactly-once run creation** — a source's cursor commits **in the same datastore transaction** as the run (or dead letter) it accounts for (`source_offsets`); on restart the source is repositioned past everything already accounted for, so a full crash-replay creates zero duplicates. To replay deliberately, delete the `source_offsets` row (the authoritative cursor); the `.offset` file is only a shell-tooling mirror and clearing it alone does nothing. | `db::create_run_with_offset` + the `pending_position` / `set_committed_position` seam on `WorkflowSource` |
| Drain mode (`STREAM_FOLLOW=false`) — process a backlog then exit: batch replay of the same stream | same |
| **Multi-consumer sharding** — point `STREAM_PATH` at a *directory* of NDJSON shards and N engines split them via **per-partition range leases** (one consumer per shard, heartbeat-renewed, rebalanced when a consumer dies), each shard with its own exactly-once cursor. The broker-free consumer group; the same lease primitives (`claim/renew/release_source_partitions`) back partitioned broker/CDC connectors. | `ShardedStreamSource` + `dagron-core` partition-lease fns |
| Poison-event **dead-lettering** — a durable `dead_letters` row + a `.dlq` NDJSON mirror; inspect / redrive / delete via API & UI | `dagron-source` ingest + [`docs/DLQ.md`](DLQ.md) |
| Admission backpressure — `MAX_INFLIGHT_RUNS` holds intake while the engine drains; the file buffers the burst | ingest actor |
| Micro-batch schedules + windowed **backfill/catch-up** (`{{ scheduled_time }}`) — the batching half of streaming | [`docs/BACKFILL_USECASES.md`](BACKFILL_USECASES.md) |
| Long-lived consumer tasks — the lease **heartbeat** lets a task run for hours (see [AI_WORKLOADS.md](AI_WORKLOADS.md); the same primitive powers both) | engine + executor |
| Exactly-once side effects via the transactional **outbox** (`notify.*` events, SSE) | `dagron-core` outbox |
| The `SourceFactory` seam — plug any custom source into the engine without forking it | [`source.rs`](../crates/dagron-source/src/source.rs) |

### Quick start: stream events into workflows

Each line of the stream is one workflow submission — JSON is a subset of YAML,
so a one-line JSON spec is valid as-is. Most real setups keep the spec tiny and
delegate to a saved workflow with
[`workflow_ref`](HOWTO.md#3-one-workflow-triggering-another-workflow_ref):

```bash
# 1. create the stream file, then start the engine on it (mode is
#    inspected at startup, so the path must exist — see STREAM_MODE)
touch events.ndjson
SOURCE=stream STREAM_PATH=./events.ndjson API_ADDR=127.0.0.1:8787 \
  ./target/release/dagron ./events.ndjson &

# 2. produce events (anything that appends lines works)
echo '{"name":"handle_order","parameters":{"order_id":"o-1001"},"tasks":[{"name":"process","command":["sh","-c","echo processing {{ order_id }}"]}]}' >> events.ndjson

# 3. watch runs appear
curl -s localhost:8787/runs | head
```

Delivery semantics: **exactly-once run creation.** The line's byte offset is
committed in the *same datastore transaction* as the run it becomes, and the
ingest actor hands the committed cursor back to the source at startup — so a
crash at any point (before ack, mid-ack, after ack) replays the file without
re-creating a run. The `<STREAM_PATH>.offset` file remains as a trailing
mirror for shell tooling. A line that can't parse is dead-lettered (DB row +
`.dlq` file) *with the same transactional cursor advance*, so one poison
event never wedges or re-parks. Config knobs: [`docs/CONFIG.md`](CONFIG.md)
(`STREAM_*`).

Producing from real systems is one pipe away:

```bash
mkfifo events.pipe                          # FIFO variant: no file growth
kafkacat -C -b broker -t orders -u > events.pipe   # bridge a topic…
psql -c "COPY (…) TO STDOUT" >> events.ndjson       # …or a query, or `tail -f app.log | jq -c …`
```

### Runnable case studies

Five end-to-end examples (each with its own producer + README) live in
[`examples/streaming/`](../examples/streaming/): clickstream micro-batches,
change-data-capture replication, log anomaly alerting with a dead-letter
redrive, sensor micro-batch windows with backfill, and kill-safe exactly-once
payment processing.

## The managed connector suite (dagron Enterprise)

The open build's `SOURCE=stream` and the seam are the full programming model —
what **dagron Enterprise** adds is the *managed connector fleet* on the same
trait, for teams whose events already live on brokers:

- **Broker connectors** — Kafka (manual offset commit on ack), NATS JetStream
  (durable pull consumers), SQS (visibility-timeout leases), Redis (reliable
  list), each with broker-native dead-letter routing (DLT topics, DLQ queues)
  and per-connector batching/backpressure knobs.
- **CloudEvents webhook gateway** (`SOURCE=events`) — signed HTTP ingress with
  idempotency de-duplication and declarative trigger rules mapping event types
  to workflows.
- **The streams data plane** — a per-pipeline worker that moves and reshapes
  records broker→broker (transform chains, live repartitioning, publisher
  confirms) around the workflows the engine runs.
- **Native change-data-capture** — a Postgres logical-replication connector
  (`SOURCE=postgres-cdc`): change batches become workflow runs with the batch
  LSN committed on the same exactly-once substrate as everything above, and
  the replication slot advanced only after the work is durable — plus a
  Debezium-envelope adapter (`SOURCE=debezium` over a broker connector) that
  covers MySQL, MongoDB, SQL Server, and more; a broker-free native MySQL
  binlog client follows on the same track.

Selecting a managed connector kind (`SOURCE=kafka|nats|sqs|redis|events`) in
this build prints exactly where the line is — see
[README → dagron Enterprise](../README.md#dagron-enterprise). Everything
upstream of the connector (the trait, the ingest actor, delivery semantics,
dead-lettering, backpressure) is the open code on this page, so a pipeline
proven on `SOURCE=stream` moves to Kafka by changing environment variables,
not workflows.

# Marketing workflows — twenty pipelines a HubSpot team can actually run

A demand-gen team's whole operating system, written as dagron DAGs: capture,
enrich, route, launch, measure, retire. Twenty workflows that fit together —
01 produces the dataset 02 waits on, 04 calls four publishers as child runs, 08
dispatches 07, 11 loops a child once per nurture step — and between them touch
every capability the engine has.

Every file here is **validated**, not illustrative:

```sh
dagron validate examples/marketing/           # offline lint: no DB, no server
dagron-plan examples/marketing/01_lead_capture_multisource.yaml \
            examples/marketing/02_lead_enrichment_scoring.yaml
```

The commands inside are the real shape of the work (`curl` against the HubSpot
API, `aws s3 cp`, `snowsql`, `dbt run`, `dagron-step-mcp`) with the team's own
helper binaries where a one-liner would lie about the complexity. Point them at
your own tooling; the graph is the part worth copying.

---

## The twenty

| # | Workflow | What it does | The dagron feature it is really about |
|---|---|---|---|
| 01 | [`lead_capture_multisource`](01_lead_capture_multisource.yaml) | Five lead sources → one HubSpot upsert | Labelled fan-out (`instance_key`), `allow_failure` + `all_done` join, `produces:` |
| 02 | [`lead_enrichment_scoring`](02_lead_enrichment_scoring.yaml) | Enrich, score, write back ICP fit | **`on_datasets:` trigger** — runs *because* 01 landed data, plus `cache:` and `pool:` |
| 03 | [`speed_to_lead_router`](03_speed_to_lead_router.yaml) | Route an inbound lead in under five minutes | **`result_from:`** — a durable function the form webhook calls with `?wait=true`; soft `deadline:` |
| 04 | [`campaign_launch_gate`](04_campaign_launch_gate.yaml) | Launch four channels behind one sign-off | **Dag-of-dags** (`type: workflow`), `type: approval` with `approval_on_timeout: reject`, HTTP sensor |
| 05 | [`paid_media_cost_sync`](05_paid_media_cost_sync.yaml) | Ad spend → S3 → Snowflake → HubSpot | **`{{ scheduled_time }}` + backfill** — restate any past day; capped backoff |
| 06 | [`attribution_model_refresh`](06_attribution_model_refresh.yaml) | Multi-touch attribution model | **Spot GPU + checkpoint resume**, `runner_class`, `resources.gpu`, `retry_budgets` by fault class |
| 07 | [`content_engine_llm`](07_content_engine_llm.yaml) | Brief → Claude draft → editor → publish | **Durable LLM step** — the artifact is its own idempotency check; approval gate |
| 08 | [`seo_content_refresh`](08_seo_content_refresh.yaml) | Find decaying pages, rewrite the worthwhile ones | **`with_param` dynamic width** + nested templates + runtime child-run dispatch |
| 09 | [`webinar_to_pipeline`](09_webinar_to_pipeline.yaml) | The four days after an event | **All four sensor kinds** (`for` / `until` / `url` / and `repeat` polling) — parked tasks hold no worker slot |
| 10 | [`abm_account_orchestration`](10_abm_account_orchestration.yaml) | Intent → tiering → coordinated account plays | **Nested templates** (`touch` → `account_play` → `tier1_play`), parameter-driven `when:` |
| 11 | [`lifecycle_nurture_loop`](11_lifecycle_nurture_loop.yaml) | An 8-step nurture track | **`repeat:` over a child workflow** — one run per turn, bounded three ways |
| 12 | [`crm_hygiene_dedupe`](12_crm_hygiene_dedupe.yaml) | Normalise, dedupe, merge | **Safe destructive automation** — `dry_run` parameter branch, approval, negative `priority` |
| 13 | [`reverse_etl_audience_sync`](13_reverse_etl_audience_sync.yaml) | Warehouse segments → six activation destinations | **`max_active_runs: 1`** + delta + `cache:` — the reverse-ETL race condition, prevented |
| 14 | [`realtime_intent_event`](14_realtime_intent_event.yaml) | One high-intent event, one run | **`SOURCE=stream`** — exactly-once NDJSON submissions, dead letters, `priority: 200` |
| 15 | [`form_abuse_guard`](15_form_abuse_guard.yaml) | Keep bots out of the CRM | **`retry_on_timeout: false`** and degrade-don't-block risk checks |
| 16 | [`email_send_readiness`](16_email_send_readiness.yaml) | Everything before hitting Send | **`notify.git`** — the readiness run is a commit status on the PR; bounded `repeat:` poll |
| 17 | [`weekly_funnel_report`](17_weekly_funnel_report.yaml) | Monday funnel report, restatable | **Reproducible reporting** — `{{ scheduled_time }}` everywhere, data QA as a task |
| 18 | [`churn_winback_playbook`](18_churn_winback_playbook.yaml) | Retention's marketing half | **`environment:`** — one line switches staging/prod variables *and* secrets; dataset sensor |
| 19 | [`competitor_intel_agent`](19_competitor_intel_agent.yaml) | Competitive digest + battlecard PR | **`dagron-step-mcp`** — an agent's tool calls as tasks, with retries and artifacts |
| 20 | [`campaign_teardown_reconcile`](20_campaign_teardown_reconcile.yaml) | Switch a campaign off properly | **GitOps + FinOps** — `dagron validate`/`dagron-plan` in CI, 48h wait sensor, approval before irreversible |

Supporting files: [`cron.yaml.example`](cron.yaml.example) (six schedules — the other fourteen
are event-, API- or parent-triggered) and [`env.example`](env.example)
(pools, secrets, artifact store, executor).

---

## Run one

```sh
# Lint everything first — no database, no server, CI-friendly
dagron validate examples/marketing/

# Then run one, with the pack's environment
set -a; . ./.env.marketing; set +a
dagron examples/marketing/01_lead_capture_multisource.yaml

# Or the whole stack with the console, scheduler and pools
CRON_CONFIG=examples/marketing/cron.yaml.example \
POOLS=crm-api=8,ads-api=4,enrich-api=4,activation-api=6,seo-api=2 \
API_ADDR=127.0.0.1:8787 dagron           # → http://127.0.0.1:8787
```

Two of them are not submitted by hand at all. 14 and 15 arrive per-event on the
stream:

```sh
kcat -C -t marketing.intent -o end -e | intent-to-dagron >> events.ndjson
SOURCE=stream STREAM_PATH=./events.ndjson dagron ./events.ndjson
```

And 03 is called synchronously by a HubSpot webhook, which is what
`result_from:` is for:

```sh
curl -s -X POST 'http://127.0.0.1:8787/runs?wait=true&timeout_secs=30' \
     --data-binary @examples/marketing/03_speed_to_lead_router.yaml
# {"status":"succeeded","result":"owner=ae-1@acme.com tier=enterprise"}
```

---

## Data sources these touch

| Layer | Systems |
|---|---|
| CRM & marketing automation | HubSpot (contacts, companies, deals, lists, forms, tasks, CMS, marketing emails, campaigns), Salesforce |
| Advertising | Google Ads, Meta, LinkedIn, Reddit, Bing; Meta Ad Library; Google/Meta/LinkedIn custom audiences |
| Web & search | GA4, Google Search Console, Ahrefs, SERP scraping, Webflow/HubSpot CMS |
| Intent & enrichment | 6sense, Demandbase, G2 buyer intent, Clearbit, ZoomInfo |
| Events & product | Zoom webinars, Segment/Kafka event stream, Amplitude product usage |
| Revenue & support | Payments (subscriptions, invoices), Zendesk, Intercom, Customer.io |
| Deliverability & QA | Litmus, GlockApps seed testing, SpamAssassin, ZeroBounce, IPQualityScore, reCAPTCHA |
| Warehouse & lake | Snowflake, dbt, S3 (raw / curated / archive), Parquet + NDJSON |
| AI | Anthropic Messages API (07, 19), MCP servers via `dagron-step-mcp` (19) |

## Cloud resources they assume

- **S3** for the data lake, the artifact store (`DAGRON_ARTIFACT_URL`), campaign
  archives with lifecycle tagging to Glacier, and CRM snapshots (12's undo).
- **Snowflake** as the warehouse, with idempotent partition-replace loads (05).
- **Kubernetes** (`EXECUTOR=kubernetes`) for the container tasks, with
  `runner_class` mapping to node pools: `spot-gpu`, `ondemand-gpu`, `cpu`.
- **GPUs on spot** for 06 — preemption is a retry that resumes from the last
  checkpoint, which is the only way three-hour training on spot makes sense.
- **Postgres** as the engine's state store for multi-node HA (SQLite for a
  laptop; the workflows are identical either way).
- **Kafka / Segment** feeding an NDJSON stream for 14 and 15.
- **Secrets** through the `value_from` seam — a UI-managed environment, SOPS,
  External Secrets, or plain `DAGRON_SECRET_*`.

---

## Reading the pack as an argument

A few things recur, and they are the reason this is twenty DAGs and not twenty
HubSpot workflows plus a Zapier account and a notebook.

**Dependencies are declared once, in the graph.** 01 says `produces:
hubspot://crm/contacts`; 02 says `on_datasets: [hubspot://crm/contacts]`. Nobody
maintains a cron offset, and nothing runs on stale data because someone's
extract got slower. The lineage ledger answers "what produced this, and when"
without a meeting.

**Failure is a first-class outcome, not an exception.** Vendors go down: intent
providers (10), enrichment (02), ad platforms (13), deliverability tools (16).
Every one of those is `allow_failure` with an `all_done` join, so the pipeline
degrades instead of stopping — and where degradation is *not* acceptable (04's
preflight, 01's validation), the join is `all_success` on purpose. That
distinction is written down in the YAML, which is where an on-call marketer can
find it.

**The irreversible steps have humans in front of them.** Four workflows have a
`type: approval` gate, and all four use `approval_on_timeout: reject`. A merge
of 40,000 contacts, a campaign launch, a marketing send, a decommission — none
of them should default to *yes* because it is 2am and nobody clicked.

**Money is bounded in the file.** `pool:` caps API spend, `cache:` stops paying
twice for the same enrichment or the same model call, `budget: { tasks: N }`
caps what one submission can create, `max_iterations` caps how many emails a
loop can ever send, and `retry_max_delay_secs` stops exponential backoff from
turning a blip into an outage.

**Everything is reproducible.** `{{ scheduled_time }}` rather than `now` means a
backfill regenerates history as it was, which is what makes a marketing number
defensible three months later.

## Where to go next

- [`docs/HOWTO.md`](../../docs/HOWTO.md) — copy-paste recipes: submit via CLI and
  REST, chain workflows, monitor runs, wire secrets.
- [`docs/DATASETS.md`](../../docs/DATASETS.md) — the produce → track → trigger loop
  used by 01/02, 06 and 18.
- [`docs/CONFIG.md`](../../docs/CONFIG.md) — every knob, including the per-task
  fields these files use.
- [`docs/STREAMING.md`](../../docs/STREAMING.md) — the exactly-once source behind
  14 and 15.
- [`docs/MCP.md`](../../docs/MCP.md) — both directions: agents driving dagron, and
  19's DAG driving an agent's tools.
- [`examples/personas/`](../personas/) — the same idea for data, ML and DevOps
  engineers, runnable with no credentials at all.

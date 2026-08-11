# Changelog

All notable changes to this project are documented here. The format is based on
[Keep a Changelog](https://keepachangelog.com/), and this project adheres to
[Semantic Versioning](https://semver.org/) (pre-1.0: minor = breaking).

## [Unreleased]

## [0.7.0] - 2026-08-12

Minor, not patch: task pods created by the Kubernetes executor no longer receive
a ServiceAccount token unless they asked for one. That is a changed default, and
pre-1.0 this project treats a breaking change as a minor bump (see the header
above). Everything else here is additive or a fix.

### Fixed
- **`dagron-api` and `dagron-gitops` could not connect to any Postgres that
  requires TLS.** `sqlx` is declared `default-features = false`, and neither
  crate enabled a TLS backend — so an `sslmode=require` DSN failed at connect
  with *"TLS upgrade required by connect options but SQLx was built without TLS
  support enabled"*. That is not an edge case: it rules out Amazon RDS (which
  forces SSL by default), Cloud SQL, Azure Database, and any hardened
  self-hosted instance. The engine's `postgres` feature had the same gap. All
  three now enable `tls-rustls-ring`, the stack the rest of the workspace
  already resolves, so no second TLS implementation enters the build.

  If you run Postgres without TLS, nothing changes.

- **`POST /runs` bypassed admission control.** `MAX_INFLIGHT_RUNS` was enforced
  on the ingestion path but not on the engine's own HTTP API, so the route most
  likely to be scripted against was the one route with no valve on it.

### Added
- **A second admission dimension, counted in tasks.** Runs are the wrong unit on
  their own: a run of 100,000 tasks and a run of four both count as one against
  `MAX_INFLIGHT_RUNS`, so a fleet sitting comfortably under the run cap can be
  far past what the scheduler and the datastore can carry. `MAX_INFLIGHT_TASKS`
  (default `0`, off) caps the tasks in `pending`/`ready`/`running`. A task parked
  on an approval is not pressure and is excluded. The cap counts the rows a
  submission will actually insert — a `gang:` task is one spec but `size` rows,
  so a run of ten 64-member gangs is admitted as 640, not as 10. Only queried
  when set, so the default path pays no extra round trip.

- **`DAGRON_MAX_TASKS_PER_RUN`** — a per-run ceiling on the expanded graph.
  `expand()` materialises the whole graph in memory before anything reaches the
  database, so one runaway fan-out costs far more than the engine's idle
  footprint. May only *lower* the compiled ceiling of 100,000; a larger value is
  ignored, because the knob exists to tighten a bound that prevents an OOM.

- **Security context for Kubernetes task pods.** Task pods were built with no
  `securityContext` at all — no user, no capability drop, no seccomp profile, no
  deadline, no placement — which meant a task image whose final layer is
  `USER root` ran as root, and a hung task held a node slot until the run-level
  timeout noticed. Each control is opt-in and off by default, because turning
  them on wholesale would break images that legitimately need what they remove:
  `DAGRON_TASK_RUN_AS_USER`, `DAGRON_TASK_READ_ONLY_ROOT_FS`,
  `DAGRON_TASK_DROP_ALL_CAPABILITIES`, `DAGRON_TASK_SECCOMP_RUNTIME_DEFAULT`,
  `DAGRON_TASK_ACTIVE_DEADLINE_SECS`, `DAGRON_TASK_RUNTIME_CLASS` (e.g. `gvisor`,
  for images you do not control) and `DAGRON_TASK_NODE_SELECTOR`.

- **Chart: node placement for the engine and the API.** `engine.nodeSelector` /
  `tolerations` / `affinity` and the same three under `dagronApi`. With the
  `k8s` executor the engine and the task pods it creates are separate workloads
  with opposite shapes, and an engine that lands among its own task pods
  competes with them for the CPU it needs to keep dispatching.

- **Chart: `externalDatabaseSecret.listenKey`** — an optional second Secret key
  holding the *direct* connection string, for deployments whose main DSN points
  at a transaction pooler. Transaction pooling cannot serve a session-scoped
  `LISTEN`, which the reconcile loop's wake depends on, so that one connection
  needs a non-pooled endpoint of its own.

### Changed
- **Task pods no longer get a ServiceAccount token by default.** A task that did
  not declare `service_account:` never asked for an identity, and on a cluster
  using IAM-for-ServiceAccounts the token it was being handed is a cloud
  credential handed to arbitrary task code. Tasks that *do* declare
  `service_account:` are unaffected and keep their token. Set
  `DAGRON_TASK_AUTOMOUNT_SA_TOKEN=1` to restore the previous behaviour.

## [0.6.0] - 2026-08-09

Minor, not patch: `DagronClient::from_env` now returns `Result<Self>`, and
pre-1.0 this project treats a breaking change as a minor bump (see the header
above).

### Added
- **GitOps repositories can authenticate — with a token *or* an SSH key, set from
  the console.** Until now the only credential the sync had was the worker-wide
  `DAGRON_GIT_TOKEN`: one token for every repository, changeable only by
  redeploying the container, sent only to three hard-coded forge hosts, and
  HTTPS-only. Two repos on two accounts could not both be connected, a
  self-managed forge could not be reached at all (`DAGRON_GIT_TRUSTED_HOSTS` was
  documented but read by nothing), and a forge that offers SSH but not HTTPS
  clone was simply out of reach. The console offered no way to enter a credential
  of any kind.

  Each repository now carries its own optional credential — an **HTTPS token** or
  an **SSH private key** — set on the GitOps page or through
  `PUT /api/git-repos/{id}/auth`, and removed with `DELETE`. It is stored
  AES-256-GCM encrypted by the same `DAGRON_ENV_SECRET_KEY` / KEK machinery as
  environment secrets, decrypted only by the `dagron-gitops` worker, and is never
  readable back: the API returns a kind, a username and a hint (`••••cdef`, or
  `ssh-ed25519 SHA256:…` — the same fingerprint the forge prints beside the
  deploy key), never the value.

  Consequences worth knowing:

  - **SSH URLs are accepted now.** `ssh://git@host/owner/repo` and the scp-style
    `git@host:owner/repo` both connect; the `git@` that made them fail validation
    was being read as an embedded credential, which on SSH it is not. An `https://`
    URL still may not carry userinfo — there it *is* the credential, and storing
    it would write a token into `git_repos.url` in cleartext.
  - **Neither secret reaches the worker's command line.** The token goes to a
    0600 `credential.helper` file and the key to a 0600 file named by
    `GIT_SSH_COMMAND`, both in a scratch directory removed when the sync ends.
    The previous `https://x-access-token:TOKEN@host/…` clone URL put the token in
    argv, which `/proc/<pid>/cmdline` hands to anything in the container.
  - **The mismatch is refused at write time.** A token against an SSH repo, or a
    key against HTTPS, is a `400` — not a credential the console reports as
    installed while every sync fails "permission denied". Public keys pasted into
    the private-key box and passphrase-protected keys are rejected for the same
    reason: the worker has no terminal to be prompted at.
  - **`DAGRON_GIT_SSH_STRICT=1`** refuses an SSH sync for a repo with no pinned
    `known_hosts` rather than trusting whatever host answers. Off by default; the
    console says "host key unverified" on repos in that state either way.
  - **`DAGRON_GIT_TRUSTED_HOSTS` now works.** It has been in `docs/CONFIG.md`
    since the feature shipped and was read by nothing, so a token for a
    self-managed forge was silently dropped and the clone failed as "repository
    not found". It still applies only to the worker-wide token: a per-repo
    credential is not host-filtered, because attaching it to that repository is
    the operator saying where it may go.

  The `dagron-gitops` image gains `openssh-client` (git execs `ssh`; without it
  every SSH repo fails with "ssh: not found") and needs `DAGRON_ENV_SECRET_KEY`
  set to dagron-api's value — compose and the Helm chart wire it through. A repo
  with no credential behaves exactly as before.

- **`DAGRON_MCP_TOKEN` will no longer cross the network in the clear.** A token
  sent to a remote `DAGRON_API_URL` over plaintext `http://` is readable by
  anything on the path, so the server now **refuses to start** instead. The error
  names the three ways out — use `https://`, point at loopback, or set
  `DAGRON_MCP_ALLOW_PLAINTEXT_TOKEN=1` — and names the host, and *only* the host,
  since a base URL may carry `user:password@` and reporting a leaked credential
  by printing another one would be its own disclosure.

  This started as a warning, which was the wrong call: a process cannot verify
  that a mesh is protecting the connection, and a log line nobody reads doesn't
  stop a misconfigured deployment from shipping the token in cleartext. The
  opt-out keeps the legitimate case — plaintext to an in-cluster Service,
  transport secured below us — fully supported while making that exception
  explicit and auditable rather than silent.

  Loopback is decided by parsing the host as an `IpAddr` and asking
  `is_loopback()`, so the whole `127/8` block and `::1` need no opt-out, while an
  ordinary remote name like `127.0.0.1.example.com` gets no free pass.

  **Breaking:** `DagronClient::from_env` now returns `Result<Self>`.

### Changed
- **The enterprise audit trail is no longer published.** `routes/audit.rs` shipped
  the whole feature — the `audit_log` DDL, the recording middleware, the `viewer`
  read-only denial and the `/api/audit` read — into the public mirror behind
  `#[cfg(feature = "enterprise")]`, compiled off but fully readable. The 230 gated
  lines move to `routes/audit_ee.rs`, which the sync manifest excludes the way it
  already excludes `migrations_ee`; the open tree keeps the passthrough middleware
  and every call site in `main.rs` is spelled the same in both builds.

  It is `include!`d rather than declared as a `mod`, and that is the whole trick:
  rustfmt resolves every `mod` declaration regardless of `cfg`, so the module form
  makes `cargo fmt` fail outright in a checkout where the file is absent — exactly
  the mirror. An inactive `include!` it leaves alone, and `cargo build` / `cargo
  test` are happy either way.

- **The mirror stops naming the repository it was cut from.** A sweep of all 457
  staged files found the private monorepo's layout and vocabulary spread through
  it: the CI job name in `dagron-plan/Cargo.toml`, the closed crate path in
  `dagron-mcp/README.md`, an internal load-test note cited from five code
  comments, the sync manifest named in `docker.yml`, and 25 references to
  documents that are not published — every one a dead link or a private filename
  in a public repo. All reworded.

  Two of those documents were the walkthrough a *published* example pointed at, so
  the example shipped without the page explaining it: `BACKFILL_USECASES.md` and
  `WORKFLOW_UI_GUIDE.md` are now mirrored (the latter after dropping the monorepo
  path from its header and its quickstart). The Argo CD walkthrough is not, and
  cannot be as written — every example in it points `repoURL` at the private
  clone URL, the same reason the AWS backup runbook stays unpublished.

  Two guards were added for the class of mistake that nearly shipped it: the marker
  set had nothing matching the private repository's own name, or a path inside it.
  Both now fail the pre-publish scan. (The patterns are spelled out in the sync
  manifest, not here — a changelog that quotes them would trip them.)

- **Marketing framing out of the published docs.** `DATASETS.md` carried a section
  of go-to-market reasoning written for us, not the reader — "top-of-funnel",
  "the user learns the edition line at the moment they feel the need", and a
  closing note that a GitHub issue quoting an error "*is* a qualified lead". It is
  replaced by "Limits of the open build", which keeps every gate and the workaround
  for it and drops the funnel. `audit.rs` no longer calls the feature "paid-tier",
  and `README.md` says "managed fleet" where it said "commercial fleet". The
  capability tables all stay: the engine's own signpost errors point at them.

- **The private-key tripwire stopped firing on source that merely names one.** The
  pre-publish scan matched a bare `BEGIN … PRIVATE KEY`, which the new GitOps
  credential form trips legitimately — the textarea placeholder and the key
  validation tests both contain the literal header an operator has to recognise.
  It is now two rules that match key *material*: the header alone on a line (any
  real `.pem`, including an indented YAML block scalar) and the base64 of the
  `openssh-key-v1` container header, which is context-free and so still catches a
  key pasted into a source literal — the one shape the first rule gives up.

### Fixed
- **`MAX_INFLIGHT_RUNS=0` disables the admission cap, as the chart has always
  said it does.** The chart, its `values.yaml` and `docs/CONFIG.md` all documented
  `0` as "no cap", and `POST /runs` implemented exactly that (`max_inflight_runs
  > 0` before it checks). The env parse in between clamped with `.max(1)`, so `0`
  never reached either consumer: an operator who disabled the cap got a cap of
  **one active run** instead — a near-total stall, and the opposite of what they
  asked for.

  The clamp is now `.max(0)`, and the ingest actor's throttle grew the same
  `> 0` guard the API path already had — without it a `0` that finally reaches it
  compares `active >= 0`, which is always true, and the actor would throttle
  forever while admitting nothing. Negative values normalize to `0` rather than
  meaning some third thing. Both sites are pinned by unit tests
  (`max_inflight_runs_zero_disables_the_cap`, `zero_cap_never_closes_the_valve`),
  because the previous contract was documented in three places and enforced in
  none.

- **The MCP `dagron_submit_run` tool now actually submits.** It posted the spec
  as a raw body with `content-type: application/yaml`. `POST /api/runs` binds
  `Json<SubmitBody>` — an envelope, `{"yaml": "…"}` — and axum's `Json` extractor
  refuses any request that isn't `application/json` with **415 before the handler
  runs**. So the one tool an agent needs most, hand dagron a workflow, could not
  have worked against `dagron-api` in any deployment: every submit came back as an
  unsupported-media-type error the model then reported as a tool failure.

  It sends the JSON envelope now. Worth noting *why* it survived: the crate's
  tests covered the tool catalogue and the argument validation thoroughly, and
  nothing asserted what went on the wire — a shape only visible at the HTTP
  boundary. `submit_posts_the_spec_as_json` closes that by serving one request
  from an in-crate listener and asserting the method, the content type and the
  body, so the next drift of this kind fails a test rather than a demo.

  `DagronClient::get`/`post` are also public now, so a composing server can add
  tools without re-implementing base-URL and auth handling.

## [0.5.2] - 2026-07-28

### Fixed
- **`dagron-gitops` no longer carries perl.** The image scanned with five open
  advisories — CVE-2026-57433, CVE-2026-8376, CVE-2026-13221 and CVE-2026-42496
  against perl, plus CVE-2023-45853 against `zlib1g` — reported as 17 rows
  because each perl advisory appears once per installed package (`perl`,
  `perl-base`, `libperl5.36`, `perl-modules-5.36`).

  Read them more carefully than the scanner's colour, and two things matter.
  Debian triages several as `no-dsa` — "Minor issue; can be fixed in point
  release" — which is *why* the "fixed in" column is empty: a deliberate
  deprioritisation rather than an impossibility. And CVE-2026-8376 affects only
  32-bit builds, so it never applied to the amd64 and arm64 images published
  here.

  So the case is not "the image was critically vulnerable". It is that perl was
  never wanted in the first place — Debian's `git` hard-`Depends` on it, so
  `--no-install-recommends` could not exclude it, and `debian:bookworm-slim`
  ships `perl-base` before anything is installed at all — and an unneeded
  dependency carrying open unfixed advisories is worth removing when the cost is
  a base-image change. The runtime moves to Alpine, whose `git` depends only on
  musl, libcurl, libexpat, libpcre2 and libz. Verified in the built image: zero
  perl packages, no perl binary, zlib 1.3.2, and git 2.47.3 still cloning and
  committing as the non-root user. The image is 26.7 MB.

  Cheap to do here only because this crate pulls no OpenSSL: sqlx is
  `default-features = false` with no TLS feature. **`dagron-engine-localdev` is
  still `debian:bookworm-slim` and still carries the `perl-base` and `zlib1g`
  CVEs** — left alone because Alpine would swap its GNU userland for busybox,
  and that image exists so user-authored task commands resolve.

- **Release binaries actually publish.** 0.5.1 announced them and shipped none.
  The `x86_64-apple-darwin` leg targeted `macos-13`, which GitHub has retired,
  and a `runs-on` label matching no runner does not fail — it sits in `queued`
  until the 24-hour ceiling. Because `publish` waits on the whole matrix, four
  successful builds were stranded behind it and the release got zero assets
  rather than four.

### Removed
- **The `x86_64-apple-darwin` binary.** Dropped rather than repointed at another
  Intel image: Apple stopped selling Intel Macs in 2023 and those runners are
  being retired, so repointing buys a target that breaks again on someone
  else's schedule. Remaining: `x86_64`/`aarch64-unknown-linux-musl`,
  `aarch64-apple-darwin`, `x86_64-pc-windows-msvc`. Intel Mac users can build
  from source.

## [0.5.1] - 2026-07-28

### Added

- **Personal access tokens.** Automation had no credential of its own:
  `/api/login` mints a short-lived session token meant for a browser, so a CI
  job had to store a *password* and log in to get one — a secret that cannot be
  revoked without locking the human out and cannot be told apart from that
  human's own traffic.

  `POST /api/tokens` mints a named, optionally-expiring token, `GET` lists them,
  `DELETE` revokes. It goes anywhere the session bearer goes, recognised by its
  `dgp_` prefix, and the console grows an **API tokens** screen. Only
  `sha256(token)` is stored — the plaintext exists once, in the creation
  response, and no endpoint can show it again because nothing is kept that
  could. Two rules worth knowing: a token **cannot manage tokens** (that needs a
  password session, so one leaked credential cannot mint replacements faster
  than you revoke them), and a token carries its owner's permissions read
  *live*, so demoting a user takes effect on their existing tokens at once.
  There is no per-token scoping yet — give automation its own user.
  See [`docs/HOWTO.md`](docs/HOWTO.md).

- **Per-line log timestamps.** Every log line now carries a time. Where a line
  printed its own RFC3339 stamp — a task that logs structured output, or
  Docker/Kubernetes with their `timestamps` option — it is parsed into `ts` and
  removed from the text. Where it did not, which is most output, the console
  falls back to the owning task's start and shows the task's duration alongside,
  so the estimate is judged rather than trusted: a 200 ms task makes it
  millisecond-accurate, a four-hour task makes it nearly useless, and you can
  see which you are looking at.

  Deliberately *not* interpolated across the task's window by line position —
  that would spread lines evenly over a duration they were never evenly spread
  across, inventing precision that reads as recorded fact.

  Splitting the stamp off is also what keeps the filters honest: every predicate
  now runs on the message, so an anchored `regex=^ERROR` still matches once a
  runtime starts prefixing times. The trade is that a term appearing only in the
  timestamp no longer matches, so `exclude=2026-07-26` no longer removes stamped
  lines; time-range filtering belongs on the parsed `ts` and is not built yet.

- **Failure triage.** `status` says what the engine did; nothing said what a
  person then decided, so a failed run stayed failed forever and the overview's
  "needs attention" list could only drain by the failure ageing out — which is
  not triage, it is forgetting.

  `POST /api/runs/{id}/triage` records `acknowledged` (someone is on it),
  `resolved` (dealt with) or `ignored` (a real failure we accept, with a note
  explaining why); `DELETE` puts it back in the queue. Three states rather than
  one flag because they are different answers, and the run page grows a control
  shown only on a failed run. The overview now counts *untriaged* failures, so
  the queue drains because someone acted.

- **Workflow versioning and lifecycle.** Editing a workflow overwrote its spec
  in place — there was no version column and no history, so the previous
  definition was simply gone. `workflow_versions` is now append-only, written on
  create *and* every update, and the console shows the history with a read-only
  view of any stored spec. (Past *runs* were never at risk; `task_runs`
  snapshots its TaskSpec at dispatch.)

  Workflows also gain a state: `active` / `paused` / `retired`. Both non-active
  states refuse to run — enforced in the scheduler *and* on the run endpoint, so
  "paused" stops cron and the button alike — and **neither touches the
  workflow's schedules**. That is the whole point: the only stop button used to
  be `DELETE`, and `schedules.workflow_id` is `ON DELETE CASCADE`, so deleting a
  workflow silently took its cron schedules with it.

- **Dead-letter retry policy from the console.** `DEAD_LETTER_MAX_ATTEMPTS` was
  environment-only and read once at engine start, so changing it meant a
  restart. It is now a stored setting the engine reads on the failure path —
  no restart, and no window where the console shows a number the running
  process is not using. Unset keeps the environment value, so existing
  deployments are unchanged. `STREAM_DLQ_PATH` stays deployment config on
  purpose: it is a filesystem path on the engine's host.
  New guide: [`docs/DEAD_LETTERS.md`](docs/DEAD_LETTERS.md).

- **Release binaries.** A release shipped container images and an OCI chart and
  nothing you could run. `dagron` is now published for
  `x86_64`/`aarch64-unknown-linux-musl`, `x86_64`/`aarch64-apple-darwin` and
  `x86_64-pc-windows-msvc`, with `SHA256SUMS`. musl rather than glibc so a
  static binary runs on any distro rather than inheriting the builder's glibc
  floor. Each is smoke-tested on the runner that built it.

- **`compose.quickstart.yaml`** — pulls published images instead of compiling
  four Rust crates before you have seen anything. Paired with a new
  **`dagron-engine-localdev`** image: the production engine is distroless, and
  with `EXECUTOR=local` a task's command runs *inside* that container, so
  `command: ["echo", "hello"]` cannot resolve there. The localdev image is the
  same binary on debian-slim.

- **`scripts/examples_check.py`** — validates every example spec and runs the
  ones a laptop can run, classifying the rest with a reason so "skipped" never
  quietly means "broken". Currently 35 specs valid, 27 running.

- **`scripts/logfilter_e2e.py`** — end-to-end check of the log filter grammar,
  including parity between the engine's ops API and `dagron-api`. The two
  handlers implement one grammar and nothing compared them to each other.

- **Workflow log views with an explicit, adjustable filter.** Logs were readable
  one task at a time, unfiltered: to find out why a run failed you clicked
  through every task panel looking for the one that printed the stack trace, and
  a task that logged 200k lines gave you all 200k. Both halves are now fixed.

  A new **`GET /api/runs/{id}/logs`** (and `GET /runs/{id}/logs` on the engine
  ops API) returns *every* task's output in a run as one attributed stream —
  which task, which attempt, which line number — so a failure is one call away
  instead of N guesses. Both log endpoints, old and new, accept the same
  server-side filter: `q` / `exclude` / `regex` / `level` / `case` / `context` /
  `limit` / `tail`. Filtering happens on the server because a run's output can
  be hundreds of megabytes, and "download it all, then grep in the browser" is
  not a filter.

  The console gains a **Logs** view on the run page (next to Graph and Timeline)
  with the filter controls, level toggles, and a per-task scope picker; the task
  drawer gets the same controls, and a filter survives navigation and is carried
  in the URL, so "here's the link with the error filter" works. The filter also
  composes with live tailing: it applies *within* each `?offset=` slice while
  `next_offset` keeps counting the raw text, so a filtered tail neither loses nor
  repeats output.

  Three properties the implementation is deliberate about, because a log view
  that quietly hides things is worse than none: `total`/`matched` are counted
  **before** the line cap, so a truncated view always says how much it hid; line
  numbers are positions in the *unfiltered* output, so a filtered line can still
  be found in the raw log; and an invalid filter (uncompilable regex, unknown
  level) is a `400` naming the reason rather than a silently unfiltered response
  the caller would read as "nothing matched was hidden".

  Levels are **inferred** from the head of each line — task output carries no
  structured level — and only the head, so `echo "no errors found"` isn't an
  error line. The grammar lives in one place
  (`crates/dagron-logging/src/logfilter.rs`, feature `logfilter`) so a filter
  typed in the console, sent by an SDK, or written into a runbook all mean the
  same thing. It's off by default: the engine binary only writes logs and
  shouldn't link a regex engine.

  Also reachable from the MCP server (`dagron_get_run_logs`, plus filter
  arguments on `dagron_get_task_logs` — an agent triaging a failure now makes one
  call, not one per task) and both SDKs (`get_run_logs`/`getRunLogs`, filter
  kwargs on the task-log calls). Reference:
  [docs/API.md §Log filter](docs/API.md#log-filter).
- **`scripts/ossync-dryrun.py`** — run the OSS mirror sync locally before tagging.
  Stages exactly what would publish and runs four checks: the two fail-closed
  scans CI already enforces, plus **provenance** (private repo URL, monorepo
  paths, internal doc names) and **link closure**, which the CI scan cannot see
  because it matches tokens rather than meaning. All three of the problems it
  now catches were found by hand during 0.5.0: Argo CD examples pointing at the
  private monorepo, READMEs telling readers to `cd` into a directory the mirror
  does not have, and mirrored docs linking docs that were never included.

  It stages from `git ls-files`, not the working tree, because the real sync
  rsyncs from a CI checkout with no untracked or gitignored files — staging
  differently invents leaks that could never ship. Wired into the pre-publish
  checklist the release process runs.

### Changed

- **The console shell.** One accent instead of two competing ones (primary
  actions were blue while the brand and navigation were orange), a steel chrome
  layer around a dark content ground, and a density pass: page width 1180 →
  1560 so run and task tables stop being squeezed, sticky table headers, and a
  sticky sidebar. The overview now leads with **what needs attention** — failed
  runs, approvals waiting, dead letters, repos out of sync — and when that list
  is empty it names what it checked, so a quiet panel reads as verified rather
  than broken.

- **The dev `compose.yaml` configures the artifact store** (`DAGRON_ARTIFACT_DIR`)
  with a volume shared between the engine and `dagron-api`. Without it the
  checkpoint/resume and batch-inference examples failed immediately — correct
  behaviour, and a poor first meeting with the feature those examples exist to
  demonstrate.

### Fixed

- **Fonts are self-hosted.** The console `@import`ed a font family from
  `fonts.googleapis.com` at runtime — a render-blocking request it cannot make
  at all in an air-gapped install.
- **Keyboard focus is visible.** The console had no `:focus-visible` style
  anywhere, so every button, link and form control across all 21 routes was
  invisible to a keyboard user.
- **`--dim` failed AA.** The secondary text colour measured 4.0:1 on the page
  ground; it is now 5.1:1.
- **The sidebar version is the real one.** It read `v0.1.0` on every build,
  including released images: the version fell back to `frontend/package.json`,
  which nobody bumps, and the release workflow never passed `APP_VERSION`.
  Unstamped builds now say `dev` rather than inventing a number.
- **Sub-DAG nodes no longer overflow their box.** Graph nodes declared a fixed
  height that was right for a plain task and wrong for one carrying a middle
  row, so the bottom handle sat partway up the node. Layout also now reads
  React Flow's measured sizes rather than the pre-measurement declaration.
- **The log filter no longer queries mid-word.** Text filters commit on Enter or
  on leaving the box, with the edited field outlined until applied; a regex that
  does not parse yet is held back instead of spending a round trip that can only
  return 400.

- **The OSS release gate can build the mirror cut again.** It ran
  `cargo build --workspace`, which this workspace cannot satisfy *by design*:
  `dagron-core` compiles exactly one sqlx backend, the engine resolves `sqlite`,
  and `dagron-api`/`dagron-gitops` resolve `postgres` — `--workspace` unifies the
  features, enables both, and hits the `compile_error!`. It only started failing
  now because `dagron-api` gained its `dagron-core` dependency in the run-submit
  fix, after the last release. The gate (and the two docs that told you to run
  the same command) now builds the two feature worlds separately, which is how
  the images are built anyway. Every member is covered exactly once.
- **Mirror provenance scrub.** A dry run of the OSS sync — staging the include
  set exactly as the workflow does, from tracked files only — found leaks the
  fail-closed marker scan cannot see, because it matches tokens rather than
  meaning. The `examples/gitops/` Argo CD manifests pointed their `repoURL` and
  source paths at a repository that is not this one, so anyone applying them
  publicly got an auth failure against something they cannot see; the Python
  SDK's `Roadmap` URL did the same on PyPI. Both now point here, with
  repo-relative paths. `ARCHITECTURE.md`, `DLQ.md` and the backfill/monitoring/
  sdk examples told readers to `cd` into a directory this repository does not
  have. And ~16 files cited internal design documents a public reader cannot
  open — the explanations stay, the dangling citations are gone, along with a
  workflow comment that enumerated closed add-on images by name.

## [0.5.0] - 2026-07-26

### Added
- **The Helm chart can deploy the GitOps worker** (`gitops.enabled`, off by
  default — most installs never connect a repository). Until now the chart had no
  template for it, so a Helm install could register repos in the console and have
  nothing polling them. One replica by design: each would poll every repo against
  the same rows. Runs read-only-root like its siblings, with an emptyDir mounted
  at **`/tmp`** — the worker clones into `std::env::temp_dir()`, so mounting the
  scratch anywhere else leaves every sync failing on a read-only filesystem.
  Listed in the Artifact Hub image annotation, and `helm install` now says so when
  the worker is absent instead of leaving you to wonder why nothing syncs.

### Fixed
- **`docs/DATASETS.md` can be mirrored.** It linked a **private**
  a private internal roadmap doc and twice used the edition abbreviation the mirror's own
  fail-closed tripwire rejects, so it could never be published, while three mirrored
  files (CHANGELOG, `API.md`, `CONFIG.md`) linked to it. Scrubbed and included;
  the closed connector name it advertised is gone too.

### Fixed
- **The OSS mirror stops publishing dead documentation links.** `docs/OPERATIONS.md`
  is mirrored and had grown links to `BACKUP_RECOVERY.md`, `BACKUP_AWS.md` and the
  two `scripts/*.sh` backup helpers — none of which were in the `.ossync` include
  set, so every one of them 404'd on the public repo. `BACKUP_RECOVERY.md` and the
  scripts are now mirrored; `BACKUP_AWS.md` stays out (it walks through this repo's
  terraform, which is not published) and is referenced without a link. A link-closure
  check over the whole mirrored set — every relative link in every included file,
  resolved against the include list — is what found them, and the remaining
  offenders are all pre-existing.
- **An Enterprise console no longer refuses specs the Enterprise engine accepts.**
  `dagron-api`'s `enterprise` feature did not forward to `dagron-core` (it pinned
  `features = ["postgres", "ops"]`), so every dagron-core gate stayed shut in an
  Enterprise build of the API. The visible symptom: a workflow using multi-dataset
  composition (`on_datasets: [a, b]` / `datasets_mode:`) submitted through the
  console was rejected with the open-build signpost — telling a customer to buy the
  edition they were already running — while the identical spec sent to the engine's
  ops API was accepted. Same cluster, two answers. A test now pins the console's
  answer to the engine's in both build shapes; it fails without the forwarding.
- **The GitOps worker is actually buildable and published.** #721 split GitOps
  into its own `dagron-gitops` crate + image, but the crate was never added to
  the workspace `members` and nothing path-depends on it — so
  `cargo check -p dagron-gitops` answered *"package ID specification
  `dagron-gitops` did not match any packages"*, and any cargo invocation quietly
  dropped it from `Cargo.lock`. The release workflow's image matrix listed four
  images and not this one, so a tag would have shipped the feature with no image
  to run it. Both fixed, plus a Docker Hub overview for the new public repo.

### Changed
- **Envelope encryption (BYOK/KMS) and encrypted artifacts at rest are
  Enterprise.** Drawn before 0.5.0 ships, deliberately: the KEK layer has never
  appeared in a tagged OSS release (`dagron-crypto/src/lib.rs` was 121 lines at
  `dagron-oss-v0.4.3` with no `KeyProvider`; it is 1468 now), so this is a line
  drawn rather than a takeback.

  **Open in every build, unchanged:** environment secrets encrypted with
  AES-256-GCM under `DAGRON_ENV_SECRET_KEY` — the `v1:` format the compose stack
  and [docs/HOWTO.md](docs/HOWTO.md) §5 use — and the plain artifact store.
  **Enterprise:** KEK providers (AWS KMS / GCP KMS / Azure Key Vault / the
  command seam), envelope-wrapped data keys, artifact encryption at rest, and the
  store-wide rotation sweep (`POST /api/artifacts/rotate`, a route now absent from
  open builds).

  Enforced in one place — `dagron_crypto::build_provider`, the funnel every KEK
  consumer reaches through (env-secret `v2:` mode, artifact-at-rest, rotation) —
  so the line cannot drift apart across three subsystems. An open build handed a
  `DAGRON_ENV_KEK_PROVIDER` **errors with a signpost naming the edition and the
  open alternative**, rather than returning "no provider" and quietly encrypting
  with a single env-var key: silently downgrading a deployment that asked for KMS
  is not something anyone should discover from a ciphertext dump.

### Fixed
- **dagron-api: Argon2 password hashing no longer runs on the async runtime, and
  is bounded.** Measured on the compose stack: RSS went 67 MB → 187 MB after 20
  logins → 227 MB after 60, then plateaued; 30 concurrent SSE clients moved it
  3 MB, so streaming was never the cost. `Argon2::default()` is Argon2id with
  `m_cost = 19 MiB`, and glibc raises its mmap threshold after repeated
  allocations that size and keeps one arena per thread that ever hashed — with
  hashing done inline on tokio workers, that was ~19 MiB × worker-count resident
  *and* a worker pinned for the ~50–100 ms each hash takes, stalling every other
  request on that thread. All hashing now goes through one `pwhash` module: a
  semaphore (`DAGRON_PW_HASH_CONCURRENCY`, default 2 → ~38 MiB ceiling) taken
  *before* the work is handed to `spawn_blocking`. Parameters are unchanged —
  19 MiB is the OWASP floor and lowering it would weaken the hashes.

  The residency itself is glibc's dynamic mmap threshold: it grows as it sees
  repeated frees that size, after which the scratch comes off the heap and is
  never returned. `compose.yaml` pins `MALLOC_MMAP_THRESHOLD_=131072` (plus
  `MALLOC_ARENA_MAX=2` for the idle baseline). Measured over 30 logins:

  | | baseline | after |
  |---|---|---|
  | before | 67 MB | 227 MB |
  | arena cap only | 5.6 MB | 166 MB |
  | + threshold pinned | 5.6 MB | **5.9 MB** |
- **dagron-api: `POST /api/login` is rate limited** (`DAGRON_LOGIN_RATE_LIMIT`,
  default 30 per `DAGRON_LOGIN_RATE_WINDOW_SECS`=60, per client; `0` disables).
  It was the one unauthenticated route that spends ~19 MiB and ~100 ms of CPU per
  call — and spends it even for an email that doesn't exist, because the
  no-such-user path deliberately runs the same verify to avoid leaking which
  accounts are registered. That made it free memory/CPU amplification for anyone
  who could reach the port. Keyed on the socket peer (unforgeable) by default;
  `DAGRON_TRUST_PROXY_HEADERS=true` switches to the first `X-Forwarded-For` hop
  for deployments actually behind a proxy, which is what the console's `/api`
  rewrite is (set in `compose.yaml`). In-memory and per replica: a cost ceiling
  and a guessing brake, not a distributed lockout policy.

### Added
- **Point-in-time recovery on AWS** (`docs/BACKUP_AWS.md`, not mirrored — it references this repo's terraform) —
  the follow-on to the runbook's "nightly dumps are a floor, not a plan". Three
  concrete shapes with working config: RDS/Aurora managed PITR (terraform, and
  the `backup_retention_period = 0` footgun), CloudNativePG on EKS shipping WAL +
  base backups to S3 over IRSA, and pgBackRest on EC2. Includes the archiver
  heartbeat to alert on (`pg_stat_archiver.failed_count` — a permissions mistake
  in the verification drill produced `archived=6 failed=3` while the database
  accepted writes normally), and the dagron-specific steps after a restore:
  repoint `DATABASE_URL` and start one engine first, keep old
  `DAGRON_ENV_SECRET_KEY` versions for at least the PITR window (restored
  ciphertext predates a rotation and the current key will not decrypt it), expect
  the artifact store and GC archive to be inconsistent with the restored
  database, and cancel unsafe runs before the engine reclaims stale leases. The
  full PITR loop was executed end to end (base backup → data → target time →
  TRUNCATE → recover): recovery stopped before the offending transaction and the
  100 rows came back.
- **Backup / migration / disaster-recovery runbook**
  ([docs/BACKUP_RECOVERY.md](docs/BACKUP_RECOVERY.md)) plus
  `scripts/backup-postgres.sh` and `scripts/restore-postgres.sh`. Covers what is
  state (the database **and** `DAGRON_ENV_SECRET_KEY`, which lives outside it —
  lose the key and stored secrets are unrecoverable ciphertext), how migrations
  actually behave (embedded, applied at engine startup, no `dagron migrate`
  command, forward-only, two sets sharing one `_sqlx_migrations` ledger via
  `ignore_missing`), the production upgrade order, a restore rehearsal, and
  eight recovery playbooks. Every claim was executed against a live stack:
  dump/restore round-tripped identical counts (25 runs / 54 tasks / 42
  migrations), a tampered checksum produced
  `Error: migration 40 was previously applied but has been modified` with exit 1
  and an untouched database, and an injected version-900 row was absorbed by
  `ignore_missing` rather than blocking startup. Also documents the trap that
  dagron-api recreates its own tables empty, so a partial restore loses every
  user and secret without erroring.
- **Docs: where a long script lives** ([docs/HOWTO.md](docs/HOWTO.md) §6 +
  [examples/scripts/](examples/scripts/README.md)). A task's `command:` is argv
  and there is no `script:` field or file-include, which left "my script is 400
  lines" unanswered. Four worked patterns — bake into the image, absolute path on
  the engine host, fetch at run start, or the body in an env var — with the two
  facts that decide between them: only `command`/`env`/`docker_image` reach the
  task process (`input:` does not), and no executor mounts host paths, so
  `DAGRON_ARTIFACTS` passes files between host tasks but cannot carry a script
  into a container. All four validate through `dagron validate`; the env-var one
  runs with no setup.
- **Console: template calls (`templates:` / `template:`) are first-class.** The
  engine has expanded template calls since day one, but dagron-api's own spec
  mirror knew only `command` and `workflow_ref`, so a task calling a template was
  a 400 (`needs a command or a workflow_ref`) on **every** save/submit path —
  `examples/templates/01_dag_of_dags.yaml` could not be stored through the API at
  all, let alone opened in the editor. `TaskSpecInput` now carries
  `template`/`arguments` and `DagSpecInput` carries `templates`, with save-time
  validation the engine can't do lazily: template names unique and non-empty, no
  empty template, every call resolves to a declared template (spec-local, so a
  typo is caught on Save, not at run time), each template's own task list checked
  as a sub-graph, and a task is exactly one of leaf / call / chain. Two
  silent-drop paths are now explicit errors instead: a `workflow_ref` **inside** a
  template (the chain expander only walks top-level tasks) and a `workflow_ref`
  **to** a workflow that declares its own `templates:` (inlining copies tasks,
  not templates).
- **Console: the visual editor edits template calls.** A `template:` task renders
  as a dashed sub-DAG node (`⧉ etl · 3 tasks`); its panel has a **Calls template**
  picker over the declared templates and an **Arguments** row per parameter the
  chosen template declares (defaults as placeholders, unknown-but-present
  arguments kept and flagged). Leaf-only knobs (command, image, retries, timeout,
  trigger rule) are hidden on a call, which runs no command of its own. New
  **“DAG of DAGs (template call)”** starter and **Blocks → Control flow → “Sub-DAG
  (template call)”**, which scaffolds the `templates:` entry and its call together
  — a call is only valid alongside the template it names.

### Changed
- **The visual editor refuses to silently degrade.** The canvas draws one node
  per task, which is a lie for a spec using `with_items:`/`with_param:` (one node,
  N tasks), `hook:` (edges to every task, undrawn), `gang:`, `type: wait`/
  `workflow` (command-less kinds whose panel offered a command box), `cache:` or
  `resources:`. Loading such a spec now **disables the Visual tab** — "this
  workflow uses features the visual editor can't edit — use the YAML tab" — and
  lists the responsible fields. The rule is an allowlist, so a field the editor
  has never heard of (a new engine feature, a typo) locks the tab rather than
  becoming quietly editable; everything the block palette emits stays editable.
  Nothing is dropped either way — the spec is untouched and fully editable as
  YAML. Renaming a task in the visual editor now also follows `result_from:`,
  which otherwise saved a dangling reference.
- **Datasets: data-aware scheduling** (Airflow Datasets / Dagster asset
  sensors — the #27 dataset-sensor follow-on plus the round-2 `dataset://`
  stretch slice, as one subsystem; [docs/DATASETS.md](docs/DATASETS.md)). Three
  pieces on a shared ledger:
  - **`produces: [<uri>, …]`** on a command task — on success the engine
    upserts each URI in the new `datasets` registry and appends a
    `dataset_events` lineage row (producing workflow/run/task/time): the
    cross-workflow update trail, queryable via `GET /datasets` and
    `GET /datasets/events?uri=…`. URIs template per fan-out instance. Recording
    is fence-guarded (a reclaimed task's late result can't fabricate lineage)
    and best-effort (a ledger failure never fails the run).
  - **`wait: { dataset: <uri> }`** — a dataset sensor on the #27 park
    machinery (running + NULL lease, no worker slot, **no new status**): new
    `task_runs.wait_dataset` + `wait_dataset_cursor` columns hold the ledger's
    high-water mark at park time, so only an update recorded *after* the park
    resolves it (fresh data, never history). Joins `for`/`until`/`url` in the
    exactly-one-of rule.
  - **`on_datasets: [<uri>]`** on a registered workflow — dataset-triggered
    scheduling: a sweep (~5 s, `DATASET_TRIGGERS=0` opts out) syncs
    subscriptions into the new `dataset_triggers` table and fires a run when a
    subscribed dataset updates, injecting `{{ trigger_dataset }}`. Firing is a
    **CAS cursor advance** — HA-safe with no leadership, exactly one scheduler
    wins each fire; registration never fires on history; rapid updates coalesce
    into one run; a fire refused at `max_active_runs` rolls its cursor back and
    retries. SQLite migrations 032–033, Postgres 039–040; metrics
    `scheduler_dataset_updates_total` / `scheduler_dataset_fires_total`.
  - **Open-core split** (per the open-core split): the single-team loop —
    produce, lineage, sensor, single-dataset trigger — is fully open.
    **Multi-dataset composition** (`on_datasets: [a, b, …]` +
    `datasets_mode: any|all` fan-in) and **external dataset events**
    (`POST /datasets/events`, for CDC/object-store producers outside dagron)
    ship with dagron Enterprise; the open build answers both with signpost
    errors naming the edition and the OSS workaround (the SOURCE-connector
    funnel pattern).
- **HTTP wait sensor: `wait.url`** (parity fast-win #27 follow-on — Airflow
  `HttpSensor`) — a task `{ type: wait, wait: { url: <http(s)-endpoint> } }` parks
  holding **no worker slot** and the engine GETs the endpoint every
  `WAIT_POLL_SECS` (default 15 s), succeeding the task on the first `2xx` so its
  dependents advance. Reuses the #27 park mechanics: the parked task stays
  `running` with `claimed_by`/`lease_expires_at` NULL and the endpoint in a new
  `task_runs.wait_url` column with a `next_poll_at` throttle (SQLite migration
  031, Postgres 038) — so the claim scan skips it and lease recovery leaves it
  alone, needing no new status. A non-2xx / errored poll re-parks it for the next
  window; the per-request timeout keeps a hung endpoint from stalling the tick.
  Bounded by the run's own `run_timeout_secs`. `for` / `until` / `url` are
  mutually exclusive (exactly one). The `url` may template (`{{ params.* }}`).
  (A fourth form, `wait: { dataset: … }`, ships in the datasets entry above, so
  the exactly-one-of rule is over four fields, not three.)
- **OpenTelemetry OTLP span exporter** (parity fast-win #28 follow-on) — with an
  `otel`-feature build **and** `OTEL_EXPORTER_OTLP_ENDPOINT` set, `dagron-logging`
  now installs an OTLP **HTTP/protobuf** span exporter + a `tracing-opentelemetry`
  bridge layer, so the engine's own `tracing` spans are delivered to a collector.
  Each dispatched task is wrapped in a `task.dispatch` span (dependency-free,
  always on under `otel`) so there's a span per task to export. Transport is
  HTTP/protobuf over a blocking `reqwest`/rustls client — the gRPC/tonic
  transport is deliberately not pulled in; endpoint/headers/timeouts come from the
  standard `OTEL_EXPORTER_OTLP_*` env vars. Off by default in every sense: no
  opentelemetry dependency without the feature, and no exporter without the
  endpoint (spans still propagate, just aren't exported). Dashboards/sampling/
  retention remain observability's per `SCOPE.md`.
- **OpenTelemetry trace-context propagation** (parity fast-win #28 — Dagster
  #12353 / Prefect #9271 / Airflow external-trace #69633) — built with the new
  `otel` Cargo feature, the engine injects a fresh W3C `traceparent` into every
  dispatched task's env (`TRACEPARENT`), so the task's own OpenTelemetry
  instrumentation joins an external trace (external-trace embedding). The trace
  id is logged on dispatch for log↔trace correlation. Dependency-free and off by
  default — a default build never sets `TRACEPARENT`, unchanged behavior. The
  OTLP span exporter (span **delivery**) ships as the follow-on above.
- **Deferrable wait sensor: `type: wait`** (parity fast-win #27 — Airflow
  deferrable time sensors / Argo suspend-with-duration) — a task
  `{ type: wait, wait: { for: <duration> } }` (or `{ until: <rfc3339> }`) parks
  holding **no worker slot** until its deadline, then succeeds and its dependents
  advance. `for` is a relative duration (`30s`/`5m`/`2h`) anchored when the task
  is reached; `until` is an absolute instant. Reuses the #23 park mechanics: the
  parked task stays `running` with `claimed_by`/`lease_expires_at` NULL and its
  resume time in a new `task_runs.wake_at` column (SQLite migration 030, Postgres
  037) — so the claim scan skips it and lease recovery leaves it alone, needing
  no new status. A reconcile sweep resolves due sensors each tick (idempotent,
  HA-safe); a deadline already in the past resolves at once. The HTTP (`url`)
  sensor ships as a follow-on above; dataset sensors remain a follow-on.
- **Sub-workflow trigger: `type: workflow`** (parity fast-win #23 — Airflow
  TriggerDagRunOperator / Argo #6922 workflow-of-workflows) — a task
  `{ type: workflow, workflow: <name> }` submits the named **registered**
  workflow as a child run when reached, then parks until the child is terminal:
  the parent succeeds if the child succeeded and fails if it failed/cancelled,
  after which its dependents advance. The parked task stays `running` with
  `claimed_by`/`lease_expires_at` NULL and the child id in a new
  `task_runs.sub_run_id` column (SQLite migration 029, Postgres 036) — so the
  claim scan never re-picks it and lease recovery never reclaims it, needing **no
  new status value and no status-CHECK rebuild**. A reconcile sweep resolves
  parked triggers each tick (idempotent, HA-safe). If the child is at its own
  `max_active_runs` cap the trigger is released back to `ready` and retries; an
  unknown or unparseable child fails the task. Nesting is bounded by
  `SUBWORKFLOW_MAX_DEPTH` (default 8) — a workflow that names itself, directly
  or around a cycle, fails the offending task instead of spawning child runs
  without end (see **Security** below); the global `max_inflight_runs`
  admission valve caps concurrent depth on top of that.
- **Workflow tags** (parity fast-win #26 — Airflow #16432 colored tags / #24464
  folder view, Dagster #14530) — a DAG may declare `tags: [..]` (each
  `[A-Za-z0-9_.-]`, ≤64 chars) to organize and filter workflows. The registry
  surfaces them on `GET /api/workflows` and `GET /api/workflows/:id`, and
  `GET /api/workflows?tag=<t>` returns only workflows carrying that tag. Tags are
  **parsed from the stored spec on read** — no denormalized column to keep in
  sync, so they always reflect the current definition — and validated when the
  engine loads the spec. The **Workflows UI** renders per-tag colored chips
  (deterministic color per tag) in the table and board views; clicking a chip
  filters the list to that tag, and tags join the text search. Purely
  organizational; the engine ignores them.
- **Result memoization: `cache`** (parity fast-win #22 — Argo memoization /
  Prefect task caching) — a task may declare `cache: { key, max_age_secs? }`. A
  successful run stores its output keyed by `(workflow, task, resolved key)`; a
  later task whose key matches reuses that output and **skips execution
  entirely** — no worker, no secrets, no artifacts — then its dependents advance
  as for a normal success. The `key` is a template resolved at expansion, so
  `{{ scheduled_time }}` / `{{ params.* }}` make repeated and backfilled runs hit
  the cache (reproducible backfills). `max_age_secs` expires stale entries so the
  task re-runs and refreshes. New `task_memo` table (SQLite 028, Postgres 035);
  the memo write reuses the per-success spec parse the `repeat` operator already
  does, so uncached tasks pay nothing; `scheduler_cache_hits_total` metric. No
  behavior change without a `cache:` block.
- **Named concurrency pools: `pool`** (parity fast-win #21, second slice —
  Airflow #13975 "multiple pools" / Argo Kueue) — a task may name a `pool:`, and
  a scheduler claims a pooled task only while fewer than the pool's capacity are
  already running in it (capacities from the `POOLS` env, e.g. `POOLS=etl:4,db:2`).
  An over-budget task simply waits in `ready` until a slot frees — no run is
  dropped and there is no new run state — because the ready-claim itself is the
  enforcement point (`task_runs.pool`; SQLite migration 027, Postgres 033 column
  / 034 `CREATE INDEX CONCURRENTLY`). **When `POOLS` is unset the claim path is
  byte-for-byte unchanged** (zero cost/risk for deployments not using pools). On
  Postgres, unpooled/uncapped tasks keep the lock-free `FOR UPDATE SKIP LOCKED`
  fast path; only capped-pool claims serialize, under a global advisory lock, so
  concurrent controllers can't over-commit a pool's slots (SQLite is single-writer,
  so its one write-tx claim is already race-free). DAG-wide default via
  `task_defaults.pool`; fan-out instances inherit the parent's pool; names are
  validated `[a-z0-9_-]{1,64}`. Together with `max_active_runs` (run-level), this
  completes #21's concurrency-control story: per-run and per-task-pool limits.
- **Per-workflow concurrency limit: `max_active_runs`** (parity fast-win #21,
  first slice — Argo #12757 workflow concurrency control / Prefect deployment
  concurrency limits) — a DAG may set `max_active_runs: <n>` to cap how many of
  its runs (matched by workflow name) are `running` at once. `create_run`
  enforces it at the single choke point every fire path flows through, so the
  cap holds for API submits, cron/DB schedules, backfills, catch-up, queue
  sources, and dead-letter redrive alike — with **no change to the hot task-claim
  path**. Over the cap, `create_run` returns a typed `MaxActiveRunsReached`: the
  API answers **429 + Retry-After** (not 500), a queue source **requeues** the
  message instead of dead-lettering it, and schedule/backfill fires are held back
  (backfill releases its slot and retries when capacity frees). The count filters
  the small set of `running` runs and PK-joins their definitions — no schema
  change, no migration. `None`/`0` = unlimited (unchanged behavior). Caps
  concurrent *runs*, not per-task concurrency (named task pools are a follow-on).
- **Cause-conditional retry: `retry_on_timeout`** (parity fast-win #24 —
  Airflow #9232) — a task may set `retry_on_timeout: false` so that a run killed
  by its `timeout_secs` deadline **fails immediately** instead of burning the
  rest of its `max_attempts` (a deadline kill usually recurs, so the retries just
  delay the failure). Timeout-only: a non-zero exit or a backend error still
  retries under the normal attempts rule. Every executor backend (local, Docker,
  Kubernetes) now aborts a deadline via a typed `TimeoutError` that the worker
  downcasts, so the reconcile loop distinguishes a timeout from any other
  failure; the terminal-fail log records `timed_out` / `not_retried_due_to_timeout`.
  DAG-wide default via `task_defaults.retry_on_timeout` (a task's own value wins).
  Default `true` = unchanged behavior. Pure logic — no migration.
- **Task dispatch priority** (parity fast-win #25 — Airflow `priority_weight` /
  Argo Kueue analog) — a task may declare `priority: <int>` (default `0`); among
  the tasks that are `ready` at the same moment a scheduler claims higher
  priority first (`claim_ready` now orders `priority DESC, scheduled_at`), so a
  latency-sensitive branch jumps a deep backlog of low-priority work. A pure
  tiebreak: it never lets a task run before its dependencies, and it persists on
  the row so a retry / lease recovery keeps its place. A DAG-wide default is
  settable via `task_defaults.priority` (a task's own non-zero value wins);
  fan-out instances inherit their parent's priority. `task_runs.priority`
  (SQLite migration 026 + partial ready index; Postgres 031 column / 032
  `CREATE INDEX CONCURRENTLY`), threaded through both DB backends. No behavior
  change for existing/unprioritized workflows.
- **Gang co-scheduling schema (`gang: {size: N}`)** — a task expands at run
  creation into N member rows (`<name>.<rank>`, shared gang id), dependents
  wait for every member, and members receive `DAGRON_GANG_ID` /
  `DAGRON_GANG_RANK` / `DAGRON_GANG_SIZE` for rendezvous — in **any** build, so
  a member always knows its rank however it was scheduled. Validation enforces
  leaf, single-attempt gang tasks (die-together retry is a unit-level rerun).
  The all-or-nothing gang claimer (`RUNNER_GANGS=1` — claim a gang only when
  every member is ready and capacity fits it whole, cancel a failed member's
  siblings) ships with the dagron Enterprise scheduler; without it, members
  schedule as ordinary tasks. Placement sweeps leave gang members in place.
- **Per-partition range leases + sharded streams (multi-consumer)** — a
  `source_partitions` lease table with claim/renew/release primitives (both
  backends) lets N engines split one logical stream, one consumer per
  partition, heartbeat-renewed and rebalanced when a consumer dies. Committed
  positions namespace per shard (`<source>/<partition>` in `source_offsets`)
  via the new `PendingPosition.substream`, so exactly-once holds per
  partition. Reference consumer: point `STREAM_PATH` at a **directory** and
  each `*.ndjson` file becomes a leased shard (`STREAM_SUFFIX`,
  `STREAM_MAX_PARTITIONS`) — the broker-free consumer group.
- **Exactly-once streaming ingestion (transactional source offsets)** — a
  source's resumable coordinate (byte offset, broker offset, replication LSN)
  now commits **in the same datastore transaction** as the run — or dead
  letter — it accounts for (`source_offsets` table; `create_run_with_offset`
  / `record_dead_letter_with_offset`), and the ingest actor hands the
  committed cursor back to the source at startup. A crash-replay of the whole
  stream creates zero duplicate runs and zero duplicate dead letters. Sources
  opt in via two new defaulted `WorkflowSource` methods (`pending_position`,
  `set_committed_position`) — existing sources are untouched; `SOURCE=stream`
  implements them (its offset file remains as a trailing mirror).
- **Cloud artifact stores: S3 / GCS / Azure Blob** — `DAGRON_ARTIFACT_URL=
  s3://bucket/prefix` (or `gs://`, `az://`; S3-compatible MinIO/Ceph via
  `AWS_ENDPOINT_URL`) selects an `object_store`-backed `ArtifactStore`
  (features `s3`/`gcs`/`azure` on `dagron-artifact`, forwarded by the
  `archive-*` features on the API binary). Same sanitized
  `<run>/<task>/<name>` layout as the local store, bounded-memory multipart
  `put_stream` (aborted on failure), streaming `get_stream`, listing (so
  KEK rotation sweeps buckets too), and transparent composition with the
  BYOK/KMS `EncryptedStore` — ciphertext in the bucket. The engine injects
  per-task `DAGRON_ARTIFACTS_URL` / `DAGRON_CHECKPOINT_URL` so checkpoints
  written on one machine resume on any other that can reach the bucket
  (multi-cloud checkpoint resume); a URL in a build without the matching
  backend feature errors loudly instead of falling back to local.
- **Streaming ingestion built in: `SOURCE=stream`** — follow an append-only
  NDJSON event file or named pipe, one workflow submission per line, with
  at-least-once delivery off a durable byte-offset checkpoint (committed only
  after the run is created; delete the offset file to replay, or
  `STREAM_FOLLOW=false` to drain a backlog and exit). Poison lines are
  dead-lettered (durable row + a `.dlq` NDJSON mirror) instead of wedging the
  stream. Selecting a managed connector kind (`kafka`/`nats`/`sqs`/`redis`/
  `events`) now errors with an accurate signpost to the dagron Enterprise
  connector suite and the built-in alternatives. Guide: `docs/STREAMING.md`;
  runnable case studies: `examples/streaming/`.
- **Long-running tasks: lease heartbeat** — workers renew a running task's
  lease every 10 s (claim triple–guarded), so a task may run for hours under
  the same short-lease crash recovery; a worker that dies stops heartbeating
  and its task is reclaimed exactly as before. Losing the claim aborts the
  local execution (the reclaimer owns the task). Opt out with
  `TASK_LEASE_HEARTBEAT=false`.
- **Checkpoint-aware resume for retries** — a running task reports its
  committed checkpoint (`POST /runs/{id}/tasks/{task_id}/checkpoint` with its
  injected `DAGRON_RUN_ID`/`DAGRON_TASK_ID`, or the
  `$DAGRON_CHECKPOINT_DIR/latest` file convention); the pointer survives
  retries and *Rerun failed*, is cleared by *Clear task*, and the next attempt
  is dispatched with `DAGRON_RESUME_FROM` / `DAGRON_RESUME_MARKER` — resume
  from epoch N, not epoch 0. Tasks also now receive their identity
  (`DAGRON_RUN_ID`, `DAGRON_TASK`, `DAGRON_TASK_ID`) in env. Guide:
  `docs/AI_WORKLOADS.md`; runnable case studies: `examples/ai/`.
- **GPU resource sugar** — `resources: { gpu: { count: N, resource: … } }`
  expands to the Kubernetes extended-resource limit
  (default `nvidia.com/gpu`); an explicit `limits` entry for the same key
  wins. Validation rejects `count: 0`.

### Security
- **Sub-workflow nesting is bounded (`SUBWORKFLOW_MAX_DEPTH`, default 8).** A
  `type: workflow` task that names its own workflow — directly or around a cycle
  — spawned child runs without end, each leaving a parked parent row behind.
  There is no recursion on a stack here, so nothing failed: it was an unbounded
  run factory that a single spec could start. A trigger at or past the cap now
  fails that task with a message naming the depth. Depth is read by walking
  `task_runs.sub_run_id` up from the triggering task's own run — the parentage
  the parking column already records — so no `parent_run_id` column and no
  backfill are needed; SQLite migration 034 / Postgres 041 add the partial index
  that makes each hop a lookup (and also covers the reconcile sweep's existing
  forward read). The walk stops at the cap, so the check is O(depth) and
  terminates even if the column ever describes a cycle.
- **`wait.url` sensors no longer follow redirects.** A `wait.url` poll is issued
  by the **scheduler**, not the task sandbox, so an external endpoint answering
  `302 http://169.254.169.254/…` could previously use the scheduler as a deputy
  to reach addresses a task pod may not. The scheme check at validation time
  cannot see a redirect target; the client now declines to follow one, and a 3xx
  simply reads as "not ready" and re-parks. A failed client build is now a
  startup error rather than a silent fall back to a default (redirect-following,
  untimed) client.
- **Optional `WAIT_URL_DENY_PRIVATE=1`** restricts `wait.url` polls to
  globally-routable addresses — private/loopback/link-local (incl. the cloud
  metadata address), CGNAT, multicast and reserved ranges are refused across
  IPv4, IPv6 and IPv4-mapped IPv6. **Off by default on purpose:** the common
  `wait.url` *is* an internal address (`http://svc.default.svc/ready`), so a
  deny-by-default would break the primary use case to harden the case where the
  scheduler's network position is genuinely wider than the executor's
  (`EXECUTOR=kubernetes` with a differentiated NetworkPolicy). Enforcement is
  split so neither half is bypassable: IP-literal hosts are checked before the
  request (a literal never reaches a resolver), and names are filtered **inside
  the client's DNS resolver**, so the addresses dialed are exactly the ones that
  passed the filter — no check-to-connect window for a DNS rebind. Proxies are
  bypassed (`no_proxy`) while the policy is on: `HTTP_PROXY`/`HTTPS_PROXY` would
  otherwise mean the resolver only ever judges the *proxy's* address while the
  proxy dials the blocked target. A refused URL logs a WARN and re-parks as "not
  ready" rather than failing the task, so relaxing the policy resolves parked
  sensors in place. Editing the workflow spec does not: the endpoint is
  materialized into `task_runs.wait_url` when the task row is created, so a spec
  fix applies to the next run, not to a task already parked on the old URL.

## [0.4.0] - 2026-07-23

### Added
- **Live-updating UI with a global pause toggle** — the Runs, Workflows, and
  Overview pages now refresh in realtime off a new account-wide SSE endpoint
  (`GET /api/events/stream`, fanned out from the same shared Postgres
  `LISTEN task_events` as the per-run stream), replacing Overview's fixed 5s
  poll with event-driven refetches (debounced 400ms, flushed at least every 2s
  during bursts; a slow 30s poll keeps GitOps sync state and next-fire times
  fresh). A **Live/Paused pill** on each page header (and the run detail
  header) toggles one persisted preference (`localStorage`, synced across tabs
  and pages): paused holds no streams open and does zero background reads —
  data loads once, with a ⟳ manual-refresh button — for sessions where live
  reads are too costly; resuming (or an SSE reconnect) refetches to catch up.
  Hidden tabs cost nothing either: streams close on tab hide and reopen on
  re-show (with a catch-up refetch), and Overview's slow poll skips ticks
  while hidden.
- **DAG layout directions** (frontend) — the DAG graph (Submit live preview and
  the run viewer) now offers three layouts via a ↓/→/↘ segmented control on the
  canvas: vertical (top→bottom, the old default), horizontal (left→right, with
  edges leaving node sides), and a diagonal cascade (ranks step down-and-right
  like a staircase). The choice persists in localStorage and follows the user
  across views.
- **Resizable editor/preview split** (frontend) — the border between the YAML
  editor and the live DAG preview on Submit (and the rerun dialog) is now a
  draggable divider: slide it to grow either pane (clamped 20–80%), double-click
  to reset to even, and the ratio persists across sessions.
- **Offline UI assets (no CDN)** — the frontend self-hosts the Monaco editor
  (staged into `public/monaco/vs` at build, no `cdn.jsdelivr.net`) and the engine
  serves vendored Swagger UI assets at `/docs`, so no browser asset needs a CDN.
- **Native GCS + Azure Blob archive backends** (multi-cloud) — the GC archive
  sink, `dagron archive-compact`, and the `dagron-api` archive-history reads now
  speak `gs://` (feature `archive-gcs`) and `az://`/`azure://` (`archive-azure`)
  natively, alongside `s3://` (`archive-s3`, which also covers S3-compatible
  MinIO/Ceph). `GC_ARCHIVE_URL`'s scheme selects the backend; credentials come
  from each backend's standard env (`AWS_*`/`GOOGLE_*`/`AZURE_*`). URL→store
  dispatch is centralized in a small `objstore` module in both crates; a scheme
  whose backend feature isn't compiled in is a hard startup error, never a
  silent plain purge.
- **Archive history reads** — the archive-before-purge GC now upserts an
  `archived_runs` index row (run_id, name, status, timestamps; SQLite
  migration 020 / Postgres 024) before purging, and `dagron-api` gains
  `GET /api/archive/runs` (index-only list, filter by `name`, paged) and
  `GET /api/archive/runs/{id}` (fetches the run's `dagron.run-archive.v1`
  document from `GC_ARCHIVE_DIR`/`GC_ARCHIVE_URL`; the api's `archive-s3`
  feature mirrors the engine's). Purge now fails closed on an index-write
  failure too — an archived-but-unlisted run would be invisible history.
- **Parquet compaction** (`dagron archive-compact`, cargo feature
  `archive-parquet`) — a bounded, CronJob-shaped sweep that folds archived
  `run-*.json` documents older than `GC_ARCHIVE_COMPACT_MIN_AGE_DAYS`
  (default 30) into a date-partitioned Parquet dataset
  (`compact/tasks/dt=<date>/part-<uuid>.parquet`, one row per task with run
  columns denormalized), stamps `archived_runs.compacted_at`/`parquet_path`,
  and deletes source documents **only after** the part file verifiably
  landed (at-least-once across crashes — dedup on `(run_id, task_id)` when
  querying). Works over both the dir sink and, with `archive-s3`, the S3
  sink. A compacted run's detail endpoint answers 410 Gone with the
  `parquet_path`.
- **Split LISTEN DSN** (`DATABASE_LISTEN_URL`) — the engine's reconcile-loop
  `Waker` and dagron-api's shared SSE listener connect their session-scoped
  `LISTEN` to this URL when set (the direct Postgres endpoint), while
  `DATABASE_URL` may point at transaction-mode PgBouncer — which cannot serve
  `LISTEN`. Unset = the listener shares the pool config, exactly as before.
  Unlocks pooled query traffic on shared state-store cells.
- **S3-native GC archive sink** (`GC_ARCHIVE_URL=s3://bucket[/prefix]`,
  cargo feature `archive-s3`) — archive-before-purge without the intermediary
  volume: each expired run's `dagron.run-archive.v1` document is one atomic S3
  `PUT` (credentials/region/endpoint from standard `AWS_*` env), and only
  verified PUTs are purged. Setting the URL without the feature is a startup
  error (same contract as `EXECUTOR=kubernetes` without `kubernetes`), and
  `GC_ARCHIVE_DIR` local archiving is unchanged.
- **Runner-class routing** (`runner_class`) — segment the scheduler fleet by
  workload shape. A task (or a whole DAG via a spec-level default) may name a
  **runner class** (`[a-z0-9_-]{1,64}`, default `default`); the class is
  persisted on `task_runs` (SQLite migration 019 / Postgres 022, with a
  class-scoped partial ready-index) and a scheduler started with
  `RUNNER_CLASSES=etl,pulse` claims **only** those classes
  (`db::claim_ready_classes`, both backends — same `SKIP LOCKED`/CAS + lease +
  fence contract). Unset `RUNNER_CLASSES` claims every class, so existing
  single-pool deployments are unchanged. Template expansion substitutes
  `{{ param }}`s in `runner_class` like `docker_image`; invalid names fail
  validation at submit (spec) or startup (env). SDKs:
  `Dag(name, runner_class=...)` / `task(..., runner_class=...)` (Python),
  `new Dag(name, {runnerClass})` / `task(..., {runnerClass})` (TypeScript).
- **`DB_MAX_CONNECTIONS`** — the Postgres pool size (previously hard-coded 8)
  is env-tunable (min 2), so many lean engines can share one pooled state
  cluster.
- **Archive-before-purge retention GC** (`GC_ARCHIVE_DIR`) — with an archive
  directory configured, the leadership-gated GC sweep first exports each
  expired terminal run as a self-contained JSON document
  (`dagron.run-archive.v1`: run + definition + task rows + outbox events),
  written atomically (tmp → fsync → rename), and purges **only** the runs
  whose export verifiably landed (`db::archivable_runs` +
  `db::purge_runs_by_id`, both backends). A failed write keeps the run in the
  hot store; re-archiving after a crash is idempotent. Unset, GC behaves
  exactly as before.
- **Per-class backlog gauges + stale-ready alert** — `/metrics` now exposes
  `scheduler_ready_tasks_by_class{runner_class=…}` and
  `scheduler_ready_oldest_age_seconds{runner_class=…}`, and a leadership-gated
  loop WARNs when a class's oldest ready task waits longer than
  `READY_AGE_ALERT_SECS` (default 300 s; `0` disables;
  `READY_AGE_CHECK_INTERVAL_SECS` tunes the cadence). Catches the
  segmentation footgun where every scheduler's `RUNNER_CLASSES` excludes a
  class and its tasks age silently.

### Fixed
- **Approval-gate timeouts in lean builds** — `db::resolve_approval` /
  `db::resolve_expired_approvals` were `ops`-gated while the reconcile loop
  calls the sweep unconditionally, so an engine built without the `ops`
  feature failed to compile (and, conceptually, a lean engine would never
  fail-safe an expired gate). Both are now available in every build.
- **SDKs cover the 0.3.0 API.** The Python SDK (`dagron` 0.3.0) gains `approve_task`
  / `reject_task` for `type: approval` gates, the durable backfill-job methods
  (`create_backfill` / `list_backfills` / `get_backfill` / `cancel_backfill` over
  `/api/backfills`), and an optional `path` on `connect_git_repo`. The TypeScript
  SDK (`@dagron/sdk` 0.3.0) gains a full `Client` class (it previously shipped only
  the `Dag` builder) mirroring the Python client's whole surface, including the same
  new 0.3.0 methods and an SSE `streamRun` / `waitForRun`. (The TS package version
  jumps `0.1.1 → 0.3.0`, skipping `0.2.0`, to line up with the dagron-api / Python
  SDK version — no `0.2.0` TS release was lost.)

## [0.4.1] - 2026-07-23

### Added
- **Artifact Hub-ready OSS Helm chart** — chart annotations/metadata so the
  published OCI chart is listed and browsable.

## [0.4.2] - 2026-07-23

### Added
- **Task-oriented [`HOWTO.md`](docs/HOWTO.md)**, and the reference docs
  (`API`/`CONFIG`/`OPERATIONS`/`MCP`) added to the OSS mirror.
- Artifact Hub badge on the README; the Grafana dashboard screenshot committed
  (the repo-wide `*.png` ignore had silently dropped it).

### Fixed
- **Frontend image CVEs** — dependency bumps and a distroless runtime.
- Open-core framing scrubbed from the mirrored docs.

## [0.4.3] - 2026-07-23

### Added
- **`mancube/dagron-mcp`** — the OSS MCP server image (eight stdio tools over the
  same JWT-gated API the console uses).
- Product landing site (S3 + CloudFront + Route53) and Artifact Hub repo metadata.

### Fixed
- **CRITICAL CVE in the frontend image** — runtime moved to a continuously
  patched base.

## [0.3.0] - 2026-07-08

### Added
- **Human approval tasks** (`type: approval`) — a task can now be a **human
  gate**: when its dependencies are
  satisfied it parks in a new `awaiting_approval` status (never claimed by a
  worker, so it needs no command) and the run waits until an operator approves or
  rejects it. `POST /runs/{id}/tasks/{task_id}/approve` (→ the task succeeds and
  the DAG proceeds) and `.../reject` (→ it fails, so `all_success` downstream
  skips), mirrored on the UI edge (`POST /api/runs/:id/tasks/:tid/approve|reject`).
  A gate may set `approval_timeout_secs` with `approval_on_timeout: approve|reject`
  (default **reject** — a gate fails safe): a reconcile-loop sweep auto-resolves
  an expired gate. Reuses the trigger-rule dependency model (approve decrements
  dependents like any success; reject like any failure). New `awaiting_approval`
  task status + `task_runs.is_approval` / `approval_timeout_secs` /
  `approval_on_timeout` columns (SQLite migration 018 — a table rebuild to widen
  the status CHECK — Postgres 021). Named approvers/groups, notifications, and
  audit build on this primitive behind the `enterprise` feature.
- **Backfill as a first-class API object** — a
  date-range backfill is now a durable, listable, monitorable, cancellable
  *job* the scheduler **paces**, instead of the synchronous capped
  `POST /schedules/:id/backfill` that materializes a whole window in one call.
  `POST /api/backfills` (`{schedule_id, from, to, max_runs?}`) snapshots the
  schedule's cron + timezone + workflow spec into a `backfills` row; the engine's
  leadership-gated pacer fires a bounded number of the range's fire-times per
  tick (default 20, `BACKFILL_PACE_PER_TICK`), advancing a cursor, so a large
  backfill drips into the cluster over many ticks rather than stampeding it —
  and the paced job can cover far more than the synchronous endpoint's 1000-run
  cap (job cap 100k). `GET /api/backfills` (list, `?schedule_id=`),
  `GET /api/backfills/:id` (monitor `fired`/`requested`/`status`), and
  `POST /api/backfills/:id/cancel` (stop pacing) round out the lifecycle. Runs
  are still deduped through the shared `schedule_backfills` ledger (a job never
  double-runs a slot a manual/auto backfill already materialized) and each
  backfilled run gets its logical date as `{{ scheduled_time }}`. New `backfills`
  table (SQLite migration 017, Postgres 020).
- **Live log tailing** — a running task's output is now visible *as it runs*,
  not only after it exits. `LocalExecutor` streams each stdout line to the
  datastore mid-run (fence-guarded, so a stale attempt can't corrupt a re-run;
  secrets are masked per-chunk like the final output, #8), and the task-logs
  endpoints gained an `?offset=` tail: `GET /runs/{id}/tasks/{task_id}/logs`
  (engine ops) and `GET /api/runs/:id/tasks/:tid/logs` (UI edge) return only the
  output past a character offset plus `next_offset` (resume point) and `eof`
  (task terminal), so a client polls with `?offset=next_offset` until `eof`.
  Offsets are Unicode-scalar counts (never split a multibyte character). No
  schema change — appends reuse the existing `task_runs.output` column. Docker
  surfaces its captured output through the same tail path (true mid-run
  `follow: true` is a follow-up); Kubernetes is unchanged (output at completion).
- **Synchronous invocation + run results** (`result_from`) — makes dagron
  callable as a durable
  function. A workflow can name the task whose output *is* the run's result with
  `result_from: <task>`; on success the engine copies that task's output into
  `workflow_runs.output`. Two ways to get it back synchronously: `POST /runs?
  wait=true` blocks until the run is terminal and returns `{run_id, status,
  finished, result}` (200, not 201) instead of just the id; `GET /runs/{id}/wait`
  long-polls an already-submitted run to completion. Both take `?timeout_secs=`
  (default 30, clamp 1–600); a timed-out wait returns `finished: false` with the
  live status so the caller re-polls (not an error). Mirrored on the `dagron-api`
  UI edge (`GET /api/runs/:id/wait` + `result_from` on submit). New nullable
  `workflow_runs.result_from` column (SQLite migration 016, Postgres 019);
  `result_from` must name a real, non-hook task (rejected at parse time).
- **Clear task + downstream** — a new
  recovery verb that re-runs a *single completed task together with everything
  that transitively depends on it*, without re-running the whole DAG or waiting
  for a failure. `POST /runs/{id}/tasks/{task_id}/clear` (engine ops) and
  `POST /api/runs/:id/tasks/:tid/clear` (UI edge) reset the target and its
  downstream cone from any terminal state (`succeeded`/`failed`/`skipped`/
  `cancelled`) back to `pending`, recompute each reset task's `remaining_deps`,
  bump `version` to fence stale workers, and re-arm a finished run so the
  reconcile loop resumes — while ancestors and unrelated branches stay intact.
  Use it to pick up a fixed input on a *green* node (which `rerun-from-failed`
  can't reach, since that only resets the failure frontier). 404 for an unknown
  run/task, 409 if the task is still running/pending. Reuses the existing
  fencing + `remaining_deps` model, so no schema change.
- **Deadline alerts** (`deadline: { in: 45m }`) — a soft
  SLA on a run: when a still-running run passes it, the engine emits a
  `run.deadline_exceeded` event to the transactional outbox (drained by the
  outbox delivery worker for webhook/Slack) and bumps `scheduler_deadline_alerts_total`,
  **without** cancelling — unlike `run_timeout_secs`, which fails the run.
  Fire-once and winner-take-all across schedulers. New `alert_deadline_at` /
  `alert_fired_at` columns (SQLite migration 015, Postgres 018); a shared
  duration parser accepts `45m` / `2h` / `90s` / `1d` / bare seconds.
- **Lifecycle hooks + `allow_failure`** — `hook: on_exit` makes a task a
  finalizer that runs once every
  non-hook task is terminal, `hook: on_failure` runs it only when the run is
  failing; both are auto-wired to depend on all other tasks with the matching
  trigger rule (`all_done` / `one_failed`), so no `depends_on` is needed.
  `allow_failure: true` lets an optional/best-effort task fail without failing
  the run (the task still records `failed`). New `task_runs.allow_failure`
  column (SQLite migration 014, Postgres 017); `reap_completed_runs` ignores
  allow-failure tasks when deciding the run status.
- **Secret env references** (`value_from`) — a task env var can pull its value
  from an external secret instead of storing it inline, so a credential never
  lands in the workflow spec or the datastore:
  `env: [{ name: DB_PASSWORD, value_from: { secret: prod-db-password } }]`.
  Resolved at dispatch from `DAGRON_SECRET_<NAME>` (process env) or a file
  `<DAGRON_SECRETS_DIR>/<NAME>` (the SOPS / External-Secrets / k8s-secret mount
  convention); a missing secret fails the task rather than running it empty.
  Resolved `value_from` values are always masked in output (regardless of the
  var's name), building on the #8 redactor. (Vault/cloud secret backends are
  a follow-up behind the same seam.)
- **Task trigger rules** (`trigger_rule:`) — a task can now run
  based on its dependencies' *outcomes*, not just their success: `all_success`
  (default), `all_done` (cleanup joins), `one_failed` / `all_failed` (failure
  handlers), `none_failed`. The scheduler's dependency model was generalized so
  every terminal transition (success/failure/skip) decrements dependents, and
  `advance_ready_tasks` evaluates each task's rule once its deps are all terminal
  → `ready` or `skipped` (with skips cascading). New `task_runs.trigger_rule`
  column (SQLite migration 013, Postgres 016). **Behavior change:** a task
  skipped because an upstream failed now shows as `skipped` (was `cancelled`);
  `cancelled` now means only an operator cancel or a run-deadline sweep. The run
  is still `failed` if any task failed, and `rerun-from-failed` re-runs the
  failed + skipped frontier.
- **Secret masking in task output.** Sensitive task-env values are now masked to
  `***` before a task's output is stored or logged, so a task that echoes a
  credential (or a library that prints one in a stack trace) no longer leaks it
  into the datastore or UI. On by default: any task env var whose **name** matches
  a sensitive pattern (`TOKEN`/`PASSWORD`/`SECRET`/`KEY`/… — overridable via
  `DAGRON_SENSITIVE_ENV_PATTERNS`, empty to disable), plus the values of any
  engine-process vars named in `DAGRON_REDACT_ENV` (e.g. `DATABASE_URL`). Masking
  is applied centrally in the worker (covers local/Docker/Kubernetes backends)
  and to the local executor's live stderr log; only values ≥ 4 chars are masked.
- **Forge feedback — commit statuses + run badges.** A workflow can declare a
  `notify.git` block (`provider: github|gitlab`, `repo`, `sha`, optional
  `context`/`target_url`, all `{{ param }}`-templated); when the run finishes the
  engine posts a success/failure commit status to the forge, so a dagron run
  shows up as a green/red check on the commit that triggered it. Best-effort and
  off by default — active only when `GITHUB_TOKEN`/`GITLAB_TOKEN` is set
  (`GITHUB_API_BASE`/`GITLAB_API_BASE` override for GHE/self-managed) — and a
  forge being down never affects run execution (mirrors the OpenLineage
  emitter). New `dagron-forge` crate holds the `ForgeClient` + GitHub/GitLab
  request builders. Plus a public, unauthenticated **run badge**:
  `GET /api/badges/:name` returns a shields-style SVG of a workflow's latest run
  status for embedding in a README.
- **GitOps pull sync (Git → datastore).** The GitOps page's **Sync** action now
  performs a real reconcile instead of just flipping UI state: it shallow-clones
  the registered branch, validates every `*.yaml`/`*.yml` under the repo's
  configured `path` (default `dagron/`) through the same parser the submit path
  uses, and upserts each valid workflow into the `workflows` table keyed by name
  — the Git → datastore *pull* half of GitOps, with no CRDs required. The
  fetched commit (`rev`), synced count, and per-file validation errors are
  recorded on the repo row; one bad file doesn't block the good ones. `POST
  /api/git-repos` gains an optional `path`; private repos clone with a token from
  `DAGRON_GIT_TOKEN`/`GITHUB_TOKEN` (injected only into `https://` URLs and
  redacted from errors); only `https/http/git/ssh/file` URL schemes are accepted.
  (An `auto_sync` background poller is the remaining follow-up; the flag is
  stored.)
- **`dagron-plan` — workflow diff for pull requests.** A new binary crate
  (`crates/dagron-plan`, depends on `dagron-core` only) that resolves two specs
  through the real parse → expand → validate pipeline and reports what would
  actually change: added/removed/changed leaf tasks with field-level diffs
  (command, deps, image, env, retries, timeouts), run-timeout changes, and a
  Mermaid graph of the resulting DAG with added/changed tasks flagged. Because
  it diffs the *resolved* DAG, two different YAML spellings of the same fan-out
  show as no change. `dagron-plan <base.yaml> <head.yaml>` or
  `dagron-plan --git <base>..<head> <path>` (shells `git show`); `git diff`-style
  exit codes with `--exit-code` (2 when the plan is non-empty) for a CI drift
  gate. Pairs with `dagron validate` to gate merges.
- **Cron `when` gate + `stopStrategy`** — two
  optional per-schedule expressions, both reusing the task-level `when:`
  evaluator:
  - **`when`**: a per-fire conditional gate for conditions cron can't express
    (e.g. `"{{ day }} == {{ days_in_month }}"` = last day of month,
    `"{{ weekday }} <= 5"` = weekdays only). Evaluated against the scheduled
    time's calendar fields in the schedule's timezone; a false result skips the
    fire (only `next_fire_at` advances). Supported on both file-cron config
    entries (`when:`) and UI schedules (`when_expr`). Skips counted in
    `scheduler_schedule_gated_total`.
  - **`stopStrategy`** (`stop_expr`, UI schedules): a comparison over the
    schedule's run outcome counts — `{{ succeeded }}` / `{{ failed }}` /
    `{{ total }}` — evaluated before each fire; when true the schedule
    auto-stops (disabled, with `stopped_at`/`stop_reason` surfaced via the API).
    Re-enabling clears the stop record. Counted in
    `scheduler_schedules_stopped_total`.
  New `schedules.when_expr/stop_expr/stopped_at/stop_reason` columns and a
  `workflow_runs.schedule_id` stamp for outcome counting (SQLite migration 012,
  Postgres 015).
- **Timezone-aware cron schedules** — a schedule now carries an IANA `timezone`
  (e.g. `America/New_York`); its cron expression is evaluated in that zone so a
  "02:00 daily" job keeps firing at 02:00 wall-clock across DST transitions.
  Threaded through the file-cron
  config (`timezone:` per entry), the DB-schedule loop, the manual + automatic
  backfill catch-up, the `dagron-api` schedule drawer (`timezone` field on
  create/update, validated → 400 on an unknown zone), and the operator's
  `CronWorkflow` CRD (`spec.timezone`). New `schedules.timezone` column
  (SQLite migration 011, Postgres 014), `DEFAULT 'UTC'` so existing rows are
  unchanged. The tz-aware next-fire computation is one shared helper
  (`dagron-engine::schedule_time`) mirrored by the API.
- **`dagron validate <file|dir>... [--json]`** — offline workflow lint through
  the exact parse → template-expansion → graph-validation pipeline every submit
  path uses. Directories are walked recursively; `--json` emits one object per
  file for CI; exits non-zero on any invalid spec. (A pre-merge GitOps check.)
- **Run-level timeout** — `run_timeout_secs` on the workflow spec. The engine's
  deadline sweep marks an overdue run `failed`, cancels its remaining tasks
  (fence-guarded against late executor writes), and counts it in the new
  `scheduler_runs_deadline_exceeded_total` metric. New nullable
  `workflow_runs.deadline_at` column (SQLite migration 010, Postgres 013).
- **Retry backoff cap** — `retry_max_delay_secs` on a task clamps the
  exponential backoff to a ceiling.
- **Named fan-out instances** — `instance_key: "{{ item.region }}"` on a
  `with_items`/`with_param` task names each expanded instance `<task>.<label>`
  instead of `<task>.<index>`. Labels are
  sanitized to `[A-Za-z0-9_-]` and must be unique within the fan-out.
- **`{{ scheduled_time }}` parameter** — every time-originated run (file cron,
  DB schedules, automatic backfill catch-up) receives its *nominal* fire time as
  an RFC-3339 workflow parameter, so tasks can reference their logical date
  (the data-interval idiom; a backfilled run processes *its* interval,
  not "now").
- The Python and TypeScript SDKs (`sdks/`) now ship in the distribution, so
  the `examples/sdk/` scripts resolve against the bundled SDK out of the box.
- Runnable SDK examples under `examples/sdk/` (Python + TypeScript) that drive a
  live `dagron-api`: quickstart, workflow+schedule, live SSE streaming, and
  cascade-rerun recovery, with a README covering setup and env config.
- Initial open-source cut of the dagron engine.

### Fixed
- **TypeScript SDK `Dag.submit()`** posted the raw spec to `POST /api/runs`; the
  gateway expects `{"yaml": "<spec>"}` and rejected it with `422 missing field
  yaml`. It now wraps the spec and returns the parsed `run_id` (`@dagron/sdk`
  0.1.0 → 0.1.1). Mirrors the Python SDK's v0.2 fix.
- `Executor` trait + `LocalExecutor` (subprocess) reference backend.
- `WorkflowSource` trait + `FileSource` and `ChannelSource` reference sources.
- In-memory `run_dag` scheduler: dependency-driven concurrency, retries with
  exponential backoff, and downstream skip-on-failure.
- `dagron run <file.yaml>` CLI and a bundled example DAG.

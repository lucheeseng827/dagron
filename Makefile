# dagron — every command in the docs, callable.
#
#   make            # the target list, grouped
#   make up         # the full stack: engine, API, console, Postgres
#   make dev        # one binary, SQLite, no infra
#   make ci         # exactly what CI runs, in CI's order
#
# The rule this file follows: a target here runs a command that already exists
# in the docs, unchanged. It is a shortcut, not a second way to operate dagron —
# so anything you learn from `make` is still true when you type the command by
# hand, and every target names the page it came from.
#
# Overridable variables are listed at the bottom of `make help`.

.DEFAULT_GOAL := help
SHELL := /bin/sh

# --- knobs ------------------------------------------------------------------

COMPOSE      ?= docker compose
QUICKSTART   ?= compose.quickstart.yaml
SOURCE_STACK ?= compose.yaml

# The console + REST API (compose stack). The engine's own management API is a
# different port with a different auth model — see the two "submit" groups.
API      ?= http://localhost:8080
ENGINE   ?= http://localhost:8787

# The dev admin compose seeds on first start only. Change both for a real
# deployment; docs/OPERATIONS.md says how.
EMAIL    ?= admin@local
PASSWORD ?= dagron-admin
COOKIES  ?= cookies.txt

DAG      ?= examples/simple_dag.yaml
CARGO    ?= cargo

# Point BIN at a downloaded release binary and the targets below stop building
# one — see the `bin` rule for why that check is worth a variable.
BIN_DEFAULT := ./target/release/dagron
BIN         ?= $(BIN_DEFAULT)

# The chart is `terraform/charts/dagron` in the source tree and `charts/dagron`
# once published; take whichever exists so `make helm-*` works in both.
CHART := $(firstword $(wildcard terraform/charts/dagron charts/dagron))

# Per-invocation arguments. Assigned empty with `:=`, not `?=`, on purpose: make
# imports the caller's environment, and NAME, PROFILE, DIR, DB and LEVEL are
# common enough there that `?=` would let a shell variable silently become a
# query filter or a target database. A makefile assignment beats the
# environment; a command-line `make runs NAME=etl` beats both, which is the only
# way any of these should ever be set.
NAME      :=
STATUS    :=
LIMIT     :=
DAYS      :=
LEVEL     :=
WAIT      :=
RUN       :=
ID        :=
FILE      :=
BASE      :=
HEAD      :=
DB        :=
NS        :=
DIR       :=
PROFILE   :=
HELM_ARGS :=

# `cargo test --workspace` cannot work here and that is by design: dagron-core
# compiles exactly one sqlx backend, the engine wants sqlite, dagron-api and
# dagron-gitops want postgres, and one unified feature set enables both and
# fails to compile. Hence the pairs below — the same split CI and the images use.
SQLITE_WORLD := --workspace --exclude dagron-api --exclude dagron-gitops
PG_WORLD     := -p dagron-api -p dagron-gitops
MQTT_WORLD   := -p dagron-source --features mqtt,sqlite

.PHONY: bin help up up-build down reset logs ps open dev run validate plan config \
        edge-config submit submit-wait swagger login runs run-status run-graph \
        run-logs run-wait watch metrics metrics-trend dead-letters redrive api-submit \
        build test clippy fmt fmt-check ci mcp mcp-readonly import-argo \
        pi-smoke pi-smoke-fresh pi-load backup restore helm-template \
        helm-install examples clean

##@ Quickstart — the full stack (engine · API · console · Postgres)

up: ## Start the pinned stack in the background (README "Option A")
	$(COMPOSE) -f $(QUICKSTART) up -d
	@echo
	@echo "console  $(API)   ($(EMAIL) / $(PASSWORD))"
	@echo "logs     make logs"

up-build: ## Build every service from source and start it (for changing dagron itself)
	$(COMPOSE) -f $(SOURCE_STACK) up -d --build

down: ## Stop the stack, keep the volumes (runs, users and workflows survive)
	$(COMPOSE) -f $(QUICKSTART) down

reset: ## Stop the stack AND delete its volumes — a real first boot, seeds included
	$(COMPOSE) -f $(QUICKSTART) down -v

logs: ## Follow the engine and API logs (`up -d` detaches, so this is where the boot is)
	$(COMPOSE) -f $(QUICKSTART) logs -f engine dagron-api

ps: ## Show what is running (schema-gate exiting 0 is correct, not a failure)
	$(COMPOSE) -f $(QUICKSTART) ps

open: ## Print the console URL and the seeded dev login
	@echo "$(API)  —  $(EMAIL) / $(PASSWORD)"
	@echo "seeded on first start only; changing them later needs 'make reset'"

##@ One binary, zero infra (SQLite + the management API)

dev: bin ## Run `dagron dev` — datastore workflow.db, API + Swagger on :8787
	$(BIN) dev

run: bin ## Run one DAG to completion and exit (DAG=examples/simple_dag.yaml)
	$(BIN) $(DAG)

validate: bin ## Parse + cycle-check specs offline, no database (DAG=file|dir)
	$(BIN) validate $(DAG)

plan: ## Diff a workflow spec across two files or two git refs (BASE=… HEAD=…)
	@[ -n "$(BASE)" ] && [ -n "$(HEAD)" ] || { echo "usage: make plan BASE=old.yaml HEAD=new.yaml"; exit 2; }
	$(CARGO) run --locked --release -p dagron-plan -- $(BASE) $(HEAD)

config: bin ## Print every knob with the source that set it (env / file / profile / default)
	$(BIN) config

# The config file is a temp file, not `.dagron-edge.yaml` in the caller's cwd:
# that is a plausible name for a real edge config, and this target would truncate
# it and then delete it. The trap cleans up on a signal too, and `config` is the
# last command in the recipe so a failing one is still a failing `make`.
edge-config: bin ## Same, under `profile: edge` — the constrained-host preset (docs/EDGE_PROFILE.md)
	@tmp=$$(mktemp "$${TMPDIR:-/tmp}/dagron-edge.XXXXXX") || exit $$?; \
	  trap 'rm -f "$$tmp"' EXIT INT TERM; \
	  printf 'profile: edge\n' > "$$tmp" || exit $$?; \
	  DAGRON_CONFIG="$$tmp" $(BIN) config

# A phony rule, not a file rule on $(BIN). Make would call an existing binary
# up to date whatever the sources have done since, which is how you end up
# debugging a change that was never compiled; cargo is the thing that actually
# knows, and it is nearly free when there is nothing to do. Skipped entirely
# when BIN names a binary the caller already has — someone running a downloaded
# release does not need a Rust toolchain to use the targets above.
bin:
ifeq ($(BIN),$(BIN_DEFAULT))
	@$(CARGO) build --locked --release -p dagron
endif

##@ Submit and watch — against `dagron dev` on :8787

# The engine's management API takes raw workflow YAML and no auth. The
# quickstart deliberately does not publish 8787, so these target `make dev`.

submit: ## POST a DAG to the engine (DAG=…) — prints {"run_id": …}
	curl -sS -X POST $(ENGINE)/runs --data-binary @$(DAG)

submit-wait: ## Submit and block until the run is terminal (DAG=…, WAIT=30)
	curl -sS -X POST '$(ENGINE)/runs?wait=true&timeout_secs=$(or $(WAIT),30)' --data-binary @$(DAG)

swagger: ## Print the engine's Swagger UI URL
	@echo "$(ENGINE)/docs"

##@ Submit and watch — against the stack on :8080 (docs/HOWTO.md §2, §4)

login: ## Log in and store the session cookie in cookies.txt
	curl -sS -c $(COOKIES) -X POST $(API)/api/login \
	  -H 'Content-Type: application/json' \
	  -d '{"email":"$(EMAIL)","password":"$(PASSWORD)"}'

api-submit: login ## Submit a DAG through dagron-api (DAG=…; needs jq)
	curl -sS -b $(COOKIES) -X POST $(API)/api/runs \
	  -H 'Content-Type: application/json' \
	  -d "$$(jq -Rn --arg y "$$(cat $(DAG))" '{yaml:$$y}')"

runs: ## List recent runs (STATUS=, NAME=, LIMIT= filter the query)
	curl -sS -b $(COOKIES) "$(API)/api/runs?limit=$(or $(LIMIT),20)$(if $(STATUS),&status=$(STATUS))$(if $(NAME),&name=$(NAME))"

run-status: ## One run: status and every task's status, attempt and output (RUN=…)
	@[ -n "$(RUN)" ] || { echo "usage: make run-status RUN=<run-id>"; exit 2; }
	curl -sS -b $(COOKIES) $(API)/api/runs/$(RUN)

run-graph: ## The DAG as nodes and edges — what the console's graph draws (RUN=…)
	@[ -n "$(RUN)" ] || { echo "usage: make run-graph RUN=<run-id>"; exit 2; }
	curl -sS -b $(COOKIES) $(API)/api/runs/$(RUN)/graph

run-logs: ## The whole run's logs, merged and attributed (RUN=…, LEVEL=error)
	@[ -n "$(RUN)" ] || { echo "usage: make run-logs RUN=<run-id> [LEVEL=error]"; exit 2; }
	curl -sS -b $(COOKIES) "$(API)/api/runs/$(RUN)/logs$(if $(LEVEL),?level=$(LEVEL)&context=1)"

run-wait: ## Block until a run finishes (RUN=…, WAIT=120; server max 600)
	@[ -n "$(RUN)" ] || { echo "usage: make run-wait RUN=<run-id>"; exit 2; }
	curl -sS -b $(COOKIES) "$(API)/api/runs/$(RUN)/wait?timeout_secs=$(or $(WAIT),120)"

watch: ## Stream the change signal for every run (SSE; re-GET on each message)
	curl -sS -N -b $(COOKIES) $(API)/api/events/stream

metrics: ## Run and task counts by status, plus the dead-letter total
	curl -sS -b $(COOKIES) $(API)/api/metrics

# `-f` and the jar check, because `curl -sS` exits 0 on a 401 and `-b` accepts a
# cookie file that is not there: without both, `make metrics-trend` before
# `make login` prints {"error":"unauthorized"} and reports success. The nine
# sibling targets above share the gap — see the PR thread.
metrics-trend: ## Per-day buckets behind the metrics charts (DAYS=14, max 90; NAME= one workflow)
	@test -r "$(COOKIES)" || { echo "no $(COOKIES) — run 'make login' first"; exit 2; }
	curl -fsS -b "$(COOKIES)" "$(API)/api/metrics/timeseries?days=$(or $(DAYS),14)$(if $(NAME),&name=$(NAME))"

dead-letters: ## Tasks that exhausted their retries (docs/DEAD_LETTERS.md)
	curl -sS -b $(COOKIES) "$(API)/api/dead-letters?limit=$(or $(LIMIT),50)"

redrive: ## Put one dead letter back on the queue (ID=…)
	@[ -n "$(ID)" ] || { echo "usage: make redrive ID=<dead-letter-id>"; exit 2; }
	curl -sS -b $(COOKIES) -X POST $(API)/api/dead-letters/$(ID)/redrive

##@ Build · test · lint (the same commands, in the same order, as CI)

build: ## Build both feature worlds
	$(CARGO) build --locked $(SQLITE_WORLD)
	$(CARGO) build --locked $(PG_WORLD)

test: ## Test both feature worlds, plus the mqtt one no default build compiles
	$(CARGO) test --locked $(SQLITE_WORLD)
	$(CARGO) test --locked $(PG_WORLD)
	$(CARGO) test --locked $(MQTT_WORLD)

clippy: ## Lint every crate (advisory in CI — a finding does not fail a PR)
	$(CARGO) clippy --locked $(SQLITE_WORLD) --all-targets -- -D warnings
	$(CARGO) clippy --locked $(PG_WORLD) --all-targets -- -D warnings
	$(CARGO) clippy --locked $(MQTT_WORLD) --all-targets -- -D warnings

fmt: ## Reformat the tree
	$(CARGO) fmt --all

fmt-check: ## Report formatting diffs (advisory in CI: there is no rustfmt.toml yet)
	$(CARGO) fmt --all -- --check

ci: ## fmt-check, clippy, build, test — what a PR is judged on
	-$(MAKE) fmt-check
	-$(MAKE) clippy
	$(MAKE) build
	$(MAKE) test

clean: ## Remove build artifacts and the local cookie jar
	$(CARGO) clean
	rm -f $(COOKIES)

##@ Agents, migration, examples

mcp: ## Run the MCP server on stdio — 42 tools (docs/MCP.md)
	$(CARGO) run --locked -p dagron-mcp

mcp-readonly: ## Same, with all 18 mutating tools hidden from tools/list and refused
	DAGRON_MCP_READONLY=1 $(CARGO) run --locked -p dagron-mcp

import-argo: ## Convert an Argo Workflows spec to dagron YAML (FILE=…, stdout)
	@[ -n "$(FILE)" ] || { echo "usage: make import-argo FILE=workflow.yaml"; exit 2; }
	$(CARGO) run --locked -p dagron-import -- argo $(FILE)

examples: ## List the runnable example DAGs
	@find examples -name '*.yaml' | sort

##@ Raspberry Pi / edge rig — these run ON the board (docs/RASPBERRY_PI.md)

# The engine's :8787 is not published to the host, so the load driver has to be
# local to the stack. Copy the tree first — run.sh resolves the driver at
# ../../loadtest/harness/, so the directory structure has to survive the copy.

pi-smoke: ## Install docker, bring the stack up, assert a diamond DAG runs 4/4 green
	sudo bash scripts/rpi-smoke.sh

pi-smoke-fresh: ## Same, but tear volumes down first — exercises first-boot migrations
	sudo bash scripts/rpi-smoke.sh --fresh

pi-load: ## Load profile: PROFILE=smoke|ramp|steady|push (default ramp)
	sudo bash loadtest/pi/run.sh $(or $(PROFILE),ramp)

##@ Operations (docs/OPERATIONS.md · docs/BACKUP_RECOVERY.md)

backup: ## pg_dump the whole database (DIR=. ; reads DATABASE_URL)
	bash scripts/backup-postgres.sh -d $(or $(DIR),.)

restore: ## Restore a dump (FILE=…, DB=workflow_dr_test) — rehearse into a scratch DB first
	@[ -n "$(FILE)" ] || { echo "usage: make restore FILE=workflow-….dump [DB=workflow_dr_test]"; exit 2; }
	bash scripts/restore-postgres.sh -f $(FILE) -n $(or $(DB),workflow_dr_test) --create

helm-template: ## Render the chart locally, change nothing
	@[ -n "$(CHART)" ] || { echo "no chart directory in this tree"; exit 2; }
	helm template dagron $(CHART) $(HELM_ARGS)

helm-install: ## Install the chart into the current kube context (NS=dagron)
	@[ -n "$(CHART)" ] || { echo "no chart directory in this tree"; exit 2; }
	helm install dagron $(CHART) -n $(or $(NS),dagron) --create-namespace $(HELM_ARGS)

##@ Help

help: ## Show this list
	@awk 'BEGIN { FS = ":.*##" } \
	  /^##@/ { printf "\n\033[1m%s\033[0m\n", substr($$0, 5); next } \
	  /^[a-zA-Z0-9_-]+:.*##/ { printf "  \033[36m%-16s\033[0m %s\n", $$1, $$2 }' $(MAKEFILE_LIST)
	@printf '\n\033[1mVariables\033[0m (make TARGET VAR=value)\n'
	@printf '  %-16s %s\n' \
	  'DAG'      'workflow to submit or run   (default $(DAG))' \
	  'RUN'      'run id, for the run-* targets' \
	  'API'      'dagron-api base URL         (default $(API))' \
	  'ENGINE'   'engine management API       (default $(ENGINE))' \
	  'DAGRON_VERSION' 'image tag the stack pulls (default: the pinned release)' \
	  'COMPOSE'  'compose command             (default $(COMPOSE))'
	@printf '\nEvery target runs a command from the docs unchanged — README.md,\n'
	@printf 'docs/HOWTO.md, docs/OPERATIONS.md, docs/RASPBERRY_PI.md.\n\n'

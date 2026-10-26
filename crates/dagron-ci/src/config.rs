//! dagron-ci's configuration: environment variables only, read once at start.
//!
//! A value that is set but wrong stops the process with a message naming the
//! variable. Nothing falls back silently: a typo in a listen address or a
//! schema name is cheaper to find at start than in production.

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::time::Duration;

/// One environment variable dagron-ci reads, with the default shown when it is
/// unset. Kept sorted; the crate README's table mirrors it.
pub struct Knob {
    pub name: &'static str,
    pub default: &'static str,
    pub meaning: &'static str,
}

pub const KNOBS: &[Knob] = &[
    Knob {
        name: "CI_ACTIONS_ALLOW",
        default: "— (any action, at any ref)",
        meaning: "The actions of other repositories a workflow's steps may use, as owner/* and owner/repo, comma-separated: set, a step that uses any other is refused, and each is pinned to a commit's full id",
    },
    Knob {
        name: "CI_ACTIONS_MIRROR",
        default: "https://github.com",
        meaning: "Where an action of another repository a workflow's step uses is fetched from, as <this>/<owner>/<repo>.git, with no credential, before the job starts: a mirror, for a site that does not reach github.com",
    },
    Knob {
        name: "CI_ACTIONS_MIRROR_RAW",
        default: "— (github.com's raw files, where no mirror is named)",
        meaning: "Where one file of an action's repository is read at a ref, with {owner}, {repo}, {ref} and {path} in it: how dagron-ci reads the action.yml of an action a step uses to fetch the actions it uses in turn. With a mirror named and this unset, an action's step that uses an action of another repository is refused",
    },
    Knob {
        name: "CI_AGENT_RUNS",
        default: "refuse",
        meaning: "What a run an agent asks for through the run API gets (a caller whose subject is agent:<name>, as an agent's token presents): refuse, or untrusted, which runs it as an agent run, under the floor an untrusted run is held to, on a cell whose dagron-api records it so; an agent reruns and cancels the runs it asked for, and no other",
    },
    Knob {
        name: "CI_API_TOKEN_FILE",
        default: "— (required with CI_CELLS)",
        meaning: "File holding the dagron-api token dagron-ci runs workflows with",
    },
    Knob {
        name: "CI_CELLS",
        default: "— (events stay queued)",
        meaning: "name=url pairs, comma-separated: the dagron-api of each cell a repository's runs can go to. Each URL is https://, or http:// to this machine: CI_API_TOKEN_FILE's token, and the credential of each caller of the run API, are sent there",
    },
    Knob {
        name: "CI_CELLS_INSECURE",
        default: "—",
        meaning: "`allow`: a cell in CI_CELLS may be plain http:// to another host, on a closed network where that is accepted, such as a dagron-api beside dagron-ci behind a network policy. CI_API_TOKEN_FILE's token, and each caller's credential, cross that network unencrypted",
    },
    Knob {
        name: "CI_CHECKOUT_TOKEN_SECRETS_DIR",
        default: "—",
        meaning: "Directory holding one file per cell, named as the cell, with the secret that cell's engines present: an engine holding one is answered for its own cell's runs alone",
    },
    Knob {
        name: "CI_CHECKOUT_TOKEN_SECRET_FILE",
        default: "— (POST /ci/v1/checkout-tokens answers 404, unless CI_CHECKOUT_TOKEN_SECRETS_DIR is set)",
        meaning: "File holding the secret engines present (DAGRON_CHECKOUT_TOKEN_SECRET_FILE) to ask for a checkout's credential, for a run on any cell. For one cell: dagron-ci does not start on it where CI_CELLS names two, since an engine of either could ask for the other's run",
    },
    Knob {
        name: "CI_DATABASE_MAX_CONNECTIONS",
        default: "8",
        meaning: "Pool size for CI_DATABASE_URL",
    },
    Knob {
        name: "CI_DATABASE_SCHEMA",
        default: "dagron_ci",
        meaning: "Postgres schema for dagron-ci's tables and migration history; created if missing",
    },
    Knob {
        name: "CI_DATABASE_URL",
        default: "— (required)",
        meaning: "postgres://… or sqlite://…; holds the event queue",
    },
    Knob {
        name: "CI_DEFAULT_TIMEOUT_SECS",
        default: "3600",
        meaning: "The timeout of a compiled pipeline's tasks when the pipeline sets none: 1 to 604800 seconds",
    },
    Knob {
        name: "CI_EVENT_LEASE_SECS",
        default: "60",
        meaning: "How long a worker holds an event before another may take it over",
    },
    Knob {
        name: "CI_EVENT_MAX_ATTEMPTS",
        default: "8",
        meaning: "Attempts at an event before it is left failed",
    },
    Knob {
        name: "CI_EVENT_WORKERS",
        default: "8",
        meaning: "Event workers in this replica; 0 runs the receivers alone",
    },
    Knob {
        name: "CI_FLAKE_DAYS",
        default: "14",
        meaning: "How many days back a job's flakes are counted for CI_FLAKE_ISSUES, and kept: a flake older than this is forgotten",
    },
    Knob {
        name: "CI_FLAKE_ISSUES",
        default: "off",
        meaning: "Whether a job that flakes is filed an issue, and after how many flakes: a number N files one of a job of a trusted run that passed only after an attempt of it failed, N times within CI_FLAKE_DAYS, and writes it again in place as it flakes again; a person closes it, and its flakes are counted again from then. Written with the App's token for issues: write alone, on a repository it is installed on, or with the token CI_GITLAB_TOKENS_DIR keeps for a GitLab host; off keeps no flake and files none",
    },
    Knob {
        name: "CI_FLEET_ENVIRONMENT",
        default: "dagron-ci-fleet",
        meaning: "The environment, in each cell's dagron-api, that a fleet runner's configuration is kept in as a secret while the runner is out; made where a cell has none of the name",
    },
    Knob {
        name: "CI_FLEET_UNTRUSTED_ENVIRONMENT",
        default: "dagron-ci-fleet-untrusted",
        meaning: "The same, of a runner started for a fork's job (a label's untrusted: floor), whose run is not trusted: made where a cell has none, and given a policy that admits such runs of the fleet's where it has none and dagron-ci's token may write one. Not CI_FLEET_ENVIRONMENT's",
    },
    Knob {
        name: "CI_FORK_APPROVAL",
        default: "first-time",
        meaning: "With CI_FORK_PRS=untrusted, which such pull requests wait for an approval before anything of them is run: first-time (their author has no commit in the repository), always, or never",
    },
    Knob {
        name: "CI_FORK_APPROVAL_TIMEOUT_SECS",
        default: "604800",
        meaning: "How long that approval waits before it is taken as refused: a minute to ninety days",
    },
    Knob {
        name: "CI_FORK_APPROVERS",
        default: "—",
        meaning: "Who may give that approval, comma-separated: an email, or group:<name>. Required with CI_FORK_PRS=untrusted unless CI_FORK_APPROVAL=never",
    },
    Knob {
        name: "CI_FORK_PRS",
        default: "refuse",
        meaning: "What a pull request from a fork or a non-member gets: refuse, or untrusted, which runs it as an untrusted run on a cell whose engine runs such work",
    },
    Knob {
        name: "CI_GITHUB_API_FOR_JOBS",
        default: "— (a job reaches GitHub's API itself)",
        meaning: "Where a workflow's job reaches dagron-ci's GitHub API door (`/ci/v1/github-api`, at a URL jobs can reach): its GITHUB_API_URL. The door answers download-artifact's `run-id` for a run of dagron's, of the job's repository, and passes every other call on to GitHub as it came. Needs CI_CHECKOUT_TOKEN_SECRET_FILE (or _SECRETS_DIR), which signs its links, and CI_API_TOKEN_FILE",
    },
    Knob {
        name: "CI_GITHUB_API_URL",
        default: "https://api.github.com",
        meaning: "Where github.com's REST API is reached: an egress proxy or a cache in front of it. https://, or http:// to this machine; CI_GITHUB_TOKEN_FILE's token, and the App's, go there",
    },
    Knob {
        name: "CI_GITHUB_APP_ID",
        default: "— (checkouts without a credential)",
        meaning: "The GitHub App dagron-ci acts as on github.com: it mints each checkout's installation token (contents: read, the one repository), and the token dagron-ci's own calls for a repository are made with where the App is installed on it; with CI_GITHUB_APP_KEY_FILE, or CI_GITHUB_APP_TOKEN_URL",
    },
    Knob {
        name: "CI_GITHUB_APP_KEY_FILE",
        default: "— (with CI_GITHUB_APP_ID)",
        meaning: "File holding that App's private key, PEM, which signs its tokens",
    },
    Knob {
        name: "CI_GITHUB_APP_TOKEN_SECRET_FILE",
        default: "— (with CI_GITHUB_APP_TOKEN_URL)",
        meaning: "File holding the secret dagron-ci asks CI_GITHUB_APP_TOKEN_URL for tokens under, as a bearer token",
    },
    Knob {
        name: "CI_GITHUB_APP_TOKEN_URL",
        default: "— (dagron-ci holds the App's key)",
        meaning: "Where the App's installation tokens are asked for, in place of CI_GITHUB_APP_KEY_FILE: a service that holds the key, for an App whose one key must not be in every dagron-ci it serves. POST {repository, permissions, one_repository} answers GitHub's {token, expires_at}, 404 where the App is not installed for the asker. https://, or http:// to this machine",
    },
    Knob {
        name: "CI_GITHUB_APP_TOKEN_URL_INSECURE",
        default: "—",
        meaning: "`allow`: CI_GITHUB_APP_TOKEN_URL may be plain http:// to another host, on a closed network where that is accepted. Its secret, and every token it gives, cross that network unencrypted",
    },
    Knob {
        name: "CI_GITHUB_CHECKS",
        default: "off",
        meaning: "Whether a job is reported as a check run: on reports each job of a repository the App is installed on with checks: write as a check run and not a commit status, under the same name, and takes a check's Re-run as a rerun of its run; off, and for any other repository, a job is a commit status",
    },
    Knob {
        name: "CI_GITHUB_DOCKER",
        default: "— (no job is given a Docker daemon)",
        meaning: "The repositories whose workflow jobs may have a Docker daemon of their own, which is privileged on the engine's host (unless the engine runs it on Sysbox): owner/* and owner/repo, comma-separated. A listed repository's `github.docker: true` gives each job of its trusted runs one, where the engine offers it (DAGRON_DOCKER_DAEMON=dind); any other repository's asks for nothing",
    },
    Knob {
        name: "CI_GITHUB_FLEET_FILE",
        default: "— (no job of GitHub Actions' own is served)",
        meaning: "File holding the fleet's label map, YAML: each label a job's runs-on may ask for, with the runner image it is run from and the runner class, pool, priority and time it is given; read and checked at start; needs CI_GITHUB_APP_ID, and CI_CELLS with CI_API_TOKEN_FILE",
    },
    Knob {
        name: "CI_GITHUB_OIDC",
        default: "off",
        meaning: "Whether a workflow's job that may ask for an OIDC token (permissions: id-token: write) is given the engine's: on gives a trusted run's such job the identity the engine gives a task (DAGRON_OIDC_REQUEST_URL), for the forge's own audience and those the repository names (github.oidc_audiences), with GitHub's two variables; off gives none, and the job runs without",
    },
    Knob {
        name: "CI_GITHUB_PR_COMMENT",
        default: "off",
        meaning: "Whether a pull request is given a comment that says how the runs of its latest commit went: on keeps one on each pull request of a repository the App is installed on with pull requests: write, and writes it again in place as its runs change; off, and for any other repository, none is written",
    },
    Knob {
        name: "CI_GITHUB_TOKEN",
        default: "off",
        meaning: "Whether a workflow's job is given a GITHUB_TOKEN: on asks the engine to mint each attempt one, from this dagron-ci's token service and its GitHub App, for the job's repository with what its permissions grant (contents: read where it says nothing); a run that is not trusted is given read alone. off gives none",
    },
    Knob {
        name: "CI_GITHUB_TOKEN_FILE",
        default: "— (github.com is read without one)",
        meaning: "File holding a GitHub token for the github.com repositories the App is not installed on (or all of them, without CI_GITHUB_APP_ID): refs, pull requests, pipelines and changed files are read with it, and statuses posted; sent to no other host",
    },
    Knob {
        name: "CI_GITHUB_WEBHOOK_SECRET_FILE",
        default: "— (the GitHub receiver answers 404)",
        meaning:
            "File holding the webhook secret POST /hooks/github verifies X-Hub-Signature-256 with",
    },
    Knob {
        name: "CI_GITHUB_WORKFLOWS",
        default: "off",
        meaning: "Whether a repository's .github/workflows files are read: on runs the ones a repository lists in its .dagron/ci.yaml (github.workflows), as pipelines, by level 1 of the GitHub Actions front-end; off reads none, and asks for no .dagron/ci.yaml",
    },
    Knob {
        name: "CI_GITLAB_FLEET_FILE",
        default: "— (no job of GitLab CI's own is served)",
        meaning: "File holding the GitLab fleet's label map, YAML, as CI_GITHUB_FLEET_FILE's: each tag a job may name, and the runner image, runner class, pool, priority and time a job that names it is given, its runner GitLab's own (gitlab-runner run-single). Read and checked at start. Needs CI_CELLS with CI_API_TOKEN_FILE, and CI_GITLAB_TOKENS_DIR's token for each host whose projects it serves, which makes and removes the runners; a job is heard of by the Job Hook, so some replica needs CI_GITLAB_WEBHOOK_SECRET_FILE",
    },
    Knob {
        name: "CI_GITLAB_MR_NOTES",
        default: "off",
        meaning: "Whether a GitLab merge request is given a note that says how the runs of its latest commit went, as CI_GITHUB_PR_COMMENT gives a pull request a comment: on keeps one on each merge request of a project on a host CI_GITLAB_TOKENS_DIR keeps a token for, written with that token, and writes it again in place as its runs change; off, and for any other project, none is written",
    },
    Knob {
        name: "CI_GITLAB_PIPELINES",
        default: "off",
        meaning: "Whether a GitLab project's .gitlab-ci.yml is read: on runs the one a project names in its .dagron/ci.yaml (gitlab.pipeline), with the files it includes from the project, by level 1 of the GitLab CI front-end; off reads none",
    },
    Knob {
        name: "CI_GITLAB_TOKENS_DIR",
        default: "— (GitLab is read without credentials, and given no statuses)",
        meaning: "Directory of GitLab access tokens, one file per GitLab host, named as the host (gitlab.com) and holding an access token with the api scope: that host's projects' refs, merge requests, pipelines and changed files are read with it, statuses posted, and each checkout's own project access token minted (for that, a personal access token of the project's Maintainer, a user's or a service account's: a group's or a project's access token mints none); each is sent to its own host and no other, and never to a job",
    },
    Knob {
        name: "CI_GITLAB_WEBHOOK_SECRET_FILE",
        default: "— (the GitLab receiver answers 404)",
        meaning: "File holding the secret token POST /hooks/gitlab compares X-Gitlab-Token with",
    },
    Knob {
        name: "CI_LISTEN",
        default: "0.0.0.0:8090",
        meaning: "HTTP listen address",
    },
    Knob {
        name: "CI_OBJECT_STORE_URL",
        default: "— (a payload over 64 KiB is not kept, and no record is written out)",
        meaning: "dagron-ci's object store: s3://bucket/prefix, gs://bucket/prefix, az://container/prefix (credentials from the environment, as the engine's stores read them) or file:///path. A webhook payload too large for its event's row (over 64 KiB) is kept there, named by its event; and a day past CI_RECORD_DAYS is written there, as zstd segments read back before the day leaves the database. Unset, such a payload is not kept, and a day past CI_RECORD_DAYS is dropped unwritten",
    },
    Knob {
        name: "CI_POLL_SECS",
        default: "300",
        meaning: "How often the poll sweep reads a repository that sets no interval of its own: 30 to 86400 seconds, or 0 for never",
    },
    Knob {
        name: "CI_RECORD_DAYS",
        default: "— (every record is kept)",
        meaning: "How many whole days before today dagron-ci's records are kept in the database, 1 to 3650: each hour the older days are written to CI_OBJECT_STORE_URL, where it is set, and dropped: their events (a partition of ci_events a day on Postgres, the rows on SQLite), their runs' rows, their run API requests, their report log, their deliveries and the payloads their events kept in the store. Who caused a run stays on the run's record in the engine's database. Unset, nothing is dropped",
    },
    Knob {
        name: "CI_RUN_URL",
        default: "— (statuses link nowhere)",
        meaning: "The page a commit status links to: an http(s) URL in which {run_id} and {cell} are the run's",
    },
    Knob {
        name: "CI_SEGMENT_DAYS",
        default: "— (a segment is kept)",
        meaning: "How many days after its day a segment of records written to CI_OBJECT_STORE_URL is kept, 1 to 36500; each hourly pass sets every segment's time from it. Needs CI_OBJECT_STORE_URL. Unset, segments are kept",
    },
    Knob {
        name: "CI_URL_FOR_JOBS",
        default: "— (no patch bundles)",
        meaning: "Where a job reaches dagron-ci, an https:// URL with no path (http:// to this machine, or with CI_URL_FOR_JOBS_INSECURE): where a run on a patch bundle's checkout fetches the bundle (`/ci/v1/bundles/{id}/content`, a link of an hour). With CI_OBJECT_STORE_URL, which keeps the bundles, and CI_CHECKOUT_TOKEN_SECRET_FILE (or _SECRETS_DIR), which signs the links, POST /ci/v1/bundles takes bundles",
    },
    Knob {
        name: "CI_URL_FOR_JOBS_INSECURE",
        default: "— (https only, or http:// to this machine)",
        meaning: "`allow`: CI_URL_FOR_JOBS may be plain http:// to another host, on a closed network where that is accepted. A link to a bundle is a bearer's: whoever sees it in passing reads the change until it expires",
    },
];

#[derive(Clone)]
pub struct Config {
    pub listen: SocketAddr,
    pub database_url: String,
    pub database_schema: String,
    pub database_max_connections: u32,
    /// The GitHub webhook secret, or `None` when no receiver is configured.
    pub github_webhook_secret: Option<Vec<u8>>,
    /// The GitLab webhook's secret token, or `None` when no receiver is
    /// configured.
    pub gitlab_webhook_secret: Option<Vec<u8>>,
    /// Each GitLab host's access token, by the host, in lower case
    /// (`CI_GITLAB_TOKENS_DIR`).
    pub gitlab_tokens: BTreeMap<String, String>,
    /// Cell name to its dagron-api base URL, without a trailing slash. Empty:
    /// no worker runs anything, and events stay queued.
    pub cells: BTreeMap<String, String>,
    /// dagron-ci's dagron-api token. Set whenever `cells` is not empty.
    pub api_token: Option<String>,
    pub event_workers: usize,
    pub event_lease: Duration,
    pub event_max_attempts: i64,
    /// How many whole days before today an event is kept
    /// (`CI_RECORD_DAYS`); `None` keeps every one.
    pub record_days: Option<u32>,
    /// dagron-ci's object store (`CI_OBJECT_STORE_URL`): payloads too large
    /// for their event's row, and the segments records are written to.
    pub object_store_url: Option<String>,
    /// How long a segment is kept after its day (`CI_SEGMENT_DAYS`); `None`
    /// keeps it.
    pub segment_days: Option<u32>,
    /// The poll interval of a repository that sets none; 0 polls only those
    /// that do.
    pub poll_secs: u64,
    /// The token github.com is read with.
    pub github_token: Option<String>,
    /// Where github.com's REST API is reached, when not at its own address.
    pub github_api: Option<String>,
    /// Where a workflow's job reaches dagron-ci's GitHub API door, which is
    /// its `GITHUB_API_URL` then (`CI_GITHUB_API_FOR_JOBS`).
    pub github_api_for_jobs: Option<String>,
    /// Where jobs reach dagron-ci (CI_URL_FOR_JOBS): where a patched
    /// checkout fetches its bundle. `None`: no bundle is taken.
    pub url_for_jobs: Option<String>,
    /// The timeout of a compiled pipeline's tasks when it sets none.
    pub default_timeout_secs: u64,
    /// The page a commit status links to, with `{run_id}` and `{cell}` in it.
    pub run_url: Option<String>,
    /// The secret an engine asks for a checkout's credential with, for a run
    /// on any cell.
    pub checkout_token_secret: Option<String>,
    /// Each cell's own secret: an engine presenting one is answered for that
    /// cell's runs alone. The endpoint is off when this is empty and
    /// `checkout_token_secret` is `None`.
    pub checkout_cell_secrets: BTreeMap<String, String>,
    /// The GitHub App a checkout's credential is minted as, its key read and
    /// checked at start.
    pub github_app: Option<crate::checkout::GitHubApp>,
    /// What a pull request from a fork or a non-member gets, where it is run
    /// (`CI_FORK_PRS=untrusted`). `None` refuses it.
    pub forks: Option<crate::forks::Forks>,
    /// Whether a run an agent asks for is run, as an agent run
    /// (`CI_AGENT_RUNS=untrusted`): refused otherwise.
    pub agent_runs: bool,
    /// Whether a repository's `.github/workflows` files are read
    /// (`CI_GITHUB_WORKFLOWS=on`).
    pub github_workflows: bool,
    /// Whether a GitLab project's `.gitlab-ci.yml` is read
    /// (`CI_GITLAB_PIPELINES=on`).
    pub gitlab_pipelines: bool,
    /// Where the actions a workflow's steps use are fetched from, and
    /// which may be (`CI_ACTIONS_MIRROR`, `CI_ACTIONS_ALLOW`).
    pub actions: crate::pipeline::github::ActionSource,
    /// Whether a workflow's job that may ask for an OIDC token is given
    /// the engine's (`CI_GITHUB_OIDC=on`).
    pub github_oidc: bool,
    /// Whether a workflow's job is given a `GITHUB_TOKEN`
    /// (`CI_GITHUB_TOKEN=on`).
    pub github_job_token: bool,
    /// The repositories whose jobs may have a Docker daemon of their own
    /// (`CI_GITHUB_DOCKER`).
    pub github_docker: crate::pipeline::github::DockerRepos,
    /// Whether a job is reported as a check run, where the App can post one
    /// (`CI_GITHUB_CHECKS=on`).
    pub github_checks: bool,
    /// Whether a pull request is given a comment that says how its runs
    /// went, where the App can write one (`CI_GITHUB_PR_COMMENT=on`).
    pub github_pr_comment: bool,
    /// Whether a GitLab merge request is given a note that says the same,
    /// with its host's token (`CI_GITLAB_MR_NOTES=on`).
    pub gitlab_mr_notes: bool,
    /// How many flakes of a job, within how many days, file an issue of it
    /// (`CI_FLAKE_ISSUES`, `CI_FLAKE_DAYS`). `None`: none is filed.
    pub flake_issues: Option<(i64, i64)>,
    /// The labels of GitHub Actions' own jobs this dagron-ci serves, and
    /// what each is run with (`CI_GITHUB_FLEET_FILE`). `None` serves none.
    pub fleet: Option<crate::fleet::Fleet>,
    /// The labels GitLab CI's own jobs are served for, by tag
    /// (`CI_GITLAB_FLEET_FILE`). `None` serves none.
    pub gitlab_fleet: Option<crate::fleet::Fleet>,
    /// The environment a fleet runner's configuration is a secret of
    /// (`CI_FLEET_ENVIRONMENT`).
    pub fleet_environment: String,
    /// The same, of a runner started for a fork's job, which admits runs
    /// that are not trusted (`CI_FLEET_UNTRUSTED_ENVIRONMENT`).
    pub fleet_untrusted_environment: String,
}

impl std::fmt::Debug for Config {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Config")
            .field("listen", &self.listen)
            .field("database_url", &redact_url(&self.database_url))
            .field("database_schema", &self.database_schema)
            .field("database_max_connections", &self.database_max_connections)
            .field(
                "github_webhook_secret",
                &self.github_webhook_secret.as_ref().map(|_| "<redacted>"),
            )
            .field(
                "gitlab_webhook_secret",
                &self.gitlab_webhook_secret.as_ref().map(|_| "<redacted>"),
            )
            .field(
                "gitlab_tokens",
                &self
                    .gitlab_tokens
                    .keys()
                    .map(|host| (host.as_str(), "<redacted>"))
                    .collect::<Vec<_>>(),
            )
            .field("cells", &self.cells)
            .field("api_token", &self.api_token.as_ref().map(|_| "<redacted>"))
            .field("event_workers", &self.event_workers)
            .field("event_lease", &self.event_lease)
            .field("event_max_attempts", &self.event_max_attempts)
            .field("record_days", &self.record_days)
            .field("object_store_url", &self.object_store_url)
            .field("segment_days", &self.segment_days)
            .field("poll_secs", &self.poll_secs)
            .field(
                "github_token",
                &self.github_token.as_ref().map(|_| "<redacted>"),
            )
            .field("github_api", &self.github_api)
            .field("default_timeout_secs", &self.default_timeout_secs)
            .field("forks", &self.forks)
            .field("run_url", &self.run_url)
            .field(
                "checkout_token_secret",
                &self.checkout_token_secret.as_ref().map(|_| "<redacted>"),
            )
            .field(
                "checkout_cell_secrets",
                &self
                    .checkout_cell_secrets
                    .keys()
                    .map(|cell| (cell.as_str(), "<redacted>"))
                    .collect::<Vec<_>>(),
            )
            .field(
                "github_app",
                &self.github_app.as_ref().map(|app| (app.id(), app.keeper())),
            )
            .finish()
    }
}

impl Config {
    pub fn from_env() -> anyhow::Result<Config> {
        Config::from_lookup(|name| std::env::var(name).ok())
    }

    /// The same as [`Config::from_env`], with the environment supplied by the
    /// caller, so tests never touch the process's own.
    pub fn from_lookup(get: impl Fn(&str) -> Option<String>) -> anyhow::Result<Config> {
        let get = |name: &str| {
            get(name)
                .map(|v| v.trim().to_string())
                .filter(|v| !v.is_empty())
        };

        let listen = match get("CI_LISTEN") {
            Some(v) => v.parse().map_err(|_| {
                anyhow::anyhow!("CI_LISTEN={v:?} is not an address such as 0.0.0.0:8090")
            })?,
            None => SocketAddr::from(([0, 0, 0, 0], 8090)),
        };

        let database_url = get("CI_DATABASE_URL").ok_or_else(|| {
            anyhow::anyhow!("CI_DATABASE_URL is required (postgres://… or sqlite://…)")
        })?;
        if !is_postgres(&database_url) && !is_sqlite(&database_url) {
            anyhow::bail!(
                "CI_DATABASE_URL must start with postgres://, postgresql:// or sqlite:, not {:?}",
                redact_url(&database_url)
            );
        }

        let database_schema = get("CI_DATABASE_SCHEMA").unwrap_or_else(|| "dagron_ci".into());
        if !is_identifier(&database_schema) {
            anyhow::bail!(
                "CI_DATABASE_SCHEMA={database_schema:?} must be a lowercase identifier: \
                 a letter or _, then letters, digits or _, at most 63 bytes"
            );
        }

        let database_max_connections = match get("CI_DATABASE_MAX_CONNECTIONS") {
            Some(v) => match v.parse::<u32>() {
                Ok(n) if n >= 1 => n,
                _ => anyhow::bail!(
                    "CI_DATABASE_MAX_CONNECTIONS={v:?} must be a whole number of at least 1"
                ),
            },
            None => 8,
        };

        let github_webhook_secret = match get("CI_GITHUB_WEBHOOK_SECRET_FILE") {
            Some(path) => Some(read_secret(&path, "CI_GITHUB_WEBHOOK_SECRET_FILE")?),
            None => None,
        };
        let gitlab_webhook_secret = match get("CI_GITLAB_WEBHOOK_SECRET_FILE") {
            Some(path) => Some(read_secret(&path, "CI_GITLAB_WEBHOOK_SECRET_FILE")?),
            None => None,
        };
        let gitlab_tokens = match get("CI_GITLAB_TOKENS_DIR") {
            Some(dir) => read_host_tokens(&dir)?,
            None => BTreeMap::new(),
        };

        let cells = match get("CI_CELLS") {
            Some(v) => parse_cells(&v, get("CI_CELLS_INSECURE").as_deref())?,
            None => BTreeMap::new(),
        };
        let api_token = match get("CI_API_TOKEN_FILE") {
            Some(path) => Some(read_token(&path, "CI_API_TOKEN_FILE")?),
            None => None,
        };
        if !cells.is_empty() && api_token.is_none() {
            anyhow::bail!(
                "CI_CELLS is set but CI_API_TOKEN_FILE is not: dagron-api refuses a run \
                 without a token, so every event would fail"
            );
        }

        let event_workers = number(&get, "CI_EVENT_WORKERS", 8, 0, 256)? as usize;
        let event_lease = Duration::from_secs(number(&get, "CI_EVENT_LEASE_SECS", 60, 5, 3600)?);
        let event_max_attempts = number(&get, "CI_EVENT_MAX_ATTEMPTS", 8, 1, 100)? as i64;
        let record_days = match get("CI_RECORD_DAYS") {
            None => None,
            Some(_) => Some(number(&get, "CI_RECORD_DAYS", 0, 1, 3650)? as u32),
        };
        let object_store_url = get("CI_OBJECT_STORE_URL").map(|u| u.trim().to_string());
        let segment_days = match get("CI_SEGMENT_DAYS") {
            None => None,
            Some(_) => Some(number(&get, "CI_SEGMENT_DAYS", 0, 1, 36500)? as u32),
        };
        if let (Some(keep), Some(segments)) = (record_days, segment_days) {
            if segments <= keep {
                anyhow::bail!(
                    "CI_SEGMENT_DAYS={segments} is not past CI_RECORD_DAYS={keep}: a day's segments \
                     would be past their time as they are written, and deleted in the pass that \
                     writes them"
                );
            }
        }
        if segment_days.is_some() && object_store_url.is_none() {
            anyhow::bail!("CI_SEGMENT_DAYS says how long segments are kept in CI_OBJECT_STORE_URL, which is not set");
        }
        if let Some(url) = &object_store_url {
            if !["s3://", "gs://", "az://", "file:///"]
                .iter()
                .any(|s| url.starts_with(s))
            {
                anyhow::bail!(
                    "CI_OBJECT_STORE_URL={url:?} is none of s3://, gs://, az:// or file:///path"
                );
            }
        }

        let poll_secs = number(&get, "CI_POLL_SECS", 300, 0, MAX_POLL_SECS)?;
        if (1..MIN_POLL_SECS).contains(&poll_secs) {
            anyhow::bail!(
                "CI_POLL_SECS={poll_secs} is under {MIN_POLL_SECS}: a forge's rate limit \
                 would not last; 0 turns polling off"
            );
        }
        let github_token = match get("CI_GITHUB_TOKEN_FILE") {
            Some(path) => Some(read_token(&path, "CI_GITHUB_TOKEN_FILE")?),
            None => None,
        };
        let github_api = match get("CI_GITHUB_API_URL") {
            Some(url) => Some(parse_github_api(&url)?),
            None => None,
        };
        let url_for_jobs = match get("CI_URL_FOR_JOBS") {
            Some(url) => Some(parse_url_for_jobs(
                &url,
                get("CI_URL_FOR_JOBS_INSECURE").as_deref(),
            )?),
            None => None,
        };
        let github_api_for_jobs = match get("CI_GITHUB_API_FOR_JOBS") {
            Some(url) => Some(parse_api_for_jobs(&url)?),
            None => None,
        };
        let default_timeout_secs = default_timeout_secs(&get)?;
        let run_url = match get("CI_RUN_URL") {
            Some(url) => Some(parse_run_url(&url)?),
            None => None,
        };

        let checkout_token_secret = match get("CI_CHECKOUT_TOKEN_SECRET_FILE") {
            Some(path) => Some(read_token(&path, "CI_CHECKOUT_TOKEN_SECRET_FILE")?),
            None => None,
        };
        let checkout_cell_secrets = match get("CI_CHECKOUT_TOKEN_SECRETS_DIR") {
            Some(dir) => read_cell_secrets(&dir, &cells)?,
            None => BTreeMap::new(),
        };
        // The one secret answers for every cell. With one cell that is the
        // cell's own. With two, an engine of either could ask for the
        // credential of the other's run.
        if checkout_token_secret.is_some() && cells.len() > 1 {
            anyhow::bail!(
                "CI_CHECKOUT_TOKEN_SECRET_FILE's secret answers for every cell, and CI_CELLS \
                 names {}: an engine of one cell could ask for the credential of another's \
                 run. Give each cell a secret of its own, in CI_CHECKOUT_TOKEN_SECRETS_DIR",
                cells.len()
            );
        }
        if let Some(shared) = &checkout_token_secret {
            if let Some(cell) = checkout_cell_secrets
                .iter()
                .find_map(|(cell, secret)| (secret == shared).then_some(cell))
            {
                anyhow::bail!(
                    "CI_CHECKOUT_TOKEN_SECRETS_DIR: cell {cell:?}'s secret is \
                     CI_CHECKOUT_TOKEN_SECRET_FILE's, which answers for every cell; give each \
                     cell a secret of its own"
                );
            }
        }
        let github_app = parse_github_app(&get)?;

        let forks = crate::forks::Forks::from_lookup(&get)?;
        let agent_runs = match get("CI_AGENT_RUNS").as_deref().map(str::trim) {
            None | Some("") | Some("refuse") => false,
            Some("untrusted") => true,
            Some(other) => anyhow::bail!("CI_AGENT_RUNS={other:?} must be refuse or untrusted"),
        };
        let github_workflows = match get("CI_GITHUB_WORKFLOWS").as_deref().map(str::trim) {
            None | Some("") | Some("off") => false,
            Some("on") => true,
            Some(other) => anyhow::bail!("CI_GITHUB_WORKFLOWS={other:?} must be on or off"),
        };
        let gitlab_pipelines = match get("CI_GITLAB_PIPELINES").as_deref().map(str::trim) {
            None | Some("") | Some("off") => false,
            Some("on") => true,
            Some(other) => anyhow::bail!("CI_GITLAB_PIPELINES={other:?} must be on or off"),
        };
        let github_oidc = match get("CI_GITHUB_OIDC").as_deref().map(str::trim) {
            None | Some("") | Some("off") => false,
            Some("on") => true,
            Some(other) => anyhow::bail!("CI_GITHUB_OIDC={other:?} must be on or off"),
        };
        let github_job_token = match get("CI_GITHUB_TOKEN").as_deref().map(str::trim) {
            None | Some("") | Some("off") => false,
            Some("on") if github_app.is_none() => anyhow::bail!(
                "CI_GITHUB_TOKEN=on needs the App that mints it (CI_GITHUB_APP_ID): a job's \
                 token is an installation token of the App's"
            ),
            Some("on") => true,
            Some(other) => anyhow::bail!("CI_GITHUB_TOKEN={other:?} must be on or off"),
        };
        let actions = crate::pipeline::github::ActionSource::parse(
            get("CI_ACTIONS_MIRROR").as_deref().unwrap_or_default(),
            get("CI_ACTIONS_ALLOW").as_deref().unwrap_or_default(),
        )
        .and_then(|source| {
            source.with_raw(get("CI_ACTIONS_MIRROR_RAW").as_deref().unwrap_or_default())
        })
        .map_err(|why| anyhow::anyhow!(why))?;
        let github_docker = crate::pipeline::github::DockerRepos::parse(
            get("CI_GITHUB_DOCKER").as_deref().unwrap_or_default(),
        )
        .map_err(|why| anyhow::anyhow!(why))?;
        let github_checks = match get("CI_GITHUB_CHECKS").as_deref().map(str::trim) {
            None | Some("") | Some("off") => false,
            Some("on") if github_app.is_none() => anyhow::bail!(
                "CI_GITHUB_CHECKS=on needs the App that posts them (CI_GITHUB_APP_ID): a check \
                 run is an App's, and no token's"
            ),
            Some("on") => true,
            Some(other) => anyhow::bail!("CI_GITHUB_CHECKS={other:?} must be on or off"),
        };
        let gitlab_mr_notes = match get("CI_GITLAB_MR_NOTES").as_deref().map(str::trim) {
            None | Some("") | Some("off") => false,
            Some("on") if gitlab_tokens.is_empty() => anyhow::bail!(
                "CI_GITLAB_MR_NOTES=on needs a token to write the notes with: CI_GITLAB_TOKENS_DIR \
                 keeps none"
            ),
            Some("on") => true,
            Some(other) => anyhow::bail!("CI_GITLAB_MR_NOTES={other:?} must be on or off"),
        };
        let flake_days = number(&get, "CI_FLAKE_DAYS", 14, 1, 90)? as i64;
        let flake_issues = match get("CI_FLAKE_ISSUES").as_deref().map(str::trim) {
            None | Some("") | Some("off") => None,
            Some(_) if github_app.is_none() && gitlab_tokens.is_empty() => anyhow::bail!(
                "CI_FLAKE_ISSUES needs what writes the issues: the App (CI_GITHUB_APP_ID), or a \
                 GitLab host's token (CI_GITLAB_TOKENS_DIR)"
            ),
            Some(_) => Some((
                number(&get, "CI_FLAKE_ISSUES", 0, 1, 1000)? as i64,
                flake_days,
            )),
        };
        let github_pr_comment = match get("CI_GITHUB_PR_COMMENT").as_deref().map(str::trim) {
            None | Some("") | Some("off") => false,
            Some("on") if github_app.is_none() => anyhow::bail!(
                "CI_GITHUB_PR_COMMENT=on needs the App that writes it (CI_GITHUB_APP_ID): the \
                 comment is the App's, with a token for it alone"
            ),
            Some("on") => true,
            Some(other) => anyhow::bail!("CI_GITHUB_PR_COMMENT={other:?} must be on or off"),
        };
        let environment_named = |knob: &str, default: &str| -> anyhow::Result<String> {
            match get(knob).map(|name| name.trim().to_string()) {
                None => Ok(default.to_string()),
                Some(name)
                    if !name.is_empty()
                        && name.len() <= 64
                        && name
                            .bytes()
                            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_')) =>
                {
                    Ok(name)
                }
                Some(name) => anyhow::bail!(
                    "{knob}={name:?} is an environment's name: letters, digits, '-' and '_', at \
                     most 64"
                ),
            }
        };
        let fleet_environment = environment_named("CI_FLEET_ENVIRONMENT", "dagron-ci-fleet")?;
        let fleet_untrusted_environment = environment_named(
            "CI_FLEET_UNTRUSTED_ENVIRONMENT",
            "dagron-ci-fleet-untrusted",
        )?;
        // One environment for both would admit a fork's job's runner to the
        // configurations of every other runner.
        if fleet_untrusted_environment == fleet_environment {
            anyhow::bail!(
                "CI_FLEET_UNTRUSTED_ENVIRONMENT and CI_FLEET_ENVIRONMENT are both \
                 {fleet_environment:?}: the environment of a fork's job's runners admits runs \
                 that are not trusted, and would admit them to every runner's configuration"
            );
        }
        let fleet = match get("CI_GITHUB_FLEET_FILE").filter(|path| !path.trim().is_empty()) {
            None => None,
            // Refused, and not served with a warning: a job's delivery would
            // be taken and kept, and nothing would ever start it a runner.
            Some(_) if cells.is_empty() || api_token.is_none() => anyhow::bail!(
                "CI_GITHUB_FLEET_FILE needs a cell to start each runner in, and dagron-ci's \
                 token for it (CI_CELLS, CI_API_TOKEN_FILE)"
            ),
            Some(_) if github_app.is_none() => anyhow::bail!(
                "CI_GITHUB_FLEET_FILE needs the App that registers each runner \
                 (CI_GITHUB_APP_ID)"
            ),
            Some(path) => {
                let text = std::fs::read_to_string(path.trim())
                    .map_err(|e| anyhow::anyhow!("CI_GITHUB_FLEET_FILE={path:?}: {e}"))?;
                Some(
                    crate::fleet::Fleet::parse(&text)
                        .map_err(|why| anyhow::anyhow!("CI_GITHUB_FLEET_FILE={path:?} {why}"))?,
                )
            }
        };

        let gitlab_fleet = match get("CI_GITLAB_FLEET_FILE").filter(|path| !path.trim().is_empty())
        {
            None => None,
            Some(_) if cells.is_empty() || api_token.is_none() => anyhow::bail!(
                "CI_GITLAB_FLEET_FILE needs a cell to start each runner in, and dagron-ci's \
                 token for it (CI_CELLS, CI_API_TOKEN_FILE)"
            ),
            Some(_) if gitlab_tokens.is_empty() => anyhow::bail!(
                "CI_GITLAB_FLEET_FILE needs a token for each GitLab whose jobs it serves, \
                 which makes and removes the runners (CI_GITLAB_TOKENS_DIR)"
            ),
            Some(path) => {
                let text = std::fs::read_to_string(path.trim())
                    .map_err(|e| anyhow::anyhow!("CI_GITLAB_FLEET_FILE={path:?}: {e}"))?;
                Some(
                    crate::fleet::Fleet::parse_gitlab(&text)
                        .map_err(|why| anyhow::anyhow!("CI_GITLAB_FLEET_FILE={path:?} {why}"))?,
                )
            }
        };
        // A label is counted against its quota by its name, whichever
        // forge's job asked for it: one name in both maps would be two
        // labels' runners counted as one.
        if let (Some(github), Some(gitlab)) = (&fleet, &gitlab_fleet) {
            if let Some(both) = github.labels().find(|l| gitlab.class(l).is_some()) {
                anyhow::bail!(
                    "{both:?} is a label of both CI_GITHUB_FLEET_FILE's and \
                     CI_GITLAB_FLEET_FILE's: name each fleet's labels apart"
                );
            }
        }

        // A door with nothing to sign its links with, or no token to ask a
        // cell with, would answer nothing of its own: it is refused here,
        // not served as a pass-through that looks like one.
        if github_api_for_jobs.is_some() {
            if checkout_token_secret.is_none() && checkout_cell_secrets.is_empty() {
                anyhow::bail!(
                    "CI_GITHUB_API_FOR_JOBS is set, and neither CI_CHECKOUT_TOKEN_SECRET_FILE \
                     nor CI_CHECKOUT_TOKEN_SECRETS_DIR is: the door reads the tokens that service \
                     mints, and signs its links with its secret"
                );
            }
            if api_token.is_none() {
                anyhow::bail!(
                    "CI_GITHUB_API_FOR_JOBS is set and CI_API_TOKEN_FILE is not: the door asks a \
                     run's cell for its artifacts with that token"
                );
            }
        }
        Ok(Config {
            listen,
            database_url,
            database_schema,
            database_max_connections,
            github_webhook_secret,
            gitlab_webhook_secret,
            gitlab_tokens,
            cells,
            api_token,
            event_workers,
            event_lease,
            event_max_attempts,
            record_days,
            object_store_url,
            segment_days,
            poll_secs,
            github_token,
            github_api,
            github_api_for_jobs,
            url_for_jobs,
            default_timeout_secs,
            run_url,
            checkout_token_secret,
            checkout_cell_secrets,
            github_app,
            forks,
            agent_runs,
            github_workflows,
            gitlab_pipelines,
            actions,
            github_oidc,
            github_job_token,
            github_docker,
            github_checks,
            github_pr_comment,
            gitlab_mr_notes,
            flake_issues,
            fleet,
            gitlab_fleet,
            fleet_environment,
            fleet_untrusted_environment,
        })
    }

    /// One line per knob at start, so an operator sees what this process runs
    /// with. Secrets are never printed; the path of a secret file is.
    pub fn log_effective(&self) {
        tracing::info!(
            listen = %self.listen,
            database_url = %redact_url(&self.database_url),
            database_schema = %self.database_schema,
            database_max_connections = self.database_max_connections,
            github_receiver = self.github_webhook_secret.is_some(),
            gitlab_receiver = self.gitlab_webhook_secret.is_some(),
            gitlab_hosts = ?self.gitlab_tokens.keys().collect::<Vec<_>>(),
            gitlab_mr_notes = self.gitlab_mr_notes,
            cells = ?self.cells,
            event_workers = self.event_workers,
            event_lease_secs = self.event_lease.as_secs(),
            event_max_attempts = self.event_max_attempts,
            record_days = ?self.record_days,
            object_store = self.object_store_url.as_deref().unwrap_or("—"),
            segment_days = ?self.segment_days,
            poll_secs = self.poll_secs,
            github_token = self.github_token.is_some(),
            github_api = self.github_api.as_deref().unwrap_or("https://api.github.com"),
            run_url = self.run_url.as_deref().unwrap_or("—"),
            checkout_tokens = self.checkout_token_secret.is_some(),
            checkout_cells = ?self.checkout_cell_secrets.keys().collect::<Vec<_>>(),
            github_app = self.github_app.as_ref().map(|app| app.id()),
            github_app_key = self.github_app.as_ref().map(|app| app.keeper()),
            agent_runs = if self.agent_runs { "untrusted" } else { "refuse" },
            github_oidc = self.github_oidc,
            github_job_token = self.github_job_token,
            github_docker = ?self.github_docker.listed(),
            actions_mirror = self.actions.mirror(),
            actions_allowed = ?self.actions.allow,
            "dagron-ci configuration"
        );
        let checkouts =
            self.checkout_token_secret.is_some() || !self.checkout_cell_secrets.is_empty();
        if checkouts && self.github_app.is_none() {
            tracing::info!(
                "CI_GITHUB_APP_ID is unset: a GitHub repository's checkout is fetched without a \
                 credential, so a private one cannot be"
            );
        }
        if self.github_webhook_secret.is_none() && self.gitlab_webhook_secret.is_none() {
            tracing::warn!(
                "CI_GITHUB_WEBHOOK_SECRET_FILE and CI_GITLAB_WEBHOOK_SECRET_FILE are unset: \
                 POST /hooks/github and /hooks/gitlab answer 404, and no forge can deliver events"
            );
        }
        // Said, and not refused as a fleet with no cell is: a replica that
        // takes no GitHub delivery still starts the runners for the jobs
        // another replica took. GitLab's deliveries bring it none.
        if self.github_webhook_secret.is_none() && self.fleet.is_some() {
            tracing::warn!(
                "CI_GITHUB_FLEET_FILE is set, and a fleet hears of a job by its \
                 delivery alone: this replica serves the jobs another replica takes, \
                 and none where no replica has the webhook's secret"
            );
        }
        if self.gitlab_webhook_secret.is_none() && self.gitlab_fleet.is_some() {
            tracing::warn!(
                "CI_GITLAB_FLEET_FILE is set, and a GitLab project's fleet is looked over \
                 when its Job Hook says a job waits: this replica serves the jobs another \
                 replica hears of, and none where no replica has the webhook's secret"
            );
        }
        if self.cells.is_empty() {
            tracing::warn!("CI_CELLS is unset: events are queued and nothing runs them");
        } else if self.event_workers == 0 {
            tracing::warn!("CI_EVENT_WORKERS=0: this replica queues events and runs none");
        }
    }
}

/// The shortest poll interval a repository or CI_POLL_SECS may set.
pub const MIN_POLL_SECS: u64 = 30;
/// The longest: a day.
pub const MAX_POLL_SECS: u64 = 86_400;

/// CI_DEFAULT_TIMEOUT_SECS, for `serve` and for `dagron-ci compile`, which
/// reads nothing else. Blank is unset.
pub fn default_timeout_secs(get: &impl Fn(&str) -> Option<String>) -> anyhow::Result<u64> {
    let get = |name: &str| {
        get(name)
            .map(|v| v.trim().to_string())
            .filter(|v| !v.is_empty())
    };
    number(&get, "CI_DEFAULT_TIMEOUT_SECS", 3600, 1, 604_800)
}

/// A whole number knob, `default` when unset, refused outside `min..=max`.
fn number(
    get: &impl Fn(&str) -> Option<String>,
    name: &str,
    default: u64,
    min: u64,
    max: u64,
) -> anyhow::Result<u64> {
    match get(name) {
        None => Ok(default),
        Some(v) => match v.parse::<u64>() {
            Ok(n) if (min..=max).contains(&n) => Ok(n),
            _ => anyhow::bail!("{name}={v:?} must be a whole number from {min} to {max}"),
        },
    }
}

/// `CI_CELLS`: `name=url` pairs, comma-separated. A name is what a repository
/// is bound to; a URL is the cell's dagron-api.
///
/// dagron-ci's own token for dagron-api goes to that URL with every run it
/// submits or follows, and so does the credential of each caller of the run
/// API, which the cell is asked to recognise. So a cell is `https://`, or
/// `http://` to this machine (a sidecar that carries it on, a test), and
/// dagron-ci starts on no other: never in the clear off this machine.
fn parse_cells(v: &str, insecure: Option<&str>) -> anyhow::Result<BTreeMap<String, String>> {
    let insecure = match insecure.map(str::trim) {
        None | Some("") => false,
        Some("allow") => true,
        Some(other) => anyhow::bail!("CI_CELLS_INSECURE={other:?} is allow, or unset"),
    };
    let mut cells = BTreeMap::new();
    for pair in v.split(',').map(str::trim).filter(|p| !p.is_empty()) {
        let Some((name, url)) = pair.split_once('=') else {
            anyhow::bail!("CI_CELLS: {pair:?} is not name=url");
        };
        let (name, url) = (name.trim(), url.trim().trim_end_matches('/'));
        if !is_cell_name(name) {
            anyhow::bail!(
                "CI_CELLS: cell name {name:?} must be letters, digits, '-', '_' or '.', at most 64"
            );
        }
        let needs =
            || anyhow::anyhow!("CI_CELLS: cell {name:?} needs an https:// URL, not {url:?}");
        if url.contains(char::is_whitespace) {
            return Err(needs());
        }
        let parsed = reqwest::Url::parse(url).map_err(|_| needs())?;
        match parsed.scheme() {
            "https" => {}
            "http" if insecure || to_this_machine(&parsed) => {}
            "http" => anyhow::bail!(
                "CI_CELLS: cell {name:?} at {url:?} must be https://, or http:// to this \
                 machine: CI_API_TOKEN_FILE's token, and the credential of each caller of the \
                 run API, are sent there. On a closed network where that is accepted, set \
                 CI_CELLS_INSECURE=allow"
            ),
            _ => return Err(needs()),
        }
        if parsed.host_str().unwrap_or("").is_empty() {
            return Err(needs());
        }
        if cells.insert(name.to_string(), url.to_string()).is_some() {
            anyhow::bail!("CI_CELLS names cell {name:?} twice");
        }
    }
    if cells.is_empty() {
        anyhow::bail!("CI_CELLS is set but names no cell");
    }
    Ok(cells)
}

/// Whether `url` is plain http. dagron-ci takes plain http for an address
/// of its own machine alone, so what is sent to such a URL is meant to leave
/// no interface.
pub(crate) fn is_plain_http(url: &str) -> bool {
    url.trim_start()
        .get(..7)
        .is_some_and(|scheme| scheme.eq_ignore_ascii_case("http://"))
}

/// The clients dagron-ci's requests are made with: one that uses the
/// environment's proxy, as any client does, and one built alike that uses
/// none. Which is used is chosen for each request, by where it goes
/// ([`Clients::to`]).
///
/// A request over plain http goes to this machine, and by no proxy: one
/// named in the environment (HTTP_PROXY, ALL_PROXY) would be handed the
/// request, and the token in it, and carry both off the machine in the
/// clear. An https request keeps the environment's proxy, since it is TLS
/// to its end and may not be reachable otherwise. The choice is a request's
/// and not a client's, so that a cell on this machine and a cell behind a
/// proxy are served side by side.
#[derive(Clone)]
pub(crate) struct Clients {
    proxied: reqwest::Client,
    direct: reqwest::Client,
}

impl Clients {
    /// Both clients, each as `builder` sets one up.
    pub(crate) fn new(builder: impl Fn() -> reqwest::ClientBuilder) -> Clients {
        Clients {
            proxied: builder().build().unwrap_or_default(),
            direct: builder().no_proxy().build().unwrap_or_default(),
        }
    }

    /// The client for a request to `url`.
    pub(crate) fn to(&self, url: &str) -> &reqwest::Client {
        match is_plain_http(url) {
            true => &self.direct,
            false => &self.proxied,
        }
    }
}

/// Whether `url` names this machine: `localhost`, or a loopback address. What
/// is sent there leaves no interface, so plain http is taken for it.
pub(crate) fn to_this_machine(url: &reqwest::Url) -> bool {
    let host = url.host_str().unwrap_or("");
    host == "localhost"
        || host
            .trim_start_matches('[')
            .trim_end_matches(']')
            .parse::<std::net::IpAddr>()
            .is_ok_and(|ip| ip.is_loopback())
}

/// CI_GITHUB_API_URL, without a trailing slash. The GitHub token goes there,
/// so it is https://, or http:// to this machine (a sidecar proxy, a test).
fn parse_github_api(v: &str) -> anyhow::Result<String> {
    let v = v.trim().trim_end_matches('/');
    let url = reqwest::Url::parse(v)
        .map_err(|e| anyhow::anyhow!("CI_GITHUB_API_URL={v:?} is not a URL: {e}"))?;
    match url.scheme() {
        "https" => {}
        "http" if to_this_machine(&url) => {}
        _ => anyhow::bail!(
            "CI_GITHUB_API_URL={v:?} must be https://, or http:// to this machine: \
             CI_GITHUB_TOKEN_FILE's token, and the App's, are sent there"
        ),
    }
    if !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
    {
        anyhow::bail!("CI_GITHUB_API_URL={v:?} takes no user, query or fragment");
    }
    Ok(v.to_string())
}

/// The GitHub App, from CI_GITHUB_APP_ID and whichever holds its key:
/// CI_GITHUB_APP_KEY_FILE, dagron-ci itself, or CI_GITHUB_APP_TOKEN_URL, a
/// service asked under CI_GITHUB_APP_TOKEN_SECRET_FILE's secret. One or the
/// other, never both; none of them, no App.
fn parse_github_app(
    get: &impl Fn(&str) -> Option<String>,
) -> anyhow::Result<Option<crate::checkout::GitHubApp>> {
    let (id, key, url, secret) = (
        get("CI_GITHUB_APP_ID"),
        get("CI_GITHUB_APP_KEY_FILE"),
        get("CI_GITHUB_APP_TOKEN_URL"),
        get("CI_GITHUB_APP_TOKEN_SECRET_FILE"),
    );
    if url.is_none() && secret.is_some() {
        anyhow::bail!("CI_GITHUB_APP_TOKEN_SECRET_FILE is set without CI_GITHUB_APP_TOKEN_URL");
    }
    let Some(id) = id else {
        return match (key, url) {
            (None, None) => Ok(None),
            (Some(_), _) => anyhow::bail!("CI_GITHUB_APP_KEY_FILE is set without CI_GITHUB_APP_ID"),
            (None, Some(_)) => {
                anyhow::bail!("CI_GITHUB_APP_TOKEN_URL is set without CI_GITHUB_APP_ID")
            }
        };
    };
    let id: u64 = id
        .parse()
        .ok()
        .filter(|n| *n > 0)
        .ok_or_else(|| anyhow::anyhow!("CI_GITHUB_APP_ID={id:?} is not an App id, a number"))?;
    match (key, url) {
        (Some(path), None) => {
            let key = read_secret(&path, "CI_GITHUB_APP_KEY_FILE")?;
            crate::checkout::GitHubApp::new(id, &key)
                .map(Some)
                .map_err(|e| anyhow::anyhow!("CI_GITHUB_APP_KEY_FILE: {e}"))
        }
        (None, Some(url)) => {
            let url = parse_token_url(&url, get("CI_GITHUB_APP_TOKEN_URL_INSECURE").as_deref())?;
            let path = secret.ok_or_else(|| {
                anyhow::anyhow!(
                    "CI_GITHUB_APP_TOKEN_URL is set without CI_GITHUB_APP_TOKEN_SECRET_FILE, \
                     the secret it is asked under"
                )
            })?;
            let secret = String::from_utf8(read_secret(&path, "CI_GITHUB_APP_TOKEN_SECRET_FILE")?)
                .map_err(|_| anyhow::anyhow!("CI_GITHUB_APP_TOKEN_SECRET_FILE is not text"))?;
            Ok(Some(crate::checkout::GitHubApp::with_token_service(
                id, url, secret,
            )))
        }
        (Some(_), Some(_)) => anyhow::bail!(
            "CI_GITHUB_APP_KEY_FILE and CI_GITHUB_APP_TOKEN_URL are both set: the App's key is \
             held by dagron-ci or by its token service, not both"
        ),
        (None, None) => anyhow::bail!(
            "CI_GITHUB_APP_ID is set without CI_GITHUB_APP_KEY_FILE, which signs its tokens, or \
             CI_GITHUB_APP_TOKEN_URL, which mints them"
        ),
    }
}

/// CI_GITHUB_APP_TOKEN_URL: an http(s) URL with no user, query or fragment.
/// Its secret and the tokens it gives go there, so plain http is for this
/// machine, or for another host where CI_GITHUB_APP_TOKEN_URL_INSECURE says
/// the network is closed.
fn parse_token_url(v: &str, insecure: Option<&str>) -> anyhow::Result<String> {
    let insecure = match insecure.map(str::trim) {
        None | Some("") => false,
        Some("allow") => true,
        Some(other) => {
            anyhow::bail!("CI_GITHUB_APP_TOKEN_URL_INSECURE={other:?} is allow, or unset")
        }
    };
    let v = v.trim();
    let url = reqwest::Url::parse(v)
        .map_err(|e| anyhow::anyhow!("CI_GITHUB_APP_TOKEN_URL={v:?} is not a URL: {e}"))?;
    if !matches!(url.scheme(), "https" | "http")
        || url.host_str().is_none_or(str::is_empty)
        || !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
    {
        anyhow::bail!(
            "CI_GITHUB_APP_TOKEN_URL={v:?} is not where the App's tokens are asked for: an \
             https:// URL, with no user, query or fragment"
        );
    }
    if url.scheme() == "http" && !insecure && !to_this_machine(&url) {
        anyhow::bail!(
            "CI_GITHUB_APP_TOKEN_URL={v:?} is plain http to another host: the secret it is asked \
             under, and every token it gives, would cross the network unencrypted. Give it an \
             https:// URL, or, on a closed network where that is accepted, set \
             CI_GITHUB_APP_TOKEN_URL_INSECURE=allow"
        );
    }
    Ok(v.to_string())
}

/// CI_URL_FOR_JOBS, without a trailing slash: an http(s) URL with no path,
/// user, query or fragment, at which jobs reach dagron-ci.
fn parse_url_for_jobs(v: &str, insecure: Option<&str>) -> anyhow::Result<String> {
    let insecure = match insecure.map(str::trim) {
        None | Some("") => false,
        Some("allow") => true,
        Some(other) => anyhow::bail!("CI_URL_FOR_JOBS_INSECURE={other:?} is allow, or unset"),
    };
    let v = v.trim().trim_end_matches('/');
    let url = reqwest::Url::parse(v)
        .map_err(|e| anyhow::anyhow!("CI_URL_FOR_JOBS={v:?} is not a URL: {e}"))?;
    if url.scheme() == "http" && !insecure && !to_this_machine(&url) {
        anyhow::bail!(
            "CI_URL_FOR_JOBS={v:?} is plain http to another host: a link to a patch bundle is a \
             bearer's, and whoever sees it on the way reads the change. Give it an https:// URL, \
             or, on a closed network where that is accepted, set CI_URL_FOR_JOBS_INSECURE=allow"
        );
    }
    if !matches!(url.scheme(), "https" | "http")
        || url.host_str().is_none_or(str::is_empty)
        || !url.path().trim_end_matches('/').is_empty()
        || !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
    {
        anyhow::bail!(
            "CI_URL_FOR_JOBS={v:?} is not where jobs reach dagron-ci: an https:// URL, with no \
             path, user, query or fragment"
        );
    }
    Ok(v.to_string())
}

/// CI_GITHUB_API_FOR_JOBS, without a trailing slash: an http(s) URL with no
/// user, query or fragment, at which jobs reach the door. A job's
/// `GITHUB_TOKEN` is sent there, so plain http:// is the operator's to
/// choose where nothing on the way needs hiding.
fn parse_api_for_jobs(v: &str) -> anyhow::Result<String> {
    let v = v.trim().trim_end_matches('/');
    let url = reqwest::Url::parse(v)
        .map_err(|e| anyhow::anyhow!("CI_GITHUB_API_FOR_JOBS={v:?} is not a URL: {e}"))?;
    if !matches!(url.scheme(), "https" | "http")
        || url.host_str().is_none_or(str::is_empty)
        || !url
            .path()
            .trim_end_matches('/')
            .ends_with(crate::github_door::PATH)
        || !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
    {
        anyhow::bail!(
            "CI_GITHUB_API_FOR_JOBS={v:?} is not where jobs reach dagron-ci's GitHub API door: an \
             https:// URL (http:// where nothing on the way needs hiding), with no user, query or \
             fragment, ending in {}",
            crate::github_door::PATH
        );
    }
    Ok(v.to_string())
}

/// CI_RUN_URL: an http(s) URL, once `{run_id}` and `{cell}` are filled in.
/// Another `{…}` is a typo that would link every status nowhere, and so is a
/// host made of the run id (`https:///{run_id}` parses that way); a host per
/// cell is a console per cell, and is taken.
fn parse_run_url(v: &str) -> anyhow::Result<String> {
    let fill = |run_id: &str| v.replace("{run_id}", run_id).replace("{cell}", "c");
    if fill("r").contains(['{', '}']) {
        anyhow::bail!(
            "CI_RUN_URL={v:?} may name {{run_id}} and {{cell}}, and nothing else in braces"
        );
    }
    let parse = |run_id: &str| {
        reqwest::Url::parse(&fill(run_id))
            .map_err(|e| anyhow::anyhow!("CI_RUN_URL={v:?} is not a URL: {e}"))
    };
    let (url, other) = (parse("r")?, parse("s")?);
    if !matches!(url.scheme(), "https" | "http") || url.host_str().is_none() {
        anyhow::bail!("CI_RUN_URL={v:?} must be an http:// or https:// URL");
    }
    if url.host_str() != other.host_str() {
        anyhow::bail!("CI_RUN_URL={v:?} puts the run id in the host; it goes in the path");
    }
    Ok(v.to_string())
}

fn is_cell_name(s: &str) -> bool {
    !s.is_empty()
        && s.len() <= 64
        && s.bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.'))
}

pub(crate) fn is_postgres(url: &str) -> bool {
    url.starts_with("postgres://") || url.starts_with("postgresql://")
}

pub(crate) fn is_sqlite(url: &str) -> bool {
    url.starts_with("sqlite:")
}

/// A Postgres identifier dagron-ci will interpolate into `CREATE SCHEMA` and
/// `search_path`: nothing that needs quoting, so nothing that could escape it.
fn is_identifier(s: &str) -> bool {
    let mut bytes = s.bytes();
    matches!(bytes.next(), Some(b'a'..=b'z' | b'_'))
        && bytes.all(|b| matches!(b, b'a'..=b'z' | b'0'..=b'9' | b'_'))
        && s.len() <= 63
}

/// A secret file's contents, without the trailing newline an editor adds.
fn read_secret(path: &str, var: &str) -> anyhow::Result<Vec<u8>> {
    let raw = std::fs::read(path).map_err(|e| anyhow::anyhow!("{var}: cannot read {path}: {e}"))?;
    let trimmed = raw.trim_ascii_end();
    if trimmed.is_empty() {
        anyhow::bail!("{var}: {path} is empty");
    }
    Ok(trimmed.to_vec())
}

/// CI_CHECKOUT_TOKEN_SECRETS_DIR: one file per cell, named as the cell, the
/// way a Kubernetes Secret with a key per cell mounts. A mounted Secret's own
/// entries (`..data`, its timestamped directory) are passed over, as is any
/// directory. A file for a cell CI_CELLS does not name is refused, and so are
/// two cells with one secret: the secret is what says which cell an engine is.
fn read_cell_secrets(
    dir: &str,
    cells: &BTreeMap<String, String>,
) -> anyhow::Result<BTreeMap<String, String>> {
    const VAR: &str = "CI_CHECKOUT_TOKEN_SECRETS_DIR";
    let entries =
        std::fs::read_dir(dir).map_err(|e| anyhow::anyhow!("{VAR}: cannot read {dir}: {e}"))?;
    let mut secrets = BTreeMap::new();
    for entry in entries {
        let entry = entry.map_err(|e| anyhow::anyhow!("{VAR}: cannot read {dir}: {e}"))?;
        let name = entry.file_name().to_string_lossy().into_owned();
        let path = entry.path();
        if name.starts_with("..") || path.is_dir() {
            continue;
        }
        if !cells.contains_key(&name) {
            anyhow::bail!(
                "{VAR}: {} is for cell {name:?}, which CI_CELLS does not name",
                path.display()
            );
        }
        let secret = read_token(&path.to_string_lossy(), VAR)?;
        if let Some(other) = secrets
            .iter()
            .find_map(|(cell, known): (&String, &String)| (*known == secret).then_some(cell))
        {
            anyhow::bail!(
                "{VAR}: cells {other:?} and {name:?} have one secret; each cell's must be its own"
            );
        }
        secrets.insert(name, secret);
    }
    if secrets.is_empty() {
        anyhow::bail!("{VAR}: {dir} holds no cell's secret");
    }
    Ok(secrets)
}

/// CI_GITLAB_TOKENS_DIR: one file per GitLab host, named as the host, the
/// way a Kubernetes Secret with a key per host mounts; a mounted Secret's own
/// entries and any directory are passed over. A name that is no host is
/// refused, and so is a directory with no token in it.
fn read_host_tokens(dir: &str) -> anyhow::Result<BTreeMap<String, String>> {
    const VAR: &str = "CI_GITLAB_TOKENS_DIR";
    let entries =
        std::fs::read_dir(dir).map_err(|e| anyhow::anyhow!("{VAR}: cannot read {dir}: {e}"))?;
    let mut tokens = BTreeMap::new();
    for entry in entries {
        let entry = entry.map_err(|e| anyhow::anyhow!("{VAR}: cannot read {dir}: {e}"))?;
        let name = entry.file_name().to_string_lossy().into_owned();
        let path = entry.path();
        if name.starts_with("..") || path.is_dir() {
            continue;
        }
        // As a repository's forge host is written: a name, and a port where
        // it is not the default.
        let host = name.to_ascii_lowercase();
        let label = |l: &str| {
            !l.is_empty()
                && !l.starts_with('-')
                && !l.ends_with('-')
                && l.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'-')
        };
        let (named, port) = match host.split_once(':') {
            Some((named, port)) => (named, Some(port)),
            None => (host.as_str(), None),
        };
        let port_ok = port
            .is_none_or(|p| !p.is_empty() && p.len() <= 5 && p.bytes().all(|b| b.is_ascii_digit()));
        if named.len() > 253 || !named.split('.').all(label) || !port_ok {
            anyhow::bail!(
                "{VAR}: {} is named {name:?}, which is no host name: each file is named as the \
                 GitLab host its token is for",
                path.display()
            );
        }
        tokens.insert(host, read_token(&path.to_string_lossy(), VAR)?);
    }
    if tokens.is_empty() {
        anyhow::bail!("{VAR}: {dir} holds no host's token");
    }
    Ok(tokens)
}

/// A token file's contents, which must be UTF-8 to go into a header.
fn read_token(path: &str, var: &str) -> anyhow::Result<String> {
    String::from_utf8(read_secret(path, var)?)
        .map_err(|_| anyhow::anyhow!("{var}: {path} does not hold a UTF-8 token"))
}

/// A database URL with its password replaced, fit for a log line.
pub fn redact_url(url: &str) -> String {
    let Some((scheme, rest)) = url.split_once("://") else {
        return url.to_string();
    };
    let (authority, tail) = match rest.find('/') {
        Some(i) => rest.split_at(i),
        None => (rest, ""),
    };
    match authority.rsplit_once('@') {
        Some((userinfo, host)) => {
            let user = userinfo.split_once(':').map_or(userinfo, |(u, _)| u);
            format!("{scheme}://{user}:***@{host}{tail}")
        }
        None => url.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn config(vars: &[(&str, &str)]) -> anyhow::Result<Config> {
        let vars: HashMap<String, String> = vars
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        Config::from_lookup(|name| vars.get(name).cloned())
    }

    #[test]
    fn defaults_with_only_a_database() {
        let c = config(&[("CI_DATABASE_URL", "sqlite://ci.db")]).unwrap();
        assert_eq!(c.listen, "0.0.0.0:8090".parse().unwrap());
        assert_eq!(c.database_schema, "dagron_ci");
        assert_eq!(c.database_max_connections, 8);
        assert!(c.github_webhook_secret.is_none());
    }

    #[test]
    fn the_database_is_required_and_must_be_one_dagron_ci_speaks() {
        assert!(config(&[]).is_err());
        assert!(
            config(&[("CI_DATABASE_URL", "   ")]).is_err(),
            "blank is unset"
        );
        let err = config(&[("CI_DATABASE_URL", "mysql://u:hunter2@db/ci")]).unwrap_err();
        assert!(
            !err.to_string().contains("hunter2"),
            "the password stays out of the error: {err}"
        );
        assert!(config(&[("CI_DATABASE_URL", "postgresql://db/ci")]).is_ok());
    }

    #[test]
    fn a_wrong_value_stops_start_up() {
        let db = ("CI_DATABASE_URL", "sqlite://ci.db");
        assert!(config(&[db, ("CI_LISTEN", "8090")]).is_err());
        assert!(config(&[db, ("CI_DATABASE_MAX_CONNECTIONS", "0")]).is_err());
        assert!(config(&[db, ("CI_DATABASE_MAX_CONNECTIONS", "many")]).is_err());
        for schema in [
            "Dagron",
            "1ci",
            "ci;drop",
            "ci-x",
            "public\"",
            &"a".repeat(64),
        ] {
            assert!(
                config(&[db, ("CI_DATABASE_SCHEMA", schema)]).is_err(),
                "{schema:?}"
            );
        }
        assert!(config(&[db, ("CI_DATABASE_SCHEMA", "_ci_2")]).is_ok());
    }

    #[test]
    fn the_secret_file_is_read_without_its_trailing_newline() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("secret");
        std::fs::write(&path, "s3cret\n").unwrap();
        let c = config(&[
            ("CI_DATABASE_URL", "sqlite://ci.db"),
            ("CI_GITHUB_WEBHOOK_SECRET_FILE", path.to_str().unwrap()),
        ])
        .unwrap();
        assert_eq!(c.github_webhook_secret.as_deref(), Some(&b"s3cret"[..]));
        assert!(!format!("{c:?}").contains("s3cret"), "Debug redacts it");
    }

    #[test]
    fn an_empty_or_missing_secret_file_stops_start_up() {
        let dir = tempfile::tempdir().unwrap();
        let empty = dir.path().join("empty");
        std::fs::write(&empty, "\n").unwrap();
        for path in [empty.to_str().unwrap(), "/nonexistent/secret"] {
            assert!(config(&[
                ("CI_DATABASE_URL", "sqlite://ci.db"),
                ("CI_GITHUB_WEBHOOK_SECRET_FILE", path),
            ])
            .is_err());
        }
    }

    #[test]
    fn workers_default_off_until_a_cell_is_named() {
        let c = config(&[("CI_DATABASE_URL", "sqlite://ci.db")]).unwrap();
        assert!(c.cells.is_empty());
        assert_eq!(
            (
                c.event_workers,
                c.event_lease.as_secs(),
                c.event_max_attempts
            ),
            (8, 60, 8)
        );
    }

    #[test]
    fn cells_are_name_url_pairs_and_need_a_token() {
        let dir = tempfile::tempdir().unwrap();
        let token = dir.path().join("token");
        std::fs::write(&token, "dgr_abc\n").unwrap();
        let db = ("CI_DATABASE_URL", "sqlite://ci.db");
        let tok = ("CI_API_TOKEN_FILE", token.to_str().unwrap());

        let c = config(&[
            db,
            tok,
            (
                "CI_CELLS",
                " default=https://dagron-api:8443/ , eu=https://eu.example ",
            ),
        ])
        .unwrap();
        assert_eq!(c.cells["default"], "https://dagron-api:8443");
        assert_eq!(c.cells["eu"], "https://eu.example");
        assert_eq!(c.api_token.as_deref(), Some("dgr_abc"));
        assert!(
            !format!("{c:?}").contains("dgr_abc"),
            "Debug redacts the token"
        );

        assert!(
            config(&[db, ("CI_CELLS", "default=https://a")]).is_err(),
            "no token"
        );
        for bad in [
            "default",
            "default=ftp://a",
            "=https://a",
            "a b=https://a",
            "a=https://a,a=https://b",
            ",",
            "a=https://",
        ] {
            assert!(config(&[db, tok, ("CI_CELLS", bad)]).is_err(), "{bad:?}");
        }

        // A dagron-api beside dagron-ci, over plain http behind a network
        // policy: only where its operator says the network is closed.
        let beside = ("CI_CELLS", "default=http://dagron-api.ws-8f3a.svc:8080");
        let err = config(&[db, tok, beside]).unwrap_err().to_string();
        assert!(err.contains("CI_CELLS_INSECURE=allow"), "{err}");
        let c = config(&[db, tok, beside, ("CI_CELLS_INSECURE", "allow")]).unwrap();
        assert_eq!(c.cells["default"], "http://dagron-api.ws-8f3a.svc:8080");
        let err = config(&[db, tok, beside, ("CI_CELLS_INSECURE", "yes")])
            .unwrap_err()
            .to_string();
        assert!(err.contains("is allow, or unset"), "{err}");
    }

    #[test]
    fn worker_knobs_are_bounded_and_fork_prs_are_refused() {
        let db = ("CI_DATABASE_URL", "sqlite://ci.db");
        assert_eq!(
            config(&[db, ("CI_EVENT_WORKERS", "0")])
                .unwrap()
                .event_workers,
            0
        );
        assert!(config(&[db, ("CI_EVENT_WORKERS", "257")]).is_err());
        assert!(config(&[db, ("CI_EVENT_LEASE_SECS", "4")]).is_err());
        assert!(config(&[db, ("CI_EVENT_MAX_ATTEMPTS", "0")]).is_err());
        // Every event is kept unless a horizon is said, of a day at least.
        assert_eq!(config(&[db]).unwrap().record_days, None);
        assert_eq!(
            config(&[db, ("CI_RECORD_DAYS", "30")]).unwrap().record_days,
            Some(30)
        );
        for bad in ["0", "3651", "a week", "-1"] {
            assert!(config(&[db, ("CI_RECORD_DAYS", bad)]).is_err(), "{bad}");
        }
        assert_eq!(config(&[db]).unwrap().object_store_url, None);
        for good in [
            "s3://bucket/ci",
            "gs://b",
            "az://c/p",
            "file:///var/lib/dagron-ci/payloads",
        ] {
            let c = config(&[db, ("CI_OBJECT_STORE_URL", good)]).unwrap();
            assert_eq!(c.object_store_url.as_deref(), Some(good));
        }
        // A segment kept no longer than its day is kept in the database
        // would go in the pass that writes it.
        let store = ("CI_OBJECT_STORE_URL", "file:///var/lib/dagron-ci");
        let err = config(&[
            db,
            store,
            ("CI_RECORD_DAYS", "30"),
            ("CI_SEGMENT_DAYS", "30"),
        ])
        .unwrap_err();
        assert!(
            err.to_string()
                .starts_with("CI_SEGMENT_DAYS=30 is not past CI_RECORD_DAYS=30"),
            "{err}"
        );
        let both = config(&[
            db,
            store,
            ("CI_RECORD_DAYS", "30"),
            ("CI_SEGMENT_DAYS", "31"),
        ])
        .unwrap();
        assert_eq!((both.record_days, both.segment_days), (Some(30), Some(31)));
        for bad in ["http://store", "file://relative", "bucket"] {
            assert!(
                config(&[db, ("CI_OBJECT_STORE_URL", bad)]).is_err(),
                "{bad}"
            );
        }
        assert_eq!(
            config(&[db, ("CI_FORK_PRS", "refuse")]).unwrap().forks,
            None
        );
        assert_eq!(config(&[db]).unwrap().forks, None);
        // Run, with who may approve one named (crate::forks has the rest).
        let err = config(&[db, ("CI_FORK_PRS", "untrusted")])
            .unwrap_err()
            .to_string();
        assert!(err.contains("CI_FORK_APPROVERS names nobody"), "{err}");
        let forks = config(&[
            db,
            ("CI_FORK_PRS", "untrusted"),
            ("CI_FORK_APPROVERS", "group:maintainers"),
        ])
        .unwrap()
        .forks
        .expect("fork pull requests are run");
        assert_eq!(forks.approvers, ["group:maintainers"]);
        assert!(config(&[db, ("CI_FORK_PRS", "allow")]).is_err());
    }

    #[test]
    fn an_agent_s_runs_are_refused_unless_they_are_run_as_untrusted() {
        let db = ("CI_DATABASE_URL", "sqlite://ci.db");
        assert!(!config(&[db]).unwrap().agent_runs);
        assert!(
            !config(&[db, ("CI_AGENT_RUNS", "refuse")])
                .unwrap()
                .agent_runs
        );
        assert!(
            config(&[db, ("CI_AGENT_RUNS", "untrusted")])
                .unwrap()
                .agent_runs
        );
        let err = config(&[db, ("CI_AGENT_RUNS", "trusted")])
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("CI_AGENT_RUNS=\"trusted\" must be refuse or untrusted"),
            "{err}"
        );
    }

    #[test]
    fn polling_defaults_to_five_minutes_and_its_token_is_redacted() {
        let db = ("CI_DATABASE_URL", "sqlite://ci.db");
        let c = config(&[db]).unwrap();
        assert_eq!((c.poll_secs, c.github_token.is_none()), (300, true));
        assert_eq!(config(&[db, ("CI_POLL_SECS", "0")]).unwrap().poll_secs, 0);
        assert_eq!(config(&[db, ("CI_POLL_SECS", "30")]).unwrap().poll_secs, 30);
        for bad in ["29", "1", "86401", "5m", "-1"] {
            assert!(config(&[db, ("CI_POLL_SECS", bad)]).is_err(), "{bad:?}");
        }

        let dir = tempfile::tempdir().unwrap();
        let token = dir.path().join("token");
        std::fs::write(&token, "github_pat_abc\n").unwrap();
        let c = config(&[db, ("CI_GITHUB_TOKEN_FILE", token.to_str().unwrap())]).unwrap();
        assert_eq!(c.github_token.as_deref(), Some("github_pat_abc"));
        assert!(!format!("{c:?}").contains("github_pat_abc"));
        assert!(config(&[db, ("CI_GITHUB_TOKEN_FILE", "/nonexistent/token")]).is_err());
    }

    /// dagron-ci's token for dagron-api goes wherever a cell's URL points, and
    /// so does the credential of each caller of the run API: never in the
    /// clear off this machine. A cell over plain http to another host stops
    /// start-up, and the reason names the cell.
    #[test]
    fn a_cell_is_reached_over_tls_or_on_this_machine() {
        let dir = tempfile::tempdir().unwrap();
        let token = dir.path().join("token");
        std::fs::write(&token, "dgr_abc\n").unwrap();
        let db = ("CI_DATABASE_URL", "sqlite://ci.db");
        let tok = ("CI_API_TOKEN_FILE", token.to_str().unwrap());
        for (cells, want) in [
            ("a=https://api.internal", "https://api.internal"),
            ("a=https://api.internal:8443/", "https://api.internal:8443"),
            ("a=http://127.0.0.1:8080", "http://127.0.0.1:8080"),
            ("a=http://localhost:8080/", "http://localhost:8080"),
            ("a=http://[::1]:8080", "http://[::1]:8080"),
        ] {
            let c = config(&[db, tok, ("CI_CELLS", cells)]).unwrap();
            assert_eq!(c.cells["a"], want, "{cells}");
        }
        for (cells, named) in [
            ("a=http://dagron-api:8080", "\"a\""),
            ("a=http://10.0.0.7:8080", "\"a\""),
            ("a=http://localhost.example:8080", "\"a\""),
            (
                "eu=https://eu.example,us=http://dagron-api.us.svc:8080",
                "\"us\"",
            ),
        ] {
            let e = config(&[db, tok, ("CI_CELLS", cells)])
                .unwrap_err()
                .to_string();
            assert!(
                e.contains("must be https://, or http:// to this machine") && e.contains(named),
                "{cells}: {e}"
            );
        }
    }

    /// The token goes wherever CI_GITHUB_API_URL points: never in the clear
    /// off this machine.
    #[test]
    fn github_s_api_may_be_reached_through_a_proxy() {
        let db = ("CI_DATABASE_URL", "sqlite://ci.db");
        assert_eq!(config(&[db]).unwrap().github_api, None);
        for (url, want) in [
            ("https://gh-proxy.internal/", "https://gh-proxy.internal"),
            (
                "https://proxy.internal:8443/github",
                "https://proxy.internal:8443/github",
            ),
            ("http://127.0.0.1:9000", "http://127.0.0.1:9000"),
            ("http://localhost:9000/", "http://localhost:9000"),
            ("http://[::1]:9000", "http://[::1]:9000"),
        ] {
            let c = config(&[db, ("CI_GITHUB_API_URL", url)]).unwrap();
            assert_eq!(c.github_api.as_deref(), Some(want), "{url}");
        }
        for bad in [
            "http://gh-proxy.internal",
            "ftp://127.0.0.1",
            "api.github.com",
            "https://user:pw@gh-proxy.internal",
            "https://gh-proxy.internal/?x=1",
        ] {
            assert!(config(&[db, ("CI_GITHUB_API_URL", bad)]).is_err(), "{bad}");
        }
    }

    /// Each cell's checkout secret is a file named as the cell, the way a
    /// Kubernetes Secret with a key per cell mounts; the secret is what says
    /// which cell an engine is, so none is shared.
    #[test]
    fn each_cell_s_checkout_secret_is_a_file_named_as_the_cell() {
        let dir = tempfile::tempdir().unwrap();
        let token = dir.path().join("api-token");
        std::fs::write(&token, "dgr_x").unwrap();
        let secrets = dir.path().join("secrets");
        std::fs::create_dir(&secrets).unwrap();
        std::fs::write(secrets.join("eu-1"), "eu-s3cret\n").unwrap();
        std::fs::write(secrets.join("us-1"), "us-s3cret\n").unwrap();
        // A mounted Secret's own entries.
        std::fs::create_dir(secrets.join("..2026_10_05_12_00_00.1")).unwrap();
        std::os::unix::fs::symlink("..2026_10_05_12_00_00.1", secrets.join("..data")).unwrap();
        let base = [
            ("CI_DATABASE_URL", "sqlite://ci.db"),
            (
                "CI_CELLS",
                "eu-1=https://eu.internal,us-1=https://us.internal",
            ),
            ("CI_API_TOKEN_FILE", token.to_str().unwrap()),
        ];
        let with = |extra: &[(&str, &str)]| {
            let mut vars = base.to_vec();
            vars.extend_from_slice(extra);
            config(&vars)
        };
        let dir_var = ("CI_CHECKOUT_TOKEN_SECRETS_DIR", secrets.to_str().unwrap());
        let c = with(&[dir_var]).unwrap();
        assert_eq!(
            c.checkout_cell_secrets,
            BTreeMap::from([
                ("eu-1".to_string(), "eu-s3cret".to_string()),
                ("us-1".to_string(), "us-s3cret".to_string()),
            ])
        );
        assert!(c.checkout_token_secret.is_none());
        assert!(!format!("{c:?}").contains("s3cret"), "Debug redacts them");

        // Every cell's secret is refused once two cells are named, beside
        // theirs or alone: an engine of either could ask for the other's run.
        let shared = dir.path().join("shared");
        std::fs::write(&shared, "all-s3cret").unwrap();
        let shared_var = ("CI_CHECKOUT_TOKEN_SECRET_FILE", shared.to_str().unwrap());
        for vars in [&[dir_var, shared_var][..], &[shared_var]] {
            let e = with(vars).unwrap_err().to_string();
            assert!(
                e.contains("answers for every cell, and CI_CELLS names 2"),
                "{e}"
            );
        }
        // With one cell it is that cell's, and may stand beside the cell's
        // own, as long as it is not the same secret.
        let one_dir = dir.path().join("one");
        std::fs::create_dir(&one_dir).unwrap();
        std::fs::write(one_dir.join("eu-1"), "eu-s3cret\n").unwrap();
        let one = || {
            config(&[
                ("CI_DATABASE_URL", "sqlite://ci.db"),
                ("CI_CELLS", "eu-1=https://eu.internal"),
                ("CI_API_TOKEN_FILE", token.to_str().unwrap()),
                ("CI_CHECKOUT_TOKEN_SECRETS_DIR", one_dir.to_str().unwrap()),
                shared_var,
            ])
        };
        assert!(one().is_ok());
        std::fs::write(&shared, "eu-s3cret").unwrap();
        let e = one().unwrap_err().to_string();
        assert!(e.contains("give each cell a secret of its own"), "{e}");

        // One secret for two cells, a cell CI_CELLS does not name, nothing.
        std::fs::write(secrets.join("us-1"), "eu-s3cret").unwrap();
        let e = with(&[dir_var]).unwrap_err().to_string();
        assert!(e.contains("have one secret"), "{e}");
        std::fs::write(secrets.join("us-1"), "us-s3cret").unwrap();
        std::fs::write(secrets.join("ap-1"), "ap-s3cret").unwrap();
        let e = with(&[dir_var]).unwrap_err().to_string();
        assert!(e.contains("\"ap-1\", which CI_CELLS does not name"), "{e}");
        let empty = dir.path().join("none");
        std::fs::create_dir(&empty).unwrap();
        let e = with(&[("CI_CHECKOUT_TOKEN_SECRETS_DIR", empty.to_str().unwrap())])
            .unwrap_err()
            .to_string();
        assert!(e.contains("holds no cell's secret"), "{e}");
    }

    /// Each GitLab host's token is the file named as the host; a mounted
    /// Secret's own entries are passed over, and a name that is no host, or
    /// a directory with no token, is refused.
    #[test]
    fn gitlab_tokens_are_one_file_per_host() {
        let dir = tempfile::tempdir().unwrap();
        let tokens = dir.path().join("tokens");
        std::fs::create_dir(&tokens).unwrap();
        std::fs::write(tokens.join("GitLab.com"), "glpat-a\n").unwrap();
        std::fs::write(tokens.join("git.corp.example"), "glpat-b").unwrap();
        std::fs::write(tokens.join("git.lab.example:8443"), "glpat-p").unwrap();
        std::fs::create_dir(tokens.join("..2026_10_18")).unwrap();
        std::fs::write(tokens.join("..data"), "x").unwrap();
        let with = |dir: &std::path::Path| {
            config(&[
                ("CI_DATABASE_URL", "sqlite://ci.db"),
                ("CI_GITLAB_TOKENS_DIR", dir.to_str().unwrap()),
            ])
        };
        let c = with(&tokens).unwrap();
        assert_eq!(
            c.gitlab_tokens,
            BTreeMap::from([
                ("git.corp.example".to_string(), "glpat-b".to_string()),
                ("git.lab.example:8443".to_string(), "glpat-p".to_string()),
                ("gitlab.com".to_string(), "glpat-a".to_string()),
            ])
        );
        let shown = format!("{c:?}");
        assert!(!shown.contains("glpat"), "{shown}");

        std::fs::write(tokens.join("not a host"), "glpat-c").unwrap();
        let e = with(&tokens).unwrap_err().to_string();
        assert!(e.contains("which is no host name"), "{e}");
        let empty = dir.path().join("none");
        std::fs::create_dir(&empty).unwrap();
        let e = with(&empty).unwrap_err().to_string();
        assert!(e.contains("holds no host's token"), "{e}");
    }

    /// Plain http is this machine's, and a client for it uses no proxy.
    #[test]
    fn plain_http_is_told_from_https() {
        for local in [
            "http://127.0.0.1:8080",
            " HTTP://localhost:8090/ci",
            "http://[::1]:1",
        ] {
            assert!(is_plain_http(local), "{local}");
        }
        for remote in [
            "https://dagron.example.com",
            "https://127.0.0.1",
            "httpx://a",
            "",
        ] {
            assert!(!is_plain_http(remote), "{remote}");
        }
        // The choice is each request's: a cell on this machine is asked
        // directly, and an https cell beside it keeps the environment's
        // proxy.
        let clients = Clients::new(reqwest::Client::builder);
        assert!(std::ptr::eq(
            clients.to("http://127.0.0.1:8080/api/me"),
            &clients.direct
        ));
        assert!(std::ptr::eq(
            clients.to("https://dagron.example.com/api/me"),
            &clients.proxied
        ));
    }

    /// A checkout's credential is for engines holding a secret, and minted as
    /// an App named by both its id and its key, read and checked at start.
    #[test]
    fn checkout_credentials_take_a_secret_and_an_app_whole() {
        let dir = tempfile::tempdir().unwrap();
        let db = ("CI_DATABASE_URL", "sqlite://ci.db");
        let c = config(&[db]).unwrap();
        assert!(c.checkout_token_secret.is_none() && c.github_app.is_none());

        let secret = dir.path().join("secret");
        std::fs::write(&secret, "engine-s3cret\n").unwrap();
        let secret = ("CI_CHECKOUT_TOKEN_SECRET_FILE", secret.to_str().unwrap());
        let c = config(&[db, secret]).unwrap();
        assert_eq!(c.checkout_token_secret.as_deref(), Some("engine-s3cret"));
        assert!(!format!("{c:?}").contains("s3cret"), "Debug redacts it");
        let empty = dir.path().join("empty");
        std::fs::write(&empty, "\n").unwrap();
        assert!(config(&[
            db,
            ("CI_CHECKOUT_TOKEN_SECRET_FILE", empty.to_str().unwrap())
        ])
        .is_err());

        let key = dir.path().join("key.pem");
        std::fs::write(
            &key,
            "-----BEGIN RSA PRIVATE KEY-----\nbm90IGEga2V5\n-----END RSA PRIVATE KEY-----\n",
        )
        .unwrap();
        let key = ("CI_GITHUB_APP_KEY_FILE", key.to_str().unwrap());
        assert!(
            config(&[db, secret, ("CI_GITHUB_APP_ID", "1")]).is_err(),
            "an id without its key"
        );
        assert!(config(&[db, secret, key]).is_err(), "a key without its id");
        for id in ["0", "app", "-1", ""] {
            assert!(
                config(&[db, secret, ("CI_GITHUB_APP_ID", id), key]).is_err(),
                "{id:?}"
            );
        }
        let err = config(&[db, secret, ("CI_GITHUB_APP_ID", "1"), key]).unwrap_err();
        assert!(
            err.to_string().starts_with("CI_GITHUB_APP_KEY_FILE: "),
            "{err}"
        );
        assert!(config(&[
            db,
            secret,
            ("CI_GITHUB_APP_ID", "1"),
            ("CI_GITHUB_APP_KEY_FILE", "/nonexistent/key")
        ])
        .is_err());

        // A fleet is refused without what it is served with: a cell to
        // start each runner in, with dagron-ci's token for it, and the App
        // that registers the runner. Served without, it would take a job's
        // delivery and keep it, and no runner would ever be started.
        let labels = dir.path().join("fleet.yaml");
        std::fs::write(&labels, "labels:\n  dagron:\n    image: runner:1\n").unwrap();
        let fleet = ("CI_GITHUB_FLEET_FILE", labels.to_str().unwrap());
        let err = config(&[db, fleet]).unwrap_err();
        assert!(
            err.to_string()
                .starts_with("CI_GITHUB_FLEET_FILE needs a cell"),
            "{err}"
        );
        let token = dir.path().join("api-token");
        std::fs::write(&token, "dgt_token\n").unwrap();
        let cell = ("CI_CELLS", "default=https://cell.example");
        let token = ("CI_API_TOKEN_FILE", token.to_str().unwrap());
        let err = config(&[db, cell, token, fleet]).unwrap_err();
        assert!(
            err.to_string()
                .starts_with("CI_GITHUB_FLEET_FILE needs the App"),
            "{err}"
        );

        // GitLab's needs a token for each host, which makes its runners, and
        // no App.
        let gitlab = ("CI_GITLAB_FLEET_FILE", labels.to_str().unwrap());
        let err = config(&[db, gitlab]).unwrap_err();
        assert!(
            err.to_string()
                .starts_with("CI_GITLAB_FLEET_FILE needs a cell"),
            "{err}"
        );
        let err = config(&[db, cell, token, gitlab]).unwrap_err();
        assert!(
            err.to_string()
                .starts_with("CI_GITLAB_FLEET_FILE needs a token"),
            "{err}"
        );
        let tokens = dir.path().join("gitlab-tokens");
        std::fs::create_dir(&tokens).unwrap();
        std::fs::write(tokens.join("gitlab.example"), "glpat-x\n").unwrap();
        let tokens = ("CI_GITLAB_TOKENS_DIR", tokens.to_str().unwrap());
        let served = config(&[db, cell, token, gitlab, tokens]).unwrap();
        let served = served.gitlab_fleet.unwrap();
        assert_eq!(served.of(), crate::fleet::Of::GitLab);
        assert_eq!(served.labels().collect::<Vec<_>>(), ["dagron"]);

        // A fork's job's runners keep their configurations apart.
        let err = config(&[
            db,
            ("CI_FLEET_ENVIRONMENT", "fleet"),
            ("CI_FLEET_UNTRUSTED_ENVIRONMENT", "fleet"),
        ])
        .unwrap_err();
        assert!(err.to_string().contains("are both \"fleet\""), "{err}");
        let err = config(&[db, ("CI_FLEET_UNTRUSTED_ENVIRONMENT", "a b")]).unwrap_err();
        assert!(err.to_string().contains("an environment's name"), "{err}");
    }

    #[test]
    fn a_compiled_task_times_out_after_an_hour_unless_told() {
        let db = ("CI_DATABASE_URL", "sqlite://ci.db");
        assert_eq!(config(&[db]).unwrap().default_timeout_secs, 3600);
        assert_eq!(
            config(&[db, ("CI_DEFAULT_TIMEOUT_SECS", "600")])
                .unwrap()
                .default_timeout_secs,
            600
        );
        for bad in ["0", "604801", "1h"] {
            assert!(
                config(&[db, ("CI_DEFAULT_TIMEOUT_SECS", bad)]).is_err(),
                "{bad:?}"
            );
        }
    }

    #[test]
    fn urls_are_redacted_for_logs() {
        assert_eq!(
            redact_url("postgres://ci:hunter2@db:5432/ci?sslmode=require"),
            "postgres://ci:***@db:5432/ci?sslmode=require"
        );
        assert_eq!(redact_url("postgres://db/ci"), "postgres://db/ci");
        assert_eq!(redact_url("sqlite://ci.db"), "sqlite://ci.db");
    }

    #[test]
    fn a_run_url_names_the_run_and_nothing_else() {
        let db = ("CI_DATABASE_URL", "sqlite://ci.db");
        let c = config(&[
            db,
            ("CI_RUN_URL", "https://ci.example.com/{cell}/runs/{run_id}"),
        ])
        .unwrap();
        assert_eq!(
            c.run_url.as_deref(),
            Some("https://ci.example.com/{cell}/runs/{run_id}")
        );
        assert_eq!(config(&[db]).unwrap().run_url, None);
        // A console per cell.
        assert!(config(&[
            db,
            ("CI_RUN_URL", "https://{cell}.ci.example.com/runs/{run_id}")
        ])
        .is_ok());
        for bad in [
            "https://ci.example.com/runs/{runid}",
            "ci.example.com/runs/{run_id}",
            "ftp://ci.example.com/{run_id}",
            "https:///{run_id}",
        ] {
            assert!(config(&[db, ("CI_RUN_URL", bad)]).is_err(), "{bad}");
        }
    }

    #[test]
    fn a_pull_request_s_comment_is_off_unless_asked_for_and_needs_the_app() {
        let db = ("CI_DATABASE_URL", "sqlite://ci.db");
        assert!(!config(&[db]).unwrap().github_pr_comment);
        for off in ["off", "", " off "] {
            let c = config(&[db, ("CI_GITHUB_PR_COMMENT", off)]).unwrap();
            assert!(!c.github_pr_comment, "{off:?}");
        }
        // The comment is the App's: a token of the operator's writes none.
        let err = config(&[db, ("CI_GITHUB_PR_COMMENT", "on")]).unwrap_err();
        assert!(
            err.to_string()
                .starts_with("CI_GITHUB_PR_COMMENT=on needs the App"),
            "{err}"
        );
        assert!(config(&[db, ("CI_GITHUB_PR_COMMENT", "yes")]).is_err());
    }

    #[test]
    fn a_flaky_job_s_issue_is_off_unless_asked_for_and_needs_what_writes_it() {
        let db = ("CI_DATABASE_URL", "sqlite://ci.db");
        assert_eq!(config(&[db]).unwrap().flake_issues, None);
        assert_eq!(
            config(&[db, ("CI_FLAKE_ISSUES", "off")])
                .unwrap()
                .flake_issues,
            None
        );
        let err = config(&[db, ("CI_FLAKE_ISSUES", "3")]).unwrap_err();
        assert!(
            err.to_string()
                .starts_with("CI_FLAKE_ISSUES needs what writes"),
            "{err}"
        );
        let tokens = tempfile::tempdir().unwrap();
        std::fs::write(tokens.path().join("gitlab.com"), "glpat-a\n").unwrap();
        let dir = ("CI_GITLAB_TOKENS_DIR", tokens.path().to_str().unwrap());
        let on = config(&[db, dir, ("CI_FLAKE_ISSUES", "3")]).unwrap();
        assert_eq!(on.flake_issues, Some((3, 14)));
        let on = config(&[db, dir, ("CI_FLAKE_ISSUES", "2"), ("CI_FLAKE_DAYS", "30")]).unwrap();
        assert_eq!(on.flake_issues, Some((2, 30)));
        for (knob, bad) in [
            ("CI_FLAKE_ISSUES", "0"),
            ("CI_FLAKE_ISSUES", "on"),
            ("CI_FLAKE_DAYS", "0"),
            ("CI_FLAKE_DAYS", "365"),
        ] {
            let mut env = vec![db, dir, (knob, bad)];
            if knob == "CI_FLAKE_DAYS" {
                env.push(("CI_FLAKE_ISSUES", "3"));
            }
            assert!(config(&env).is_err(), "{knob}={bad}");
        }
    }

    #[test]
    fn a_merge_request_s_note_is_off_unless_asked_for_and_needs_a_token() {
        let db = ("CI_DATABASE_URL", "sqlite://ci.db");
        assert!(!config(&[db]).unwrap().gitlab_mr_notes);
        let err = config(&[db, ("CI_GITLAB_MR_NOTES", "on")]).unwrap_err();
        assert!(
            err.to_string()
                .starts_with("CI_GITLAB_MR_NOTES=on needs a token"),
            "{err}"
        );
        let tokens = tempfile::tempdir().unwrap();
        std::fs::write(tokens.path().join("gitlab.com"), "glpat-a\n").unwrap();
        let dir = tokens.path().to_str().unwrap();
        let on = config(&[
            db,
            ("CI_GITLAB_TOKENS_DIR", dir),
            ("CI_GITLAB_MR_NOTES", "on"),
        ])
        .unwrap();
        assert!(on.gitlab_mr_notes);
        assert!(config(&[
            db,
            ("CI_GITLAB_TOKENS_DIR", dir),
            ("CI_GITLAB_MR_NOTES", "yes")
        ])
        .is_err());
    }

    /// The App's key is dagron-ci's, or a token service's that dagron-ci asks
    /// under a secret: one or the other, and the service over https unless
    /// it is on this machine or the network is said to be closed.
    #[test]
    fn an_app_s_key_is_held_here_or_by_its_token_service() {
        let dir = tempfile::tempdir().unwrap();
        let db = ("CI_DATABASE_URL", "sqlite://ci.db");
        let secret = dir.path().join("token-secret");
        std::fs::write(&secret, "broker-s3cret\n").unwrap();
        let secret = ("CI_GITHUB_APP_TOKEN_SECRET_FILE", secret.to_str().unwrap());
        let id = ("CI_GITHUB_APP_ID", "7");
        let at = |url| ("CI_GITHUB_APP_TOKEN_URL", url);

        let c = config(&[db, id, at("https://tokens.example/ws/1"), secret]).unwrap();
        let app = c.github_app.as_ref().unwrap();
        assert_eq!((app.id(), app.keeper()), (7, "https://tokens.example/ws/1"));
        assert!(!format!("{c:?}").contains("s3cret"), "Debug redacts it");
        assert!(app.jwt(0).is_err(), "it holds no key to sign with");
        let local = config(&[db, id, at("http://127.0.0.1:9000/tokens"), secret]).unwrap();
        assert!(local.github_app.is_some());

        let cases: [(&[(&str, &str)], &str); 7] = [
            (
                &[at("https://tokens.example"), secret],
                "without CI_GITHUB_APP_ID",
            ),
            (
                &[id, at("https://tokens.example")],
                "without CI_GITHUB_APP_TOKEN_SECRET_FILE",
            ),
            (&[id, secret], "without CI_GITHUB_APP_TOKEN_URL"),
            (
                &[
                    id,
                    at("https://tokens.example"),
                    secret,
                    ("CI_GITHUB_APP_KEY_FILE", "/k"),
                ],
                "not both",
            ),
            (
                &[id, at("http://tokens.example/ws/1"), secret],
                "plain http to another host",
            ),
            (
                &[id, at("https://u:p@tokens.example"), secret],
                "no user, query or fragment",
            ),
            (
                &[
                    id,
                    at("http://tokens.example"),
                    secret,
                    ("CI_GITHUB_APP_TOKEN_URL_INSECURE", "yes"),
                ],
                "is allow, or unset",
            ),
        ];
        for (vars, said) in cases {
            let err = config(&[&[db][..], vars].concat()).unwrap_err().to_string();
            assert!(err.contains(said), "{vars:?}: {err}");
        }
        let closed = config(&[
            db,
            id,
            at("http://tokens.ci.svc:8090/tokens/w"),
            secret,
            ("CI_GITHUB_APP_TOKEN_URL_INSECURE", "allow"),
        ])
        .unwrap();
        assert!(
            closed.github_app.is_some(),
            "a closed network, as its operator says"
        );
    }

    #[test]
    fn the_knob_table_is_sorted() {
        let names: Vec<_> = KNOBS.iter().map(|k| k.name).collect();
        let mut sorted = names.clone();
        sorted.sort_unstable();
        assert_eq!(names, sorted);
    }
}

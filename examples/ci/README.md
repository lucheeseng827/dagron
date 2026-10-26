# Starter pipelines

Six pipelines to start a repository's CI from. Copy one to
`.dagron/ci/ci.yaml` in the repository, check it, and push:

```sh
mkdir -p .dagron/ci
cp path/to/dagron/examples/ci/rust.yaml .dagron/ci/ci.yaml
dagron-ci validate
```

| File | For | What it runs |
|---|---|---|
| [`rust.yaml`](rust.yaml) | a Cargo project | `cargo fmt --check` and `cargo clippy` in one job, `cargo test` in another |
| [`go.yaml`](go.yaml) | a Go module | `gofmt` and `go vet` in one job, `go test` in another |
| [`node.yaml`](node.yaml) | an npm project with a lockfile | `npm ci`, the `lint` script if there is one, `npm test` |
| [`python.yaml`](python.yaml) | a project with `requirements.txt` or `pyproject.toml` | a virtual environment, the project installed into it, `pytest`, and its report of what it ran |
| [`java.yaml`](java.yaml) | a Maven project | `mvn verify` |
| [`dbt.yaml`](dbt.yaml) | a dbt project | `dbt build`, against a Postgres of the job's own ([below](#a-dbt-project)) |

Each runs on a push to `main`, and on a pull request from one of the
repository's own branches whose author is its owner, a member of its
organization or a collaborator. Change the branch where yours is called
something else. A pull request from a fork, or by anyone else, runs nothing
in 0.11.0: its commit gets one status that says so
([what dagron-ci refuses](../../docs/CI.md)).

What they have in common, and why:

- **Each job names its image**, and runs its steps in order in one
  workspace, with the commit checked out.
- **Each job starts cold.** There is no cache between runs yet, so each
  downloads and builds what it needs every time. The steps are in the order
  that fails soonest.
- **Nothing is written outside the workspace** that the job cannot write as
  any user: npm's cache, the Python environments and Maven's repository
  are in it, and Go's build cache is under `/tmp`. A job may not run as
  root where you deploy this.
- **A job may say what its tests found** (`report:`). The Python starter
  does: pytest writes a JUnit file, and where the repository's jobs are
  check runs a test that failed is an annotation on its line of the commit
  ([`report:`](../../docs/CI_PIPELINES.md#report)). The others do not: Go's
  and Cargo's test commands write no such file by themselves, and Maven
  writes one a test class, which a `report:` cannot name before they exist.
- **A job's `env:` is a list**, of names and values. A step's is a map.
  `dagron-ci validate` says so when one is written as the other.

**Agents.** [`agents`](agents) has three recipes: a pipeline in which a
model reviews a pull request and annotates the lines it finds fault with;
triage by what broke a job; and a coding agent that runs the pipelines,
reads what failed and fixes it, as itself.

## A dbt project

`dbt.yaml` builds the project against a Postgres that is started beside the
job and removed with it (`services:`), so a pull request's models and tests
run against a database nothing else uses. It needs one thing of the
project: a profile that reads its connection from the job's variables,
kept in the repository beside `dbt_project.yml`.

```yaml
# profiles.yml
ci:                    # the name dbt_project.yml gives as `profile:`
  target: ci
  outputs:
    ci:
      type: postgres
      host: "{{ env_var('DBT_HOST') }}"
      port: 5432
      user: postgres
      password: "{{ env_var('DBT_PASSWORD') }}"
      dbname: postgres
      schema: ci
      threads: 4
```

- The starter sets `DBT_PROFILES_DIR`, `DBT_HOST` and `DBT_PASSWORD`. The
  password is written in the pipeline, since the database is the job's and
  goes with it. A project that builds against a warehouse of yours gives
  its credential as a secret
  ([secrets](../../docs/CI_PIPELINES.md#secrets)), and has no service.
- It builds every model, each time: nothing is kept between runs, so there
  is no earlier state to build only what changed against.
- `dbt deps` runs where the project has a `packages.yml`. A package that
  is fetched with git needs an image that has git, which this one has not.

A test in dagron's own tree (`crates/dagron-ci/tests/starters.rs`) compiles
each of these and has dagron read the result, so a starter that dagron would
refuse does not ship.

The reference for everything a pipeline may say is
[`docs/CI_PIPELINES.md`](../../docs/CI_PIPELINES.md), and
[`docs/CI_QUICKSTART.md`](../../docs/CI_QUICKSTART.md) gets a first check
green.

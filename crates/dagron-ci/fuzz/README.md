# Fuzzing dagron-ci's parsers

dagron-ci reads a good deal that is written outside it: a repository's
pipelines and workflow files, a forge's deliveries and its git, a job's own
lines and reports. Each target here hands one of those readers input nobody
wrote on purpose, and stops at the first it does not survive: a panic, an
overflow, a read out of bounds, a loop that does not end.

| Target | What it reads | Whose bytes they are |
|---|---|---|
| `pipeline` | a pipeline file, then compiled for each kind of event | a repository's, and a pull request's author's |
| `workflow` | a `.github/workflows` file, lowered for each kind of event and compiled | the same |
| `settings` | a repository's `.dagron/ci.yaml` | the same |
| `expr` | an expression of a workflow file, by itself, in a text and as a condition, and evaluated | the same |
| `glob` | a pattern of an `on:` filter, and a name asked of it | the same, and a branch's name is its pusher's |
| `webhook` | a delivery's body, as each kind of event; and a list of open pull requests | a forge's, after its signature |
| `git` | a ref listing, in either protocol | a forge's |
| `pack` | the answer to a shallow fetch, and a pack, read and its commits' trees compared | a forge's |
| `said` | the lines of a job's log that it says of itself | the job's |
| `reports` | JUnit XML, SARIF and a step's summary, as `dagron-ci-exec` reads them | the job's |
| `steps` | a task's environment, as the step runner reads its steps from it | dagron-ci's own, as an engine hands it on |
| `fleet` | a fleet file, and a job's labels asked of it | an operator's |

`pipeline` checks one thing more than survival: compiled twice for one
event, a pipeline comes to the same bytes.

## Running them

libFuzzer runs on Linux and macOS, on a nightly toolchain:

```sh
rustup toolchain install nightly
cargo install cargo-fuzz

crates/dagron-ci/fuzz/run.sh          # every target, a minute each
crates/dagron-ci/fuzz/run.sh 600 expr # one, for ten minutes
cd crates/dagron-ci && cargo +nightly fuzz run pipeline   # until stopped
```

`run.sh` first gives each target somewhere to start (`seed.sh`): a pipeline
and a workflow file start from the sources of the golden corpus
(`tests/corpus/cases`), and the rest from the few inputs in `seeds/`. What a
run finds beyond those is kept in `corpus/`, which is not in git: a longer
run starts from what the last one found.

## When a target stops

It writes the input to `artifacts/<target>/` and prints it.

```sh
cd crates/dagron-ci
cargo +nightly fuzz run <target> fuzz/artifacts/<target>/crash-…   # see it again
cargo +nightly fuzz tmin <target> fuzz/artifacts/<target>/crash-…  # make it small
```

The fix goes with a test that holds it: a case of the golden corpus where
the input is a pipeline or a workflow file, and a unit test beside the
reader otherwise. An input worth starting from is added to `seeds/`.

## A parser that is added

It gets a target: a file in `fuzz_targets/`, a `[[bin]]` in `Cargo.toml`,
and a row above. A reader that is the crate's own is reached through
`dagron_ci::fuzzing`, which exists for this.

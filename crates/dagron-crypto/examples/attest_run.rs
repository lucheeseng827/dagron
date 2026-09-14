//! `attest_run` — build, sign, verify and diff a run attestation from what a
//! *client* can see, so family 5 is demonstrable before the engine hook lands.
//!
//! ```text
//! cargo run -p dagron-crypto --example attest_run -- --keygen
//! cargo run -p dagron-crypto --example attest_run -- \
//!     sign <run.json> <spec.yaml> <hex-seed | @seed-file> \
//!     [--executor local] [--engine-version 0.9.1] [--prev <digest>] [--out <base>]
//! cargo run -p dagron-crypto --example attest_run -- verify <att.json> <att.sig>
//! cargo run -p dagron-crypto --example attest_run -- diff <a.json> <b.json>
//! cargo run -p dagron-crypto --example attest_run -- chain <a.json> <b.json> [...]
//! ```
//!
//! ## Read this before quoting anything it prints
//!
//! **This is a reconstruction, not the engine's own record.** It exists so the
//! workshop lab (`workshop/04-families/`) can demonstrate the family-5
//! primitive end to end today; `docs/FAMILIES.md` lists the engine hook that
//! will replace it.
//!
//! How good the reconstruction is depends on which API you curled, and the tool
//! reports which source it used per run rather than leaving you to guess:
//!
//! * **The engine's ops API** (`GET /runs/{id}`, port 8787) puts each task's
//!   persisted `input` on the row — the **expanded** `TaskSpec` the engine
//!   actually dispatched, after template expansion and `{{ param }}`
//!   substitution. Digests taken from it describe what ran.
//! * **dagron-api** (`GET /api/runs/{id}`, port 8080) does not: its `TaskRow`
//!   carries a name, status, attempt, output and timestamps and nothing else.
//!   There the tool falls back to the **spec file** you pass, which is what was
//!   *authored* — the same thing only for a spec with no templating.
//!
//! Two limits remain even on the ops-API path, and they are why the real hook
//! belongs inside the engine:
//!
//! 1. **No exit codes.** Neither API reports one, so the field is left absent
//!    rather than guessed from the status.
//! 2. **The trust envelope is the declared one, not the effective one.** What a
//!    task actually ran under also depends on the engine's
//!    `DAGRON_TASK_ISOLATION_FLOOR`, which no client can see at all — so a
//!    floor that tightened a task is invisible here and would not be in a
//!    record the engine wrote itself.
//!
//! So an empty `diff` means the two runs dispatched the same work and recorded
//! the same results, as far as a client can see. It is a weaker claim than the
//! shipped hook will make, and saying so is the difference between a
//! demonstration and a false one.
//!
//! No dependency beyond the crate's own plus `serde_yaml` as a dev-dependency:
//! the spec has to be parsed to get argv out of it, and dagron-crypto stays
//! free of a YAML parser at runtime.

use std::path::PathBuf;
use std::process::ExitCode;

use aes_gcm::aead::rand_core::RngCore as _;
use aes_gcm::aead::OsRng;
use anyhow::{bail, Context, Result};
use dagron_crypto::attest::{
    command_digest, env_digest, pubkeys_from_env, replay_diff, signature_b64, verify,
    verify_chain, Attestation, Divergence, TaskRecord, WhichRun, PUBKEYS_ENV,
};
use dagron_crypto::bundle::{sha256_hex, SigningKey};

const USAGE: &str = "usage:
  attest_run --keygen
  attest_run sign <run.json> <spec.yaml> <hex-seed | @seed-file>
             [--executor <local|docker|k8s>] [--engine-version <v>]
             [--prev <digest>] [--key-id <id>] [--out <base>]
  attest_run verify <att.json> <att.sig>
  attest_run diff  <att-a.json> <att-b.json>
  attest_run chain <att-1.json> <att-2.json> [...]        (oldest first)

  run.json   the body of GET /api/runs/{id}
  spec.yaml  the workflow the run was created from — see the module docs for
             why both are needed, and what this reconstruction does NOT prove";

/// Map a `Result` to a process exit code, printing the whole error chain.
///
/// `diff` is the one subcommand whose non-zero exit is not an error: it means
/// divergence was found, which is a finding rather than a failure, so it
/// returns its code through `Ok` rather than through here.
fn main() -> ExitCode {
    match run() {
        Ok(code) => code,
        Err(e) => {
            eprintln!("error: {e:#}");
            ExitCode::FAILURE
        }
    }
}

/// Dispatch on the subcommand, printing usage for anything unrecognised.
fn run() -> Result<ExitCode> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    match args.first().map(String::as_str) {
        Some("--keygen") => keygen(),
        Some("sign") => sign(&args[1..]),
        Some("verify") => verify_cmd(&args[1..]),
        Some("diff") => diff_cmd(&args[1..]),
        Some("chain") => chain_cmd(&args[1..]),
        _ => {
            eprintln!("{USAGE}");
            Ok(ExitCode::FAILURE)
        }
    }
}

// ── keygen ──────────────────────────────────────────────────────────────────

/// Print a fresh ed25519 seed, its public key, and the `export` line to paste
/// on every verifier.
///
/// The seed comes from the OS CSPRNG through the same `OsRng` the crate's AES
/// nonces use. It is printed once and never stored: a signing key this tool
/// wrote to disk on its own would be a key nobody chose the protection for.
fn keygen() -> Result<ExitCode> {
    let mut seed = [0u8; 32];
    OsRng.fill_bytes(&mut seed);
    let key = SigningKey::from_bytes(&seed);
    println!("seed (KEEP SECRET):  {}", hex::encode(seed));
    println!("public key:          {}", hex::encode(key.verifying_key().to_bytes()));
    println!();
    println!("export {PUBKEYS_ENV}={}", hex::encode(key.verifying_key().to_bytes()));
    Ok(ExitCode::SUCCESS)
}

// ── sign ────────────────────────────────────────────────────────────────────

/// Build an attestation from a run body and its spec, sign it, and write
/// `<base>.json` + `<base>.sig`.
///
/// Verifies what it just wrote before reporting success, so the tool can never
/// leave behind a record it would itself refuse — the same rule `bundle_sign`
/// follows.
fn sign(args: &[String]) -> Result<ExitCode> {
    let (positional, flags) = split_args(args)?;
    let [run_path, spec_path, seed_arg] = positional.as_slice() else {
        eprintln!("{USAGE}");
        return Ok(ExitCode::FAILURE);
    };

    let run_bytes = std::fs::read(run_path).with_context(|| format!("reading {run_path}"))?;
    let run: serde_json::Value =
        serde_json::from_slice(&run_bytes).with_context(|| format!("parsing {run_path} as JSON"))?;
    let run = normalize_run(run);
    let spec_text = std::fs::read_to_string(spec_path)
        .with_context(|| format!("reading {spec_path}"))?;
    let spec: serde_yaml::Value = serde_yaml::from_str(&spec_text)
        .with_context(|| format!("parsing {spec_path} as YAML"))?;

    // An unrecognised flag is refused, not ignored: `--exectuor k8s` would
    // otherwise leave `executor` at its default and put a claim nobody made
    // under a signature. The same reasoning as `deny_unknown_fields` on the
    // isolation block — a typo must not silently change what is attested.
    const SIGN_FLAGS: [&str; 5] = ["executor", "engine-version", "prev", "key-id", "out"];
    if let Some(unknown) = flags.keys().find(|k| !SIGN_FLAGS.contains(&k.as_str())) {
        bail!("unknown flag --{unknown}; sign accepts --{}", SIGN_FLAGS.join(", --"));
    }

    let key = signing_key(seed_arg)?;
    let executor = flags.get("executor").cloned().unwrap_or_else(|| "local".into());
    let engine_version = flags.get("engine-version").cloned().unwrap_or_else(|| "unknown".into());

    let att = build(&run, &spec, &spec_text, &executor, &engine_version)?;
    let att = match flags.get("prev") {
        Some(p) => att.chained_to(p.clone()),
        None => att,
    };
    let att = match flags.get("key-id") {
        Some(k) => att.with_key_id(k.clone()),
        None => att,
    };

    let (bytes, sig) = att.sign(&key)?;
    let digest = sha256_hex(&bytes);

    let base = flags
        .get("out")
        .cloned()
        .unwrap_or_else(|| format!("attestation-{}", short(&att.run_id)));
    let json_path = PathBuf::from(format!("{base}.json"));
    let sig_path = PathBuf::from(format!("{base}.sig"));
    std::fs::write(&json_path, &bytes).with_context(|| format!("writing {}", json_path.display()))?;
    std::fs::write(&sig_path, format!("{}\n", signature_b64(&sig)))
        .with_context(|| format!("writing {}", sig_path.display()))?;

    // Verify what was just written, with the derived public key: the tool must
    // never leave behind a record it would itself refuse. Same rule as
    // `bundle_sign`.
    let written = std::fs::read(&json_path)?;
    let written_sig = base64_decode(&std::fs::read_to_string(&sig_path)?)?;
    verify(&written, &written_sig, &[key.verifying_key()])
        .context("the record this tool just wrote does not verify — refusing to claim it does")?;

    println!("run       {}", att.run_id);
    println!("workflow  {}", att.workflow);
    println!("status    {}", att.status);
    println!("tasks     {}", att.tasks.len());
    println!("digest    {digest}");
    if let Some(p) = &att.prev {
        println!("prev      {p}");
    }
    println!("wrote     {} + {}", json_path.display(), sig_path.display());
    println!();
    println!("NOTE: a reconstruction from the API and the spec file, not the engine's");
    println!("      own record — see the module docs for what it does not prove.");
    Ok(ExitCode::SUCCESS)
}

/// Assemble an attestation from a run detail body and the spec it came from.
fn build(
    run: &serde_json::Value,
    spec: &serde_yaml::Value,
    spec_text: &str,
    executor: &str,
    engine_version: &str,
) -> Result<Attestation> {
    let run_id = str_field(run, "id").context("run JSON has no `id`")?;
    // `name` is the definition's name and may be absent on an ad-hoc run; fall
    // back to the spec's own, which is where it came from either way.
    let workflow = str_field(run, "name")
        .or_else(|| spec.get("name").and_then(|v| v.as_str()).map(str::to_string))
        .unwrap_or_else(|| "unknown".into());
    let status = str_field(run, "status").context("run JSON has no `status`")?;
    let started_at = str_field(run, "created_at").unwrap_or_default();
    let finished_at = str_field(run, "finished_at").unwrap_or_default();

    let authored = authored_tasks(spec)?;

    let rows = run
        .get("tasks")
        .and_then(|t| t.as_array())
        .context("run JSON has no `tasks` array")?;

    let mut tasks = Vec::with_capacity(rows.len());
    let mut from_dispatched = 0usize;
    for row in rows {
        let name = str_field(row, "name").context("a task row has no `name`")?;

        // Prefer the row's persisted `input`: that is the *expanded* TaskSpec
        // the engine dispatched, so its digests describe what ran rather than
        // what someone wrote. Only the ops API exposes it.
        let dispatched = match row.get("input").and_then(|v| v.as_str()) {
            // Absent is fine — dagron-api exposes no dispatched spec, and the
            // authored fallback below is the documented weaker path.
            None => None,
            // Present and unparseable is not. Falling back here would sign a
            // record that claims to describe what the engine dispatched while
            // actually describing what someone wrote.
            Some(text) => {
                let spec: serde_json::Value = serde_json::from_str(text)
                    .with_context(|| format!("task '{name}': its persisted input is not valid JSON"))?;
                Some(authored_from_json(&spec).with_context(|| format!("task '{name}'"))?)
            }
        };

        let source = match &dispatched {
            Some(d) => {
                from_dispatched += 1;
                Some(d)
            }
            // Fall back to the authored spec. Fan-out and gang instances are
            // named `<task>.<n>` and share the parent's definition, so match the
            // stem too — without it every expanded instance would attest an
            // empty command digest and the diff would be blind to exactly the
            // workloads that fan out.
            None => authored
                .iter()
                .find(|(n, _)| *n == name)
                .or_else(|| {
                    name.rsplit_once('.')
                        .and_then(|(stem, _)| authored.iter().find(|(n, _)| n == stem))
                })
                .map(|(_, t)| t),
        };

        let (command_dig, image, env_dig, iso_dig) = match source {
            Some(t) => (
                command_digest(&t.command),
                t.image.clone(),
                t.env.as_ref().map(|e| env_digest(e)),
                t.isolation.clone(),
            ),
            // Signing a task with empty digests is signing partial evidence:
            // replay_diff would report no drift for it however much changed.
            None => bail!(
                "task '{name}' matches neither a dispatched nor an authored specification, \
                 so there is nothing to attest about what it ran"
            ),
        };

        tasks.push(TaskRecord {
            name,
            status: str_field(row, "status").unwrap_or_else(|| "unknown".into()),
            attempts: row.get("attempt").and_then(|v| v.as_i64()).unwrap_or(0).max(0) as u32,
            // The API reports no exit code, so this is left absent rather than
            // guessed from the status — an attestation that invents a field is
            // worse than one that omits it.
            exit_code: None,
            command_digest: command_dig,
            image,
            env_digest: env_dig,
            isolation_digest: iso_dig,
            output_digest: row
                .get("output")
                .and_then(|v| v.as_str())
                .map(|o| sha256_hex(o.as_bytes())),
            started_at: str_field(row, "scheduled_at"),
            finished_at: str_field(row, "finished_at"),
        });
    }

    // Say which source was used, because the strength of every digest below
    // depends on it and a reader must not have to infer that from a port number.
    if from_dispatched == tasks.len() && !tasks.is_empty() {
        eprintln!("source    dispatched TaskSpec (ops API `input`) for all {} tasks", tasks.len());
    } else if from_dispatched == 0 {
        eprintln!("source    the AUTHORED spec file — this API exposes no dispatched spec,");
        eprintln!("          so templated fields may differ from what actually ran");
    } else {
        eprintln!(
            "source    mixed: {from_dispatched} of {} tasks from the dispatched spec, the rest authored",
            tasks.len()
        );
    }

    Ok(Attestation::new(
        run_id,
        workflow,
        sha256_hex(spec_text.as_bytes()),
        status,
        started_at,
        finished_at,
        engine_version,
        executor,
        tasks,
    ))
}

/// Pull the attestable fields out of a persisted (expanded) `TaskSpec`.
///
/// An isolation block that is present but does not parse is an error, not an
/// absent digest: omitting it would sign a record that says nothing about the
/// privileges a task ran under, and `replay_diff` would then miss trust-envelope
/// drift entirely. `deny_unknown_fields` on `IsolationSpec` makes this reachable
/// from a single misspelled key.
fn authored_from_json(spec: &serde_json::Value) -> Result<Authored> {
    Ok(Authored {
        command: spec
            .get("command")
            .and_then(|c| c.as_array())
            .map(|a| a.iter().filter_map(|x| x.as_str().map(str::to_string)).collect())
            .unwrap_or_default(),
        image: spec.get("docker_image").and_then(|v| v.as_str()).map(str::to_string),
        env: spec.get("env").and_then(|e| e.as_array()).map(|a| {
            a.iter()
                .filter_map(|kv| {
                    Some((
                        kv.get("name")?.as_str()?.to_string(),
                        kv.get("value").and_then(|v| v.as_str()).unwrap_or_default().to_string(),
                    ))
                })
                .collect()
        }),
        isolation: match spec.get("isolation") {
            None => None,
            Some(i) => {
                let parsed: dagron_core_isolation::IsolationSpec =
                    serde_json::from_value(i.clone())
                        .context("its isolation block did not parse")?;
                Some(sha256_hex(parsed.canonical().as_bytes()))
            }
        },
    })
}

/// Accept either API's run-detail shape.
///
/// dagron-api (`GET /api/runs/{id}`, port 8080) returns the run's fields at the
/// top level with `tasks` beside them. The engine's ops API
/// (`GET /runs/{id}`, port 8787) returns `{"run": {...}, "tasks": [...]}`.
/// Both are legitimate things for a lab to curl — the ops API is what a
/// single-binary local run exposes, dagron-api is what the compose stack does —
/// and requiring the reader to reshape one of them by hand would be a step that
/// exists only because this tool was lazy.
fn normalize_run(v: serde_json::Value) -> serde_json::Value {
    let (Some(run), Some(tasks)) = (v.get("run"), v.get("tasks")) else {
        return v;
    };
    let Some(obj) = run.as_object() else { return v };
    let mut flat = obj.clone();
    flat.insert("tasks".into(), tasks.clone());
    serde_json::Value::Object(flat)
}

/// What the spec says about each task, as far as a client can read it.
struct Authored {
    command: Vec<String>,
    image: Option<String>,
    env: Option<Vec<(String, String)>>,
    isolation: Option<String>,
}

/// Read each task's attestable fields out of the authored spec.
///
/// The fallback path, used only when the run body carries no dispatched
/// `TaskSpec` — see the module docs for what that costs.
fn authored_tasks(spec: &serde_yaml::Value) -> Result<Vec<(String, Authored)>> {
    let Some(tasks) = spec.get("tasks").and_then(|t| t.as_sequence()) else {
        return Ok(Vec::new());
    };
    let mut out = Vec::with_capacity(tasks.len());
    for t in tasks {
        // A sequence entry with no name is not a task; skipping it is right.
        // A task whose isolation block does not parse is a different matter —
        // see `authored_from_json`.
        let Some(name) = t.get("name").and_then(|n| n.as_str()).map(str::to_string) else {
            continue;
        };
        out.push({
            let build_one = || -> Result<(String, Authored)> {
            let command = t
                .get("command")
                .and_then(|c| c.as_sequence())
                .map(|s| s.iter().filter_map(|a| a.as_str().map(str::to_string)).collect())
                .unwrap_or_default();
            let env = t.get("env").and_then(|e| e.as_sequence()).map(|s| {
                s.iter()
                    .filter_map(|kv| {
                        Some((
                            kv.get("name")?.as_str()?.to_string(),
                            kv.get("value").and_then(|v| v.as_str()).unwrap_or_default().to_string(),
                        ))
                    })
                    .collect()
            });
            // The isolation block is digested through its own canonical form so
            // this matches what the engine will attest, rather than hashing
            // whatever byte order the YAML happened to be written in.
            let isolation = match t.get("isolation") {
                None => None,
                Some(i) => {
                    let parsed: dagron_core_isolation::IsolationSpec =
                        serde_yaml::from_value(i.clone())
                            .context("its isolation block did not parse")?;
                    Some(sha256_hex(parsed.canonical().as_bytes()))
                }
            };
            Ok((
                name.clone(),
                Authored {
                    command,
                    image: t.get("docker_image").and_then(|v| v.as_str()).map(str::to_string),
                    env,
                    isolation,
                },
            ))
            };
            build_one().with_context(|| format!("task '{name}'"))?
        });
    }
    Ok(out)
}

/// `dagron-core` is not a dependency of `dagron-crypto` (and must not become
/// one — the crate is deliberately free of the sqlite/postgres feature
/// exclusivity that would follow). The isolation block is small and stable, so
/// the example re-declares just enough of it to canonicalise one, matching
/// `dagron_core::isolation::IsolationSpec` field for field — **and attribute
/// for attribute**. The fields are the obvious half; `deny_unknown_fields` is
/// the half that was missed once, and a mirror that accepts what the original
/// rejects reports an envelope nobody declared.
mod dagron_core_isolation {
    use serde::Deserialize;
    use std::collections::BTreeMap;

    #[derive(Deserialize)]
    #[serde(rename_all = "snake_case")]
    pub enum Seccomp {
        Unconfined,
        RuntimeDefault,
    }

    // `deny_unknown_fields` is not decoration here: it is the attribute the real
    // `dagron_core::isolation::IsolationSpec` carries, and without it this
    // mirror accepts a misspelled key, deserializes to an all-`None` envelope
    // and digests the empty string. The record would then carry an isolation
    // digest that silently describes no isolation at all — worse than the
    // missing digest the caller now refuses, because it looks like evidence.
    #[derive(Deserialize, Default)]
    #[serde(deny_unknown_fields)]
    pub struct IsolationSpec {
        #[serde(default)]
        pub runtime_class: Option<String>,
        #[serde(default)]
        pub seccomp: Option<Seccomp>,
        #[serde(default)]
        pub read_only_root_fs: Option<bool>,
        #[serde(default)]
        pub no_new_privileges: Option<bool>,
        #[serde(default)]
        pub drop_all_capabilities: Option<bool>,
        #[serde(default)]
        pub run_as_non_root: Option<bool>,
        #[serde(default)]
        pub run_as_user: Option<i64>,
        #[serde(default)]
        pub service_account_token: Option<bool>,
    }

    impl IsolationSpec {
        /// Render the envelope as sorted `key=value` lines, matching
        /// `dagron_core::isolation::IsolationSpec::canonical` exactly.
        ///
        /// Unset fields are omitted rather than defaulted, so a digest
        /// distinguishes "nobody required a read-only root" from "explicitly
        /// not required".
        pub fn canonical(&self) -> String {
            let mut f: BTreeMap<&'static str, String> = BTreeMap::new();
            if let Some(v) = &self.runtime_class {
                f.insert("runtime_class", v.clone());
            }
            if let Some(v) = &self.seccomp {
                f.insert(
                    "seccomp",
                    match v {
                        Seccomp::Unconfined => "unconfined",
                        Seccomp::RuntimeDefault => "runtime_default",
                    }
                    .to_string(),
                );
            }
            for (k, v) in [
                ("read_only_root_fs", self.read_only_root_fs),
                ("no_new_privileges", self.no_new_privileges),
                ("drop_all_capabilities", self.drop_all_capabilities),
                ("run_as_non_root", self.run_as_non_root),
                ("service_account_token", self.service_account_token),
            ] {
                if let Some(v) = v {
                    f.insert(k, v.to_string());
                }
            }
            if let Some(v) = self.run_as_user {
                f.insert("run_as_user", v.to_string());
            }
            f.into_iter().map(|(k, v)| format!("{k}={v}")).collect::<Vec<_>>().join("\n")
        }
    }
}

// ── verify / diff / chain ───────────────────────────────────────────────────

/// Check one record's signature against `DAGRON_ATTEST_PUBKEYS`.
fn verify_cmd(args: &[String]) -> Result<ExitCode> {
    let [att_path, sig_path] = args else {
        eprintln!("{USAGE}");
        return Ok(ExitCode::FAILURE);
    };
    let bytes = std::fs::read(att_path).with_context(|| format!("reading {att_path}"))?;
    let sig = base64_decode(&std::fs::read_to_string(sig_path)?)?;
    let keys = pubkeys_from_env()?;

    let v = verify(&bytes, &sig, &keys)?;
    println!("VERIFIED");
    println!("  run     {}", v.attestation.run_id);
    println!("  digest  {}", v.digest);
    println!("  tasks   {}", v.attestation.tasks.len());
    Ok(ExitCode::SUCCESS)
}

/// Compare two attestations and report input drift separately from output
/// drift.
///
/// Exits non-zero when anything diverged, so a CI gate can use it directly —
/// divergence is a finding, not a crash.
fn diff_cmd(args: &[String]) -> Result<ExitCode> {
    let [a_path, b_path] = args else {
        eprintln!("{USAGE}");
        return Ok(ExitCode::FAILURE);
    };
    let a: Attestation = read_attestation(a_path)?;
    let b: Attestation = read_attestation(b_path)?;

    let diff = replay_diff(&a, &b);
    println!("A  {} ({})", a.run_id, a_path);
    println!("B  {} ({})", b.run_id, b_path);
    println!();

    if diff.is_empty() {
        println!("NO DIVERGENCE — same spec, task set, commands, images, environments,");
        println!("trust envelopes and outputs.");
        return Ok(ExitCode::SUCCESS);
    }

    let (input, output): (Vec<_>, Vec<_>) = diff.iter().partition(|d| d.is_input_drift());

    if !input.is_empty() {
        println!("INPUT DRIFT ({}) — the two runs were not asked to do the same thing,", input.len());
        println!("so the output comparison below is VOID.");
        for d in &input {
            println!("  {}", render(d));
        }
        println!();
    }
    if !output.is_empty() {
        let head = if input.is_empty() {
            "OUTPUT DRIFT — identical inputs, different results. This is nondeterminism"
        } else {
            "OUTPUT DRIFT — reported for completeness only; see the input drift above"
        };
        println!("{head}");
        if input.is_empty() {
            println!("in the workload, and these are the tasks responsible.");
        }
        for d in &output {
            println!("  {}", render(d));
        }
    }
    // Non-zero so a CI gate can use it: a divergence is a finding, not a crash.
    Ok(ExitCode::from(1))
}

/// Verify every record in a chain, then that each links to the one before it.
///
/// Takes the records oldest first and expects each `<name>.json` to have its
/// `<name>.sig` beside it.
fn chain_cmd(args: &[String]) -> Result<ExitCode> {
    if args.len() < 2 {
        eprintln!("{USAGE}");
        return Ok(ExitCode::FAILURE);
    }
    let keys = pubkeys_from_env()?;

    let mut verified = Vec::with_capacity(args.len());
    for path in args {
        let bytes = std::fs::read(path).with_context(|| format!("reading {path}"))?;
        let sig_path = match path.strip_suffix(".json") {
            Some(base) => format!("{base}.sig"),
            // Otherwise the derived path is empty and the error reads
            // "reading  (the signature beside foo.att)", which names nothing.
            None => bail!("{path} must end in .json — the signature is read from the matching .sig"),
        };
        let sig = base64_decode(
            &std::fs::read_to_string(&sig_path)
                .with_context(|| format!("reading {sig_path} (the signature beside {path})"))?,
        )?;
        verified.push(verify(&bytes, &sig, &keys).with_context(|| format!("verifying {path}"))?);
    }

    verify_chain(&verified)?;
    println!("CHAIN INTACT — {} records, each linked to the one before it.", verified.len());
    for v in &verified {
        println!("  {}  {}", &v.digest[..12], v.attestation.run_id);
    }
    Ok(ExitCode::SUCCESS)
}

/// One divergence as a single readable line, digests abbreviated.
fn render(d: &Divergence) -> String {
    match d {
        Divergence::SpecDigest { a, b } => {
            format!("spec_digest: {} != {} — a different workflow", short(a), short(b))
        }
        Divergence::TaskOnlyIn { run, task } => {
            let which = match run {
                WhichRun::A => "A",
                WhichRun::B => "B",
            };
            format!("task '{task}' present only in {which}")
        }
        Divergence::InputDrift { task, field, a, b } => {
            format!("{task}.{field}: {} != {}", short(a), short(b))
        }
        Divergence::OutputDrift { task, field, a, b } => {
            format!("{task}.{field}: {} != {}", short(a), short(b))
        }
        Divergence::DuplicateTask { run, task } => {
            let which = match run {
                WhichRun::A => "A",
                WhichRun::B => "B",
            };
            format!("run {which} lists task '{task}' more than once — it cannot be compared")
        }
    }
}

// ── plumbing ────────────────────────────────────────────────────────────────

/// Parse an attestation without checking its signature.
///
/// Deliberately unsigned: `diff` answers "did these two runs do the same
/// thing", which is a question about content. Whether either record is
/// trustworthy is what `verify` and `chain` are for, and conflating them would
/// make a diff impossible to run on a record whose key you do not hold.
fn read_attestation(path: &str) -> Result<Attestation> {
    let bytes = std::fs::read(path).with_context(|| format!("reading {path}"))?;
    serde_json::from_slice(&bytes).with_context(|| format!("parsing {path} as an attestation"))
}

/// Load a signing key from a hex seed, or from `@path` to keep it out of shell
/// history.
fn signing_key(arg: &str) -> Result<SigningKey> {
    let hex_seed = match arg.strip_prefix('@') {
        Some(path) => std::fs::read_to_string(path)
            .with_context(|| format!("reading seed file {path}"))?
            .trim()
            .to_string(),
        None => arg.to_string(),
    };
    let raw = hex::decode(hex_seed.trim()).context("seed must be 64 hex characters")?;
    let seed: [u8; 32] = raw
        .try_into()
        .map_err(|_| anyhow::anyhow!("seed must decode to exactly 32 bytes"))?;
    Ok(SigningKey::from_bytes(&seed))
}

/// Decode a signature file's base64, tolerating the trailing newline it is
/// written with.
fn base64_decode(text: &str) -> Result<Vec<u8>> {
    use base64::Engine as _;
    base64::engine::general_purpose::STANDARD
        .decode(text.trim())
        .context("signature is not valid base64")
}

/// A string field from a JSON object, or `None` when absent or not a string.
fn str_field(v: &serde_json::Value, key: &str) -> Option<String> {
    v.get(key).and_then(|x| x.as_str()).map(str::to_string)
}

/// First 12 characters, for readable output. Full values stay in the record.
fn short(s: &str) -> String {
    // By character, not by byte: `&s[..12]` panics when byte 12 lands inside a
    // multibyte sequence, and `render` puts image references and arbitrary
    // field values through here.
    match s.char_indices().nth(12) {
        Some((cut, _)) => format!("{}…", &s[..cut]),
        None => s.to_string(),
    }
}

/// Split `--flag value` pairs out of positional arguments.
fn split_args(args: &[String]) -> Result<(Vec<String>, std::collections::BTreeMap<String, String>)> {
    let mut positional = Vec::new();
    let mut flags = std::collections::BTreeMap::new();
    let mut i = 0;
    while i < args.len() {
        match args[i].strip_prefix("--") {
            Some(name) => {
                let value = args
                    .get(i + 1)
                    .with_context(|| format!("--{name} needs a value"))?;
                if value.starts_with("--") {
                    bail!("--{name} needs a value, got the flag '{value}'");
                }
                flags.insert(name.to_string(), value.clone());
                i += 2;
            }
            None => {
                positional.push(args[i].clone());
                i += 1;
            }
        }
    }
    Ok((positional, flags))
}

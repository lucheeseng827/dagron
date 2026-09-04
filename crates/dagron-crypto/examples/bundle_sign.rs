//! `bundle_sign` — make a signing key, and sign a directory of workflow specs
//! into a bundle the GitOps worker, the management API and the fleet plane
//! all verify the same way (docs/BUNDLES.md).
//!
//! ```text
//! cargo run -p dagron-crypto --example bundle_sign -- --keygen
//! cargo run -p dagron-crypto --example bundle_sign -- <dir> <hex-seed | @seed-file> \
//!     [--version <v>] [--name <n>] [--key-id <id>]
//! ```
//!
//! * `--keygen` prints a fresh 32-byte seed (hex — keep it secret), the
//!   matching public key, and the `export DAGRON_BUNDLE_PUBKEYS=…` line to
//!   paste on every verifier. The seed comes from the OS CSPRNG through the
//!   same `OsRng` the crate's AES nonces use.
//! * Signing walks `<dir>` for `*.yaml` / `*.yml` (hidden entries and symlinks
//!   skipped), writes `manifest.json` listing each by path and SHA-256, signs
//!   the exact manifest bytes, writes `manifest.sig` (base64), and then
//!   **verifies the directory back** with the derived public key — so the
//!   tool never leaves behind a bundle it would itself refuse.
//!
//! The seed may be given as `@path` to read it from a file rather than the
//! command line, where it would land in shell history.
//!
//! No dependencies beyond the crate's own: the RFC 3339 timestamp is computed
//! from `SystemTime` with a civil-date conversion, because `chrono` is not a
//! dependency of dagron-crypto and this example must build wherever the crate
//! does.

use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::time::{SystemTime, UNIX_EPOCH};

use aes_gcm::aead::rand_core::RngCore as _;
use aes_gcm::aead::OsRng;
use anyhow::{bail, Context, Result};
use dagron_crypto::bundle::{
    pubkeys_from_env, sha256_hex, sign_manifest, signature_b64, verify_bundle_dir, Manifest,
    SigningKey, SpecEntry, BUNDLE_FORMAT, MANIFEST_FILE, PUBKEYS_ENV, SIGNATURE_FILE,
};

const USAGE: &str = "usage:
  bundle_sign --keygen
  bundle_sign <dir> <hex-seed | @seed-file> [--version <v>] [--name <n>] [--key-id <id>]
  bundle_sign --verify <dir>

  --keygen        print a new seed (secret), its public key, and the
                  DAGRON_BUNDLE_PUBKEYS line for verifiers
  <dir>           directory of *.yaml / *.yml specs to sign in place
  <hex-seed>      64 hex characters from --keygen; @path reads them from a file
  --version <v>   bundle version (default: UTC timestamp)
  --name <n>      bundle name (default: the directory's name)
  --key-id <id>   free-form key identifier recorded in the manifest
  --verify <dir>  verify a bundle directory against DAGRON_BUNDLE_PUBKEYS, as
                  the GitOps worker, the API and a unit would; exit 1 if refused";

fn main() -> ExitCode {
    match run(std::env::args().skip(1).collect()) {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("bundle_sign: {e:#}");
            ExitCode::FAILURE
        }
    }
}

fn run(args: Vec<String>) -> Result<()> {
    if args.iter().any(|a| a == "-h" || a == "--help") || args.is_empty() {
        println!("{USAGE}");
        return Ok(());
    }
    if args.iter().any(|a| a == "--keygen") {
        return keygen();
    }
    if args.first().map(String::as_str) == Some("--verify") {
        let [_, dir] = args.as_slice() else {
            bail!("--verify takes exactly one directory\n{USAGE}");
        };
        return verify(Path::new(dir));
    }

    let mut positional = Vec::new();
    let mut version = None;
    let mut name = None;
    let mut key_id = None;
    let mut it = args.into_iter();
    while let Some(a) = it.next() {
        match a.as_str() {
            "--version" => version = Some(it.next().context("--version needs a value")?),
            "--name" => name = Some(it.next().context("--name needs a value")?),
            "--key-id" => key_id = Some(it.next().context("--key-id needs a value")?),
            other if other.starts_with("--") => bail!("unknown flag '{other}'\n{USAGE}"),
            _ => positional.push(a),
        }
    }
    let [dir, seed] = positional.as_slice() else {
        bail!("expected <dir> <hex-seed>\n{USAGE}");
    };
    sign_dir(Path::new(dir), seed, version, name, key_id)
}

fn keygen() -> Result<()> {
    let mut seed = [0u8; 32];
    OsRng.fill_bytes(&mut seed);
    let key = SigningKey::from_bytes(&seed);
    let public = hex::encode(key.verifying_key().to_bytes());
    println!("# signing seed — keep this secret; it is the whole private key");
    println!("{}", hex::encode(seed));
    println!("# public key — safe to publish");
    println!("{public}");
    println!("# on every verifier (dagron-gitops, dagron-api, each unit):");
    println!("export {PUBKEYS_ENV}={public}");
    Ok(())
}

fn sign_dir(
    dir: &Path,
    seed_arg: &str,
    version: Option<String>,
    name: Option<String>,
    key_id: Option<String>,
) -> Result<()> {
    let key = SigningKey::from_bytes(&read_seed(seed_arg)?);
    if !dir.is_dir() {
        bail!("{} is not a directory", dir.display());
    }

    let mut paths = Vec::new();
    collect_specs(dir, dir, &mut paths).with_context(|| format!("scanning {}", dir.display()))?;
    paths.sort();
    if paths.is_empty() {
        bail!("no *.yaml / *.yml specs under {}", dir.display());
    }
    let mut specs = Vec::with_capacity(paths.len());
    for rel in &paths {
        let bytes = std::fs::read(dir.join(rel)).with_context(|| format!("reading {rel}"))?;
        specs.push(SpecEntry { path: rel.clone(), sha256: sha256_hex(&bytes) });
    }

    let now = SystemTime::now().duration_since(UNIX_EPOCH).context("system clock is before 1970")?.as_secs();
    let name = name.or_else(|| dir.file_name().map(|n| n.to_string_lossy().into_owned())).unwrap_or_default();
    if name.trim().is_empty() {
        bail!("cannot derive a bundle name from {} — pass --name", dir.display());
    }
    let manifest = Manifest {
        format: BUNDLE_FORMAT.into(),
        name,
        version: version.unwrap_or_else(|| compact_utc(now)),
        created_at: rfc3339_utc(now),
        key_id,
        specs,
    };
    let manifest_bytes = manifest.to_bytes()?;
    let signature = sign_manifest(&manifest_bytes, &key);

    std::fs::write(dir.join(MANIFEST_FILE), &manifest_bytes)
        .with_context(|| format!("writing {MANIFEST_FILE}"))?;
    std::fs::write(dir.join(SIGNATURE_FILE), format!("{}\n", signature_b64(&signature)))
        .with_context(|| format!("writing {SIGNATURE_FILE}"))?;

    // Never leave behind a bundle this tool's own verifier would refuse.
    let verified = verify_bundle_dir(dir, &[key.verifying_key()])
        .context("the bundle just written does not verify — nothing else was changed, inspect the directory")?;
    println!("signed {} spec(s) in {}", verified.specs.len(), dir.display());
    for (path, _) in &verified.specs {
        println!("  {path}");
    }
    println!("provenance: {}", verified.provenance());
    println!("digest:     {}", verified.digest);
    Ok(())
}

/// What every consumer does before applying: the trust set from
/// `DAGRON_BUNDLE_PUBKEYS`, the directory verified against it. A refusal is
/// the same message the GitOps worker would store on the repo row.
fn verify(dir: &Path) -> Result<()> {
    let keys = pubkeys_from_env()?;
    let verified = verify_bundle_dir(dir, &keys)?;
    println!("verified {} against {} trusted key(s)", dir.display(), keys.len());
    for (path, _) in &verified.specs {
        println!("  {path}");
    }
    println!("provenance: {}", verified.provenance());
    println!("digest:     {}", verified.digest);
    Ok(())
}

/// The 32-byte seed from 64 hex characters, or from a file when given as `@path`.
fn read_seed(arg: &str) -> Result<[u8; 32]> {
    let text = match arg.strip_prefix('@') {
        Some(path) => std::fs::read_to_string(path).with_context(|| format!("reading seed file {path}"))?,
        None => arg.to_string(),
    };
    let bytes = hex::decode(text.trim()).context("seed is not hex")?;
    <[u8; 32]>::try_from(bytes.as_slice())
        .map_err(|_| anyhow::anyhow!("seed decodes to {} bytes, expected 32", bytes.len()))
}

/// Every `*.yaml` / `*.yml` under `dir` as a `/`-joined path relative to
/// `root`. Hidden entries and symlinks are skipped — the same rule the
/// verifier applies, so what gets listed is what will be read back.
fn collect_specs(root: &Path, dir: &Path, out: &mut Vec<String>) -> Result<()> {
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        if entry.file_type()?.is_symlink() {
            continue;
        }
        let file_name = entry.file_name();
        if file_name.to_string_lossy().starts_with('.') {
            continue;
        }
        let path: PathBuf = entry.path();
        if path.is_dir() {
            collect_specs(root, &path, out)?;
        } else if matches!(path.extension().and_then(|e| e.to_str()), Some("yaml") | Some("yml")) {
            let rel = path.strip_prefix(root).unwrap_or(&path);
            let parts: Vec<String> =
                rel.components().map(|c| c.as_os_str().to_string_lossy().into_owned()).collect();
            out.push(parts.join("/"));
        }
    }
    Ok(())
}

/// `YYYY-MM-DDTHH:MM:SSZ` for a Unix timestamp.
fn rfc3339_utc(secs: u64) -> String {
    let (y, m, d, hh, mm, ss) = civil(secs);
    format!("{y:04}-{m:02}-{d:02}T{hh:02}:{mm:02}:{ss:02}Z")
}

/// `YYYYMMDDTHHMMSSZ` — the default bundle version: sortable, unambiguous,
/// and free of characters a shell or a path would mind.
fn compact_utc(secs: u64) -> String {
    let (y, m, d, hh, mm, ss) = civil(secs);
    format!("{y:04}{m:02}{d:02}T{hh:02}{mm:02}{ss:02}Z")
}

/// Unix seconds → proleptic Gregorian UTC (Howard Hinnant's days-to-civil).
fn civil(secs: u64) -> (i64, u32, u32, u32, u32, u32) {
    let days = (secs / 86_400) as i64;
    let rem = secs % 86_400;
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    let y = if m <= 2 { y + 1 } else { y };
    (y, m, d, (rem / 3600) as u32, ((rem % 3600) / 60) as u32, (rem % 60) as u32)
}

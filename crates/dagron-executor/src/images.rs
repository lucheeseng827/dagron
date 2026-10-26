//! The image cache's settings: where a task's image is pulled from, and with
//! what.
//!
//! - **A mirror per registry** ([`ENV_MIRRORS`]): `docker.io=mirror:5000/hub`
//!   pulls `alpine:3.20` as `mirror:5000/hub/library/alpine:3.20`; `*`
//!   takes every registry not named, keeping the registry's name in the
//!   path (`*=mirror:5000` pulls `ghcr.io/o/i` as `mirror:5000/ghcr.io/o/i`),
//!   which is an install with a mirror and nothing else. An image is never
//!   pulled from its own registry once a mirror is named for it: nothing
//!   falls back. Docker and Kubernetes alike.
//! - **The engine's credentials** ([`ENV_AUTH_FILE`], Docker; `ENV_PULL_SECRETS`
//!   is Kubernetes' own): a Docker `config.json`'s `auths`, sent to the
//!   daemon with a pull, never to a task.
//!
//! What a pull of a run that is not trusted is given is the caller's to say:
//! the executors give such a run's images neither credentials nor secrets,
//! so that it cannot name a private image the engine may read.

use std::collections::BTreeMap;

use anyhow::{anyhow, bail, Context, Result};
use bollard::auth::DockerCredentials;

/// A registry per line or comma: `REGISTRY=MIRROR[/PATH]`, `*` for every
/// registry not named.
pub const ENV_MIRRORS: &str = "DAGRON_REGISTRY_MIRRORS";
/// A Docker `config.json` whose `auths` the Docker executor pulls with.
pub const ENV_AUTH_FILE: &str = "DAGRON_REGISTRY_AUTH_FILE";
/// The pull secrets a task pod names (Kubernetes), comma separated.
pub const ENV_PULL_SECRETS: &str = "DAGRON_IMAGE_PULL_SECRETS";

/// Docker Hub, as an image reference names it when it names none.
pub const DOCKER_HUB: &str = "docker.io";

/// An image reference taken apart as Docker reads it: its registry, its
/// repository's path on it, and what follows (`:tag`, `@digest`, both, or
/// nothing).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Reference {
    pub registry: String,
    pub path: String,
    pub rest: String,
}

impl Reference {
    pub fn parse(image: &str) -> Reference {
        let (name, rest) = match image.find('@') {
            Some(at) => split_tag(&image[..at], &image[at..]),
            None => split_tag(image, ""),
        };
        let (registry, path) = match name.split_once('/') {
            Some((first, path))
                if first.contains('.') || first.contains(':') || first == "localhost" =>
            {
                (canonical_registry(first), path.to_string())
            }
            _ => (DOCKER_HUB.to_string(), name.to_string()),
        };
        // Docker Hub's official images live under `library/`.
        let path = if registry == DOCKER_HUB && !path.contains('/') {
            format!("library/{path}")
        } else {
            path
        };
        Reference {
            registry,
            path,
            rest,
        }
    }

    /// The repository, without a tag or digest: `docker.io/library/alpine`.
    pub fn repository(&self) -> String {
        format!("{}/{}", self.registry, self.path)
    }

    /// Whether the reference names a digest already.
    pub fn pinned(&self) -> bool {
        self.rest.contains('@')
    }
}

/// `name` and `tail`, with a `:tag` at the end of `name` (after its last
/// `/`, so a registry's port is not taken for one) moved to `tail`.
fn split_tag(name: &str, tail: &str) -> (String, String) {
    let slash = name.rfind('/').map_or(0, |i| i + 1);
    match name[slash..].rfind(':') {
        Some(colon) => (
            name[..slash + colon].to_string(),
            format!("{}{tail}", &name[slash + colon..]),
        ),
        None => (name.to_string(), tail.to_string()),
    }
}

/// A registry's name as a reference or an auth file writes it, made one:
/// Docker Hub's several names are `docker.io`.
fn canonical_registry(host: &str) -> String {
    let host = host
        .trim()
        .trim_start_matches("https://")
        .trim_start_matches("http://");
    let host = host.split('/').next().unwrap_or(host);
    match host {
        "index.docker.io" | "registry-1.docker.io" | "registry.hub.docker.com" => {
            DOCKER_HUB.to_string()
        }
        _ => host.to_ascii_lowercase(),
    }
}

/// One registry's credential, as an auth file holds it.
#[derive(Clone, PartialEq, Eq)]
struct Auth {
    username: Option<String>,
    password: Option<String>,
    identity_token: Option<String>,
}

impl std::fmt::Debug for Auth {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Auth")
            .field("username", &self.username)
            .field("password", &self.password.as_ref().map(|_| "***"))
            .field(
                "identity_token",
                &self.identity_token.as_ref().map(|_| "***"),
            )
            .finish()
    }
}

/// Where images are pulled from, and with what: [`ENV_MIRRORS`] and
/// [`ENV_AUTH_FILE`].
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Registries {
    /// Each registry's mirror, `*` for the rest; a mirror is `HOST[/PATH]`.
    mirrors: BTreeMap<String, String>,
    auths: BTreeMap<String, Auth>,
}

impl Registries {
    /// What the environment says; a setting that cannot be read stops the
    /// engine here.
    pub fn from_env() -> Result<Registries> {
        let var = |name: &str| std::env::var(name).ok().filter(|v| !v.trim().is_empty());
        let auths = match var(ENV_AUTH_FILE) {
            Some(path) => Some(
                std::fs::read_to_string(path.trim())
                    .with_context(|| format!("{ENV_AUTH_FILE}: cannot read {}", path.trim()))?,
            ),
            None => None,
        };
        Registries::parse(var(ENV_MIRRORS).as_deref(), auths.as_deref())
    }

    /// The mirrors' setting and the auth file's content.
    pub fn parse(mirrors: Option<&str>, auth_file: Option<&str>) -> Result<Registries> {
        let mut out = Registries::default();
        for entry in mirrors
            .unwrap_or_default()
            .split([',', '\n'])
            .map(str::trim)
            .filter(|e| !e.is_empty())
        {
            let Some((registry, mirror)) = entry.split_once('=') else {
                bail!("{ENV_MIRRORS}: {entry:?} is not REGISTRY=MIRROR");
            };
            let mirror = mirror.trim().trim_end_matches('/');
            if mirror.is_empty() || mirror.contains("://") || mirror.contains(['@', ' ']) {
                bail!(
                    "{ENV_MIRRORS}: {entry:?} names no mirror as HOST[/PATH] (no scheme, no \
                     user, no space)"
                );
            }
            let registry = match registry.trim() {
                "*" => "*".to_string(),
                "" => bail!("{ENV_MIRRORS}: {entry:?} names no registry"),
                other => canonical_registry(other),
            };
            if out
                .mirrors
                .insert(registry.clone(), mirror.to_string())
                .is_some()
            {
                bail!("{ENV_MIRRORS}: {registry} is named twice");
            }
        }
        if let Some(text) = auth_file {
            out.auths = parse_auths(text)?;
        }
        Ok(out)
    }

    pub fn is_empty(&self) -> bool {
        self.mirrors.is_empty() && self.auths.is_empty()
    }

    /// `image` as it is pulled: through its registry's mirror, or `*`'s, or
    /// as it is. An image already on a mirror is left as it is.
    pub fn mirrored(&self, image: &str) -> String {
        if self.mirrors.is_empty() {
            return image.to_string();
        }
        let r = Reference::parse(image);
        let on_a_mirror = self
            .mirrors
            .values()
            .any(|m| canonical_registry(m) == r.registry);
        if on_a_mirror {
            return image.to_string();
        }
        match (self.mirrors.get(&r.registry), self.mirrors.get("*")) {
            (Some(mirror), _) => format!("{mirror}/{}{}", r.path, r.rest),
            (None, Some(mirror)) => format!("{mirror}/{}/{}{}", r.registry, r.path, r.rest),
            (None, None) => image.to_string(),
        }
    }

    /// The credential for `image`'s registry (the image as it is pulled,
    /// after [`Self::mirrored`]), if the auth file has one.
    pub fn credentials(&self, image: &str) -> Option<DockerCredentials> {
        let registry = Reference::parse(image).registry;
        let auth = self.auths.get(&registry)?;
        Some(DockerCredentials {
            username: auth.username.clone(),
            password: auth.password.clone(),
            identitytoken: auth.identity_token.clone(),
            serveraddress: Some(match registry.as_str() {
                DOCKER_HUB => "https://index.docker.io/v1/".to_string(),
                other => other.to_string(),
            }),
            ..Default::default()
        })
    }
}

/// A Docker `config.json`'s `auths`, by registry. What a credential helper
/// or store holds is not read: the engine has no helper to run.
fn parse_auths(text: &str) -> Result<BTreeMap<String, Auth>> {
    let doc: serde_json::Value = serde_json::from_str(text)
        .map_err(|e| anyhow!("{ENV_AUTH_FILE} is not a Docker config.json: {e}"))?;
    for helper in ["credsStore", "credHelpers"] {
        if doc.get(helper).is_some_and(|v| !v.is_null()) {
            bail!(
                "{ENV_AUTH_FILE} names a credential helper ({helper}), which the engine does \
                 not run: write each registry's credential under \"auths\""
            );
        }
    }
    let mut out = BTreeMap::new();
    let Some(auths) = doc.get("auths") else {
        return Ok(out);
    };
    let auths = auths
        .as_object()
        .ok_or_else(|| anyhow!("{ENV_AUTH_FILE}: \"auths\" is not an object"))?;
    for (registry, entry) in auths {
        let field = |name: &str| {
            entry
                .get(name)
                .and_then(|v| v.as_str())
                .filter(|v| !v.is_empty())
                .map(str::to_string)
        };
        let (mut username, mut password) = (field("username"), field("password"));
        if let Some(encoded) = field("auth") {
            let plain = decode_base64(&encoded)
                .and_then(|b| String::from_utf8(b).ok())
                .ok_or_else(|| anyhow!("{ENV_AUTH_FILE}: {registry}'s \"auth\" is not base64"))?;
            let (user, pass) = plain.split_once(':').ok_or_else(|| {
                anyhow!("{ENV_AUTH_FILE}: {registry}'s \"auth\" is not USER:PASSWORD")
            })?;
            username = Some(user.to_string());
            password = Some(pass.to_string());
        }
        let identity_token = field("identitytoken");
        if password.is_none() && identity_token.is_none() {
            bail!("{ENV_AUTH_FILE}: {registry} has no password, \"auth\" or \"identitytoken\"");
        }
        out.insert(
            canonical_registry(registry),
            Auth {
                username,
                password,
                identity_token,
            },
        );
    }
    Ok(out)
}

/// Standard base64, with or without padding; `None` where it is not.
fn decode_base64(text: &str) -> Option<Vec<u8>> {
    let value = |c: u8| -> Option<u32> {
        Some(match c {
            b'A'..=b'Z' => c - b'A',
            b'a'..=b'z' => c - b'a' + 26,
            b'0'..=b'9' => c - b'0' + 52,
            b'+' => 62,
            b'/' => 63,
            _ => return None,
        } as u32)
    };
    let bytes: Vec<u8> = text.trim().trim_end_matches('=').bytes().collect();
    if bytes.len() % 4 == 1 {
        return None;
    }
    let mut out = Vec::with_capacity(bytes.len() * 3 / 4);
    for chunk in bytes.chunks(4) {
        let mut n = 0u32;
        for (i, &c) in chunk.iter().enumerate() {
            n |= value(c)? << (18 - 6 * i);
        }
        out.push((n >> 16) as u8);
        if chunk.len() > 2 {
            out.push((n >> 8) as u8);
        }
        if chunk.len() > 3 {
            out.push(n as u8);
        }
    }
    Some(out)
}

/// The digest `image` was pulled at, among the daemon's `RepoDigests` for it
/// (`repo@sha256:…`), as a reference to run by: the one of its own
/// repository. `None` where it has none (built here, or never pulled).
pub fn pinned(image: &str, repo_digests: &[String]) -> Option<String> {
    let wanted = Reference::parse(image);
    if wanted.pinned() {
        return None;
    }
    repo_digests.iter().find_map(|held| {
        let (repo, digest) = held.split_once('@')?;
        let got = Reference::parse(repo);
        (got.registry == wanted.registry && got.path == wanted.path)
            .then(|| format!("{}@{digest}", image_name(image)))
    })
}

/// `image` without its tag or digest, as it was written.
fn image_name(image: &str) -> String {
    let name = image.split('@').next().unwrap_or(image);
    split_tag(name, "").0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_reference_is_taken_apart_as_docker_reads_it() {
        for (image, registry, path, rest) in [
            ("alpine", "docker.io", "library/alpine", ""),
            ("alpine:3.20", "docker.io", "library/alpine", ":3.20"),
            ("acme/tool:1", "docker.io", "acme/tool", ":1"),
            (
                "docker.io/library/alpine:3",
                "docker.io",
                "library/alpine",
                ":3",
            ),
            ("index.docker.io/acme/tool", "docker.io", "acme/tool", ""),
            (
                "ghcr.io/o/i:v1@sha256:ab",
                "ghcr.io",
                "o/i",
                ":v1@sha256:ab",
            ),
            ("localhost:5000/x", "localhost:5000", "x", ""),
            ("localhost/x:1", "localhost", "x", ":1"),
            (
                "mirror:5000/hub/library/alpine",
                "mirror:5000",
                "hub/library/alpine",
                "",
            ),
            ("quay.io/o/i@sha256:cd", "quay.io", "o/i", "@sha256:cd"),
        ] {
            assert_eq!(
                Reference::parse(image),
                Reference {
                    registry: registry.into(),
                    path: path.into(),
                    rest: rest.into()
                },
                "{image}"
            );
        }
    }

    #[test]
    fn an_image_is_pulled_through_its_registry_s_mirror() {
        let r = Registries::parse(
            Some("docker.io=mirror.local:5000/hub, ghcr.io=mirror.local:5000/ghcr"),
            None,
        )
        .unwrap();
        assert_eq!(
            r.mirrored("alpine:3.20"),
            "mirror.local:5000/hub/library/alpine:3.20"
        );
        assert_eq!(
            r.mirrored("acme/tool@sha256:ab"),
            "mirror.local:5000/hub/acme/tool@sha256:ab"
        );
        assert_eq!(
            r.mirrored("ghcr.io/o/i:v1"),
            "mirror.local:5000/ghcr/o/i:v1"
        );
        assert_eq!(
            r.mirrored("quay.io/o/i:v1"),
            "quay.io/o/i:v1",
            "no mirror for it"
        );
        assert_eq!(
            r.mirrored("mirror.local:5000/hub/library/alpine:3.20"),
            "mirror.local:5000/hub/library/alpine:3.20",
            "on a mirror already"
        );
        let all = Registries::parse(
            Some("*=mirror.local:5000\ndocker.io=mirror.local:5000/hub"),
            None,
        )
        .unwrap();
        assert_eq!(
            all.mirrored("quay.io/o/i:v1"),
            "mirror.local:5000/quay.io/o/i:v1"
        );
        assert_eq!(
            all.mirrored("alpine"),
            "mirror.local:5000/hub/library/alpine"
        );
        assert_eq!(Registries::default().mirrored("alpine"), "alpine");
    }

    #[test]
    fn a_mirror_that_cannot_be_read_stops_the_engine() {
        for (bad, why) in [
            ("docker.io", "REGISTRY=MIRROR"),
            ("docker.io=", "names no mirror"),
            ("docker.io=https://m", "names no mirror"),
            ("docker.io=u@m", "names no mirror"),
            ("=m", "names no registry"),
            ("docker.io=a,index.docker.io=b", "named twice"),
        ] {
            let err = Registries::parse(Some(bad), None).unwrap_err().to_string();
            assert!(err.contains(why), "{bad}: {err}");
        }
    }

    #[test]
    fn an_auth_file_gives_each_registry_its_credential() {
        let file = r#"{"auths": {
            "https://index.docker.io/v1/": {"auth": "dXNlcjpwYTpzcw=="},
            "ghcr.io": {"username": "bot", "password": "ghp_x"},
            "registry.example:5000": {"identitytoken": "tok"}
        }}"#;
        let r = Registries::parse(None, Some(file)).unwrap();
        let hub = r.credentials("alpine:3").unwrap();
        assert_eq!(
            (
                hub.username.as_deref(),
                hub.password.as_deref(),
                hub.serveraddress.as_deref()
            ),
            (
                Some("user"),
                Some("pa:ss"),
                Some("https://index.docker.io/v1/")
            )
        );
        let ghcr = r.credentials("ghcr.io/o/i").unwrap();
        assert_eq!(
            (ghcr.username.as_deref(), ghcr.password.as_deref()),
            (Some("bot"), Some("ghp_x"))
        );
        let token = r.credentials("registry.example:5000/x").unwrap();
        assert_eq!(token.identitytoken.as_deref(), Some("tok"));
        assert!(r.credentials("quay.io/o/i").is_none());
        assert!(
            !format!("{r:?}").contains("ghp_x"),
            "a password is not printed"
        );
    }

    #[test]
    fn an_auth_file_the_engine_cannot_use_stops_it() {
        for (bad, why) in [
            ("not json", "not a Docker config.json"),
            (r#"{"credsStore": "desktop"}"#, "credential helper"),
            (
                r#"{"credHelpers": {"gcr.io": "gcloud"}}"#,
                "credential helper",
            ),
            (r#"{"auths": []}"#, "not an object"),
            (r#"{"auths": {"r.io": {"auth": "!!"}}}"#, "not base64"),
            (
                r#"{"auths": {"r.io": {"auth": "bm9jb2xvbg=="}}}"#,
                "USER:PASSWORD",
            ),
            (r#"{"auths": {"r.io": {"username": "u"}}}"#, "no password"),
        ] {
            let err = Registries::parse(None, Some(bad)).unwrap_err().to_string();
            assert!(err.contains(why), "{bad}: {err}");
        }
        assert!(Registries::parse(None, Some("{}")).unwrap().is_empty());
    }

    #[test]
    fn base64_decodes_with_and_without_padding() {
        assert_eq!(decode_base64("aGk=").unwrap(), b"hi");
        assert_eq!(decode_base64("aGk").unwrap(), b"hi");
        assert_eq!(decode_base64("dXNlcjpwYXNz").unwrap(), b"user:pass");
        assert!(decode_base64("a").is_none());
        assert!(decode_base64("a-b_").is_none());
    }

    #[test]
    fn a_tag_is_pinned_to_the_digest_it_was_pulled_at() {
        let digests = vec![
            "docker.io/acme/other@sha256:00".to_string(),
            "alpine@sha256:11".to_string(),
        ];
        assert_eq!(
            pinned("alpine:3.20", &digests).as_deref(),
            Some("alpine@sha256:11")
        );
        assert_eq!(
            pinned("docker.io/library/alpine:3.20", &digests).as_deref(),
            Some("docker.io/library/alpine@sha256:11"),
            "Docker writes an official image's digest without its registry"
        );
        let podman = vec!["docker.io/library/alpine@sha256:22".to_string()];
        assert_eq!(
            pinned("alpine", &podman).as_deref(),
            Some("alpine@sha256:22")
        );
        let mirrored = vec!["mirror:5000/hub/library/alpine@sha256:33".to_string()];
        assert_eq!(
            pinned("mirror:5000/hub/library/alpine:3", &mirrored).as_deref(),
            Some("mirror:5000/hub/library/alpine@sha256:33")
        );
        assert_eq!(pinned("localhost:5000/x:1", &[]), None, "never pulled");
        assert_eq!(pinned("alpine@sha256:11", &digests), None, "pinned already");
        assert_eq!(pinned("busybox", &digests), None, "another repository's");
    }
}

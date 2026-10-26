//! Where a task that asks for an identity asks dagron-api's issuer for
//! tokens: `DAGRON_OIDC_REQUEST_URL`, read once at start.
//!
//! The task sends its request credential (`DAGRON_OIDC_REQUEST_TOKEN`) to
//! that URL as a bearer token, and with it a cloud's tokens are had for the
//! task's audiences while it runs. So the URL is `https://`. It is the task
//! that asks, from its own container or pod, so no address is "this host's"
//! to it: a plain `http://` URL crosses a network whatever host it names.
//! An operator who has a closed network the credential may cross in the
//! clear (a test rig's) says so, `DAGRON_OIDC_REQUEST_INSECURE=allow`, and
//! the engine says it at start.

use anyhow::{anyhow, bail, Result};

pub const ENV_URL: &str = "DAGRON_OIDC_REQUEST_URL";
pub const ENV_INSECURE: &str = "DAGRON_OIDC_REQUEST_INSECURE";

/// The request URL, from the two settings as the environment has them, as
/// a URL parser writes it (its scheme and host in lowercase): `None` where
/// none is set, or why the engine does not start.
pub fn from_settings(url: Option<&str>, insecure: Option<&str>) -> Result<Option<String>> {
    task_url(
        ENV_URL,
        ENV_INSECURE,
        "a cloud's tokens are had",
        url,
        insecure,
    )
}

/// A URL a task sends a credential of its own to, from the two settings
/// named `env_url` and `env_insecure` as the environment has them: what
/// [`from_settings`] holds the request URL to, for any such URL, where with
/// the credential `what` (for the message that refuses plain http).
pub fn task_url(
    env_url: &str,
    env_insecure: &str,
    what: &str,
    url: Option<&str>,
    insecure: Option<&str>,
) -> Result<Option<String>> {
    let insecure = match insecure.map(str::trim) {
        None | Some("") => false,
        Some("allow") => true,
        Some(other) => bail!("{env_insecure}={other:?} is allow, or unset"),
    };
    let Some(url) = url.map(str::trim).filter(|u| !u.is_empty()) else {
        return Ok(None);
    };
    let parsed = reqwest::Url::parse(url).map_err(|e| anyhow!("{env_url}={url:?}: {e}"))?;
    if parsed.host_str().is_none_or(str::is_empty) {
        bail!("{env_url}={url:?} names no host");
    }
    if !parsed.username().is_empty() || parsed.password().is_some() {
        bail!("{env_url}={url:?} carries a user: a task's credential is all it sends");
    }
    match parsed.scheme() {
        "https" => {}
        "http" if insecure => {}
        "http" => bail!(
            "{env_url}={url:?} is plain http: a task sends a credential of its own there, from \
             its own container, across the network, and with it {what}. Give it an https:// \
             URL, or, on a closed network where that is accepted, set {env_insecure}=allow"
        ),
        other => bail!("{env_url}={url:?} is {other}://, not https://"),
    }
    Ok(Some(parsed.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_request_url_is_https_unless_the_operator_allows_plain_http() {
        assert_eq!(from_settings(None, None).unwrap(), None);
        assert_eq!(from_settings(Some("  "), None).unwrap(), None);
        assert_eq!(
            from_settings(Some("https://api.example/oidc/token"), None).unwrap(),
            Some("https://api.example/oidc/token".to_string())
        );
        let refused = from_settings(Some("http://oidc.ws.svc:8088/oidc/token"), None)
            .unwrap_err()
            .to_string();
        assert!(refused.contains("is plain http"), "{refused}");
        assert!(
            refused.contains("DAGRON_OIDC_REQUEST_INSECURE=allow"),
            "{refused}"
        );
        // Not even to this host: it is the task that asks, from its own.
        assert!(from_settings(Some("http://127.0.0.1:8088/oidc/token"), None).is_err());
        assert_eq!(
            from_settings(Some("http://oidc.ws.svc:8088/oidc/token"), Some("allow")).unwrap(),
            Some("http://oidc.ws.svc:8088/oidc/token".to_string())
        );
        // A scheme in any case is the scheme, and is written in lowercase:
        // the engine tells plain http by it.
        assert!(from_settings(Some("HTTP://oidc.ws.svc:8088/oidc/token"), None).is_err());
        assert_eq!(
            from_settings(Some("HTTP://Oidc.WS.svc:8088/oidc/token"), Some("allow")).unwrap(),
            Some("http://oidc.ws.svc:8088/oidc/token".to_string())
        );
        assert_eq!(
            from_settings(Some("HTTPS://api.example/oidc/token"), None).unwrap(),
            Some("https://api.example/oidc/token".to_string())
        );
        for (url, insecure, why) in [
            ("ftp://x/oidc", None, "is ftp://"),
            ("not a url", None, "DAGRON_OIDC_REQUEST_URL"),
            ("https://u:p@x/oidc", None, "carries a user"),
            ("https://x/oidc", Some("yes"), "is allow, or unset"),
        ] {
            let err = from_settings(Some(url), insecure).unwrap_err().to_string();
            assert!(err.contains(why), "{url} {insecure:?}: {err}");
        }
    }
}

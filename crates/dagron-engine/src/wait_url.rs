//! Network policy for `wait.url` HTTP sensors.
//!
//! A parked `wait.url` task is polled *by the scheduler*, not by the executor.
//! On `EXECUTOR=local` / `docker` that distinction is academic — the two share a
//! network position. Under `EXECUTOR=kubernetes` with a differentiated
//! NetworkPolicy it is not: a workflow author who cannot reach the cluster's
//! internal services from a task pod can still name one in `wait.url` and learn
//! whether it answers, because the scheduler asks on their behalf.
//!
//! That is a confused-deputy escalation only for operators who run untrusted
//! workflow specs. For everyone else the primary use of `wait.url` *is* an
//! in-cluster address (`http://svc.default.svc/ready`), so blocking private
//! ranges by default would break the feature for the majority to harden it for
//! a minority. The policy is therefore opt-in, via `WAIT_URL_DENY_PRIVATE=1`.
//!
//! When enabled, two things are denied:
//!
//! 1. A URL whose host is an **IP literal** in a private / loopback /
//!    link-local / CGNAT / multicast range. hyper never consults a resolver for
//!    a literal, so this is checked before the request is issued.
//! 2. A **hostname** that resolves to any such address. This is enforced inside
//!    the resolver the client actually connects with, not as a pre-flight
//!    check, so there is no window between "checked" and "connected" for a DNS
//!    rebind to slip through: the addresses reqwest dials are exactly the ones
//!    that survived the filter.
//!
//! Redirect following is disabled unconditionally (see the client builder in
//! `lib.rs`) — a 3xx target is invisible to both checks above, so the sensor
//! reads a redirect as "not ready" instead of chasing it.

use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

/// Is `WAIT_URL_DENY_PRIVATE` set to an affirmative value? Off unless asked
/// for: the default has to keep `http://svc.default.svc/ready` working.
pub(crate) fn deny_private_enabled() -> bool {
    std::env::var("WAIT_URL_DENY_PRIVATE")
        .map(|v| matches!(v.trim().to_ascii_lowercase().as_str(), "1" | "true" | "on" | "yes"))
        .unwrap_or(false)
}

/// Addresses a `wait.url` sensor must not reach under the deny-private policy:
/// anything that is not a globally-routable unicast destination.
///
/// Deliberately broader than "RFC 1918": the escalation being closed is
/// "the scheduler can reach hosts the task pod cannot", and loopback, the
/// link-local block that carries cloud instance metadata (169.254.169.254),
/// and the CGNAT range all qualify. Only the stable inherent predicates are
/// used; the ranges whose `std` accessors are still unstable
/// (`is_shared`, `is_unique_local`, `is_unicast_link_local`) are spelled out.
pub(crate) fn is_blocked_ip(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(v4) => is_blocked_v4(v4),
        // An IPv4-mapped address (`::ffff:127.0.0.1`) reaches an IPv4
        // destination, so it has to be judged by the IPv4 rules — checking only
        // the v6 predicates would wave loopback straight through.
        IpAddr::V6(v6) => match v6.to_ipv4_mapped() {
            Some(v4) => is_blocked_v4(v4),
            None => is_blocked_v6(v6),
        },
    }
}

fn is_blocked_v4(ip: Ipv4Addr) -> bool {
    let o = ip.octets();
    // 0.0.0.0/8 — "this network". `is_unspecified()` only covers 0.0.0.0
    // itself, but IANA marks the whole /8 non-global, and on Linux a connect to
    // 0.0.0.x lands on the local host.
    o[0] == 0
        || ip.is_private()
        || ip.is_loopback()
        || ip.is_link_local()
        || ip.is_multicast()
        || ip.is_broadcast()
        || ip.is_documentation()
        // 100.64.0.0/10 — carrier-grade NAT (`Ipv4Addr::is_shared`, unstable).
        || (o[0] == 100 && (64..128).contains(&o[1]))
        // 192.0.0.0/24 — IETF protocol assignments.
        || (o[0] == 192 && o[1] == 0 && o[2] == 0)
        // 198.18.0.0/15 — benchmarking (`is_benchmarking`, unstable).
        || (o[0] == 198 && (o[1] == 18 || o[1] == 19))
        // 240.0.0.0/4 — reserved.
        || o[0] >= 240
}

fn is_blocked_v6(ip: Ipv6Addr) -> bool {
    let s = ip.segments();
    ip.is_loopback()
        || ip.is_unspecified()
        || ip.is_multicast()
        // fc00::/7 — unique local (`is_unique_local`, unstable).
        || (s[0] & 0xfe00) == 0xfc00
        // fe80::/10 — link-local unicast (`is_unicast_link_local`, unstable).
        || (s[0] & 0xffc0) == 0xfe80
        // 2001:db8::/32 — documentation.
        || (s[0] == 0x2001 && s[1] == 0x0db8)
}

/// The host of an `http(s)://` URL as **reqwest itself** will see it, with IPv6
/// brackets stripped so the result parses as an `IpAddr`.
///
/// Uses `reqwest::Url` (a re-export of the `url` crate's WHATWG parser) rather
/// than splitting the string by hand, because the two disagree in exactly the
/// place that matters: `url` normalizes the legacy IPv4 forms — `2130706433`,
/// `0x7f000001`, `127.1` — to `127.0.0.1`, while a textual split hands back
/// something that fails `IpAddr::parse` and so reads as a *name*. Since hyper
/// then treats the normalized value as a literal and never calls the resolver,
/// a hand-parsed check would have let every one of those spellings walk past
/// the policy. Parsing with the same parser that decides the connection is the
/// only way the two can't drift apart.
fn host_of(url: &str) -> Option<String> {
    let parsed = reqwest::Url::parse(url).ok()?;
    let host = parsed.host_str()?;
    // A v6 host serializes bracketed (`[::1]`); `IpAddr` wants it bare.
    Some(
        host.strip_prefix('[')
            .and_then(|h| h.strip_suffix(']'))
            .unwrap_or(host)
            .to_owned(),
    )
}

/// Under the deny-private policy, does this URL name a blocked address
/// *literally*? Hostnames return `false` here and are judged by the resolver.
pub(crate) fn literal_host_blocked(url: &str) -> bool {
    host_of(url)
        .and_then(|h| h.parse::<IpAddr>().ok())
        .is_some_and(is_blocked_ip)
}

/// A `reqwest` resolver that drops every non-global address before the client
/// can dial it.
///
/// The filter lives here — rather than in a pre-flight `lookup_host` — so that
/// the set of addresses checked is the set of addresses connected to. A name
/// that resolves to both a public and a private address still connects, to the
/// public one only; a name with nothing left after filtering fails the poll,
/// which the sensor reads as "not ready" and re-parks.
pub(crate) struct PublicOnlyResolver;

impl reqwest::dns::Resolve for PublicOnlyResolver {
    fn resolve(&self, name: reqwest::dns::Name) -> reqwest::dns::Resolving {
        Box::pin(async move {
            // Port 0: hyper-util overwrites it with the URL's port after
            // resolution, so the resolver only has to supply addresses.
            let host = name.as_str().to_owned();
            let resolved = tokio::net::lookup_host((host.as_str(), 0)).await?;
            let allowed: Vec<std::net::SocketAddr> =
                resolved.filter(|sa| !is_blocked_ip(sa.ip())).collect();
            if allowed.is_empty() {
                return Err(format!(
                    "wait.url host '{host}' resolves only to non-global addresses \
                     (blocked by WAIT_URL_DENY_PRIVATE)"
                )
                .into());
            }
            Ok(Box::new(allowed.into_iter()) as reqwest::dns::Addrs)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ip(s: &str) -> IpAddr {
        s.parse().expect("test address parses")
    }

    #[test]
    fn blocks_the_ranges_the_policy_is_about() {
        for s in [
            "127.0.0.1",
            "10.1.2.3",
            "172.16.0.1",
            "192.168.1.1",
            "169.254.169.254", // cloud instance metadata — the headline case
            "100.64.0.1",      // CGNAT
            "0.0.0.0",
            "0.0.0.1", // the rest of 0.0.0.0/8, not just the unspecified address
            "255.255.255.255",
            "::1",
            "fe80::1",
            "fd00::1",
            "::ffff:127.0.0.1", // v4-mapped loopback must not slip past
            "::ffff:10.0.0.1",
        ] {
            assert!(is_blocked_ip(ip(s)), "{s} must be blocked");
        }
    }

    #[test]
    fn allows_globally_routable_addresses() {
        for s in ["93.184.216.34", "8.8.8.8", "172.32.0.1", "2606:4700::1111"] {
            assert!(!is_blocked_ip(ip(s)), "{s} must be allowed");
        }
    }

    #[test]
    fn extracts_the_host_from_realistic_urls() {
        assert_eq!(host_of("http://example.com/ready").as_deref(), Some("example.com"));
        assert_eq!(host_of("https://example.com:8443/x?y=1").as_deref(), Some("example.com"));
        assert_eq!(host_of("http://user:pa%40ss@example.com/").as_deref(), Some("example.com"));
        assert_eq!(host_of("http://[::1]:8080/ready").as_deref(), Some("::1"));
        assert_eq!(host_of("http://127.0.0.1").as_deref(), Some("127.0.0.1"));
        assert_eq!(host_of("not a url").as_deref(), None);
    }

    /// The reason this uses reqwest's parser: every one of these spellings is
    /// `127.0.0.1` to the URL parser (and therefore to hyper, which then skips
    /// the resolver), but none of them parses as an `IpAddr` verbatim. A
    /// textual host split would classify them as names and wave them through.
    #[test]
    fn normalizes_legacy_ipv4_spellings_before_judging_them() {
        for u in [
            "http://2130706433/ready",   // decimal
            "http://0x7f000001/ready",   // hex
            "http://127.1/ready",        // short form
            "http://0177.0.0.1/ready",   // octal
        ] {
            assert_eq!(host_of(u).as_deref(), Some("127.0.0.1"), "{u} normalizes");
            assert!(literal_host_blocked(u), "{u} must be blocked");
        }
    }

    #[test]
    fn literal_check_catches_ip_urls_and_defers_on_names() {
        assert!(literal_host_blocked("http://127.0.0.1:9000/ready"));
        assert!(literal_host_blocked("http://169.254.169.254/latest/meta-data/"));
        assert!(literal_host_blocked("http://[::1]/ready"));
        assert!(!literal_host_blocked("https://example.com/ready"));
        // A hostname is not decided here — the resolver sees it, even when the
        // name is one that will obviously resolve to loopback.
        assert!(!literal_host_blocked("http://localhost:8080/ready"));
        assert!(!literal_host_blocked("http://svc.default.svc/ready"));
    }

    // `localhost` is the case the literal check deliberately defers on: it is a
    // name, so only the resolver can catch it. Resolution here is served from
    // `/etc/hosts`, so this does not need the network.
    #[tokio::test]
    async fn resolver_rejects_a_name_that_only_resolves_to_loopback() {
        use reqwest::dns::Resolve;
        let name: reqwest::dns::Name = "localhost".parse().expect("name parses");
        let err = PublicOnlyResolver
            .resolve(name)
            .await
            .err()
            .expect("localhost must not survive the filter");
        assert!(
            err.to_string().contains("non-global"),
            "unexpected resolver error: {err}"
        );
    }
}

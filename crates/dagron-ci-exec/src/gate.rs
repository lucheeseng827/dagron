//! `dagron-ci-exec wait-no-network SECS`: hold a pod until its network is
//! seen to be gone.
//!
//! On Kubernetes a task that asked for no network (`isolation: { network:
//! none }`) runs in a pod labelled for a NetworkPolicy that allows it
//! nothing. Whether that policy holds the pod is the cluster's CNI's doing,
//! and it is not every CNI's from a pod's first instant: one was seen to
//! leave a labelled pod its network for a second or two after it started.
//! A task that began then would reach what it was told it could not.
//!
//! So the task's pod is given an init container that runs this, last. It
//! asks for a connection at the addresses every pod is given: the cluster's
//! API server, and the pod's name servers. It ends once none of them has
//! answered [`UNANSWERED`] times running. The task's container starts after
//! it, in a pod that has been seen to reach none of them. A pod that still
//! gets an answer after `SECS` fails, and the task with it: nothing on its
//! cluster took its network.
//!
//! **What an answer is:** a connection, and nothing else. A refusal is not
//! one. These ports are open wherever their servers are reachable, so a
//! refusal comes from what stands between, and a CNI that rejects a packet
//! it will not carry, rather than dropping it, looks exactly so. An attempt
//! that could not be made at all (no descriptor to make it with, no memory)
//! is neither: it says nothing of the network, and the wait fails saying so
//! rather than count it as silence.
//!
//! **Why more than one address.** Silence at one address is also what that
//! address being down looks like, and the API server being down is when a
//! CNI is slowest to act on a new pod: it learns of the pod from it. A name
//! server is another process on other machines. With both asked, a pod is
//! let through on silence only if both are silent at once.
//!
//! **`wait-allowlist SECS`** is the same wait for a pod held to an allowlist
//! (`isolation: { network: allow:<name> }`): a policy of the operator's that
//! lets it reach some addresses and no others. It asks the API server alone.
//! A name server is what an allowlist most often allows, so its answer says
//! nothing; and the API server is what an allowlist must not allow, so that
//! there is an address whose silence shows the policy has hold of the pod.
//! With one address asked, an outage of that one is all it takes to be let
//! through unheld: the weaker of the two waits, and said to be.
//!
//! **What this checks, and what it does not.** These addresses, as far as a
//! connection goes. A CNI applies a policy to a pod as a whole, so they
//! stand for the rest; that is not a proof of each of them, and a policy
//! that named these addresses alone would pass. The engine reads the
//! namespace's policies for that reason, as it starts and before each such
//! task. And silence is not told apart from an outage: a pod whose API
//! server and name servers were all down, on a cluster that had not yet
//! taken its network, would be let through.

use std::io;
use std::net::{IpAddr, SocketAddr, TcpStream};
use std::time::{Duration, Instant};

/// How long one connection is given to be made. One inside a cluster is
/// made in milliseconds, so a second with nothing is nothing.
pub const ATTEMPT: Duration = Duration::from_secs(1);

/// How many times in a row every address must go unanswered. More than one,
/// so that a packet lost on a working path is not taken for a path that is
/// gone.
pub const UNANSWERED: u32 = 3;

/// The wait between two rounds of asking.
pub const PAUSE: Duration = Duration::from_millis(200);

/// The port a name server answers on, by TCP as by UDP.
const NAME_SERVER_PORT: u16 = 53;

/// Where a pod's name servers are written.
pub const RESOLV_CONF: &str = "/etc/resolv.conf";

/// An address the gate asks, and what it is to the pod.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Place {
    pub addr: SocketAddr,
    pub what: &'static str,
}

impl std::fmt::Display for Place {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}, {}", self.addr, self.what)
    }
}

/// What asking came to, once: at one address, or at all of them together.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Reply {
    /// A connection was made. At all of them: at this one of them.
    Answered(String),
    /// None was made: refused, timed out, no way there, or turned back by a
    /// rule of the pod's own.
    Unanswered,
    /// The attempt could not be made, which says nothing of the network.
    NotAsked(String),
}

/// Why the wait ended without the network being gone.
#[derive(Debug, PartialEq, Eq)]
pub enum Stopped {
    /// Something answered, as late as this long after the wait began.
    Answered { who: String, after: Duration },
    /// It could not be asked.
    NotAsked(String),
}

/// Ask until nothing has answered [`UNANSWERED`] times running, or until
/// something still answers once `bound` has passed. `Ok` is how long it
/// took.
///
/// `ask` asks once, everywhere, and says what came of it; `elapsed` is the
/// time since the wait began; `pause` waits between two rounds. Given as
/// functions so that a test can be the network and the clock.
pub fn wait(
    mut ask: impl FnMut() -> Reply,
    mut elapsed: impl FnMut() -> Duration,
    mut pause: impl FnMut(Duration),
    bound: Duration,
) -> Result<Duration, Stopped> {
    let mut unanswered = 0;
    loop {
        match ask() {
            Reply::Answered(who) => {
                unanswered = 0;
                let after = elapsed();
                if after >= bound {
                    return Err(Stopped::Answered { who, after });
                }
            }
            Reply::Unanswered => {
                unanswered += 1;
                if unanswered >= UNANSWERED {
                    return Ok(elapsed());
                }
            }
            Reply::NotAsked(why) => return Err(Stopped::NotAsked(why)),
        }
        pause(PAUSE);
    }
}

/// What one attempt at a connection came to. A connection is an answer. The
/// errors a path gives are none: a refusal, a timeout, no route to the host
/// or its network, a reset, and the local refusal a pod's own rules give
/// (`EPERM` from a filter on its socket). Any other error is the attempt's
/// own, and is not taken for silence. `EADDRNOTAVAIL` is one of those: it
/// can be this pod with no port left to connect from, which says nothing of
/// where the connection was going.
pub fn reply(attempt: &io::Result<TcpStream>, place: &Place) -> Reply {
    use io::ErrorKind::*;
    match attempt {
        Ok(_) => Reply::Answered(place.to_string()),
        Err(e) => match e.kind() {
            ConnectionRefused | ConnectionReset | ConnectionAborted | TimedOut
            | HostUnreachable | NetworkUnreachable | NetworkDown | PermissionDenied => {
                Reply::Unanswered
            }
            _ => Reply::NotAsked(format!("{place}: {e}")),
        },
    }
}

/// Ask every one of `places` at once, each given [`ATTEMPT`]: an answer at
/// any is an answer, and with none, an attempt that could not be made is
/// what is said.
pub fn ask(places: &[Place]) -> Reply {
    let replies: Vec<Reply> = std::thread::scope(|scope| {
        let asking: Vec<_> = places
            .iter()
            .map(|place| {
                scope.spawn(move || reply(&TcpStream::connect_timeout(&place.addr, ATTEMPT), place))
            })
            .collect();
        asking
            .into_iter()
            .map(|handle| {
                handle
                    .join()
                    .unwrap_or_else(|_| Reply::NotAsked("an attempt did not end".into()))
            })
            .collect()
    });
    all_of(replies)
}

/// What several replies come to together: see [`ask`].
pub fn all_of(replies: Vec<Reply>) -> Reply {
    let mut not_asked = None;
    for reply in replies {
        match reply {
            Reply::Answered(_) => return reply,
            Reply::NotAsked(_) if not_asked.is_none() => not_asked = Some(reply),
            _ => {}
        }
    }
    not_asked.unwrap_or(Reply::Unanswered)
}

/// The API server's address, as a pod's environment gives it:
/// `KUBERNETES_SERVICE_HOST` and `KUBERNETES_SERVICE_PORT`.
pub fn api_server(host: Option<&str>, port: Option<&str>) -> Result<SocketAddr, String> {
    let (Some(host), Some(port)) = (host, port) else {
        return Err(
            "KUBERNETES_SERVICE_HOST and KUBERNETES_SERVICE_PORT are not both set: this is \
             not a pod's container"
                .into(),
        );
    };
    let ip: IpAddr = host
        .trim()
        .parse()
        .map_err(|_| format!("KUBERNETES_SERVICE_HOST is {host:?}, which is no address"))?;
    let port: u16 = port
        .trim()
        .parse()
        .map_err(|_| format!("KUBERNETES_SERVICE_PORT is {port:?}, which is no port"))?;
    Ok(SocketAddr::new(ip, port))
}

/// The name servers a `resolv.conf` names, as addresses to ask by TCP. One
/// on the pod's own loopback is left out: that is the pod, which a task with
/// no network still has. A line that names no address is left out too.
pub fn name_servers(resolv_conf: &str) -> Vec<SocketAddr> {
    resolv_conf
        .lines()
        .filter_map(|line| {
            let mut words = line.split_whitespace();
            (words.next()? == "nameserver").then_some(())?;
            // An IPv6 address may carry a zone (`fe80::1%eth0`).
            let ip: IpAddr = words.next()?.split('%').next()?.parse().ok()?;
            (!ip.is_loopback()).then(|| SocketAddr::new(ip, NAME_SERVER_PORT))
        })
        .collect()
}

/// A pod's `resolv.conf` as it was read, for [`run`]: its text, or none
/// where the pod has no such file, which is a pod with no name server to
/// ask. Any other failure to read it is `Err`: the file is there and names
/// what should be asked, and to go on without it would be to ask less than
/// a pod is held by, and say it was held.
pub fn resolv_conf(read: io::Result<String>) -> Result<Option<String>, String> {
    match read {
        Ok(text) => Ok(Some(text)),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(format!(
            "whether this pod has a network could not be asked: {RESOLV_CONF}, which names \
             its name servers, could not be read: {e}"
        )),
    }
}

/// `SECS` as the command line gives it: a whole number of seconds, at
/// least one.
pub fn bound(secs: &str) -> Result<Duration, String> {
    match secs.trim().parse::<u64>() {
        Ok(secs) if secs > 0 => Ok(Duration::from_secs(secs)),
        _ => Err(format!(
            "SECS is {secs:?}: a whole number of seconds, at least 1"
        )),
    }
}

/// Where a pod is asked: the API server its environment names, and the name
/// servers `resolv_conf` names.
pub fn places(
    host: Option<&str>,
    port: Option<&str>,
    resolv_conf: Option<&str>,
) -> Result<Vec<Place>, String> {
    let mut places = vec![Place {
        addr: api_server(host, port)?,
        what: "the cluster's API server",
    }];
    places.extend(
        name_servers(resolv_conf.unwrap_or_default())
            .into_iter()
            .map(|addr| Place {
                addr,
                what: "its name server",
            }),
    );
    Ok(places)
}

/// What a pod is waited on to be held to, which is what is said of it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Held {
    /// No network: nothing it is asked at may answer.
    NoNetwork,
    /// An allowlist: the address it must not allow may not answer.
    Allowlist,
}

/// Run the wait against the API server this container was given and the
/// name servers `resolv_conf` names, and say how it went. `Err` is what to
/// say as it fails.
pub fn run(
    secs: &str,
    host: Option<&str>,
    port: Option<&str>,
    resolv_conf: Option<&str>,
) -> Result<String, String> {
    let bound = bound(secs)?;
    hold(&places(host, port, resolv_conf)?, bound, Held::NoNetwork)
}

/// Run the wait for a pod held to an allowlist: against the API server this
/// container was given, and nowhere else. See the module's account of why.
pub fn run_allowlist(secs: &str, host: Option<&str>, port: Option<&str>) -> Result<String, String> {
    let bound = bound(secs)?;
    hold(&places(host, port, None)?, bound, Held::Allowlist)
}

/// Wait at `places` for as long as `bound`, and say how it went.
pub fn hold(places: &[Place], bound: Duration, held: Held) -> Result<String, String> {
    let began = Instant::now();
    let waited = wait(
        || ask(places),
        || began.elapsed(),
        std::thread::sleep,
        bound,
    );
    let asked = || {
        places
            .iter()
            .map(|place| format!("{} ({})", place.addr, place.what))
            .collect::<Vec<_>>()
            .join(" or ")
    };
    match (held, waited) {
        (Held::NoNetwork, Ok(took)) => Ok(format!(
            "this pod's network is gone: nothing answered at {}, {UNANSWERED} times running \
             ({:.1} s after the wait began)",
            asked(),
            took.as_secs_f64()
        )),
        (Held::Allowlist, Ok(took)) => Ok(format!(
            "this pod is held: nothing answered at {}, {UNANSWERED} times running ({:.1} s \
             after the wait began). What its allowlist lets it reach was not asked",
            asked(),
            took.as_secs_f64()
        )),
        (Held::NoNetwork, Err(Stopped::Answered { who, after })) => Err(format!(
            "this pod still has a network: {who}, answered {:.0} s after the wait began",
            after.as_secs_f64()
        )),
        (Held::Allowlist, Err(Stopped::Answered { who, after })) => Err(format!(
            "this pod still reaches {who}, which answered {:.0} s after the wait began",
            after.as_secs_f64()
        )),
        (Held::NoNetwork, Err(Stopped::NotAsked(why))) => Err(format!(
            "whether this pod has a network could not be asked: {why}"
        )),
        (Held::Allowlist, Err(Stopped::NotAsked(why))) => Err(format!(
            "whether this pod is held could not be asked: {why}"
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::Cell;
    use std::net::TcpListener;

    fn somewhere() -> Place {
        Place {
            addr: "10.43.0.1:443".parse().unwrap(),
            what: "the cluster's API server",
        }
    }

    /// A network that answers as `answers` says, round by round, and then
    /// goes on as its last entry; and a clock that moves a second for each
    /// round asked.
    fn waited(answers: &[bool], bound_secs: u64) -> (Result<Duration, Stopped>, u32) {
        let rounds = Cell::new(0u32);
        let pauses = Cell::new(0u32);
        let got = wait(
            || {
                let n = rounds.get() as usize;
                rounds.set(rounds.get() + 1);
                if *answers.get(n).or(answers.last()).unwrap() {
                    Reply::Answered(somewhere().to_string())
                } else {
                    Reply::Unanswered
                }
            },
            || Duration::from_secs(u64::from(rounds.get())),
            |pause| {
                assert_eq!(pause, PAUSE);
                pauses.set(pauses.get() + 1);
            },
            Duration::from_secs(bound_secs),
        );
        (got, rounds.get())
    }

    /// A pod whose network is gone from the first is let go after the
    /// rounds it takes to be sure, and no more.
    #[test]
    fn a_network_that_is_gone_is_seen_gone_after_three_rounds() {
        let (got, rounds) = waited(&[false], 30);
        assert_eq!(rounds, UNANSWERED);
        assert_eq!(got, Ok(Duration::from_secs(3)));
    }

    /// One that still answers for a while is waited for: what a CNI that
    /// takes a pod's network a moment after it starts looks like.
    #[test]
    fn a_network_taken_late_is_waited_for() {
        let (got, rounds) = waited(&[true, true, true, true, false], 30);
        assert_eq!(rounds, 4 + UNANSWERED);
        assert_eq!(got, Ok(Duration::from_secs(7)));
    }

    /// An answer in the middle of a run of none starts the count again: a
    /// lost packet on a working path is not a path that is gone.
    #[test]
    fn an_answer_starts_the_count_again() {
        let (got, rounds) = waited(&[false, false, true, false, false, true, false], 30);
        assert_eq!(rounds, 6 + UNANSWERED);
        assert!(got.is_ok());
    }

    /// One that answers to the end of the bound fails, and says who last
    /// answered and when. It is asked once past the bound, not cut off
    /// before it.
    #[test]
    fn a_network_that_stays_fails_once_the_bound_has_passed() {
        let (got, rounds) = waited(&[true], 5);
        assert_eq!(rounds, 5);
        assert_eq!(
            got,
            Err(Stopped::Answered {
                who: somewhere().to_string(),
                after: Duration::from_secs(5)
            })
        );
    }

    /// A run of none that began before the bound is let finish: the network
    /// went in time.
    #[test]
    fn a_network_that_goes_as_the_bound_passes_is_gone() {
        let (got, _) = waited(&[true, true, true, true, false], 5);
        assert!(got.is_ok(), "{got:?}");
    }

    /// A round that could not be asked ends the wait at once, as itself: it
    /// is not silence, and is not counted towards any.
    #[test]
    fn a_round_that_could_not_be_asked_ends_the_wait() {
        let rounds = Cell::new(0);
        let got = wait(
            || {
                rounds.set(rounds.get() + 1);
                match rounds.get() {
                    1 | 2 => Reply::Unanswered,
                    _ => Reply::NotAsked("no descriptor".into()),
                }
            },
            || Duration::ZERO,
            |_| {},
            Duration::from_secs(30),
        );
        assert_eq!(got, Err(Stopped::NotAsked("no descriptor".into())));
        assert_eq!(rounds.get(), 3);
    }

    /// A connection is an answer. What a path says when it will not carry
    /// one is none. Anything else is the attempt's own failure, and is not
    /// taken for a path that is gone.
    #[test]
    fn a_connection_is_an_answer_a_path_s_refusal_is_none_and_the_rest_is_neither() {
        use io::ErrorKind::*;
        let place = somewhere();
        for none in [
            ConnectionRefused,
            ConnectionReset,
            ConnectionAborted,
            TimedOut,
            HostUnreachable,
            NetworkUnreachable,
            NetworkDown,
            PermissionDenied,
        ] {
            assert_eq!(
                reply(&Err(io::Error::from(none)), &place),
                Reply::Unanswered,
                "{none:?}"
            );
        }
        for own in [
            io::Error::other("Too many open files"),
            io::Error::from(OutOfMemory),
            // No port left to connect from: this pod's, and not the path's.
            io::Error::from(AddrNotAvailable),
            io::Error::from(InvalidInput),
            io::Error::from(Unsupported),
        ] {
            let said = format!("{place}: {own}");
            assert_eq!(reply(&Err(own), &place), Reply::NotAsked(said));
        }

        // And against real ports: one that is open, and then is not.
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let open = Place {
            addr: listener.local_addr().unwrap(),
            what: "a port of this test's",
        };
        assert_eq!(ask(&[open.clone()]), Reply::Answered(open.to_string()));
        drop(listener);
        assert_eq!(ask(&[open]), Reply::Unanswered);
    }

    /// Asked together, an answer at any address is an answer, whatever the
    /// others said; with none, an attempt that could not be made is said
    /// before silence is.
    #[test]
    fn an_answer_anywhere_is_an_answer() {
        let answered = || Reply::Answered("there".into());
        let not_asked = || Reply::NotAsked("why".into());
        assert_eq!(all_of(vec![]), Reply::Unanswered);
        assert_eq!(
            all_of(vec![Reply::Unanswered, Reply::Unanswered]),
            Reply::Unanswered
        );
        assert_eq!(all_of(vec![Reply::Unanswered, answered()]), answered());
        assert_eq!(all_of(vec![not_asked(), answered()]), answered());
        assert_eq!(all_of(vec![Reply::Unanswered, not_asked()]), not_asked());
        assert_eq!(
            all_of(vec![not_asked(), Reply::NotAsked("another".into())]),
            not_asked(),
            "the first of them"
        );

        // One closed port and one open one: the open one answers for both.
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let closed = TcpListener::bind("127.0.0.1:0").unwrap();
        let (open, shut) = (listener.local_addr().unwrap(), closed.local_addr().unwrap());
        drop(closed);
        let place = |addr, what| Place { addr, what };
        let both = [
            place(shut, "the cluster's API server"),
            place(open, "its name server"),
        ];
        assert_eq!(
            ask(&both),
            Reply::Answered(format!("{open}, its name server"))
        );
        drop(listener);
        assert_eq!(ask(&both), Reply::Unanswered);
    }

    #[test]
    fn the_api_server_is_where_a_pod_s_environment_says() {
        assert_eq!(
            api_server(Some("10.43.0.1"), Some("443")),
            Ok("10.43.0.1:443".parse().unwrap())
        );
        assert_eq!(
            api_server(Some("fd00::1"), Some("6443")),
            Ok("[fd00::1]:6443".parse().unwrap())
        );
        assert!(api_server(None, Some("443"))
            .unwrap_err()
            .contains("not a pod's container"));
        assert!(api_server(Some("10.43.0.1"), None)
            .unwrap_err()
            .contains("not a pod's container"));
        assert!(api_server(Some("kubernetes"), Some("443"))
            .unwrap_err()
            .contains("no address"));
        assert!(api_server(Some("10.43.0.1"), Some("https"))
            .unwrap_err()
            .contains("no port"));
    }

    /// A pod's name servers are read from its resolv.conf: each address
    /// named, of either family, and none of the rest of the file. One on the
    /// pod's own loopback is the pod, and is not asked.
    #[test]
    fn the_name_servers_are_the_ones_resolv_conf_names() {
        let conf = "search default.svc.cluster.local svc.cluster.local cluster.local\n\
                    nameserver 10.43.0.10\n\
                    # nameserver 192.0.2.1\n\
                    nameserver\tfd00::a\n\
                    nameserver fe80::1%eth0\n\
                    nameserver 127.0.0.53\n\
                    nameserver ::1\n\
                    nameserver not-an-address\n\
                    nameserver\n\
                    options ndots:5\n";
        let named: Vec<String> = name_servers(conf).iter().map(|a| a.to_string()).collect();
        assert_eq!(named, ["10.43.0.10:53", "[fd00::a]:53", "[fe80::1]:53"]);
        assert!(name_servers("").is_empty());
    }

    /// A pod with no resolv.conf has no name server to ask. One whose file is
    /// there and cannot be read is not taken for that: the wait fails.
    #[test]
    fn a_resolv_conf_that_cannot_be_read_is_not_a_pod_with_no_name_server() {
        assert_eq!(
            resolv_conf(Ok("nameserver 10.43.0.10\n".into())),
            Ok(Some("nameserver 10.43.0.10\n".into()))
        );
        assert_eq!(
            resolv_conf(Err(io::Error::from(io::ErrorKind::NotFound))),
            Ok(None)
        );
        for kind in [io::ErrorKind::PermissionDenied, io::ErrorKind::InvalidData] {
            let why = resolv_conf(Err(io::Error::from(kind))).unwrap_err();
            assert!(
                why.contains("could not be asked") && why.contains("/etc/resolv.conf"),
                "{kind:?}: {why}"
            );
        }
    }

    #[test]
    fn the_bound_is_a_whole_number_of_seconds() {
        assert_eq!(bound(" 30 "), Ok(Duration::from_secs(30)));
        for wrong in ["", "0", "-1", "1.5", "soon"] {
            assert!(bound(wrong).is_err(), "{wrong:?}");
        }
    }

    /// The whole of it, outside a pod: it says so, and waits for nothing.
    #[test]
    fn outside_a_pod_it_says_so() {
        let err = run("30", None, None, None).unwrap_err();
        assert!(err.contains("not a pod's container"), "{err}");
        assert!(run("0", Some("10.43.0.1"), Some("443"), None)
            .unwrap_err()
            .contains("SECS"));
    }

    /// And against real ports. An open API server is waited out, and so is
    /// an open name server beside an API server that does not answer: the
    /// pod has a network either way. With neither answering it is gone, and
    /// what is said names every address that was asked.
    #[test]
    fn against_real_ports_an_open_one_is_waited_out_and_closed_ones_are_gone() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let (host, port) = (addr.ip().to_string(), addr.port().to_string());
        let err = run("1", Some(&host), Some(&port), None).unwrap_err();
        assert!(
            err.contains("still has a network")
                && err.contains(&format!("{addr}, the cluster's API server")),
            "{err}"
        );
        drop(listener);
        let said = run("30", Some(&host), Some(&port), None).unwrap();
        assert!(
            said.contains("network is gone")
                && said.contains(&format!("{addr} (the cluster's API server)")),
            "{said}"
        );

        // A name server is asked on port 53, which a test cannot open. So
        // the wait is given places directly, as `run` makes them.
        let api = TcpListener::bind("127.0.0.1:0").unwrap();
        let dns = TcpListener::bind("127.0.0.1:0").unwrap();
        let places = [
            Place {
                addr: api.local_addr().unwrap(),
                what: "the cluster's API server",
            },
            Place {
                addr: dns.local_addr().unwrap(),
                what: "its name server",
            },
        ];
        drop(api);
        let err = hold(&places, Duration::from_secs(1), Held::NoNetwork).unwrap_err();
        assert!(
            err.contains("still has a network")
                && err.contains(&format!("{}, its name server", places[1].addr)),
            "an API server that is silent beside a name server that answers: {err}"
        );
        drop(dns);
        let said = hold(&places, Duration::from_secs(30), Held::NoNetwork).unwrap();
        assert!(
            said.contains(&format!(
                "{} (the cluster's API server) or {} (its name server)",
                places[0].addr, places[1].addr
            )),
            "{said}"
        );
    }

    /// The wait for a pod held to an allowlist asks its API server and
    /// nowhere else, and says what it saw in its own words: an API server
    /// that still answers is what such a pod "still reaches", and one that
    /// does not leaves it "held", with what its allowlist allows not asked.
    #[test]
    fn a_pod_held_to_an_allowlist_is_asked_at_its_api_server_alone() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let (host, port) = (addr.ip().to_string(), addr.port().to_string());
        let err = run_allowlist("1", Some(&host), Some(&port)).unwrap_err();
        assert!(
            err.contains(&format!(
                "this pod still reaches {addr}, the cluster's API server"
            )) && !err.contains("still has a network"),
            "{err}"
        );
        drop(listener);
        let said = run_allowlist("30", Some(&host), Some(&port)).unwrap();
        assert!(
            said.contains(&format!(
                "this pod is held: nothing answered at {addr} (the cluster's API server)"
            )) && said.contains("What its allowlist lets it reach was not asked")
                && !said.contains("name server"),
            "{said}"
        );
        assert!(run_allowlist("30", None, None)
            .unwrap_err()
            .contains("not a pod's container"));
        assert!(run_allowlist("soon", Some(&host), Some(&port))
            .unwrap_err()
            .contains("SECS"));
    }

    /// A pod is asked at its API server and at each name server its
    /// resolv.conf names, on the port a name server answers by TCP.
    #[test]
    fn a_pod_is_asked_at_its_api_server_and_its_name_servers() {
        let asked = |conf: Option<&str>| -> Vec<String> {
            places(Some("10.43.0.1"), Some("443"), conf)
                .unwrap()
                .iter()
                .map(Place::to_string)
                .collect()
        };
        assert_eq!(asked(None), ["10.43.0.1:443, the cluster's API server"]);
        assert_eq!(
            asked(Some("nameserver 10.43.0.10\nnameserver 127.0.0.1\n")),
            [
                "10.43.0.1:443, the cluster's API server",
                "10.43.0.10:53, its name server"
            ]
        );
        assert!(places(None, None, Some("nameserver 10.43.0.10\n")).is_err());
    }
}

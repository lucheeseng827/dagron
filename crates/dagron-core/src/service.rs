//! **Services**: containers a task has beside it for as long as it runs.
//!
//! A test wants a database, a data task wants a proxy to one. Started from
//! the task's own command, such a thing has to be in the task's image, is
//! the task's to wait for, and is the task's to stop. A service is a
//! container of its own instead, from an image of its own, which the
//! executor starts before the task, sees ready, and removes with it:
//!
//! | Executor   | A service |
//! |------------|-----------|
//! | Kubernetes | a sidecar container of the task's pod (an init container that restarts, which Kubernetes has from 1.29), with `ready` as its startup probe |
//! | Docker     | a container made for the attempt, whose network the task's container and the other services share |
//! | Local      | refused: a task there is a process on the engine's host |
//!
//! **Where a task finds one.** At the service's `name`, on the port the
//! image listens on (`pg:5432`). The task and its services share one
//! network, as a pod's containers do, so each is at the loopback address
//! and the name is one for it. Two services of a task cannot listen on one
//! port. The name is the task's alone. The port is not: what else is on
//! the pod's or the container's network reaches it at that address, as it
//! would reach a port the task itself listened on.
//!
//! **Ready.** `ready` is a command the executor runs in the service's own
//! container until it exits 0. The task starts once every service of it is
//! ready, in the order they are written. A service that stops before it is
//! ready, or is not ready within `ready_timeout_secs`, fails the attempt
//! with what the service said. One with no `ready` is ready once it has
//! started.
//!
//! **What a service is held to** is what its task is: the task's isolation
//! (its uid and its network among it), and the limits of a run that is not
//! trusted. A service's image and arguments are its author's, as the task's
//! are.
//!
//! What is not here: a secret in a service's environment. A service's
//! variables are plain values.

use anyhow::{bail, Result};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

use crate::dag::EnvVar;

/// The most services one task may have.
pub const MAX_SERVICES: usize = 8;

/// How long a service with a `ready` has to become ready when it names no
/// `ready_timeout_secs`.
pub const DEFAULT_READY_TIMEOUT_SECS: u64 = 60;

/// The longest `ready_timeout_secs` a service may name.
pub const MAX_READY_TIMEOUT_SECS: u64 = 600;

/// The longest name a service may have: it is a host name, and part of a
/// container's.
pub const MAX_NAME_LEN: usize = 40;

/// One of a task's `services:`.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ServiceSpec {
    /// The name the task reaches it by: lowercase letters, digits and `-`,
    /// beginning with a letter.
    pub name: String,
    /// The image it runs. May name the run's parameters.
    pub image: String,
    /// What the image's entrypoint is given in place of the image's own
    /// command (Docker's `CMD`, a pod's `args`). Unset: the image's own.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub args: Vec<String>,
    /// Its variables, as a task's are written. Plain values: a `value_from`
    /// is refused.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub env: Vec<EnvVar>,
    /// The command that says it is ready. Unset: ready once started.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ready: Option<Ready>,
    /// How long it has to become ready, in seconds
    /// ([`DEFAULT_READY_TIMEOUT_SECS`] when unset).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ready_timeout_secs: Option<u64>,
    /// What its container asks of a node and is held to, as a task's
    /// `resources` are written. Kubernetes only.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resources: Option<ServiceResources>,
    /// The task's Docker daemon ([`crate::docker_daemon`]), which the
    /// engine adds, and which no one writes: run privileged.
    #[serde(skip)]
    pub daemon: bool,
}

/// A service's `resources`: copied onto its container as written.
#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ServiceResources {
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub requests: BTreeMap<String, String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub limits: BTreeMap<String, String>,
}

/// A service's `ready`: a line for `sh -c`, or a program and its arguments
/// for an image that has no shell.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Ready {
    Shell(String),
    Exec(Vec<String>),
}

impl Ready {
    /// The command as an executor runs it in the service's container.
    pub fn argv(&self) -> Vec<String> {
        match self {
            Ready::Shell(line) => vec!["sh".to_string(), "-c".to_string(), line.clone()],
            Ready::Exec(argv) => argv.clone(),
        }
    }

    /// With each `{{ … }}` of it filled in by `fill`.
    fn filled(&self, fill: &impl Fn(&str) -> String) -> Ready {
        match self {
            Ready::Shell(line) => Ready::Shell(fill(line)),
            Ready::Exec(argv) => Ready::Exec(argv.iter().map(|a| fill(a)).collect()),
        }
    }
}

impl Serialize for Ready {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self {
            Ready::Shell(line) => serializer.serialize_str(line),
            Ready::Exec(argv) => argv.serialize(serializer),
        }
    }
}

impl<'de> Deserialize<'de> for Ready {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct Either;
        impl<'de> serde::de::Visitor<'de> for Either {
            type Value = Ready;

            fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                f.write_str(
                    "a command: one line for `sh -c`, or a list of a program and its arguments",
                )
            }

            fn visit_str<E: serde::de::Error>(self, line: &str) -> Result<Ready, E> {
                Ok(Ready::Shell(line.to_string()))
            }

            fn visit_seq<A: serde::de::SeqAccess<'de>>(
                self,
                mut seq: A,
            ) -> Result<Ready, A::Error> {
                let mut argv = Vec::new();
                while let Some(arg) = seq.next_element::<String>()? {
                    argv.push(arg);
                }
                Ok(Ready::Exec(argv))
            }
        }
        deserializer.deserialize_any(Either)
    }
}

/// A service's name: a host name's label, beginning with a letter.
pub fn check_name(name: &str) -> std::result::Result<(), String> {
    let mut chars = name.chars();
    let shaped = chars.next().is_some_and(|c| c.is_ascii_lowercase())
        && chars.all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-')
        && !name.ends_with('-')
        && name.len() <= MAX_NAME_LEN;
    if !shaped {
        return Err(format!(
            "{name:?} is not a service's name: lowercase letters, digits and '-', beginning \
             with a letter and not ending with '-', at most {MAX_NAME_LEN}"
        ));
    }
    if name == "localhost" {
        return Err("\"localhost\" is every container's name for itself".to_string());
    }
    Ok(())
}

/// An image a service may name: something written, with nothing in it a
/// registry would not take and no parameter left unfilled.
pub fn check_image(image: &str) -> std::result::Result<(), String> {
    if image.contains("{{") {
        return Err(format!("{image:?} names a parameter the run does not have"));
    }
    if image.is_empty()
        || image.len() > 512
        || image.chars().any(|c| c.is_whitespace() || c.is_control())
    {
        return Err(format!("{image:?} is not an image's name"));
    }
    Ok(())
}

/// A variable's name: a letter or `_`, then letters, digits or `_`.
pub(crate) fn is_variable(name: &str) -> bool {
    let mut chars = name.chars();
    chars
        .next()
        .is_some_and(|c| c.is_ascii_alphabetic() || c == '_')
        && chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
}

impl ServiceSpec {
    /// How long this service has to become ready.
    pub fn ready_timeout(&self) -> u64 {
        self.ready_timeout_secs
            .unwrap_or(DEFAULT_READY_TIMEOUT_SECS)
    }

    /// With each `{{ … }}` of its image, arguments, variables' values and
    /// `ready` filled in by `fill`. Its name is not a template: it is a
    /// host name the task's own text names.
    pub fn filled(&self, fill: impl Fn(&str) -> String) -> ServiceSpec {
        ServiceSpec {
            name: self.name.clone(),
            image: fill(&self.image),
            args: self.args.iter().map(|a| fill(a)).collect(),
            env: self
                .env
                .iter()
                .map(|e| EnvVar {
                    name: e.name.clone(),
                    value: fill(&e.value),
                    value_from: e.value_from.clone(),
                })
                .collect(),
            ready: self.ready.as_ref().map(|r| r.filled(&fill)),
            ready_timeout_secs: self.ready_timeout_secs,
            resources: self.resources.clone(),
            daemon: self.daemon,
        }
    }

    /// One service, on its own: its name, its image, its variables and what
    /// says it is ready.
    pub fn validate(&self) -> Result<()> {
        if let Err(why) = check_name(&self.name) {
            bail!("services: {why}");
        }
        let at = format!("service '{}'", self.name);
        if let Err(why) = check_image(&self.image) {
            bail!("{at}: image: {why}");
        }
        let mut seen = BTreeSet::new();
        for var in &self.env {
            if !is_variable(&var.name) {
                bail!("{at}: env: {:?} is not a variable's name", var.name);
            }
            if var.value_from.is_some() {
                bail!(
                    "{at}: env: {} has a `value_from`. A service's variables are plain values: \
                     no secret is given to a service",
                    var.name
                );
            }
            if !seen.insert(var.name.as_str()) {
                bail!("{at}: env: {} is set twice", var.name);
            }
        }
        match &self.ready {
            Some(Ready::Shell(line)) if line.trim().is_empty() => {
                bail!(
                    "{at}: ready: is empty. Leave it out for a service that is ready once started"
                )
            }
            Some(Ready::Exec(argv))
                if argv.first().is_none_or(|program| program.trim().is_empty()) =>
            {
                bail!("{at}: ready: names no program")
            }
            Some(_) => {}
            None if self.ready_timeout_secs.is_some() => bail!(
                "{at}: ready_timeout_secs is how long `ready` has to succeed, and it has no `ready`"
            ),
            None => {}
        }
        if let Some(secs) = self.ready_timeout_secs {
            if !(1..=MAX_READY_TIMEOUT_SECS).contains(&secs) {
                bail!("{at}: ready_timeout_secs is {secs}: from 1 to {MAX_READY_TIMEOUT_SECS}");
            }
        }
        Ok(())
    }
}

/// A task's services together: each one as [`ServiceSpec::validate`] takes
/// it, no two of a name, and no more than [`MAX_SERVICES`].
pub fn validate(services: &[ServiceSpec]) -> Result<()> {
    if services.len() > MAX_SERVICES {
        bail!(
            "services: {} of them, and a task may have {MAX_SERVICES}",
            services.len()
        );
    }
    let mut names = BTreeSet::new();
    for service in services {
        service.validate()?;
        if !names.insert(service.name.as_str()) {
            bail!("services: two are named '{}'", service.name);
        }
    }
    Ok(())
}

/// Why a task with these services is not run by an engine whose tasks are
/// its own processes; `None` for a task with none.
pub fn not_run_locally(services: &[ServiceSpec]) -> Option<String> {
    let first = services.first()?;
    Some(format!(
        "this task has a service ('{}'), and this engine runs a task as a process of its own \
         (EXECUTOR=local): it has nothing to run a service in. Run the task on an engine with \
         the Docker or the Kubernetes executor",
        first.name
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn services(yaml: &str) -> Vec<ServiceSpec> {
        serde_yaml::from_str(yaml).unwrap_or_else(|e| panic!("{yaml}: {e}"))
    }

    #[test]
    fn a_service_is_read_with_everything_it_carries_and_written_back_the_same() {
        let read = services(
            "- name: pg\n  image: postgres:16-alpine\n  args: [postgres, -c, fsync=off]\n  \
             env:\n    - { name: POSTGRES_PASSWORD, value: pw }\n  \
             ready: pg_isready -h localhost -U postgres\n  ready_timeout_secs: 90\n  \
             resources: { requests: { cpu: 100m }, limits: { memory: 256Mi } }\n\
             - name: cache\n  image: redis:7\n  ready: [redis-cli, ping]\n\
             - name: plain\n  image: busybox:1.36.1\n",
        );
        validate(&read).expect("valid");
        let pg = &read[0];
        assert_eq!(
            (pg.name.as_str(), pg.image.as_str()),
            ("pg", "postgres:16-alpine")
        );
        assert_eq!(pg.args, ["postgres", "-c", "fsync=off"]);
        assert_eq!(pg.env[0].value, "pw");
        assert_eq!(
            pg.ready.as_ref().map(Ready::argv),
            Some(vec![
                "sh".to_string(),
                "-c".to_string(),
                "pg_isready -h localhost -U postgres".to_string()
            ])
        );
        assert_eq!(pg.ready_timeout(), 90);
        assert_eq!(
            pg.resources.as_ref().map(|r| r.limits["memory"].as_str()),
            Some("256Mi")
        );
        assert_eq!(
            read[1].ready,
            Some(Ready::Exec(vec!["redis-cli".into(), "ping".into()]))
        );
        assert_eq!(read[1].ready_timeout(), DEFAULT_READY_TIMEOUT_SECS);
        assert!(read[2].ready.is_none() && read[2].args.is_empty());

        // As a run stores it, and as an engine reads it back.
        let stored = serde_json::to_string(&read).expect("serializes");
        let back: Vec<ServiceSpec> = serde_json::from_str(&stored).expect("reads back");
        assert_eq!(serde_json::to_string(&back).unwrap(), stored);
        assert_eq!(back[0].ready, read[0].ready);
        assert_eq!(back[1].ready, read[1].ready);
        assert!(!stored.contains("ready_timeout_secs\":null") && !stored.contains("\"args\":[]"));
    }

    #[test]
    fn a_service_that_could_not_be_run_or_found_is_refused() {
        for (yaml, why) in [
            ("- { name: PG, image: x }", "not a service's name"),
            ("- { name: 1pg, image: x }", "not a service's name"),
            ("- { name: pg-, image: x }", "not a service's name"),
            ("- { name: pg_1, image: x }", "not a service's name"),
            (
                "- { name: localhost, image: x }",
                "every container's name for itself",
            ),
            ("- { name: pg, image: \"\" }", "not an image's name"),
            ("- { name: pg, image: \"a b\" }", "not an image's name"),
            (
                "- { name: pg, image: \"{{ nope }}\" }",
                "a parameter the run does not have",
            ),
            ("- { name: pg, image: x, ready: \" \" }", "ready: is empty"),
            ("- { name: pg, image: x, ready: [] }", "names no program"),
            (
                "- { name: pg, image: x, ready_timeout_secs: 5 }",
                "it has no `ready`",
            ),
            (
                "- { name: pg, image: x, ready: \"true\", ready_timeout_secs: 0 }",
                "from 1 to 600",
            ),
            (
                "- { name: pg, image: x, ready: \"true\", ready_timeout_secs: 601 }",
                "from 1 to 600",
            ),
            (
                "- { name: pg, image: x, env: [{ name: 1X, value: y }] }",
                "not a variable's name",
            ),
            (
                "- { name: pg, image: x, env: [{ name: X, value: a }, { name: X, value: b }] }",
                "set twice",
            ),
            (
                "- { name: pg, image: x, env: [{ name: X, value_from: { secret: S } }] }",
                "no secret is given to a service",
            ),
            (
                "- { name: pg, image: x }\n- { name: pg, image: y }",
                "two are named 'pg'",
            ),
        ] {
            let err = validate(&services(yaml)).expect_err(yaml).to_string();
            assert!(err.contains(why), "{yaml}: {err}");
        }
        let many: String = (0..=MAX_SERVICES)
            .map(|i| format!("- {{ name: s{i}, image: x }}\n"))
            .collect();
        let err = validate(&services(&many)).unwrap_err().to_string();
        assert!(err.contains("a task may have 8"), "{err}");

        // What is not a service's to say is not read as one.
        for (yaml, why) in [
            (
                "- { name: pg, image: x, ports: [5432] }",
                "unknown field `ports`",
            ),
            (
                "- { name: pg, image: x, ready: { tcp: 5432 } }",
                "a command",
            ),
            (
                "- { name: pg, image: x, resources: { gpu: { count: 1 } } }",
                "unknown field `gpu`",
            ),
        ] {
            let err = serde_yaml::from_str::<Vec<ServiceSpec>>(yaml)
                .expect_err(yaml)
                .to_string();
            assert!(err.contains(why), "{yaml}: {err}");
        }
    }

    #[test]
    fn a_service_s_parameters_are_filled_in_and_its_name_is_not_one() {
        let read = services(
            "- name: pg\n  image: \"postgres:{{ pg }}\"\n  args: [postgres, -c, \"port={{ port }}\"]\n  \
             env: [{ name: POSTGRES_DB, value: \"{{ db }}\" }]\n  \
             ready: \"pg_isready -p {{ port }}\"\n\
             - name: probe\n  image: x\n  ready: [check, \"--port={{ port }}\"]\n",
        );
        let fill = |text: &str| {
            text.replace("{{ pg }}", "16")
                .replace("{{ port }}", "5433")
                .replace("{{ db }}", "app")
        };
        let pg = read[0].filled(fill);
        assert_eq!(pg.name, "pg");
        assert_eq!(pg.image, "postgres:16");
        assert_eq!(pg.args[2], "port=5433");
        assert_eq!(pg.env[0].value, "app");
        assert_eq!(pg.ready, Some(Ready::Shell("pg_isready -p 5433".into())));
        assert_eq!(
            read[1].filled(fill).ready,
            Some(Ready::Exec(vec!["check".into(), "--port=5433".into()]))
        );
    }
}

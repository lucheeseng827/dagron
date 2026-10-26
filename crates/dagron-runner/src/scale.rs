//! Autoscaling runners by the work there is for each label.
//!
//! `dagron-runner scale` reads dagron-api's queue for the labels it scales
//! (`GET /runner/v1/queue`, with a runner credential that serves them; it
//! opens no session and runs nothing), asks [`ScalePolicy`] how many
//! replicas that work asks for, and sets each target to it: up at once, down
//! only once the work has asked for fewer for a while ([`Damper`]).
//!
//! A target is anything a [`Scaler`] can size. Here: a Kubernetes Deployment
//! of runners (`kubernetes` feature). The VM pools of a cloud's API are the
//! enterprise build's, on the same loop and the same policy.
//!
//! A replica that is scaled away is sent SIGTERM, and drains: it takes
//! nothing more and finishes what it runs. Give its pod a termination grace
//! as long as its longest task.

use std::time::{Duration, Instant};

use anyhow::{bail, Context, Result};
use async_trait::async_trait;
use dagron_core::runner::{Damper, ScalePolicy};
use tokio::sync::watch;

use crate::client::{Backoff, Client};
use crate::runner::Shutdown;

pub const ENV_TARGETS: &str = "DAGRON_SCALE_TARGETS";
pub const ENV_MIN: &str = "DAGRON_SCALE_MIN";
pub const ENV_MAX: &str = "DAGRON_SCALE_MAX";
pub const ENV_PER_REPLICA: &str = "DAGRON_SCALE_PER_REPLICA";
pub const ENV_INTERVAL: &str = "DAGRON_SCALE_INTERVAL_SECS";
pub const ENV_DOWN_AFTER: &str = "DAGRON_SCALE_DOWN_AFTER_SECS";

/// Something whose number of replicas can be read and set.
#[async_trait]
pub trait Scaler: Send + Sync {
    /// What it is, for the log.
    fn describe(&self) -> String;
    async fn current(&self) -> Result<i64>;
    async fn set(&self, replicas: i64) -> Result<()>;
}

/// One label, and what is sized for it.
pub struct Target {
    pub label: String,
    pub scaler: Box<dyn Scaler>,
    damper: Damper,
}

impl Target {
    pub fn new(label: String, scaler: Box<dyn Scaler>, down_after: Duration) -> Target {
        Target {
            label,
            scaler,
            damper: Damper::new(down_after),
        }
    }
}

/// How the loop is set: `DAGRON_SCALE_*`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Settings {
    /// Each label, and the name of what is sized for it: `label=name`.
    pub targets: Vec<(String, String)>,
    pub policy: ScalePolicy,
    pub every: Duration,
    pub down_after: Duration,
}

impl Settings {
    pub fn from_env() -> Result<Settings> {
        Settings::from_lookup(|name| std::env::var(name).ok())
    }

    pub fn from_lookup(get: impl Fn(&str) -> Option<String>) -> Result<Settings> {
        let raw = get(ENV_TARGETS)
            .map(|v| v.trim().to_string())
            .filter(|v| !v.is_empty());
        let raw = raw.with_context(|| {
            format!("{ENV_TARGETS} is not set: what to scale, as label=name (comma-separated)")
        })?;
        let mut targets = Vec::new();
        for entry in raw.split(',').map(str::trim).filter(|e| !e.is_empty()) {
            let (label, name) = entry
                .split_once('=')
                .with_context(|| format!("{ENV_TARGETS}: '{entry}' is not label=name"))?;
            let (label, name) = (label.trim(), name.trim());
            dagron_core::dag::validate_runner_class(label)
                .map_err(|e| anyhow::anyhow!("{ENV_TARGETS}: {e}"))?;
            if name.is_empty() {
                bail!("{ENV_TARGETS}: '{entry}' names nothing to scale");
            }
            targets.push((label.to_string(), name.to_string()));
        }
        let pace = Pace::from_lookup(get)?;
        Ok(Settings {
            targets,
            policy: pace.policy,
            every: pace.every,
            down_after: pace.down_after,
        })
    }
}

/// How the loop sizes, whatever it sizes: the policy, how often it looks, and
/// how long the work must ask for fewer before a target is scaled down.
/// `DAGRON_SCALE_MIN`, `_MAX`, `_PER_REPLICA`, `_INTERVAL_SECS`,
/// `_DOWN_AFTER_SECS`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Pace {
    pub policy: ScalePolicy,
    pub every: Duration,
    pub down_after: Duration,
}

impl Pace {
    pub fn from_lookup(get: impl Fn(&str) -> Option<String>) -> Result<Pace> {
        let get = |name: &str| {
            get(name)
                .map(|v| v.trim().to_string())
                .filter(|v| !v.is_empty())
        };
        let number = |name: &str, default: i64| -> Result<i64> {
            match get(name) {
                None => Ok(default),
                Some(raw) => raw
                    .parse::<i64>()
                    .ok()
                    .filter(|n| *n >= 0)
                    .with_context(|| format!("{name} is a whole number, not '{raw}'")),
            }
        };
        let policy = ScalePolicy {
            min: number(ENV_MIN, 0)?,
            max: number(ENV_MAX, 10)?,
            per_replica: number(ENV_PER_REPLICA, 1)?.max(1),
        };
        if policy.max < policy.min {
            bail!(
                "{ENV_MAX} ({}) is less than {ENV_MIN} ({})",
                policy.max,
                policy.min
            );
        }
        Ok(Pace {
            policy,
            every: Duration::from_secs(number(ENV_INTERVAL, 15)?.max(1) as u64),
            down_after: Duration::from_secs(number(ENV_DOWN_AFTER, 300)? as u64),
        })
    }
}

/// Size each target to the work there is for its label, every `every`, until
/// asked to stop. A queue that cannot be read, or a target that cannot be
/// sized, is said and tried again: the replicas stay as they are meanwhile.
pub async fn control_loop(
    client: Client,
    mut targets: Vec<Target>,
    policy: ScalePolicy,
    every: Duration,
    mut shutdown: watch::Receiver<Shutdown>,
) -> Result<()> {
    let labels: Vec<String> = targets.iter().map(|t| t.label.clone()).collect();
    let mut backoff = Backoff::default();
    loop {
        if *shutdown.borrow() != Shutdown::No {
            return Ok(());
        }
        match client.queue(&labels).await {
            Ok(queue) => {
                backoff.reset();
                for target in targets.iter_mut() {
                    let Some(q) = queue.labels.iter().find(|q| q.label == target.label) else {
                        continue;
                    };
                    let desired = policy.desired(q);
                    if let Err(e) = step(target, desired).await {
                        tracing::warn!(label = %target.label, target = %target.scaler.describe(), error = %e, "could not scale");
                    }
                }
            }
            Err(e) => {
                tracing::warn!(error = %e, "could not read the queue; trying again");
                tokio::select! {
                    _ = backoff.wait() => {}
                    _ = shutdown.changed() => {}
                }
                continue;
            }
        }
        tokio::select! {
            _ = tokio::time::sleep(every) => {}
            _ = shutdown.changed() => {}
        }
    }
}

async fn step(target: &mut Target, desired: i64) -> Result<()> {
    let current = target.scaler.current().await?;
    let next = target.damper.next(current, desired, Instant::now());
    if next != current {
        tracing::info!(
            label = %target.label, target = %target.scaler.describe(), current, desired, next,
            "scaling"
        );
        target.scaler.set(next).await?;
    }
    Ok(())
}

/// A Deployment of runners, sized through its `scale` subresource.
#[cfg(feature = "kubernetes")]
pub struct Deployment {
    api: kube::Api<k8s_openapi::api::apps::v1::Deployment>,
    namespace: String,
    name: String,
}

#[cfg(feature = "kubernetes")]
impl Deployment {
    /// `namespace/name`, or `name` in `default_namespace`.
    pub fn new(client: kube::Client, target: &str, default_namespace: &str) -> Deployment {
        let (namespace, name) = match target.split_once('/') {
            Some((ns, name)) => (ns.to_string(), name.to_string()),
            None => (default_namespace.to_string(), target.to_string()),
        };
        Deployment {
            api: kube::Api::namespaced(client, &namespace),
            namespace,
            name,
        }
    }
}

#[cfg(feature = "kubernetes")]
#[async_trait]
impl Scaler for Deployment {
    fn describe(&self) -> String {
        format!("deployment {}/{}", self.namespace, self.name)
    }

    async fn current(&self) -> Result<i64> {
        let scale = self.api.get_scale(&self.name).await?;
        Ok(scale.spec.and_then(|s| s.replicas).unwrap_or(0) as i64)
    }

    async fn set(&self, replicas: i64) -> Result<()> {
        let patch = serde_json::json!({ "spec": { "replicas": replicas } });
        self.api
            .patch_scale(
                &self.name,
                &kube::api::PatchParams::default(),
                &kube::api::Patch::Merge(&patch),
            )
            .await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};

    fn read(vars: &[(&str, &str)]) -> Result<Settings> {
        let vars: Vec<(String, String)> = vars
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        Settings::from_lookup(|name| vars.iter().find(|(k, _)| k == name).map(|(_, v)| v.clone()))
    }

    #[test]
    fn targets_and_bounds_are_read_or_refused_by_name() {
        let s = read(&[
            (ENV_TARGETS, "gpu=ci/runners-gpu, linux=runners"),
            (ENV_MAX, "20"),
            (ENV_PER_REPLICA, "4"),
        ])
        .unwrap();
        assert_eq!(
            s.targets,
            [
                ("gpu".into(), "ci/runners-gpu".into()),
                ("linux".into(), "runners".into())
            ]
        );
        assert_eq!(
            s.policy,
            ScalePolicy {
                min: 0,
                max: 20,
                per_replica: 4
            }
        );
        assert_eq!(
            (s.every, s.down_after),
            (Duration::from_secs(15), Duration::from_secs(300))
        );
        assert!(read(&[]).unwrap_err().to_string().contains(ENV_TARGETS));
        assert!(read(&[(ENV_TARGETS, "gpu")]).is_err());
        assert!(read(&[(ENV_TARGETS, "gpu=x"), (ENV_MIN, "5"), (ENV_MAX, "2")]).is_err());
        assert!(read(&[(ENV_TARGETS, "gpu=x"), (ENV_MAX, "-1")]).is_err());
    }

    struct Fake(Arc<Mutex<i64>>);

    #[async_trait]
    impl Scaler for Fake {
        fn describe(&self) -> String {
            "fake".into()
        }
        async fn current(&self) -> Result<i64> {
            Ok(*self.0.lock().unwrap())
        }
        async fn set(&self, replicas: i64) -> Result<()> {
            *self.0.lock().unwrap() = replicas;
            Ok(())
        }
    }

    #[tokio::test]
    async fn a_step_scales_up_at_once_and_holds_on_the_way_down() {
        let replicas = Arc::new(Mutex::new(1));
        let mut target = Target::new(
            "gpu".into(),
            Box::new(Fake(Arc::clone(&replicas))),
            Duration::from_secs(300),
        );
        step(&mut target, 3).await.unwrap();
        assert_eq!(*replicas.lock().unwrap(), 3);
        step(&mut target, 0).await.unwrap();
        assert_eq!(
            *replicas.lock().unwrap(),
            3,
            "held until the work has asked for fewer for a while"
        );
    }
}

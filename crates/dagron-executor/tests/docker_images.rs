//! 6.10, the image cache, on the Docker executor: a run's image pulled
//! through its registry's mirror, once on the host however many of the run's
//! tasks want it at once, and pinned for the run; another run resolves its
//! tag again. Skipped where no Docker daemon answers, or where the mirror
//! (Google's public mirror of Docker Hub) cannot be reached.

use std::sync::atomic::Ordering;
use std::sync::Arc;

use bollard::image::RemoveImageOptions;
use dagron_core::dag::ImagePullPolicy;
use dagron_core::metrics::Metrics;
use dagron_executor::docker_executor::DockerExecutor;
use dagron_executor::executor::{ExecContext, Executor, RunImage, TaskIdentity};
use dagron_executor::images::Registries;

const BASE: &str = "alpine:3.20";
const IMAGE: &str = "busybox:1.36.1";
const MIRROR: &str = "mirror.gcr.io";

static LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

async fn daemon() -> Option<bollard::Docker> {
    let docker = match bollard::Docker::connect_with_local_defaults() {
        Ok(d) => d,
        Err(e) => {
            eprintln!("no Docker client here ({e}) - skipping");
            return None;
        }
    };
    if let Err(e) = docker.ping().await {
        eprintln!("no Docker daemon answers ({e}) - skipping");
        return None;
    }
    Some(docker)
}

/// `image` taken off the host.
async fn forget(docker: &bollard::Docker, image: &str) {
    let force = RemoveImageOptions {
        force: true,
        ..Default::default()
    };
    let _ = docker.remove_image(image, Some(force), None).await;
}

/// A task of `run` that says `ran`, from [`IMAGE`], under `policy`.
fn task(run: &str, n: u32, policy: ImagePullPolicy) -> ExecContext {
    let mut ctx = ExecContext::new(
        vec!["sh".into(), "-c".into(), "echo ran".into()],
        Some(120),
        Some(IMAGE.into()),
    );
    ctx.image_pull_policy = Some(policy);
    ctx.identity = Some(TaskIdentity {
        task_id: format!("t-{run}-{n}"),
        run_id: run.to_string(),
        attempt: 1,
        fence: 1,
        installation: None,
    });
    ctx
}

#[tokio::test]
async fn a_run_s_image_is_pulled_through_its_mirror_once_and_pinned_for_the_run() {
    let _one = LOCK.lock().await;
    let Some(docker) = daemon().await else {
        return;
    };
    let mirrored = format!("{MIRROR}/library/{IMAGE}");
    forget(&docker, &mirrored).await;
    let metrics = Arc::new(Metrics::new());
    let exec = DockerExecutor::connect(BASE)
        .await
        .unwrap()
        .with_registries(Registries::parse(Some(&format!("docker.io={MIRROR}")), None).unwrap())
        .with_metrics(Arc::clone(&metrics));
    let run = format!("r-{}", uuid::Uuid::new_v4().simple());

    // The run's images fetched ahead, and two of its tasks at once, each
    // with `always`: one pull, from the mirror.
    let ahead = vec![RunImage {
        image: Some(IMAGE.into()),
        policy: Some(ImagePullPolicy::Always),
    }];
    let (first, second) = (
        task(&run, 1, ImagePullPolicy::Always),
        task(&run, 2, ImagePullPolicy::Always),
    );
    let (_, a, b) = tokio::join!(
        exec.prefetch(&run, ahead),
        exec.execute(&first),
        exec.execute(&second),
    );
    let (a, b) = match (a, b) {
        (Ok(a), Ok(b)) => (a, b),
        (Err(e), _) | (_, Err(e)) => {
            let why = format!("{e:#}");
            if why.contains("could not be pulled")
                && metrics.image_pull_failures.load(Ordering::Relaxed) > 0
            {
                eprintln!("{MIRROR} cannot be reached from here ({why}) - skipping");
                return;
            }
            panic!("{why}");
        }
    };
    assert!(a.success && a.output.contains("ran"), "{:?}", a.output);
    assert!(b.success && b.output.contains("ran"), "{:?}", b.output);
    assert_eq!(
        metrics.image_pulls.load(Ordering::Relaxed),
        1,
        "one pull of the run's image on this host"
    );
    assert!(
        metrics.image_cache_hits.load(Ordering::Relaxed) >= 2,
        "the run's other two wants of it were given it"
    );
    assert!(
        docker.inspect_image(&mirrored).await.is_ok(),
        "pulled through the mirror"
    );

    // A later task of the run, with `always`, pulls nothing: its tag was
    // resolved once for the run.
    let out = exec
        .execute(&task(&run, 3, ImagePullPolicy::Always))
        .await
        .unwrap();
    assert!(out.success, "{:?}", out.output);
    assert_eq!(metrics.image_pulls.load(Ordering::Relaxed), 1);

    // Another run's tag is resolved for it: `always` pulls again.
    let other = format!("r-{}", uuid::Uuid::new_v4().simple());
    let out = exec
        .execute(&task(&other, 1, ImagePullPolicy::Always))
        .await
        .unwrap();
    assert!(out.success, "{:?}", out.output);
    assert_eq!(metrics.image_pulls.load(Ordering::Relaxed), 2);

    // A task with no run, `if_not_present`, of an image the host holds:
    // no pull.
    let mut alone = task(&run, 4, ImagePullPolicy::IfNotPresent);
    alone.identity = None;
    let out = exec.execute(&alone).await.unwrap();
    assert!(out.success, "{:?}", out.output);
    assert_eq!(metrics.image_pulls.load(Ordering::Relaxed), 2);
    forget(&docker, &mirrored).await;
}

/// The engine's credential for a registry, from its auth file, goes with a
/// trusted run's pull and with no other: a run that is not trusted pulls as
/// no one, so a private image is not its to have. Run where a registry that
/// checks credentials holds `IMAGE` under `DAGRON_TEST_REGISTRY` (a host
/// the daemon reaches, such as `localhost:5000` in a dind container) and
/// `DAGRON_TEST_REGISTRY_AUTH` is its `user:password`; skipped otherwise.
#[tokio::test]
async fn the_engine_s_credential_goes_with_a_trusted_run_s_pull_alone() {
    let _one = LOCK.lock().await;
    let (Ok(registry), Ok(auth)) = (
        std::env::var("DAGRON_TEST_REGISTRY"),
        std::env::var("DAGRON_TEST_REGISTRY_AUTH"),
    ) else {
        eprintln!("no DAGRON_TEST_REGISTRY with credentials here - skipping");
        return;
    };
    let Some(docker) = daemon().await else {
        return;
    };
    let private = format!("{registry}/private/{IMAGE}");
    let (user, password) = auth.split_once(':').expect("user:password");
    let file = serde_json::json!({
        "auths": { registry.as_str(): { "username": user, "password": password } }
    })
    .to_string();
    let with = DockerExecutor::connect(BASE)
        .await
        .unwrap()
        .with_registries(Registries::parse(None, Some(&file)).unwrap());
    let without = DockerExecutor::connect(BASE).await.unwrap();
    let from = |run: &str| {
        let mut ctx = task(run, 1, ImagePullPolicy::Always);
        ctx.docker_image = Some(private.clone());
        ctx
    };

    forget(&docker, &private).await;
    let err = without
        .execute(&from("r-none"))
        .await
        .err()
        .expect("a pull as no one is refused");
    assert!(
        format!("{err:#}").contains("could not be pulled"),
        "{err:#}"
    );

    let mut confined = from("r-untrusted");
    confined.confinement = Some(dagron_executor::executor::Confinement {
        memory_bytes: 256 << 20,
        pids: 256,
        nano_cpus: 1_000_000_000,
    });
    let err = with
        .execute(&confined)
        .await
        .err()
        .expect("a run that is not trusted pulls as no one");
    assert!(
        format!("{err:#}").contains("could not be pulled"),
        "{err:#}"
    );
    assert!(
        docker.inspect_image(&private).await.is_err(),
        "nothing was pulled for it"
    );

    let out = with
        .execute(&from("r-trusted"))
        .await
        .expect("pulled with the engine's credential");
    assert!(
        out.success && out.output.contains("ran"),
        "{:?}",
        out.output
    );

    // The host holds it now; a run that is not trusted is still not given
    // it, under any policy: it pulls, as no one, and is refused.
    for policy in [ImagePullPolicy::IfNotPresent, ImagePullPolicy::Never] {
        let mut ctx = from("r-untrusted-later");
        ctx.image_pull_policy = Some(policy);
        ctx.confinement = confined.confinement;
        let err = with
            .execute(&ctx)
            .await
            .err()
            .unwrap_or_else(|| panic!("{}: the host's copy was given to it", policy.as_str()));
        assert!(
            format!("{err:#}").contains("could not be pulled"),
            "{err:#}"
        );
    }
    assert!(
        docker.inspect_image(&private).await.is_ok(),
        "still held, for the trusted run"
    );
    forget(&docker, &private).await;
}

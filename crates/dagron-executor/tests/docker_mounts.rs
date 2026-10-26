//! The engine's task mounts and working directory, against a real Docker
//! daemon: a task works in a host directory it was given, writes into it, and
//! cannot write where it was given a read-only one.
//!
//! Needs a daemon, and skips, saying so, where none answers. The image is
//! pulled first, since the executor runs only images the daemon has.

use bollard::image::CreateImageOptions;
use dagron_executor::docker_executor::{DockerExecutor, TaskMount};
use dagron_executor::executor::{ExecContext, Executor};
use futures_util::TryStreamExt;

const IMAGE: &str = "alpine:3.20";

#[tokio::test]
async fn a_task_works_in_the_directory_it_was_given() {
    let Ok(docker) = bollard::Docker::connect_with_local_defaults() else {
        return eprintln!("no Docker client here - skipping");
    };
    if let Err(e) = docker.ping().await {
        return eprintln!("no Docker daemon answers ({e}) - skipping");
    }
    docker
        .create_image(
            Some(CreateImageOptions {
                from_image: IMAGE,
                ..Default::default()
            }),
            None,
            None,
        )
        .try_collect::<Vec<_>>()
        .await
        .expect("pull the test image");

    let tree =
        std::env::temp_dir().join(format!("dagron-mounts-{}", uuid::Uuid::new_v4().simple()));
    let shared = tree.join("shared");
    std::fs::create_dir_all(&shared).unwrap();
    std::fs::write(tree.join("input.txt"), "from the host\n").unwrap();
    std::fs::write(shared.join("note"), "read only\n").unwrap();
    let tree_path = tree.to_str().unwrap().to_string();

    let exec = DockerExecutor::connect(IMAGE)
        .await
        .unwrap()
        .with_task_mounts(
            vec![
                TaskMount {
                    source: tree_path.clone(),
                    target: tree_path.clone(),
                    read_only: false,
                },
                TaskMount {
                    source: shared.to_str().unwrap().into(),
                    target: "/shared".into(),
                    read_only: true,
                },
            ],
            Some(tree_path.clone()),
        );
    let script = "pwd; cat input.txt; echo made > output.txt; cat /shared/note; \
                  if touch /shared/x 2>/dev/null; then echo writable; else echo refused; fi";
    let out = exec
        .execute(&ExecContext::new(
            vec!["sh".into(), "-c".into(), script.into()],
            Some(60),
            None,
        ))
        .await
        .unwrap();

    assert!(out.success, "{}", out.output);
    assert_eq!(
        out.output,
        format!("{tree_path}\nfrom the host\nread only\nrefused\n")
    );
    assert_eq!(
        std::fs::read_to_string(tree.join("output.txt")).unwrap(),
        "made\n"
    );
    assert!(!shared.join("x").exists());
    // Best effort: the container wrote as its own user, which may be root.
    let _ = std::fs::remove_dir_all(&tree);
}

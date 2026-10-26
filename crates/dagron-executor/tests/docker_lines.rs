//! A container's two streams, against a real Docker daemon: a line of one is
//! not cut in two by a line of the other.
//!
//! The daemon hands stdout and stderr over as frames, and a frame need not
//! end where a line does: one of an attached container's streams ends
//! wherever the daemon's read of its pipe ended, and one of its log read back
//! ends short of a newline in a line longer than its buffer of 16 KiB. So a
//! task that writes such a line in two parts, a second apart, and a line to
//! stderr between them, is handed the long line in two frames with the other
//! stream's between them, however its output is read.
//!
//! Needs a daemon, and skips, saying so, where none answers. The image is
//! pulled first, since the executor runs only images the daemon has.

use bollard::image::CreateImageOptions;
use dagron_executor::docker_executor::DockerExecutor;
use dagron_executor::executor::{ExecContext, Executor};
use futures_util::TryStreamExt;

const IMAGE: &str = "alpine:3.20";

/// How much of a line the task writes before it writes to stderr: more than
/// the buffer of the daemon's log, so that the daemon has handed part of it
/// over however the output is read.
const LONG: usize = 20_000;

#[tokio::test]
async fn a_line_longer_than_the_daemon_s_buffer_is_whole_around_the_other_stream_s() {
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

    // The long line's beginning, a line of stderr a second later, and the
    // long line's end a second after that: the waits are for the daemon to
    // have read each before the next is written.
    let script = format!(
        "head -c {LONG} /dev/zero | tr '\\0' a; sleep 1; \
         echo between >&2; sleep 1; echo ' and its end'"
    );
    let exec = DockerExecutor::connect(IMAGE).await.unwrap();
    let ctx = ExecContext::new(vec!["sh".into(), "-c".into(), script], Some(60), None);
    let out = exec.execute(&ctx).await.expect("the task runs");
    assert!(out.success, "{}", out.output);

    let long = format!("{} and its end\n", "a".repeat(LONG));
    assert_eq!(
        out.output.len(),
        "between\n".len() + long.len(),
        "nothing is lost and nothing is added"
    );
    assert!(
        out.output.contains(&long),
        "the long line is whole: the other stream's line is at byte {:?} of the output",
        out.output.find("between")
    );
    assert_eq!(
        out.output,
        format!("between\n{long}"),
        "and each line is where it ended"
    );
}

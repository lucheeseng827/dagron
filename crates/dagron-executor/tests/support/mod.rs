//! What the integration tests share. Each test file takes what it needs.
#![allow(dead_code)]

#[cfg(unix)]
pub mod docker_proxy;
pub mod git_server;

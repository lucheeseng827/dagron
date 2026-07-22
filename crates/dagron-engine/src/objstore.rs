//! Build an `object_store` client from a cloud archive URL, dispatching by
//! scheme to whichever backend was compiled in:
//!
//! * `s3://bucket/prefix`   — feature `archive-s3` (also S3-compatible MinIO /
//!   Ceph via `AWS_ENDPOINT_URL`),
//! * `gs://bucket/prefix`   — feature `archive-gcs`,
//! * `az://container/prefix` (or `azure://`) — feature `archive-azure`.
//!
//! Each backend's credentials/region/endpoint come from its standard env
//! (`AWS_*` / `GOOGLE_*` / `AZURE_*`), matching the `from_env()` convention — so
//! an in-cluster MinIO, a GCS bucket, or an Azure Blob container needs no code
//! change, only the right env. A scheme whose backend feature is not compiled in
//! is a hard error, never a silent downgrade to a plain purge.

use std::sync::Arc;

use anyhow::{bail, Result};
use object_store::{path::Path, ObjectStore};

/// Parse a cloud object-store URL into `(store, prefix)`: the store is rooted at
/// the bucket/container, `prefix` is the in-bucket path (possibly empty).
pub fn from_url(url: &str) -> Result<(Arc<dyn ObjectStore>, Path)> {
    let url = url.trim();
    match url.split_once("://") {
        Some(("s3", rest)) => {
            #[cfg(feature = "archive-s3")]
            {
                let (bucket, prefix) = split(rest, url)?;
                let store = object_store::aws::AmazonS3Builder::from_env()
                    .with_bucket_name(bucket)
                    .build()
                    .map_err(|e| anyhow::anyhow!("{url} S3 config: {e}"))?;
                Ok((Arc::new(store), Path::from(prefix)))
            }
            #[cfg(not(feature = "archive-s3"))]
            {
                let _ = rest;
                bail!("{url} needs the `archive-s3` feature");
            }
        }
        Some(("gs", rest)) => {
            #[cfg(feature = "archive-gcs")]
            {
                let (bucket, prefix) = split(rest, url)?;
                let store = object_store::gcp::GoogleCloudStorageBuilder::from_env()
                    .with_bucket_name(bucket)
                    .build()
                    .map_err(|e| anyhow::anyhow!("{url} GCS config: {e}"))?;
                Ok((Arc::new(store), Path::from(prefix)))
            }
            #[cfg(not(feature = "archive-gcs"))]
            {
                let _ = rest;
                bail!("{url} needs the `archive-gcs` feature");
            }
        }
        Some(("az" | "azure", rest)) => {
            #[cfg(feature = "archive-azure")]
            {
                let (container, prefix) = split(rest, url)?;
                let store = object_store::azure::MicrosoftAzureBuilder::from_env()
                    .with_container_name(container)
                    .build()
                    .map_err(|e| anyhow::anyhow!("{url} Azure config: {e}"))?;
                Ok((Arc::new(store), Path::from(prefix)))
            }
            #[cfg(not(feature = "archive-azure"))]
            {
                let _ = rest;
                bail!("{url} needs the `archive-azure` feature");
            }
        }
        _ => bail!("archive URL must be s3://, gs://, or az://…, got '{url}'"),
    }
}

/// Split "bucket/prefix" → ("bucket", "prefix") with the prefix trimmed of `/`.
/// Errors on an empty bucket/container.
#[cfg(any(feature = "archive-s3", feature = "archive-gcs", feature = "archive-azure"))]
fn split<'a>(rest: &'a str, url: &str) -> Result<(&'a str, &'a str)> {
    let (bucket, prefix) = match rest.split_once('/') {
        Some((b, p)) => (b, p.trim_matches('/')),
        None => (rest, ""),
    };
    if bucket.is_empty() {
        bail!("archive URL is missing a bucket/container: '{url}'");
    }
    Ok((bucket, prefix))
}

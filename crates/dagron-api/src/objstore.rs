//! Build an `object_store` client from a cloud archive URL, dispatching by
//! scheme to whichever backend was compiled in — `s3://` (feature `archive-s3`,
//! incl. S3-compatible MinIO/Ceph via `AWS_ENDPOINT_URL`), `gs://`
//! (`archive-gcs`), `az://`/`azure://` (`archive-azure`). Mirrors the engine's
//! `objstore` so the `/api/archive` fetch honors the same `GC_ARCHIVE_URL` +
//! backend env contract the GC writes with.

use std::sync::Arc;

use anyhow::{bail, Result};
use object_store::{path::Path, ObjectStore};

/// Parse a cloud object-store URL into `(store, prefix)`: the store rooted at
/// the bucket/container, `prefix` the in-bucket path (possibly empty).
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

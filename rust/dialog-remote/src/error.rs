use thiserror::Error;

#[derive(Debug, Error)]
pub enum DialogRemoteError {
    #[error("Could not create an S3-compatible pre-signed URL: {0}")]
    S3UrlSigner(String),
}

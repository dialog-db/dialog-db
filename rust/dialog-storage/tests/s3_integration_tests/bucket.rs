//! Test helpers for S3 integration tests.

#![cfg(feature = "s3-integration-tests")]

use dialog_storage::s3::{Address, Bucket, S3, S3Credentials, Session};

/// Adds timestamp to the given string to make it unique
pub fn unique(base: &str) -> String {
    let millis = dialog_common::time::now()
        .duration_since(dialog_common::time::UNIX_EPOCH)
        .unwrap()
        .as_millis();
    format!("{}-{}", base, millis)
}

/// Helper to create an S3 backend from environment variables.
///
/// Uses `option_env!` instead of `env!` so that `cargo check --tests --all-features`
/// doesn't fail when the R2S3_* environment variables aren't set at compile time.
pub fn open() -> Bucket<Session> {
    let address = Address::new(
        option_env!("R2S3_ENDPOINT").expect("R2S3_ENDPOINT not set"),
        option_env!("R2S3_REGION").expect("R2S3_REGION not set"),
        option_env!("R2S3_BUCKET").expect("R2S3_BUCKET not set"),
    );

    // Use the bucket name as subject by default for integration tests
    let subject = option_env!("R2S3_SUBJECT").unwrap_or("did:key:zTestSubject");

    let credentials = S3Credentials::private(
        address,
        option_env!("R2S3_ACCESS_KEY_ID").expect("R2S3_ACCESS_KEY_ID not set"),
        option_env!("R2S3_SECRET_ACCESS_KEY").expect("R2S3_SECRET_ACCESS_KEY not set"),
    )
    .expect("Failed to create credentials");

    let s3 = S3::from_s3(credentials, Session::new(subject));
    Bucket::new(s3, subject, "integration-tests")
}

pub fn open_unique_at(base: &str) -> Bucket<Session> {
    open().at(unique(base))
}

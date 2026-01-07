use dialog_storage::s3::{Address, Bucket, Credentials};

/// Generate a globally unique test prefix using timestamp
pub fn unique_prefix(base: &str) -> String {
    #[cfg(not(target_arch = "wasm32"))]
    let millis = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis();

    #[cfg(target_arch = "wasm32")]
    let millis = {
        use web_time::web::SystemTimeExt;
        web_time::SystemTime::now()
            .to_std()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis()
    };

    format!("{}-{}", base, millis)
}

/// Helper to create an S3 backend from environment variables.
///
/// Uses `option_env!` instead of `env!` so that `cargo check --tests --all-features`
/// doesn't fail when the R2S3_* environment variables aren't set at compile time.
pub fn open() -> Bucket<Vec<u8>, Vec<u8>> {
    let credentials = Credentials {
        access_key_id: option_env!("R2S3_ACCESS_KEY_ID")
            .expect("R2S3_ACCESS_KEY_ID not set")
            .into(),
        secret_access_key: option_env!("R2S3_SECRET_ACCESS_KEY")
            .expect("R2S3_SECRET_ACCESS_KEY not set")
            .into(),
    };

    let address = Address::new(
        option_env!("R2S3_ENDPOINT").expect("R2S3_ENDPOINT not set"),
        option_env!("R2S3_REGION").expect("R2S3_REGION not set"),
        option_env!("R2S3_BUCKET").expect("R2S3_BUCKET not set"),
    );

    Bucket::open(address, Some(credentials)).expect("Failed to open bucket")
}

pub fn open_unque_at(base: &str) -> Bucket<Vec<u8>, Vec<u8>> {
    open().at(unique_prefix(base))
}

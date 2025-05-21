#![cfg(not(target_arch = "wasm32"))]

use anyhow::Result;
use dialog_remote::s3::{Credentials, SignOptions, sign_url};

#[tokio::main]
pub async fn main() -> Result<()> {
    let access_key_id = std::env::var("ACCESS_KEY_ID")?;
    let secret_access_key = std::env::var("SECRET_ACCESS_KEY")?;
    let bucket_name = std::env::var("BUCKET_NAME")?;

    let credentials = Credentials {
        access_key_id,
        secret_access_key,
        session_token: None,
    };

    let options = SignOptions {
        region: "auto".into(),
        bucket: bucket_name,
        key: Default::default(),
        checksum: None,
        endpoint: None,
        expires: 3600,
        method: "GET".into(),
        public_read: false,
        service: "s3".into(),
        time: None,
    };

    let signed = sign_url(&credentials, &options)?;

    println!("GET: {}", signed);

    Ok(())
}

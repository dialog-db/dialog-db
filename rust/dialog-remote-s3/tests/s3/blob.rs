//! Integration tests for blob (streaming, hash-addressed) operations with S3.

#![cfg(feature = "s3-integration-tests")]

use dialog_common::Blake3Hash;
use dialog_effects::blob::{BlobError, BlobReader, Read};
use dialog_effects::prelude::*;

use super::environment::Environment;

#[cfg(target_arch = "wasm32")]
wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

async fn drain(mut reader: BlobReader) -> Vec<u8> {
    let mut out = Vec::new();
    while let Some(chunk) = reader.next().await.unwrap() {
        out.extend(chunk);
    }
    out
}

#[dialog_common::test]
async fn it_imports_then_reads_a_blob() -> anyhow::Result<()> {
    let env = Environment::open();

    // A per-run nonce keeps the content hash unique on the shared bucket, so
    // parallel runs never collide.
    let mut payload = Environment::unique("blob").into_bytes();
    payload.extend((0..50_000u32).map(|i| (i % 251) as u8));
    let digest = Blake3Hash::hash(&payload);

    // Import: stream the bytes in, get the verified digest back.
    let mut sink = env
        .subject()
        .archive()
        .blob()
        .import(digest.clone(), payload.len() as u64)
        .fork(&env.address)
        .perform(&env.network)
        .await?;
    for chunk in payload.chunks(4096) {
        sink.write_all(chunk).await?;
    }
    assert_eq!(sink.finish().await?, digest);

    // Whole read.
    let reader = env
        .subject()
        .archive()
        .blob()
        .read(digest.clone())
        .fork(&env.address)
        .perform(&env.network)
        .await?;
    assert_eq!(drain(reader).await, payload);

    // Ranged read: 9 bytes from offset 10.
    let reader = env
        .subject()
        .archive()
        .blob()
        .invoke(Read::range(digest, 10, Some(9)))
        .fork(&env.address)
        .perform(&env.network)
        .await?;
    assert_eq!(drain(reader).await, payload[10..19]);

    Ok(())
}

#[dialog_common::test]
async fn it_reports_missing_blobs() -> anyhow::Result<()> {
    let env = Environment::open();
    // The digest of a unique payload that was never uploaded.
    let digest = Blake3Hash::hash(Environment::unique("missing").as_bytes());

    let missing = env
        .subject()
        .archive()
        .blob()
        .read(digest)
        .fork(&env.address)
        .perform(&env.network)
        .await;

    assert!(matches!(missing, Err(BlobError::NotFound(_))));
    Ok(())
}

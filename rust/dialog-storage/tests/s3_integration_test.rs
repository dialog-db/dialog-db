//! Integration tests that run against a real S3/R2/MinIO endpoint.
//!
//! ## Environment Variables
//!
//! These tests require the following environment variables:
//! - R2S3_HOST: The S3-compatible endpoint (e.g., "https://s3.amazonaws.com" or "https://xxx.r2.cloudflarestorage.com")
//! - R2S3_REGION: AWS region (e.g., "us-east-1" or "auto" for R2)
//! - R2S3_BUCKET: Bucket name
//! - R2S3_ACCESS_KEY_ID: Access key ID
//! - R2S3_SECRET_ACCESS_KEY: Secret access key
//!
//! Run these tests with:
//! ```bash
//! R2S3_HOST=https://2fc7ca2f9584223662c5a882977b89ac.r2.cloudflarestorage.com \
//!   R2S3_REGION=auto \
//!   R2S3_BUCKET=dialog-test \
//!   R2S3_ACCESS_KEY_ID=access_key \
//!   R2S3_SECRET_ACCESS_KEY=secret \
//!   cargo test s3_integration_tests --features s3-integration-tests
//! ```

#![cfg(feature = "s3-integration-tests")]

use anyhow::Result;
use async_stream::try_stream;
use dialog_storage::s3::{Credentials, S3, Service, Session, encode_s3_key};
use dialog_storage::{StorageBackend, StorageSink, StorageSource};
use futures_util::TryStreamExt;

#[cfg(target_arch = "wasm32")]
wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

/// Generate a globally unique test prefix using timestamp
fn unique_prefix(base: &str) -> String {
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
fn create_s3_backend_from_env() -> S3<Vec<u8>, Vec<u8>> {
    let credentials = Credentials {
        access_key_id: option_env!("R2S3_ACCESS_KEY_ID")
            .expect("R2S3_ACCESS_KEY_ID not set")
            .into(),
        secret_access_key: option_env!("R2S3_SECRET_ACCESS_KEY")
            .expect("R2S3_SECRET_ACCESS_KEY not set")
            .into(),
        session_token: option_env!("R2S3_SESSION_TOKEN").map(Into::into),
    };

    let region = option_env!("R2S3_REGION").expect("R2S3_REGION not set");
    let service = Service::s3(region);
    let session = Session::new(&credentials, &service, 3600);

    let endpoint = option_env!("R2S3_HOST").expect("R2S3_HOST not set");
    let bucket = option_env!("R2S3_BUCKET").expect("R2S3_BUCKET not set");

    S3::open(endpoint, bucket, session).with_prefix("test-prefix")
}

#[dialog_common::test]
async fn it_sets_and_gets_values() -> Result<()> {
    let mut backend = create_s3_backend_from_env();

    // Test data
    let key = b"test-key-1".to_vec();
    let value = b"test-value-1".to_vec();

    // Set the value
    backend.set(key.clone(), value.clone()).await?;

    // Get the value back
    let retrieved = backend.get(&key).await?;
    assert_eq!(retrieved, Some(value));

    Ok(())
}

#[dialog_common::test]
async fn it_returns_none_for_missing_key() -> Result<()> {
    let backend = create_s3_backend_from_env();

    // Try to get a key that doesn't exist
    let key = b"nonexistent-key-12345".to_vec();
    let retrieved = backend.get(&key).await?;

    assert_eq!(retrieved, None);

    Ok(())
}

#[dialog_common::test]
async fn it_overwrites_values() -> Result<()> {
    let mut backend = create_s3_backend_from_env();

    let key = b"test-key-overwrite".to_vec();
    let value1 = b"original-value".to_vec();
    let value2 = b"updated-value".to_vec();

    // Set initial value
    backend.set(key.clone(), value1.clone()).await?;

    // Verify it was set
    let retrieved = backend.get(&key).await?;
    assert_eq!(retrieved, Some(value1));

    // Overwrite with new value
    backend.set(key.clone(), value2.clone()).await?;

    // Verify it was updated
    let retrieved = backend.get(&key).await?;
    assert_eq!(retrieved, Some(value2));

    Ok(())
}

#[dialog_common::test]
async fn it_handles_large_values() -> Result<()> {
    let mut backend = create_s3_backend_from_env();

    let key = b"test-key-large".to_vec();
    // Create a 1MB value
    let value: Vec<u8> = (0..1_000_000).map(|i| (i % 256) as u8).collect();

    // Set the large value
    backend.set(key.clone(), value.clone()).await?;

    // Get it back and verify
    let retrieved = backend.get(&key).await?;
    assert_eq!(retrieved, Some(value));

    Ok(())
}

#[dialog_common::test]
async fn it_handles_multiple_keys() -> Result<()> {
    let mut backend = create_s3_backend_from_env();

    // Set multiple key-value pairs
    let pairs = vec![
        (b"key1".to_vec(), b"value1".to_vec()),
        (b"key2".to_vec(), b"value2".to_vec()),
        (b"key3".to_vec(), b"value3".to_vec()),
    ];

    for (key, value) in &pairs {
        backend.set(key.clone(), value.clone()).await?;
    }

    // Verify all keys can be retrieved
    for (key, expected_value) in &pairs {
        let retrieved = backend.get(key).await?;
        assert_eq!(retrieved.as_ref(), Some(expected_value));
    }

    Ok(())
}

#[dialog_common::test]
async fn it_handles_binary_data() -> Result<()> {
    let mut backend = create_s3_backend_from_env();

    let key = b"test-key-binary".to_vec();
    // Create binary data with all possible byte values
    let value: Vec<u8> = (0..=255).collect();

    // Set the binary value
    backend.set(key.clone(), value.clone()).await?;

    // Get it back and verify
    let retrieved = backend.get(&key).await?;
    assert_eq!(retrieved, Some(value));

    Ok(())
}

#[dialog_common::test]
async fn it_performs_bulk_operations() -> Result<()> {
    let mut backend = create_s3_backend_from_env();

    // Create a stream of test data

    let test_data = vec![
        (b"bulk1".to_vec(), b"value1".to_vec()),
        (b"bulk2".to_vec(), b"value2".to_vec()),
        (b"bulk3".to_vec(), b"value3".to_vec()),
    ];

    let data_clone = test_data.clone();
    let source_stream = try_stream! {
        for (key, value) in data_clone {
            yield (key, value);
        }
    };

    // Write all data
    backend.write(source_stream).await?;

    // Verify all items were written
    for (key, expected_value) in test_data {
        let retrieved = backend.get(&key).await?;
        assert_eq!(retrieved, Some(expected_value));
    }

    Ok(())
}

/// Helper to create an S3 backend without prefix from environment variables.
fn create_s3_backend_without_prefix_from_env() -> S3<Vec<u8>, Vec<u8>> {
    let credentials = Credentials {
        access_key_id: option_env!("R2S3_ACCESS_KEY_ID")
            .expect("R2S3_ACCESS_KEY_ID not set")
            .into(),
        secret_access_key: option_env!("R2S3_SECRET_ACCESS_KEY")
            .expect("R2S3_SECRET_ACCESS_KEY not set")
            .into(),
        session_token: option_env!("R2S3_SESSION_TOKEN").map(Into::into),
    };

    let region = option_env!("R2S3_REGION").expect("R2S3_REGION not set");
    let service = Service::s3(region);
    let session = Session::new(&credentials, &service, 3600);

    let endpoint = option_env!("R2S3_HOST").expect("R2S3_HOST not set");
    let bucket = option_env!("R2S3_BUCKET").expect("R2S3_BUCKET not set");

    // No prefix - keys go directly into the bucket root
    S3::open(endpoint, bucket, session)
}

#[dialog_common::test]
async fn it_works_without_prefix() -> Result<()> {
    let mut backend = create_s3_backend_without_prefix_from_env();

    // Test data - use unique key to avoid conflicts
    let key = b"no-prefix-test-key".to_vec();
    let value = b"no-prefix-test-value".to_vec();

    // Set the value
    backend.set(key.clone(), value.clone()).await?;

    // Get the value back
    let retrieved = backend.get(&key).await?;
    assert_eq!(retrieved, Some(value));

    Ok(())
}

#[dialog_common::test]
async fn it_handles_encoded_key_segments() -> Result<()> {
    let mut backend = create_s3_backend_from_env();

    // Test key with path structure where one segment is safe and another needs encoding
    // "safe-segment/user@example.com" - first segment is safe, second has @ which is unsafe
    let key_mixed = b"safe-segment/user@example.com".to_vec();
    let value_mixed = b"value-for-mixed-key".to_vec();

    // Verify encoding behavior
    let encoded = encode_s3_key(&key_mixed);
    // Should be "safe-segment/!<base58>" where first part is unchanged and second is encoded
    assert!(
        encoded.starts_with("safe-segment/!"),
        "First segment should be safe, second should be encoded with ! prefix: {}",
        encoded
    );

    // Write and read back
    backend.set(key_mixed.clone(), value_mixed.clone()).await?;
    let retrieved = backend.get(&key_mixed).await?;
    assert_eq!(retrieved, Some(value_mixed));

    Ok(())
}

#[dialog_common::test]
async fn it_handles_fully_encoded_key() -> Result<()> {
    let mut backend = create_s3_backend_from_env();

    // Test key that is fully binary (all segments need encoding)
    let key_binary = vec![0x01, 0x02, 0xFF, 0xFE];
    let value_binary = b"value-for-binary-key".to_vec();

    // Verify encoding behavior - binary data should be encoded
    let encoded = encode_s3_key(&key_binary);
    assert!(
        encoded.starts_with('!'),
        "Binary key should be encoded with ! prefix: {}",
        encoded
    );

    // Write and read back
    backend
        .set(key_binary.clone(), value_binary.clone())
        .await?;
    let retrieved = backend.get(&key_binary).await?;
    assert_eq!(retrieved, Some(value_binary));

    Ok(())
}

#[dialog_common::test]
async fn it_handles_multi_segment_mixed_encoding() -> Result<()> {
    let mut backend = create_s3_backend_from_env();

    // Test key with multiple segments: safe/unsafe/safe/unsafe pattern
    // "data/file name with spaces/v1/special!chars"
    let key = b"data/file name with spaces/v1/special!chars".to_vec();
    let value = b"value-for-complex-path".to_vec();

    // Verify encoding behavior
    let encoded = encode_s3_key(&key);
    let segments: Vec<&str> = encoded.split('/').collect();
    assert_eq!(segments.len(), 4, "Should have 4 segments");
    assert_eq!(segments[0], "data", "First segment should be safe");
    assert!(
        segments[1].starts_with('!'),
        "Second segment should be encoded (has spaces)"
    );
    assert_eq!(segments[2], "v1", "Third segment should be safe");
    assert!(
        segments[3].starts_with('!'),
        "Fourth segment should be encoded (has !)"
    );

    // Write and read back
    backend.set(key.clone(), value.clone()).await?;
    let retrieved = backend.get(&key).await?;
    assert_eq!(retrieved, Some(value));

    Ok(())
}

#[dialog_common::test]
async fn it_handles_encoded_key_without_prefix() -> Result<()> {
    let mut backend = create_s3_backend_without_prefix_from_env();

    // Test encoded key without prefix
    let key = b"path/with spaces/data".to_vec();
    let value = b"value-for-encoded-no-prefix".to_vec();

    // Verify encoding
    let encoded = encode_s3_key(&key);
    let segments: Vec<&str> = encoded.split('/').collect();
    assert_eq!(segments[0], "path", "First segment should be safe");
    assert!(
        segments[1].starts_with('!'),
        "Second segment should be encoded"
    );
    assert_eq!(segments[2], "data", "Third segment should be safe");

    // Write and read back without prefix
    backend.set(key.clone(), value.clone()).await?;
    let retrieved = backend.get(&key).await?;
    assert_eq!(retrieved, Some(value));

    Ok(())
}

#[dialog_common::test]
async fn it_deletes_values() -> Result<()> {
    let mut backend = create_s3_backend_from_env();

    let key = b"delete-integration-test".to_vec();
    let value = b"value-to-delete".to_vec();

    // Set the value
    backend.set(key.clone(), value.clone()).await?;

    // Verify it exists
    let retrieved = backend.get(&key).await?;
    assert_eq!(retrieved, Some(value));

    // Delete it
    backend.delete(&key).await?;

    // Verify it's gone
    let retrieved = backend.get(&key).await?;
    assert_eq!(retrieved, None);

    Ok(())
}

#[dialog_common::test]
async fn it_lists_objects() -> Result<()> {
    // Use a unique prefix for this test
    let test_prefix = unique_prefix("list-test");

    // Create a backend with the unique prefix
    let credentials = Credentials {
        access_key_id: option_env!("R2S3_ACCESS_KEY_ID")
            .expect("R2S3_ACCESS_KEY_ID not set")
            .into(),
        secret_access_key: option_env!("R2S3_SECRET_ACCESS_KEY")
            .expect("R2S3_SECRET_ACCESS_KEY not set")
            .into(),
        session_token: option_env!("R2S3_SESSION_TOKEN").map(Into::into),
    };
    let region = option_env!("R2S3_REGION").expect("R2S3_REGION not set");
    let service = Service::s3(region);
    let session = Session::new(&credentials, &service, 3600);
    let endpoint = option_env!("R2S3_HOST").expect("R2S3_HOST not set");
    let bucket = option_env!("R2S3_BUCKET").expect("R2S3_BUCKET not set");

    let mut backend = S3::open(endpoint, bucket, session).with_prefix(&test_prefix);

    // Set a few values
    backend
        .set(b"list-key1".to_vec(), b"value1".to_vec())
        .await?;
    backend
        .set(b"list-key2".to_vec(), b"value2".to_vec())
        .await?;

    // List objects
    let result = backend.list(None).await?;

    // Should have at least 2 keys (may have more if other tests ran)
    assert!(
        result.keys.len() >= 2,
        "Expected at least 2 keys, got {}",
        result.keys.len()
    );

    // All keys should have our prefix
    for key in &result.keys {
        assert!(
            key.starts_with(&test_prefix),
            "Key {} should start with prefix {}",
            key,
            test_prefix
        );
    }

    Ok(())
}

#[dialog_common::test]
async fn it_reads_stream() -> Result<()> {
    // Use a unique prefix for this test
    let test_prefix = unique_prefix("stream-test");

    let credentials = Credentials {
        access_key_id: option_env!("R2S3_ACCESS_KEY_ID")
            .expect("R2S3_ACCESS_KEY_ID not set")
            .into(),
        secret_access_key: option_env!("R2S3_SECRET_ACCESS_KEY")
            .expect("R2S3_SECRET_ACCESS_KEY not set")
            .into(),
        session_token: option_env!("R2S3_SESSION_TOKEN").map(Into::into),
    };
    let region = option_env!("R2S3_REGION").expect("R2S3_REGION not set");
    let service = Service::s3(region);
    let session = Session::new(&credentials, &service, 3600);
    let endpoint = option_env!("R2S3_HOST").expect("R2S3_HOST not set");
    let bucket = option_env!("R2S3_BUCKET").expect("R2S3_BUCKET not set");

    let mut backend = S3::open(endpoint, bucket, session).with_prefix(&test_prefix);

    // Set a few values
    backend
        .set(b"stream-a".to_vec(), b"value-a".to_vec())
        .await?;
    backend
        .set(b"stream-b".to_vec(), b"value-b".to_vec())
        .await?;

    // Read all items via StorageSource
    let mut items: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
    let mut stream = Box::pin(backend.read());

    while let Some((key, value)) = stream.try_next().await? {
        items.push((key, value));
    }

    assert_eq!(items.len(), 2);

    // Verify the items (order may vary)
    let keys: Vec<&[u8]> = items.iter().map(|(k, _)| k.as_slice()).collect();
    assert!(keys.contains(&b"stream-a".as_slice()));
    assert!(keys.contains(&b"stream-b".as_slice()));

    Ok(())
}

/// Test that listing with a nonexistent prefix returns an empty list (not an error).
///
/// This verifies real S3/R2 behavior: a prefix is just a filter, not a path that must exist.
#[dialog_common::test]
async fn it_lists_empty_for_nonexistent_prefix() -> Result<()> {
    let credentials = Credentials {
        access_key_id: option_env!("R2S3_ACCESS_KEY_ID")
            .expect("R2S3_ACCESS_KEY_ID not set")
            .into(),
        secret_access_key: option_env!("R2S3_SECRET_ACCESS_KEY")
            .expect("R2S3_SECRET_ACCESS_KEY not set")
            .into(),
        session_token: option_env!("R2S3_SESSION_TOKEN").map(Into::into),
    };

    let region = option_env!("R2S3_REGION").expect("R2S3_REGION not set");
    let service = Service::s3(region);
    let session = Session::new(&credentials, &service, 3600);

    let endpoint = option_env!("R2S3_HOST").expect("R2S3_HOST not set");
    let bucket = option_env!("R2S3_BUCKET").expect("R2S3_BUCKET not set");

    // Use a prefix that definitely doesn't exist
    let backend = S3::<Vec<u8>, Vec<u8>>::open(endpoint, bucket, session)
        .with_prefix("nonexistent-prefix-that-should-not-exist-12345");

    // Listing should return empty result, not an error
    let result = backend.list(None).await?;

    assert!(result.keys.is_empty());
    assert!(!result.is_truncated);
    assert!(result.next_continuation_token.is_none());

    Ok(())
}

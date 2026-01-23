//! Integration tests that run against a real S3/R2/MinIO endpoint.
//!
//! ## Environment Variables
//!
//! These tests require the following environment variables:
//! - R2S3_ENDPOINT: The S3-compatible endpoint (e.g., "https://s3.amazonaws.com" or "https://xxx.r2.cloudflarestorage.com")
//! - R2S3_REGION: AWS region (e.g., "us-east-1" or "auto" for R2)
//! - R2S3_BUCKET: Bucket name
//! - R2S3_ACCESS_KEY_ID: Access key ID
//! - R2S3_SECRET_ACCESS_KEY: Secret access key
//!
//! Run these tests with:
//! ```bash
//! R2S3_ENDPOINT=https://2fc7ca2f9584223662c5a882977b89ac.r2.cloudflarestorage.com \
//!   R2S3_REGION=auto \
//!   R2S3_BUCKET=dialog-test \
//!   R2S3_ACCESS_KEY_ID=access_key \
//!   R2S3_SECRET_ACCESS_KEY=secret \
//!   cargo test s3_integration_test --features s3-integration-tests
//! ```

#![cfg(feature = "s3-integration-tests")]

use super::bucket;
use anyhow::Result;
use dialog_storage::s3::encode_s3_key;
use dialog_storage::StorageBackend;

#[cfg(target_arch = "wasm32")]
wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

#[dialog_common::test]
async fn it_sets_and_gets_values() -> Result<()> {
    let mut backend = bucket::open_unique_at("it_sets_and_gets_values");

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
    let backend = bucket::open_unique_at("it_returns_none_for_missing_key");

    // Try to get a key that doesn't exist
    let key = b"nonexistent-key-12345".to_vec();
    let retrieved = backend.get(&key).await?;

    assert_eq!(retrieved, None);

    Ok(())
}

#[dialog_common::test]
async fn it_overwrites_values() -> Result<()> {
    let mut backend = bucket::open_unique_at("it_overwrites_values");

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
    let mut backend = bucket::open_unique_at("it_handles_large_values");

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
    let mut backend = bucket::open_unique_at("it_handles_multiple_keys");

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
    let mut backend = bucket::open_unique_at("it_handles_binary_data");

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
    let mut backend = bucket::open_unique_at("it_performs_bulk_operations");

    // Create test data
    let test_data = vec![
        (b"bulk1".to_vec(), b"value1".to_vec()),
        (b"bulk2".to_vec(), b"value2".to_vec()),
        (b"bulk3".to_vec(), b"value3".to_vec()),
    ];

    // Write all data using set
    for (key, value) in &test_data {
        backend.set(key.clone(), value.clone()).await?;
    }

    // Verify all items were written
    for (key, expected_value) in test_data {
        let retrieved = backend.get(&key).await?;
        assert_eq!(retrieved, Some(expected_value));
    }

    Ok(())
}

#[dialog_common::test]
async fn it_works_without_prefix() -> Result<()> {
    let mut backend = bucket::open();

    // Test data - use unique key to avoid conflicts
    let key = bucket::unique("no-prefix-test-key").into_bytes();
    let value = bucket::unique("no-prefix-test-value").into_bytes();

    // Set the value
    backend.set(key.clone(), value.clone()).await?;

    // Get the value back
    let retrieved = backend.get(&key).await?;
    assert_eq!(retrieved, Some(value));

    Ok(())
}

#[dialog_common::test]
async fn it_handles_encoded_key_segments() -> Result<()> {
    let mut backend = bucket::open_unique_at("it_handles_encoded_key_segments");

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
    let mut backend = bucket::open_unique_at("it_handles_fully_encoded_key");

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
    let mut backend = bucket::open_unique_at("it_handles_multi_segment_mixed_encoding");

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
    let mut backend = bucket::open();

    // Test encoded key without prefix

    let name = bucket::unique("data");
    let key = format!("path/with spaces/{name}",).into_bytes();
    let value = b"value-for-encoded-no-prefix".to_vec();

    // Verify encoding
    let encoded = encode_s3_key(&key);
    let segments: Vec<&str> = encoded.split('/').collect();
    assert_eq!(segments[0], "path", "First segment should be safe");
    assert!(
        segments[1].starts_with('!'),
        "Second segment should be encoded"
    );
    assert_eq!(segments[2], name, "Third segment should be safe");

    // Write and read back without prefix
    backend.set(key.clone(), value.clone()).await?;
    let retrieved = backend.get(&key).await?;
    assert_eq!(retrieved, Some(value));

    Ok(())
}

#[dialog_common::test]
async fn it_deletes_values() -> Result<()> {
    let mut backend = bucket::open_unique_at("it_deletes_values");

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

#[cfg(feature = "s3-list")]
#[dialog_common::test]
async fn it_lists_objects() -> Result<()> {
    // Use a unique prefix for this test
    let test_prefix = bucket::unique("it_lists_objects");

    let mut backend = bucket::open().at(&test_prefix);

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

    Ok(())
}

/// Test that listing with a nonexistent prefix returns an empty list (not an error).
///
/// This verifies real S3/R2 behavior: a prefix is just a filter, not a path that must exist.
#[cfg(feature = "s3-list")]
#[dialog_common::test]
async fn it_lists_empty_for_nonexistent_prefix() -> Result<()> {
    let backend = bucket::open_unique_at("it_lists_empty_for_nonexistent_prefix");

    // Listing should return empty result, not an error
    let result = backend.list(None).await?;

    assert!(result.keys.is_empty());
    assert!(!result.is_truncated);
    assert!(result.next_continuation_token.is_none());

    Ok(())
}

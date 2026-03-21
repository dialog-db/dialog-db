//! Integration tests that run against a real S3/R2/MinIO endpoint.

#![cfg(feature = "integration-tests")]

use super::bucket;
use anyhow::Result;
use dialog_remote_s3::encode_s3_key;

#[cfg(target_arch = "wasm32")]
wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

#[dialog_common::test]
async fn it_sets_and_gets_values() -> Result<()> {
    let backend = bucket::open_unique_at("it_sets_and_gets_values");

    let key = b"test-key-1".to_vec();
    let value = b"test-value-1".to_vec();

    backend.set(key.clone(), value.clone()).await?;

    let retrieved = backend.get(&key).await?;
    assert_eq!(retrieved, Some(value));

    Ok(())
}

#[dialog_common::test]
async fn it_returns_none_for_missing_key() -> Result<()> {
    let backend = bucket::open_unique_at("it_returns_none_for_missing_key");

    let key = b"nonexistent-key-12345".to_vec();
    let retrieved = backend.get(&key).await?;

    assert_eq!(retrieved, None);

    Ok(())
}

#[dialog_common::test]
async fn it_overwrites_values() -> Result<()> {
    let backend = bucket::open_unique_at("it_overwrites_values");

    let key = b"test-key-overwrite".to_vec();
    let value1 = b"original-value".to_vec();
    let value2 = b"updated-value".to_vec();

    backend.set(key.clone(), value1.clone()).await?;

    let retrieved = backend.get(&key).await?;
    assert_eq!(retrieved, Some(value1));

    backend.set(key.clone(), value2.clone()).await?;

    let retrieved = backend.get(&key).await?;
    assert_eq!(retrieved, Some(value2));

    Ok(())
}

#[dialog_common::test]
async fn it_handles_large_values() -> Result<()> {
    let backend = bucket::open_unique_at("it_handles_large_values");

    let key = b"test-key-large".to_vec();
    let value: Vec<u8> = (0..1_000_000).map(|i| (i % 256) as u8).collect();

    backend.set(key.clone(), value.clone()).await?;

    let retrieved = backend.get(&key).await?;
    assert_eq!(retrieved, Some(value));

    Ok(())
}

#[dialog_common::test]
async fn it_handles_multiple_keys() -> Result<()> {
    let backend = bucket::open_unique_at("it_handles_multiple_keys");

    let pairs = vec![
        (b"key1".to_vec(), b"value1".to_vec()),
        (b"key2".to_vec(), b"value2".to_vec()),
        (b"key3".to_vec(), b"value3".to_vec()),
    ];

    for (key, value) in &pairs {
        backend.set(key.clone(), value.clone()).await?;
    }

    for (key, expected_value) in &pairs {
        let retrieved = backend.get(key).await?;
        assert_eq!(retrieved.as_ref(), Some(expected_value));
    }

    Ok(())
}

#[dialog_common::test]
async fn it_handles_binary_data() -> Result<()> {
    let backend = bucket::open_unique_at("it_handles_binary_data");

    let key = b"test-key-binary".to_vec();
    let value: Vec<u8> = (0..=255).collect();

    backend.set(key.clone(), value.clone()).await?;

    let retrieved = backend.get(&key).await?;
    assert_eq!(retrieved, Some(value));

    Ok(())
}

#[dialog_common::test]
async fn it_performs_bulk_operations() -> Result<()> {
    let backend = bucket::open_unique_at("it_performs_bulk_operations");

    let test_data = vec![
        (b"bulk1".to_vec(), b"value1".to_vec()),
        (b"bulk2".to_vec(), b"value2".to_vec()),
        (b"bulk3".to_vec(), b"value3".to_vec()),
    ];

    for (key, value) in &test_data {
        backend.set(key.clone(), value.clone()).await?;
    }

    for (key, expected_value) in test_data {
        let retrieved = backend.get(&key).await?;
        assert_eq!(retrieved, Some(expected_value));
    }

    Ok(())
}

#[dialog_common::test]
async fn it_works_without_prefix() -> Result<()> {
    let backend = bucket::open();

    let key = bucket::unique("no-prefix-test-key").into_bytes();
    let value = bucket::unique("no-prefix-test-value").into_bytes();

    backend.set(key.clone(), value.clone()).await?;

    let retrieved = backend.get(&key).await?;
    assert_eq!(retrieved, Some(value));

    Ok(())
}

#[dialog_common::test]
async fn it_handles_encoded_key_segments() -> Result<()> {
    let backend = bucket::open_unique_at("it_handles_encoded_key_segments");

    let key_mixed = b"safe-segment/user@example.com".to_vec();
    let value_mixed = b"value-for-mixed-key".to_vec();

    let encoded = encode_s3_key(&key_mixed);
    assert!(
        encoded.starts_with("safe-segment/!"),
        "First segment should be safe, second should be encoded with ! prefix: {}",
        encoded
    );

    backend.set(key_mixed.clone(), value_mixed.clone()).await?;
    let retrieved = backend.get(&key_mixed).await?;
    assert_eq!(retrieved, Some(value_mixed));

    Ok(())
}

#[dialog_common::test]
async fn it_handles_fully_encoded_key() -> Result<()> {
    let backend = bucket::open_unique_at("it_handles_fully_encoded_key");

    let key_binary = vec![0x01, 0x02, 0xFF, 0xFE];
    let value_binary = b"value-for-binary-key".to_vec();

    let encoded = encode_s3_key(&key_binary);
    assert!(
        encoded.starts_with('!'),
        "Binary key should be encoded with ! prefix: {}",
        encoded
    );

    backend
        .set(key_binary.clone(), value_binary.clone())
        .await?;
    let retrieved = backend.get(&key_binary).await?;
    assert_eq!(retrieved, Some(value_binary));

    Ok(())
}

#[dialog_common::test]
async fn it_handles_multi_segment_mixed_encoding() -> Result<()> {
    let backend = bucket::open_unique_at("it_handles_multi_segment_mixed_encoding");

    let key = b"data/file name with spaces/v1/special!chars".to_vec();
    let value = b"value-for-complex-path".to_vec();

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

    backend.set(key.clone(), value.clone()).await?;
    let retrieved = backend.get(&key).await?;
    assert_eq!(retrieved, Some(value));

    Ok(())
}

#[dialog_common::test]
async fn it_handles_encoded_key_without_prefix() -> Result<()> {
    let backend = bucket::open();

    let name = bucket::unique("data");
    let key = format!("path/with spaces/{name}",).into_bytes();
    let value = b"value-for-encoded-no-prefix".to_vec();

    let encoded = encode_s3_key(&key);
    let segments: Vec<&str> = encoded.split('/').collect();
    assert_eq!(segments[0], "path", "First segment should be safe");
    assert!(
        segments[1].starts_with('!'),
        "Second segment should be encoded"
    );
    assert_eq!(segments[2], name, "Third segment should be safe");

    backend.set(key.clone(), value.clone()).await?;
    let retrieved = backend.get(&key).await?;
    assert_eq!(retrieved, Some(value));

    Ok(())
}

#[dialog_common::test]
async fn it_deletes_values() -> Result<()> {
    let backend = bucket::open_unique_at("it_deletes_values");

    let key = b"delete-integration-test".to_vec();
    let value = b"value-to-delete".to_vec();

    backend.set(key.clone(), value.clone()).await?;

    let retrieved = backend.get(&key).await?;
    assert_eq!(retrieved, Some(value));

    backend.delete(&key).await?;

    let retrieved = backend.get(&key).await?;
    assert_eq!(retrieved, None);

    Ok(())
}

#[cfg(feature = "list")]
#[dialog_common::test]
async fn it_lists_objects() -> Result<()> {
    let test_prefix = bucket::unique("it_lists_objects");

    let backend = bucket::open().at(&test_prefix);

    backend
        .set(b"list-key1".to_vec(), b"value1".to_vec())
        .await?;
    backend
        .set(b"list-key2".to_vec(), b"value2".to_vec())
        .await?;

    // TODO: list not yet implemented via capability pattern

    Ok(())
}

#[cfg(feature = "list")]
#[dialog_common::test]
async fn it_lists_empty_for_nonexistent_prefix() -> Result<()> {
    let _backend = bucket::open_unique_at("it_lists_empty_for_nonexistent_prefix");

    // TODO: list not yet implemented via capability pattern

    Ok(())
}

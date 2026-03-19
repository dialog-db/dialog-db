//! Integration tests for memory CAS (Compare-And-Swap) operations with S3 backend.
//!
//! These tests verify that memory resolve/publish/retract work correctly with the S3 backend,
//! including CAS semantics and conflict detection.
//!
//! Run with: `cargo test -p dialog-storage --features s3-integration-tests --test s3_integration_tests`

#![cfg(feature = "s3-integration-tests")]

use super::bucket;
use serde::{Deserialize, Serialize};

#[cfg(target_arch = "wasm32")]
wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct TestData {
    name: String,
    value: u32,
}

fn encode(data: &TestData) -> Vec<u8> {
    serde_json::to_vec(data).unwrap()
}

fn decode(bytes: &[u8]) -> TestData {
    serde_json::from_slice(bytes).unwrap()
}

#[dialog_common::test]
async fn it_resolves_non_existent_cell() -> anyhow::Result<()> {
    let backend = bucket::open_unique_at("it_resolves_non_existent_cell");

    let result = backend.resolve("did:key:zUser", "test-key").await?;
    assert!(result.is_none());

    Ok(())
}

#[dialog_common::test]
async fn it_publishes_and_resolves_value() -> anyhow::Result<()> {
    let backend = bucket::open_unique_at("it_publishes_and_resolves_value");

    let data = TestData {
        name: "test".to_string(),
        value: 42,
    };

    let space = "did:key:zSpace";
    let cell = &bucket::unique("test-key-rw");

    // Publish (first write, no prior edition)
    let edition = backend.publish(space, cell, encode(&data), None).await?;

    // Resolve and verify
    let publication = backend.resolve(space, cell).await?;
    assert!(publication.is_some());
    let publication = publication.unwrap();
    assert_eq!(decode(&publication.content), data);
    assert_eq!(publication.edition, edition);

    Ok(())
}

#[dialog_common::test]
async fn it_updates_existing_value() -> anyhow::Result<()> {
    let backend = bucket::open_unique_at("it_updates_existing_value");

    let space = "did:key:zSpace";
    let cell = &bucket::unique("test-update-key");

    let initial_data = TestData {
        name: "initial".to_string(),
        value: 1,
    };

    let edition1 = backend
        .publish(space, cell, encode(&initial_data), None)
        .await?;

    let updated_data = TestData {
        name: "updated".to_string(),
        value: 2,
    };

    let edition2 = backend
        .publish(space, cell, encode(&updated_data), Some(edition1))
        .await?;

    // Resolve and verify the update
    let publication = backend.resolve(space, cell).await?;
    assert!(publication.is_some());
    let publication = publication.unwrap();
    assert_eq!(decode(&publication.content), updated_data);
    assert_eq!(publication.edition, edition2);

    Ok(())
}

#[dialog_common::test]
async fn it_detects_cas_conflict() -> anyhow::Result<()> {
    let backend = bucket::open_unique_at("it_detects_cas_conflict");

    let space = "did:key:zSpace";
    let cell = &bucket::unique("test-cas-key");

    let initial_data = TestData {
        name: "initial".to_string(),
        value: 1,
    };

    // Publish initial value
    let edition1 = backend
        .publish(space, cell, encode(&initial_data), None)
        .await?;

    // Update with correct edition (simulating cell1)
    let updated_by_cell1 = TestData {
        name: "updated_by_cell1".to_string(),
        value: 10,
    };
    let _edition2 = backend
        .publish(
            space,
            cell,
            encode(&updated_by_cell1),
            Some(edition1.clone()),
        )
        .await?;

    // Try to update with stale edition (simulating cell2 with old edition)
    let updated_by_cell2 = TestData {
        name: "updated_by_cell2".to_string(),
        value: 20,
    };
    let result = backend
        .publish(space, cell, encode(&updated_by_cell2), Some(edition1))
        .await;

    assert!(result.is_err(), "CAS should fail due to edition mismatch");

    // Verify the value is still what cell1 set
    let publication = backend.resolve(space, cell).await?;
    assert!(publication.is_some());
    assert_eq!(decode(&publication.unwrap().content), updated_by_cell1);

    Ok(())
}

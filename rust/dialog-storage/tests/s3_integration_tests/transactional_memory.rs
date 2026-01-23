//! Integration tests for TransactionalMemory with S3 backend using local mock server.
//!
//! These tests verify that TransactionalMemory works correctly with the S3 backend,
//! including CAS (Compare-And-Swap) semantics and conflict detection.
//!
//! Run with: `cargo test -p dialog-storage --features s3-integration-tests --test s3_integration_tests`

#![cfg(feature = "s3-integration-tests")]

use super::bucket;
use dialog_storage::TransactionalMemory;
use serde::{Deserialize, Serialize};

#[cfg(target_arch = "wasm32")]
wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct TestData {
    name: String,
    value: u32,
}

#[dialog_common::test]
async fn it_opens_non_existent_memory() -> anyhow::Result<()> {
    let mut backend = bucket::open_unique_at("it_opens_non_existent_memory");
    let memory: TransactionalMemory<TestData, _> = TransactionalMemory::new();

    let cell = memory.open(b"test-key".to_vec(), &mut backend).await?;

    assert!(cell.read().is_none());
    Ok(())
}

#[dialog_common::test]
async fn it_writes_and_reads_value() -> anyhow::Result<()> {
    let mut backend = bucket::open_unique_at("it_writes_and_reads_value");
    let memory: TransactionalMemory<TestData, _> = TransactionalMemory::new();

    let cell = memory.open(b"test-key-rw".to_vec(), &mut backend).await?;

    let data = TestData {
        name: "test".to_string(),
        value: 42,
    };

    cell.replace(Some(data.clone()), &mut backend).await?;

    assert_eq!(cell.read(), Some(data.clone()));

    // Open again to verify persistence
    let cell2 = memory.open(b"test-key-rw".to_vec(), &mut backend).await?;
    assert_eq!(cell2.read(), Some(data));
    Ok(())
}

#[dialog_common::test]
async fn it_updates_existing_value() -> anyhow::Result<()> {
    let mut backend = bucket::open_unique_at("it_updates_existing_value");
    let memory: TransactionalMemory<TestData, _> = TransactionalMemory::new();

    let cell = memory.open(b"test-update-key".to_vec(), &mut backend).await?;

    let initial_data = TestData {
        name: "initial".to_string(),
        value: 1,
    };

    cell.replace(Some(initial_data), &mut backend).await?;

    let updated_data = TestData {
        name: "updated".to_string(),
        value: 2,
    };

    cell.replace(Some(updated_data.clone()), &mut backend).await?;

    assert_eq!(cell.read(), Some(updated_data.clone()));

    // Open again to verify the update persisted
    let cell2 = memory.open(b"test-update-key".to_vec(), &mut backend).await?;
    assert_eq!(cell2.read(), Some(updated_data));
    Ok(())
}

#[dialog_common::test]
async fn it_detects_cas_conflict() -> anyhow::Result<()> {
    let mut backend = bucket::open_unique_at("it_detects_cas_conflict");
    let memory1: TransactionalMemory<TestData, _> = TransactionalMemory::new();
    let memory2: TransactionalMemory<TestData, _> = TransactionalMemory::new();

    // Create initial value with cell1
    let cell1 = memory1.open(b"test-cas-key".to_vec(), &mut backend).await?;

    let initial_data = TestData {
        name: "initial".to_string(),
        value: 1,
    };

    cell1.replace(Some(initial_data.clone()), &mut backend).await?;

    // Open cell2 from different memory - gets the current state
    let cell2 = memory2.open(b"test-cas-key".to_vec(), &mut backend).await?;

    // cell1 updates the value
    let updated_by_cell1 = TestData {
        name: "updated_by_cell1".to_string(),
        value: 10,
    };
    cell1
        .replace(Some(updated_by_cell1.clone()), &mut backend)
        .await?;

    // cell2 tries to update with stale edition - should fail
    let updated_by_cell2 = TestData {
        name: "updated_by_cell2".to_string(),
        value: 20,
    };
    let result = cell2.replace(Some(updated_by_cell2), &mut backend).await;

    assert!(result.is_err(), "CAS should fail due to edition mismatch");

    // Verify the value is still what cell1 set
    let cell3 = memory1.open(b"test-cas-key".to_vec(), &mut backend).await?;
    assert_eq!(cell3.read(), Some(updated_by_cell1));

    Ok(())
}

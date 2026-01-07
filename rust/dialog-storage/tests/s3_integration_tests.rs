//! Integration tests for TransactionalMemory with S3 backend using local mock server.
//!
//! These tests verify that TransactionalMemory works correctly with the S3 backend,
//! including CAS (Compare-And-Swap) semantics and conflict detection.
//!
//! Run with: `cargo test -p dialog-storage --features s3,helpers,integration-tests`

#![cfg(all(feature = "s3", feature = "helpers", feature = "integration-tests"))]

use dialog_storage::TransactionalMemory;
use dialog_storage::s3::{Address, Bucket, PublicS3Address};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct TestData {
    name: String,
    value: u32,
}

#[dialog_common::test]
async fn it_opens_non_existent_memory(env: PublicS3Address) -> anyhow::Result<()> {
    let address = Address::new(&env.endpoint, "us-east-1", &env.bucket);
    let backend = Bucket::<Vec<u8>, Vec<u8>>::open(address, None)?;
    let memory: TransactionalMemory<TestData, _> = TransactionalMemory::new();

    let cell = memory.open(b"test-key".to_vec(), &backend).await?;

    assert!(cell.read().is_none());
    Ok(())
}

#[dialog_common::test]
async fn it_writes_and_reads_value(env: PublicS3Address) -> anyhow::Result<()> {
    let address = Address::new(&env.endpoint, "us-east-1", &env.bucket);
    let backend = Bucket::<Vec<u8>, Vec<u8>>::open(address, None)?;
    let memory: TransactionalMemory<TestData, _> = TransactionalMemory::new();

    let cell = memory.open(b"test-key".to_vec(), &backend).await?;

    let data = TestData {
        name: "test".to_string(),
        value: 42,
    };

    cell.replace(Some(data.clone()), &backend).await?;

    assert_eq!(cell.read(), Some(data.clone()));

    // Open again to verify persistence
    let cell2 = memory.open(b"test-key".to_vec(), &backend).await?;
    assert_eq!(cell2.read(), Some(data));
    Ok(())
}

#[dialog_common::test]
async fn it_updates_existing_value(env: PublicS3Address) -> anyhow::Result<()> {
    let address = Address::new(&env.endpoint, "us-east-1", &env.bucket);
    let backend = Bucket::<Vec<u8>, Vec<u8>>::open(address, None)?;
    let memory: TransactionalMemory<TestData, _> = TransactionalMemory::new();

    let cell = memory.open(b"test-update-key".to_vec(), &backend).await?;

    let initial_data = TestData {
        name: "initial".to_string(),
        value: 1,
    };

    cell.replace(Some(initial_data), &backend).await?;

    let updated_data = TestData {
        name: "updated".to_string(),
        value: 2,
    };

    cell.replace(Some(updated_data.clone()), &backend).await?;

    assert_eq!(cell.read(), Some(updated_data.clone()));

    // Open again to verify the update persisted
    let cell2 = memory.open(b"test-update-key".to_vec(), &backend).await?;
    assert_eq!(cell2.read(), Some(updated_data));
    Ok(())
}

#[dialog_common::test]
async fn it_detects_cas_conflict(env: PublicS3Address) -> anyhow::Result<()> {
    let address = Address::new(&env.endpoint, "us-east-1", &env.bucket);
    let backend = Bucket::<Vec<u8>, Vec<u8>>::open(address, None)?;
    let memory1: TransactionalMemory<TestData, _> = TransactionalMemory::new();
    let memory2: TransactionalMemory<TestData, _> = TransactionalMemory::new();

    // Create initial value with cell1
    let cell1 = memory1.open(b"test-cas-key".to_vec(), &backend).await?;

    let initial_data = TestData {
        name: "initial".to_string(),
        value: 1,
    };

    cell1.replace(Some(initial_data.clone()), &backend).await?;

    // Open cell2 from different memory - gets the current state
    let cell2 = memory2.open(b"test-cas-key".to_vec(), &backend).await?;

    // cell1 updates the value
    let updated_by_cell1 = TestData {
        name: "updated_by_cell1".to_string(),
        value: 10,
    };
    cell1
        .replace(Some(updated_by_cell1.clone()), &backend)
        .await?;

    // cell2 tries to update with stale edition - should fail
    let updated_by_cell2 = TestData {
        name: "updated_by_cell2".to_string(),
        value: 20,
    };
    let result = cell2.replace(Some(updated_by_cell2), &backend).await;

    assert!(result.is_err(), "CAS should fail due to edition mismatch");

    // Verify the value is still what cell1 set
    let cell3 = memory1.open(b"test-cas-key".to_vec(), &backend).await?;
    assert_eq!(cell3.read(), Some(updated_by_cell1));

    Ok(())
}

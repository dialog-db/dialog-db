//! Integration tests for memory CAS operations with S3 backend.

#![cfg(feature = "s3-integration-tests")]

use dialog_effects::memory::prelude::*;
use serde::{Deserialize, Serialize};

use super::environment::Environment;

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
    let env = Environment::open();
    let space = &Environment::unique("resolve-none");

    let result = env
        .subject()
        .memory()
        .space(space)
        .cell("test-key")
        .resolve()
        .fork(&env.address)
        .perform(&env.network)
        .await?;

    assert!(result.is_none());
    Ok(())
}

#[dialog_common::test]
async fn it_publishes_and_resolves_value() -> anyhow::Result<()> {
    let env = Environment::open();
    let space = &Environment::unique("publish-resolve");
    let cell_name = &Environment::unique("test-key-rw");
    let cell = env.subject().memory().space(space).cell(cell_name);

    let data = TestData {
        name: "test".to_string(),
        value: 42,
    };

    let edition = cell
        .clone()
        .publish(encode(&data), None)
        .fork(&env.address)
        .perform(&env.network)
        .await?;

    let publication = cell
        .resolve()
        .fork(&env.address)
        .perform(&env.network)
        .await?;

    assert!(publication.is_some());
    let publication = publication.unwrap();
    assert_eq!(decode(&publication.content), data);
    assert_eq!(publication.edition, edition);

    Ok(())
}

#[dialog_common::test]
async fn it_updates_existing_value() -> anyhow::Result<()> {
    let env = Environment::open();
    let space = &Environment::unique("update");
    let cell_name = &Environment::unique("test-update-key");
    let cell = env.subject().memory().space(space).cell(cell_name);

    let initial = TestData {
        name: "initial".to_string(),
        value: 1,
    };

    let edition1 = cell
        .clone()
        .publish(encode(&initial), None)
        .fork(&env.address)
        .perform(&env.network)
        .await?;

    let updated = TestData {
        name: "updated".to_string(),
        value: 2,
    };

    let edition2 = cell
        .clone()
        .publish(encode(&updated), Some(edition1))
        .fork(&env.address)
        .perform(&env.network)
        .await?;

    let publication = cell
        .resolve()
        .fork(&env.address)
        .perform(&env.network)
        .await?;

    assert!(publication.is_some());
    let publication = publication.unwrap();
    assert_eq!(decode(&publication.content), updated);
    assert_eq!(publication.edition, edition2);

    Ok(())
}

#[dialog_common::test]
async fn it_detects_cas_conflict() -> anyhow::Result<()> {
    let env = Environment::open();
    let space = &Environment::unique("cas-conflict");
    let cell_name = &Environment::unique("test-cas-key");
    let cell = env.subject().memory().space(space).cell(cell_name);

    let initial = TestData {
        name: "initial".to_string(),
        value: 1,
    };

    let edition1 = cell
        .clone()
        .publish(encode(&initial), None)
        .fork(&env.address)
        .perform(&env.network)
        .await?;

    let by_cell1 = TestData {
        name: "updated_by_cell1".to_string(),
        value: 10,
    };

    cell.clone()
        .publish(encode(&by_cell1), Some(edition1.clone()))
        .fork(&env.address)
        .perform(&env.network)
        .await?;

    // Try to update with stale edition
    let by_cell2 = TestData {
        name: "updated_by_cell2".to_string(),
        value: 20,
    };

    let result = cell
        .clone()
        .publish(encode(&by_cell2), Some(edition1))
        .fork(&env.address)
        .perform(&env.network)
        .await;

    assert!(result.is_err(), "CAS should fail due to edition mismatch");

    // Verify the value is still what cell1 set
    let publication = cell
        .resolve()
        .fork(&env.address)
        .perform(&env.network)
        .await?;

    assert!(publication.is_some());
    assert_eq!(decode(&publication.unwrap().content), by_cell1);

    Ok(())
}

#[dialog_common::test]
async fn it_rejects_publish_with_wrong_initial_edition() -> anyhow::Result<()> {
    let env = Environment::open();
    let space = &Environment::unique("wrong-initial");
    let cell_name = &Environment::unique("test-wrong-init");
    let cell = env.subject().memory().space(space).cell(cell_name);

    // Try to publish with an edition when cell doesn't exist
    let result = cell
        .publish(b"data".to_vec(), Some(b"nonexistent".to_vec()))
        .fork(&env.address)
        .perform(&env.network)
        .await;

    assert!(
        result.is_err(),
        "Publish with edition on empty cell should fail"
    );

    Ok(())
}

#[dialog_common::test]
async fn it_retracts_a_published_value() -> anyhow::Result<()> {
    let env = Environment::open();
    let space = &Environment::unique("retract");
    let cell_name = &Environment::unique("test-retract");
    let cell = env.subject().memory().space(space).cell(cell_name);

    let edition = cell
        .clone()
        .publish(b"to-be-retracted".to_vec(), None)
        .fork(&env.address)
        .perform(&env.network)
        .await?;

    cell.clone()
        .retract(edition)
        .fork(&env.address)
        .perform(&env.network)
        .await?;

    let result = cell
        .resolve()
        .fork(&env.address)
        .perform(&env.network)
        .await?;

    assert!(result.is_none(), "Cell should be empty after retract");

    Ok(())
}

/// R2 does not support If-Match on DeleteObject:
/// https://developers.cloudflare.com/r2/api/s3/api/
#[dialog_common::test]
async fn it_rejects_retract_with_wrong_edition() -> anyhow::Result<()> {
    let env = Environment::open();
    let space = &Environment::unique("retract-wrong");
    let cell_name = &Environment::unique("test-retract-wrong");
    let cell = env.subject().memory().space(space).cell(cell_name);

    cell.clone()
        .publish(b"data".to_vec(), None)
        .fork(&env.address)
        .perform(&env.network)
        .await?;

    let result = cell
        .retract(b"wrong-edition".to_vec())
        .fork(&env.address)
        .perform(&env.network)
        .await;

    if !env.is_r2() {
        assert!(result.is_err(), "Retract with wrong edition should fail");
    }

    Ok(())
}

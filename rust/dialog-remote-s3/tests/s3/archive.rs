//! Integration tests for archive (content-addressed) operations with S3 backend.

#![cfg(feature = "s3-integration-tests")]

use dialog_common::Blake3Hash;
use dialog_effects::archive::prelude::*;

use super::environment::Environment;

#[cfg(target_arch = "wasm32")]
wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

#[dialog_common::test]
async fn it_returns_none_for_missing_content() -> anyhow::Result<()> {
    let env = Environment::open();
    let catalog = &Environment::unique("get-missing");

    let digest = Blake3Hash::hash(b"nonexistent content");

    let result = env
        .subject()
        .archive()
        .catalog(catalog)
        .get(digest.clone())
        .fork(&env.address)
        .perform(&env.network)
        .await?;

    assert!(result.is_none());
    Ok(())
}

#[dialog_common::test]
async fn it_puts_and_gets_content() -> anyhow::Result<()> {
    let env = Environment::open();
    let catalog = &Environment::unique("put-get");
    let content = b"hello world".to_vec();
    let digest = Blake3Hash::hash(&content);

    env.subject()
        .archive()
        .catalog(catalog)
        .put(digest.clone(), content.clone())
        .fork(&env.address)
        .perform(&env.network)
        .await?;

    let retrieved = env
        .subject()
        .archive()
        .catalog(catalog)
        .get(digest.clone())
        .fork(&env.address)
        .perform(&env.network)
        .await?;

    assert_eq!(retrieved, Some(content));
    Ok(())
}

#[dialog_common::test]
async fn it_handles_binary_content() -> anyhow::Result<()> {
    let env = Environment::open();
    let catalog = &Environment::unique("binary");
    let content: Vec<u8> = (0..=255).collect();
    let digest = Blake3Hash::hash(&content);

    env.subject()
        .archive()
        .catalog(catalog)
        .put(digest.clone(), content.clone())
        .fork(&env.address)
        .perform(&env.network)
        .await?;

    let retrieved = env
        .subject()
        .archive()
        .catalog(catalog)
        .get(digest.clone())
        .fork(&env.address)
        .perform(&env.network)
        .await?;

    assert_eq!(retrieved, Some(content));
    Ok(())
}

#[dialog_common::test]
async fn it_isolates_catalogs() -> anyhow::Result<()> {
    let env = Environment::open();
    let catalog_a = &Environment::unique("catalog-a");
    let catalog_b = &Environment::unique("catalog-b");
    let content = b"isolated content".to_vec();
    let digest = Blake3Hash::hash(&content);

    // Put in catalog A
    env.subject()
        .archive()
        .catalog(catalog_a)
        .put(digest.clone(), content.clone())
        .fork(&env.address)
        .perform(&env.network)
        .await?;

    // Should exist in catalog A
    let result_a = env
        .subject()
        .archive()
        .catalog(catalog_a)
        .get(digest.clone())
        .fork(&env.address)
        .perform(&env.network)
        .await?;
    assert_eq!(result_a, Some(content));

    // Should not exist in catalog B
    let result_b = env
        .subject()
        .archive()
        .catalog(catalog_b)
        .get(digest.clone())
        .fork(&env.address)
        .perform(&env.network)
        .await?;
    assert!(result_b.is_none());

    Ok(())
}

#[dialog_common::test]
async fn it_handles_large_content() -> anyhow::Result<()> {
    let env = Environment::open();
    let catalog = &Environment::unique("large");
    let content: Vec<u8> = (0..100_000).map(|i| (i % 256) as u8).collect();
    let digest = Blake3Hash::hash(&content);

    env.subject()
        .archive()
        .catalog(catalog)
        .put(digest.clone(), content.clone())
        .fork(&env.address)
        .perform(&env.network)
        .await?;

    let retrieved = env
        .subject()
        .archive()
        .catalog(catalog)
        .get(digest.clone())
        .fork(&env.address)
        .perform(&env.network)
        .await?;

    assert_eq!(retrieved, Some(content));
    Ok(())
}

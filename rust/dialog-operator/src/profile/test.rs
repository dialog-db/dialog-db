#[cfg(target_arch = "wasm32")]
wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

use super::*;
use dialog_storage::provider::environment::Environment;

#[dialog_common::test]
async fn it_opens_profile() {
    let env = Environment::volatile();

    let profile = Profile::open("alice").perform(&env).await.unwrap();
    assert!(!profile.did().to_string().is_empty());
}

#[dialog_common::test]
async fn it_opens_same_profile_twice() {
    let env = Environment::volatile();

    let first = Profile::open("bob").perform(&env).await.unwrap();
    let second = Profile::open("bob").perform(&env).await.unwrap();

    assert_eq!(first.did(), second.did());
}

#[dialog_common::test]
async fn it_creates_then_loads() {
    let env = Environment::volatile();

    let created = Profile::create("charlie").perform(&env).await.unwrap();
    let loaded = Profile::load("charlie").perform(&env).await.unwrap();

    assert_eq!(created.did(), loaded.did());
}

#[dialog_common::test]
async fn it_fails_to_create_duplicate() {
    let env = Environment::volatile();

    Profile::create("dave").perform(&env).await.unwrap();
    let result = Profile::create("dave").perform(&env).await;

    assert!(result.is_err());
}

#[dialog_common::test]
async fn it_fails_to_load_missing() {
    let env = Environment::volatile();

    let result = Profile::load("missing").perform(&env).await;
    assert!(result.is_err());
}

//! Web environment type alias and builder default.

use dialog_storage::provider::IndexedDb;

use super::builder::Builder;
use super::provider::Environment;
use crate::Credentials;
use crate::remote::Remote;

/// Web environment with opened profile credentials and remote dispatch.
pub type WebEnvironment = Environment<Credentials, IndexedDb, Remote>;

impl Default for Builder<IndexedDb> {
    fn default() -> Self {
        Builder::new(IndexedDb::new())
    }
}

#[cfg(test)]
mod tests {
    use crate::credentials::open::Open;
    use crate::environment::Builder;
    use dialog_storage::provider::IndexedDb;

    #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_service_worker);

    #[dialog_common::test]
    async fn profile_open_creates_key() {
        let storage = IndexedDb::new();

        let profile = Open::new("web-test-create")
            .perform(&storage)
            .await
            .unwrap();
        assert!(
            !profile.did().to_string().is_empty(),
            "should produce a valid DID"
        );
    }

    #[dialog_common::test]
    async fn profile_open_returns_same_key_on_reload() {
        let storage = IndexedDb::new();

        let first = Open::new("web-test-reload")
            .perform(&storage)
            .await
            .unwrap();
        let second = Open::new("web-test-reload")
            .perform(&storage)
            .await
            .unwrap();

        assert_eq!(
            first.did(),
            second.did(),
            "same profile should produce same DID"
        );
    }

    #[dialog_common::test]
    async fn profile_open_different_names_produce_different_keys() {
        let storage = IndexedDb::new();

        let work = Open::new("web-test-work").perform(&storage).await.unwrap();
        let personal = Open::new("web-test-personal")
            .perform(&storage)
            .await
            .unwrap();

        assert_ne!(
            work.did(),
            personal.did(),
            "different profiles should have different keys"
        );
    }

    #[dialog_common::test]
    async fn builder_produces_environment() {
        let env = Builder::default().build().await.unwrap();

        assert_ne!(
            env.authority.profile_did(),
            env.authority.operator_did(),
            "profile and operator should be different keys"
        );
    }
}

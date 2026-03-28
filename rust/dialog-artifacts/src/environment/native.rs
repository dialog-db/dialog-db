//! Native environment type alias and builder default.

use dialog_storage::provider::FileStore;

use super::builder::Builder;
use super::provider::Environment;
use crate::Credentials;
use crate::credentials::open::Open;
use crate::remote::Remote;

/// Native environment with opened profile credentials and remote dispatch.
pub type NativeEnvironment = Environment<Credentials, FileStore, Remote>;

#[cfg(test)]
impl Builder<FileStore> {
    /// Create a builder backed by a temporary directory.
    ///
    /// Panics if temp directory creation fails (test infrastructure failure).
    pub fn temp() -> Self {
        let dir = tempfile::tempdir().expect("failed to create temp directory");
        let fs =
            FileStore::mount(dir.path().to_path_buf()).expect("failed to mount temp directory");
        let _ = dir.keep();
        Builder::new(fs)
    }
}

impl Default for Builder<Option<FileStore>> {
    fn default() -> Self {
        Builder::new(None)
    }
}

impl<P> Builder<Option<FileStore>, P> {
    fn resolve(self) -> Result<Builder<FileStore, P>, super::OpenError> {
        let storage = self.storage;
        let resolved = match storage {
            Some(fs) => fs,
            None => FileStore::profile_location()
                .map_err(|e| super::OpenError::Storage(e.to_string()))?
                .into(),
        };
        Ok(Builder {
            profile: self.profile,
            operator: self.operator,
            storage: resolved,
            remote: self.remote,
            permit: self.permit,
        })
    }

    /// Build the environment, resolving default storage from the platform
    /// profile directory if not explicitly set.
    pub async fn build(self) -> Result<NativeEnvironment, super::OpenError>
    where
        P: super::builder::Permit<NativeEnvironment>,
    {
        self.resolve()?.build().await
    }

    /// Build with a custom profile provider, resolving default storage.
    pub async fn build_with<Pr: dialog_capability::Provider<Open>>(
        self,
        provider: &Pr,
    ) -> Result<NativeEnvironment, super::OpenError>
    where
        P: super::builder::Permit<NativeEnvironment>,
    {
        self.resolve()?.build_with(provider).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Operator;
    use crate::credentials::open::Open;
    use crate::environment::Builder;

    fn temp_storage() -> FileStore {
        let dir = tempfile::tempdir().unwrap();
        let fs = FileStore::mount(dir.path().to_path_buf()).unwrap();
        let _ = dir.keep();
        fs
    }

    #[dialog_common::test]
    async fn profile_open_creates_key_on_first_run() {
        let storage = temp_storage();
        let profile = Open::new("default").perform(&storage).await.unwrap();
        assert!(
            !profile.did().to_string().is_empty(),
            "should produce a valid DID"
        );
    }

    #[dialog_common::test]
    async fn profile_open_returns_same_key_on_reload() {
        let storage = temp_storage();
        let first = Open::new("default").perform(&storage).await.unwrap();
        let second = Open::new("default").perform(&storage).await.unwrap();

        assert_eq!(
            first.did(),
            second.did(),
            "same profile should produce same DID"
        );
    }

    #[dialog_common::test]
    async fn profile_open_different_names_produce_different_keys() {
        let storage = temp_storage();
        let work = Open::new("work").perform(&storage).await.unwrap();
        let personal = Open::new("personal").perform(&storage).await.unwrap();

        assert_ne!(
            work.did(),
            personal.did(),
            "different profiles should have different keys"
        );
    }

    #[dialog_common::test]
    async fn builder_produces_different_profile_and_operator() {
        let env = Builder::temp().build().await.unwrap();

        assert_ne!(
            env.authority.profile_did(),
            env.authority.operator_did(),
            "profile and operator should be different keys (Operator::Unique)"
        );
    }

    #[dialog_common::test]
    async fn builder_preserves_profile_across_restarts() {
        let dir = tempfile::tempdir().unwrap();

        let storage1 = FileStore::mount(dir.path().to_path_buf()).unwrap();
        let env1 = Builder::default().storage(storage1).build().await.unwrap();

        let storage2 = FileStore::mount(dir.path().to_path_buf()).unwrap();
        let env2 = Builder::default().storage(storage2).build().await.unwrap();

        assert_eq!(
            env1.authority.profile_did(),
            env2.authority.profile_did(),
            "profile DID should persist"
        );
    }

    #[dialog_common::test]
    async fn builder_unique_operator_differs_each_time() {
        let dir = tempfile::tempdir().unwrap();

        let storage1 = FileStore::mount(dir.path().to_path_buf()).unwrap();
        let env1 = Builder::default().storage(storage1).build().await.unwrap();

        let storage2 = FileStore::mount(dir.path().to_path_buf()).unwrap();
        let env2 = Builder::default().storage(storage2).build().await.unwrap();

        assert_ne!(
            env1.authority.operator_did(),
            env2.authority.operator_did(),
            "Operator::Unique should differ each time"
        );
    }

    #[dialog_common::test]
    async fn builder_derived_operator_is_deterministic() {
        let dir = tempfile::tempdir().unwrap();

        let storage1 = FileStore::mount(dir.path().to_path_buf()).unwrap();
        let env1 = Builder::default()
            .operator(Operator::derive(b"alice"))
            .storage(storage1)
            .build()
            .await
            .unwrap();

        let storage2 = FileStore::mount(dir.path().to_path_buf()).unwrap();
        let env2 = Builder::default()
            .operator(Operator::derive(b"alice"))
            .storage(storage2)
            .build()
            .await
            .unwrap();

        assert_eq!(
            env1.authority.operator_did(),
            env2.authority.operator_did(),
            "same context should produce same operator"
        );
    }

    #[dialog_common::test]
    async fn builder_different_contexts_produce_different_operators() {
        let dir = tempfile::tempdir().unwrap();

        let storage1 = FileStore::mount(dir.path().to_path_buf()).unwrap();
        let env1 = Builder::default()
            .operator(Operator::derive(b"alice"))
            .storage(storage1)
            .build()
            .await
            .unwrap();

        let storage2 = FileStore::mount(dir.path().to_path_buf()).unwrap();
        let env2 = Builder::default()
            .operator(Operator::derive(b"bob"))
            .storage(storage2)
            .build()
            .await
            .unwrap();

        assert_ne!(
            env1.authority.operator_did(),
            env2.authority.operator_did(),
            "different contexts should produce different operators"
        );
    }

    #[dialog_common::test]
    async fn builder_with_custom_profile_provider() {
        let profile_storage = temp_storage();
        let data_storage = temp_storage();

        let env = Builder::default()
            .storage(data_storage)
            .build_with(&profile_storage)
            .await
            .unwrap();

        assert_ne!(env.authority.profile_did(), env.authority.operator_did());
    }
}

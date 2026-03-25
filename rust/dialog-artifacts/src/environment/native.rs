//! Native environment type alias and builder default.

use dialog_storage::provider::FileSystem;

use super::builder::Builder;
use super::provider::Environment;
use crate::credentials::open::Open;
use crate::remote::Remote;
use crate::{Credentials, Operator};

/// Native environment with opened profile credentials and remote dispatch.
pub type NativeEnvironment = Environment<Credentials, FileSystem, Remote>;

impl Default for Builder<Option<FileSystem>> {
    fn default() -> Self {
        Builder {
            profile: "default".into(),
            operator: Operator::Unique,
            storage: None,
            remote: Remote,
        }
    }
}

impl Builder<Option<FileSystem>> {
    fn resolve(self) -> Result<Builder<FileSystem>, super::OpenError> {
        let storage = match self.storage {
            Some(fs) => fs,
            None => FileSystem::profile()
                .map_err(|e| super::OpenError::Storage(e.to_string()))?
                .into(),
        };
        Ok(Builder {
            profile: self.profile,
            operator: self.operator,
            storage,
            remote: self.remote,
        })
    }

    /// Build the environment, resolving default storage from the platform
    /// profile directory if not explicitly set.
    pub async fn build(self) -> Result<NativeEnvironment, super::OpenError> {
        self.resolve()?.build().await
    }

    /// Build with a custom profile provider, resolving default storage.
    pub async fn build_with<P: dialog_capability::Provider<Open>>(
        self,
        provider: &P,
    ) -> Result<NativeEnvironment, super::OpenError> {
        self.resolve()?.build_with(provider).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Operator;
    use crate::credentials::open::Open;
    use crate::environment::Builder;

    #[dialog_common::test]
    async fn profile_open_creates_key_on_first_run() {
        let dir = tempfile::tempdir().unwrap();
        let storage = FileSystem::mount(dir.path().to_path_buf()).unwrap();

        let profile = Open::new("default").perform(&storage).await.unwrap();
        assert!(
            !profile.did().to_string().is_empty(),
            "should produce a valid DID"
        );
    }

    #[dialog_common::test]
    async fn profile_open_returns_same_key_on_reload() {
        let dir = tempfile::tempdir().unwrap();
        let storage = FileSystem::mount(dir.path().to_path_buf()).unwrap();

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
        let dir = tempfile::tempdir().unwrap();
        let storage = FileSystem::mount(dir.path().to_path_buf()).unwrap();

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
        let dir = tempfile::tempdir().unwrap();
        let storage = FileSystem::mount(dir.path().to_path_buf()).unwrap();

        let env = Builder::default().storage(storage).build().await.unwrap();

        assert_ne!(
            env.credentials.profile_did(),
            env.credentials.operator_did(),
            "profile and operator should be different keys (Operator::Unique)"
        );
    }

    #[dialog_common::test]
    async fn builder_preserves_profile_across_restarts() {
        let dir = tempfile::tempdir().unwrap();

        let storage1 = FileSystem::mount(dir.path().to_path_buf()).unwrap();
        let env1 = Builder::default().storage(storage1).build().await.unwrap();

        let storage2 = FileSystem::mount(dir.path().to_path_buf()).unwrap();
        let env2 = Builder::default().storage(storage2).build().await.unwrap();

        assert_eq!(
            env1.credentials.profile_did(),
            env2.credentials.profile_did(),
            "profile DID should persist"
        );
    }

    #[dialog_common::test]
    async fn builder_unique_operator_differs_each_time() {
        let dir = tempfile::tempdir().unwrap();

        let storage1 = FileSystem::mount(dir.path().to_path_buf()).unwrap();
        let env1 = Builder::default().storage(storage1).build().await.unwrap();

        let storage2 = FileSystem::mount(dir.path().to_path_buf()).unwrap();
        let env2 = Builder::default().storage(storage2).build().await.unwrap();

        assert_ne!(
            env1.credentials.operator_did(),
            env2.credentials.operator_did(),
            "Operator::Unique should differ each time"
        );
    }

    #[dialog_common::test]
    async fn builder_derived_operator_is_deterministic() {
        let dir = tempfile::tempdir().unwrap();

        let storage1 = FileSystem::mount(dir.path().to_path_buf()).unwrap();
        let env1 = Builder::default()
            .operator(Operator::derive(b"alice"))
            .storage(storage1)
            .build()
            .await
            .unwrap();

        let storage2 = FileSystem::mount(dir.path().to_path_buf()).unwrap();
        let env2 = Builder::default()
            .operator(Operator::derive(b"alice"))
            .storage(storage2)
            .build()
            .await
            .unwrap();

        assert_eq!(
            env1.credentials.operator_did(),
            env2.credentials.operator_did(),
            "same context should produce same operator"
        );
    }

    #[dialog_common::test]
    async fn builder_different_contexts_produce_different_operators() {
        let dir = tempfile::tempdir().unwrap();

        let storage1 = FileSystem::mount(dir.path().to_path_buf()).unwrap();
        let env1 = Builder::default()
            .operator(Operator::derive(b"alice"))
            .storage(storage1)
            .build()
            .await
            .unwrap();

        let storage2 = FileSystem::mount(dir.path().to_path_buf()).unwrap();
        let env2 = Builder::default()
            .operator(Operator::derive(b"bob"))
            .storage(storage2)
            .build()
            .await
            .unwrap();

        assert_ne!(
            env1.credentials.operator_did(),
            env2.credentials.operator_did(),
            "different contexts should produce different operators"
        );
    }

    #[dialog_common::test]
    async fn builder_with_custom_profile_provider() {
        let profile_dir = tempfile::tempdir().unwrap();
        let profile_storage = FileSystem::mount(profile_dir.path().to_path_buf()).unwrap();

        let data_dir = tempfile::tempdir().unwrap();
        let data_storage = FileSystem::mount(data_dir.path().to_path_buf()).unwrap();

        let env = Builder::default()
            .storage(data_storage)
            .build_with(&profile_storage)
            .await
            .unwrap();

        assert_ne!(
            env.credentials.profile_did(),
            env.credentials.operator_did(),
        );
    }
}

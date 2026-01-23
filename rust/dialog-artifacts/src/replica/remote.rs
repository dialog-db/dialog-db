//! Remote repository credentials and configuration.
//!
//! This module defines the credentials types used to connect to remote
//! repositories for synchronization.

use dialog_common::capability::{Capability, Subject};
use dialog_s3_credentials::capability::{archive, memory};
use dialog_s3_credentials::{AccessError, credentials, s3};
use dialog_storage::Blake3Hash;
use serde::{Deserialize, Serialize};
use url::Url;

#[cfg(feature = "ucan")]
pub use dialog_s3_credentials::{ucan, ucan::DelegationChain};

use super::Operator;

/// A named remote site identifier.
pub type Site = String;

/// Represents a configured remote site with its credentials.
///
/// This is the persisted state for a remote, storing the site name
/// and the credentials needed to connect to it.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RemoteSite {
    /// The name of this remote (e.g., "origin", "backup").
    pub name: Site,
    /// Credentials for connecting to this remote.
    pub credentials: RemoteCredentials,

    issuer: Operator,
}

impl RemoteSite {
    /// Create a new remote site with the given name and credentials.
    pub fn new(name: impl Into<Site>, credentials: RemoteCredentials, issuer: Operator) -> Self {
        Self {
            name: name.into(),
            issuer,
            credentials,
        }
    }

    /// Start building a reference to a repository at this remote site.
    ///
    /// The `subject` is the DID identifying the repository owner.
    pub fn repository(&self, subject: impl Into<String>) -> RemoteRepository {
        RemoteRepository {
            issuer: self.issuer.clone(),
            site: self.clone(),
            subject: subject.into(),
        }
    }
}

/// A reference to a repository at a remote site.
///
/// This is a builder step for accessing remote branches.
#[derive(Debug, Clone)]
pub struct RemoteRepository {
    /// The remote site this repository is on.
    pub site: RemoteSite,
    /// The subject DID identifying the repository owner.
    pub subject: String,

    pub issuer: Operator,
}

impl RemoteRepository {
    /// Reference a branch within this remote repository.
    pub fn branch(&self, name: impl Into<String>) -> RemoteBranchRef {
        RemoteBranchRef {
            repository: self.clone(),
            name: name.into(),
        }
    }
}

/// A reference to a branch at a remote repository.
///
/// This is the final builder step that identifies a specific branch.
/// Named `RemoteBranchRef` to distinguish from `RemoteBranch<Backend>`
/// which is the actual connected branch.
#[derive(Debug, Clone)]
pub struct RemoteBranchRef {
    /// The remote repository this branch is in.
    pub repository: RemoteRepository,
    /// The branch name.
    pub name: String,
}

impl RemoteBranchRef {
    /// Returns a capability for the archive catalog (content-addressed storage).
    ///
    /// The catalog path is: `{subject}/archive/index`
    pub fn index(&self) -> Index {
        Index {
            issuer: self.repository.issuer.clone(),
            credentials: self.repository.site.credentials.clone(),
            archive: Subject::from(self.repository.subject.as_str())
                .attenuate(archive::Archive)
                .attenuate(archive::Catalog::new("index")),
        }
    }

    /// Returns a capability for the memory cell (revision pointer).
    ///
    /// The cell path is: `{subject}/memory/{subject}/{branch_name}`
    pub fn revision(&self) -> Capability<memory::Cell> {
        Subject::from(self.repository.subject.as_str())
            .attenuate(memory::Memory)
            .attenuate(memory::Space::new("local"))
            .attenuate(memory::Cell::new(&self.name))
    }
}

pub struct Index {
    issuer: Operator,
    credentials: RemoteCredentials,
    archive: Capability<archive::Catalog>,
}

impl Index {
    pub fn get(&self, digest: Blake3Hash) -> Capability<archive::Get> {
        self.archive
            .invoke(archive::Get { digest })
            .acquire(self.credentials)
    }
}

/// Credentials for connecting to a remote repository.
///
/// This enum stores the credentials configuration that can be persisted.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum RemoteCredentials {
    /// Direct S3 access with optional signing credentials.
    S3(s3::Credentials),
    /// UCAN-based access via an authorization service.
    #[cfg(feature = "ucan")]
    Ucan(ucan::Credentials),
}

impl RemoteCredentials {
    /// Create S3 credentials for public access.
    pub fn s3_public(
        endpoint: impl Into<Url>,
        region: impl Into<String>,
        bucket: impl Into<String>,
    ) -> Self {
        let address = s3::Address::new(endpoint, region, bucket);
        Self::S3(s3::PublicCredentials::new(address))
    }

    /// Create S3 credentials with signing keys.
    pub fn s3_private(
        endpoint: impl Into<Url>,
        region: impl Into<String>,
        bucket: impl Into<String>,
        access_key_id: impl Into<String>,
        secret_access_key: impl Into<String>,
    ) -> Result<Self, AccessError> {
        let address = s3::Address::new(endpoint, region, bucket);

        let credentials = s3::PrivateCredentials::new(address, access_key_id, secret_access_key)?;

        Self::S3(credentials)
    }

    /// Create UCAN credentials from an optional delegation chain.
    #[cfg(feature = "ucan")]
    pub fn ucan(endpoint: impl Into<Url>, delegation: Option<DelegationChain>) -> Self {
        Self::Ucan(ucan::Credentials::new(endpoint.into(), delegation));
    }
}

#[cfg(test)]
mod tests {
    #[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
    #[cfg_attr(not(target_arch = "wasm32"), tokio::test)]
    async fn it_uses_new_remote_add() {
        use dialog_storage::JournaledStorage;

        // Create a replica
        let issuer = Operator::from_passphrase("test_remotes_add_v2");
        let subject = issuer.did().to_string();
        let backend = MemoryStorageBackend::default();
        let journaled = JournaledStorage::new(backend);
        let mut replica = Replica::open(issuer.clone(), subject, journaled.clone())
            .expect("Failed to create replica");

        // Add a remote using the new add_v2 API with S3 credentials
        let credentials = RemoteCredentials::S3 {
            endpoint: "https://s3.us-east-1.amazonaws.com".parse().unwrap(),
            region: "us-east-1".to_string(),
            bucket: "test-bucket".to_string(),
            access_key_id: Some("AKIATEST".to_string()),
            secret_access_key: Some("secret123".to_string()),
        };

        let origin = replica
            .remotes
            .add_v2("origin", credentials.clone())
            .await
            .expect("Failed to add remote");

        assert_eq!(origin.name, "origin");
        assert_eq!(origin.credentials, credentials);

        // Adding same remote again with same credentials should succeed
        let origin_again = replica
            .remotes
            .add_v2("origin", credentials.clone())
            .await
            .expect("Should succeed with same credentials");

        assert_eq!(origin_again.name, "origin");

        // Adding same remote with different credentials should fail
        let different_credentials = RemoteCredentials::S3 {
            endpoint: "https://s3.eu-west-1.amazonaws.com".parse().unwrap(),
            region: "eu-west-1".to_string(),
            bucket: "different-bucket".to_string(),
            access_key_id: None,
            secret_access_key: None,
        };

        let result = replica
            .remotes
            .add_v2("origin", different_credentials)
            .await;

        assert!(
            result.is_err(),
            "Should fail when adding remote with different credentials"
        );

        // Adding a different remote should succeed
        let backup_credentials = RemoteCredentials::S3 {
            endpoint: "https://backup.example.com".parse().unwrap(),
            region: "auto".to_string(),
            bucket: "backup-bucket".to_string(),
            access_key_id: None,
            secret_access_key: None,
        };

        let backup = replica
            .remotes
            .add_v2("backup", backup_credentials.clone())
            .await
            .expect("Failed to add backup remote");

        assert_eq!(backup.name, "backup");
        assert_eq!(backup.credentials, backup_credentials);
    }

    #[cfg(feature = "ucan")]
    #[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
    #[cfg_attr(not(target_arch = "wasm32"), tokio::test)]
    async fn test_remotes_add_v2_ucan() {
        use dialog_storage::JournaledStorage;

        // Create a replica
        let issuer = Operator::from_passphrase("test_remotes_add_v2_ucan");
        let subject = issuer.did().to_string();
        let backend = MemoryStorageBackend::default();
        let journaled = JournaledStorage::new(backend);
        let mut replica = Replica::open(issuer.clone(), subject, journaled.clone())
            .expect("Failed to create replica");

        // Add a remote using UCAN credentials (without delegation for now)
        let credentials = RemoteCredentials::Ucan {
            endpoint: "https://access.example.com".parse().unwrap(),
            delegation: None,
        };

        let origin = replica
            .remotes
            .add_v2("origin", credentials.clone())
            .await
            .expect("Failed to add UCAN remote");

        assert_eq!(origin.name, "origin");
        assert_eq!(origin.credentials, credentials);
    }

    #[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
    #[cfg_attr(not(target_arch = "wasm32"), tokio::test)]
    async fn test_remote_fluent_api() {
        use dialog_storage::JournaledStorage;

        // Create a replica
        let issuer = Operator::from_passphrase("test_remote_fluent_api");
        let subject = issuer.did().to_string();
        let backend = MemoryStorageBackend::default();
        let journaled = JournaledStorage::new(backend);
        let mut replica = Replica::open(issuer.clone(), subject, journaled.clone())
            .expect("Failed to create replica");

        // Add a remote
        let credentials = RemoteCredentials::S3 {
            endpoint: "https://s3.us-east-1.amazonaws.com".parse().unwrap(),
            region: "us-east-1".to_string(),
            bucket: "test-bucket".to_string(),
            access_key_id: None,
            secret_access_key: None,
        };

        let origin = replica
            .remotes
            .add_v2("origin", credentials)
            .await
            .expect("Failed to add remote");

        // Use the fluent API to reference a remote branch
        let remote_did = "did:key:z6MkhaXgBZDvotDkL5257faiztiGiC2QtKLGpbnnEGta2doK";
        let remote_branch = origin.repository(remote_did).branch("main");

        assert_eq!(remote_branch.name, "main");
        assert_eq!(remote_branch.repository.subject, remote_did);
        assert_eq!(remote_branch.repository.site.name, "origin");

        // Test capability builders
        let index_cap = remote_branch.index();
        assert_eq!(index_cap.subject(), remote_did);
        // Catalog is a Policy (not Attenuation), so only Archive contributes to ability
        assert_eq!(index_cap.ability(), "/archive");

        let revision_cap = remote_branch.revision();
        assert_eq!(revision_cap.subject(), remote_did);
        // Space and Cell are Policies, so only Memory contributes to ability
        assert_eq!(revision_cap.ability(), "/memory");
    }
}

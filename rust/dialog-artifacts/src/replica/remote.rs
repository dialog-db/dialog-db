//! Remote repository credentials and configuration.
//!
//! This module defines the credentials types used to connect to remote
//! repositories for synchronization.

use dialog_common::capability::{Capability, Subject};
use dialog_s3_credentials::capability::{archive, memory};
use serde::{Deserialize, Serialize};
use url::Url;

#[cfg(feature = "ucan")]
pub use dialog_s3_credentials::ucan::DelegationChain;

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
}

impl RemoteSite {
    /// Create a new remote site with the given name and credentials.
    pub fn new(name: impl Into<Site>, credentials: RemoteCredentials) -> Self {
        Self {
            name: name.into(),
            credentials,
        }
    }

    /// Start building a reference to a repository at this remote site.
    ///
    /// The `subject` is the DID identifying the repository owner.
    pub fn repository(&self, subject: impl Into<String>) -> RemoteRepository {
        RemoteRepository {
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
    pub fn index(&self) -> Capability<archive::Catalog> {
        Subject::from(self.repository.subject.as_str())
            .attenuate(archive::Archive)
            .attenuate(archive::Catalog::new("index"))
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

/// Credentials for connecting to a remote repository.
///
/// This enum stores the credentials configuration that can be persisted.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum RemoteCredentials {
    /// Direct S3 access with optional signing credentials.
    S3 {
        /// S3 endpoint URL.
        endpoint: Url,
        /// AWS region for signing.
        region: String,
        /// S3 bucket name.
        bucket: String,
        /// AWS access key ID (None for public access).
        access_key_id: Option<String>,
        /// AWS secret access key (None for public access).
        secret_access_key: Option<String>,
    },
    /// UCAN-based access via an authorization service.
    #[cfg(feature = "ucan")]
    Ucan {
        /// Access service endpoint URL.
        endpoint: Url,
        /// UCAN delegation chain proving authority.
        delegation: Option<DelegationChain>,
    },
}

impl RemoteCredentials {
    /// Create S3 credentials for public access.
    pub fn s3_public(
        endpoint: impl Into<Url>,
        region: impl Into<String>,
        bucket: impl Into<String>,
    ) -> Self {
        Self::S3 {
            endpoint: endpoint.into(),
            region: region.into(),
            bucket: bucket.into(),
            access_key_id: None,
            secret_access_key: None,
        }
    }

    /// Create S3 credentials with signing keys.
    pub fn s3_private(
        endpoint: impl Into<Url>,
        region: impl Into<String>,
        bucket: impl Into<String>,
        access_key_id: impl Into<String>,
        secret_access_key: impl Into<String>,
    ) -> Self {
        Self::S3 {
            endpoint: endpoint.into(),
            region: region.into(),
            bucket: bucket.into(),
            access_key_id: Some(access_key_id.into()),
            secret_access_key: Some(secret_access_key.into()),
        }
    }

    /// Create UCAN credentials from an optional delegation chain.
    #[cfg(feature = "ucan")]
    pub fn ucan(endpoint: impl Into<Url>, delegation: Option<DelegationChain>) -> Self {
        Self::Ucan {
            endpoint: endpoint.into(),
            delegation,
        }
    }
}

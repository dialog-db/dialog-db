//! Remote credentials types.
//!
//! This module provides the [`RemoteCredentials`] enum which stores
//! credentials for connecting to remote storage backends. Different
//! backends are enabled via feature flags.
//!
//! Without any remote features enabled, this enum cannot be constructed,
//! preventing the creation of remote configurations at compile time.

use dialog_capability::Did;
#[cfg(feature = "s3")]
use dialog_s3_credentials::s3;
#[cfg(feature = "ucan")]
pub use dialog_s3_credentials::{ucan, ucan::DelegationChain};
#[cfg(feature = "s3")]
use dialog_storage::s3::{Bucket, S3};
use serde::{Deserialize, Serialize};

use super::{Connection, MemoryConnection, SigningAuthority};
#[cfg(feature = "s3")]
use super::{RemoteBackend, S3Connection};
use crate::repository::RepositoryError;
#[cfg(feature = "s3")]
use crate::{ErrorMappingBackend, PlatformStorage};
#[cfg(feature = "s3")]
use dialog_storage::CborEncoder;

/// Trait for credentials that can establish a connection.
pub trait Connect {
    /// Open a connection using these credentials.
    fn connect(self, issuer: SigningAuthority, subject: &Did) -> Connection;
}

#[cfg(feature = "s3")]
impl Connect for dialog_s3_credentials::Credentials {
    fn connect(self, issuer: SigningAuthority, subject: &Did) -> Connection {
        let s3 = S3::new(self, issuer);
        let bucket = Bucket::new(s3.clone(), subject, "memory");
        let backend = RemoteBackend::S3(ErrorMappingBackend::new(bucket));
        let memory = PlatformStorage::new(backend, CborEncoder);
        let index = Bucket::new(s3, subject, "archive/index");
        S3Connection::new(memory, index).into()
    }
}

/// Credentials for connecting to a remote repository.
///
/// This enum stores the credentials configuration that can be persisted.
/// Each variant is gated behind its respective feature flag:
///
/// - `S3` - Direct S3 access (requires `s3` feature)
/// - `Ucan` - UCAN-based S3 access (requires `ucan` feature, implies `s3`)
/// - `Memory` - In-memory storage (always available, useful for testing)
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[allow(clippy::large_enum_variant)]
pub enum RemoteCredentials {
    /// Direct S3 access with optional signing credentials.
    #[cfg(feature = "s3")]
    S3(s3::Credentials),
    /// UCAN-based access via an authorization service.
    #[cfg(feature = "ucan")]
    Ucan(ucan::Credentials),
    /// In-memory storage (no credentials needed, useful for testing).
    Memory,
}

#[cfg(feature = "s3")]
impl From<s3::Credentials> for RemoteCredentials {
    fn from(credentials: s3::Credentials) -> Self {
        Self::S3(credentials)
    }
}

#[cfg(feature = "s3")]
impl From<s3::PublicCredentials> for RemoteCredentials {
    fn from(credentials: s3::PublicCredentials) -> Self {
        Self::S3(credentials.into())
    }
}

#[cfg(feature = "s3")]
impl From<s3::PrivateCredentials> for RemoteCredentials {
    fn from(credentials: s3::PrivateCredentials) -> Self {
        Self::S3(credentials.into())
    }
}

#[cfg(feature = "ucan")]
impl From<ucan::Credentials> for RemoteCredentials {
    fn from(credentials: ucan::Credentials) -> Self {
        Self::Ucan(credentials)
    }
}

impl RemoteCredentials {
    /// Open a connection to the remote storage using these credentials.
    pub fn connect(
        &self,
        issuer: SigningAuthority,
        subject: &Did,
    ) -> Result<Connection, RepositoryError> {
        match self {
            #[cfg(feature = "s3")]
            Self::S3(creds) => {
                let credentials: dialog_s3_credentials::Credentials = creds.clone().into();
                Ok(credentials.connect(issuer, subject))
            }
            #[cfg(feature = "ucan")]
            Self::Ucan(creds) => {
                let credentials: dialog_s3_credentials::Credentials = creds.clone().into();
                Ok(credentials.connect(issuer, subject))
            }
            Self::Memory => Ok(MemoryConnection::default().connect(issuer, subject)),
        }
    }
}

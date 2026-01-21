//! Provider and Access implementations for S3 credentials.
//!
//! This module implements the capability-based authorization flow for direct S3 access.
//! S3 credentials self-issue authorization when they own the bucket (subject DID matches).

use async_trait::async_trait;
use dialog_common::ConditionalSend;
use dialog_common::capability::Did;
use dialog_common::capability::{
    Ability, Access, Authorization, AuthorizationError, Capability, Claim, Principal, Provider,
};

use crate::access::{archive as access_archive, memory as access_memory, storage as access_storage};
use crate::{AuthorizationError as S3Error, RequestDescriptor};

use super::Credentials;

/// Self-issued authorization for direct S3 access.
///
/// For S3 credentials that own the bucket, authorization is self-issued.
/// This struct holds the subject, audience, and command for the authorized capability.
#[derive(Debug, Clone)]
pub struct S3Authorization {
    subject: Did,
    audience: Did,
    can: String,
}

impl S3Authorization {
    /// Create a new S3 authorization.
    pub fn new(subject: Did, audience: Did, can: String) -> Self {
        Self {
            subject,
            audience,
            can,
        }
    }
}

impl Authorization for S3Authorization {
    fn subject(&self) -> &Did {
        &self.subject
    }

    fn audience(&self) -> &Did {
        &self.audience
    }

    fn can(&self) -> &str {
        &self.can
    }
}

// --- Principal implementation for Credentials ---
//
// S3 credentials use the subject DID as their identity.
// The subject DID is used as a path prefix within the bucket.
// This allows self-issuing authorization for operations on paths under the subject.

impl Principal for Credentials {
    fn did(&self) -> &Did {
        self.subject()
    }
}

impl dialog_common::capability::Authority for Credentials {
    fn sign(&self, _payload: &[u8]) -> Vec<u8> {
        // S3 credentials sign via the SigV4 algorithm during URL generation,
        // not via a general-purpose sign method. This method exists only to
        // satisfy the Authority trait.
        // The actual signing happens in Provider::execute().
        Vec::new()
    }
}

// --- Access implementation for Credentials ---

/// Error type that combines S3 and Authorization errors.
#[derive(Debug, thiserror::Error)]
pub enum AccessError {
    #[error("Authorization error: {0}")]
    Authorization(#[from] AuthorizationError),
    #[error("S3 error: {0}")]
    S3(#[from] S3Error),
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl Access for Credentials {
    type Authorization = S3Authorization;
    type Error = AccessError;

    async fn claim<C: Ability + Clone + ConditionalSend + 'static>(
        &self,
        claim: Claim<C>,
    ) -> Result<Self::Authorization, Self::Error> {
        // For direct S3, we only support self-issued authorization.
        // If the claim's subject matches our DID, we self-issue.
        // Otherwise, we need delegation (not supported for direct S3).
        if claim.subject() == self.did() {
            // Self-issue
            Ok(S3Authorization::new(
                claim.subject().clone(),
                claim.audience().clone(),
                claim.command(),
            ))
        } else {
            Err(AccessError::Authorization(
                AuthorizationError::NoDelegationChain {
                    subject: claim.subject().clone(),
                    audience: claim.audience().clone(),
                },
            ))
        }
    }
}


// --- Provider implementations for capabilities ---
//
// Each effect type needs a Provider implementation that generates the RequestDescriptor.
// These providers work with Capability<Fx> directly. Since Capability<access::*::Fx>
// implements the access::Claim trait, we delegate to Credentials::authorize.

// Provider for access::storage::Get
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl Provider<access_storage::Get> for Credentials {
    async fn execute(
        &mut self,
        cap: Capability<access_storage::Get>,
    ) -> Result<RequestDescriptor, S3Error> {
        // Capability<access_storage::Get> implements access::Claim
        self.authorize(&cap)
    }
}

// Provider for access::storage::Set
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl Provider<access_storage::Set> for Credentials {
    async fn execute(
        &mut self,
        cap: Capability<access_storage::Set>,
    ) -> Result<RequestDescriptor, S3Error> {
        self.authorize(&cap)
    }
}

// Provider for access::storage::Delete
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl Provider<access_storage::Delete> for Credentials {
    async fn execute(
        &mut self,
        cap: Capability<access_storage::Delete>,
    ) -> Result<RequestDescriptor, S3Error> {
        self.authorize(&cap)
    }
}

// Provider for access::storage::List
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl Provider<access_storage::List> for Credentials {
    async fn execute(
        &mut self,
        cap: Capability<access_storage::List>,
    ) -> Result<RequestDescriptor, S3Error> {
        self.authorize(&cap)
    }
}

// Provider for access::memory::Resolve
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl Provider<access_memory::Resolve> for Credentials {
    async fn execute(
        &mut self,
        cap: Capability<access_memory::Resolve>,
    ) -> Result<RequestDescriptor, S3Error> {
        self.authorize(&cap)
    }
}

// Provider for access::memory::Publish
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl Provider<access_memory::Publish> for Credentials {
    async fn execute(
        &mut self,
        cap: Capability<access_memory::Publish>,
    ) -> Result<RequestDescriptor, S3Error> {
        self.authorize(&cap)
    }
}

// Provider for access::memory::Retract
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl Provider<access_memory::Retract> for Credentials {
    async fn execute(
        &mut self,
        cap: Capability<access_memory::Retract>,
    ) -> Result<RequestDescriptor, S3Error> {
        self.authorize(&cap)
    }
}

// Provider for access::archive::Get
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl Provider<access_archive::Get> for Credentials {
    async fn execute(
        &mut self,
        cap: Capability<access_archive::Get>,
    ) -> Result<RequestDescriptor, S3Error> {
        self.authorize(&cap)
    }
}

// Provider for access::archive::Put
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl Provider<access_archive::Put> for Credentials {
    async fn execute(
        &mut self,
        cap: Capability<access_archive::Put>,
    ) -> Result<RequestDescriptor, S3Error> {
        self.authorize(&cap)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Address;
    use crate::capability::{archive, memory, storage};
    use dialog_common::capability::Subject;

    const TEST_SUBJECT: &str = "did:key:zTestSubject";

    /// Helper to build a storage Get capability.
    fn get_capability(subject: &str, store: &str, key: &[u8]) -> Capability<access_storage::Get> {
        Subject::from(subject)
            .attenuate(storage::Storage)
            .attenuate(storage::Store::new(store))
            .invoke(access_storage::Get::new(key))
    }

    /// Helper to build a storage Set capability.
    fn set_capability(
        subject: &str,
        store: &str,
        key: &[u8],
        checksum: crate::Checksum,
    ) -> Capability<access_storage::Set> {
        Subject::from(subject)
            .attenuate(storage::Storage)
            .attenuate(storage::Store::new(store))
            .invoke(access_storage::Set::new(key, checksum))
    }

    #[dialog_common::test]
    async fn it_performs_storage_get() {
        let address = Address::new(
            "https://s3.us-east-1.amazonaws.com",
            "us-east-1",
            "my-bucket",
        );
        let mut creds = Credentials::public(address, TEST_SUBJECT).unwrap();

        // Build capability chain using access effect types (which implement Claim)
        let capability = get_capability(TEST_SUBJECT, "index", b"my-key");

        // Perform to get RequestDescriptor
        let descriptor = capability.perform(&mut creds).await.unwrap();

        assert_eq!(descriptor.method, "GET");
        assert!(descriptor.url.as_str().contains("my-bucket"));
        // Path should be subject/store/key
        assert!(
            descriptor.url.as_str().contains(TEST_SUBJECT),
            "URL should contain subject prefix"
        );
    }

    #[dialog_common::test]
    async fn it_performs_storage_set() {
        let address = Address::new(
            "https://s3.us-east-1.amazonaws.com",
            "us-east-1",
            "my-bucket",
        );
        let mut creds = Credentials::public(address, TEST_SUBJECT).unwrap();

        let checksum = crate::Checksum::Sha256([0u8; 32]);
        let capability = set_capability(TEST_SUBJECT, "blob", b"my-key", checksum);

        let descriptor = capability.perform(&mut creds).await.unwrap();

        assert_eq!(descriptor.method, "PUT");
        assert!(
            descriptor
                .headers
                .iter()
                .any(|(k, _)| k == "x-amz-checksum-sha256")
        );
    }

    #[dialog_common::test]
    async fn it_performs_memory_resolve() {
        let address = Address::new(
            "https://s3.us-east-1.amazonaws.com",
            "us-east-1",
            "my-bucket",
        );
        let mut creds = Credentials::public(address, TEST_SUBJECT).unwrap();

        let capability: Capability<access_memory::Resolve> = Subject::from(TEST_SUBJECT)
            .attenuate(memory::Memory)
            .attenuate(memory::Space::new("did:key:zUser"))
            .attenuate(memory::Cell::new("main"))
            .invoke(access_memory::Resolve);

        let descriptor = capability.perform(&mut creds).await.unwrap();

        assert_eq!(descriptor.method, "GET");
        // Path should include subject prefix
        assert!(
            descriptor.url.as_str().contains(TEST_SUBJECT),
            "URL should contain subject prefix"
        );
        assert!(descriptor.url.as_str().contains("did:key:zUser/main"));
    }


    #[dialog_common::test]
    async fn it_performs_archive_get() {
        let address = Address::new(
            "https://s3.us-east-1.amazonaws.com",
            "us-east-1",
            "my-bucket",
        );
        let mut creds = Credentials::public(address, TEST_SUBJECT).unwrap();

        // Blake3 digest (32 bytes)
        let digest = [0u8; 32];

        let capability: Capability<access_archive::Get> = Subject::from(TEST_SUBJECT)
            .attenuate(archive::Archive)
            .attenuate(archive::Catalog::new("blobs"))
            .invoke(access_archive::Get::new(digest));

        let descriptor = capability.perform(&mut creds).await.unwrap();

        assert_eq!(descriptor.method, "GET");
        assert!(
            descriptor.url.as_str().contains(TEST_SUBJECT),
            "URL should contain subject prefix"
        );
    }

    #[dialog_common::test]
    async fn it_performs_archive_put() {
        let address = Address::new(
            "https://s3.us-east-1.amazonaws.com",
            "us-east-1",
            "my-bucket",
        );
        let mut creds = Credentials::public(address, TEST_SUBJECT).unwrap();

        let checksum = crate::Checksum::Sha256([0u8; 32]);
        let digest = [0u8; 32];

        let capability: Capability<access_archive::Put> = Subject::from(TEST_SUBJECT)
            .attenuate(archive::Archive)
            .attenuate(archive::Catalog::new("blobs"))
            .invoke(access_archive::Put::new(digest, checksum));

        let descriptor = capability.perform(&mut creds).await.unwrap();

        assert_eq!(descriptor.method, "PUT");
        assert!(
            descriptor.url.as_str().contains(TEST_SUBJECT),
            "URL should contain subject prefix"
        );
        assert!(
            descriptor
                .headers
                .iter()
                .any(|(k, _)| k == "x-amz-checksum-sha256")
        );
    }
}

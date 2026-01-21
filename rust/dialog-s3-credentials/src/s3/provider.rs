//! Provider and Access implementations for S3 credentials.
//!
//! This module implements the capability-based authorization flow for direct S3 access.
//! S3 credentials self-issue authorization when they own the bucket (subject DID matches).

use async_trait::async_trait;
use dialog_common::capability::{
    Ability, Access, Authorization, AuthorizationError, Authorized, Capability, Claim,
    Principal, Proof, Provider,
};
use dialog_common::capability::Did;
use dialog_common::ConditionalSend;

use crate::capability::{archive, memory, storage};
use crate::s3::extract_host;
use crate::{AuthorizationError as S3Error, RequestDescriptor};

use super::Credentials;

/// Self-issued authorization for direct S3 access.
///
/// For S3 credentials that own the bucket, authorization is self-issued
/// (empty proof). This struct holds the claim that was authorized.
#[derive(Debug, Clone)]
pub struct S3Authorization<C> {
    claim: Claim<C>,
}

impl<C: Ability> Authorization<C> for S3Authorization<C> {
    fn claim(&self) -> &Claim<C> {
        &self.claim
    }

    fn proof(&self) -> Proof {
        Proof::empty()
    }

    fn issue<A: dialog_common::capability::Authority>(
        capability: C,
        issuer: &A,
    ) -> Result<Self, AuthorizationError> {
        // For self-issue, subject must match issuer
        if capability.subject() == issuer.did() {
            let claim = Claim::new(capability, issuer.did().clone());
            Ok(Self { claim })
        } else {
            Err(AuthorizationError::NotOwner {
                subject: capability.subject().clone(),
                issuer: issuer.did().clone(),
            })
        }
    }

    fn delegate<A: dialog_common::capability::Authority>(
        &self,
        _audience: &Did,
        _issuer: &A,
    ) -> Result<dialog_common::capability::Delegation<C, Self>, AuthorizationError>
    where
        C: Clone,
        Self: Clone,
    {
        // Direct S3 credentials cannot delegate - use UCAN for delegation
        Err(AuthorizationError::PolicyViolation {
            message: "Direct S3 credentials cannot delegate. Use UCAN credentials for delegation."
                .into(),
        })
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
        // satisfy the Authority trait required by acquire().
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
    type Authorization<C: Ability + Clone + ConditionalSend + 'static> = S3Authorization<C>;
    type Error = AccessError;

    async fn claim<C: Ability + Clone + ConditionalSend + 'static>(
        &self,
        claim: Claim<C>,
    ) -> Result<Self::Authorization<C>, Self::Error> {
        // For direct S3, we only support self-issued authorization.
        // If the claim's subject matches our DID, we self-issue.
        // Otherwise, we need delegation (not supported for direct S3).
        if claim.subject() == self.did() {
            // Self-issue
            Ok(S3Authorization { claim })
        } else {
            Err(AccessError::Authorization(AuthorizationError::NoDelegationChain {
                subject: claim.subject().clone(),
                audience: claim.audience().clone(),
            }))
        }
    }
}

// --- Helper for building paths with subject prefix ---

impl Credentials {
    /// Build a full path with subject prefix.
    /// Returns `{subject}/{path}` or just `{path}` if subject is empty.
    fn prefixed_path(&self, path: &str) -> String {
        let subject = self.subject();
        if subject.is_empty() {
            path.to_string()
        } else if path.is_empty() {
            subject.to_string()
        } else {
            format!("{}/{}", subject, path)
        }
    }
}

// --- Provider implementations for authorized capabilities ---
//
// Each effect type needs a Provider implementation that generates the RequestDescriptor.

// Provider for storage::Get
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl Provider<Authorized<Capability<storage::Get>, S3Authorization<Capability<storage::Get>>>>
    for Credentials
{
    async fn execute(
        &mut self,
        authorized: Authorized<Capability<storage::Get>, S3Authorization<Capability<storage::Get>>>,
    ) -> RequestDescriptor {
        let cap = authorized.capability();
        let store: &storage::Store = cap.policy();
        let get: &storage::Get = cap.policy();

        let relative_path = storage::path(store, &get.key);
        let path = self.prefixed_path(&relative_path);

        // Build URL and descriptor based on credentials type
        match self {
            Credentials::Public(creds) => {
                let url = creds.build_url(&path).expect("Failed to build URL");
                let host = extract_host(&url).expect("Failed to extract host");
                RequestDescriptor {
                    url,
                    method: "GET".to_string(),
                    headers: vec![("host".to_string(), host)],
                }
            }
            Credentials::Private(_creds) => {
                // TODO: Private credential signing needs capability-based Claim implementation
                todo!("Private credential signing for storage::Get not yet implemented")
            }
        }
    }
}

// Provider for storage::Set
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl Provider<Authorized<Capability<storage::Set>, S3Authorization<Capability<storage::Set>>>>
    for Credentials
{
    async fn execute(
        &mut self,
        authorized: Authorized<Capability<storage::Set>, S3Authorization<Capability<storage::Set>>>,
    ) -> RequestDescriptor {
        let cap = authorized.capability();
        let store: &storage::Store = cap.policy();
        let set: &storage::Set = cap.policy();

        let relative_path = storage::path(store, &set.key);
        let path = self.prefixed_path(&relative_path);

        match self {
            Credentials::Public(creds) => {
                let url = creds.build_url(&path).expect("Failed to build URL");
                let host = extract_host(&url).expect("Failed to extract host");
                let checksum_header = format!("x-amz-checksum-{}", set.checksum.name());
                RequestDescriptor {
                    url,
                    method: "PUT".to_string(),
                    headers: vec![
                        ("host".to_string(), host),
                        (checksum_header, set.checksum.to_string()),
                    ],
                }
            }
            Credentials::Private(_creds) => {
                // TODO: Private credential signing needs capability-based Claim implementation
                todo!("Private credential signing for storage::Set not yet implemented")
            }
        }
    }
}

// Provider for storage::Delete
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl Provider<Authorized<Capability<storage::Delete>, S3Authorization<Capability<storage::Delete>>>>
    for Credentials
{
    async fn execute(
        &mut self,
        authorized: Authorized<Capability<storage::Delete>, S3Authorization<Capability<storage::Delete>>>,
    ) -> RequestDescriptor {
        let cap = authorized.capability();
        let store: &storage::Store = cap.policy();
        let delete: &storage::Delete = cap.policy();

        let relative_path = storage::path(store, &delete.key);
        let path = self.prefixed_path(&relative_path);

        match self {
            Credentials::Public(creds) => {
                let url = creds.build_url(&path).expect("Failed to build URL");
                let host = extract_host(&url).expect("Failed to extract host");
                RequestDescriptor {
                    url,
                    method: "DELETE".to_string(),
                    headers: vec![("host".to_string(), host)],
                }
            }
            Credentials::Private(_creds) => {
                // TODO: Private credential signing needs capability-based Claim implementation
                todo!("Private credential signing for storage::Delete not yet implemented")
            }
        }
    }
}

// Provider for storage::List
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl Provider<Authorized<Capability<storage::List>, S3Authorization<Capability<storage::List>>>>
    for Credentials
{
    async fn execute(
        &mut self,
        authorized: Authorized<Capability<storage::List>, S3Authorization<Capability<storage::List>>>,
    ) -> RequestDescriptor {
        let cap = authorized.capability();
        let store: &storage::Store = cap.policy();
        let list: &storage::List = cap.policy();

        // For list, the prefix includes subject + store
        let prefix = self.prefixed_path(&store.name);

        match self {
            Credentials::Public(creds) => {
                let mut url = creds.build_url("").expect("Failed to build URL");
                {
                    let mut query = url.query_pairs_mut();
                    query.append_pair("list-type", "2");
                    query.append_pair("prefix", &prefix);
                    if let Some(token) = &list.continuation_token {
                        query.append_pair("continuation-token", token);
                    }
                }
                let host = extract_host(&url).expect("Failed to extract host");
                RequestDescriptor {
                    url,
                    method: "GET".to_string(),
                    headers: vec![("host".to_string(), host)],
                }
            }
            Credentials::Private(_creds) => {
                // TODO: Private credential signing needs capability-based Claim implementation
                todo!("Private credential signing for storage::List not yet implemented")
            }
        }
    }
}

// Provider for memory::Resolve
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl Provider<Authorized<Capability<memory::Resolve>, S3Authorization<Capability<memory::Resolve>>>>
    for Credentials
{
    async fn execute(
        &mut self,
        authorized: Authorized<Capability<memory::Resolve>, S3Authorization<Capability<memory::Resolve>>>,
    ) -> RequestDescriptor {
        let cap = authorized.capability();
        let space: &memory::Space = cap.policy();
        let cell: &memory::Cell = cap.policy();

        let relative_path = memory::path(space, cell);
        let path = self.prefixed_path(&relative_path);

        match self {
            Credentials::Public(creds) => {
                let url = creds.build_url(&path).expect("Failed to build URL");
                let host = extract_host(&url).expect("Failed to extract host");
                RequestDescriptor {
                    url,
                    method: "GET".to_string(),
                    headers: vec![("host".to_string(), host)],
                }
            }
            Credentials::Private(_creds) => {
                // TODO: Private credential signing needs capability-based Claim implementation
                todo!("Private credential signing for memory::Resolve not yet implemented")
            }
        }
    }
}

// Provider for memory::Publish
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl Provider<Authorized<Capability<memory::Publish>, S3Authorization<Capability<memory::Publish>>>>
    for Credentials
{
    async fn execute(
        &mut self,
        authorized: Authorized<Capability<memory::Publish>, S3Authorization<Capability<memory::Publish>>>,
    ) -> RequestDescriptor {
        let cap = authorized.capability();
        let space: &memory::Space = cap.policy();
        let cell: &memory::Cell = cap.policy();
        let publish: &memory::Publish = cap.policy();

        let relative_path = memory::path(space, cell);
        let path = self.prefixed_path(&relative_path);

        match self {
            Credentials::Public(creds) => {
                let url = creds.build_url(&path).expect("Failed to build URL");
                let host = extract_host(&url).expect("Failed to extract host");
                let checksum_header = format!("x-amz-checksum-{}", publish.checksum.name());

                let mut headers = vec![
                    ("host".to_string(), host),
                    (checksum_header, publish.checksum.to_string()),
                ];

                // Add precondition headers
                match publish.precondition() {
                    memory::Precondition::IfMatch(edition) => {
                        headers.push(("if-match".to_string(), edition));
                    }
                    memory::Precondition::IfNoneMatch => {
                        headers.push(("if-none-match".to_string(), "*".to_string()));
                    }
                    memory::Precondition::None => {}
                }

                RequestDescriptor {
                    url,
                    method: "PUT".to_string(),
                    headers,
                }
            }
            Credentials::Private(_creds) => {
                // TODO: Private credential signing needs capability-based Claim implementation
                todo!("Private credential signing for memory::Publish not yet implemented")
            }
        }
    }
}

// Provider for memory::Retract
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl Provider<Authorized<Capability<memory::Retract>, S3Authorization<Capability<memory::Retract>>>>
    for Credentials
{
    async fn execute(
        &mut self,
        authorized: Authorized<Capability<memory::Retract>, S3Authorization<Capability<memory::Retract>>>,
    ) -> RequestDescriptor {
        let cap = authorized.capability();
        let space: &memory::Space = cap.policy();
        let cell: &memory::Cell = cap.policy();
        let retract: &memory::Retract = cap.policy();

        let relative_path = memory::path(space, cell);
        let path = self.prefixed_path(&relative_path);

        match self {
            Credentials::Public(creds) => {
                let url = creds.build_url(&path).expect("Failed to build URL");
                let host = extract_host(&url).expect("Failed to extract host");
                RequestDescriptor {
                    url,
                    method: "DELETE".to_string(),
                    headers: vec![
                        ("host".to_string(), host),
                        ("if-match".to_string(), retract.when.clone()),
                    ],
                }
            }
            Credentials::Private(_creds) => {
                // TODO: Private credential signing needs capability-based Claim implementation
                todo!("Private credential signing for memory::Retract not yet implemented")
            }
        }
    }
}

// Provider for archive::Get
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl Provider<Authorized<Capability<archive::Get>, S3Authorization<Capability<archive::Get>>>>
    for Credentials
{
    async fn execute(
        &mut self,
        authorized: Authorized<Capability<archive::Get>, S3Authorization<Capability<archive::Get>>>,
    ) -> RequestDescriptor {
        let cap = authorized.capability();
        let catalog: &archive::Catalog = cap.policy();
        let get: &archive::Get = cap.policy();

        let relative_path = archive::path(catalog, &get.digest);
        let path = self.prefixed_path(&relative_path);

        match self {
            Credentials::Public(creds) => {
                let url = creds.build_url(&path).expect("Failed to build URL");
                let host = extract_host(&url).expect("Failed to extract host");
                RequestDescriptor {
                    url,
                    method: "GET".to_string(),
                    headers: vec![("host".to_string(), host)],
                }
            }
            Credentials::Private(_creds) => {
                // TODO: Private credential signing needs capability-based Claim implementation
                todo!("Private credential signing for archive::Get not yet implemented")
            }
        }
    }
}

// Provider for archive::Put
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl Provider<Authorized<Capability<archive::Put>, S3Authorization<Capability<archive::Put>>>>
    for Credentials
{
    async fn execute(
        &mut self,
        authorized: Authorized<Capability<archive::Put>, S3Authorization<Capability<archive::Put>>>,
    ) -> RequestDescriptor {
        let cap = authorized.capability();
        let catalog: &archive::Catalog = cap.policy();
        let put: &archive::Put = cap.policy();

        let relative_path = archive::path(catalog, &put.digest);
        let path = self.prefixed_path(&relative_path);

        match self {
            Credentials::Public(creds) => {
                let url = creds.build_url(&path).expect("Failed to build URL");
                let host = extract_host(&url).expect("Failed to extract host");
                let checksum_header = format!("x-amz-checksum-{}", put.checksum.name());
                RequestDescriptor {
                    url,
                    method: "PUT".to_string(),
                    headers: vec![
                        ("host".to_string(), host),
                        (checksum_header, put.checksum.to_string()),
                    ],
                }
            }
            Credentials::Private(_creds) => {
                // TODO: Private credential signing needs capability-based Claim implementation
                todo!("Private credential signing for archive::Put not yet implemented")
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Address;
    use dialog_common::capability::Subject;

    const TEST_SUBJECT: &str = "did:key:zTestSubject";

    #[dialog_common::test]
    async fn test_acquire_perform_storage_get() {
        let address = Address::new("https://s3.us-east-1.amazonaws.com", "us-east-1", "my-bucket");
        let mut creds = Credentials::public(address, TEST_SUBJECT).unwrap();

        // The subject DID is our identity (path prefix in the bucket)
        let subject_did = creds.subject().clone();

        // Build capability chain
        let capability: Capability<storage::Get> = Subject::from(subject_did)
            .attenuate(storage::Storage)
            .attenuate(storage::Store::new("index"))
            .invoke(storage::Get::new("my-key"));

        // Acquire authorization (self-issued since subject matches)
        let authorized = capability.acquire(&creds).await.unwrap();

        // Perform to get RequestDescriptor
        let descriptor = authorized.perform(&mut creds).await;

        assert_eq!(descriptor.method, "GET");
        assert!(descriptor.url.as_str().contains("my-bucket"));
        // Path should be subject/store/key
        assert!(descriptor.url.as_str().contains(TEST_SUBJECT), "URL should contain subject prefix");
        assert!(descriptor.url.as_str().contains("index/my-key"));
    }

    #[dialog_common::test]
    async fn test_acquire_perform_storage_set() {
        let address = Address::new("https://s3.us-east-1.amazonaws.com", "us-east-1", "my-bucket");
        let mut creds = Credentials::public(address, TEST_SUBJECT).unwrap();

        let subject_did = creds.subject().clone();
        let checksum = crate::Checksum::Sha256([0u8; 32]);

        let capability: Capability<storage::Set> = Subject::from(subject_did)
            .attenuate(storage::Storage)
            .attenuate(storage::Store::new("blob"))
            .invoke(storage::Set::new("my-key", checksum));

        let authorized = capability.acquire(&creds).await.unwrap();
        let descriptor = authorized.perform(&mut creds).await;

        assert_eq!(descriptor.method, "PUT");
        assert!(descriptor.headers.iter().any(|(k, _)| k == "x-amz-checksum-sha256"));
    }

    #[dialog_common::test]
    async fn test_acquire_perform_memory_resolve() {
        let address = Address::new("https://s3.us-east-1.amazonaws.com", "us-east-1", "my-bucket");
        let mut creds = Credentials::public(address, TEST_SUBJECT).unwrap();

        let subject_did = creds.subject().clone();

        let capability: Capability<memory::Resolve> = Subject::from(subject_did)
            .attenuate(memory::Memory)
            .attenuate(memory::Space::new("did:key:zUser"))
            .attenuate(memory::Cell::new("main"))
            .invoke(memory::Resolve);

        let authorized = capability.acquire(&creds).await.unwrap();
        let descriptor = authorized.perform(&mut creds).await;

        assert_eq!(descriptor.method, "GET");
        // Path should include subject prefix
        assert!(descriptor.url.as_str().contains(TEST_SUBJECT), "URL should contain subject prefix");
        assert!(descriptor.url.as_str().contains("did:key:zUser/main"));
    }

    #[dialog_common::test]
    async fn test_acquire_fails_for_wrong_subject() {
        let address = Address::new("https://s3.us-east-1.amazonaws.com", "us-east-1", "my-bucket");
        let creds = Credentials::public(address, TEST_SUBJECT).unwrap();

        // Try to acquire for a different subject (not our subject DID)
        let capability: Capability<storage::Get> = Subject::from("did:key:zOtherSubject")
            .attenuate(storage::Storage)
            .attenuate(storage::Store::new("index"))
            .invoke(storage::Get::new("key"));

        // Should fail because we don't own "did:key:zOtherSubject"
        let result = capability.acquire(&creds).await;
        assert!(result.is_err());
    }

    #[dialog_common::test]
    async fn test_acquire_perform_archive_get() {
        let address = Address::new("https://s3.us-east-1.amazonaws.com", "us-east-1", "my-bucket");
        let mut creds = Credentials::public(address, TEST_SUBJECT).unwrap();

        let subject_did = creds.subject().clone();

        let capability: Capability<archive::Get> = Subject::from(subject_did)
            .attenuate(archive::Archive)
            .attenuate(archive::Catalog::new("blobs"))
            .invoke(archive::Get::new("abc123digest"));

        let authorized = capability.acquire(&creds).await.unwrap();
        let descriptor = authorized.perform(&mut creds).await;

        assert_eq!(descriptor.method, "GET");
        assert!(descriptor.url.as_str().contains(TEST_SUBJECT), "URL should contain subject prefix");
        assert!(descriptor.url.as_str().contains("blobs/abc123digest"));
    }

    #[dialog_common::test]
    async fn test_acquire_perform_archive_put() {
        let address = Address::new("https://s3.us-east-1.amazonaws.com", "us-east-1", "my-bucket");
        let mut creds = Credentials::public(address, TEST_SUBJECT).unwrap();

        let subject_did = creds.subject().clone();
        let checksum = crate::Checksum::Sha256([0u8; 32]);

        let capability: Capability<archive::Put> = Subject::from(subject_did)
            .attenuate(archive::Archive)
            .attenuate(archive::Catalog::new("blobs"))
            .invoke(archive::Put::new("abc123digest", checksum));

        let authorized = capability.acquire(&creds).await.unwrap();
        let descriptor = authorized.perform(&mut creds).await;

        assert_eq!(descriptor.method, "PUT");
        assert!(descriptor.url.as_str().contains(TEST_SUBJECT), "URL should contain subject prefix");
        assert!(descriptor.url.as_str().contains("blobs/abc123digest"));
        assert!(descriptor.headers.iter().any(|(k, _)| k == "x-amz-checksum-sha256"));
    }
}

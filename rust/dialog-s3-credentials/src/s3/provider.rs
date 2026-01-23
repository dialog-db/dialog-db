//! Provider and Access implementations for S3 credentials.
//!
//! This module implements the capability-based authorization flow for direct S3 access.
//! S3 credentials self-issue authorization when they own the bucket (subject DID matches).

use async_trait::async_trait;
use dialog_common::ConditionalSend;
use dialog_common::capability::{Ability, Access, Capability, Claim, Effect, Provider};

use super::{Credentials, S3Authorization};
use crate::capability::{AccessError, AuthorizedRequest, S3Request};

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl Access for Credentials {
    type Authorization = S3Authorization;
    type Error = AccessError;

    async fn claim<C: Ability + Clone + ConditionalSend + 'static>(
        &self,
        claim: Claim<C>,
    ) -> Result<Self::Authorization, Self::Error> {
        // Authorization captures credentials so that pre-singed URL can
        // be issued when requested
        Ok(S3Authorization::new(
            self.clone(),
            claim.subject().clone(),
            claim.audience().clone(),
            claim.command(),
        ))
    }
}

/// Blanket implementation provider ability to
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<Do> Provider<Do> for Credentials
where
    Do: Effect<Output = Result<AuthorizedRequest, AccessError>> + 'static,
    Capability<Do>: ConditionalSend + S3Request,
{
    async fn execute(
        &mut self,
        capability: Capability<Do>,
    ) -> Result<AuthorizedRequest, AccessError> {
        self.grant(&capability).await
    }
}

// --- Provider implementations for capabilities ---
//
// Each effect type needs a Provider implementation that generates the RequestDescriptor.
// These providers work with Capability<Fx> directly. Since Capability<access::*::Fx>
// implements the access::Claim trait, we delegate to Credentials::authorize.

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Address;
    use crate::capability::{archive, memory, storage};
    use base58::ToBase58;
    use dialog_common::capability::Subject;

    const TEST_SUBJECT: &str = "did:key:zTestSubject";

    fn public_creds() -> Credentials {
        let address = Address::new(
            "https://s3.us-east-1.amazonaws.com",
            "us-east-1",
            "my-bucket",
        );
        Credentials::public(address).unwrap()
    }

    fn private_creds() -> Credentials {
        let address = Address::new(
            "https://s3.us-east-1.amazonaws.com",
            "us-east-1",
            "my-bucket",
        );
        Credentials::private(
            address,
            "AKIAIOSFODNN7EXAMPLE",
            "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
        )
        .unwrap()
    }

    fn localhost_creds() -> Credentials {
        let address = Address::new("http://localhost:9000", "us-east-1", "test-bucket");
        Credentials::public(address).unwrap()
    }

    // ==================== Storage Operations ====================

    #[dialog_common::test]
    async fn storage_get_generates_correct_url() {
        let mut creds = public_creds();
        let key = b"test-key";

        let capability = Subject::from(TEST_SUBJECT)
            .attenuate(storage::Storage)
            .attenuate(storage::Store::new("index"))
            .invoke(storage::Get::new(key));

        let req = capability.perform(&mut creds).await.unwrap();

        assert_eq!(req.method, "GET");
        assert_eq!(
            req.url.path(),
            format!("/{}/index/{}", TEST_SUBJECT, key.to_base58())
        );
        assert!(req.url.host_str().unwrap().contains("my-bucket"));
    }

    #[dialog_common::test]
    async fn storage_get_with_binary_key() {
        let mut creds = public_creds();
        let binary_key: [u8; 32] = [
            0xde, 0xad, 0xbe, 0xef, 0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99,
            0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08,
            0x09, 0x0a, 0x0b, 0x0c,
        ];

        let capability = Subject::from(TEST_SUBJECT)
            .attenuate(storage::Storage)
            .attenuate(storage::Store::new("blob"))
            .invoke(storage::Get::new(binary_key));

        let req = capability.perform(&mut creds).await.unwrap();

        assert_eq!(req.method, "GET");
        // Binary key should be base58 encoded in path
        assert!(req.url.path().contains(&binary_key.to_base58()));
    }

    #[dialog_common::test]
    async fn storage_set_generates_correct_url_and_checksum_header() {
        let mut creds = public_creds();
        let key = b"my-key";
        let checksum_bytes = [0x12u8; 32];
        let checksum = crate::Checksum::Sha256(checksum_bytes);

        let capability = Subject::from(TEST_SUBJECT)
            .attenuate(storage::Storage)
            .attenuate(storage::Store::new("blob"))
            .invoke(storage::Set::new(key, checksum));

        let req = capability.perform(&mut creds).await.unwrap();

        assert_eq!(req.method, "PUT");
        assert_eq!(
            req.url.path(),
            format!("/{}/blob/{}", TEST_SUBJECT, key.to_base58())
        );

        // Should have checksum header
        let checksum_header = req
            .headers
            .iter()
            .find(|(k, _)| k == "x-amz-checksum-sha256");
        assert!(checksum_header.is_some(), "Should have checksum header");

        // Verify checksum value is base64 encoded
        let (_, value) = checksum_header.unwrap();
        assert!(!value.is_empty());
    }

    #[dialog_common::test]
    async fn storage_delete_generates_correct_url() {
        let mut creds = public_creds();
        let key = b"key-to-delete";

        let capability = Subject::from(TEST_SUBJECT)
            .attenuate(storage::Storage)
            .attenuate(storage::Store::new("index"))
            .invoke(storage::Delete::new(key));

        let req = capability.perform(&mut creds).await.unwrap();

        assert_eq!(req.method, "DELETE");
        assert_eq!(
            req.url.path(),
            format!("/{}/index/{}", TEST_SUBJECT, key.to_base58())
        );
    }

    #[dialog_common::test]
    async fn storage_list_generates_correct_query_params() {
        let mut creds = public_creds();

        let capability = Subject::from(TEST_SUBJECT)
            .attenuate(storage::Storage)
            .attenuate(storage::Store::new("index"))
            .invoke(storage::List::new(None));

        let req = capability.perform(&mut creds).await.unwrap();

        assert_eq!(req.method, "GET");

        // Should have list-type=2 query param
        let query: Vec<(String, String)> = req
            .url
            .query_pairs()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        assert!(query.iter().any(|(k, v)| k == "list-type" && v == "2"));

        // Should have prefix query param
        let prefix = query.iter().find(|(k, _)| k == "prefix");
        assert!(prefix.is_some());
        let (_, prefix_value) = prefix.unwrap();
        assert!(prefix_value.contains(TEST_SUBJECT));
        assert!(prefix_value.contains("index"));
    }

    #[dialog_common::test]
    async fn storage_list_with_continuation_token() {
        let mut creds = public_creds();
        let token = "next-page-token-abc123";

        let capability = Subject::from(TEST_SUBJECT)
            .attenuate(storage::Storage)
            .attenuate(storage::Store::new("data"))
            .invoke(storage::List::new(Some(token.to_string())));

        let req = capability.perform(&mut creds).await.unwrap();

        let query: Vec<(String, String)> = req
            .url
            .query_pairs()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        let cont_token = query.iter().find(|(k, _)| k == "continuation-token");
        assert!(cont_token.is_some());
        assert_eq!(cont_token.unwrap().1, token);
    }

    // ==================== Memory Operations ====================

    #[dialog_common::test]
    async fn memory_resolve_generates_correct_url() {
        let mut creds = public_creds();

        let capability = Subject::from(TEST_SUBJECT)
            .attenuate(memory::Memory)
            .attenuate(memory::Space::new("did:key:zUser123"))
            .attenuate(memory::Cell::new("main"))
            .invoke(memory::Resolve);

        let req = capability.perform(&mut creds).await.unwrap();

        assert_eq!(req.method, "GET");
        assert_eq!(
            req.url.path(),
            format!("/{}/did:key:zUser123/main", TEST_SUBJECT)
        );
    }

    #[dialog_common::test]
    async fn memory_publish_with_no_prior_edition() {
        let mut creds = public_creds();
        let checksum = crate::Checksum::Sha256([0xab; 32]);

        let capability = Subject::from(TEST_SUBJECT)
            .attenuate(memory::Memory)
            .attenuate(memory::Space::new("did:key:zSpace"))
            .attenuate(memory::Cell::new("head"))
            .invoke(memory::Publish {
                checksum,
                when: None, // No prior edition - creating new cell
            });

        let req = capability.perform(&mut creds).await.unwrap();

        assert_eq!(req.method, "PUT");
        assert_eq!(
            req.url.path(),
            format!("/{}/did:key:zSpace/head", TEST_SUBJECT)
        );

        // Should have checksum header
        assert!(
            req.headers
                .iter()
                .any(|(k, _)| k == "x-amz-checksum-sha256")
        );
    }

    #[dialog_common::test]
    async fn memory_publish_with_prior_edition() {
        let mut creds = public_creds();
        let checksum = crate::Checksum::Sha256([0xcd; 32]);
        let prior_etag = "abc123etag".to_string();

        let capability = Subject::from(TEST_SUBJECT)
            .attenuate(memory::Memory)
            .attenuate(memory::Space::new("did:key:zSpace"))
            .attenuate(memory::Cell::new("main"))
            .invoke(memory::Publish {
                checksum,
                when: Some(prior_etag),
            });

        let req = capability.perform(&mut creds).await.unwrap();

        assert_eq!(req.method, "PUT");
        assert!(
            req.headers
                .iter()
                .any(|(k, _)| k == "x-amz-checksum-sha256")
        );
    }

    #[dialog_common::test]
    async fn memory_retract_generates_correct_url() {
        let mut creds = public_creds();

        let capability = Subject::from(TEST_SUBJECT)
            .attenuate(memory::Memory)
            .attenuate(memory::Space::new("did:key:zOwner"))
            .attenuate(memory::Cell::new("temp"))
            .invoke(memory::Retract::new("etag-to-match"));

        let req = capability.perform(&mut creds).await.unwrap();

        assert_eq!(req.method, "DELETE");
        assert_eq!(
            req.url.path(),
            format!("/{}/did:key:zOwner/temp", TEST_SUBJECT)
        );
    }

    // ==================== Archive Operations ====================

    #[dialog_common::test]
    async fn archive_get_generates_correct_url() {
        let mut creds = public_creds();
        let digest: [u8; 32] = [0x42; 32];

        let capability = Subject::from(TEST_SUBJECT)
            .attenuate(archive::Archive)
            .attenuate(archive::Catalog::new("blobs"))
            .invoke(archive::Get::new(digest));

        let req = capability.perform(&mut creds).await.unwrap();

        assert_eq!(req.method, "GET");
        assert_eq!(
            req.url.path(),
            format!("/{}/blobs/{}", TEST_SUBJECT, digest.to_base58())
        );
    }

    #[dialog_common::test]
    async fn archive_put_generates_correct_url_and_checksum() {
        let mut creds = public_creds();
        let digest: [u8; 32] = [0x99; 32];
        let checksum = crate::Checksum::Sha256([0x11; 32]);

        let capability = Subject::from(TEST_SUBJECT)
            .attenuate(archive::Archive)
            .attenuate(archive::Catalog::new("index"))
            .invoke(archive::Put::new(digest, checksum));

        let req = capability.perform(&mut creds).await.unwrap();

        assert_eq!(req.method, "PUT");
        assert_eq!(
            req.url.path(),
            format!("/{}/index/{}", TEST_SUBJECT, digest.to_base58())
        );
        assert!(
            req.headers
                .iter()
                .any(|(k, _)| k == "x-amz-checksum-sha256")
        );
    }

    #[dialog_common::test]
    async fn private_creds_generate_signed_url_for_get() {
        let mut creds = private_creds();
        let key = b"signed-key";

        let capability = Subject::from(TEST_SUBJECT)
            .attenuate(storage::Storage)
            .attenuate(storage::Store::new("data"))
            .invoke(storage::Get::new(key));

        let req = capability.perform(&mut creds).await.unwrap();

        assert_eq!(req.method, "GET");

        // Should have AWS signature query params
        let query: Vec<(String, String)> = req
            .url
            .query_pairs()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        assert!(query.iter().any(|(k, _)| k == "X-Amz-Algorithm"));
        assert!(query.iter().any(|(k, _)| k == "X-Amz-Credential"));
        assert!(query.iter().any(|(k, _)| k == "X-Amz-Date"));
        assert!(query.iter().any(|(k, _)| k == "X-Amz-Expires"));
        assert!(query.iter().any(|(k, _)| k == "X-Amz-SignedHeaders"));
        assert!(query.iter().any(|(k, _)| k == "X-Amz-Signature"));
    }

    #[dialog_common::test]
    async fn private_creds_generate_signed_url_for_put() {
        let mut creds = private_creds();
        let key = b"upload-key";
        let checksum = crate::Checksum::Sha256([0xff; 32]);

        let capability = Subject::from(TEST_SUBJECT)
            .attenuate(storage::Storage)
            .attenuate(storage::Store::new("uploads"))
            .invoke(storage::Set::new(key, checksum));

        let req = capability.perform(&mut creds).await.unwrap();

        assert_eq!(req.method, "PUT");

        // Should have signature
        let query: Vec<(String, String)> = req
            .url
            .query_pairs()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        assert!(query.iter().any(|(k, _)| k == "X-Amz-Signature"));

        // Should have checksum header
        assert!(
            req.headers
                .iter()
                .any(|(k, _)| k == "x-amz-checksum-sha256")
        );
    }

    #[dialog_common::test]
    async fn private_creds_generate_signed_url_for_list() {
        let mut creds = private_creds();

        let capability = Subject::from(TEST_SUBJECT)
            .attenuate(storage::Storage)
            .attenuate(storage::Store::new("files"))
            .invoke(storage::List::new(None));

        let req = capability.perform(&mut creds).await.unwrap();

        assert_eq!(req.method, "GET");

        let query: Vec<(String, String)> = req
            .url
            .query_pairs()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();

        // Should have list params
        assert!(query.iter().any(|(k, v)| k == "list-type" && v == "2"));
        assert!(query.iter().any(|(k, _)| k == "prefix"));

        // Should have signature
        assert!(query.iter().any(|(k, _)| k == "X-Amz-Signature"));
    }

    // ==================== URL Styles ====================

    #[dialog_common::test]
    async fn uses_virtual_hosted_style_for_aws() {
        let mut creds = public_creds();

        let capability = Subject::from(TEST_SUBJECT)
            .attenuate(storage::Storage)
            .attenuate(storage::Store::new("test"))
            .invoke(storage::Get::new(b"key"));

        let req = capability.perform(&mut creds).await.unwrap();

        // Virtual hosted style: bucket is in the hostname
        assert!(req.url.host_str().unwrap().starts_with("my-bucket."));
    }

    #[dialog_common::test]
    async fn uses_path_style_for_localhost() {
        let mut creds = localhost_creds();

        let capability = Subject::from(TEST_SUBJECT)
            .attenuate(storage::Storage)
            .attenuate(storage::Store::new("test"))
            .invoke(storage::Get::new(b"key"));

        let req = capability.perform(&mut creds).await.unwrap();

        // Path style: bucket is in the path
        assert_eq!(req.url.host_str().unwrap(), "localhost");
        assert!(req.url.path().starts_with("/test-bucket/"));
    }

    // ==================== Different Stores ====================

    #[dialog_common::test]
    async fn supports_different_store_names() {
        let mut creds = public_creds();
        let stores = ["index", "blob", "metadata", "cache", ""];

        for store_name in stores {
            let capability = Subject::from(TEST_SUBJECT)
                .attenuate(storage::Storage)
                .attenuate(storage::Store::new(store_name))
                .invoke(storage::Get::new(b"key"));

            let req = capability.perform(&mut creds).await.unwrap();

            if store_name.is_empty() {
                assert!(req.url.path().contains(&format!("/{}/", TEST_SUBJECT)));
            } else {
                assert!(
                    req.url
                        .path()
                        .contains(&format!("/{}/{}/", TEST_SUBJECT, store_name))
                );
            }
        }
    }

    // ==================== Different Catalogs ====================

    #[dialog_common::test]
    async fn supports_different_catalog_names() {
        let mut creds = public_creds();
        let catalogs = ["blobs", "index", "refs"];
        let digest: [u8; 32] = [0x55; 32];

        for catalog_name in catalogs {
            let capability = Subject::from(TEST_SUBJECT)
                .attenuate(archive::Archive)
                .attenuate(archive::Catalog::new(catalog_name))
                .invoke(archive::Get::new(digest));

            let req = capability.perform(&mut creds).await.unwrap();

            assert!(
                req.url
                    .path()
                    .contains(&format!("/{}/{}/", TEST_SUBJECT, catalog_name))
            );
        }
    }
}

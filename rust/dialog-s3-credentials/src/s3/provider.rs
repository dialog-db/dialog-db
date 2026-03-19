//! Remote and Provider implementations for S3 credentials.
//!
//! S3 credentials can directly grant authorization by presigning requests.
//! The `Remote` impl produces `AuthorizedRequest` (the presigned URL) directly.

use async_trait::async_trait;
use dialog_capability::{Authorization, Capability, Constraint, Provider, credential};

use super::Credentials;
use crate::capability::{AuthorizedRequest, S3Request};

/// Intermediate proof for S3 authorization.
///
/// S3 presigning doesn't need an external service, so the "proof" step
/// simply wraps the credentials ready for presigning in the redeem step.
#[derive(Debug, Clone)]
pub struct S3Permit {
    pub(crate) credentials: Credentials,
}

impl S3Permit {
    /// Get the underlying credentials.
    pub fn credentials(&self) -> &Credentials {
        &self.credentials
    }
}

impl credential::Remote for Credentials {
    type Authorization = Credentials;
    type Permit = S3Permit;
    type Access = AuthorizedRequest;
    type Address = url::Url;

    fn address(&self) -> &url::Url {
        match self {
            Credentials::Public(c) => &c.endpoint,
            Credentials::Private(c) => &c.endpoint,
        }
    }

    fn authorization(&self) -> &Credentials {
        self
    }
}

/// Provider for the Authorize step: wraps credentials into an S3Permit.
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<C> Provider<credential::Authorize<C, Credentials>> for Credentials
where
    C: Constraint + Clone + 'static,
    Capability<C>: S3Request,
{
    async fn execute(
        &self,
        input: credential::Authorize<C, Credentials>,
    ) -> Result<Authorization<C, S3Permit>, credential::AuthorizeError> {
        let permit = S3Permit {
            credentials: input.authorization.clone(),
        };
        Ok(Authorization::new(input.capability, permit))
    }
}

/// Provider for the Redeem step: presigns the URL using the S3 credentials.
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<C> Provider<credential::Redeem<C, Credentials>> for Credentials
where
    C: Constraint + Clone + 'static,
    Capability<C>: S3Request,
{
    async fn execute(
        &self,
        input: credential::Redeem<C, Credentials>,
    ) -> Result<Authorization<C, AuthorizedRequest>, credential::RedeemError> {
        let (capability, permit) = input.authorization.into_parts();
        let authorized_request = permit
            .credentials
            .grant(&capability)
            .await
            .map_err(|e| credential::RedeemError::Rejected(e.to_string()))?;
        Ok(Authorization::new(capability, authorized_request))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Address;
    use crate::capability::{archive, memory, storage};
    use base58::ToBase58;
    use dialog_capability::{Did, Subject, did};

    /// Test environment that satisfies the Provider bounds for Authorize and Redeem.
    /// S3 credentials implement these directly, so we use the credentials themselves.
    fn test_subject() -> Did {
        did!("key:zTestSubject")
    }

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

    /// Helper to authorize a capability using the new Remote flow.
    async fn authorize<C>(creds: &Credentials, capability: Capability<C>) -> AuthorizedRequest
    where
        C: Constraint + Clone + 'static,
        Capability<C>: S3Request,
    {
        use credential::Remote;
        // Use the Provider impls directly since the capability is already built.
        let authorize_input = credential::Authorize::<C, Credentials> {
            authorization: creds.authorization().clone(),
            address: creds.address().clone(),
            capability,
        };
        let authorization: Authorization<C, S3Permit> = <Credentials as Provider<
            credential::Authorize<C, Credentials>,
        >>::execute(creds, authorize_input)
        .await
        .unwrap();

        let redeem_input = credential::Redeem::<C, Credentials> {
            authorization,
            address: creds.address().clone(),
        };
        let result = <Credentials as Provider<credential::Redeem<C, Credentials>>>::execute(
            creds,
            redeem_input,
        )
        .await
        .unwrap();

        result.into_site()
    }

    #[dialog_common::test]
    async fn it_generates_correct_url_for_storage_get() {
        let creds = public_creds();
        let key = b"test-key";

        let capability = Subject::from(test_subject())
            .attenuate(storage::Storage)
            .attenuate(storage::Store::new("index"))
            .invoke(storage::Get::new(key));

        let req = authorize(&creds, capability).await;

        assert_eq!(req.method, "GET");
        assert_eq!(
            req.url.path(),
            format!("/{}/index/{}", TEST_SUBJECT, key.to_base58())
        );
        assert!(req.url.host_str().unwrap().contains("my-bucket"));
    }

    #[dialog_common::test]
    async fn it_handles_binary_key_for_storage_get() {
        let creds = public_creds();
        let binary_key: [u8; 32] = [
            0xde, 0xad, 0xbe, 0xef, 0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99,
            0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08,
            0x09, 0x0a, 0x0b, 0x0c,
        ];

        let capability = Subject::from(test_subject())
            .attenuate(storage::Storage)
            .attenuate(storage::Store::new("blob"))
            .invoke(storage::Get::new(binary_key));

        let req = authorize(&creds, capability).await;

        assert_eq!(req.method, "GET");
        assert!(req.url.path().contains(&binary_key.to_base58()));
    }

    #[dialog_common::test]
    async fn it_generates_correct_url_and_checksum_for_storage_set() {
        let creds = public_creds();
        let key = b"my-key";
        let checksum_bytes = [0x12u8; 32];
        let checksum = crate::Checksum::Sha256(checksum_bytes);

        let capability = Subject::from(test_subject())
            .attenuate(storage::Storage)
            .attenuate(storage::Store::new("blob"))
            .invoke(storage::Set::new(key, checksum));

        let req = authorize(&creds, capability).await;

        assert_eq!(req.method, "PUT");
        assert_eq!(
            req.url.path(),
            format!("/{}/blob/{}", TEST_SUBJECT, key.to_base58())
        );

        let checksum_header = req
            .headers
            .iter()
            .find(|(k, _)| k == "x-amz-checksum-sha256");
        assert!(checksum_header.is_some(), "Should have checksum header");

        let (_, value) = checksum_header.unwrap();
        assert!(!value.is_empty());
    }

    #[dialog_common::test]
    async fn it_generates_correct_url_for_storage_delete() {
        let creds = public_creds();
        let key = b"key-to-delete";

        let capability = Subject::from(test_subject())
            .attenuate(storage::Storage)
            .attenuate(storage::Store::new("index"))
            .invoke(storage::Delete::new(key));

        let req = authorize(&creds, capability).await;

        assert_eq!(req.method, "DELETE");
        assert_eq!(
            req.url.path(),
            format!("/{}/index/{}", TEST_SUBJECT, key.to_base58())
        );
    }

    #[dialog_common::test]
    async fn it_generates_correct_query_params_for_storage_list() {
        let creds = public_creds();

        let capability = Subject::from(test_subject())
            .attenuate(storage::Storage)
            .attenuate(storage::Store::new("index"))
            .invoke(storage::List::new(None));

        let req = authorize(&creds, capability).await;

        assert_eq!(req.method, "GET");

        let query: Vec<(String, String)> = req
            .url
            .query_pairs()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        assert!(query.iter().any(|(k, v)| k == "list-type" && v == "2"));

        let prefix = query.iter().find(|(k, _)| k == "prefix");
        assert!(prefix.is_some());
        let (_, prefix_value) = prefix.unwrap();
        assert!(prefix_value.contains(TEST_SUBJECT));
        assert!(prefix_value.contains("index"));
    }

    #[dialog_common::test]
    async fn it_handles_continuation_token_for_storage_list() {
        let creds = public_creds();
        let token = "next-page-token-abc123";

        let capability = Subject::from(test_subject())
            .attenuate(storage::Storage)
            .attenuate(storage::Store::new("data"))
            .invoke(storage::List::new(Some(token.to_string())));

        let req = authorize(&creds, capability).await;

        let query: Vec<(String, String)> = req
            .url
            .query_pairs()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        let cont_token = query.iter().find(|(k, _)| k == "continuation-token");
        assert!(cont_token.is_some());
        assert_eq!(cont_token.unwrap().1, token);
    }

    #[dialog_common::test]
    async fn it_generates_correct_url_for_memory_resolve() {
        let creds = public_creds();

        let capability = Subject::from(test_subject())
            .attenuate(memory::Memory)
            .attenuate(memory::Space::new("did:key:zUser123"))
            .attenuate(memory::Cell::new("main"))
            .invoke(memory::Resolve);

        let req = authorize(&creds, capability).await;

        assert_eq!(req.method, "GET");
        assert_eq!(
            req.url.path(),
            format!("/{}/did:key:zUser123/main", TEST_SUBJECT)
        );
    }

    #[dialog_common::test]
    async fn it_publishes_memory_without_prior_edition() {
        let creds = public_creds();
        let checksum = crate::Checksum::Sha256([0xab; 32]);

        let capability = Subject::from(test_subject())
            .attenuate(memory::Memory)
            .attenuate(memory::Space::new("did:key:zSpace"))
            .attenuate(memory::Cell::new("head"))
            .invoke(memory::Publish {
                checksum,
                when: None,
            });

        let req = authorize(&creds, capability).await;

        assert_eq!(req.method, "PUT");
        assert_eq!(
            req.url.path(),
            format!("/{}/did:key:zSpace/head", TEST_SUBJECT)
        );

        assert!(
            req.headers
                .iter()
                .any(|(k, _)| k == "x-amz-checksum-sha256")
        );
    }

    #[dialog_common::test]
    async fn it_publishes_memory_with_prior_edition() {
        let creds = public_creds();
        let checksum = crate::Checksum::Sha256([0xcd; 32]);
        let prior_etag = "abc123etag".to_string();

        let capability = Subject::from(test_subject())
            .attenuate(memory::Memory)
            .attenuate(memory::Space::new("did:key:zSpace"))
            .attenuate(memory::Cell::new("main"))
            .invoke(memory::Publish {
                checksum,
                when: Some(prior_etag),
            });

        let req = authorize(&creds, capability).await;

        assert_eq!(req.method, "PUT");
        assert!(
            req.headers
                .iter()
                .any(|(k, _)| k == "x-amz-checksum-sha256")
        );
    }

    #[dialog_common::test]
    async fn it_generates_correct_url_for_memory_retract() {
        let creds = public_creds();

        let capability = Subject::from(test_subject())
            .attenuate(memory::Memory)
            .attenuate(memory::Space::new("did:key:zOwner"))
            .attenuate(memory::Cell::new("temp"))
            .invoke(memory::Retract::new("etag-to-match"));

        let req = authorize(&creds, capability).await;

        assert_eq!(req.method, "DELETE");
        assert_eq!(
            req.url.path(),
            format!("/{}/did:key:zOwner/temp", TEST_SUBJECT)
        );
    }

    #[dialog_common::test]
    async fn it_generates_correct_url_for_archive_get() {
        let creds = public_creds();
        let digest: [u8; 32] = [0x42; 32];

        let capability = Subject::from(test_subject())
            .attenuate(archive::Archive)
            .attenuate(archive::Catalog::new("blobs"))
            .invoke(archive::Get::new(digest));

        let req = authorize(&creds, capability).await;

        assert_eq!(req.method, "GET");
        assert_eq!(
            req.url.path(),
            format!("/{}/blobs/{}", TEST_SUBJECT, digest.to_base58())
        );
    }

    #[dialog_common::test]
    async fn it_generates_correct_url_and_checksum_for_archive_put() {
        let creds = public_creds();
        let digest: [u8; 32] = [0x99; 32];
        let checksum = crate::Checksum::Sha256([0x11; 32]);

        let capability = Subject::from(test_subject())
            .attenuate(archive::Archive)
            .attenuate(archive::Catalog::new("index"))
            .invoke(archive::Put::new(digest, checksum));

        let req = authorize(&creds, capability).await;

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
    async fn it_generates_signed_url_for_get_with_private_creds() {
        let creds = private_creds();
        let key = b"signed-key";

        let capability = Subject::from(test_subject())
            .attenuate(storage::Storage)
            .attenuate(storage::Store::new("data"))
            .invoke(storage::Get::new(key));

        let req = authorize(&creds, capability).await;

        assert_eq!(req.method, "GET");

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
    async fn it_generates_signed_url_for_put_with_private_creds() {
        let creds = private_creds();
        let key = b"upload-key";
        let checksum = crate::Checksum::Sha256([0xff; 32]);

        let capability = Subject::from(test_subject())
            .attenuate(storage::Storage)
            .attenuate(storage::Store::new("uploads"))
            .invoke(storage::Set::new(key, checksum));

        let req = authorize(&creds, capability).await;

        assert_eq!(req.method, "PUT");

        let query: Vec<(String, String)> = req
            .url
            .query_pairs()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        assert!(query.iter().any(|(k, _)| k == "X-Amz-Signature"));

        assert!(
            req.headers
                .iter()
                .any(|(k, _)| k == "x-amz-checksum-sha256")
        );
    }

    #[dialog_common::test]
    async fn it_generates_signed_url_for_list_with_private_creds() {
        let creds = private_creds();

        let capability = Subject::from(test_subject())
            .attenuate(storage::Storage)
            .attenuate(storage::Store::new("files"))
            .invoke(storage::List::new(None));

        let req = authorize(&creds, capability).await;

        assert_eq!(req.method, "GET");

        let query: Vec<(String, String)> = req
            .url
            .query_pairs()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();

        assert!(query.iter().any(|(k, v)| k == "list-type" && v == "2"));
        assert!(query.iter().any(|(k, _)| k == "prefix"));

        assert!(query.iter().any(|(k, _)| k == "X-Amz-Signature"));
    }

    #[dialog_common::test]
    async fn it_uses_virtual_hosted_style_for_aws() {
        let creds = public_creds();

        let capability = Subject::from(test_subject())
            .attenuate(storage::Storage)
            .attenuate(storage::Store::new("test"))
            .invoke(storage::Get::new(b"key"));

        let req = authorize(&creds, capability).await;

        assert!(req.url.host_str().unwrap().starts_with("my-bucket."));
    }

    #[dialog_common::test]
    async fn it_uses_path_style_for_localhost() {
        let creds = localhost_creds();

        let capability = Subject::from(test_subject())
            .attenuate(storage::Storage)
            .attenuate(storage::Store::new("test"))
            .invoke(storage::Get::new(b"key"));

        let req = authorize(&creds, capability).await;

        assert_eq!(req.url.host_str().unwrap(), "localhost");
        assert!(req.url.path().starts_with("/test-bucket/"));
    }

    #[dialog_common::test]
    async fn it_supports_different_store_names() {
        let creds = public_creds();
        let stores = ["index", "blob", "metadata", "cache", ""];

        for store_name in stores {
            let capability = Subject::from(test_subject())
                .attenuate(storage::Storage)
                .attenuate(storage::Store::new(store_name))
                .invoke(storage::Get::new(b"key"));

            let req = authorize(&creds, capability).await;

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

    #[dialog_common::test]
    async fn it_supports_different_catalog_names() {
        let creds = public_creds();
        let catalogs = ["blobs", "index", "refs"];
        let digest: [u8; 32] = [0x55; 32];

        for catalog_name in catalogs {
            let capability = Subject::from(test_subject())
                .attenuate(archive::Archive)
                .attenuate(archive::Catalog::new(catalog_name))
                .invoke(archive::Get::new(digest));

            let req = authorize(&creds, capability).await;

            assert!(
                req.url
                    .path()
                    .contains(&format!("/{}/{}/", TEST_SUBJECT, catalog_name))
            );
        }
    }
}

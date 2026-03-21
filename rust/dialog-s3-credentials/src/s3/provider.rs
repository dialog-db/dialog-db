//! Provider implementations for S3 site authorization.
//!
//! S3 credentials can directly grant authorization by presigning requests.
//! The `Authorize` step reads the S3Access context, uses credentials to
//! presign, and produces an `Authorized<Fx, S3Access>` ready for conversion
//! to `S3Invocation<Fx>`.

use async_trait::async_trait;
use dialog_capability::authorization::Authorized;
use dialog_capability::{Capability, Constraint, Effect, Provider, credential};

use super::Credentials;
use super::site::S3Access;
use crate::capability::S3Request;

/// Intermediate proof for S3 authorization.
///
/// S3 presigning doesn't need an external service, so the "proof" step
/// simply wraps the credentials ready for presigning in the authorize step.
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

/// Provider for the Authorize step: presigns the URL using S3 credentials.
///
/// S3 credentials serve as their own env for the Authorize step. One provider
/// covers ALL sites using `S3Access`.
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<C> Provider<credential::Authorize<C, S3Access>> for Credentials
where
    C: Effect + Clone + 'static,
    C::Of: Constraint,
    Capability<C>: S3Request,
    Capability<C>: dialog_common::ConditionalSend,
    credential::Authorize<C, S3Access>: dialog_common::ConditionalSend + 'static,
{
    async fn execute(
        &self,
        input: Capability<credential::Authorize<C, S3Access>>,
    ) -> Result<Authorized<C, S3Access>, credential::AuthorizeError> {
        let authorize = input.into_inner().constraint;
        let authorized_request = self
            .grant(&authorize.capability)
            .await
            .map_err(|e| credential::AuthorizeError::Denied(e.to_string()))?;

        Ok(Authorized {
            capability: authorize.capability,
            access: authorize.access,
            authorization: authorized_request,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Address;
    use crate::capability::{AuthorizedRequest, archive, memory, storage};
    use base58::ToBase58;
    use dialog_capability::{Did, Effect, Subject, did};

    fn test_subject() -> Did {
        did!("key:zTestSubject")
    }

    const TEST_SUBJECT: &str = "did:key:zTestSubject";

    fn public_site() -> super::super::S3Site {
        let address = Address::new(
            "https://s3.us-east-1.amazonaws.com",
            "us-east-1",
            "my-bucket",
        );
        super::super::S3Site::new(address).unwrap()
    }

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

    fn private_site() -> super::super::S3Site {
        let address = Address::new(
            "https://s3.us-east-1.amazonaws.com",
            "us-east-1",
            "my-bucket",
        );
        super::super::S3Site::new(address).unwrap()
    }

    fn localhost_site() -> super::super::S3Site {
        let address = Address::new("http://localhost:9000", "us-east-1", "test-bucket");
        super::super::S3Site::new(address).unwrap()
    }

    fn localhost_creds() -> Credentials {
        let address = Address::new("http://localhost:9000", "us-east-1", "test-bucket");
        Credentials::public(address).unwrap()
    }

    /// Helper to authorize a capability using the new Access-based flow.
    async fn authorize<C>(
        creds: &Credentials,
        site: &super::super::S3Site,
        capability: Capability<C>,
    ) -> AuthorizedRequest
    where
        C: Effect + Clone + 'static,
        Capability<C>: S3Request,
        credential::Authorize<C, S3Access>: dialog_common::ConditionalSend + 'static,
    {
        use dialog_capability::site::Site;
        let access = site.access();
        let subject = capability.subject().clone();
        let authorize_cap = Subject::from(subject)
            .attenuate(credential::Credential)
            .attenuate(credential::Profile::default())
            .invoke(credential::Authorize::<C, S3Access> { capability, access });
        let authorized = <Credentials as Provider<credential::Authorize<C, S3Access>>>::execute(
            creds,
            authorize_cap,
        )
        .await
        .unwrap();

        authorized.authorization
    }

    #[dialog_common::test]
    async fn it_generates_correct_url_for_storage_get() {
        let creds = public_creds();
        let site = public_site();
        let key = b"test-key";

        let capability = Subject::from(test_subject())
            .attenuate(storage::Storage)
            .attenuate(storage::Store::new("index"))
            .invoke(storage::Get::new(key));

        let req = authorize(&creds, &site, capability).await;

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
        let site = public_site();
        let binary_key: [u8; 32] = [
            0xde, 0xad, 0xbe, 0xef, 0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99,
            0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08,
            0x09, 0x0a, 0x0b, 0x0c,
        ];

        let capability = Subject::from(test_subject())
            .attenuate(storage::Storage)
            .attenuate(storage::Store::new("blob"))
            .invoke(storage::Get::new(binary_key));

        let req = authorize(&creds, &site, capability).await;

        assert_eq!(req.method, "GET");
        assert!(req.url.path().contains(&binary_key.to_base58()));
    }

    #[dialog_common::test]
    async fn it_generates_correct_url_and_checksum_for_storage_set() {
        let creds = public_creds();
        let site = public_site();
        let key = b"my-key";
        let checksum_bytes = [0x12u8; 32];
        let checksum = crate::Checksum::Sha256(checksum_bytes);

        let capability = Subject::from(test_subject())
            .attenuate(storage::Storage)
            .attenuate(storage::Store::new("blob"))
            .invoke(storage::Set::new(key, checksum));

        let req = authorize(&creds, &site, capability).await;

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
        let site = public_site();
        let key = b"key-to-delete";

        let capability = Subject::from(test_subject())
            .attenuate(storage::Storage)
            .attenuate(storage::Store::new("index"))
            .invoke(storage::Delete::new(key));

        let req = authorize(&creds, &site, capability).await;

        assert_eq!(req.method, "DELETE");
        assert_eq!(
            req.url.path(),
            format!("/{}/index/{}", TEST_SUBJECT, key.to_base58())
        );
    }

    #[dialog_common::test]
    async fn it_generates_correct_query_params_for_storage_list() {
        let creds = public_creds();
        let site = public_site();

        let capability = Subject::from(test_subject())
            .attenuate(storage::Storage)
            .attenuate(storage::Store::new("index"))
            .invoke(storage::List::new(None));

        let req = authorize(&creds, &site, capability).await;

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
        let site = public_site();
        let token = "next-page-token-abc123";

        let capability = Subject::from(test_subject())
            .attenuate(storage::Storage)
            .attenuate(storage::Store::new("data"))
            .invoke(storage::List::new(Some(token.to_string())));

        let req = authorize(&creds, &site, capability).await;

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
        let site = public_site();

        let capability = Subject::from(test_subject())
            .attenuate(memory::Memory)
            .attenuate(memory::Space::new("did:key:zUser123"))
            .attenuate(memory::Cell::new("main"))
            .invoke(memory::Resolve);

        let req = authorize(&creds, &site, capability).await;

        assert_eq!(req.method, "GET");
        assert_eq!(
            req.url.path(),
            format!("/{}/did:key:zUser123/main", TEST_SUBJECT)
        );
    }

    #[dialog_common::test]
    async fn it_publishes_memory_without_prior_edition() {
        let creds = public_creds();
        let site = public_site();
        let checksum = crate::Checksum::Sha256([0xab; 32]);

        let capability = Subject::from(test_subject())
            .attenuate(memory::Memory)
            .attenuate(memory::Space::new("did:key:zSpace"))
            .attenuate(memory::Cell::new("head"))
            .invoke(memory::Publish {
                checksum,
                when: None,
            });

        let req = authorize(&creds, &site, capability).await;

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
        let site = public_site();
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

        let req = authorize(&creds, &site, capability).await;

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
        let site = public_site();

        let capability = Subject::from(test_subject())
            .attenuate(memory::Memory)
            .attenuate(memory::Space::new("did:key:zOwner"))
            .attenuate(memory::Cell::new("temp"))
            .invoke(memory::Retract::new("etag-to-match"));

        let req = authorize(&creds, &site, capability).await;

        assert_eq!(req.method, "DELETE");
        assert_eq!(
            req.url.path(),
            format!("/{}/did:key:zOwner/temp", TEST_SUBJECT)
        );
    }

    #[dialog_common::test]
    async fn it_generates_correct_url_for_archive_get() {
        let creds = public_creds();
        let site = public_site();
        let digest: [u8; 32] = [0x42; 32];

        let capability = Subject::from(test_subject())
            .attenuate(archive::Archive)
            .attenuate(archive::Catalog::new("blobs"))
            .invoke(archive::Get::new(digest));

        let req = authorize(&creds, &site, capability).await;

        assert_eq!(req.method, "GET");
        assert_eq!(
            req.url.path(),
            format!("/{}/blobs/{}", TEST_SUBJECT, digest.to_base58())
        );
    }

    #[dialog_common::test]
    async fn it_generates_correct_url_and_checksum_for_archive_put() {
        let creds = public_creds();
        let site = public_site();
        let digest: [u8; 32] = [0x99; 32];
        let checksum = crate::Checksum::Sha256([0x11; 32]);

        let capability = Subject::from(test_subject())
            .attenuate(archive::Archive)
            .attenuate(archive::Catalog::new("index"))
            .invoke(archive::Put::new(digest, checksum));

        let req = authorize(&creds, &site, capability).await;

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
        let site = private_site();
        let key = b"signed-key";

        let capability = Subject::from(test_subject())
            .attenuate(storage::Storage)
            .attenuate(storage::Store::new("data"))
            .invoke(storage::Get::new(key));

        let req = authorize(&creds, &site, capability).await;

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
        let site = private_site();
        let key = b"upload-key";
        let checksum = crate::Checksum::Sha256([0xff; 32]);

        let capability = Subject::from(test_subject())
            .attenuate(storage::Storage)
            .attenuate(storage::Store::new("uploads"))
            .invoke(storage::Set::new(key, checksum));

        let req = authorize(&creds, &site, capability).await;

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
        let site = private_site();

        let capability = Subject::from(test_subject())
            .attenuate(storage::Storage)
            .attenuate(storage::Store::new("files"))
            .invoke(storage::List::new(None));

        let req = authorize(&creds, &site, capability).await;

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
        let site = public_site();

        let capability = Subject::from(test_subject())
            .attenuate(storage::Storage)
            .attenuate(storage::Store::new("test"))
            .invoke(storage::Get::new(b"key"));

        let req = authorize(&creds, &site, capability).await;

        assert!(req.url.host_str().unwrap().starts_with("my-bucket."));
    }

    #[dialog_common::test]
    async fn it_uses_path_style_for_localhost() {
        let creds = localhost_creds();
        let site = localhost_site();

        let capability = Subject::from(test_subject())
            .attenuate(storage::Storage)
            .attenuate(storage::Store::new("test"))
            .invoke(storage::Get::new(b"key"));

        let req = authorize(&creds, &site, capability).await;

        assert_eq!(req.url.host_str().unwrap(), "localhost");
        assert!(req.url.path().starts_with("/test-bucket/"));
    }

    #[dialog_common::test]
    async fn it_supports_different_store_names() {
        let creds = public_creds();
        let site = public_site();
        let stores = ["index", "blob", "metadata", "cache", ""];

        for store_name in stores {
            let capability = Subject::from(test_subject())
                .attenuate(storage::Storage)
                .attenuate(storage::Store::new(store_name))
                .invoke(storage::Get::new(b"key"));

            let req = authorize(&creds, &site, capability).await;

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
        let site = public_site();
        let catalogs = ["blobs", "index", "refs"];
        let digest: [u8; 32] = [0x55; 32];

        for catalog_name in catalogs {
            let capability = Subject::from(test_subject())
                .attenuate(archive::Archive)
                .attenuate(archive::Catalog::new(catalog_name))
                .invoke(archive::Get::new(digest));

            let req = authorize(&creds, &site, capability).await;

            assert!(
                req.url
                    .path()
                    .contains(&format!("/{}/{}/", TEST_SUBJECT, catalog_name))
            );
        }
    }
}

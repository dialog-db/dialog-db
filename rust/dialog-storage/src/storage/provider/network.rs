//! Network provider that routes remote invocations to connections by address.
//!
//! This module uses [`#[derive(Router)]`](dialog_capability::Router) to compose
//! per-address-type routes into a single `Network` provider. Each route field
//! handles its own address type and connection caching.
pub mod emulator;
pub mod route;
#[cfg(feature = "s3")]
pub mod s3;
#[cfg(feature = "ucan")]
pub mod ucan;

/// Production network provider that routes remote invocations to the
/// appropriate backend based on address type.
///
/// Each field is a route that handles a specific address type. The
/// `#[derive(Router)]` macro generates `Provider<RemoteInvocation<Fx, Addr>>`
/// implementations that forward to the matching field.
#[derive(dialog_capability::Router)]
pub struct Network<Issuer: Clone> {
    #[cfg(feature = "s3")]
    s3: route::Route<Issuer, s3::Credentials, s3::Connection<Issuer>>,
    #[cfg(feature = "ucan")]
    ucan: route::Route<Issuer, ucan::Credentials, ucan::Connection<Issuer>>,
    #[cfg(not(any(feature = "s3", feature = "ucan")))]
    #[route(skip)]
    _marker: std::marker::PhantomData<Issuer>,
}

impl<Issuer: Clone> Network<Issuer> {
    /// Create a new network provider with the given issuer.
    pub fn new(#[allow(unused)] issuer: Issuer) -> Self {
        Self {
            #[cfg(feature = "s3")]
            s3: route::Route::new(issuer.clone()),
            #[cfg(feature = "ucan")]
            ucan: route::Route::new(issuer),
            #[cfg(not(any(feature = "s3", feature = "ucan")))]
            _marker: std::marker::PhantomData,
        }
    }
}

#[cfg(all(test, feature = "s3"))]
mod archive_tests {
    use crate::s3::helpers;
    use dialog_capability::{Did, Subject};
    use dialog_common::Blake3Hash;
    use dialog_effects::archive::{Archive, Catalog, Get, Put};
    use dialog_effects::remote::RemoteInvocation;

    use super::Network;

    const TEST_SUBJECT: &str = "did:key:zTestSubject";

    #[allow(dead_code)]
    fn test_subject() -> Subject {
        Subject::from(TEST_SUBJECT.parse::<Did>().unwrap())
    }

    #[allow(dead_code)]
    fn create_network_provider(_env: &helpers::PublicS3Address) -> Network<helpers::Session> {
        Network::new(helpers::Session::new(TEST_SUBJECT.parse::<Did>().unwrap()))
    }

    #[allow(dead_code)]
    fn create_address(env: &helpers::PublicS3Address) -> super::s3::Credentials {
        dialog_s3_credentials::s3::Credentials::public(dialog_s3_credentials::Address::new(
            &env.endpoint,
            "us-east-1",
            &env.bucket,
        ))
        .unwrap()
        .with_path_style(true)
    }

    #[dialog_common::test]
    async fn it_returns_none_for_missing_content(
        env: helpers::PublicS3Address,
    ) -> anyhow::Result<()> {
        let mut provider = create_network_provider(&env);
        let addr = create_address(&env);
        let digest = Blake3Hash::hash(b"nonexistent");

        let capability = test_subject()
            .attenuate(Archive)
            .attenuate(Catalog::new("index"))
            .invoke(Get::new(digest));

        let result = RemoteInvocation::new(capability, addr)
            .perform(&mut provider)
            .await?;
        assert!(result.is_none());

        Ok(())
    }

    #[dialog_common::test]
    async fn it_stores_and_retrieves_content(env: helpers::PublicS3Address) -> anyhow::Result<()> {
        let mut provider = create_network_provider(&env);
        let addr = create_address(&env);
        let content = b"hello world".to_vec();
        let digest = Blake3Hash::hash(&content);

        // Put content
        RemoteInvocation::new(
            test_subject()
                .attenuate(Archive)
                .attenuate(Catalog::new("index"))
                .invoke(Put::new(digest.clone(), content.clone())),
            addr.clone(),
        )
        .perform(&mut provider)
        .await?;

        // Get content
        let result = RemoteInvocation::new(
            test_subject()
                .attenuate(Archive)
                .attenuate(Catalog::new("index"))
                .invoke(Get::new(digest)),
            addr,
        )
        .perform(&mut provider)
        .await?;
        assert_eq!(result, Some(content));

        Ok(())
    }

    #[dialog_common::test]
    async fn it_handles_different_catalogs(env: helpers::PublicS3Address) -> anyhow::Result<()> {
        let mut provider = create_network_provider(&env);
        let addr = create_address(&env);
        let content1 = b"content for catalog 1".to_vec();
        let content2 = b"content for catalog 2".to_vec();
        let digest1 = Blake3Hash::hash(&content1);
        let digest2 = Blake3Hash::hash(&content2);

        // Store in different catalogs
        RemoteInvocation::new(
            test_subject()
                .attenuate(Archive)
                .attenuate(Catalog::new("catalog1"))
                .invoke(Put::new(digest1.clone(), content1.clone())),
            addr.clone(),
        )
        .perform(&mut provider)
        .await?;

        RemoteInvocation::new(
            test_subject()
                .attenuate(Archive)
                .attenuate(Catalog::new("catalog2"))
                .invoke(Put::new(digest2.clone(), content2.clone())),
            addr.clone(),
        )
        .perform(&mut provider)
        .await?;

        // Retrieve from catalog1
        let result1 = RemoteInvocation::new(
            test_subject()
                .attenuate(Archive)
                .attenuate(Catalog::new("catalog1"))
                .invoke(Get::new(digest1)),
            addr.clone(),
        )
        .perform(&mut provider)
        .await?;
        assert_eq!(result1, Some(content1));

        // Retrieve from catalog2
        let result2 = RemoteInvocation::new(
            test_subject()
                .attenuate(Archive)
                .attenuate(Catalog::new("catalog2"))
                .invoke(Get::new(digest2.clone())),
            addr.clone(),
        )
        .perform(&mut provider)
        .await?;
        assert_eq!(result2, Some(content2));

        // Cross-catalog lookup should return None
        let cross = RemoteInvocation::new(
            test_subject()
                .attenuate(Archive)
                .attenuate(Catalog::new("catalog1"))
                .invoke(Get::new(digest2)),
            addr,
        )
        .perform(&mut provider)
        .await?;
        assert!(cross.is_none());

        Ok(())
    }

    #[dialog_common::test]
    async fn it_is_idempotent_for_same_content(
        env: helpers::PublicS3Address,
    ) -> anyhow::Result<()> {
        let mut provider = create_network_provider(&env);
        let addr = create_address(&env);
        let content = b"idempotent content".to_vec();
        let digest = Blake3Hash::hash(&content);

        // Put twice
        for _ in 0..2 {
            RemoteInvocation::new(
                test_subject()
                    .attenuate(Archive)
                    .attenuate(Catalog::new("index"))
                    .invoke(Put::new(digest.clone(), content.clone())),
                addr.clone(),
            )
            .perform(&mut provider)
            .await?;
        }

        // Should still be retrievable
        let result = RemoteInvocation::new(
            test_subject()
                .attenuate(Archive)
                .attenuate(Catalog::new("index"))
                .invoke(Get::new(digest)),
            addr,
        )
        .perform(&mut provider)
        .await?;
        assert_eq!(result, Some(content));

        Ok(())
    }

    #[dialog_common::test]
    async fn it_handles_empty_content(env: helpers::PublicS3Address) -> anyhow::Result<()> {
        let mut provider = create_network_provider(&env);
        let addr = create_address(&env);
        let content = vec![];
        let digest = Blake3Hash::hash(&content);

        RemoteInvocation::new(
            test_subject()
                .attenuate(Archive)
                .attenuate(Catalog::new("index"))
                .invoke(Put::new(digest.clone(), content.clone())),
            addr.clone(),
        )
        .perform(&mut provider)
        .await?;

        let result = RemoteInvocation::new(
            test_subject()
                .attenuate(Archive)
                .attenuate(Catalog::new("index"))
                .invoke(Get::new(digest)),
            addr,
        )
        .perform(&mut provider)
        .await?;
        assert_eq!(result, Some(content));

        Ok(())
    }

    #[dialog_common::test]
    async fn it_handles_large_content(env: helpers::PublicS3Address) -> anyhow::Result<()> {
        let mut provider = create_network_provider(&env);
        let addr = create_address(&env);
        // 100KB content (matching S3 backend test size)
        let content: Vec<u8> = (0..100_000).map(|i| (i % 256) as u8).collect();
        let digest = Blake3Hash::hash(&content);

        RemoteInvocation::new(
            test_subject()
                .attenuate(Archive)
                .attenuate(Catalog::new("index"))
                .invoke(Put::new(digest.clone(), content.clone())),
            addr.clone(),
        )
        .perform(&mut provider)
        .await?;

        let result = RemoteInvocation::new(
            test_subject()
                .attenuate(Archive)
                .attenuate(Catalog::new("index"))
                .invoke(Get::new(digest)),
            addr,
        )
        .perform(&mut provider)
        .await?;
        assert_eq!(result, Some(content));

        Ok(())
    }

    #[dialog_common::test]
    async fn it_caches_connections_across_invocations(
        env: helpers::PublicS3Address,
    ) -> anyhow::Result<()> {
        let mut provider = create_network_provider(&env);
        let addr = create_address(&env);
        let content = b"cached connection content".to_vec();
        let digest = Blake3Hash::hash(&content);

        // Put via first invocation
        RemoteInvocation::new(
            test_subject()
                .attenuate(Archive)
                .attenuate(Catalog::new("blobs"))
                .invoke(Put::new(digest.clone(), content.clone())),
            addr.clone(),
        )
        .perform(&mut provider)
        .await?;

        // Get via second invocation (should reuse cached connection)
        let result = RemoteInvocation::new(
            test_subject()
                .attenuate(Archive)
                .attenuate(Catalog::new("blobs"))
                .invoke(Get::new(digest)),
            addr,
        )
        .perform(&mut provider)
        .await?;
        assert_eq!(result, Some(content));

        Ok(())
    }

    #[dialog_common::test]
    async fn it_performs_multiple_operations(env: helpers::PublicS3Address) -> anyhow::Result<()> {
        let mut provider = create_network_provider(&env);
        let addr = create_address(&env);

        // Store 3 different blobs
        let blobs: Vec<(Vec<u8>, Blake3Hash)> = (0..3)
            .map(|i| {
                let content = format!("blob content {}", i).into_bytes();
                let digest = Blake3Hash::hash(&content);
                (content, digest)
            })
            .collect();

        for (content, digest) in &blobs {
            RemoteInvocation::new(
                test_subject()
                    .attenuate(Archive)
                    .attenuate(Catalog::new("multi"))
                    .invoke(Put::new(digest.clone(), content.clone())),
                addr.clone(),
            )
            .perform(&mut provider)
            .await?;
        }

        // Verify all 3 are retrievable
        for (content, digest) in &blobs {
            let result = RemoteInvocation::new(
                test_subject()
                    .attenuate(Archive)
                    .attenuate(Catalog::new("multi"))
                    .invoke(Get::new(digest.clone())),
                addr.clone(),
            )
            .perform(&mut provider)
            .await?;
            assert_eq!(result, Some(content.clone()));
        }

        // Verify missing digest returns None
        let missing_digest = Blake3Hash::hash(b"not stored");
        let result = RemoteInvocation::new(
            test_subject()
                .attenuate(Archive)
                .attenuate(Catalog::new("multi"))
                .invoke(Get::new(missing_digest)),
            addr,
        )
        .perform(&mut provider)
        .await?;
        assert_eq!(result, None);

        Ok(())
    }
}

#[cfg(all(test, feature = "s3"))]
mod archive_signed_session_tests {
    use crate::s3::helpers;
    use dialog_capability::{Did, Subject};
    use dialog_common::Blake3Hash;
    use dialog_effects::archive::{Archive, Catalog, Get, Put};
    use dialog_effects::remote::RemoteInvocation;

    use super::Network;

    const TEST_SUBJECT: &str = "did:key:zTestSubject";

    #[allow(dead_code)]
    fn test_subject() -> Subject {
        Subject::from(TEST_SUBJECT.parse::<Did>().unwrap())
    }

    #[allow(dead_code)]
    fn create_network_provider(_env: &helpers::S3Address) -> Network<helpers::Session> {
        Network::new(helpers::Session::new(TEST_SUBJECT.parse::<Did>().unwrap()))
    }

    #[allow(dead_code)]
    fn create_address(env: &helpers::S3Address) -> super::s3::Credentials {
        dialog_s3_credentials::s3::Credentials::private(
            dialog_s3_credentials::Address::new(&env.endpoint, "us-east-1", &env.bucket),
            &env.access_key_id,
            &env.secret_access_key,
        )
        .unwrap()
        .with_path_style(true)
    }

    #[dialog_common::test]
    async fn it_returns_none_for_missing_content(env: helpers::S3Address) -> anyhow::Result<()> {
        let mut provider = create_network_provider(&env);
        let addr = create_address(&env);
        let digest = Blake3Hash::hash(b"nonexistent");

        let result = RemoteInvocation::new(
            test_subject()
                .attenuate(Archive)
                .attenuate(Catalog::new("index"))
                .invoke(Get::new(digest)),
            addr,
        )
        .perform(&mut provider)
        .await?;
        assert!(result.is_none());

        Ok(())
    }

    #[dialog_common::test]
    async fn it_stores_and_retrieves_content(env: helpers::S3Address) -> anyhow::Result<()> {
        let mut provider = create_network_provider(&env);
        let addr = create_address(&env);
        let content = b"hello world signed".to_vec();
        let digest = Blake3Hash::hash(&content);

        RemoteInvocation::new(
            test_subject()
                .attenuate(Archive)
                .attenuate(Catalog::new("index"))
                .invoke(Put::new(digest.clone(), content.clone())),
            addr.clone(),
        )
        .perform(&mut provider)
        .await?;

        let result = RemoteInvocation::new(
            test_subject()
                .attenuate(Archive)
                .attenuate(Catalog::new("index"))
                .invoke(Get::new(digest)),
            addr,
        )
        .perform(&mut provider)
        .await?;
        assert_eq!(result, Some(content));

        Ok(())
    }

    #[dialog_common::test]
    async fn it_handles_different_catalogs(env: helpers::S3Address) -> anyhow::Result<()> {
        let mut provider = create_network_provider(&env);
        let addr = create_address(&env);
        let content1 = b"signed catalog 1".to_vec();
        let content2 = b"signed catalog 2".to_vec();
        let digest1 = Blake3Hash::hash(&content1);
        let digest2 = Blake3Hash::hash(&content2);

        RemoteInvocation::new(
            test_subject()
                .attenuate(Archive)
                .attenuate(Catalog::new("cat1"))
                .invoke(Put::new(digest1.clone(), content1.clone())),
            addr.clone(),
        )
        .perform(&mut provider)
        .await?;

        RemoteInvocation::new(
            test_subject()
                .attenuate(Archive)
                .attenuate(Catalog::new("cat2"))
                .invoke(Put::new(digest2.clone(), content2.clone())),
            addr.clone(),
        )
        .perform(&mut provider)
        .await?;

        let result1 = RemoteInvocation::new(
            test_subject()
                .attenuate(Archive)
                .attenuate(Catalog::new("cat1"))
                .invoke(Get::new(digest1)),
            addr.clone(),
        )
        .perform(&mut provider)
        .await?;
        assert_eq!(result1, Some(content1));

        let result2 = RemoteInvocation::new(
            test_subject()
                .attenuate(Archive)
                .attenuate(Catalog::new("cat2"))
                .invoke(Get::new(digest2.clone())),
            addr.clone(),
        )
        .perform(&mut provider)
        .await?;
        assert_eq!(result2, Some(content2));

        // Cross-catalog lookup should return None
        let cross = RemoteInvocation::new(
            test_subject()
                .attenuate(Archive)
                .attenuate(Catalog::new("cat1"))
                .invoke(Get::new(digest2)),
            addr,
        )
        .perform(&mut provider)
        .await?;
        assert!(cross.is_none());

        Ok(())
    }

    #[dialog_common::test]
    async fn it_fails_with_wrong_credentials(env: helpers::S3Address) -> anyhow::Result<()> {
        let mut provider: Network<helpers::Session> =
            Network::new(helpers::Session::new(TEST_SUBJECT.parse::<Did>().unwrap()));
        let addr = dialog_s3_credentials::s3::Credentials::private(
            dialog_s3_credentials::Address::new(&env.endpoint, "us-east-1", &env.bucket),
            &env.access_key_id,
            "wrong-secret",
        )
        .unwrap()
        .with_path_style(true);

        let content = b"should fail".to_vec();
        let digest = Blake3Hash::hash(&content);

        let result = RemoteInvocation::new(
            test_subject()
                .attenuate(Archive)
                .attenuate(Catalog::new("index"))
                .invoke(Put::new(digest, content)),
            addr,
        )
        .perform(&mut provider)
        .await;

        assert!(
            result.is_err(),
            "Expected authentication failure with wrong secret key"
        );

        Ok(())
    }

    #[dialog_common::test]
    async fn it_handles_large_content(env: helpers::S3Address) -> anyhow::Result<()> {
        let mut provider = create_network_provider(&env);
        let addr = create_address(&env);
        let content: Vec<u8> = (0..100_000).map(|i| (i % 256) as u8).collect();
        let digest = Blake3Hash::hash(&content);

        RemoteInvocation::new(
            test_subject()
                .attenuate(Archive)
                .attenuate(Catalog::new("index"))
                .invoke(Put::new(digest.clone(), content.clone())),
            addr.clone(),
        )
        .perform(&mut provider)
        .await?;

        let result = RemoteInvocation::new(
            test_subject()
                .attenuate(Archive)
                .attenuate(Catalog::new("index"))
                .invoke(Get::new(digest)),
            addr,
        )
        .perform(&mut provider)
        .await?;
        assert_eq!(result, Some(content));

        Ok(())
    }
}

#[cfg(all(test, feature = "s3", feature = "ucan"))]
mod ucan_archive_tests {
    use crate::s3::helpers;
    use crate::s3::helpers::Ed25519Signer;
    use dialog_capability::{Principal, Subject};
    use dialog_common::Blake3Hash;
    use dialog_effects::archive::{Archive, Catalog, Get, Put};
    use dialog_effects::remote::RemoteInvocation;
    use dialog_s3_credentials::ucan::{
        Credentials as UcanCredentials, DelegationChain, test_helpers::create_delegation,
    };

    use super::Network;

    #[allow(dead_code)]
    async fn create_test_delegation_chain(
        subject_signer: &Ed25519Signer,
        audience: &Ed25519Signer,
        can: &[&str],
    ) -> DelegationChain {
        let delegation = create_delegation(subject_signer, audience, subject_signer, can)
            .await
            .expect("Failed to create test delegation");
        DelegationChain::new(delegation)
    }

    #[allow(dead_code)]
    fn create_network_provider(operator: &Ed25519Signer) -> Network<Ed25519Signer> {
        Network::new(operator.clone())
    }

    #[allow(dead_code)]
    async fn create_address(
        env: &helpers::UcanS3Address,
        operator: &Ed25519Signer,
    ) -> super::ucan::Credentials {
        let delegation = create_test_delegation_chain(operator, operator, &["archive"]).await;
        UcanCredentials::new(env.access_service_url.clone(), delegation)
    }

    #[dialog_common::test]
    async fn it_returns_none_for_missing_content(
        env: helpers::UcanS3Address,
    ) -> anyhow::Result<()> {
        let operator = Ed25519Signer::generate().await.unwrap();
        let mut provider = create_network_provider(&operator);
        let addr = create_address(&env, &operator).await;
        let digest = Blake3Hash::hash(b"nonexistent");

        let result = RemoteInvocation::new(
            Subject::from(operator.did())
                .attenuate(Archive)
                .attenuate(Catalog::new("index"))
                .invoke(Get::new(digest)),
            addr,
        )
        .perform(&mut provider)
        .await?;
        assert!(result.is_none());

        Ok(())
    }

    #[dialog_common::test]
    async fn it_stores_and_retrieves_content(env: helpers::UcanS3Address) -> anyhow::Result<()> {
        let operator = Ed25519Signer::generate().await.unwrap();
        let mut provider = create_network_provider(&operator);
        let addr = create_address(&env, &operator).await;
        let content = b"hello world ucan".to_vec();
        let digest = Blake3Hash::hash(&content);

        RemoteInvocation::new(
            Subject::from(operator.did())
                .attenuate(Archive)
                .attenuate(Catalog::new("blobs"))
                .invoke(Put::new(digest.clone(), content.clone())),
            addr.clone(),
        )
        .perform(&mut provider)
        .await?;

        let result = RemoteInvocation::new(
            Subject::from(operator.did())
                .attenuate(Archive)
                .attenuate(Catalog::new("blobs"))
                .invoke(Get::new(digest)),
            addr,
        )
        .perform(&mut provider)
        .await?;
        assert_eq!(result, Some(content));

        Ok(())
    }

    #[dialog_common::test]
    async fn it_handles_different_catalogs(env: helpers::UcanS3Address) -> anyhow::Result<()> {
        let operator = Ed25519Signer::generate().await.unwrap();
        let mut provider = create_network_provider(&operator);
        let addr = create_address(&env, &operator).await;
        let content1 = b"ucan catalog 1".to_vec();
        let content2 = b"ucan catalog 2".to_vec();
        let digest1 = Blake3Hash::hash(&content1);
        let digest2 = Blake3Hash::hash(&content2);

        RemoteInvocation::new(
            Subject::from(operator.did())
                .attenuate(Archive)
                .attenuate(Catalog::new("cat1"))
                .invoke(Put::new(digest1.clone(), content1.clone())),
            addr.clone(),
        )
        .perform(&mut provider)
        .await?;

        RemoteInvocation::new(
            Subject::from(operator.did())
                .attenuate(Archive)
                .attenuate(Catalog::new("cat2"))
                .invoke(Put::new(digest2.clone(), content2.clone())),
            addr.clone(),
        )
        .perform(&mut provider)
        .await?;

        let result1 = RemoteInvocation::new(
            Subject::from(operator.did())
                .attenuate(Archive)
                .attenuate(Catalog::new("cat1"))
                .invoke(Get::new(digest1)),
            addr.clone(),
        )
        .perform(&mut provider)
        .await?;
        assert_eq!(result1, Some(content1));

        let result2 = RemoteInvocation::new(
            Subject::from(operator.did())
                .attenuate(Archive)
                .attenuate(Catalog::new("cat2"))
                .invoke(Get::new(digest2.clone())),
            addr.clone(),
        )
        .perform(&mut provider)
        .await?;
        assert_eq!(result2, Some(content2));

        // Cross-catalog lookup should return None
        let cross = RemoteInvocation::new(
            Subject::from(operator.did())
                .attenuate(Archive)
                .attenuate(Catalog::new("cat1"))
                .invoke(Get::new(digest2)),
            addr,
        )
        .perform(&mut provider)
        .await?;
        assert!(cross.is_none());

        Ok(())
    }

    #[dialog_common::test]
    async fn it_is_idempotent_for_same_content(env: helpers::UcanS3Address) -> anyhow::Result<()> {
        let operator = Ed25519Signer::generate().await.unwrap();
        let mut provider = create_network_provider(&operator);
        let addr = create_address(&env, &operator).await;
        let content = b"idempotent ucan content".to_vec();
        let digest = Blake3Hash::hash(&content);

        for _ in 0..2 {
            RemoteInvocation::new(
                Subject::from(operator.did())
                    .attenuate(Archive)
                    .attenuate(Catalog::new("index"))
                    .invoke(Put::new(digest.clone(), content.clone())),
                addr.clone(),
            )
            .perform(&mut provider)
            .await?;
        }

        let result = RemoteInvocation::new(
            Subject::from(operator.did())
                .attenuate(Archive)
                .attenuate(Catalog::new("index"))
                .invoke(Get::new(digest)),
            addr,
        )
        .perform(&mut provider)
        .await?;
        assert_eq!(result, Some(content));

        Ok(())
    }

    #[dialog_common::test]
    async fn it_handles_large_content(env: helpers::UcanS3Address) -> anyhow::Result<()> {
        let operator = Ed25519Signer::generate().await.unwrap();
        let mut provider = create_network_provider(&operator);
        let addr = create_address(&env, &operator).await;
        let content: Vec<u8> = (0..100_000).map(|i| (i % 256) as u8).collect();
        let digest = Blake3Hash::hash(&content);

        RemoteInvocation::new(
            Subject::from(operator.did())
                .attenuate(Archive)
                .attenuate(Catalog::new("index"))
                .invoke(Put::new(digest.clone(), content.clone())),
            addr.clone(),
        )
        .perform(&mut provider)
        .await?;

        let result = RemoteInvocation::new(
            Subject::from(operator.did())
                .attenuate(Archive)
                .attenuate(Catalog::new("index"))
                .invoke(Get::new(digest)),
            addr,
        )
        .perform(&mut provider)
        .await?;
        assert_eq!(result, Some(content));

        Ok(())
    }
}

#[cfg(all(test, feature = "s3"))]
mod memory_tests {
    use crate::s3::helpers;
    use dialog_capability::{Did, Subject};
    use dialog_effects::memory::{Cell, Memory, MemoryError, Publish, Resolve, Retract, Space};
    use dialog_effects::remote::RemoteInvocation;

    use super::Network;

    const TEST_SUBJECT: &str = "did:key:zTestSubject";

    #[allow(dead_code)]
    fn test_subject() -> Subject {
        Subject::from(TEST_SUBJECT.parse::<Did>().unwrap())
    }

    #[allow(dead_code)]
    fn create_network_provider(_env: &helpers::PublicS3Address) -> Network<helpers::Session> {
        Network::new(helpers::Session::new(TEST_SUBJECT.parse::<Did>().unwrap()))
    }

    #[allow(dead_code)]
    fn create_address(env: &helpers::PublicS3Address) -> super::s3::Credentials {
        dialog_s3_credentials::s3::Credentials::public(dialog_s3_credentials::Address::new(
            &env.endpoint,
            "us-east-1",
            &env.bucket,
        ))
        .unwrap()
        .with_path_style(true)
    }

    #[dialog_common::test]
    async fn it_resolves_non_existent_cell(env: helpers::PublicS3Address) -> anyhow::Result<()> {
        let mut provider = create_network_provider(&env);
        let addr = create_address(&env);

        let cap = test_subject()
            .attenuate(Memory)
            .attenuate(Space::new("local"))
            .attenuate(Cell::new("missing"))
            .invoke(Resolve);

        let result = RemoteInvocation::new(cap, addr)
            .perform(&mut provider)
            .await?;
        assert!(result.is_none());

        Ok(())
    }

    #[dialog_common::test]
    async fn it_publishes_new_content(env: helpers::PublicS3Address) -> anyhow::Result<()> {
        let mut provider = create_network_provider(&env);
        let addr = create_address(&env);
        let content = b"hello world".to_vec();

        // Publish new content (when = None means expect empty)
        let edition = RemoteInvocation::new(
            test_subject()
                .attenuate(Memory)
                .attenuate(Space::new("local"))
                .attenuate(Cell::new("test"))
                .invoke(Publish::new(content.clone(), None)),
            addr.clone(),
        )
        .perform(&mut provider)
        .await?;

        assert!(!edition.is_empty());

        // Resolve to verify
        let resolved = RemoteInvocation::new(
            test_subject()
                .attenuate(Memory)
                .attenuate(Space::new("local"))
                .attenuate(Cell::new("test"))
                .invoke(Resolve),
            addr,
        )
        .perform(&mut provider)
        .await?;

        let publication = resolved.expect("should have content");
        assert_eq!(publication.content, content);
        assert_eq!(publication.edition, edition);

        Ok(())
    }

    #[dialog_common::test]
    async fn it_updates_existing_content(env: helpers::PublicS3Address) -> anyhow::Result<()> {
        let mut provider = create_network_provider(&env);
        let addr = create_address(&env);

        // Create initial content
        let edition1 = RemoteInvocation::new(
            test_subject()
                .attenuate(Memory)
                .attenuate(Space::new("local"))
                .attenuate(Cell::new("update-test"))
                .invoke(Publish::new(b"initial", None)),
            addr.clone(),
        )
        .perform(&mut provider)
        .await?;

        // Update with correct edition
        let edition2 = RemoteInvocation::new(
            test_subject()
                .attenuate(Memory)
                .attenuate(Space::new("local"))
                .attenuate(Cell::new("update-test"))
                .invoke(Publish::new(b"updated", Some(edition1.clone()))),
            addr.clone(),
        )
        .perform(&mut provider)
        .await?;

        assert_ne!(edition1, edition2);

        // Verify update
        let resolved = RemoteInvocation::new(
            test_subject()
                .attenuate(Memory)
                .attenuate(Space::new("local"))
                .attenuate(Cell::new("update-test"))
                .invoke(Resolve),
            addr,
        )
        .perform(&mut provider)
        .await?;

        let publication = resolved.expect("should have content");
        assert_eq!(publication.content, b"updated".to_vec());

        Ok(())
    }

    #[dialog_common::test]
    async fn it_fails_on_edition_mismatch(env: helpers::PublicS3Address) -> anyhow::Result<()> {
        let mut provider = create_network_provider(&env);
        let addr = create_address(&env);

        RemoteInvocation::new(
            test_subject()
                .attenuate(Memory)
                .attenuate(Space::new("local"))
                .attenuate(Cell::new("mismatch-test"))
                .invoke(Publish::new(b"initial", None)),
            addr.clone(),
        )
        .perform(&mut provider)
        .await?;

        let wrong_edition = b"wrong-etag".to_vec();
        let result = RemoteInvocation::new(
            test_subject()
                .attenuate(Memory)
                .attenuate(Space::new("local"))
                .attenuate(Cell::new("mismatch-test"))
                .invoke(Publish::new(b"updated", Some(wrong_edition))),
            addr,
        )
        .perform(&mut provider)
        .await;

        assert!(matches!(result, Err(MemoryError::EditionMismatch { .. })));

        Ok(())
    }

    #[dialog_common::test]
    async fn it_fails_creating_when_exists(env: helpers::PublicS3Address) -> anyhow::Result<()> {
        let mut provider = create_network_provider(&env);
        let addr = create_address(&env);

        RemoteInvocation::new(
            test_subject()
                .attenuate(Memory)
                .attenuate(Space::new("local"))
                .attenuate(Cell::new("create-exists-test"))
                .invoke(Publish::new(b"initial", None)),
            addr.clone(),
        )
        .perform(&mut provider)
        .await?;

        let result = RemoteInvocation::new(
            test_subject()
                .attenuate(Memory)
                .attenuate(Space::new("local"))
                .attenuate(Cell::new("create-exists-test"))
                .invoke(Publish::new(b"new", None)),
            addr,
        )
        .perform(&mut provider)
        .await;

        assert!(matches!(result, Err(MemoryError::EditionMismatch { .. })));

        Ok(())
    }

    #[dialog_common::test]
    async fn it_retracts_content(env: helpers::PublicS3Address) -> anyhow::Result<()> {
        let mut provider = create_network_provider(&env);
        let addr = create_address(&env);

        // Create content
        let edition = RemoteInvocation::new(
            test_subject()
                .attenuate(Memory)
                .attenuate(Space::new("local"))
                .attenuate(Cell::new("retract-test"))
                .invoke(Publish::new(b"to be deleted", None)),
            addr.clone(),
        )
        .perform(&mut provider)
        .await?;

        // Retract with correct edition
        RemoteInvocation::new(
            test_subject()
                .attenuate(Memory)
                .attenuate(Space::new("local"))
                .attenuate(Cell::new("retract-test"))
                .invoke(Retract::new(edition)),
            addr.clone(),
        )
        .perform(&mut provider)
        .await?;

        // Verify deleted
        let resolved = RemoteInvocation::new(
            test_subject()
                .attenuate(Memory)
                .attenuate(Space::new("local"))
                .attenuate(Cell::new("retract-test"))
                .invoke(Resolve),
            addr,
        )
        .perform(&mut provider)
        .await?;

        assert!(resolved.is_none());

        Ok(())
    }

    #[dialog_common::test]
    async fn it_handles_different_spaces(env: helpers::PublicS3Address) -> anyhow::Result<()> {
        let mut provider = create_network_provider(&env);
        let addr = create_address(&env);

        // Publish to different spaces
        RemoteInvocation::new(
            test_subject()
                .attenuate(Memory)
                .attenuate(Space::new("space1"))
                .attenuate(Cell::new("cell"))
                .invoke(Publish::new(b"content1", None)),
            addr.clone(),
        )
        .perform(&mut provider)
        .await?;

        RemoteInvocation::new(
            test_subject()
                .attenuate(Memory)
                .attenuate(Space::new("space2"))
                .attenuate(Cell::new("cell"))
                .invoke(Publish::new(b"content2", None)),
            addr.clone(),
        )
        .perform(&mut provider)
        .await?;

        // Resolve from space1
        let result1 = RemoteInvocation::new(
            test_subject()
                .attenuate(Memory)
                .attenuate(Space::new("space1"))
                .attenuate(Cell::new("cell"))
                .invoke(Resolve),
            addr.clone(),
        )
        .perform(&mut provider)
        .await?;
        assert_eq!(result1.unwrap().content, b"content1".to_vec());

        // Resolve from space2
        let result2 = RemoteInvocation::new(
            test_subject()
                .attenuate(Memory)
                .attenuate(Space::new("space2"))
                .attenuate(Cell::new("cell"))
                .invoke(Resolve),
            addr,
        )
        .perform(&mut provider)
        .await?;
        assert_eq!(result2.unwrap().content, b"content2".to_vec());

        Ok(())
    }

    #[dialog_common::test]
    async fn it_handles_nested_spaces(env: helpers::PublicS3Address) -> anyhow::Result<()> {
        let mut provider = create_network_provider(&env);
        let addr = create_address(&env);
        let content = b"nested content".to_vec();

        // Publish to nested space path
        let edition = RemoteInvocation::new(
            test_subject()
                .attenuate(Memory)
                .attenuate(Space::new("parent/child/grandchild"))
                .attenuate(Cell::new("cell"))
                .invoke(Publish::new(content.clone(), None)),
            addr.clone(),
        )
        .perform(&mut provider)
        .await?;

        assert!(!edition.is_empty());

        // Resolve to verify
        let resolved = RemoteInvocation::new(
            test_subject()
                .attenuate(Memory)
                .attenuate(Space::new("parent/child/grandchild"))
                .attenuate(Cell::new("cell"))
                .invoke(Resolve),
            addr,
        )
        .perform(&mut provider)
        .await?;

        let publication = resolved.expect("should have content");
        assert_eq!(publication.content, content);

        Ok(())
    }

    #[dialog_common::test]
    async fn it_handles_empty_content(env: helpers::PublicS3Address) -> anyhow::Result<()> {
        let mut provider = create_network_provider(&env);
        let addr = create_address(&env);
        let content = vec![];

        let edition = RemoteInvocation::new(
            test_subject()
                .attenuate(Memory)
                .attenuate(Space::new("local"))
                .attenuate(Cell::new("empty"))
                .invoke(Publish::new(content.clone(), None)),
            addr.clone(),
        )
        .perform(&mut provider)
        .await?;

        assert!(!edition.is_empty());

        let resolved = RemoteInvocation::new(
            test_subject()
                .attenuate(Memory)
                .attenuate(Space::new("local"))
                .attenuate(Cell::new("empty"))
                .invoke(Resolve),
            addr,
        )
        .perform(&mut provider)
        .await?;

        let publication = resolved.expect("should have content");
        assert_eq!(publication.content, content);

        Ok(())
    }

    #[dialog_common::test]
    async fn it_handles_large_content(env: helpers::PublicS3Address) -> anyhow::Result<()> {
        let mut provider = create_network_provider(&env);
        let addr = create_address(&env);
        // 100KB content (matching S3 backend test size)
        let content: Vec<u8> = (0..100_000).map(|i| (i % 256) as u8).collect();

        let edition = RemoteInvocation::new(
            test_subject()
                .attenuate(Memory)
                .attenuate(Space::new("local"))
                .attenuate(Cell::new("large"))
                .invoke(Publish::new(content.clone(), None)),
            addr.clone(),
        )
        .perform(&mut provider)
        .await?;

        assert!(!edition.is_empty());

        let resolved = RemoteInvocation::new(
            test_subject()
                .attenuate(Memory)
                .attenuate(Space::new("local"))
                .attenuate(Cell::new("large"))
                .invoke(Resolve),
            addr,
        )
        .perform(&mut provider)
        .await?;

        let publication = resolved.expect("should have content");
        assert_eq!(publication.content, content);

        Ok(())
    }

    #[dialog_common::test]
    async fn it_succeeds_retracting_already_retracted(
        env: helpers::PublicS3Address,
    ) -> anyhow::Result<()> {
        let mut provider = create_network_provider(&env);
        let addr = create_address(&env);

        // Try to retract non-existent cell - should succeed
        let wrong_edition = b"wrong-etag".to_vec();
        let result = RemoteInvocation::new(
            test_subject()
                .attenuate(Memory)
                .attenuate(Space::new("local"))
                .attenuate(Cell::new("nonexistent"))
                .invoke(Retract::new(wrong_edition)),
            addr,
        )
        .perform(&mut provider)
        .await;

        assert!(result.is_ok());

        Ok(())
    }
}

#[cfg(all(test, feature = "s3"))]
mod memory_signed_session_tests {
    use crate::s3::helpers;
    use dialog_capability::{Did, Subject};
    use dialog_effects::memory::{Cell, Memory, MemoryError, Publish, Resolve, Retract, Space};
    use dialog_effects::remote::RemoteInvocation;

    use super::Network;

    const TEST_SUBJECT: &str = "did:key:zTestSubject";

    #[allow(dead_code)]
    fn test_subject() -> Subject {
        Subject::from(TEST_SUBJECT.parse::<Did>().unwrap())
    }

    #[allow(dead_code)]
    fn create_network_provider(_env: &helpers::S3Address) -> Network<helpers::Session> {
        Network::new(helpers::Session::new(TEST_SUBJECT.parse::<Did>().unwrap()))
    }

    #[allow(dead_code)]
    fn create_address(env: &helpers::S3Address) -> super::s3::Credentials {
        dialog_s3_credentials::s3::Credentials::private(
            dialog_s3_credentials::Address::new(&env.endpoint, "us-east-1", &env.bucket),
            &env.access_key_id,
            &env.secret_access_key,
        )
        .unwrap()
        .with_path_style(true)
    }

    #[dialog_common::test]
    async fn it_resolves_non_existent_cell(env: helpers::S3Address) -> anyhow::Result<()> {
        let mut provider = create_network_provider(&env);
        let addr = create_address(&env);

        let result = RemoteInvocation::new(
            test_subject()
                .attenuate(Memory)
                .attenuate(Space::new("local"))
                .attenuate(Cell::new("missing"))
                .invoke(Resolve),
            addr,
        )
        .perform(&mut provider)
        .await?;
        assert!(result.is_none());

        Ok(())
    }

    #[dialog_common::test]
    async fn it_publishes_new_content(env: helpers::S3Address) -> anyhow::Result<()> {
        let mut provider = create_network_provider(&env);
        let addr = create_address(&env);
        let content = b"signed memory content".to_vec();

        let edition = RemoteInvocation::new(
            test_subject()
                .attenuate(Memory)
                .attenuate(Space::new("local"))
                .attenuate(Cell::new("test"))
                .invoke(Publish::new(content.clone(), None)),
            addr.clone(),
        )
        .perform(&mut provider)
        .await?;

        assert!(!edition.is_empty());

        let resolved = RemoteInvocation::new(
            test_subject()
                .attenuate(Memory)
                .attenuate(Space::new("local"))
                .attenuate(Cell::new("test"))
                .invoke(Resolve),
            addr,
        )
        .perform(&mut provider)
        .await?;

        let publication = resolved.expect("should have content");
        assert_eq!(publication.content, content);
        assert_eq!(publication.edition, edition);

        Ok(())
    }

    #[dialog_common::test]
    async fn it_updates_existing_content(env: helpers::S3Address) -> anyhow::Result<()> {
        let mut provider = create_network_provider(&env);
        let addr = create_address(&env);

        let edition1 = RemoteInvocation::new(
            test_subject()
                .attenuate(Memory)
                .attenuate(Space::new("local"))
                .attenuate(Cell::new("signed-update-test"))
                .invoke(Publish::new(b"initial", None)),
            addr.clone(),
        )
        .perform(&mut provider)
        .await?;

        let edition2 = RemoteInvocation::new(
            test_subject()
                .attenuate(Memory)
                .attenuate(Space::new("local"))
                .attenuate(Cell::new("signed-update-test"))
                .invoke(Publish::new(b"updated", Some(edition1.clone()))),
            addr.clone(),
        )
        .perform(&mut provider)
        .await?;

        assert_ne!(edition1, edition2);

        let resolved = RemoteInvocation::new(
            test_subject()
                .attenuate(Memory)
                .attenuate(Space::new("local"))
                .attenuate(Cell::new("signed-update-test"))
                .invoke(Resolve),
            addr,
        )
        .perform(&mut provider)
        .await?;

        let publication = resolved.expect("should have content");
        assert_eq!(publication.content, b"updated".to_vec());

        Ok(())
    }

    #[dialog_common::test]
    async fn it_fails_on_edition_mismatch(env: helpers::S3Address) -> anyhow::Result<()> {
        let mut provider = create_network_provider(&env);
        let addr = create_address(&env);

        RemoteInvocation::new(
            test_subject()
                .attenuate(Memory)
                .attenuate(Space::new("local"))
                .attenuate(Cell::new("signed-mismatch-test"))
                .invoke(Publish::new(b"initial", None)),
            addr.clone(),
        )
        .perform(&mut provider)
        .await?;

        let wrong_edition = b"wrong-etag".to_vec();
        let result = RemoteInvocation::new(
            test_subject()
                .attenuate(Memory)
                .attenuate(Space::new("local"))
                .attenuate(Cell::new("signed-mismatch-test"))
                .invoke(Publish::new(b"updated", Some(wrong_edition))),
            addr,
        )
        .perform(&mut provider)
        .await;

        assert!(matches!(result, Err(MemoryError::EditionMismatch { .. })));

        Ok(())
    }

    #[dialog_common::test]
    async fn it_retracts_content(env: helpers::S3Address) -> anyhow::Result<()> {
        let mut provider = create_network_provider(&env);
        let addr = create_address(&env);

        let edition = RemoteInvocation::new(
            test_subject()
                .attenuate(Memory)
                .attenuate(Space::new("local"))
                .attenuate(Cell::new("signed-retract-test"))
                .invoke(Publish::new(b"to be deleted", None)),
            addr.clone(),
        )
        .perform(&mut provider)
        .await?;

        RemoteInvocation::new(
            test_subject()
                .attenuate(Memory)
                .attenuate(Space::new("local"))
                .attenuate(Cell::new("signed-retract-test"))
                .invoke(Retract::new(edition)),
            addr.clone(),
        )
        .perform(&mut provider)
        .await?;

        let resolved = RemoteInvocation::new(
            test_subject()
                .attenuate(Memory)
                .attenuate(Space::new("local"))
                .attenuate(Cell::new("signed-retract-test"))
                .invoke(Resolve),
            addr,
        )
        .perform(&mut provider)
        .await?;

        assert!(resolved.is_none());

        Ok(())
    }

    #[dialog_common::test]
    async fn it_handles_different_spaces(env: helpers::S3Address) -> anyhow::Result<()> {
        let mut provider = create_network_provider(&env);
        let addr = create_address(&env);

        RemoteInvocation::new(
            test_subject()
                .attenuate(Memory)
                .attenuate(Space::new("signed-space1"))
                .attenuate(Cell::new("cell"))
                .invoke(Publish::new(b"content1", None)),
            addr.clone(),
        )
        .perform(&mut provider)
        .await?;

        RemoteInvocation::new(
            test_subject()
                .attenuate(Memory)
                .attenuate(Space::new("signed-space2"))
                .attenuate(Cell::new("cell"))
                .invoke(Publish::new(b"content2", None)),
            addr.clone(),
        )
        .perform(&mut provider)
        .await?;

        let result1 = RemoteInvocation::new(
            test_subject()
                .attenuate(Memory)
                .attenuate(Space::new("signed-space1"))
                .attenuate(Cell::new("cell"))
                .invoke(Resolve),
            addr.clone(),
        )
        .perform(&mut provider)
        .await?;
        assert_eq!(result1.unwrap().content, b"content1".to_vec());

        let result2 = RemoteInvocation::new(
            test_subject()
                .attenuate(Memory)
                .attenuate(Space::new("signed-space2"))
                .attenuate(Cell::new("cell"))
                .invoke(Resolve),
            addr,
        )
        .perform(&mut provider)
        .await?;
        assert_eq!(result2.unwrap().content, b"content2".to_vec());

        Ok(())
    }

    #[dialog_common::test]
    async fn it_handles_large_content(env: helpers::S3Address) -> anyhow::Result<()> {
        let mut provider = create_network_provider(&env);
        let addr = create_address(&env);
        let content: Vec<u8> = (0..100_000).map(|i| (i % 256) as u8).collect();

        let edition = RemoteInvocation::new(
            test_subject()
                .attenuate(Memory)
                .attenuate(Space::new("local"))
                .attenuate(Cell::new("signed-large"))
                .invoke(Publish::new(content.clone(), None)),
            addr.clone(),
        )
        .perform(&mut provider)
        .await?;

        assert!(!edition.is_empty());

        let resolved = RemoteInvocation::new(
            test_subject()
                .attenuate(Memory)
                .attenuate(Space::new("local"))
                .attenuate(Cell::new("signed-large"))
                .invoke(Resolve),
            addr,
        )
        .perform(&mut provider)
        .await?;

        let publication = resolved.expect("should have content");
        assert_eq!(publication.content, content);

        Ok(())
    }
}

#[cfg(all(test, feature = "s3", feature = "ucan"))]
mod ucan_memory_tests {
    use crate::s3::helpers;
    use crate::s3::helpers::Ed25519Signer;
    use dialog_capability::{Principal, Subject};
    use dialog_effects::memory::{Cell, Memory, MemoryError, Publish, Resolve, Retract, Space};
    use dialog_effects::remote::RemoteInvocation;
    use dialog_s3_credentials::ucan::{
        Credentials as UcanCredentials, DelegationChain, test_helpers::create_delegation,
    };

    use super::Network;

    #[allow(dead_code)]
    async fn create_test_delegation_chain(
        subject_signer: &Ed25519Signer,
        audience: &Ed25519Signer,
        can: &[&str],
    ) -> DelegationChain {
        let delegation = create_delegation(subject_signer, audience, subject_signer, can)
            .await
            .expect("Failed to create test delegation");
        DelegationChain::new(delegation)
    }

    #[allow(dead_code)]
    fn create_network_provider(operator: &Ed25519Signer) -> Network<Ed25519Signer> {
        Network::new(operator.clone())
    }

    #[allow(dead_code)]
    async fn create_address(
        env: &helpers::UcanS3Address,
        operator: &Ed25519Signer,
    ) -> super::ucan::Credentials {
        let delegation = create_test_delegation_chain(operator, operator, &["memory"]).await;
        UcanCredentials::new(env.access_service_url.clone(), delegation)
    }

    #[dialog_common::test]
    async fn it_resolves_non_existent_cell(env: helpers::UcanS3Address) -> anyhow::Result<()> {
        let operator = Ed25519Signer::generate().await.unwrap();
        let mut provider = create_network_provider(&operator);
        let addr = create_address(&env, &operator).await;

        let result = RemoteInvocation::new(
            Subject::from(operator.did())
                .attenuate(Memory)
                .attenuate(Space::new("local"))
                .attenuate(Cell::new("missing"))
                .invoke(Resolve),
            addr,
        )
        .perform(&mut provider)
        .await?;
        assert!(result.is_none());

        Ok(())
    }

    #[dialog_common::test]
    async fn it_publishes_new_content(env: helpers::UcanS3Address) -> anyhow::Result<()> {
        let operator = Ed25519Signer::generate().await.unwrap();
        let mut provider = create_network_provider(&operator);
        let addr = create_address(&env, &operator).await;
        let content = b"ucan memory content".to_vec();

        let edition = RemoteInvocation::new(
            Subject::from(operator.did())
                .attenuate(Memory)
                .attenuate(Space::new("local"))
                .attenuate(Cell::new("test"))
                .invoke(Publish::new(content.clone(), None)),
            addr.clone(),
        )
        .perform(&mut provider)
        .await?;

        assert!(!edition.is_empty());

        let resolved = RemoteInvocation::new(
            Subject::from(operator.did())
                .attenuate(Memory)
                .attenuate(Space::new("local"))
                .attenuate(Cell::new("test"))
                .invoke(Resolve),
            addr,
        )
        .perform(&mut provider)
        .await?;

        let publication = resolved.expect("should have content");
        assert_eq!(publication.content, content);
        assert_eq!(publication.edition, edition);

        Ok(())
    }

    #[dialog_common::test]
    async fn it_updates_existing_content(env: helpers::UcanS3Address) -> anyhow::Result<()> {
        let operator = Ed25519Signer::generate().await.unwrap();
        let mut provider = create_network_provider(&operator);
        let addr = create_address(&env, &operator).await;

        let edition1 = RemoteInvocation::new(
            Subject::from(operator.did())
                .attenuate(Memory)
                .attenuate(Space::new("local"))
                .attenuate(Cell::new("ucan-update-test"))
                .invoke(Publish::new(b"initial", None)),
            addr.clone(),
        )
        .perform(&mut provider)
        .await?;

        let edition2 = RemoteInvocation::new(
            Subject::from(operator.did())
                .attenuate(Memory)
                .attenuate(Space::new("local"))
                .attenuate(Cell::new("ucan-update-test"))
                .invoke(Publish::new(b"updated", Some(edition1.clone()))),
            addr.clone(),
        )
        .perform(&mut provider)
        .await?;

        assert_ne!(edition1, edition2);

        let resolved = RemoteInvocation::new(
            Subject::from(operator.did())
                .attenuate(Memory)
                .attenuate(Space::new("local"))
                .attenuate(Cell::new("ucan-update-test"))
                .invoke(Resolve),
            addr,
        )
        .perform(&mut provider)
        .await?;

        let publication = resolved.expect("should have content");
        assert_eq!(publication.content, b"updated".to_vec());

        Ok(())
    }

    #[dialog_common::test]
    async fn it_fails_on_edition_mismatch(env: helpers::UcanS3Address) -> anyhow::Result<()> {
        let operator = Ed25519Signer::generate().await.unwrap();
        let mut provider = create_network_provider(&operator);
        let addr = create_address(&env, &operator).await;

        RemoteInvocation::new(
            Subject::from(operator.did())
                .attenuate(Memory)
                .attenuate(Space::new("local"))
                .attenuate(Cell::new("ucan-mismatch-test"))
                .invoke(Publish::new(b"initial", None)),
            addr.clone(),
        )
        .perform(&mut provider)
        .await?;

        let wrong_edition = b"wrong-etag".to_vec();
        let result = RemoteInvocation::new(
            Subject::from(operator.did())
                .attenuate(Memory)
                .attenuate(Space::new("local"))
                .attenuate(Cell::new("ucan-mismatch-test"))
                .invoke(Publish::new(b"updated", Some(wrong_edition))),
            addr,
        )
        .perform(&mut provider)
        .await;

        assert!(matches!(result, Err(MemoryError::EditionMismatch { .. })));

        Ok(())
    }

    #[dialog_common::test]
    async fn it_retracts_content(env: helpers::UcanS3Address) -> anyhow::Result<()> {
        let operator = Ed25519Signer::generate().await.unwrap();
        let mut provider = create_network_provider(&operator);
        let addr = create_address(&env, &operator).await;

        let edition = RemoteInvocation::new(
            Subject::from(operator.did())
                .attenuate(Memory)
                .attenuate(Space::new("local"))
                .attenuate(Cell::new("ucan-retract-test"))
                .invoke(Publish::new(b"to be deleted", None)),
            addr.clone(),
        )
        .perform(&mut provider)
        .await?;

        RemoteInvocation::new(
            Subject::from(operator.did())
                .attenuate(Memory)
                .attenuate(Space::new("local"))
                .attenuate(Cell::new("ucan-retract-test"))
                .invoke(Retract::new(edition)),
            addr.clone(),
        )
        .perform(&mut provider)
        .await?;

        let resolved = RemoteInvocation::new(
            Subject::from(operator.did())
                .attenuate(Memory)
                .attenuate(Space::new("local"))
                .attenuate(Cell::new("ucan-retract-test"))
                .invoke(Resolve),
            addr,
        )
        .perform(&mut provider)
        .await?;

        assert!(resolved.is_none());

        Ok(())
    }

    #[dialog_common::test]
    async fn it_handles_different_spaces(env: helpers::UcanS3Address) -> anyhow::Result<()> {
        let operator = Ed25519Signer::generate().await.unwrap();
        let mut provider = create_network_provider(&operator);
        let addr = create_address(&env, &operator).await;

        RemoteInvocation::new(
            Subject::from(operator.did())
                .attenuate(Memory)
                .attenuate(Space::new("ucan-space1"))
                .attenuate(Cell::new("cell"))
                .invoke(Publish::new(b"content1", None)),
            addr.clone(),
        )
        .perform(&mut provider)
        .await?;

        RemoteInvocation::new(
            Subject::from(operator.did())
                .attenuate(Memory)
                .attenuate(Space::new("ucan-space2"))
                .attenuate(Cell::new("cell"))
                .invoke(Publish::new(b"content2", None)),
            addr.clone(),
        )
        .perform(&mut provider)
        .await?;

        let result1 = RemoteInvocation::new(
            Subject::from(operator.did())
                .attenuate(Memory)
                .attenuate(Space::new("ucan-space1"))
                .attenuate(Cell::new("cell"))
                .invoke(Resolve),
            addr.clone(),
        )
        .perform(&mut provider)
        .await?;
        assert_eq!(result1.unwrap().content, b"content1".to_vec());

        let result2 = RemoteInvocation::new(
            Subject::from(operator.did())
                .attenuate(Memory)
                .attenuate(Space::new("ucan-space2"))
                .attenuate(Cell::new("cell"))
                .invoke(Resolve),
            addr,
        )
        .perform(&mut provider)
        .await?;
        assert_eq!(result2.unwrap().content, b"content2".to_vec());

        Ok(())
    }

    #[dialog_common::test]
    async fn it_handles_large_content(env: helpers::UcanS3Address) -> anyhow::Result<()> {
        let operator = Ed25519Signer::generate().await.unwrap();
        let mut provider = create_network_provider(&operator);
        let addr = create_address(&env, &operator).await;
        let content: Vec<u8> = (0..100_000).map(|i| (i % 256) as u8).collect();

        let edition = RemoteInvocation::new(
            Subject::from(operator.did())
                .attenuate(Memory)
                .attenuate(Space::new("local"))
                .attenuate(Cell::new("ucan-large"))
                .invoke(Publish::new(content.clone(), None)),
            addr.clone(),
        )
        .perform(&mut provider)
        .await?;

        assert!(!edition.is_empty());

        let resolved = RemoteInvocation::new(
            Subject::from(operator.did())
                .attenuate(Memory)
                .attenuate(Space::new("local"))
                .attenuate(Cell::new("ucan-large"))
                .invoke(Resolve),
            addr,
        )
        .perform(&mut provider)
        .await?;

        let publication = resolved.expect("should have content");
        assert_eq!(publication.content, content);

        Ok(())
    }
}

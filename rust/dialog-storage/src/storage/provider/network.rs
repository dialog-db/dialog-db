//! Network provider that routes remote invocations to connections by address.
//!
//! This module builds on [`Router`] to provide concrete types for routing
//! capability invocations to remote sites backed by S3 or in-memory storage.
pub mod connector;
pub mod emulator;
pub mod router;
pub mod s3;
#[cfg(feature = "ucan")]
pub mod ucan;

use async_trait::async_trait;
use connector::Connector;
use dialog_capability::{Capability, Constraint, Effect, Provider};
use dialog_common::{ConditionalSend, ConditionalSync};
use dialog_effects::remote::RemoteInvocation;
use router::Router;
use std::convert::Infallible;

/// Address identifying a remote storage site.
///
/// Each variant maps to a credential type that can be resolved into a
/// [`Connection`]. Feature gates ensure only the relevant variants are compiled.
///
/// New types of remote could be introduced in the future by adding additional
/// variants to address space.
#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub enum Address {
    /// S3-compatible storage addressed by S3 credentials.
    #[cfg(feature = "s3")]
    S3(s3::Address),
    /// UCAN-delegated storage addressed by UCAN credentials.
    #[cfg(feature = "ucan")]
    Ucan(ucan::Address),
}

/// A connection to a remote storage site.
///
/// Wraps a backend that implements the various `Provider` traits for archive
/// and memory effects. Feature-gated S3 variant for production; `Emulator`
/// variant for in-memory testing.
#[derive(Debug)]
pub enum Connection<Issuer> {
    /// S3-backed connection.
    #[cfg(feature = "s3")]
    S3(s3::Connection<Issuer>),
    /// In-memory volatile connection (for testing). Only present so that
    /// `Emulator<Network>` can use same Connection type as `Network`.
    Emulator(emulator::Connection<Issuer>),
}

/// Production network provider that resolves addresses into connections.
///
/// On cache miss, matches the address variant to construct the appropriate
/// [`Connection`] backed by an [`S3`](crate::s3::S3) instance.
// Fields are used by Connector impl but only through feature-gated match arms.
#[allow(dead_code)]
pub struct Network<Issuer: Clone> {
    // pub(crate) because the emulator submodule needs direct access to build
    // emulated connections bypassing the real Connector::open.
    pub(crate) issuer: Issuer,
    pub(crate) router: Router<Address, Connection<Issuer>>,
}

impl<Issuer: Clone> Network<Issuer> {
    /// Create a new network provider with the given issuer.
    pub fn new(issuer: Issuer) -> Self {
        Self {
            issuer,
            router: Router::new(),
        }
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<Issuer> Connector<Address> for Network<Issuer>
where
    Issuer: Clone + ConditionalSend + ConditionalSync,
{
    type Connection = Connection<Issuer>;
    type Error = Infallible;

    // Address variants are feature-gated; when none are enabled Address is
    // uninhabited so this method can never be called, making the body
    // unreachable and `connection` unused.
    #[allow(unreachable_code, unused_variables)]
    async fn open(&mut self, address: &Address) -> Result<&mut Self::Connection, Infallible> {
        if self.router.get_mut(address).is_none() {
            // Match on *address (value) instead of address (reference) so that
            // when all variants are cfg'd away the empty enum is exhaustive
            // with zero arms — Rust treats references as always inhabited.
            let connection = match *address {
                #[cfg(feature = "s3")]
                Address::S3(ref credentials) => Connection::S3(s3::Connection::new(
                    credentials.clone().into(),
                    self.issuer.clone(),
                )),
                #[cfg(feature = "ucan")]
                Address::Ucan(ref credentials) => Connection::S3(ucan::Connection::new(
                    credentials.clone().into(),
                    self.issuer.clone(),
                )),
            };
            self.router.insert(address.clone(), connection);
        }
        Ok(self.router.get_mut(address).unwrap())
    }
}

/// Blanket [`Provider`] for [`Connection`]: dispatches to the S3 or emulator
/// backend depending on which variant is active.
///
/// When `s3` is disabled, [`s3::Connection`] resolves to [`Impossible`] which
/// trivially satisfies `Provider<Fx>`, and the `S3` match arm is compiled out.
///
/// [`Impossible`]: dialog_common::Impossible
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<Issuer, Fx> Provider<Fx> for Connection<Issuer>
where
    Fx: Effect + 'static,
    Fx::Of: Constraint,
    Capability<Fx>: ConditionalSend,
    Issuer: ConditionalSend + ConditionalSync,
    s3::Connection<Issuer>: Provider<Fx>,
    emulator::Connection<Issuer>: Provider<Fx>,
{
    async fn execute(&mut self, input: Capability<Fx>) -> Fx::Output {
        match self {
            #[cfg(feature = "s3")]
            Self::S3(s3) => <s3::Connection<Issuer> as Provider<Fx>>::execute(s3, input).await,
            Self::Emulator(emulated) => {
                <emulator::Connection<Issuer> as Provider<Fx>>::execute(emulated, input).await
            }
        }
    }
}

/// Blanket [`Provider`] for remote invocations on [`Network`].
///
/// Opens (or reuses) a connection for the target address, then delegates
/// to the [`Connection`] provider for the given effect.
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<Issuer, Fx> Provider<RemoteInvocation<Fx, Address>> for Network<Issuer>
where
    Fx: Effect + 'static,
    Fx::Of: Constraint,
    Capability<Fx>: ConditionalSend,
    Issuer: Clone + ConditionalSend + ConditionalSync + 'static,
    Connection<Issuer>: Provider<Fx>,
{
    // `Address` is a feature-gated enum; with no backends enabled it is
    // uninhabited, making this body unreachable.
    #[allow(unreachable_code)]
    async fn execute(&mut self, input: RemoteInvocation<Fx, Address>) -> Fx::Output {
        let (capability, address) = input.into_parts();
        let connection = match self.open(&address).await {
            Ok(c) => c,
            Err(infallible) => match infallible {},
        };
        <Connection<Issuer> as Provider<Fx>>::execute(connection, capability).await
    }
}

#[cfg(all(test, feature = "s3"))]
mod archive_tests {
    use crate::s3::helpers;
    use dialog_capability::{Did, Subject};
    use dialog_common::Blake3Hash;
    use dialog_effects::archive::{Archive, Catalog, Get, Put};
    use dialog_effects::remote::RemoteInvocation;

    use super::Address;
    use super::Network;

    const TEST_SUBJECT: &str = "did:key:zTestSubject";

    #[allow(dead_code)]
    fn test_subject() -> Subject {
        Subject::from(TEST_SUBJECT.parse::<Did>().unwrap())
    }

    #[allow(dead_code)]
    fn create_network_provider(env: &helpers::PublicS3Address) -> Network<helpers::Session> {
        Network::new(helpers::Session::new(TEST_SUBJECT.parse::<Did>().unwrap()))
    }

    #[allow(dead_code)]
    fn create_address(env: &helpers::PublicS3Address) -> Address {
        Address::S3(
            dialog_s3_credentials::s3::Credentials::public(dialog_s3_credentials::Address::new(
                &env.endpoint,
                "us-east-1",
                &env.bucket,
            ))
            .unwrap()
            .with_path_style(true),
        )
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

    use super::Address;
    use super::Network;

    const TEST_SUBJECT: &str = "did:key:zTestSubject";

    #[allow(dead_code)]
    fn test_subject() -> Subject {
        Subject::from(TEST_SUBJECT.parse::<Did>().unwrap())
    }

    #[allow(dead_code)]
    fn create_network_provider(env: &helpers::S3Address) -> Network<helpers::Session> {
        Network::new(helpers::Session::new(TEST_SUBJECT.parse::<Did>().unwrap()))
    }

    #[allow(dead_code)]
    fn create_address(env: &helpers::S3Address) -> Address {
        Address::S3(
            dialog_s3_credentials::s3::Credentials::private(
                dialog_s3_credentials::Address::new(&env.endpoint, "us-east-1", &env.bucket),
                &env.access_key_id,
                &env.secret_access_key,
            )
            .unwrap()
            .with_path_style(true),
        )
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
        let mut provider =
            Network::new(helpers::Session::new(TEST_SUBJECT.parse::<Did>().unwrap()));
        let addr = Address::S3(
            dialog_s3_credentials::s3::Credentials::private(
                dialog_s3_credentials::Address::new(&env.endpoint, "us-east-1", &env.bucket),
                &env.access_key_id,
                "wrong-secret",
            )
            .unwrap()
            .with_path_style(true),
        );

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

    use super::Address;
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
    async fn create_address(env: &helpers::UcanS3Address, operator: &Ed25519Signer) -> Address {
        let delegation = create_test_delegation_chain(operator, operator, &["archive"]).await;
        let ucan_credentials = UcanCredentials::new(env.access_service_url.clone(), delegation);
        Address::Ucan(ucan_credentials)
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
            Subject::from(operator.did().clone())
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
            Subject::from(operator.did().clone())
                .attenuate(Archive)
                .attenuate(Catalog::new("cat1"))
                .invoke(Put::new(digest1.clone(), content1.clone())),
            addr.clone(),
        )
        .perform(&mut provider)
        .await?;

        RemoteInvocation::new(
            Subject::from(operator.did().clone())
                .attenuate(Archive)
                .attenuate(Catalog::new("cat2"))
                .invoke(Put::new(digest2.clone(), content2.clone())),
            addr.clone(),
        )
        .perform(&mut provider)
        .await?;

        let result1 = RemoteInvocation::new(
            Subject::from(operator.did().clone())
                .attenuate(Archive)
                .attenuate(Catalog::new("cat1"))
                .invoke(Get::new(digest1)),
            addr.clone(),
        )
        .perform(&mut provider)
        .await?;
        assert_eq!(result1, Some(content1));

        let result2 = RemoteInvocation::new(
            Subject::from(operator.did().clone())
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
                Subject::from(operator.did().clone())
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
            Subject::from(operator.did().clone())
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

    use super::Address;
    use super::Network;

    const TEST_SUBJECT: &str = "did:key:zTestSubject";

    #[allow(dead_code)]
    fn test_subject() -> Subject {
        Subject::from(TEST_SUBJECT.parse::<Did>().unwrap())
    }

    #[allow(dead_code)]
    fn create_network_provider(env: &helpers::PublicS3Address) -> Network<helpers::Session> {
        Network::new(helpers::Session::new(TEST_SUBJECT.parse::<Did>().unwrap()))
    }

    #[allow(dead_code)]
    fn create_address(env: &helpers::PublicS3Address) -> Address {
        Address::S3(
            dialog_s3_credentials::s3::Credentials::public(dialog_s3_credentials::Address::new(
                &env.endpoint,
                "us-east-1",
                &env.bucket,
            ))
            .unwrap()
            .with_path_style(true),
        )
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

    use super::Address;
    use super::Network;

    const TEST_SUBJECT: &str = "did:key:zTestSubject";

    #[allow(dead_code)]
    fn test_subject() -> Subject {
        Subject::from(TEST_SUBJECT.parse::<Did>().unwrap())
    }

    #[allow(dead_code)]
    fn create_network_provider(env: &helpers::S3Address) -> Network<helpers::Session> {
        Network::new(helpers::Session::new(TEST_SUBJECT.parse::<Did>().unwrap()))
    }

    #[allow(dead_code)]
    fn create_address(env: &helpers::S3Address) -> Address {
        Address::S3(
            dialog_s3_credentials::s3::Credentials::private(
                dialog_s3_credentials::Address::new(&env.endpoint, "us-east-1", &env.bucket),
                &env.access_key_id,
                &env.secret_access_key,
            )
            .unwrap()
            .with_path_style(true),
        )
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

    use super::Address;
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
    async fn create_address(env: &helpers::UcanS3Address, operator: &Ed25519Signer) -> Address {
        let delegation = create_test_delegation_chain(operator, operator, &["memory"]).await;
        let ucan_credentials = UcanCredentials::new(env.access_service_url.clone(), delegation);
        Address::Ucan(ucan_credentials)
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
            Subject::from(operator.did().clone())
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
            Subject::from(operator.did().clone())
                .attenuate(Memory)
                .attenuate(Space::new("local"))
                .attenuate(Cell::new("ucan-update-test"))
                .invoke(Publish::new(b"initial", None)),
            addr.clone(),
        )
        .perform(&mut provider)
        .await?;

        let edition2 = RemoteInvocation::new(
            Subject::from(operator.did().clone())
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
            Subject::from(operator.did().clone())
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
            Subject::from(operator.did().clone())
                .attenuate(Memory)
                .attenuate(Space::new("local"))
                .attenuate(Cell::new("ucan-retract-test"))
                .invoke(Publish::new(b"to be deleted", None)),
            addr.clone(),
        )
        .perform(&mut provider)
        .await?;

        RemoteInvocation::new(
            Subject::from(operator.did().clone())
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
            Subject::from(operator.did().clone())
                .attenuate(Memory)
                .attenuate(Space::new("ucan-space1"))
                .attenuate(Cell::new("cell"))
                .invoke(Publish::new(b"content1", None)),
            addr.clone(),
        )
        .perform(&mut provider)
        .await?;

        RemoteInvocation::new(
            Subject::from(operator.did().clone())
                .attenuate(Memory)
                .attenuate(Space::new("ucan-space2"))
                .attenuate(Cell::new("cell"))
                .invoke(Publish::new(b"content2", None)),
            addr.clone(),
        )
        .perform(&mut provider)
        .await?;

        let result1 = RemoteInvocation::new(
            Subject::from(operator.did().clone())
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
            Subject::from(operator.did().clone())
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

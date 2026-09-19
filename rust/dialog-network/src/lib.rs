#![warn(missing_docs)]
#![warn(clippy::absolute_paths)]
#![warn(clippy::default_trait_access)]
#![warn(clippy::fallible_impl_from)]
#![warn(clippy::panicking_unwrap)]
#![warn(clippy::unused_async)]
#![deny(clippy::partial_pub_fields)]
#![deny(clippy::unnecessary_self_imports)]
#![cfg_attr(not(test), warn(clippy::large_futures))]
#![cfg_attr(not(test), deny(clippy::panic))]

//! Composite network dispatch for Dialog-DB.
//!
//! This crate exposes [`Network`], the composite
//! [`Site`](dialog_capability::Site) that dispatches fork invocations to
//! the appropriate transport (S3, UCAN access service, ...). The associated
//! [`NetworkAddress`], [`NetworkAuthorization`], and `NetworkFork` types
//! are generated from the struct fields by `#[derive(Site)]` in
//! `dialog-capability`.

mod hydrate;
pub use hydrate::{Hydrate, HydrationRequest, HydrationScheduler};

use dialog_capability::Site;
use dialog_iroh_remote::channel::Channel;
use dialog_iroh_remote::site::Iroh;
use dialog_remote_fs::Fs;
use dialog_remote_s3::S3;
use dialog_remote_ucan::UcanSite;

/// Network dispatch table for fork invocations.
///
/// Holds one concrete site per supported transport. The `#[derive(Site)]`
/// macro inspects the field types and generates:
/// - [`NetworkAddress`] -- composite address enum
/// - [`NetworkAuthorization`] -- composite authorization enum
/// - `NetworkFork<Fx>` -- composite site-owned fork wrapper
/// - `Site for Network`, `SiteAddress for NetworkAddress`,
///   `Authorize<Env> for NetworkFork<Fx>`, and
///   `Provider<ForkInvocation<Network, Fx>> for Network`.
#[derive(Debug, Clone, Default, Site)]
pub struct Network {
    s3: S3,
    ucan: UcanSite,
    fs: Fs,
    iroh: Iroh,
}

impl Network {
    /// Reach peers over `channel`.
    ///
    /// The other three transports are addressed by URL and need nothing
    /// configured; a peer is dialed, so the [`Iroh`] variant of this
    /// table can only answer once something has been given a way to
    /// dial. Until then it refuses by name — see
    /// [`Unconfigured`](dialog_iroh_remote::channel::Unconfigured).
    pub fn with_iroh(mut self, channel: impl Channel + 'static) -> Self {
        self.iroh = Iroh::new(channel);
        self
    }
}

#[cfg(test)]
mod tests {
    //! Tests verifying that `#[derive(Site)]` produces the expected types
    //! and trait impls.

    use super::*;
    use dialog_capability::Subject;
    use dialog_capability::{Fork, Provider, SiteFork};
    use dialog_capability::{Site, SiteAddress};
    use dialog_common::Buffer;
    use dialog_did_web::{CachingResolver, WebResolver};
    use dialog_effects::storage::Location;
    use dialog_effects::{Use, archive};
    use dialog_iroh_remote::channel::{Channel, ChannelError};
    use dialog_iroh_remote::helpers::Volatile;
    use dialog_iroh_remote::serve::Responder;
    use dialog_iroh_remote::site::IrohAddress;
    use dialog_iroh_remote::wire::encode;
    use dialog_operator::helpers::test_operator_with_profile;
    use dialog_remote_fs::FsAddress;
    use dialog_remote_s3::Address as S3Address;
    use dialog_remote_ucan::UcanAddress;
    use iroh_base::{EndpointAddr, SecretKey};
    use std::sync::Arc;

    fn s3_address() -> S3Address {
        S3Address::builder("https://s3.amazonaws.com")
            .region("us-east-1")
            .bucket("test")
            .build()
            .unwrap()
    }

    fn ucan_address() -> UcanAddress {
        UcanAddress::new("https://access.example.com")
    }

    fn fs_address() -> FsAddress {
        FsAddress::new(Location::temp("test-vault"))
    }

    fn iroh_address() -> IrohAddress {
        IrohAddress::from(EndpointAddr::from(SecretKey::generate().public()))
    }

    /// `NetworkAddress` is a public enum with one variant per field. Variant
    /// names are field names converted to PascalCase.
    #[test]
    fn it_generates_address_enum_with_variant_per_field() {
        let _: NetworkAddress = NetworkAddress::S3(s3_address());
        let _: NetworkAddress = NetworkAddress::Ucan(ucan_address());
        let _: NetworkAddress = NetworkAddress::Fs(fs_address());
        let _: NetworkAddress = NetworkAddress::Iroh(iroh_address());
    }

    /// `From<VariantAddress> for NetworkAddress` is generated for each
    /// concrete variant address type via the `FromSiteAddress` helper trait.
    #[test]
    fn it_generates_from_impls_via_helper_trait() {
        let net: NetworkAddress = s3_address().into();
        assert!(matches!(net, NetworkAddress::S3(_)));

        let net: NetworkAddress = ucan_address().into();
        assert!(matches!(net, NetworkAddress::Ucan(_)));

        let net: NetworkAddress = fs_address().into();
        assert!(matches!(net, NetworkAddress::Fs(_)));

        let net: NetworkAddress = iroh_address().into();
        assert!(matches!(net, NetworkAddress::Iroh(_)));
    }

    /// `Network` implements `Site` (with the generated enums as associated
    /// types) and `NetworkAddress` implements `SiteAddress`, closing the
    /// cycle back to `Network` as the Site type.
    #[test]
    fn network_implements_site_and_address() {
        fn assert_site<S: Site>() {}
        fn assert_site_address<A: SiteAddress>() {}
        assert_site::<Network>();
        assert_site_address::<NetworkAddress>();
    }

    /// Hands the container to a responder, which is what a stream would
    /// do with one more hop.
    struct Loopback(Arc<Responder<Volatile, CachingResolver<WebResolver>>>);

    #[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
    #[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
    impl Channel for Loopback {
        async fn exchange(
            &self,
            _peer: &IrohAddress,
            request: Vec<u8>,
        ) -> Result<Vec<u8>, ChannelError> {
            let response = self.0.answer(&request).await;
            Ok(encode("response", &response).expect("a response encodes"))
        }
    }

    /// The point of the field: an address selects a transport, and an
    /// iroh address has to reach the peer rather than any of the three
    /// URL-addressed sites beside it. Nothing below `Network` can check
    /// this — each site's own tests only ever see their own variant.
    #[dialog_common::test]
    async fn an_iroh_address_dispatches_to_the_peer() {
        let (operator, profile) = test_operator_with_profile().await;
        let responder = Arc::new(Responder::new(
            Volatile::default(),
            CachingResolver::new(WebResolver::new()),
        ));
        let network = Network::default().with_iroh(Loopback(responder.clone()));

        let bytes = b"routed by address alone".to_vec();
        let put = Subject::from(profile.did())
            .attenuate(Use)
            .attenuate(archive::Archive)
            .attenuate(archive::Catalog::new("blocks"))
            .invoke(archive::Put::new(Buffer::from(bytes.clone())));

        let fork: NetworkFork<archive::Put> =
            Fork::<Network, _>::new(put, NetworkAddress::Iroh(iroh_address())).into();
        let invocation = fork.authorize(&operator).await.expect("authorized");
        let outcome: Result<(), archive::ArchiveError> =
            Provider::execute(&network, invocation).await;

        outcome.expect("the peer performs the put");
        assert_eq!(
            responder
                .store()
                .get(Buffer::from(bytes.clone()).blake3_hash()),
            Some(bytes),
            "the block reached the peer, so the iroh address picked the iroh site"
        );
    }

    /// A table nobody configured a channel for still dispatches to the
    /// peer site -- and that site says what is missing rather than
    /// reporting the peer down, which are different bugs.
    #[dialog_common::test]
    async fn an_unconfigured_table_says_so() {
        let (operator, profile) = test_operator_with_profile().await;
        let put = Subject::from(profile.did())
            .attenuate(Use)
            .attenuate(archive::Archive)
            .attenuate(archive::Catalog::new("blocks"))
            .invoke(archive::Put::new(Buffer::from(b"nowhere to go".to_vec())));

        let fork: NetworkFork<archive::Put> =
            Fork::<Network, _>::new(put, NetworkAddress::Iroh(iroh_address())).into();
        let invocation = fork.authorize(&operator).await.expect("authorized");
        let outcome: Result<(), archive::ArchiveError> =
            Provider::execute(&Network::default(), invocation).await;

        let error = outcome.expect_err("nothing was dialed, so nothing was stored");
        assert!(
            format!("{error}").contains("no channel was configured"),
            "the failure names the missing configuration: {error}"
        );
    }
}

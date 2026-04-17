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
//! the appropriate transport (S3, UCAN-over-S3, ...). The associated
//! [`NetworkAddress`], [`NetworkAuthorization`], and `NetworkClaim` types
//! are generated from the struct fields by `#[derive(Site)]` in
//! `dialog-capability`.

use dialog_capability::Site;
use dialog_remote_s3::S3;
use dialog_remote_ucan_s3::UcanSite;

/// Network dispatch table for fork invocations.
///
/// Holds one concrete site per supported transport. The `#[derive(Site)]`
/// macro inspects the field types and generates:
/// - [`NetworkAddress`] -- composite address enum
/// - [`NetworkAuthorization`] -- composite authorization enum
/// - `NetworkClaim<Fx>` -- composite claim enum
/// - `Site for Network`, `SiteAddress for NetworkAddress`,
///   `Acquire<Env> for NetworkClaim<Fx>`, and
///   `Provider<ForkInvocation<Network, Fx>> for Network`.
#[derive(Debug, Clone, Copy, Default, Site)]
pub struct Network {
    s3: S3,
    ucan: UcanSite,
}

#[cfg(test)]
mod tests {
    //! Tests verifying that `#[derive(Site)]` produces the expected types
    //! and trait impls.

    use super::*;
    use dialog_capability::SiteAddress;
    use dialog_capability::site::Site;
    use dialog_remote_s3::Address as S3Address;
    use dialog_remote_ucan_s3::UcanAddress;

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

    /// `NetworkAddress` is a public enum with one variant per field. Variant
    /// names are field names converted to PascalCase.
    #[test]
    fn it_generates_address_enum_with_variant_per_field() {
        let _: NetworkAddress = NetworkAddress::S3(s3_address());
        let _: NetworkAddress = NetworkAddress::Ucan(ucan_address());
    }

    /// `From<VariantAddress> for NetworkAddress` is generated for each
    /// concrete variant address type via the `FromSiteAddress` helper trait.
    #[test]
    fn it_generates_from_impls_via_helper_trait() {
        let net: NetworkAddress = s3_address().into();
        assert!(matches!(net, NetworkAddress::S3(_)));

        let net: NetworkAddress = ucan_address().into();
        assert!(matches!(net, NetworkAddress::Ucan(_)));
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
}

//! Site trait for declaring remote execution targets.
//!
//! A [`Site`] is a marker trait that declares what authorization format
//! and address type are needed for a target location.
//!
//! No methods — all execution logic lives in [`Fork`](crate::fork::Fork)
//! and [`Provider`](crate::Provider) impls.

use dialog_common::ConditionalSend;
use serde::Serialize;
use serde::de::DeserializeOwned;

/// Authorization material for a [`Site`].
///
/// Every site's authorization type implements this trait, declaring which
/// protocol produced it. The Operator uses this to determine the
/// authorization path:
///
/// ```text
/// S::Authorization: SiteAuthorization<Protocol: Protocol>       → capability-based
/// S::Authorization: SiteAuthorization<Protocol: Authentication> → credential-based
/// ```
pub trait SiteAuthorization: ConditionalSend + 'static {
    /// The protocol that produced this authorization.
    type Protocol;
}

/// Credential-based authentication.
///
/// For sites that use ambient credentials (API keys, SigV4 signatures)
/// rather than capability delegation chains. The Operator looks up
/// credentials from a secret store and passes them to the site provider.
pub trait Authentication: ConditionalSend + 'static {
    /// The credential type (e.g., S3 access key + secret key).
    type Credentials: ConditionalSend;
}

/// Associates an address type with its corresponding site.
///
/// This trait allows inferring the site type from an address type,
/// enabling ergonomic `.fork(address)` calls without explicit site type parameters.
pub trait SiteAddress: Serialize + DeserializeOwned + Clone + ConditionalSend + 'static {
    /// The site type this address belongs to.
    type Site: Site<Address = Self>;
}

/// Pure site marker — declares types needed for remote execution.
///
/// No methods. Configuration (address) is carried by
/// [`Fork`](crate::fork::Fork) at execution time.
///
/// The Operator's `Provider<Fork<S, Fx>>` impl constrains
/// `S::Authorization` to determine the authorization path:
/// - Capability-based: `<S::Authorization as SiteAuthorization>::Protocol: Protocol`
/// - Credential-based: `<S::Authorization as SiteAuthorization>::Protocol: Authentication`
pub trait Site: Clone + ConditionalSend + 'static {
    /// The authorization material passed to the site provider.
    ///
    /// For capability-based sites, this is the protocol's authorization
    /// type (e.g., a verified UCAN proof chain with signer).
    /// For credential-based sites, this is the credentials type
    /// (e.g., S3 access key + secret key).
    type Authorization: SiteAuthorization;

    /// The address type for this site (serializable for storage/transport).
    type Address: Serialize + DeserializeOwned + Clone + ConditionalSend + 'static;
}

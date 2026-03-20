//! Access trait for declaring authorization format.

use dialog_common::ConditionalSend;

/// Declares the authorization format for a particular access pathway.
///
/// `Access` is both a trait AND a value — the type implementing it carries
/// whatever context the `Authorize` provider needs (addresses, endpoints,
/// delegation chains). The provider reads it from the `Authorize` input.
pub trait Access: ConditionalSend + 'static {
    /// What Authorize produces for this access format.
    type Authorization: ConditionalSend;
}

/// Local access — lightweight permission check, no signing.
#[derive(Debug, Clone, Copy)]
pub struct LocalAccess;

/// Proof that a local capability was authorized.
///
/// Produced by `Provider<Authorize<Fx, LocalAccess>>` after a lightweight
/// permission check. Contains no cryptographic material — just evidence
/// that the check passed.
#[derive(Debug, Clone, Copy)]
pub struct LocalAuthorization;

impl Access for LocalAccess {
    type Authorization = LocalAuthorization;
}

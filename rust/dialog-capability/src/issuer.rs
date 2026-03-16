pub use dialog_varsig::{Principal, Signature, Signer};

/// An issuer that can sign data.
///
/// Combines [`Principal`] (from dialog-varsig) with [`Signer`](dialog_varsig::Signer)
/// for a specific [`Signature`](dialog_varsig::Signature) type.
///
/// Any type implementing `Issuer` automatically satisfies
/// `dialog_ucan::Issuer<Self::Signature>` when the `ucan` feature is enabled,
/// because `Issuer<S>` is a blanket impl for `Signer<S> + Principal`.
pub trait Issuer: Principal + Signer<Self::Signature> {
    /// The signature type produced by this issuer.
    type Signature: Signature;
}

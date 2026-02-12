pub use varsig::{Principal, Signature, Signer};

/// An authority that can sign data.
///
/// Combines [`Principal`] (from varsig) with [`Signer`](varsig::Signer)
/// for a specific [`Signature`](varsig::Signature) type.
///
/// Any type implementing `Authority` automatically satisfies
/// `ucan::Issuer<Self::Signature>` when the `ucan` feature is enabled,
/// because `Issuer<S>` is a blanket impl for `Signer<S> + Principal`.
pub trait Authority: Principal + Signer<Self::Signature> {
    /// The signature type produced by this authority.
    type Signature: Signature;
}

//! Builder for [`Revocation`].
//!
//! Wraps [`InvocationBuilder`] rather than re-deriving it, fixing the fields
//! the schema pins (`cmd`, `nnc`, the shape of `args`) and leaving the caller
//! only the choices that are actually theirs.

use super::action::{PATH, REVOKE, REVOKED, Revocation};
use crate::{
    invocation::{Invocation, builder::BuildError},
    issuer::Issuer,
    promise::Promised,
};
use dialog_varsig::{Did, Principal, Signature};
use ipld_core::cid::Cid;

/// Builds a `/ucan/revoke` invocation.
///
/// The revoker is given explicitly rather than derived from the revoked
/// delegation. Deriving it conflates two different questions — *what the
/// capability was about* and *who is withdrawing it* — in one `sub` field.
/// Here `sub` is the revoker, so "what" and "by whom" stay independent.
#[derive(Debug, Clone)]
pub struct RevocationBuilder<S: Signature, I: Issuer<S>> {
    revoker: I,
    revoked: Cid,
    path: Vec<Cid>,
    marker: std::marker::PhantomData<fn() -> S>,
}

impl<S: Signature, I: Issuer<S> + Principal> RevocationBuilder<S, I> {
    /// Revoke `revoked`, signed by `revoker`.
    ///
    /// The witness path starts empty; add hops with
    /// [`witness`](Self::witness) or [`path`](Self::path).
    #[must_use]
    pub fn new(revoker: I, revoked: Cid) -> Self {
        Self {
            revoker,
            revoked,
            path: Vec::new(),
            marker: std::marker::PhantomData,
        }
    }

    /// Append one hop to the witness path.
    #[must_use]
    pub fn witness(mut self, hop: Cid) -> Self {
        self.path.push(hop);
        self
    }

    /// Set the whole witness path, in root-to-leaf order.
    #[must_use]
    pub fn path(mut self, path: Vec<Cid>) -> Self {
        self.path = path;
        self
    }

    /// Build and sign the revocation.
    ///
    /// # Errors
    ///
    /// Returns a [`BuildError`] if encoding or signing fails.
    pub async fn try_build(self) -> Result<Revocation<S>, BuildError>
    where
        S: 'static,
    {
        let revoker_did = self.revoker.did();
        let arguments = [
            (REVOKED.to_string(), Promised::Link(self.revoked)),
            (
                PATH.to_string(),
                Promised::List(self.path.iter().copied().map(Promised::Link).collect()),
            ),
        ]
        .into_iter()
        .collect();

        let invocation: Invocation<S> = Invocation::builder()
            .issuer(self.revoker)
            // The revoker is the subject: this artifact is about *their*
            // withdrawal, not about the capability being withdrawn.
            .subject(&revoker_did)
            .audience(&revoker_did)
            .command(REVOKE.segments().clone())
            .arguments(arguments)
            .proofs(vec![])
            // `nnc ""`: revocation is idempotent, so two revocations of the
            // same delegation by the same principal are the same artifact.
            .nonce(crate::crypto::nonce::Nonce::Custom(Vec::new()))
            .try_build()
            .await?;

        #[allow(clippy::expect_used)]
        Ok(Revocation::try_from(invocation)
            .expect("a revocation this builder produced must be well-formed"))
    }

    /// Build with an explicit proof chain authorizing the revoker to invoke.
    ///
    /// Distinct from the witness path: `prf` answers "may this principal
    /// invoke at all", `pth` answers "why may they revoke *this*".
    ///
    /// # Errors
    ///
    /// Returns a [`BuildError`] if encoding or signing fails.
    pub async fn try_build_with_proofs(
        self,
        proofs: Vec<Cid>,
        subject: &Did,
    ) -> Result<Revocation<S>, BuildError>
    where
        S: 'static,
    {
        let arguments = [
            (REVOKED.to_string(), Promised::Link(self.revoked)),
            (
                PATH.to_string(),
                Promised::List(self.path.iter().copied().map(Promised::Link).collect()),
            ),
        ]
        .into_iter()
        .collect();

        let invocation: Invocation<S> = Invocation::builder()
            .issuer(self.revoker)
            .subject(subject)
            .audience(subject)
            .command(REVOKE.segments().clone())
            .arguments(arguments)
            .proofs(proofs)
            .nonce(crate::crypto::nonce::Nonce::Custom(Vec::new()))
            .try_build()
            .await?;

        #[allow(clippy::expect_used)]
        Ok(Revocation::try_from(invocation)
            .expect("a revocation this builder produced must be well-formed"))
    }
}

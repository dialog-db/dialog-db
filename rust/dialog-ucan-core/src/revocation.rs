//! Revocation lookup.
//!
//! Per the [UCAN revocation spec](https://github.com/ucan-wg/revocation), an
//! issuer of a delegation in a proof chain may revoke that delegation, and a
//! revocation counts only if its issuer appears in the chain. So the question
//! a verifier asks is never "is this CID revoked?" in the abstract — it is
//! "did any of *these* principals revoke it?" Only the verifier knows the
//! chain, so it supplies the candidate set.

use crate::sync::{ConditionalSend, ConditionalSync};
use dialog_varsig::Did;
use ipld_core::cid::Cid;
use std::error::Error;
use std::future::Future;

/// Which revocations to match: those of a given delegation, issued by any of
/// a set of principals.
#[derive(Debug, Clone, Copy)]
pub struct RevocationSelector<'a> {
    /// The delegation being checked.
    pub delegation: Cid,

    /// The principals whose revocation would count for this link — the
    /// issuers in the proof chain. A revocation by anyone else is not
    /// authorized and must not be reported.
    pub by: &'a [Did],
}

impl<'a> RevocationSelector<'a> {
    /// Match revocations of `delegation` issued by any of `by`.
    #[must_use]
    pub const fn new(delegation: Cid, by: &'a [Did]) -> Self {
        Self { delegation, by }
    }
}

/// A revocation that matched a [`RevocationSelector`].
///
/// Carries the revocation's own address rather than just a verdict, so a
/// refusal can be audited back to the document that caused it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RevocationMatch {
    /// The revocation document.
    pub revocation: Cid,

    /// The principal that issued it. Always one of the `by` set in the
    /// [`RevocationSelector`] that produced this match.
    pub principal: Did,
}

/// Queries whether a delegation has been revoked by anyone who could.
pub trait RevocationChecker {
    /// Error type for query failures.
    ///
    /// A failure means the question went unanswered — never that the answer
    /// was "not revoked".
    type Error: Error + ConditionalSend + ConditionalSync + 'static;

    /// Find a revocation of `selector.delegation` issued by any of `selector.by`.
    ///
    /// `Ok(None)` means no such revocation exists: a positive finding, not an
    /// absence of information. When the query cannot be performed, that is
    /// `Err`, and verification fails rather than proceeding as though the
    /// delegation stood.
    ///
    /// Async because the answer generally lives across a network boundary.
    fn query(
        &self,
        selector: RevocationSelector<'_>,
    ) -> impl Future<Output = Result<Option<RevocationMatch>, Self::Error>>;

    /// Treat an unavailable service as "no revocation found".
    ///
    /// Verification is otherwise strict: a query that cannot be performed
    /// fails the chain. Wrapping a checker in this is how a caller opts into
    /// proceeding without that evidence — an explicit choice, made where the
    /// environment is built.
    ///
    /// This tolerates *not knowing*, never knowing-and-ignoring: a revocation
    /// the inner checker did find is still returned and still fails the chain.
    /// To tolerate only some links, wrap with a checker that applies that
    /// policy per selector.
    fn tolerate_unavailable(self) -> TolerateUnavailability<Self>
    where
        Self: Sized,
    {
        TolerateUnavailability(self)
    }
}

/// A checker that queries nothing: every delegation comes back unrevoked.
///
/// Named for what it does rather than for its effect. It does not establish
/// that nothing was revoked — it never looks. It exists so the verification
/// pipeline is complete and every call site is revocation-aware before a real
/// revocation store lands.
#[derive(Debug, Clone, Copy, Default)]
pub struct UnverifiedRevocations;

/// Error type for checkers that cannot fail.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("unreachable: this checker cannot fail")]
pub struct Never;

impl RevocationChecker for UnverifiedRevocations {
    type Error = Never;

    async fn query(
        &self,
        _selector: RevocationSelector<'_>,
    ) -> Result<Option<RevocationMatch>, Self::Error> {
        Ok(None)
    }
}

/// Turns query failures into "no revocation matched".
///
/// Built via [`RevocationChecker::tolerate_unavailable`].
#[derive(Debug, Clone, Copy)]
pub struct TolerateUnavailability<T>(pub T);

impl<T: RevocationChecker> RevocationChecker for TolerateUnavailability<T> {
    type Error = Never;

    async fn query(
        &self,
        selector: RevocationSelector<'_>,
    ) -> Result<Option<RevocationMatch>, Self::Error> {
        // A failed query becomes "nothing matched" rather than a failed
        // verification. A revocation the inner checker did match passes
        // through untouched.
        Ok(self.0.query(selector).await.unwrap_or_default())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cid::to_dagcbor_cid;

    #[derive(Debug, thiserror::Error)]
    #[error("service unavailable")]
    struct Unavailable;

    struct AlwaysFails;

    impl RevocationChecker for AlwaysFails {
        type Error = Unavailable;

        async fn query(
            &self,
            _selector: RevocationSelector<'_>,
        ) -> Result<Option<RevocationMatch>, Self::Error> {
            Err(Unavailable)
        }
    }

    struct AlwaysRevoked;

    impl RevocationChecker for AlwaysRevoked {
        type Error = Unavailable;

        async fn query(
            &self,
            selector: RevocationSelector<'_>,
        ) -> Result<Option<RevocationMatch>, Self::Error> {
            Ok(Some(RevocationMatch {
                revocation: cid(),
                principal: selector.by.first().cloned().unwrap_or_else(did),
            }))
        }
    }

    fn cid() -> Cid {
        to_dagcbor_cid(&"test")
    }

    fn did() -> Did {
        "did:key:z6MkrZ1r5XBFZjBU34qyD8fueMbMRkKw17BZaq2ivKFjnz2z"
            .parse()
            .expect("valid did")
    }

    #[dialog_common::test]
    async fn unverified_matches_nothing() {
        let found = UnverifiedRevocations
            .query(RevocationSelector::new(cid(), &[]))
            .await
            .expect("cannot fail");
        assert!(found.is_none());
    }

    #[dialog_common::test]
    async fn an_unavailable_query_is_an_error_by_default() {
        // Strict by default: the question went unanswered, so the caller must
        // deal with it rather than proceed as though nothing was revoked.
        assert!(
            AlwaysFails
                .query(RevocationSelector::new(cid(), &[]))
                .await
                .is_err()
        );
    }

    #[dialog_common::test]
    async fn tolerating_turns_a_failure_into_nothing_matched() {
        let found = AlwaysFails
            .tolerate_unavailable()
            .query(RevocationSelector::new(cid(), &[]))
            .await
            .expect("tolerated");
        assert!(found.is_none());
    }

    #[dialog_common::test]
    async fn tolerating_does_not_swallow_a_revocation() {
        // Tolerance is about not knowing, never about knowing and ignoring.
        let revoker = did();
        let found = AlwaysRevoked
            .tolerate_unavailable()
            .query(RevocationSelector::new(
                cid(),
                std::slice::from_ref(&revoker),
            ))
            .await
            .expect("tolerated")
            .expect("the revocation must survive");
        assert_eq!(found.principal, revoker);
    }
}

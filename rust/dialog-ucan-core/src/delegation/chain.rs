//! Structural properties of a delegation sequence.
//!
//! Three properties make a list of delegations a *chain* rather than a bag,
//! and none of them need an invocation to judge:
//!
//! - **linkage** — each hop is issued by whoever the previous hop delegated to
//! - **rooting** — the first hop is issued by the subject it speaks for
//! - **time** — the intersected validity window is non-empty, and contains the
//!   instant being judged against
//!
//! [`InvocationPayload::check`](crate::InvocationPayload) applies these to an
//! invocation's `prf` alongside checks that *do* need the invocation (command
//! attenuation, policy predicates over `args`). A `/ucan/revoke` witness path
//! needs the same three and none of the others, so they live here rather than
//! being re-derived: hand-rolled reimplementations are how holes get missed.

use super::Delegation;
use crate::{
    invocation::CheckFailed,
    subject::Subject,
    time::{range::TimeRange, timestamp::Timestamp},
};
use dialog_varsig::{Did, Signature};

/// Check that `hops` form a chain rooted at `subject`, valid at `now`.
///
/// Hops are expected in root-to-leaf order: the first is issued by `subject`,
/// and each subsequent hop is issued by the previous hop's audience.
///
/// Returns the intersected [`TimeRange`] across every hop.
///
/// This deliberately says nothing about commands or policy: those need an
/// invocation to judge against, and a caller that has one should use
/// [`InvocationPayload::syntactic_checks`](crate::InvocationPayload::syntactic_checks)
/// instead, which applies these same properties plus those.
///
/// # Errors
///
/// Returns a [`CheckFailed`] if the hops do not link up, are not rooted at
/// `subject`, or have no validity window containing `now`.
pub fn check_chain<'a, S: Signature + 'a, I: IntoIterator<Item = &'a Delegation<S>>>(
    hops: I,
    subject: &Did,
    now: Option<Timestamp>,
) -> Result<TimeRange, CheckFailed> {
    let mut time_range = TimeRange::unbounded();
    let mut previous: Option<&'a Delegation<S>> = None;

    for hop in hops {
        // Resolve the hop's subject: Specific(did) names it outright, Any
        // inherits — from the issuer at the root, from the established
        // subject thereafter.
        let claimed = match hop.subject() {
            Subject::Specific(specific) => specific,
            Subject::Any => {
                if previous.is_none() {
                    hop.issuer()
                } else {
                    subject
                }
            }
        };

        if claimed != subject {
            if previous.is_none() && matches!(hop.subject(), Subject::Any) {
                return Err(CheckFailed::UnprovenSubject {
                    subject: subject.clone(),
                    issuer: hop.issuer().clone(),
                });
            }
            return Err(CheckFailed::UnauthorizedSubject {
                claimed: subject.clone(),
                authorized: claimed.clone(),
            });
        }

        // Linkage at every hop but the first; rooting at the first.
        if let Some(evidence) = previous {
            if hop.issuer() != evidence.audience() {
                return Err(CheckFailed::DelegationAudienceMismatch {
                    claimed: hop.issuer().clone(),
                    authorized: evidence.audience().clone(),
                });
            }
        } else if hop.issuer() != subject {
            return Err(CheckFailed::UnprovenSubject {
                subject: subject.clone(),
                issuer: hop.issuer().clone(),
            });
        }

        time_range = time_range.intersect(hop.into());
        previous = Some(hop);
    }

    if !time_range.is_valid() {
        return Err(CheckFailed::InvalidTimeWindow { range: time_range });
    }

    // A non-empty window only says the chain could be valid at *some* instant.
    // `None` is a deliberate opt-out (historical replay, no trusted clock).
    if let Some(now) = now {
        time_range.check(&now)?;
    }

    Ok(time_range)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::DelegationBuilder;
    use dialog_credentials::{Ed25519Signer, Signer};
    use dialog_varsig::{AnySignature, Principal};
    use testresult::TestResult;

    async fn signer() -> Signer {
        Signer::from(Ed25519Signer::generate().await.expect("generate"))
    }

    async fn hop(
        issuer: &Signer,
        audience: &Signer,
        subject: &Signer,
    ) -> TestResult<Delegation<AnySignature>> {
        Ok(DelegationBuilder::new()
            .issuer(issuer.clone())
            .audience(&audience.did())
            .subject(Subject::Specific(subject.did()))
            .command(vec!["test".to_string()])
            .try_build()
            .await?)
    }

    #[dialog_common::test]
    async fn an_empty_chain_is_unbounded() -> TestResult {
        let alice = signer().await;
        let range = check_chain::<AnySignature, _>(&[], &alice.did(), None)?;
        assert!(range.is_valid());
        Ok(())
    }

    #[dialog_common::test]
    async fn a_linked_chain_rooted_at_the_subject_holds() -> TestResult {
        let alice = signer().await;
        let bob = signer().await;
        let carol = signer().await;

        let first = hop(&alice, &bob, &alice).await?;
        let second = hop(&bob, &carol, &alice).await?;

        check_chain([&first, &second], &alice.did(), Some(Timestamp::now()))?;
        Ok(())
    }

    #[dialog_common::test]
    async fn a_broken_link_is_refused() -> TestResult {
        // alice -> bob, then carol -> dave: carol never received anything.
        let alice = signer().await;
        let bob = signer().await;
        let carol = signer().await;
        let dave = signer().await;

        let first = hop(&alice, &bob, &alice).await?;
        let unlinked = hop(&carol, &dave, &alice).await?;

        let result = check_chain([&first, &unlinked], &alice.did(), None);
        assert!(
            matches!(result, Err(CheckFailed::DelegationAudienceMismatch { .. })),
            "a hop not issued by the previous audience must be refused: {result:?}"
        );
        Ok(())
    }

    #[dialog_common::test]
    async fn a_chain_not_rooted_at_the_subject_is_refused() -> TestResult {
        // The chain is internally well-linked but speaks for alice while
        // being rooted at bob.
        let alice = signer().await;
        let bob = signer().await;
        let carol = signer().await;

        let rooted_at_bob = hop(&bob, &carol, &alice).await?;

        let result = check_chain([&rooted_at_bob], &alice.did(), None);
        assert!(
            matches!(result, Err(CheckFailed::UnprovenSubject { .. })),
            "a chain must be rooted at the subject it speaks for: {result:?}"
        );
        Ok(())
    }

    #[dialog_common::test]
    async fn an_expired_hop_is_refused_at_the_judged_instant() -> TestResult {
        let alice = signer().await;
        let bob = signer().await;

        let expired = DelegationBuilder::new()
            .issuer(alice.clone())
            .audience(&bob.did())
            .subject(Subject::Specific(alice.did()))
            .command(vec!["test".to_string()])
            .expiration(Timestamp::try_from(
                std::time::UNIX_EPOCH + std::time::Duration::from_secs(1_000_000_000),
            )?)
            .try_build()
            .await?;

        let now = Some(Timestamp::now());
        assert!(
            check_chain([&expired], &alice.did(), now).is_err(),
            "an expired hop must be refused at the current instant"
        );
        // The same chain judged with no instant is a deliberate opt-out.
        assert!(
            check_chain([&expired], &alice.did(), None).is_ok(),
            "no instant means time is not judged"
        );
        Ok(())
    }
}

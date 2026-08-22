//! A `/ucan/revoke` invocation together with the blocks it names.
//!
//! [`InvocationChain::verify`] establishes that an invocation is a valid
//! invocation: signature, `prf` chain (linkage, rooting, attenuation, policy),
//! time bounds, and that no `prf` hop is revoked. All command-agnostic. For a
//! revocation that means *"whoever signed this was authorized to issue an
//! invocation with `sub` as its subject"* — nothing about `rev` or `pth`,
//! which are opaque arguments to it.
//!
//! [`RevocationChain::validate`] adds only what is specific to the command,
//! and deliberately re-derives none of the above: hand-rolled
//! reimplementations of the generic path are how holes get missed.

use super::ContainerError;
use super::invocation::InvocationChain;
use crate::container::Container;
use crate::{
    Delegation,
    delegation::{SignatureVerificationError, chain::check_chain, store::DelegationStore},
    future::FutureKind,
    invocation::{CheckError, CheckFailed},
    revocation::action::{MalformedRevocation, PATH, REVOKED, Revocation},
    subject::Subject,
    verification::{Verifiable, VerificationContext},
};
use dialog_varsig::{Did, Resolver, Signature};
use ipld_core::cid::Cid;
use std::borrow::Borrow;
use std::collections::HashMap;
use std::sync::Arc;
use thiserror::Error;

/// A revocation with the delegations its arguments name resolved from the
/// container.
///
/// Constructing one proves the artifact is *shaped* like a revocation and
/// that every CID it names is present. [`validate`](Self::validate) proves
/// the revoker was entitled to withdraw what it names.
#[derive(Debug, Clone)]
pub struct RevocationChain<S: Signature> {
    chain: InvocationChain<S>,
    revocation: Revocation<S>,
    revoked: Arc<Delegation<S>>,
    path: Vec<Arc<Delegation<S>>>,
}

impl<S: Signature> RevocationChain<S> {
    /// The underlying invocation chain, for
    /// [`verify`](InvocationChain::verify).
    #[must_use]
    pub const fn chain(&self) -> &InvocationChain<S> {
        &self.chain
    }

    /// The revocation artifact.
    #[must_use]
    pub const fn revocation(&self) -> &Revocation<S> {
        &self.revocation
    }

    /// The delegation being revoked.
    #[must_use]
    pub fn revoked(&self) -> &Delegation<S> {
        &self.revoked
    }

    /// The witness path as supplied, in whatever order it arrived.
    #[must_use]
    pub fn path(&self) -> &[Arc<Delegation<S>>] {
        &self.path
    }

    /// The principal issuing this revocation.
    #[must_use]
    pub const fn revoker(&self) -> &Did {
        self.revocation.revoker()
    }

    /// Does the revoker's own relationship to the target settle it, with no
    /// evidence required?
    ///
    /// Four cases need no witness path, and for them `pth` is neither
    /// required nor examined:
    ///
    /// - **audience** — refusing what you were handed is not a claim of
    ///   authority over anyone else, it is declining your own grant
    /// - **issuer** — you may withdraw what you issued
    /// - **subject** — it is your capability being delegated
    /// - **powerline target** — a `Subject::Any` delegation lets its holder
    ///   mint a delegation for any subject at all, so no witness could prove
    ///   anything a forger could not manufacture. Requiring one is theatre.
    fn settled_without_evidence(&self) -> bool {
        let revoker = self.revocation.revoker();

        if revoker == self.revoked.audience() || revoker == self.revoked.issuer() {
            return true;
        }

        match self.revoked.subject() {
            Subject::Any => true,
            Subject::Specific(subject) => revoker == subject,
        }
    }

    /// Check that the revoker was entitled to revoke what this names.
    ///
    /// When [`settled_without_evidence`](Self::settled_without_evidence)
    /// does not apply — the revoker is an intermediary rather than a party
    /// to the delegation — `pth` must witness their authority.
    ///
    /// The path is treated as a **pool**, not an ordered chain: the relevant
    /// subchain is the walk from a hop issued by the target's subject,
    /// following audience-to-issuer links, to a hop whose audience is the
    /// revoker. Hops off that walk are ignored, so carrying more than is
    /// relevant is not an error. The walk itself must align, be valid at the
    /// context's instant, and carry a real signature at every hop; the target
    /// must lie on it. A powerline hop within the walk stands in wherever a
    /// hop for the subject would, since a powerline implies its own subject.
    ///
    /// Whether the `pth` hops were themselves revoked is deliberately not
    /// checked: revocation is monotonic, so a revoked hop means everything
    /// below it is already dead and a revocation citing it is redundant
    /// rather than dangerous.
    ///
    /// # Errors
    ///
    /// [`RevocationError::Denied`] when the evidence does not establish the
    /// revoker's authority, and [`RevocationError::Unavailable`] when a
    /// signature could not be checked because an issuer would not resolve.
    pub async fn validate<K, T, St, C>(
        &self,
        ctx: &VerificationContext<'_, C>,
    ) -> Result<(), RevocationError<K, S, C>>
    where
        K: FutureKind,
        T: Borrow<Delegation<S>> + std::fmt::Debug,
        St: DelegationStore<K, S, T> + std::fmt::Debug,
        C: Verifiable<K, S, Proof = T, Delegations = St>,
    {
        if self.settled_without_evidence() {
            return Ok(());
        }

        // Only a `Specific` subject reaches here: `Any` settled above.
        let Subject::Specific(subject) = self.revoked.subject() else {
            return Ok(());
        };
        let revoker = self.revocation.revoker();

        // The evidence must show the revoker *held* the capability: a chain
        // from the subject that owns it down to the revoker. Whether a chain
        // continues from the revoker to the delegation being revoked is
        // irrelevant — holding the capability, they could always have
        // created one, so its absence proves nothing.
        let walk = self.walk::<K, C>(subject, revoker)?;

        // Alignment and time over the walk. This is the same implementation
        // `syntactic_checks` uses for a proof chain, so a witness path cannot
        // drift from one.
        check_chain(walk.iter().copied(), subject, ctx.time())
            .map_err(|source| RevocationError::Denied(Denial::Path { source }))?;

        // Every hop must be signed by whoever it claims issued it; without
        // this the walk is a story rather than evidence.
        for hop in &walk {
            hop.verify_signature(ctx.environment().resolver())
                .await
                .map_err(|source| match source {
                    SignatureVerificationError::ResolutionError(detail) => {
                        RevocationError::Unavailable {
                            did: hop.issuer().clone(),
                            detail,
                        }
                    }
                    other => RevocationError::Denied(Denial::HopSignature {
                        issuer: hop.issuer().clone(),
                        detail: other.to_string(),
                    }),
                })?;
        }

        Ok(())
    }

    /// Verify the invocation, then validate the revocation.
    ///
    /// The two halves answer different questions and fail differently:
    /// [`Invalid`](RevocationError::Invalid) means this is not a valid
    /// invocation at all, [`Denied`](RevocationError::Denied) means it is a
    /// valid invocation whose evidence does not justify the revocation, and
    /// [`Unavailable`](RevocationError::Unavailable) means we could not
    /// establish either.
    ///
    /// # Errors
    ///
    /// See [`RevocationError`].
    pub async fn verify<K, T, St, C>(
        &self,
        ctx: &VerificationContext<'_, C>,
    ) -> Result<(), RevocationError<K, S, C>>
    where
        K: FutureKind,
        T: Borrow<Delegation<S>> + std::fmt::Debug,
        St: DelegationStore<K, S, T> + std::fmt::Debug,
        C: Verifiable<K, S, Proof = T, Delegations = St>,
        <C::Resolver as Resolver<S>>::Error: Clone,
    {
        self.chain
            .invocation
            .check::<K, T, St, C>(ctx)
            .await
            .map_err(|error| RevocationError::Invalid(Box::new(error)))?;

        self.validate::<K, T, St, C>(ctx).await
    }

    /// The relevant subchain: from a hop issued by `subject`, following
    /// audience-to-issuer links, until a hop delegates to `revoker`.
    ///
    /// This witnesses that the revoker *held* the capability. `pth` is a
    /// pool, so hops off this walk are ignored rather than rejected —
    /// carrying more than is relevant is not an error.
    fn walk<K, C>(
        &self,
        subject: &Did,
        revoker: &Did,
    ) -> Result<Vec<&Delegation<S>>, RevocationError<K, S, C>>
    where
        K: FutureKind,
        C: Verifiable<K, S, Proof: std::fmt::Debug, Delegations: std::fmt::Debug>,
    {
        let missing = || {
            RevocationError::Denied(Denial::NoEvidenceOfPossession {
                subject: subject.clone(),
                revoker: revoker.clone(),
            })
        };

        // Rooted at the subject: evidence that does not descend from the
        // authority in question witnesses nothing about it.
        let mut holder = subject;
        let mut walk: Vec<&Delegation<S>> = Vec::new();

        while walk.len() <= self.path.len() {
            let next = self
                .path
                .iter()
                .map(AsRef::as_ref)
                .find(|hop| {
                    hop.issuer() == holder
                        // A powerline implies its own subject, so it stands
                        // in wherever a hop for this subject would.
                        && hop.subject().allows(subject)
                        && !walk.iter().any(|seen| std::ptr::eq(*seen, *hop))
                })
                .ok_or_else(missing)?;

            walk.push(next);
            holder = next.audience();

            if holder == revoker {
                return Ok(walk);
            }
        }

        Err(missing())
    }
}

/// Why a revocation was not accepted.
///
/// Mirrors the split [`VerifyError`](crate::VerifyError) draws: a statement
/// about the caller's material, or one about our own reach. A denial is
/// grounds to refuse; an unavailability is grounds to ask again.
#[derive(Debug, Error)]
pub enum RevocationError<K, S, C>
where
    K: FutureKind,
    S: Signature,
    // `Debug` on the store and proof types so this can derive `Debug` the
    // way `VerifyError` does; they reach it through `C`'s associated types
    // rather than as direct parameters, so it must be said here.
    C: Verifiable<K, S, Proof: std::fmt::Debug, Delegations: std::fmt::Debug>,
{
    /// The invocation carrying this revocation is not itself valid: a bad
    /// signature, a broken `prf` chain, expired bounds, a revoked proof.
    ///
    /// Wraps the underlying [`CheckError`] rather than rendering it, so a
    /// caller can match on *which* way the invocation failed rather than
    /// re-parsing prose. Distinct from [`Denied`](Self::Denied) because the
    /// two are different findings: this one says the artifact is not a valid
    /// invocation at all, before any question of revocation authority arises.
    ///
    /// Boxed because it is much larger than the other variants, and an
    /// unboxed one would widen every `Result` this returns.
    #[error(transparent)]
    Invalid(Box<CheckError<K, S, C>>),

    /// The invocation is valid, but its evidence does not establish that the
    /// revoker could revoke what it names.
    #[error(transparent)]
    Denied(#[from] Denial),

    /// Something we depend on could not be reached, so no finding was made.
    /// Says nothing about whether the revocation is good.
    #[error("could not resolve '{did}' to check a witness signature: {detail}")]
    Unavailable {
        /// The issuer that could not be resolved.
        did: Did,
        /// Why resolution failed.
        detail: <C::Resolver as Resolver<S>>::Error,
    },
}

/// Why the evidence does not justify a revocation.
#[derive(Debug, Clone, Error)]
pub enum Denial {
    /// The witness path does not show the revoker ever held the capability:
    /// no chain in it runs from the subject that owns the capability down to
    /// the revoker.
    #[error("the witness path does not show '{revoker}' ever held '{subject}'s capability")]
    NoEvidenceOfPossession {
        /// The principal whose capability is being revoked.
        subject: Did,
        /// The principal attempting the revocation.
        revoker: Did,
    },

    /// The walk does not align, or is not valid at the judged instant.
    #[error("the witness path is not a valid delegation chain: {source}")]
    Path {
        /// What about the chain did not hold.
        source: CheckFailed,
    },

    /// A hop is not signed by the principal it claims issued it.
    #[error("a witness hop claiming issuer '{issuer}' is not signed by them: {detail}")]
    HopSignature {
        /// The principal the hop claims as its issuer.
        issuer: Did,
        /// The underlying verification failure.
        detail: String,
    },
}

/// Why an invocation chain is not a well-formed revocation chain.
///
/// Distinct from [`Denial`]: these say the artifact could not be read, not
/// that its author lacked authority.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum MalformedRevocationChain {
    /// The invocation is not shaped like a revocation.
    #[error(transparent)]
    Malformed(#[from] MalformedRevocation),

    /// An argument names a delegation the container does not carry.
    #[error("the container does not carry the delegation '{link}' named by '{argument}'")]
    MissingBlock {
        /// Which argument named it.
        argument: &'static str,
        /// The absent block.
        link: Cid,
    },
}

impl From<MalformedRevocationChain> for ContainerError {
    fn from(error: MalformedRevocationChain) -> Self {
        ContainerError::Invocation(error.to_string())
    }
}

impl<S: Signature + serde::Serialize> RevocationChain<S>
where
    Delegation<S>: serde::Serialize,
{
    /// Assemble a revocation together with the blocks its arguments name.
    ///
    /// `delegations` must contain every CID the revocation names — the target
    /// and each witness hop — plus any delegation cited in `prf`. Missing
    /// blocks are [`MalformedRevocationChain::MissingBlock`], the same
    /// finding parsing a container short of them produces.
    ///
    /// # Errors
    ///
    /// Returns [`MalformedRevocationChain`] if a named block is absent.
    pub fn assemble(
        revocation: Revocation<S>,
        delegations: HashMap<Cid, Arc<Delegation<S>>>,
    ) -> Result<Self, MalformedRevocationChain> {
        let chain = InvocationChain::new(revocation.invocation().clone(), delegations);
        Self::try_from(chain)
    }

    /// Serialize to a container carrying every block a verifier needs.
    ///
    /// Distinct from [`InvocationChain::to_bytes`], which emits only the
    /// delegations named in `prf`. A revocation's witness is named by
    /// `args.pth` instead, so that writer would drop it and leave the
    /// receiver unable to resolve the links it was handed.
    ///
    /// # Errors
    ///
    /// Returns a [`ContainerError`] if encoding fails.
    pub fn to_bytes(&self) -> Result<Vec<u8>, ContainerError> {
        let mut tokens =
            vec![
                serde_ipld_dagcbor::to_vec(&self.chain.invocation).map_err(|error| {
                    ContainerError::Invocation(format!("failed to encode the revocation: {error}"))
                })?,
            ];
        let mut seen: std::collections::BTreeSet<Cid> = std::collections::BTreeSet::new();

        // The witness first, then anything `prf` cites: a receiver needs
        // both, and a block named twice is emitted once.
        let witness = std::iter::once(&self.revoked).chain(self.path.iter());
        let proofs = self
            .chain
            .invocation
            .proofs()
            .iter()
            .filter_map(|cid| self.chain.delegation(cid));
        for delegation in witness.chain(proofs) {
            if seen.insert(delegation.to_cid()) {
                tokens.push(delegation.encoded().to_vec());
            }
        }

        Container::new(tokens).into_bytes()
    }
}

impl<S: Signature> TryFrom<InvocationChain<S>> for RevocationChain<S> {
    type Error = MalformedRevocationChain;

    fn try_from(chain: InvocationChain<S>) -> Result<Self, Self::Error> {
        let revocation = Revocation::try_from(chain.invocation.clone())?;

        let resolve = |argument, link: &Cid| {
            chain
                .delegation(link)
                .cloned()
                .ok_or(MalformedRevocationChain::MissingBlock {
                    argument,
                    link: *link,
                })
        };

        let revoked = resolve(REVOKED, revocation.revoked())?;
        let path = revocation
            .path()
            .iter()
            .map(|link| resolve(PATH, link))
            .collect::<Result<Vec<_>, _>>()?;

        Ok(Self {
            chain,
            revocation,
            revoked,
            path,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::container::invocation::ProofStore;
    use crate::helpers::generate_signer;
    use crate::revocation::{UnverifiedRevocations, builder::RevocationBuilder};
    use crate::verification::Environment;
    use crate::{
        DelegationBuilder,
        InvocationChain,
        command::Command,
        // The crate's re-exports, not `std::time`: on wasm a `Timestamp` wraps
        // `web_time::SystemTime`, so `std::time` values do not convert.
        time::{
            Timestamp,
            timestamp::{Duration, UNIX_EPOCH},
        },
    };
    use dialog_credentials::{DidKeyResolver, Signer};
    use dialog_varsig::{AnySignature, Principal};
    use std::collections::HashMap;
    use testresult::TestResult;

    type Env = Environment<
        ProofStore<AnySignature>,
        DidKeyResolver,
        UnverifiedRevocations,
        Arc<Delegation<AnySignature>>,
    >;

    /// `issuer -> audience`, speaking for `subject`.
    async fn hop(
        issuer: &Signer,
        audience: &Signer,
        subject: &Signer,
    ) -> TestResult<Delegation<AnySignature>> {
        Ok(DelegationBuilder::new()
            .issuer(issuer.clone())
            .audience(&audience.did())
            .subject(Subject::Specific(subject.did()))
            .command(vec!["storage".to_string()])
            .try_build()
            .await?)
    }

    /// `issuer -> audience` as a powerline: subject is `Any`.
    async fn powerline(issuer: &Signer, audience: &Signer) -> TestResult<Delegation<AnySignature>> {
        Ok(DelegationBuilder::new()
            .issuer(issuer.clone())
            .audience(&audience.did())
            .subject(Subject::Any)
            .command(vec!["storage".to_string()])
            .try_build()
            .await?)
    }

    async fn revocation_chain(
        revoker: &Signer,
        target: &Delegation<AnySignature>,
        path: &[&Delegation<AnySignature>],
    ) -> TestResult<RevocationChain<AnySignature>> {
        let revocation = RevocationBuilder::new(revoker.clone(), target.to_cid())
            .path(path.iter().map(|d| d.to_cid()).collect())
            .try_build()
            .await?;

        let mut blocks: HashMap<_, _> = path
            .iter()
            .map(|d| (d.to_cid(), Arc::new((*d).clone())))
            .collect();
        blocks.insert(target.to_cid(), Arc::new(target.clone()));

        let chain = InvocationChain::new(revocation.into_invocation(), blocks);
        Ok(RevocationChain::try_from(chain)?)
    }

    /// Validate against the system clock, resolving `did:key` issuers.
    async fn validate(
        chain: &RevocationChain<AnySignature>,
    ) -> Result<(), RevocationError<crate::future::Local, AnySignature, Env>> {
        let env: Env = Environment::new(
            chain.chain().proof_store(),
            DidKeyResolver,
            UnverifiedRevocations,
        );
        chain
            .validate::<crate::future::Local, _, _, _>(&VerificationContext::new(&env))
            .await
    }

    // Cases that need no evidence at all: the revoker's own relationship to
    // the delegation settles it, so `pth` is neither required nor examined.

    #[dialog_common::test]
    async fn an_audience_may_refuse_its_own_grant_without_evidence() -> TestResult {
        let alice = generate_signer().await;
        let bob = generate_signer().await;
        let target = hop(&alice, &bob, &alice).await?;

        // Bob was handed it; declining is not a claim over anyone else.
        validate(&revocation_chain(&bob, &target, &[]).await?).await?;
        Ok(())
    }

    #[dialog_common::test]
    async fn an_issuer_may_withdraw_what_it_issued_without_evidence() -> TestResult {
        let alice = generate_signer().await;
        let bob = generate_signer().await;
        let carol = generate_signer().await;
        // Issued by alice on carol's behalf: issuer but not subject.
        let target = hop(&alice, &bob, &carol).await?;

        validate(&revocation_chain(&alice, &target, &[]).await?).await?;
        Ok(())
    }

    #[dialog_common::test]
    async fn a_subject_may_revoke_its_own_capability_without_evidence() -> TestResult {
        let alice = generate_signer().await;
        let bob = generate_signer().await;
        let carol = generate_signer().await;
        // Speaks for alice, but issued by bob to carol.
        let target = hop(&bob, &carol, &alice).await?;

        validate(&revocation_chain(&alice, &target, &[]).await?).await?;
        Ok(())
    }

    #[dialog_common::test]
    async fn a_powerline_target_needs_no_evidence_at_all() -> TestResult {
        // A powerline holder can mint a delegation for any subject, so a
        // witness proves nothing that could not be manufactured.
        let alice = generate_signer().await;
        let bob = generate_signer().await;
        let mallory = generate_signer().await;
        let target = powerline(&alice, &bob).await?;

        validate(&revocation_chain(&mallory, &target, &[]).await?).await?;
        Ok(())
    }

    #[dialog_common::test]
    async fn a_powerline_targets_path_is_not_even_examined() -> TestResult {
        // Deliberately garbage evidence: unlinked and rooted nowhere.
        let alice = generate_signer().await;
        let bob = generate_signer().await;
        let carol = generate_signer().await;
        let dave = generate_signer().await;
        let mallory = generate_signer().await;

        let target = powerline(&alice, &bob).await?;
        let nonsense = hop(&carol, &dave, &carol).await?;

        validate(&revocation_chain(&mallory, &target, &[&nonsense]).await?).await?;
        Ok(())
    }

    // Cases where the revoker is an intermediary and must show authority.

    #[dialog_common::test]
    async fn an_intermediary_may_revoke_with_a_witness_path() -> TestResult {
        // alice -> bob -> carol -> dave. Bob revokes carol -> dave: he is
        // neither party to it, so he must witness his authority over it.
        let alice = generate_signer().await;
        let bob = generate_signer().await;
        let carol = generate_signer().await;
        let dave = generate_signer().await;

        let first = hop(&alice, &bob, &alice).await?;
        let second = hop(&bob, &carol, &alice).await?;
        let target = hop(&carol, &dave, &alice).await?;

        validate(&revocation_chain(&bob, &target, &[&first, &second, &target]).await?).await?;
        Ok(())
    }

    #[dialog_common::test]
    async fn extra_hops_outside_the_relevant_walk_are_ignored() -> TestResult {
        // `pth` is a pool: carrying more than is relevant is not an error.
        let alice = generate_signer().await;
        let bob = generate_signer().await;
        let carol = generate_signer().await;
        let dave = generate_signer().await;
        let erin = generate_signer().await;

        let first = hop(&alice, &bob, &alice).await?;
        let second = hop(&bob, &carol, &alice).await?;
        let target = hop(&carol, &dave, &alice).await?;
        // Rooted elsewhere, entirely off the walk.
        let unrelated = hop(&erin, &dave, &erin).await?;

        validate(&revocation_chain(&bob, &target, &[&unrelated, &first, &second, &target]).await?)
            .await?;
        Ok(())
    }

    #[dialog_common::test]
    async fn a_powerline_hop_roots_the_walk() -> TestResult {
        // A powerline implies its own subject, so alice's grant to bob roots
        // a walk about alice's capability even though its `sub` is `Any`.
        let alice = generate_signer().await;
        let bob = generate_signer().await;
        let carol = generate_signer().await;
        let dave = generate_signer().await;

        let line = powerline(&alice, &bob).await?;
        let onward = hop(&bob, &carol, &alice).await?;
        let target = hop(&carol, &dave, &alice).await?;

        validate(&revocation_chain(&bob, &target, &[&line, &onward, &target]).await?).await?;
        Ok(())
    }

    #[dialog_common::test]
    async fn an_empty_path_denies_an_intermediary() -> TestResult {
        let alice = generate_signer().await;
        let bob = generate_signer().await;
        let carol = generate_signer().await;
        let dave = generate_signer().await;

        let target = hop(&carol, &dave, &alice).await?;

        let result = validate(&revocation_chain(&bob, &target, &[]).await?).await;
        assert!(
            matches!(
                result,
                Err(RevocationError::Denied(
                    Denial::NoEvidenceOfPossession { .. }
                ))
            ),
            "an intermediary with no evidence must be denied: {result:?}"
        );
        Ok(())
    }

    #[dialog_common::test]
    async fn a_path_not_rooted_at_the_subject_is_denied() -> TestResult {
        // Rooted at bob, who was never granted anything by alice. Evidence
        // that does not descend from the authority witnesses nothing.
        let alice = generate_signer().await;
        let bob = generate_signer().await;
        let carol = generate_signer().await;
        let dave = generate_signer().await;

        let fabricated = hop(&bob, &carol, &alice).await?;
        let target = hop(&carol, &dave, &alice).await?;

        let result =
            validate(&revocation_chain(&bob, &target, &[&fabricated, &target]).await?).await;
        assert!(
            matches!(
                result,
                Err(RevocationError::Denied(
                    Denial::NoEvidenceOfPossession { .. }
                ))
            ),
            "evidence not rooted at the subject must be denied: {result:?}"
        );
        Ok(())
    }

    #[dialog_common::test]
    async fn evidence_need_not_reach_the_target() -> TestResult {
        // Bob holds alice's capability via alice -> bob. He revokes a hop
        // on a sibling branch (alice -> carol -> dave) that he is not on.
        //
        // Holding the capability, bob could have issued that hop himself,
        // so requiring a chain from him down to it would prove nothing that
        // possession does not already establish.
        let alice = generate_signer().await;
        let bob = generate_signer().await;
        let carol = generate_signer().await;
        let dave = generate_signer().await;

        let possession = hop(&alice, &bob, &alice).await?;
        let sibling = hop(&alice, &carol, &alice).await?;
        let target = hop(&carol, &dave, &alice).await?;

        validate(&revocation_chain(&bob, &target, &[&possession, &sibling, &target]).await?)
            .await?;
        Ok(())
    }

    #[dialog_common::test]
    async fn an_expired_hop_in_the_walk_is_denied() -> TestResult {
        let alice = generate_signer().await;
        let bob = generate_signer().await;
        let carol = generate_signer().await;
        let dave = generate_signer().await;

        let expired = DelegationBuilder::new()
            .issuer(alice.clone())
            .audience(&bob.did())
            .subject(Subject::Specific(alice.did()))
            .command(vec!["storage".to_string()])
            .expiration(Timestamp::try_from(
                UNIX_EPOCH + Duration::from_secs(1_000_000_000),
            )?)
            .try_build()
            .await?;
        let second = hop(&bob, &carol, &alice).await?;
        let target = hop(&carol, &dave, &alice).await?;

        let result =
            validate(&revocation_chain(&bob, &target, &[&expired, &second, &target]).await?).await;
        assert!(
            matches!(result, Err(RevocationError::Denied(Denial::Path { .. }))),
            "an expired hop must deny the revocation: {result:?}"
        );
        Ok(())
    }

    #[dialog_common::test]
    async fn a_forged_hop_signature_is_denied() -> TestResult {
        // A hop claiming alice as issuer but signed by mallory. Without the
        // signature check the walk would align perfectly.
        let alice = generate_signer().await;
        let bob = generate_signer().await;
        let carol = generate_signer().await;
        let dave = generate_signer().await;
        let mallory = generate_signer().await;

        let forged: Delegation<AnySignature> = Delegation::forge(
            alice.did(),
            bob.did(),
            Subject::Specific(alice.did()),
            Command::new(vec!["storage".to_string()]),
            &mallory,
        )
        .await?;
        let second = hop(&bob, &carol, &alice).await?;
        let target = hop(&carol, &dave, &alice).await?;

        let result =
            validate(&revocation_chain(&bob, &target, &[&forged, &second, &target]).await?).await;
        assert!(
            matches!(
                result,
                Err(RevocationError::Denied(Denial::HopSignature { .. }))
            ),
            "a forged witness hop must deny the revocation: {result:?}"
        );
        Ok(())
    }

    #[dialog_common::test]
    async fn a_holder_may_revoke_a_hop_it_descends_from() -> TestResult {
        // alice -> bob -> carol -> dave, with dave revoking bob -> carol.
        //
        // Dave holds alice's capability, and that is the whole question:
        // possession is what the evidence establishes. Revoking a hop he
        // descends from cuts off his own authority too, which is his to do.
        let alice = generate_signer().await;
        let bob = generate_signer().await;
        let carol = generate_signer().await;
        let dave = generate_signer().await;

        let first = hop(&alice, &bob, &alice).await?;
        let target = hop(&bob, &carol, &alice).await?;
        let onward = hop(&carol, &dave, &alice).await?;

        validate(&revocation_chain(&dave, &target, &[&first, &target, &onward]).await?).await?;
        Ok(())
    }

    #[dialog_common::test]
    async fn a_packed_revocation_round_trips_with_its_witness() -> TestResult {
        // `InvocationChain::to_bytes` emits only the delegations named in
        // `prf`. A revocation's witness is named by `args.pth` instead, so
        // that writer drops it and leaves the receiver unable to resolve
        // the links it was handed.
        let alice = generate_signer().await;
        let bob = generate_signer().await;
        let carol = generate_signer().await;

        let first = hop(&alice, &bob, &alice).await?;
        let target = hop(&bob, &carol, &alice).await?;
        let chain = revocation_chain(&bob, &target, &[&first, &target]).await?;

        let bytes = chain.to_bytes()?;
        let parsed = RevocationChain::try_from(InvocationChain::try_from(bytes.as_slice())?)?;

        assert_eq!(parsed.revoked().to_cid(), target.to_cid());
        assert_eq!(parsed.path().len(), 2, "both witness hops must survive");
        validate(&parsed).await?;
        Ok(())
    }

    #[dialog_common::test]
    async fn assembling_without_a_named_block_is_malformed() -> TestResult {
        // Assembly answers the same question parsing does, so a caller
        // that forgets a block gets the same finding rather than a
        // half-built chain.
        let alice = generate_signer().await;
        let bob = generate_signer().await;
        let target = hop(&alice, &bob, &alice).await?;

        let revocation = RevocationBuilder::new(alice.clone(), target.to_cid())
            .path(vec![target.to_cid()])
            .try_build()
            .await?;

        let result = RevocationChain::assemble(revocation, HashMap::new());
        assert!(
            matches!(result, Err(MalformedRevocationChain::MissingBlock { .. })),
            "a missing block must be malformed, not a partial chain"
        );
        Ok(())
    }

    #[dialog_common::test]
    async fn a_stranger_on_no_part_of_the_path_is_denied() -> TestResult {
        let alice = generate_signer().await;
        let bob = generate_signer().await;
        let carol = generate_signer().await;
        let mallory = generate_signer().await;

        let first = hop(&alice, &bob, &alice).await?;
        let target = hop(&bob, &carol, &alice).await?;

        let result =
            validate(&revocation_chain(&mallory, &target, &[&first, &target]).await?).await;
        assert!(
            matches!(
                result,
                Err(RevocationError::Denied(
                    Denial::NoEvidenceOfPossession { .. }
                ))
            ),
            "a principal absent from the path must be denied: {result:?}"
        );
        Ok(())
    }

    #[dialog_common::test]
    async fn an_invalid_invocation_is_distinguishable_from_a_denial() -> TestResult {
        // The revocation names a proof its container does not carry, so the
        // invocation itself does not verify. That is a different finding
        // from "valid invocation, insufficient evidence", and a caller
        // needs to tell them apart.
        let alice = generate_signer().await;
        let bob = generate_signer().await;
        let target = hop(&alice, &bob, &alice).await?;
        let absent = hop(&bob, &alice, &alice).await?;

        let revocation = RevocationBuilder::new(bob.clone(), target.to_cid())
            .path(vec![])
            .try_build_with_proofs(vec![absent.to_cid()], &alice.did())
            .await?;

        let mut blocks = HashMap::new();
        blocks.insert(target.to_cid(), Arc::new(target.clone()));
        let chain =
            RevocationChain::try_from(InvocationChain::new(revocation.into_invocation(), blocks))?;

        let env: Env = Environment::new(
            chain.chain().proof_store(),
            DidKeyResolver,
            UnverifiedRevocations,
        );
        let result = chain
            .verify::<crate::future::Local, _, _, _>(&VerificationContext::new(&env))
            .await;

        assert!(
            matches!(result, Err(RevocationError::Invalid(_))),
            "a chain whose invocation does not verify must report Invalid, \
             not a denial: {result:?}"
        );
        Ok(())
    }

    #[dialog_common::test]
    async fn a_valid_invocation_with_good_evidence_verifies_end_to_end() -> TestResult {
        // The positive counterpart: both halves pass.
        let alice = generate_signer().await;
        let bob = generate_signer().await;
        let target = hop(&alice, &bob, &alice).await?;

        // Bob is the audience, so no evidence is needed; the invocation is
        // self-issued by bob over his own subject.
        let chain = revocation_chain(&bob, &target, &[]).await?;
        let env: Env = Environment::new(
            chain.chain().proof_store(),
            DidKeyResolver,
            UnverifiedRevocations,
        );
        chain
            .verify::<crate::future::Local, _, _, _>(&VerificationContext::new(&env))
            .await?;
        Ok(())
    }

    #[dialog_common::test]
    async fn a_missing_block_is_malformed_not_denied() -> TestResult {
        let alice = generate_signer().await;
        let bob = generate_signer().await;
        let target = hop(&alice, &bob, &alice).await?;

        let revocation = RevocationBuilder::new(alice.clone(), target.to_cid())
            .witness(target.to_cid())
            .try_build()
            .await?;
        let chain = InvocationChain::new(revocation.into_invocation(), HashMap::new());

        assert!(
            matches!(
                RevocationChain::try_from(chain),
                Err(MalformedRevocationChain::MissingBlock { .. })
            ),
            "an unresolvable link is malformed input, not a denial"
        );
        Ok(())
    }
}

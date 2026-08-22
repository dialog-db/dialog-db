//! Verification context and verdict.
//!
//! Verifying an invocation chain needs four things from its environment: the
//! delegations the chain refers to, a resolver for issuer keys, a revocation
//! checker, and the instant to judge time bounds against. [`Verifiable`] names
//! the first three; [`VerificationContext`] pairs an environment with a
//! sampled instant.

use crate::sync::{ConditionalSend, ConditionalSync};
use crate::{
    Delegation, delegation::store::DelegationStore, future::FutureKind,
    revocation::RevocationChecker, time::timestamp::Timestamp,
};
use dialog_varsig::{Resolver, Signature};
use std::borrow::Borrow;
use std::marker::PhantomData;

/// What an environment must supply to verify an invocation chain.
pub trait Verifiable<K: FutureKind, S: Signature> {
    /// The proof type the delegation store yields.
    type Proof: Borrow<Delegation<S>>;

    /// Where the chain's delegations are read from.
    type Delegations: DelegationStore<K, S, Self::Proof>;

    /// Resolves issuer DIDs to verifiers.
    ///
    /// The error is `'static` so a failed resolution can be type-erased and
    /// carried through the concurrent pass alongside failures from other
    /// resolvers.
    type Resolver: Resolver<S, Error: ConditionalSend + ConditionalSync + 'static>;

    /// Looks up revocation status.
    type Revocations: RevocationChecker;

    /// The delegation store.
    fn delegations(&self) -> &Self::Delegations;

    /// The DID resolver.
    fn resolver(&self) -> &Self::Resolver;

    /// The revocation checker.
    fn revocations(&self) -> &Self::Revocations;
}

/// An environment plus the instant this verification judges against.
///
/// `time` is sampled once, when the context is built, and is a field rather
/// than a method — so no phase can re-read the clock and judge two parts of
/// one chain against two different instants. Re-sampling is not something a
/// phase neglects to avoid; it is not expressible, because no phase holds a
/// clock.
pub struct VerificationContext<'a, T> {
    ctx: &'a T,
    time: Option<Timestamp>,
}

impl<'a, T> VerificationContext<'a, T> {
    /// Judge against the system clock, sampled now.
    #[must_use]
    pub fn new(ctx: &'a T) -> Self {
        Self {
            ctx,
            time: Some(Timestamp::now()),
        }
    }

    /// Judge against a caller-supplied instant.
    ///
    /// `None` means "do not judge time at all" — for replaying a historical
    /// chain, or checking a token on a device with no trusted clock. It is a
    /// deliberate opt-out, never a default.
    #[must_use]
    pub const fn at(ctx: &'a T, time: Option<Timestamp>) -> Self {
        Self { ctx, time }
    }

    /// The environment.
    #[must_use]
    pub const fn environment(&self) -> &'a T {
        self.ctx
    }

    /// The instant this verification judges against, sampled once.
    #[must_use]
    pub const fn time(&self) -> Option<Timestamp> {
        self.time
    }
}

/// A ready-made environment: a proof store, a resolver, and a revocation
/// checker bundled into something [`Verifiable`].
///
/// Most callers want this rather than implementing [`Verifiable`] themselves.
/// Pair it with [`VerificationContext::new`] to judge against the system
/// clock:
///
/// ```no_run
/// # use dialog_ucan_core::{
/// #     Delegation, InvocationChain, UnverifiedRevocations, VerificationContext,
/// #     verification::Environment,
/// # };
/// # use dialog_credentials::DidKeyResolver;
/// # use dialog_varsig::AnySignature;
/// # use std::sync::Arc;
/// # async fn example(chain: &InvocationChain<AnySignature>) {
/// // `Environment` is generic over the proof type, which the store fixes.
/// let env: Environment<_, _, _, Arc<Delegation<AnySignature>>> =
///     Environment::new(chain.proof_store(), DidKeyResolver, UnverifiedRevocations);
///
/// let range = chain.verify(&VerificationContext::new(&env)).await;
/// # let _ = range;
/// # }
/// ```
#[derive(Debug, Clone, Copy)]
pub struct Environment<St, R, Rev, T> {
    delegations: St,
    resolver: R,
    revocations: Rev,
    proof: PhantomData<fn() -> T>,
}

impl<St, R, Rev, T> Environment<St, R, Rev, T> {
    /// Bundle a store, a resolver, and a revocation checker.
    pub const fn new(delegations: St, resolver: R, revocations: Rev) -> Self {
        Self {
            delegations,
            resolver,
            revocations,
            proof: PhantomData,
        }
    }
}

impl<K, S, T, St, R, Rev> Verifiable<K, S> for Environment<St, R, Rev, T>
where
    K: FutureKind,
    S: Signature,
    T: Borrow<Delegation<S>>,
    St: DelegationStore<K, S, T>,
    R: Resolver<S, Error: ConditionalSend + ConditionalSync + 'static>,
    Rev: RevocationChecker,
{
    type Proof = T;
    type Delegations = St;
    type Resolver = R;
    type Revocations = Rev;

    fn delegations(&self) -> &Self::Delegations {
        &self.delegations
    }

    fn resolver(&self) -> &Self::Resolver {
        &self.resolver
    }

    fn revocations(&self) -> &Self::Revocations {
        &self.revocations
    }
}

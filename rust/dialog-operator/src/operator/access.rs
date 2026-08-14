//! Access capability providers for Operator.
//!
//! Proofs resolve from the profile repository's access branch: the walk
//! over its `dialog.ucan/*` facts finds the cross-party chain proving the
//! PROFILE's authority, and the operator's own link — the in-memory
//! session grant minted at build — is appended on top:
//!
//! ```text
//! prove(operator, subject) = prove(profile, subject) ++ [session grant]
//! ```
//!
//! The session grant is never persisted: the operator key derives from
//! the profile key, so any device holding the profile re-mints identical
//! authority on demand. Retaining (`Retain`) is the explicit act that
//! persists a cross-party delegation into the branch, where it replicates
//! by ordinary push/pull.
//!
//! A resolved-chain cache sits over the walk. A chain is cached under
//! `(principal, subject, command)` and re-verified on every hit against
//! the claim's full access (policy predicates run against the fresh
//! parameters, the chain window must cover the requested duration), so a
//! hit is as sound as a walk — it skips the search, never the checks. The
//! epoch is the branch head version: any commit, retain, retract, or pull
//! drops the cache. Failures are never cached.

use super::Operator;
use dialog_capability::access::{
    Access, Authorize, AuthorizeError, Certificate as _, Export, Proof as _, Protocol, Prove,
    Retain, Scope as _, TimeRange,
};
use dialog_capability::{Capability, Command, Did, Fork, Policy as _, Provider, Subject};
use dialog_common::{ConditionalSend, ConditionalSync};
use dialog_effects::archive::{Get, Import, Put};
use dialog_effects::authority::{Attest, Identify};
use dialog_effects::blob::{BlobError, Import as BlobImport, Read as BlobRead, Write as BlobWrite};
use dialog_effects::memory::{Publish, Resolve};
use dialog_repository::RemoteSite;
use dialog_storage::provider::storage::Storage;
use dialog_ucan::{Ucan, UcanCertificate, UcanProof};
use dialog_ucan_core::subject::Subject as UcanSubject;
use std::collections::HashMap;

use dialog_artifacts::history::Version;

/// Resolved chains, valid for one branch head version.
#[derive(Default)]
pub(crate) struct ChainCache {
    /// The branch head the cached chains were resolved against.
    epoch: Option<Version>,
    /// Resolved chains by `(principal, subject, command)`.
    chains: HashMap<(Did, Did, String), Vec<UcanCertificate>>,
}

/// The local provider set the delegation walk needs from the operator.
///
/// Deliberately excludes the remote fork providers: forking is what
/// requires authorization, so an env that could fork from inside a proof
/// would make the trait solution (and the runtime) circular. See
/// [`AccessEnv`].
trait LocalEnv:
    Provider<Get>
    + Provider<Put>
    + Provider<Import>
    + Provider<Resolve>
    + Provider<Publish>
    + Provider<Identify>
    + Provider<Attest>
    + Provider<BlobRead>
    + Provider<BlobWrite>
    + Provider<BlobImport>
    + ConditionalSync
{
}
impl<T> LocalEnv for T where
    T: Provider<Get>
        + Provider<Put>
        + Provider<Import>
        + Provider<Resolve>
        + Provider<Publish>
        + Provider<Identify>
        + Provider<Attest>
        + Provider<BlobRead>
        + Provider<BlobWrite>
        + Provider<BlobImport>
        + ConditionalSync
{
}

/// The environment the delegation walk and retain run against.
///
/// Local effects delegate to the operator (storage and authority); the
/// remote fork providers are stubs that report content as unavailable.
/// Authorization therefore reads only locally hydrated state — it never
/// reaches for a remote mid-proof, which would require authorizing the
/// reach itself. Sync (pull through the ordinary flows) is what brings
/// access-branch state local.
struct AccessEnv<S: Clone> {
    operator: Operator<S>,
}

macro_rules! delegate_local {
    ($($effect:ty),+ $(,)?) => {$(
        #[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
        #[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
        impl<S> Provider<$effect> for AccessEnv<S>
        where
            S: Clone + ConditionalSend + ConditionalSync + 'static,
            <$effect as Command>::Input: ConditionalSend,
            Operator<S>: Provider<$effect> + ConditionalSync,
        {
            async fn execute(
                &self,
                input: <$effect as Command>::Input,
            ) -> <$effect as Command>::Output {
                Provider::<$effect>::execute(&self.operator, input).await
            }
        }
    )+};
}

delegate_local!(
    Get, Put, Import, Resolve, Publish, Identify, Attest, BlobRead, BlobWrite, BlobImport,
);

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl<S> Provider<Fork<RemoteSite, Get>> for AccessEnv<S>
where
    S: Clone + ConditionalSend + ConditionalSync + 'static,
    Self: ConditionalSync,
{
    async fn execute(
        &self,
        _input: <Fork<RemoteSite, Get> as Command>::Input,
    ) -> <Fork<RemoteSite, Get> as Command>::Output {
        // Remote content is unavailable during authorization; a block the
        // walk needs must already be local (sync hydrates it).
        Ok(None)
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl<S> Provider<Fork<RemoteSite, Resolve>> for AccessEnv<S>
where
    S: Clone + ConditionalSend + ConditionalSync + 'static,
    Self: ConditionalSync,
{
    async fn execute(
        &self,
        _input: <Fork<RemoteSite, Resolve> as Command>::Input,
    ) -> <Fork<RemoteSite, Resolve> as Command>::Output {
        Ok(None)
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl<S> Provider<Fork<RemoteSite, BlobRead>> for AccessEnv<S>
where
    S: Clone + ConditionalSend + ConditionalSync + 'static,
    Self: ConditionalSync,
{
    async fn execute(
        &self,
        _input: <Fork<RemoteSite, BlobRead> as Command>::Input,
    ) -> <Fork<RemoteSite, BlobRead> as Command>::Output {
        Err(BlobError::NotFound(
            "remote content is unavailable during authorization".to_string(),
        ))
    }
}

impl<S: Clone> Operator<S> {
    /// The number of chains currently cached (for tests).
    #[cfg(test)]
    pub(crate) fn cached_chains(&self) -> usize {
        self.chains.lock().chains.len()
    }

    /// The cache key for a claim, or `None` when the claim is not
    /// cacheable (a wildcard subject or self-authorization resolves to an
    /// empty proof without touching storage).
    fn cache_key(claim: &Prove<Ucan>) -> Option<(Did, Did, String)> {
        let subject = match &claim.access.subject {
            UcanSubject::Specific(did) => did.clone(),
            UcanSubject::Any => return None,
        };
        if claim.principal == subject {
            return None;
        }
        Some((
            claim.principal.clone(),
            subject,
            claim.access.command.to_string(),
        ))
    }

    /// Serve a claim from the cache: the chain resolved for this key at
    /// the current epoch, re-verified against the claim's access and
    /// duration. `None` on a miss or when the cached chain rejects this
    /// particular claim.
    fn cached(&self, key: &(Did, Did, String), claim: &Prove<Ucan>) -> Option<UcanProof> {
        let branch = self.delegations.get()?;
        let epoch = branch.revision().map(|revision| revision.version());
        let cache = self.chains.lock();
        if cache.epoch != epoch {
            return None;
        }
        let chain = cache.chains.get(key)?;

        let mut effective = TimeRange::unbounded();
        for certificate in chain {
            let range = certificate.verify(&claim.access).ok()?;
            effective = effective.intersect(&range);
        }
        if !effective.covers(&claim.duration) {
            return None;
        }

        let mut proof = UcanProof::new(claim.access.clone());
        for certificate in chain {
            proof.push(certificate.clone());
        }
        proof.set_duration(effective);
        Some(proof)
    }

    /// Record a resolved chain for this key at `epoch` — the branch head
    /// captured BEFORE the walk that resolved it. Stamping the head at
    /// record time instead would let a retract that lands mid-walk cache
    /// the just-retracted chain under the post-retract head, serving it
    /// until the next head movement; stamped with the pre-walk head, such
    /// an entry simply never matches a live epoch again.
    fn record(&self, key: (Did, Did, String), epoch: Option<Version>, proof: &UcanProof) {
        let mut cache = self.chains.lock();
        if cache.epoch != epoch {
            cache.chains.clear();
            cache.epoch = epoch;
        }
        cache.chains.insert(key, proof.proofs().to_vec());
    }

    /// The session grant covering `claim`, if the operator holds one.
    fn session_grant(&self, claim: &Prove<Ucan>) -> Option<&UcanCertificate> {
        self.session
            .iter()
            .find(|grant| grant.verify(&claim.access).is_ok())
    }

    /// Resolve a proof for `claim` from the access branch, with the
    /// session composition when the principal is this operator.
    async fn resolve(&self, claim: Prove<Ucan>) -> Result<UcanProof, AuthorizeError>
    where
        Self: LocalEnv,
        S: ConditionalSend + ConditionalSync + 'static,
    {
        let key = Self::cache_key(&claim);
        if let Some(key) = &key
            && let Some(proof) = self.cached(key, &claim)
        {
            return Ok(proof);
        }
        // Captured before the walk: the facts the walk reads are at most
        // this fresh, so the record must not claim a later head.
        let epoch = self
            .delegations
            .get()
            .and_then(|branch| branch.revision())
            .map(|revision| revision.version());

        let proof = if claim.principal == self.did() {
            self.prove_as_operator(&claim).await?
        } else {
            self.walk(claim.principal.clone(), &claim).await?
        };

        if let Some(key) = key {
            self.record(key, epoch, &proof);
        }
        Ok(proof)
    }

    /// `prove(operator, subject) = prove(profile, subject) ++ [session]`.
    ///
    /// The session grant covers the claim or the operator was never
    /// allowed this scope; the branch walk then proves the PROFILE's
    /// authority (empty for the profile's own subjects) and the in-memory
    /// link completes the chain. When no session grant covers the claim,
    /// fall back to a plain walk: a cross-party delegation directly to
    /// this operator may still prove it.
    async fn prove_as_operator(&self, claim: &Prove<Ucan>) -> Result<UcanProof, AuthorizeError>
    where
        Self: LocalEnv,
        S: ConditionalSend + ConditionalSync + 'static,
    {
        let Some(grant) = self.session_grant(claim) else {
            return self.walk(claim.principal.clone(), claim).await;
        };
        let grant = grant.clone();

        let mut proof = self.walk(self.profile_did(), claim).await?;
        let range = grant.verify(&claim.access)?;
        let effective = proof.duration().intersect(&range);
        if !effective.covers(&claim.duration) {
            return Err(AuthorizeError::UnprovenSubject {
                claimed: claim.principal.clone(),
                authorized: claim.access.subject().clone(),
            });
        }
        proof.push(grant);
        proof.set_duration(effective);
        Ok(proof)
    }

    /// The tree walk over the access branch's delegation records.
    async fn walk(&self, principal: Did, claim: &Prove<Ucan>) -> Result<UcanProof, AuthorizeError>
    where
        Self: LocalEnv,
        S: ConditionalSend + ConditionalSync + 'static,
    {
        let env = AccessEnv {
            operator: self.clone(),
        };
        Box::pin(
            self.delegations()?
                .delegations()
                .prove(principal, claim.access.clone())
                .during(claim.duration)
                .perform(&env),
        )
        .await
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl<S> Provider<Prove<Ucan>> for Operator<S>
where
    S: Clone + ConditionalSend + ConditionalSync + 'static,
    Self: LocalEnv + ConditionalSend,
{
    async fn execute(&self, input: Capability<Prove<Ucan>>) -> Result<UcanProof, AuthorizeError> {
        let claim = Prove::<Ucan>::of(&input);
        let mut prove = Prove::<Ucan>::new(claim.principal.clone(), claim.access.clone());
        prove.duration = claim.duration;
        self.resolve(prove).await
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl<S> Provider<Retain<Ucan>> for Operator<S>
where
    S: Clone + ConditionalSend + ConditionalSync + 'static,
    Self: LocalEnv + ConditionalSend,
{
    async fn execute(&self, input: Capability<Retain<Ucan>>) -> Result<(), AuthorizeError> {
        let delegation = Retain::<Ucan>::of(&input).delegation.clone();
        let env = AccessEnv {
            operator: self.clone(),
        };
        Box::pin(
            self.delegations()?
                .delegations()
                .retain(delegation)
                .perform(&env),
        )
        .await
        .map(|_| ())
        .map_err(|error| AuthorizeError::Malformed {
            detail: format!("failed to retain delegation: {error}"),
        })
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl<S, P> Provider<Export<P>> for Operator<S>
where
    S: Clone + ConditionalSend + ConditionalSync + 'static,
    P: Protocol,
    P::Certificate: ConditionalSend + ConditionalSync,
    Storage<S>: Provider<Export<P>>,
    Self: ConditionalSend + ConditionalSync,
{
    async fn execute(
        &self,
        input: Capability<Export<P>>,
    ) -> Result<Vec<P::Certificate>, AuthorizeError> {
        // Enumeration reads the legacy storage certificate provider: it
        // exists to migrate certificates out of it.
        input.perform(&self.storage).await
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl<S> Provider<Authorize<Ucan>> for Operator<S>
where
    S: Clone + ConditionalSend + ConditionalSync + 'static,
    Self: LocalEnv + ConditionalSend,
{
    async fn execute(
        &self,
        input: Capability<Authorize<Ucan>>,
    ) -> Result<<Ucan as Protocol>::Authorization, AuthorizeError> {
        let subject = input.subject().clone();
        let prove: Prove<Ucan> = input.into_effect().into();

        let proof = Subject::from(subject)
            .attenuate(Access)
            .invoke(prove)
            .perform(self)
            .await?;

        proof.claim(self.authority.operator_signer().clone())
    }
}

#[cfg(test)]
mod tests {
    #[cfg(target_arch = "wasm32")]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    use super::*;
    use crate::DeriveOperator as _;
    use crate::Profile;
    use crate::helpers::unique_name;
    use anyhow::Result;
    use dialog_common::time;
    use dialog_credentials::Ed25519Signer;
    use dialog_network::Network;
    use dialog_storage::provider::storage::{Storage, VolatileSpace};
    use dialog_ucan::{Parameters, Scope, UcanDelegation};
    use dialog_ucan_core::DelegationBuilder;
    use dialog_ucan_core::DelegationChain;
    use dialog_ucan_core::command::Command as UcanCommand;
    use dialog_ucan_core::time::timestamp::Timestamp;
    use dialog_varsig::Principal as _;

    fn unique(prefix: &str) -> String {
        unique_name(prefix)
    }

    async fn operator(name: &str) -> (Operator<VolatileSpace>, Profile) {
        let storage = Storage::volatile();
        let profile = Profile::open(unique(name)).perform(&storage).await.unwrap();
        let operator = profile
            .derive(b"test")
            .network(Network::default())
            .build(storage)
            .await
            .unwrap();
        (operator, profile)
    }

    fn storage_scope(subject: &Ed25519Signer) -> Scope {
        Scope {
            subject: UcanSubject::Specific(subject.did()),
            command: UcanCommand(vec!["storage".to_string()]),
            parameters: Parameters::default(),
        }
    }

    async fn retain_grant(
        operator: &Operator<VolatileSpace>,
        space: &Ed25519Signer,
        holder: &Ed25519Signer,
        expiration: Option<Timestamp>,
    ) -> UcanDelegation {
        let mut builder = DelegationBuilder::new()
            .issuer(space.clone())
            .audience(holder)
            .subject(UcanSubject::Specific(space.did()))
            .command(vec!["storage".to_string()]);
        if let Some(expiration) = expiration {
            builder = builder.expiration(expiration);
        }
        let delegation = builder.try_build().await.unwrap();
        let chain = UcanDelegation::new(DelegationChain::new(delegation));
        Subject::from(operator.profile_did())
            .attenuate(Access)
            .invoke(Retain::<Ucan>::new(chain.clone()))
            .perform(operator)
            .await
            .unwrap();
        chain
    }

    fn claim(holder: &Ed25519Signer, space: &Ed25519Signer) -> Prove<Ucan> {
        Prove::<Ucan>::new(holder.did(), storage_scope(space))
    }

    #[dialog_common::test]
    async fn it_serves_repeat_proofs_from_the_cache() -> Result<()> {
        let (operator, _profile) = operator("cache-repeat").await;
        let space = Ed25519Signer::generate().await?;
        let holder = Ed25519Signer::generate().await?;
        retain_grant(&operator, &space, &holder, None).await;

        let first = operator.resolve(claim(&holder, &space)).await?;
        assert_eq!(operator.cached_chains(), 1, "the walk's chain is cached");
        let second = operator.resolve(claim(&holder, &space)).await?;
        assert_eq!(first.proofs().len(), second.proofs().len());
        assert_eq!(operator.cached_chains(), 1);
        Ok(())
    }

    /// Any head movement drops the cache: retaining another delegation
    /// moves the branch head, and the next prove re-walks rather than
    /// serving a chain resolved against the old head.
    #[dialog_common::test]
    async fn it_invalidates_on_head_movement() -> Result<()> {
        let (operator, _profile) = operator("cache-invalidate").await;
        let space = Ed25519Signer::generate().await?;
        let holder = Ed25519Signer::generate().await?;
        retain_grant(&operator, &space, &holder, None).await;

        operator.resolve(claim(&holder, &space)).await?;
        assert_eq!(operator.cached_chains(), 1);

        // A retain moves the head; the stale epoch empties the cache and
        // the new delegation proves, which only a fresh walk can find.
        let other = Ed25519Signer::generate().await?;
        retain_grant(&operator, &other, &holder, None).await;
        let proof = operator.resolve(claim(&holder, &other)).await?;
        assert_eq!(proof.proofs().len(), 1);
        Ok(())
    }

    /// A hit is re-verified against the claim's duration: a cached chain
    /// whose window cannot cover the request refuses rather than serving.
    #[dialog_common::test]
    async fn it_rejects_a_hit_outside_the_chain_window() -> Result<()> {
        let (operator, _profile) = operator("cache-window").await;
        let space = Ed25519Signer::generate().await?;
        let holder = Ed25519Signer::generate().await?;

        let now = time::now()
            .duration_since(time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let expiration = Timestamp::try_from((now + 3600) as i128).unwrap();
        retain_grant(&operator, &space, &holder, Some(expiration)).await;

        // Warm with an unbounded claim.
        operator.resolve(claim(&holder, &space)).await?;
        assert_eq!(operator.cached_chains(), 1);

        // A claim needing validity past the chain expiry must refuse.
        let mut widened = claim(&holder, &space);
        widened.duration = TimeRange {
            not_before: Some(now),
            expiration: Some(now + 7200),
        };
        assert!(operator.resolve(widened).await.is_err());
        Ok(())
    }

    /// The cache key deliberately excludes parameters, so the hit-time
    /// re-verification is all that stands between a cached chain and a
    /// policy bypass: a chain resolved for parameters the policy covers
    /// must not answer a claim whose parameters it refuses.
    #[dialog_common::test]
    async fn it_rejects_a_hit_whose_parameters_the_policy_refuses() -> Result<()> {
        use dialog_ucan_core::delegation::policy::predicate::Predicate;
        use dialog_ucan_core::delegation::policy::selector::filter::Filter;
        use dialog_ucan_core::delegation::policy::selector::select::Select;
        use ipld_core::ipld::Ipld;

        let (operator, _profile) = operator("cache-policy").await;
        let space = Ed25519Signer::generate().await?;
        let holder = Ed25519Signer::generate().await?;

        let constrained = DelegationBuilder::new()
            .issuer(space.clone())
            .audience(&holder)
            .subject(UcanSubject::Specific(space.did()))
            .command(vec!["storage".to_string()])
            .policy(vec![Predicate::Equal(
                Select::new(vec![Filter::Field("space".to_string())]),
                Ipld::String("alpha".to_string()),
            )])
            .try_build()
            .await
            .unwrap();
        Subject::from(operator.profile_did())
            .attenuate(Access)
            .invoke(Retain::<Ucan>::new(UcanDelegation::new(
                DelegationChain::new(constrained),
            )))
            .perform(&operator)
            .await
            .unwrap();

        let with_space = |value: &str| {
            let mut scope = storage_scope(&space);
            scope.parameters = Parameters(
                [("space".to_string(), Ipld::String(value.to_string()))]
                    .into_iter()
                    .collect(),
            );
            Prove::<Ucan>::new(holder.did(), scope)
        };

        operator
            .resolve(with_space("alpha"))
            .await
            .expect("covered parameters prove");
        assert_eq!(operator.cached_chains(), 1, "the chain is cached");
        assert!(
            operator.resolve(with_space("beta")).await.is_err(),
            "a hit must be re-verified against this claim's parameters"
        );
        Ok(())
    }

    /// A record is stamped with the head captured BEFORE the walk: an
    /// entry recorded under a stale epoch must never serve once the head
    /// has moved (the retract-mid-walk race, sequential form).
    #[dialog_common::test]
    async fn it_refuses_entries_recorded_under_a_stale_epoch() -> Result<()> {
        let (operator, _profile) = operator("cache-stale-record").await;
        let space = Ed25519Signer::generate().await?;
        let holder = Ed25519Signer::generate().await?;
        retain_grant(&operator, &space, &holder, None).await;

        // The epoch a hypothetical walk would have started under.
        let stale = operator
            .delegations
            .get()
            .and_then(|branch| branch.revision())
            .map(|revision| revision.version());
        let proof = operator.resolve(claim(&holder, &space)).await?;

        // The head moves (another retain lands, as a mid-walk retract or
        // retain would); a record stamped with the pre-move epoch must
        // not serve afterwards.
        let other = Ed25519Signer::generate().await?;
        retain_grant(&operator, &other, &holder, None).await;
        let key =
            Operator::<VolatileSpace>::cache_key(&claim(&holder, &space)).expect("cacheable claim");
        operator.record(key.clone(), stale, &proof);
        assert!(
            operator.cached(&key, &claim(&holder, &space)).is_none(),
            "a stale-stamped record must miss under the moved head"
        );
        Ok(())
    }

    /// The session is in memory only: building an operator persists no
    /// certificate anywhere — the legacy store enumerates empty and the
    /// access branch holds no delegation records. This is the fix for the
    /// accumulation pathology (one immortal certificate per session).
    #[dialog_common::test]
    async fn it_leaves_no_session_residue() -> Result<()> {
        let (operator, profile) = {
            let storage = Storage::volatile();
            let profile = Profile::open(unique("no-residue"))
                .perform(&storage)
                .await
                .unwrap();
            let operator = profile
                .derive(b"test")
                .allow(Subject::any())
                .network(Network::default())
                .build(storage)
                .await
                .unwrap();
            (operator, profile)
        };

        let exported = Subject::from(profile.did())
            .attenuate(Access)
            .invoke(Export::<Ucan>::new())
            .perform(&operator)
            .await?;
        assert!(exported.is_empty(), "the legacy store must stay empty");

        let head = operator.delegations()?.revision();
        assert!(
            head.is_none(),
            "building an operator commits nothing to the access branch"
        );

        // And yet the operator authorizes: the session link is the chain.
        let space = Ed25519Signer::generate().await?;
        let proof = operator
            .resolve(Prove::<Ucan>::new(operator.did(), storage_scope(&space)))
            .await;
        // A powerline session grant covers any subject, but proving access
        // to a subject the PROFILE holds no authority over must still
        // refuse: the session link alone reaches the profile, not the
        // space.
        assert!(proof.is_err());

        // For the profile's own space the session link IS the whole chain.
        let own = operator
            .resolve(Prove::<Ucan>::new(
                operator.did(),
                Scope {
                    subject: UcanSubject::Specific(operator.profile_did()),
                    command: UcanCommand(vec![]),
                    parameters: Parameters::default(),
                },
            ))
            .await?;
        assert_eq!(own.proofs().len(), 1, "the in-memory session link");
        Ok(())
    }

    /// A retained cross-party chain composes with the session link:
    /// space grants the profile, the session carries profile to operator.
    #[dialog_common::test]
    async fn it_composes_the_session_link_over_a_retained_chain() -> Result<()> {
        let (operator, profile) = {
            let storage = Storage::volatile();
            let profile = Profile::open(unique("compose"))
                .perform(&storage)
                .await
                .unwrap();
            let operator = profile
                .derive(b"test")
                .allow(Subject::any())
                .network(Network::default())
                .build(storage)
                .await
                .unwrap();
            (operator, profile)
        };
        let space = Ed25519Signer::generate().await?;

        // space -> profile, retained (the explicit, synced act).
        let delegation = DelegationBuilder::new()
            .issuer(space.clone())
            .audience(&profile.did())
            .subject(UcanSubject::Specific(space.did()))
            .command(vec![])
            .try_build()
            .await?;
        Subject::from(profile.did())
            .attenuate(Access)
            .invoke(Retain::<Ucan>::new(UcanDelegation::new(
                DelegationChain::new(delegation),
            )))
            .perform(&operator)
            .await?;

        // operator proves: [space->profile] ++ [profile->operator session].
        let proof = operator
            .resolve(Prove::<Ucan>::new(operator.did(), storage_scope(&space)))
            .await?;
        assert_eq!(proof.proofs().len(), 2);
        Ok(())
    }
}

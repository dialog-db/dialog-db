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
//!
//! The walk's reads replicate content on demand like any other read: a
//! delegation record or envelope the local store does not hold is fetched
//! from the branch's upstream through the operator's [`WalkReach`](super::WalkReach)
//! and cached locally. The proof authorizing such a fetch resolves from
//! what is already local (bounding the recursion — see [`AccessEnv`]);
//! offline, the walk simply skips what it cannot read. An embedder that
//! does not want proving to pay download latency materializes the branch
//! up front with `Branch::download`.

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
/// Local effects delegate to the operator (storage and authority). The
/// remote fork providers dispatch through the operator's [`WalkReach`] —
/// dyn-erased fork effects installed at build — so the walk's tree and
/// envelope reads replicate content on demand exactly as any other read
/// does. The operator clone captured inside the reach closures carries
/// no reach of its own: the proof that authorizes such a fetch resolves
/// from what is already local (the retained cross-party grants and the
/// in-memory session), which bounds the recursion a fork-inside-a-proof
/// would otherwise open. Before the reach is installed (during build)
/// and offline, the forks degrade to reporting content unavailable, and
/// the walk skips what it cannot read.
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
        input: <Fork<RemoteSite, Get> as Command>::Input,
    ) -> <Fork<RemoteSite, Get> as Command>::Output {
        match self.operator.reach.get() {
            Some(reach) => (reach.get)(input).await,
            // No reach installed (mid-build, or the recursion-bounding
            // inner clone): the block must already be local.
            None => Ok(None),
        }
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
        input: <Fork<RemoteSite, Resolve> as Command>::Input,
    ) -> <Fork<RemoteSite, Resolve> as Command>::Output {
        match self.operator.reach.get() {
            Some(reach) => (reach.resolve)(input).await,
            None => Ok(None),
        }
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
        input: <Fork<RemoteSite, BlobRead> as Command>::Input,
    ) -> <Fork<RemoteSite, BlobRead> as Command>::Output {
        match self.operator.reach.get() {
            Some(reach) => (reach.blob_read)(input).await,
            None => Err(BlobError::NotFound(
                "remote content is unavailable while the walk's reach is not installed".to_string(),
            )),
        }
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

    /// Re-resolve the access branch handle's head and upstream from
    /// storage. This is what makes a pull through ANOTHER handle of the
    /// branch visible here: a pull moves the head in storage, not in
    /// this handle's cache, and both the walk and the chain-cache epoch
    /// read the cache. Best-effort: on a failure the walk still runs
    /// over the last resolved head.
    async fn refresh(&self)
    where
        Self: LocalEnv,
        S: ConditionalSend + ConditionalSync + 'static,
    {
        let Ok(branch) = self.delegations() else {
            return;
        };
        if let Err(error) = branch.refresh(self).await {
            tracing::warn!(%error, "failed to refresh the access branch head");
        }
    }

    /// Resolve a proof for `claim` from the access branch, with the
    /// session composition when the principal is this operator.
    async fn resolve(&self, claim: Prove<Ucan>) -> Result<UcanProof, AuthorizeError>
    where
        Self: LocalEnv,
        S: ConditionalSend + ConditionalSync + 'static,
    {
        self.refresh().await;

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
        // How many times a lost head race is retried before the failure
        // surfaces. One concurrent writer per attempt is already unlikely;
        // three in a row is not a race, it is a stampede worth reporting.
        const RETRY_LIMIT: usize = 3;

        let delegation = Retain::<Ucan>::of(&input).delegation.clone();
        let env = AccessEnv {
            operator: self.clone(),
        };
        let branch = self.delegations()?;
        let mut attempt = 0;
        loop {
            // The access branch is the profile repository's main branch,
            // which other handles (content commits, pulls) advance
            // concurrently — and this handle caches its head, so the
            // retain's commit would CAS against a snapshot the handle can
            // never advance on its own. Refresh first, and again after a
            // lost race, the same refresh-and-retry contract
            // documents for its callers.
            branch
                .refresh(&env)
                .await
                .map_err(|error| AuthorizeError::Unavailable {
                    detail: format!("failed to refresh the access branch: {error}"),
                })?;
            match Box::pin(
                branch
                    .delegations()
                    .retain(delegation.clone())
                    .perform(&env),
            )
            .await
            {
                Ok(_) => return Ok(()),
                Err(dialog_repository::CommitError::Publish(
                    dialog_repository::PublishError::VersionMismatch { .. },
                )) if attempt < RETRY_LIMIT => {
                    attempt += 1;
                }
                Err(error) => {
                    return Err(AuthorizeError::Malformed {
                        detail: format!("failed to retain delegation: {error}"),
                    });
                }
            }
        }
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

        let operator_signer = self.authority.operator_signer().clone();
        proof.claim(operator_signer)
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
            .issuer(dialog_credentials::Signer::from(space.clone()))
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

    /// A retain succeeds after another handle advanced the access branch.
    ///
    /// The access branch IS the profile repository's [`ACCESS_BRANCH`], so
    /// anything else writing to the profile repo — a display-name fact, a
    /// projection, a pull — moves the same head this operator's build-time
    /// handle caches. That handle cannot observe the movement on its own,
    /// so a retain CAS'ing against its cached snapshot fails with a version
    /// mismatch, and every later delegation save through the same operator
    /// fails identically. Field symptom: sign-in worked, then saving the
    /// account's root delegation failed with `Version mismatch` forever.
    #[dialog_common::test]
    async fn it_retains_after_another_handle_moved_the_head() -> Result<()> {
        use dialog_artifacts::{Attribute, Changes, Entity, Update as _, Value};
        use dialog_repository::{ACCESS_BRANCH, Repository};

        let (operator, profile) = operator("retain-stale-head").await;

        // Retain once so the operator's handle caches a real head — the
        // state a signing session leaves after saving its session grant.
        let first_space = Ed25519Signer::generate().await?;
        let first_holder = Ed25519Signer::generate().await?;
        retain_grant(&operator, &first_space, &first_holder, None).await;

        // Another handle on the same branch, opened fresh (so it sees that
        // head) and then committing through it — a display-name write, a
        // projection, a pull. This advances the head the operator's own
        // build-time handle still caches.
        let elsewhere = Repository::from(&profile)
            .branch(ACCESS_BRANCH)
            .open()
            .perform(&operator)
            .await?;
        for note in ["moved the head", "and again"] {
            let mut moved = Changes::new();
            moved.associate(
                Attribute::try_from("test.profile/name".to_string())?,
                Entity::new()?,
                Value::String(note.to_string()),
            );
            elsewhere
                .transaction()
                .integrate(moved)
                .commit()
                .perform(&operator)
                .await?;
        }

        // The operator's own handle still caches the pre-commit head.
        // Retaining through it must not fail on that staleness.
        let space = Ed25519Signer::generate().await?;
        let holder = Ed25519Signer::generate().await?;
        retain_grant(&operator, &space, &holder, None).await;

        // The retained grant proves, so the retain really landed.
        let proof = operator.resolve(claim(&holder, &space)).await?;
        assert_eq!(proof.proofs().len(), 1);
        Ok(())
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
            .issuer(dialog_credentials::Signer::from(space.clone()))
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
            .issuer(dialog_credentials::Signer::from(space.clone()))
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

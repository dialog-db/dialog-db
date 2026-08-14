//! Synced access: the operator's delegations served from a branch.
//!
//! [`SyncedAccess`] implements the operator's
//! [`AccessProvider`](dialog_operator::AccessProvider) over a branch's
//! retained delegations, so proofs resolve through the tree walk
//! ([`Delegations::prove`](crate::Delegations::prove)) and retains land as
//! synced `dialog.ucan/*` facts — replicated by ordinary push/pull instead
//! of living in a per-device certificate store.
//!
//! [`synced_access`] is the installer: it opens the profile's own
//! repository branch (the store that is always locally present — proving
//! access to it needs only the profile-to-operator delegation the builder
//! mints locally, so a fresh device can pull it before it holds anything
//! else), migrates every certificate the legacy store retains into the
//! branch, and returns an operator with the override installed:
//!
//! ```no_run
//! # use dialog_operator::{Operator, Profile};
//! # use dialog_repository::synced_access;
//! # use dialog_storage::provider::storage::VolatileSpace;
//! # async fn example(
//! #     profile: &Profile,
//! #     operator: Operator<VolatileSpace>,
//! # ) -> anyhow::Result<()> {
//! let operator = synced_access(profile, operator).await?;
//! // proofs now resolve from the profile branch's delegation records
//! # Ok(())
//! # }
//! ```

use std::sync::Arc;

use std::collections::HashMap;

use crate::Branch;
use dialog_artifacts::history::Version;
use dialog_capability::access::{
    Access, AuthorizeError, Certificate as _, Export, Proof as _, Prove, TimeRange,
};
use dialog_capability::{Did, Provider, Subject};
use dialog_common::{ConditionalSend, ConditionalSync};
use dialog_effects::archive::{Get, Import, Put};
use dialog_effects::authority::{Attest, Identify};
use dialog_effects::blob::{Import as BlobImport, Read as BlobRead, Write as BlobWrite};
use dialog_effects::memory::{Publish, Resolve};
use dialog_operator::{AccessProvider, Operator, Profile};
use dialog_storage::provider::space::SpaceProvider;
use dialog_storage::provider::storage::Storage;
use dialog_ucan::UcanCertificate;
use dialog_ucan::{Ucan, UcanDelegation, UcanProof};
use dialog_ucan_core::DelegationChain;
use dialog_ucan_core::subject::Subject as UcanSubject;
use parking_lot::Mutex;

use crate::RemoteSite;

/// The branch name delegations are retained under in the profile's own
/// repository.
pub const ACCESS_BRANCH: &str = "main";

/// An [`AccessProvider`] serving proofs and retains from a branch's
/// delegation records, with a resolved-chain cache.
///
/// The cache is what removes the per-effect amplification: a sync session
/// authorizes every block get and put, and without it each one re-walks
/// the delegation graph for an answer that cannot have changed mid-session.
/// A resolved chain is cached under `(principal, subject, command)` and
/// **re-verified on every hit** against the claim's full access (policy
/// predicates run against the fresh parameters, and the chain's window
/// must cover the requested duration), so a hit is as sound as a walk —
/// it only skips the search, never the checks. The cache epoch is the
/// branch head version: any commit, retain, retract, or pull moves the
/// head and drops the cache, so staleness is impossible by construction.
/// Failures are never cached; a claim the chain rejects falls through to
/// a full walk.
pub struct SyncedAccess<Env> {
    branch: Branch,
    env: Env,
    cache: Mutex<ChainCache>,
}

/// Resolved chains, valid for one branch head version.
#[derive(Default)]
struct ChainCache {
    /// The branch head the cached chains were resolved against.
    epoch: Option<Version>,
    /// Resolved chains by `(principal, subject, command)`.
    chains: HashMap<(Did, Did, String), Vec<UcanCertificate>>,
}

impl<Env> SyncedAccess<Env> {
    /// Serve access from `branch`'s delegation records, performing the
    /// walk's effects against `env`.
    pub fn new(branch: Branch, env: Env) -> Self {
        Self {
            branch,
            env,
            cache: Mutex::new(ChainCache::default()),
        }
    }

    /// The branch this access provider serves from.
    pub fn branch(&self) -> &Branch {
        &self.branch
    }

    /// The number of chains currently cached (for tests).
    #[cfg(test)]
    fn cached_chains(&self) -> usize {
        self.cache.lock().chains.len()
    }

    /// The cache key for a claim, or `None` when the claim is not
    /// cacheable (wildcard subject or self-authorization resolve to an
    /// empty proof without touching storage, so caching them buys
    /// nothing).
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
    /// particular claim (a fresh walk decides then).
    fn cached(&self, key: &(Did, Did, String), claim: &Prove<Ucan>) -> Option<UcanProof> {
        let epoch = self.branch.revision().map(|revision| revision.version());
        let cache = self.cache.lock();
        if cache.epoch != epoch {
            return None;
        }
        let chain = cache.chains.get(key)?;

        // Verify-on-hit: policy predicates run against THIS claim's
        // parameters, and the chain's effective window must cover the
        // requested duration. A hit skips the search, never the checks.
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

    /// Record a resolved chain for this key at the current epoch.
    fn record(&self, key: (Did, Did, String), proof: &UcanProof) {
        let epoch = self.branch.revision().map(|revision| revision.version());
        let mut cache = self.cache.lock();
        if cache.epoch != epoch {
            cache.chains.clear();
            cache.epoch = epoch;
        }
        cache.chains.insert(key, proof.proofs().to_vec());
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl<Env> AccessProvider for SyncedAccess<Env>
where
    Env: Provider<Get>
        + Provider<Put>
        + Provider<Import>
        + Provider<Resolve>
        + Provider<Publish>
        + Provider<Identify>
        + Provider<Attest>
        + Provider<BlobRead>
        + Provider<BlobWrite>
        + Provider<BlobImport>
        + Provider<Fork<RemoteSite, Get>>
        + Provider<Fork<RemoteSite, Resolve>>
        + Provider<Fork<RemoteSite, BlobRead>>
        + ConditionalSend
        + ConditionalSync
        + 'static,
{
    async fn prove(&self, claim: Prove<Ucan>) -> Result<UcanProof, AuthorizeError> {
        let key = Self::cache_key(&claim);
        if let Some(key) = &key
            && let Some(proof) = self.cached(key, &claim)
        {
            return Ok(proof);
        }

        let proof = Box::pin(
            self.branch
                .delegations()
                .prove(claim.principal.clone(), claim.access.clone())
                .during(claim.duration)
                .perform(&self.env),
        )
        .await?;

        if let Some(key) = key {
            self.record(key, &proof);
        }
        Ok(proof)
    }

    async fn retain(&self, delegation: UcanDelegation) -> Result<(), AuthorizeError> {
        self.branch
            .delegations()
            .retain(delegation)
            .perform(&self.env)
            .await
            .map(|_| ())
            .map_err(|error| AuthorizeError::Malformed {
                detail: format!("failed to retain delegation: {error}"),
            })
    }
}

use dialog_capability::Fork;

/// Serve the operator's access from the profile's own repository branch.
///
/// Opens the profile repository's [`ACCESS_BRANCH`], migrates every
/// certificate the legacy store retains into it (idempotent by content
/// address, so repeated installs re-import nothing), and returns the
/// operator with a [`SyncedAccess`] override installed. From then on
/// proofs resolve through the tree walk over the branch's `dialog.ucan/*`
/// facts and retained delegations land as synced facts.
pub async fn synced_access<S>(
    profile: &Profile,
    operator: Operator<S>,
) -> Result<Operator<S>, AuthorizeError>
where
    S: SpaceProvider
        + Provider<BlobRead>
        + Provider<BlobWrite>
        + Provider<BlobImport>
        + Clone
        + 'static,
    Operator<S>: ConditionalSend + ConditionalSync,
    Storage<S>: Provider<Prove<Ucan>> + Provider<Export<Ucan>>,
{
    let repository = crate::Repository::from(profile);
    let branch = repository
        .branch(ACCESS_BRANCH)
        .open()
        .perform(&operator)
        .await
        .map_err(|error| AuthorizeError::Malformed {
            detail: format!("failed to open the profile access branch: {error}"),
        })?;

    // Migrate the legacy certificate store: enumerate everything it holds
    // and retain it into the branch as one commit. Content-addressed
    // idempotence makes this a no-op when nothing is new.
    let certificates = Subject::from(profile.did())
        .attenuate(Access)
        .invoke(Export::<Ucan>::new())
        .perform(&operator)
        .await?;
    if !certificates.is_empty() {
        let chains = certificates
            .into_iter()
            .map(|certificate| UcanDelegation::new(DelegationChain::new(certificate.0)))
            .collect();
        branch
            .delegations()
            .retain_all(chains)
            .perform(&operator)
            .await
            .map_err(|error| AuthorizeError::Malformed {
                detail: format!("failed to migrate legacy certificates: {error}"),
            })?;
    }

    let access = SyncedAccess::new(branch, operator.clone());
    Ok(operator.with_access(Arc::new(access)))
}

#[cfg(test)]
mod tests {
    #[cfg(target_arch = "wasm32")]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    use super::*;
    use crate::DELEGATION_AUDIENCE;
    use crate::RepositoryExt as _;
    use anyhow::Result;
    use dialog_artifacts::{ArtifactSelector, Value};
    use dialog_capability::access::Retain;
    use dialog_network::Network;
    use dialog_storage::provider::storage::{Storage, VolatileSpace};
    use dialog_ucan::Scope;
    use dialog_ucan_core::DelegationBuilder;
    use dialog_ucan_core::command::Command;
    use dialog_ucan_core::subject::Subject as UcanSubject;
    use dialog_varsig::Principal as _;
    use futures_util::StreamExt as _;

    use crate::helpers::unique_name;

    fn scope(subject: dialog_capability::Did) -> Scope {
        Scope {
            subject: UcanSubject::Specific(subject),
            command: Command(vec![]),
            parameters: dialog_ucan::Parameters::default(),
        }
    }

    #[dialog_common::test]
    async fn it_migrates_legacy_certificates_and_serves_proofs_from_the_branch() -> Result<()> {
        let storage = Storage::volatile();
        let profile = Profile::open(unique_name("synced-access"))
            .perform(&storage)
            .await?;
        // `allow` retains a profile-to-operator delegation through the
        // LEGACY certificate store at build time.
        let operator = profile
            .derive(b"test")
            .allow(dialog_capability::Subject::any())
            .network(Network::default())
            .build(storage)
            .await?;

        let operator = synced_access(&profile, operator).await?;

        // The migrated delegation stands as facts in the profile branch.
        let repository = crate::Repository::from(&profile);
        let branch = repository
            .branch(ACCESS_BRANCH)
            .open()
            .perform(&operator)
            .await?;
        let facts: Vec<_> = branch
            .claims()
            .select(
                ArtifactSelector::new()
                    .the(DELEGATION_AUDIENCE.parse()?)
                    .is(Value::String(operator.did().to_string())),
            )
            .perform(&operator)
            .await?
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<Result<Vec<_>, _>>()?;
        assert_eq!(facts.len(), 1, "the legacy delegation migrated: {facts:?}");

        // Proofs resolve through the branch: the operator proves the
        // migrated powerline against the profile's own space (the
        // powerline's issuer IS that subject, so the chain is one direct
        // grant; a subject the profile holds no grant for would rightly
        // refuse on either store).
        let mut claim = Prove::<Ucan>::new(operator.did(), scope(profile.did()));
        claim.duration = TimeRange::unbounded();
        let proof = Subject::from(profile.did())
            .attenuate(Access)
            .invoke(claim)
            .perform(&operator)
            .await?;
        assert_eq!(proof.proofs().len(), 1, "proved via the migrated grant");

        Ok(())
    }

    #[dialog_common::test]
    async fn it_retains_through_the_branch_not_the_legacy_store() -> Result<()> {
        let storage = Storage::volatile();
        let profile = Profile::open(unique_name("synced-retain"))
            .perform(&storage)
            .await?;
        let operator = profile
            .derive(b"test")
            .network(Network::default())
            .build(storage)
            .await?;
        let operator = synced_access(&profile, operator).await?;

        // Retain a fresh delegation through the operator's Retain effect.
        let space = dialog_credentials::Ed25519Signer::generate().await?;
        let holder = dialog_credentials::Ed25519Signer::generate().await?;
        let delegation = DelegationBuilder::new()
            .issuer(space.clone())
            .audience(&holder)
            .subject(UcanSubject::Specific(space.did()))
            .command(vec!["storage".to_string()])
            .try_build()
            .await?;
        let chain = UcanDelegation::new(DelegationChain::new(delegation));
        Subject::from(profile.did())
            .attenuate(Access)
            .invoke(Retain::<Ucan>::new(chain))
            .perform(&operator)
            .await?;

        // The legacy store never saw it: enumeration is empty.
        let exported = Subject::from(profile.did())
            .attenuate(Access)
            .invoke(Export::<Ucan>::new())
            .perform(&operator)
            .await?;
        assert!(
            exported.is_empty(),
            "retains route to the branch, not the legacy store"
        );

        // The branch proves it.
        let mut claim = Prove::<Ucan>::new(
            holder.did(),
            Scope {
                subject: UcanSubject::Specific(space.did()),
                command: Command(vec!["storage".to_string()]),
                parameters: dialog_ucan::Parameters::default(),
            },
        );
        claim.duration = TimeRange::unbounded();
        let proof = Subject::from(profile.did())
            .attenuate(Access)
            .invoke(claim)
            .perform(&operator)
            .await?;
        assert_eq!(proof.proofs().len(), 1);

        Ok(())
    }

    #[dialog_common::test]
    async fn it_installs_idempotently() -> Result<()> {
        let storage = Storage::volatile();
        let profile = Profile::open(unique_name("synced-idempotent"))
            .perform(&storage)
            .await?;
        let operator = profile
            .derive(b"test")
            .allow(dialog_capability::Subject::any())
            .network(Network::default())
            .build(storage)
            .await?;

        let operator = synced_access(&profile, operator).await?;
        let repository = crate::Repository::from(&profile);
        let branch = repository
            .branch(ACCESS_BRANCH)
            .open()
            .perform(&operator)
            .await?;
        let head = branch.revision().map(|revision| revision.version());

        // Installing again re-migrates nothing: the branch head is
        // unchanged.
        let operator = synced_access(&profile, operator).await?;
        let branch = repository
            .branch(ACCESS_BRANCH)
            .open()
            .perform(&operator)
            .await?;
        assert_eq!(
            branch.revision().map(|revision| revision.version()),
            head,
            "a second install migrates nothing new"
        );

        Ok(())
    }

    /// The full repository flow works with synced access installed: create
    /// a repository (whose delegations now land in the profile branch),
    /// open a branch, commit and read back — every authorize along the way
    /// resolves through the tree walk.
    #[dialog_common::test]
    async fn it_serves_the_repository_flow() -> Result<()> {
        use dialog_artifacts::{Artifact, Instruction};
        use futures_util::stream;

        let storage = Storage::volatile();
        let profile = Profile::open(unique_name("synced-repo-flow"))
            .perform(&storage)
            .await?;
        let operator = profile
            .derive(b"test")
            .allow(dialog_capability::Subject::any())
            .network(Network::default())
            .build(storage)
            .await?;
        let operator = synced_access(&profile, operator).await?;

        let repo = profile
            .repository(unique_name("repo"))
            .open()
            .perform(&operator)
            .await?;
        let branch = repo.branch("main").open().perform(&operator).await?;

        branch
            .commit(stream::iter(vec![Instruction::Assert(Artifact {
                the: "user/name".parse()?,
                of: "user:1".parse()?,
                is: Value::String("Alice".into()),
                cause: None,
            })]))
            .perform(&operator)
            .await?;

        let facts: Vec<_> = branch
            .claims()
            .select(ArtifactSelector::new().the("user/name".parse()?))
            .perform(&operator)
            .await?
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<Result<Vec<_>, _>>()?;
        assert_eq!(facts.len(), 1);

        Ok(())
    }

    /// A synced-access harness with direct hands on the [`SyncedAccess`]
    /// instance, for cache assertions.
    async fn cache_harness(
        name: &str,
    ) -> Result<(
        SyncedAccess<Operator<VolatileSpace>>,
        dialog_credentials::Ed25519Signer,
        dialog_credentials::Ed25519Signer,
    )> {
        let storage = Storage::volatile();
        let profile = Profile::open(unique_name(name)).perform(&storage).await?;
        let operator = profile
            .derive(b"test")
            .network(Network::default())
            .build(storage)
            .await?;
        let repository = crate::Repository::from(&profile);
        let branch = repository
            .branch(ACCESS_BRANCH)
            .open()
            .perform(&operator)
            .await?;

        let space = dialog_credentials::Ed25519Signer::generate().await?;
        let holder = dialog_credentials::Ed25519Signer::generate().await?;
        let delegation = DelegationBuilder::new()
            .issuer(space.clone())
            .audience(&holder)
            .subject(UcanSubject::Specific(space.did()))
            .command(vec!["storage".to_string()])
            .try_build()
            .await?;
        branch
            .delegations()
            .retain(UcanDelegation::new(DelegationChain::new(delegation)))
            .perform(&operator)
            .await?;

        Ok((SyncedAccess::new(branch, operator), space, holder))
    }

    fn storage_claim(
        holder: &dialog_credentials::Ed25519Signer,
        space: &dialog_credentials::Ed25519Signer,
    ) -> Prove<Ucan> {
        Prove::<Ucan>::new(
            holder.did(),
            Scope {
                subject: UcanSubject::Specific(space.did()),
                command: Command(vec!["storage".to_string()]),
                parameters: dialog_ucan::Parameters::default(),
            },
        )
    }

    #[dialog_common::test]
    async fn it_serves_repeat_proofs_from_the_cache() -> Result<()> {
        let (access, space, holder) = cache_harness("cache-repeat").await?;

        let first = access.prove(storage_claim(&holder, &space)).await?;
        assert_eq!(access.cached_chains(), 1, "the walk's chain is cached");

        let second = access.prove(storage_claim(&holder, &space)).await?;
        assert_eq!(
            first.proofs().len(),
            second.proofs().len(),
            "a hit serves the same chain the walk found"
        );
        assert_eq!(access.cached_chains(), 1);
        Ok(())
    }

    /// Any head movement drops the cache: after retracting the delegation
    /// through the same branch, a repeat claim must fail rather than serve
    /// the stale chain.
    #[dialog_common::test]
    async fn it_invalidates_on_head_movement() -> Result<()> {
        let (access, space, holder) = cache_harness("cache-invalidate").await?;

        access.prove(storage_claim(&holder, &space)).await?;
        assert_eq!(access.cached_chains(), 1);

        // Rebuild the delegation deterministically? No — retract needs the
        // chain; re-derive it from the branch's own record via the walk's
        // proof instead.
        let proof = access.prove(storage_claim(&holder, &space)).await?;
        let chain = proof.proofs()[0].0.clone();
        access
            .branch()
            .delegations()
            .retract(UcanDelegation::new(DelegationChain::new(chain)))
            .perform(&access.env)
            .await?;

        let refused = access.prove(storage_claim(&holder, &space)).await;
        assert!(
            matches!(refused, Err(AuthorizeError::UnprovenSubject { .. })),
            "a retracted chain must not serve from the cache: {:?}",
            refused.is_ok()
        );
        Ok(())
    }

    /// A hit is re-verified against the claim's duration: a cached chain
    /// whose window cannot cover the request falls through to the walk,
    /// which refuses it.
    #[dialog_common::test]
    async fn it_rejects_a_hit_outside_the_chain_window() -> Result<()> {
        use dialog_common::time;
        use dialog_ucan_core::time::timestamp::Timestamp;

        let storage = Storage::volatile();
        let profile = Profile::open(unique_name("cache-window"))
            .perform(&storage)
            .await?;
        let operator = profile
            .derive(b"test")
            .network(Network::default())
            .build(storage)
            .await?;
        let repository = crate::Repository::from(&profile);
        let branch = repository
            .branch(ACCESS_BRANCH)
            .open()
            .perform(&operator)
            .await?;

        let now = time::now()
            .duration_since(time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let space = dialog_credentials::Ed25519Signer::generate().await?;
        let holder = dialog_credentials::Ed25519Signer::generate().await?;
        let delegation = DelegationBuilder::new()
            .issuer(space.clone())
            .audience(&holder)
            .subject(UcanSubject::Specific(space.did()))
            .command(vec!["storage".to_string()])
            .expiration(Timestamp::try_from((now + 3600) as i128).unwrap())
            .try_build()
            .await?;
        branch
            .delegations()
            .retain(UcanDelegation::new(DelegationChain::new(delegation)))
            .perform(&operator)
            .await?;
        let access = SyncedAccess::new(branch, operator);

        // Warm the cache with an unbounded claim.
        access.prove(storage_claim(&holder, &space)).await?;
        assert_eq!(access.cached_chains(), 1);

        // A claim needing validity past the chain's expiry must refuse.
        let mut claim = storage_claim(&holder, &space);
        claim.duration = TimeRange {
            not_before: Some(now),
            expiration: Some(now + 7200),
        };
        let refused = access.prove(claim).await;
        assert!(
            refused.is_err(),
            "a hit must not outlive the chain's window"
        );
        Ok(())
    }
}

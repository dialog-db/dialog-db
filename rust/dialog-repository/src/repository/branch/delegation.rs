//! UCAN delegations as data in the branch tree.
//!
//! A retained delegation decomposes into two synced pieces:
//!
//! - **Slim facts** under the reserved `dialog.ucan/*` attributes, one per
//!   queried field (audience, subject, issuer, command, and the validity
//!   bounds when present), on the entity `blob:<hash>` of the certificate's
//!   encoded envelope. They are ordinary version-controlled facts: they ride
//!   the fact indexes, replicate with the tree, merge with observed-remove
//!   semantics, and answer datalog queries.
//! - **The signed envelope as a blob**, addressed by that same hash and
//!   recorded in the blob index so push ships the bytes and a replica
//!   hydrates them on demand. The envelope is never decomposed: policy,
//!   meta, nonce, and signature live only there, and proof assembly reads
//!   it back byte-identical.
//!
//! Both land in ONE commit, so the denormalized fields cannot drift from the
//! envelope. Retraction mirrors it: the facts are retracted and the blob
//! reference tombstoned in one commit; the bytes stay in the blob store
//! (reclaiming unreferenced bytes is the deferred GC concern).
//!
//! The surface is a [`Delegations`] handle on the branch:
//!
//! ```no_run
//! # use dialog_capability::{Fork, Provider};
//! # use dialog_effects::archive::{Get, Import, Put};
//! # use dialog_effects::authority::{Attest, Identify};
//! # use dialog_effects::blob::Write as BlobWrite;
//! # use dialog_effects::memory::{Publish, Resolve};
//! # use dialog_repository::{Branch, CommitError, RemoteSite};
//! # use dialog_ucan::UcanDelegation;
//! # async fn example<Env>(
//! #     branch: &Branch,
//! #     env: &Env,
//! #     chain: UcanDelegation,
//! # ) -> Result<(), CommitError>
//! # where
//! #     Env: Provider<Get>
//! #         + Provider<Put>
//! #         + Provider<Import>
//! #         + Provider<Resolve>
//! #         + Provider<Publish>
//! #         + Provider<Identify>
//! #         + Provider<Attest>
//! #         + Provider<BlobWrite>
//! #         + Provider<Fork<RemoteSite, Get>>
//! #         + Provider<Fork<RemoteSite, Resolve>>
//! #         + dialog_common::ConditionalSync
//! #         + 'static,
//! # {
//! // retain: every certificate in the chain becomes facts + an envelope blob
//! let entities = branch.delegations().retain(chain.clone()).perform(env).await?;
//!
//! // retract: facts retracted, blob reference tombstoned, bytes untouched
//! branch.delegations().retract(chain).perform(env).await?;
//! # Ok(())
//! # }
//! ```

mod prove;
pub use prove::*;

use crate::repository::branch::blob::index_store;
use crate::{Branch, CommitError, Index, RemoteSite};
use dialog_artifacts::{
    Artifact, Attribute, BlobIndexExt as _, BlobRecord, DialogArtifactsError, Entity, Instruction,
    Value,
};
use dialog_capability::access::{Certificate as _, Delegation as _};
use dialog_capability::{ANY_SUBJECT, Fork, Provider};
use dialog_common::Blake3Hash as NodeHash;
use dialog_common::ConditionalSync;
use dialog_effects::archive::{Get, Import, Put};
use dialog_effects::authority::{Attest, Identify};
use dialog_effects::blob::Write as BlobWrite;
use dialog_effects::blob::prelude::{ArchiveBlobExt as _, BlobExt as _};
use dialog_effects::blob::{Import as EnvelopeImport, Read as EnvelopeRead};
use dialog_effects::memory::{Publish, Resolve};
use dialog_ucan::{UcanCertificate, UcanDelegation};
use futures_util::stream;
use std::collections::HashSet;

/// Attribute naming who receives the delegated authority.
pub const DELEGATION_AUDIENCE: &str = "dialog.ucan/audience";
/// Attribute naming the subject the delegation applies to.
/// A [powerline](https://github.com/ucan-wg/delegation#powerline) delegation
/// (any subject) carries [`ANY_SUBJECT`] as its value, keeping the lookup a
/// plain equality.
pub const DELEGATION_SUBJECT: &str = "dialog.ucan/subject";
/// Attribute naming who issued (signed) the delegation.
pub const DELEGATION_ISSUER: &str = "dialog.ucan/issuer";
/// Attribute carrying the delegated command path (`/storage/get`).
pub const DELEGATION_COMMAND: &str = "dialog.ucan/command";
/// Attribute carrying the expiration as unix seconds, absent when the
/// delegation never expires.
pub const DELEGATION_EXPIRATION: &str = "dialog.ucan/expiration";
/// Attribute carrying the earliest validity as unix seconds, absent when
/// unbounded.
pub const DELEGATION_NOT_BEFORE: &str = "dialog.ucan/notBefore";

/// A branch's retained delegations: the target that delegation retain and
/// retract bind to. Obtain one with [`Branch::delegations`].
#[derive(Clone, Copy)]
pub struct Delegations<'a> {
    branch: &'a Branch,
}

impl Branch {
    /// This branch's retained delegations, the target for
    /// [`Delegations::retain`] and [`Delegations::retract`].
    pub fn delegations(&self) -> Delegations<'_> {
        Delegations { branch: self }
    }
}

impl<'a> Delegations<'a> {
    /// Retain a delegation chain: decompose every certificate into its
    /// `dialog.ucan/*` facts plus its envelope blob, in one commit.
    pub fn retain(self, chain: UcanDelegation) -> RetainDelegation<'a> {
        self.retain_all(vec![chain])
    }

    /// Retain many delegation chains in one commit — the bulk form of
    /// [`retain`](Delegations::retain), for imports.
    pub fn retain_all(self, chains: Vec<UcanDelegation>) -> RetainDelegation<'a> {
        RetainDelegation {
            branch: self.branch,
            chains,
        }
    }

    /// Retract a delegation chain: retract every certificate's facts and
    /// tombstone its blob reference, in one commit. The envelope bytes stay
    /// in the blob store.
    pub fn retract(self, chain: UcanDelegation) -> RetractDelegation<'a> {
        RetractDelegation {
            branch: self.branch,
            chain,
        }
    }

    /// Hydrate every retained delegation into local storage: the tree
    /// blocks its facts live in (a pulled tree adopts subtrees by link,
    /// leaving their blocks remote until read) and its envelope blob.
    /// The operator runs this when the branch head moves — the
    /// authorization walk deliberately reads only local state, so this
    /// is the step that brings that state local.
    pub fn hydrate(self) -> HydrateDelegations<'a> {
        HydrateDelegations {
            branch: self.branch,
        }
    }
}

/// Hydrate retained delegation envelopes from the branch's remote.
/// Created by [`Delegations::hydrate`].
pub struct HydrateDelegations<'a> {
    branch: &'a Branch,
}

impl HydrateDelegations<'_> {
    /// Fetch every retained delegation's fact blocks and envelope that
    /// are not yet local, returning how many delegations are locally
    /// provable afterward. The scan warms exactly what the walk reads:
    /// the audience index it discovers candidates through, each
    /// delegation entity's own facts, and the envelope bytes admission
    /// decodes. A delegation the remote cannot serve is skipped: the
    /// prover treats it as no candidate, and a later attempt can
    /// complete it.
    pub async fn perform<Env>(self, env: &Env) -> Result<usize, CommitError>
    where
        Env: Provider<Get>
            + Provider<Put>
            + Provider<Resolve>
            + Provider<EnvelopeRead>
            + Provider<EnvelopeImport>
            + Provider<Fork<RemoteSite, Get>>
            + Provider<Fork<RemoteSite, Resolve>>
            + Provider<Fork<RemoteSite, EnvelopeRead>>
            + ConditionalSync
            + 'static,
    {
        use dialog_artifacts::ArtifactSelector;
        use futures_util::StreamExt as _;

        let branch = self.branch;
        let store = index_store(branch, env).await;
        let facts = crate::Select::new(
            branch,
            ArtifactSelector::new().the(
                DELEGATION_AUDIENCE
                    .parse()
                    .expect("the audience attribute is valid"),
            ),
        )
        .execute(store.clone())
        .await
        .map_err(DialogArtifactsError::from)?;
        futures_util::pin_mut!(facts);

        let mut entities = Vec::new();
        {
            let mut seen = HashSet::new();
            while let Some(fact) = facts.next().await {
                let Ok(artifact) = fact.and_then(|view| view.to_owned()) else {
                    continue;
                };
                if seen.insert(artifact.of.to_string()) {
                    entities.push(artifact.of);
                }
            }
        }

        let mut hydrated = 0;
        for entity in entities {
            // Warm the entity's own fact blocks: the walk reads them
            // through the entity ordering, which the audience scan above
            // does not touch.
            let entity_facts =
                crate::Select::new(branch, ArtifactSelector::new().of(entity.clone()))
                    .execute(store.clone())
                    .await
                    .map_err(DialogArtifactsError::from)?;
            futures_util::pin_mut!(entity_facts);
            while entity_facts.next().await.is_some() {}

            // Local-first read with remote fallback: a hit caches the
            // bytes locally, a miss (remote cannot serve it either) is
            // skipped.
            let Ok(mut reader) = crate::Blob::from(entity)
                .read(branch.into())
                .perform(env)
                .await
            else {
                continue;
            };
            while let Ok(Some(_)) = reader.next().await {}
            hydrated += 1;
        }
        Ok(hydrated)
    }
}

/// One fact on the delegation's entity.
fn field(entity: &Entity, attribute: &str, value: Value) -> Result<Artifact, DialogArtifactsError> {
    Ok(Artifact {
        the: Attribute::try_from(attribute.to_string())?,
        of: entity.clone(),
        is: value,
        cause: None,
    })
}

/// The slim facts a certificate decomposes into, on `entity`.
///
/// Exactly the queried fields: policy, meta, and nonce stay inside the
/// envelope, which the entity's blob carries byte-identical.
fn field_artifacts(
    entity: &Entity,
    certificate: &UcanCertificate,
) -> Result<Vec<Artifact>, DialogArtifactsError> {
    let subject = match certificate.subject() {
        Some(did) => did.to_string(),
        None => ANY_SUBJECT.to_string(),
    };
    let mut artifacts = vec![
        field(
            entity,
            DELEGATION_AUDIENCE,
            Value::String(certificate.audience().to_string()),
        )?,
        field(entity, DELEGATION_SUBJECT, Value::String(subject))?,
        field(
            entity,
            DELEGATION_ISSUER,
            Value::String(certificate.issuer().to_string()),
        )?,
        field(
            entity,
            DELEGATION_COMMAND,
            Value::String(certificate.0.command().to_string()),
        )?,
    ];
    if let Some(expiration) = certificate.0.expiration() {
        artifacts.push(field(
            entity,
            DELEGATION_EXPIRATION,
            Value::UnsignedInt(expiration.to_unix() as u128),
        )?);
    }
    if let Some(not_before) = certificate.0.not_before() {
        artifacts.push(field(
            entity,
            DELEGATION_NOT_BEFORE,
            Value::UnsignedInt(not_before.to_unix() as u128),
        )?);
    }
    Ok(artifacts)
}

/// A certificate's encoded envelope, as [`CommitError`].
fn encode(certificate: &UcanCertificate) -> Result<Vec<u8>, CommitError> {
    certificate.encode().map_err(|error| {
        CommitError::Artifact(DialogArtifactsError::InvalidValue(format!(
            "failed to encode delegation envelope: {error}"
        )))
    })
}

/// Retain one or many delegation chains as one commit. Created by
/// [`Delegations::retain`] or [`Delegations::retain_all`].
pub struct RetainDelegation<'a> {
    branch: &'a Branch,
    chains: Vec<UcanDelegation>,
}

impl RetainDelegation<'_> {
    /// Execute the retain, returning the entity of every certificate this
    /// commit newly retained (empty when the whole chain was already
    /// retained: content-addressed entities make a re-retain a no-op that
    /// mints no revision).
    pub async fn perform<Env>(self, env: &Env) -> Result<Vec<Entity>, CommitError>
    where
        Env: Provider<Get>
            + Provider<Put>
            + Provider<Import>
            + Provider<Resolve>
            + Provider<Publish>
            + Provider<Identify>
            + Provider<Attest>
            + Provider<BlobWrite>
            + Provider<Fork<RemoteSite, Get>>
            + Provider<Fork<RemoteSite, Resolve>>
            + ConditionalSync
            + 'static,
    {
        let branch = self.branch;
        let store = index_store(branch, env).await;
        let tree = branch
            .revision()
            .map(|revision| Index::from_hash(NodeHash::from(*revision.tree.hash())));

        let mut instructions = Vec::new();
        let mut entries = Vec::new();
        let mut retained = Vec::new();
        let mut batched = HashSet::new();
        for certificate in self.chains.iter().flat_map(|chain| chain.certificates()) {
            let bytes = encode(&certificate)?;

            // The envelope bytes must be durable before the revision minted
            // below references them (the `WriteBlob` invariant). The sink is
            // also the hashing authority: the entity is derived from the
            // digest the blob store reports, never computed on the side.
            let mut sink = branch.archive().blob().write().perform(env).await?;
            sink.write_all(&bytes).await?;
            let hash = sink.finish().await?;
            let index_hash: dialog_storage::Blake3Hash = *hash.as_bytes();

            // Idempotence: a certificate the tree already references was
            // retained with its facts in one commit, so there is nothing to
            // add for it. Chains sharing a certificate (common: they share
            // a proof prefix) contribute it to this batch once.
            if !batched.insert(index_hash) {
                continue;
            }
            if let Some(tree) = &tree
                && tree.get_blob(&store, &index_hash).await?.is_some()
            {
                continue;
            }

            let entity = Entity::from_blob(&index_hash)?;
            for artifact in field_artifacts(&entity, &certificate)? {
                instructions.push(Instruction::Assert(artifact));
            }
            entries.push(BlobRecord::new(bytes.len() as u64).entry(&index_hash));
            retained.push(entity);
        }

        if retained.is_empty() {
            return Ok(retained);
        }

        Box::pin(
            branch
                .commit(stream::iter(instructions))
                .machinery()
                .with_entries(entries)
                .perform(env),
        )
        .await?;

        Ok(retained)
    }
}

/// Retract a delegation chain as one commit. Created by
/// [`Delegations::retract`].
pub struct RetractDelegation<'a> {
    branch: &'a Branch,
    chain: UcanDelegation,
}

impl RetractDelegation<'_> {
    /// Execute the retract, returning the entity of every certificate this
    /// commit retracted (empty when none were retained: retracting an
    /// unretained chain is a no-op that mints no revision).
    ///
    /// The envelope bytes are not touched — a replica that hydrated them
    /// keeps reading its local copy; reclaiming unreferenced bytes is a
    /// separate, local concern. (The bytes are in fact re-written through
    /// the blob sink here: the sink is the hashing authority the entity is
    /// derived from, and a content-addressed re-write of bytes this store
    /// holds — or is about to stop referencing — is idempotent.)
    pub async fn perform<Env>(self, env: &Env) -> Result<Vec<Entity>, CommitError>
    where
        Env: Provider<Get>
            + Provider<Put>
            + Provider<Import>
            + Provider<Resolve>
            + Provider<Publish>
            + Provider<Identify>
            + Provider<Attest>
            + Provider<BlobWrite>
            + Provider<Fork<RemoteSite, Get>>
            + Provider<Fork<RemoteSite, Resolve>>
            + ConditionalSync
            + 'static,
    {
        let branch = self.branch;
        let Some(revision) = branch.revision() else {
            return Ok(Vec::new());
        };
        let store = index_store(branch, env).await;
        let tree = Index::from_hash(NodeHash::from(*revision.tree.hash()));

        let mut instructions = Vec::new();
        let mut entries = Vec::new();
        let mut retracted = Vec::new();
        for certificate in self.chain.certificates() {
            let bytes = encode(&certificate)?;
            let mut sink = branch.archive().blob().write().perform(env).await?;
            sink.write_all(&bytes).await?;
            let hash = sink.finish().await?;
            let index_hash: dialog_storage::Blake3Hash = *hash.as_bytes();

            // A certificate the tree does not reference has nothing to
            // retract.
            if tree.get_blob(&store, &index_hash).await?.is_none() {
                continue;
            }

            let entity = Entity::from_blob(&index_hash)?;
            for artifact in field_artifacts(&entity, &certificate)? {
                instructions.push(Instruction::Retract(artifact));
            }
            entries.push(BlobRecord::retract_entry(&index_hash));
            retracted.push(entity);
        }

        if retracted.is_empty() {
            return Ok(retracted);
        }

        Box::pin(
            branch
                .commit(stream::iter(instructions))
                .machinery()
                .with_entries(entries)
                .perform(env),
        )
        .await?;

        Ok(retracted)
    }
}

#[cfg(test)]
mod tests {
    #[cfg(target_arch = "wasm32")]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    use super::*;
    use crate::{Blob, RepositoryExt as _};
    use anyhow::Result;
    use dialog_artifacts::ArtifactSelector;
    use dialog_capability::Subject;
    use dialog_credentials::Ed25519Signer;
    use dialog_effects::blob::BlobReader;
    use dialog_network::Network;
    use dialog_operator::{DeriveOperator as _, Operator, Profile};
    use dialog_storage::provider::storage::{Storage, VolatileSpace};
    use dialog_ucan_core::subject::Subject as UcanSubject;
    use dialog_ucan_core::{DelegationBuilder, DelegationChain};
    use dialog_varsig::Principal as _;
    use futures_util::StreamExt as _;

    use dialog_operator::helpers::unique_name;

    async fn delegate(
        issuer: &Ed25519Signer,
        audience: &Ed25519Signer,
        subject: UcanSubject,
    ) -> UcanDelegation {
        let delegation = DelegationBuilder::new()
            .issuer(issuer.clone())
            .audience(audience)
            .subject(subject)
            .command(vec!["storage".to_string()])
            .try_build()
            .await
            .unwrap();
        UcanDelegation::new(DelegationChain::new(delegation))
    }

    async fn open_branch(name: &str) -> Result<(crate::Branch, Operator<VolatileSpace>)> {
        let storage = Storage::volatile();
        let profile = Profile::open(unique_name(name)).perform(&storage).await?;
        let operator = profile
            .derive(b"test")
            .allow(Subject::any())
            .network(Network::default())
            .build(storage)
            .await?;
        let repo = profile
            .repository(unique_name("repo"))
            .open()
            .perform(&operator)
            .await?;
        let branch = repo.branch("main").open().perform(&operator).await?;
        Ok((branch, operator))
    }

    async fn drain(mut reader: BlobReader) -> Vec<u8> {
        let mut out = Vec::new();
        while let Some(chunk) = reader.next().await.unwrap() {
            out.extend(chunk);
        }
        out
    }

    #[dialog_common::test]
    async fn it_retains_a_delegation_as_facts_and_envelope() -> Result<()> {
        let (branch, operator) = open_branch("delegation-retain").await?;
        let space = Ed25519Signer::generate().await?;
        let holder = Ed25519Signer::generate().await?;
        let chain = delegate(&space, &holder, UcanSubject::Specific(space.did())).await;
        let certificate = chain.certificates().pop().unwrap();
        let envelope = certificate.encode().unwrap();

        let entities = branch
            .delegations()
            .retain(chain.clone())
            .perform(&operator)
            .await?;
        assert_eq!(entities.len(), 1);
        let entity = entities[0].clone();

        // The slim facts stand on the envelope's entity.
        let facts: Vec<_> = branch
            .claims()
            .select(ArtifactSelector::new().of(entity.clone()))
            .perform(&operator)
            .await?
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .map(|item| item.and_then(|view| view.to_owned()))
            .collect::<Result<Vec<_>, _>>()?;
        let get = |attribute: &str| {
            facts
                .iter()
                .find(|fact| fact.the.as_str() == attribute)
                .map(|fact| fact.is.clone())
        };
        assert_eq!(
            get(DELEGATION_AUDIENCE),
            Some(Value::String(holder.did().to_string()))
        );
        assert_eq!(
            get(DELEGATION_SUBJECT),
            Some(Value::String(space.did().to_string()))
        );
        assert_eq!(
            get(DELEGATION_ISSUER),
            Some(Value::String(space.did().to_string()))
        );
        assert_eq!(
            get(DELEGATION_COMMAND),
            Some(Value::String("/storage".to_string()))
        );
        assert_eq!(get(DELEGATION_NOT_BEFORE), None);

        // The envelope reads back byte-identical and decodes to the same
        // certificate.
        let reader = Blob::from(entity.clone())
            .read((&branch).into())
            .perform(&operator)
            .await?;
        let bytes = drain(reader).await;
        assert_eq!(bytes, envelope);
        let decoded = UcanCertificate::decode(&bytes).unwrap();
        assert_eq!(decoded.issuer(), certificate.issuer());
        assert_eq!(decoded.audience(), certificate.audience());
        assert_eq!(decoded.subject(), certificate.subject());

        Ok(())
    }

    #[dialog_common::test]
    async fn it_records_a_powerline_subject_as_the_wildcard_did() -> Result<()> {
        let (branch, operator) = open_branch("delegation-powerline").await?;
        let space = Ed25519Signer::generate().await?;
        let holder = Ed25519Signer::generate().await?;
        let chain = delegate(&space, &holder, UcanSubject::Any).await;

        let entities = branch
            .delegations()
            .retain(chain)
            .perform(&operator)
            .await?;
        let facts: Vec<_> = branch
            .claims()
            .select(ArtifactSelector::new().of(entities[0].clone()))
            .perform(&operator)
            .await?
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .map(|item| item.and_then(|view| view.to_owned()))
            .collect::<Result<Vec<_>, _>>()?;
        let subject = facts
            .iter()
            .find(|fact| fact.the.as_str() == DELEGATION_SUBJECT)
            .unwrap();
        assert_eq!(subject.is, Value::String(ANY_SUBJECT.to_string()));
        Ok(())
    }

    #[dialog_common::test]
    async fn it_retains_idempotently() -> Result<()> {
        let (branch, operator) = open_branch("delegation-idempotent").await?;
        let space = Ed25519Signer::generate().await?;
        let holder = Ed25519Signer::generate().await?;
        let chain = delegate(&space, &holder, UcanSubject::Specific(space.did())).await;

        let first = branch
            .delegations()
            .retain(chain.clone())
            .perform(&operator)
            .await?;
        assert_eq!(first.len(), 1);
        let head = branch.revision();

        let second = branch
            .delegations()
            .retain(chain)
            .perform(&operator)
            .await?;
        assert!(second.is_empty(), "a re-retain retains nothing new");
        assert_eq!(
            branch.revision().map(|revision| revision.version()),
            head.map(|revision| revision.version()),
            "a no-op retain mints no revision"
        );
        Ok(())
    }

    #[dialog_common::test]
    async fn it_retracts_facts_and_reference_but_not_bytes() -> Result<()> {
        let (branch, operator) = open_branch("delegation-retract").await?;
        let space = Ed25519Signer::generate().await?;
        let holder = Ed25519Signer::generate().await?;
        let chain = delegate(&space, &holder, UcanSubject::Specific(space.did())).await;

        let entities = branch
            .delegations()
            .retain(chain.clone())
            .perform(&operator)
            .await?;
        let entity = entities[0].clone();

        let retracted = branch
            .delegations()
            .retract(chain.clone())
            .perform(&operator)
            .await?;
        assert_eq!(retracted, entities);

        // Facts gone, index reference gone...
        let facts: Vec<_> = branch
            .claims()
            .select(ArtifactSelector::new().of(entity.clone()))
            .perform(&operator)
            .await?
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .map(|item| item.and_then(|view| view.to_owned()))
            .collect::<Result<Vec<_>, _>>()?;
        assert!(facts.is_empty(), "retract removes the facts: {facts:?}");
        assert_eq!(
            Blob::from(entity.clone())
                .size((&branch).into())
                .perform(&operator)
                .await?,
            None
        );

        // ...bytes untouched.
        let reader = Blob::from(entity)
            .read((&branch).into())
            .perform(&operator)
            .await?;
        assert!(!drain(reader).await.is_empty());

        // Retracting again is a no-op that mints no revision.
        let head = branch.revision();
        let again = branch
            .delegations()
            .retract(chain)
            .perform(&operator)
            .await?;
        assert!(again.is_empty());
        assert_eq!(
            branch.revision().map(|revision| revision.version()),
            head.map(|revision| revision.version()),
        );
        Ok(())
    }

    #[dialog_common::test]
    async fn it_keeps_the_namespace_reserved_for_applications() -> Result<()> {
        let (branch, operator) = open_branch("delegation-reserved").await?;

        let result = branch
            .commit(stream::iter(vec![Instruction::Assert(Artifact {
                the: DELEGATION_AUDIENCE.parse()?,
                of: "user:mallory".parse()?,
                is: Value::String("did:key:zForged".to_string()),
                cause: None,
            })]))
            .perform(&operator)
            .await;

        assert!(
            matches!(
                result,
                Err(CommitError::Artifact(
                    DialogArtifactsError::ReservedAttribute(_)
                ))
            ),
            "application commits must not write dialog.ucan facts"
        );
        Ok(())
    }
}

//! Re-installing rules an earlier release stored, under the identity
//! this release gives them.
//!
//! The release before canonical rule identities stored a rule under
//! the hash of its encoded bytes. This release reads a rule only under
//! its canonical identity (see [`DeductiveRule::stored_as`]), so a rule
//! stored the older way is inert: no read finds it and no commit fires
//! it. There is no permanent compatibility path, which no read would
//! otherwise stop paying for.
//!
//! [`Branch::upgrade_rules`] is the migration. It reads every rule body
//! the branch holds, and each one stored under an entity that is not
//! its identity is retracted from that entity and asserted again, which
//! writes it under its identity with every index a current install
//! writes.
//!
//! The earlier release also gave every attribute and concept another
//! identity (see [`dialog_query::migration`]), and keyed a concept's
//! `dialog.concept/transient` marker by it. For the concept every rule
//! on the branch concludes, a marker under the concept's earlier
//! identity is moved to its current one.
//!
//! The upgrade is idempotent and convergent: a second run finds
//! nothing, and two replicas that upgrade concurrently write the same
//! facts, since every identity is a function of what it identifies.
//!
//! A replica still on the earlier release can write such rules again,
//! and sync brings them in. They stay inert until the upgrade runs
//! again; when to run it, once after a full replication or on every
//! pull, is the embedder's decision.
//!
//! [`DeductiveRule::stored_as`]: dialog_query::rule::DeductiveRule::stored_as

use crate::rules::transient_attr;
use crate::{Branch, CommitError, RemoteSite, Revision, Transient};
use dialog_artifacts::{ArtifactSelector, Entity, Pick, Relation as ArtifactsRelation, Value};
use dialog_capability::{Fork, Provider};
use dialog_common::ConditionalSync;
use dialog_effects::archive::{Get, Import, Put};
use dialog_effects::authority::{Attest, Identify};
use dialog_effects::blob::{Import as BlobImport, Read as BlobRead, Size as BlobSize};
use dialog_effects::memory::{Publish, Resolve};
use dialog_query::attribute::Relation;
use dialog_query::migration::concept_identity_v0;
use dialog_query::rule::statement::source_attr;
use dialog_query::rule::{DeductiveRule, InductiveRule};
use dialog_query::{AttributeStatement, Cardinality, ConceptDescriptor};
use futures_util::TryStreamExt as _;

/// What [`Branch::upgrade_rules`] did.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RulesUpgraded {
    /// Each rule re-installed: the entity it was stored under, and its
    /// identity now.
    pub reinstalled: Vec<(Entity, Entity)>,
    /// Rule entities whose stored body this release cannot decode as
    /// either kind of rule. They are left as they are.
    pub undecodable: Vec<Entity>,
    /// Each transient marker moved: the concept identity the earlier
    /// release keyed it by, and the concept's identity now.
    pub remarked: Vec<(Entity, Entity)>,
    /// The revision the upgrade committed, or `None` when there was
    /// nothing to re-install.
    pub revision: Option<Revision>,
}

impl Branch {
    /// Re-install every rule this branch stores under an entity that is
    /// not its identity, as an earlier release stored rules. See the
    /// [module documentation](self) for when to run it.
    pub fn upgrade_rules(&self) -> UpgradeRules<'_> {
        UpgradeRules { branch: self }
    }
}

/// Command created by [`Branch::upgrade_rules`].
pub struct UpgradeRules<'a> {
    branch: &'a Branch,
}

/// A rule body decoded from what a branch stores.
enum Decoded {
    Deductive(DeductiveRule),
    Inductive(InductiveRule),
}

impl Decoded {
    fn decode(bytes: &[u8]) -> Option<Self> {
        if let Ok(rule) = DeductiveRule::decode(bytes) {
            return Some(Decoded::Deductive(rule));
        }
        InductiveRule::decode(bytes).ok().map(Decoded::Inductive)
    }

    fn identity(&self) -> Option<Entity> {
        match self {
            Decoded::Deductive(rule) => rule.try_this(),
            Decoded::Inductive(rule) => rule.try_this(),
        }
    }

    fn conclusion(&self) -> &ConceptDescriptor {
        match self {
            Decoded::Deductive(rule) => rule.conclusion(),
            Decoded::Inductive(rule) => rule.conclusion(),
        }
    }
}

impl UpgradeRules<'_> {
    /// Read the branch's rule bodies, re-install those stored under
    /// another entity than their identity in one commit, and publish it.
    pub async fn perform<Env>(self, env: &Env) -> Result<RulesUpgraded, CommitError>
    where
        Env: Provider<BlobSize>
            + Provider<BlobImport>
            + Provider<Get>
            + Provider<BlobRead>
            + Provider<Put>
            + Provider<Import>
            + Provider<Resolve>
            + Provider<Publish>
            + Provider<Identify>
            + Provider<Attest>
            + Provider<crate::Hydrate>
            + Provider<dialog_artifacts::Preload>
            + Provider<dialog_artifacts::Speculation>
            + Provider<Fork<RemoteSite, Resolve>>
            + ConditionalSync
            + 'static,
    {
        let branch = self.branch;
        let sources: Vec<_> = branch
            .claims()
            .select(ArtifactSelector::new().the(source_attr()))
            .perform(env)
            .await?
            .try_collect::<Vec<_>>()
            .await?;

        let mut upgraded = RulesUpgraded::default();
        let mut transaction = branch.transaction();
        let mut conclusions: Vec<ConceptDescriptor> = Vec::new();
        for view in sources {
            let stored = view.to_owned()?;
            let Value::Bytes(bytes) = &stored.is else {
                continue;
            };
            let Some(rule) = Decoded::decode(bytes) else {
                upgraded.undecodable.push(stored.of.clone());
                continue;
            };
            let Some(identity) = rule.identity() else {
                continue;
            };
            if !conclusions
                .iter()
                .any(|known| known.this() == rule.conclusion().this())
            {
                conclusions.push(rule.conclusion().clone());
            }
            if identity == stored.of
                || upgraded
                    .reinstalled
                    .iter()
                    .any(|(from, _)| *from == stored.of)
            {
                continue;
            }
            // Every rule fact the earlier release wrote under the
            // entity, withdrawn.
            let facts: Vec<_> = branch
                .claims()
                .select(ArtifactSelector::new().of(stored.of.clone()))
                .perform(env)
                .await?
                .try_collect::<Vec<_>>()
                .await?;
            for fact in facts {
                let fact = fact.to_owned()?;
                if !fact.the.as_str().starts_with("dialog.rule/") {
                    continue;
                }
                transaction = transaction.retract(statement(fact.the, fact.of, fact.is));
            }
            transaction = match &rule {
                Decoded::Deductive(rule) => transaction.assert(rule),
                Decoded::Inductive(rule) => transaction.assert(rule),
            };
            upgraded.reinstalled.push((stored.of.clone(), identity));
        }
        // A transient marker under a concluded concept's earlier
        // identity, moved to its current one.
        for concept in conclusions {
            let earlier = concept_identity_v0(&concept);
            let current = concept.this();
            if earlier == current {
                continue;
            }
            let marked = branch
                .claims()
                .select(
                    ArtifactSelector::new()
                        .the(transient_attr())
                        .of(earlier.clone()),
                )
                .perform(env)
                .await?
                .try_collect::<Vec<_>>()
                .await?;
            if marked.is_empty() {
                continue;
            }
            transaction = transaction
                .retract(Transient(earlier.clone()))
                .assert(Transient(current.clone()));
            upgraded.remarked.push((earlier, current));
        }
        if upgraded.reinstalled.is_empty() && upgraded.remarked.is_empty() {
            return Ok(upgraded);
        }
        upgraded.revision = Some(transaction.commit().publish().perform(env).await?);
        Ok(upgraded)
    }
}

/// A rule fact as a statement, under `all`, the pick rule facts are
/// written under.
fn statement(the: ArtifactsRelation, of: Entity, is: Value) -> AttributeStatement {
    AttributeStatement {
        the: Relation::from(the),
        of,
        is,
        cause: None,
        cardinality: Some(Cardinality::Many),
        pick: Some(Pick::All),
    }
}

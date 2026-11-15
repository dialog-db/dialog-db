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
//! It also rewrites every body still in an earlier format, under its
//! identity or not, into the format this release writes (see
//! [`dialog_query::rule::body`]): the stored body is retracted by its
//! bytes and the rule asserted again. After one run every body is in
//! the current format, which holds the rule's canonical spelling, so a
//! later retraction finds the body by encoding the rule.
//!
//! The earlier releases also gave every attribute and concept other
//! identities (see [`dialog_query::migration`]), and keyed a concept's
//! `dialog.concept/transient` marker by them. For the concept every rule
//! on the branch concludes, a marker under either of the concept's
//! earlier identities is moved to its current one.
//!
//! The release before this one (0.3 and 0.4) read every attribute under
//! a pick, and stored its rules spelling one. This release refuses an
//! attribute spelled with a pick, so such a body does not decode and its
//! rule is inert. The upgrade rewrites it with
//! [`dialog_query::migration::rewrite_body`]: an attribute read under
//! `last` is cardinality one and one read under `all` cardinality many,
//! which is what each pick read and wrote. The rewritten rule is
//! asserted under its identity now, and every rule fact the earlier
//! release wrote is retracted. A rule reading an attribute under `max`,
//! `min` or `top` (which a listed `as` without a pick meant), or over a
//! chain of relations, has no rewriting with the same meaning: a ranked
//! read is now a rule folding with that aggregator, which the author
//! writes. Such a rule is left as it is, inert, and reported in
//! [`RulesUpgraded::unmapped`] with why.
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
use dialog_capability::{Fork, Provider};
use dialog_common::{ConditionalSync, Holds};
use dialog_corpus::tree::CorpusNode;
use dialog_corpus::{Entity, Relation as CorpusRelation, Selector, Value};
use dialog_effects::archive::Load as ArchiveLoad;
use dialog_effects::archive::{Get, Import, Put};
use dialog_effects::authority::{Attest, Identify};
use dialog_effects::blob::Load as BlobLoad;
use dialog_effects::blob::{Import as BlobImport, Read as BlobRead, Size as BlobSize};
use dialog_effects::memory::{Publish, Resolve};
use dialog_query::attribute::Relation;
use dialog_query::migration::{
    Rewritten, Unmapped, concept_identity_v0, concept_identity_v1, rewrite_body,
};
use dialog_query::rule::body::{FORMAT, format_of};
use dialog_query::rule::statement::source_attr;
use dialog_query::rule::{DeductiveRule, InductiveRule};
use dialog_query::{AttributeStatement, Cardinality, ConceptDescriptor};
use dialog_search_tree::nodes;
use futures_util::TryStreamExt as _;

/// What [`Branch::upgrade_rules`] did.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RulesUpgraded {
    /// Each rule re-installed: the entity it was stored under, and its
    /// identity now.
    pub reinstalled: Vec<(Entity, Entity)>,
    /// Each rule already under its identity whose body was in an
    /// earlier format and is now rewritten in the current one.
    pub rewritten: Vec<Entity>,
    /// Rule entities whose stored body this release cannot decode as
    /// either kind of rule, in its own format or rewritten from an
    /// earlier one. They are left as they are.
    pub undecodable: Vec<Entity>,
    /// Rule entities an earlier release stored whose body reads some
    /// attribute in a way no cardinality reproduces: under `max`, `min`
    /// or `top`, or over a chain of relations. Each is left as it is,
    /// inert, with what keeps it from being rewritten. A ranked read is
    /// now a rule folding with that aggregator, for the author to write.
    pub unmapped: Vec<(Entity, Vec<Unmapped>)>,
    /// Each transient marker moved: the concept identity the earlier
    /// release keyed it by, and the concept's identity now.
    pub remarked: Vec<(Entity, Entity)>,
    /// The revision the upgrade committed, or `None` when there was
    /// nothing to re-install.
    pub revision: Option<Revision>,
}

impl Branch {
    /// Re-install every rule this branch stores under an entity that is
    /// not its identity, as an earlier release stored rules, and rewrite
    /// every body stored in an earlier format, the bodies spelling picks
    /// among them. See the
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

/// What reading a stored rule body came to.
enum Read {
    /// The rule, decoded as stored or rewritten from an earlier format.
    Rule(Decoded),
    /// A body an earlier release stored that no rewriting reads with
    /// its meaning.
    Unmapped(Vec<Unmapped>),
    /// A body that decodes as no rule.
    Undecodable,
}

impl Decoded {
    fn decode(bytes: &[u8]) -> Option<Self> {
        if let Ok(rule) = DeductiveRule::decode(bytes) {
            return Some(Decoded::Deductive(rule));
        }
        InductiveRule::decode(bytes).ok().map(Decoded::Inductive)
    }

    /// The rule `bytes` hold. A body in an earlier format is rewritten
    /// first ([`rewrite_body`]): its attributes may spell the picks 0.3
    /// and 0.4 wrote, or list values without one, which those releases
    /// read under `top` and this one reads as an enumerated type, so such
    /// a body is never decoded as it stands. A body in the current
    /// format is decoded as it is.
    fn read(bytes: &[u8]) -> Read {
        if format_of(bytes) != Some(FORMAT) {
            match rewrite_body(bytes) {
                Ok(Rewritten::Rewritten(rewritten)) => {
                    return match Decoded::decode(&rewritten) {
                        Some(rule) => Read::Rule(rule),
                        None => Read::Undecodable,
                    };
                }
                Ok(Rewritten::Unmapped(unmapped)) => return Read::Unmapped(unmapped),
                Ok(Rewritten::Unchanged) | Err(_) => {}
            }
        }
        match Decoded::decode(bytes) {
            Some(rule) => Read::Rule(rule),
            None => Read::Undecodable,
        }
    }

    fn identity(&self) -> Option<Entity> {
        match self {
            Decoded::Deductive(rule) => rule.try_this(),
            Decoded::Inductive(rule) => rule.try_this(),
        }
    }

    fn encode(&self) -> Option<Vec<u8>> {
        match self {
            Decoded::Deductive(rule) => rule.try_encode(),
            Decoded::Inductive(rule) => rule.try_encode(),
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
    /// another entity than their identity and rewrite those in an
    /// earlier format, in one commit, and publish it.
    pub async fn perform<Env>(self, env: &Env) -> Result<RulesUpgraded, CommitError>
    where
        Env: Provider<BlobSize>
            + Provider<BlobImport>
            + Provider<Get>
            + Provider<BlobRead>
            + Provider<Put>
            + Provider<Import>
            + Provider<nodes::Write<CorpusNode>>
            + ConditionalSync
            + Provider<Resolve>
            + Provider<Publish>
            + Provider<Identify>
            + Provider<Attest>
            + Provider<ArchiveLoad>
            + Provider<nodes::Write<CorpusNode>>
            + ConditionalSync
            + Provider<nodes::Read<CorpusNode>>
            + Provider<nodes::Preload<CorpusNode>>
            + ConditionalSync
            + Provider<BlobLoad>
            + Provider<dialog_corpus::Preload>
            + Provider<dialog_corpus::Speculation>
            + Provider<Fork<RemoteSite, Resolve>>
            + Holds
            + ConditionalSync
            + 'static,
    {
        let branch = self.branch;
        let sources: Vec<_> = branch
            .claims()
            .select(Selector::new().the(source_attr()))
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
            let rule = match Decoded::read(bytes) {
                Read::Rule(rule) => rule,
                Read::Unmapped(unmapped) => {
                    if !upgraded.unmapped.iter().any(|(of, _)| *of == stored.of) {
                        upgraded.unmapped.push((stored.of.clone(), unmapped));
                    }
                    continue;
                }
                Read::Undecodable => {
                    upgraded.undecodable.push(stored.of.clone());
                    continue;
                }
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
            if identity == stored.of {
                // Under its identity: rewritten if its body is in an
                // earlier format, which is any body other than the one
                // this release writes.
                if rule.encode().as_deref() == Some(bytes.as_slice()) {
                    continue;
                }
                transaction = transaction.retract(statement(
                    source_attr(),
                    stored.of.clone(),
                    stored.is.clone(),
                ));
                transaction = match &rule {
                    Decoded::Deductive(rule) => transaction.assert(rule),
                    Decoded::Inductive(rule) => transaction.assert(rule),
                };
                if !upgraded.rewritten.contains(&stored.of) {
                    upgraded.rewritten.push(stored.of.clone());
                }
                continue;
            }
            if upgraded
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
                .select(Selector::new().of(stored.of.clone()))
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
        // identities, moved to its current one.
        for concept in conclusions {
            let current = concept.this();
            for earlier in [concept_identity_v0(&concept), concept_identity_v1(&concept)] {
                if earlier == current {
                    continue;
                }
                let marked = branch
                    .claims()
                    .select(Selector::new().the(transient_attr()).of(earlier.clone()))
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
                upgraded.remarked.push((earlier, current.clone()));
            }
        }
        if upgraded.reinstalled.is_empty()
            && upgraded.rewritten.is_empty()
            && upgraded.remarked.is_empty()
        {
            return Ok(upgraded);
        }
        upgraded.revision = Some(transaction.commit().publish().perform(env).await?);
        Ok(upgraded)
    }
}

/// A rule fact as a statement, under cardinality many, the cardinality
/// rule facts are written under.
fn statement(the: CorpusRelation, of: Entity, is: Value) -> AttributeStatement {
    AttributeStatement {
        the: Relation::from(the),
        of,
        is,
        cause: None,
        cardinality: Some(Cardinality::Many),
    }
}

//! Rules as [`Statement`]s: the `dialog.rule/*` storage vocabulary and
//! the lowering that installs a rule by plain assertion.
//!
//! A rule *is* its facts: asserting a [`StorableRule`], a
//! [`DeductiveRule`] or an [`InductiveRule`] checked to have a stored
//! form, into any [`Update`] target stages the
//! `dialog.rule/*` claims that persist it, and retracting the same rule
//! erases them — install and uninstall are ordinary writes, no dedicated
//! API. The kind decides the fact shape:
//!
//! - deductive: the `derives` head index resolution finds the rule by,
//!   one entry per attribute its conclusion carries, the `conclusion`
//!   concept it was written against, the `source` body, and the `reads`
//!   reverse index over the body's attributes;
//! - inductive: the shared `source` body, the `induces` head index,
//!   and the `on` trigger index commit-time dispatch probes by touched
//!   attribute.
//!
//! The rule entity is the content address of its canonical body, which
//! is what makes these facts safe to accept from the ordinary write
//! path: a reader verifies the decoded body against the entity it was
//! stored under and treats mismatched entries as inert.

use std::collections::BTreeSet;

use crate::attribute::The;
use crate::corpus::{Entity, Value};
use crate::error::TypeError;
use crate::rule::{DeductiveRule, InductiveRule, Rule};
use crate::{Proposition, Statement, Update, the};
use dialog_corpus::{NameShape, Relation as CorpusRelation, Symbol};

/// The `dialog.rule/source` body attribute, validated at compile time.
/// Shared by both rule kinds — hydration dispatches on the decoded
/// descriptor's head field.
pub fn source_attr() -> CorpusRelation {
    the!("dialog.rule/source").into()
}

/// The `dialog.rule/conclusion` index attribute, validated at compile time.
pub fn conclusion_attr() -> CorpusRelation {
    the!("dialog.rule/conclusion").into()
}

/// The `dialog.rule/induces` index attribute — the inductive sibling of
/// `dialog.rule/conclusion`, kept separate so deductive resolution never
/// hydrates (and discards) inductive rules concluding a queried
/// concept.
pub fn induces_attr() -> CorpusRelation {
    the!("dialog.rule/induces").into()
}

/// The `dialog.rule/on` trigger-index attribute: one claim per attribute an
/// inductive rule's concept premises name, valued `on:<domain>/<name>`.
/// This is the index commit-time dispatch probes by touched attribute.
pub fn on_attr() -> CorpusRelation {
    the!("dialog.rule/on").into()
}

/// The `dialog.rule/reads` reverse index for *deductive* rules: one claim
/// per attribute the rule's body names, valued `on:<domain>/<name>`.
/// Commit-time dispatch composes these at probe time to close the
/// trigger footprint over derivation — a base-fact write reaches
/// inductive rules premised on the concepts it (transitively)
/// supports. Per-rule and derived from the rule's own immutable body,
/// so an entry is never stale; the closure itself is never stored.
pub fn reads_attr() -> CorpusRelation {
    the!("dialog.rule/reads").into()
}

/// The `dialog.rule/derives` head index for *deductive* rules: one claim
/// per attribute the rule's conclusion carries, valued
/// `on:<domain>/<name>`. Resolution probes it per attribute, so a rule
/// is found by every concept selecting any attribute it derives, not
/// only by the concept it was written against.
pub fn derives_attr() -> CorpusRelation {
    the!("dialog.rule/derives").into()
}

/// The `dialog.rule/quarantined` attribute: a rule the program analysis
/// sets aside, valued with the concept whose cycle it closed. Never
/// stored: a branch answers it from the rules its layers hold, so it
/// moves as rules are installed and retracted.
pub fn quarantined_attr() -> CorpusRelation {
    the!("dialog.rule/quarantined").into()
}

/// The `dialog.rule/invalid` attribute: a stored rule body a branch does
/// not read, valued with why. A rule in a format older than this
/// release reads is waiting for `Branch::upgrade_rules`; one in a newer
/// format, for a newer release; one that does not decode, or is stored
/// under an identity it does not hash to, is not a rule. None of them
/// is evaluated, and no query fails over one. Never stored: a branch
/// answers it from the rule bodies its layers hold.
pub fn invalid_attr() -> CorpusRelation {
    the!("dialog.rule/invalid").into()
}

/// The `on:<domain>/<name>` trigger-index entity for an attribute.
/// Derivable from a runtime instruction alone — no schema lookup —
/// which is what keeps dispatch probing cheap.
pub fn on_entity(attribute: &CorpusRelation) -> Option<Entity> {
    format!("on:{attribute}").parse().ok()
}

/// What a rule reaches through one concept field: a single attribute,
/// or one keyed half of a domain. Induction tracks a rule's reach in
/// these units — the attributes a stimulus touches, the attributes a
/// premise reads, the attributes a head writes — and a fact is inside
/// a reach when the reach [`covers`](Reach::covers) its attribute.
///
/// The trigger index stores one `on:` entity per reach: an attribute's
/// own `on:<domain>/<name>`, and for a domain half a cover key,
/// `on:<domain>/_position` or `on:<domain>/_symbol`, spelled with a
/// leading underscore so it can never collide with a stored name
/// (symbols begin with a letter, positions with an uppercase one). A
/// touched attribute probes both its own key and its half's cover
/// key, so a rule reading a collection wakes for any member write.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Reach {
    /// One relation.
    Relation(CorpusRelation),
    /// Every attribute of `domain` whose name has `shape`.
    Domain {
        /// The domain the attributes share.
        domain: Symbol,
        /// Which half of the domain.
        shape: NameShape,
    },
}

impl Reach {
    /// The reach of a concept field.
    pub fn of(relation: &The) -> Reach {
        match relation {
            The::Relation(the) => Reach::Relation(the.into()),
            The::Collection { domain, keyed } => Reach::Domain {
                domain: domain.clone(),
                shape: NameShape::from(*keyed),
            },
        }
    }

    /// The cover key for one half of a domain.
    fn cover_entity(domain: &str, shape: NameShape) -> Option<Entity> {
        let half = match shape {
            NameShape::Position => "_position",
            NameShape::Symbol => "_symbol",
        };
        format!("on:{domain}/{half}").parse().ok()
    }

    /// Whether `attribute` is inside this reach.
    pub fn covers(&self, attribute: &CorpusRelation) -> bool {
        match self {
            Reach::Relation(the) => the == attribute,
            Reach::Domain { domain, shape } => attribute
                .split()
                .is_ok_and(|(of, name)| &of == domain && name.shape() == *shape),
        }
    }

    /// Whether the two reaches share an attribute.
    pub fn overlaps(&self, other: &Reach) -> bool {
        match (self, other) {
            (Reach::Relation(the), other) | (other, Reach::Relation(the)) => other.covers(the),
            (Reach::Domain { .. }, Reach::Domain { .. }) => self == other,
        }
    }

    /// The trigger-index entity a rule is filed under for this reach.
    pub fn on_entity(&self) -> Option<Entity> {
        match self {
            Reach::Relation(the) => on_entity(the),
            Reach::Domain { domain, shape } => Self::cover_entity(domain.as_str(), *shape),
        }
    }

    /// The trigger-index entities a touched reach probes: an attribute
    /// probes its own key and its half's cover key, so both a rule
    /// reading that attribute and a rule reading the collection it
    /// belongs to are found; a domain half probes its cover key.
    pub fn probes(&self) -> Vec<Entity> {
        match self {
            Reach::Relation(the) => {
                let mut probes = Vec::new();
                probes.extend(on_entity(the));
                if let Ok((domain, name)) = the.split() {
                    probes.extend(Self::cover_entity(domain.as_str(), name.shape()));
                }
                probes
            }
            Reach::Domain { .. } => self.on_entity().into_iter().collect(),
        }
    }
}

/// The `on:` entities for the attributes a set of propositions' concept
/// premises name. Formula and constraint premises contribute nothing;
/// attribute-query premises can't occur in stored rules (they have no
/// formal-notation encoding).
fn premise_trigger_entities<'p>(
    propositions: impl Iterator<Item = &'p Proposition>,
) -> BTreeSet<Entity> {
    let mut entities = BTreeSet::new();
    for proposition in propositions {
        if let Proposition::Concept(query) = proposition {
            for (_, field) in query.predicate.with().iter() {
                if let Some(entity) = Reach::of(field.the()).on_entity() {
                    entities.insert(entity);
                }
            }
        }
    }
    entities
}

/// The trigger-index entities for an inductive rule: one per attribute
/// named by any concept premise, `when` and `unless` alike. `unless`
/// premises are indexed because a *retraction* can newly enable a rule
/// (the guard it failed on clears).
pub fn on_entities(rule: &InductiveRule) -> BTreeSet<Entity> {
    let descriptor = rule.descriptor();
    premise_trigger_entities(descriptor.when.iter().chain(descriptor.unless.iter()))
}

/// The reverse-index entities for a deductive rule's body: one per
/// attribute any concept premise names. Stored as `dialog.rule/reads` so
/// dispatch can walk base attribute → deductive rules reading it →
/// their conclusions, closing the trigger footprint over derivation.
pub fn reads_entities(rule: &DeductiveRule) -> BTreeSet<Entity> {
    let descriptor = rule.descriptor();
    premise_trigger_entities(descriptor.when.iter().chain(descriptor.unless.iter()))
}

/// The head-index entities for a deductive rule: one per attribute of
/// its conclusion. Stored as `dialog.rule/derives` so resolution can
/// find, for one attribute, every rule deriving it, whatever concept
/// the rule was written against.
pub fn derives_entities(rule: &DeductiveRule) -> BTreeSet<Entity> {
    head_entities(rule.conclusion())
}

/// The head-index entities of a concept: the reach entity of each of
/// its attributes, as [`derives_entities`] records them for a rule
/// concluding it.
pub fn head_entities(concept: &crate::ConceptDescriptor) -> BTreeSet<Entity> {
    concept
        .with()
        .iter()
        .filter_map(|(_, field)| Reach::of(field.the()).on_entity())
        .collect()
}

/// A rule that can be stored: a [`DeductiveRule`] or an
/// [`InductiveRule`] together with the identity and the body it is
/// stored under, computed when it was checked. Only a storable rule is
/// a [`Statement`], so a rule with no stored form (a concept's implicit
/// rule, or one that reads an attribute by a raw scan) cannot be
/// asserted or retracted.
///
/// Asserting a deductive rule installs it as `dialog.rule/*` facts:
/// the `derives` head index over the conclusion's attributes, which
/// resolution looks the rule up by, the `conclusion` concept it was
/// written against, the `source` body, and the `reads` reverse index
/// over the body's attributes that lets commit-time dispatch close its
/// trigger footprint over derivation. Asserting an inductive rule
/// installs the `source` body (shared attribute with deductive rules:
/// hydration dispatches on the head field), the `induces` head index,
/// and the `on` trigger index dispatch probes by touched attribute.
/// Retracting the same rule erases those facts, uninstalling it. A rule
/// that holds a pattern is stored as any other, its body in format 5
/// (see [`body`](crate::rule::body)).
///
/// ```no_run
/// # use dialog_query::{Changes, DeductiveRule, StorableRule};
/// # fn example(rule: DeductiveRule) -> Result<(), dialog_query::error::TypeError> {
/// let mut changes = Changes::new();
/// changes.assert(StorableRule::try_from(&rule)?);
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone, PartialEq)]
pub struct StorableRule {
    rule: Rule,
    this: Entity,
    body: Vec<u8>,
}

impl StorableRule {
    /// The rule's content-addressed identity, the entity its facts are
    /// stored under.
    pub fn this(&self) -> &Entity {
        &self.this
    }

    /// The rule's stored body.
    pub fn body(&self) -> &[u8] {
        &self.body
    }

    /// The rule.
    pub fn rule(&self) -> &Rule {
        &self.rule
    }

    fn checked(rule: Rule, this: Option<Entity>, body: Option<Vec<u8>>) -> Result<Self, TypeError> {
        match (this, body) {
            (Some(this), Some(body)) => Ok(StorableRule { rule, this, body }),
            _ => Err(TypeError::RuleWithoutIdentity {
                concept: rule.conclusion().this().to_string(),
            }),
        }
    }
}

impl TryFrom<Rule> for StorableRule {
    type Error = TypeError;

    /// Fails with [`TypeError::RuleWithoutIdentity`] when the rule has
    /// no stored form.
    fn try_from(rule: Rule) -> Result<Self, Self::Error> {
        let (this, body) = match &rule {
            Rule::Deductive(rule) => (rule.try_this(), rule.try_encode()),
            Rule::Inductive(rule) => (rule.try_this(), rule.try_encode()),
        };
        Self::checked(rule, this, body)
    }
}

impl TryFrom<&Rule> for StorableRule {
    type Error = TypeError;

    fn try_from(rule: &Rule) -> Result<Self, Self::Error> {
        StorableRule::try_from(rule.clone())
    }
}

impl TryFrom<DeductiveRule> for StorableRule {
    type Error = TypeError;

    fn try_from(rule: DeductiveRule) -> Result<Self, Self::Error> {
        StorableRule::try_from(Rule::Deductive(rule))
    }
}

impl TryFrom<&DeductiveRule> for StorableRule {
    type Error = TypeError;

    fn try_from(rule: &DeductiveRule) -> Result<Self, Self::Error> {
        StorableRule::try_from(Rule::Deductive(rule.clone()))
    }
}

impl TryFrom<InductiveRule> for StorableRule {
    type Error = TypeError;

    fn try_from(rule: InductiveRule) -> Result<Self, Self::Error> {
        StorableRule::try_from(Rule::Inductive(rule))
    }
}

impl TryFrom<&InductiveRule> for StorableRule {
    type Error = TypeError;

    fn try_from(rule: &InductiveRule) -> Result<Self, Self::Error> {
        StorableRule::try_from(Rule::Inductive(rule.clone()))
    }
}

impl Statement for &StorableRule {
    fn assert(self, update: &mut impl Update) {
        let entity = self.this.clone();
        let body = Value::Bytes(self.body.clone());
        match &self.rule {
            Rule::Deductive(rule) => {
                update.associate(
                    conclusion_attr(),
                    entity.clone(),
                    Value::Entity(rule.conclusion().this()),
                    dialog_corpus::Cardinality::Many,
                );
                update.associate(
                    source_attr(),
                    entity.clone(),
                    body,
                    dialog_corpus::Cardinality::Many,
                );
                for reads in reads_entities(rule) {
                    update.associate(
                        reads_attr(),
                        entity.clone(),
                        Value::Entity(reads),
                        dialog_corpus::Cardinality::Many,
                    );
                }
                for derives in derives_entities(rule) {
                    update.associate(
                        derives_attr(),
                        entity.clone(),
                        Value::Entity(derives),
                        dialog_corpus::Cardinality::Many,
                    );
                }
            }
            Rule::Inductive(rule) => {
                update.associate(
                    source_attr(),
                    entity.clone(),
                    body,
                    dialog_corpus::Cardinality::Many,
                );
                update.associate(
                    induces_attr(),
                    entity.clone(),
                    Value::Entity(rule.conclusion().this()),
                    dialog_corpus::Cardinality::Many,
                );
                for on in on_entities(rule) {
                    update.associate(
                        on_attr(),
                        entity.clone(),
                        Value::Entity(on),
                        dialog_corpus::Cardinality::Many,
                    );
                }
            }
        }
    }

    fn retract(self, update: &mut impl Update) {
        let entity = self.this.clone();
        let body = Value::Bytes(self.body.clone());
        match &self.rule {
            Rule::Deductive(rule) => {
                update.dissociate(
                    conclusion_attr(),
                    entity.clone(),
                    Value::Entity(rule.conclusion().this()),
                );
                update.dissociate(source_attr(), entity.clone(), body);
                for reads in reads_entities(rule) {
                    update.dissociate(reads_attr(), entity.clone(), Value::Entity(reads));
                }
                for derives in derives_entities(rule) {
                    update.dissociate(derives_attr(), entity.clone(), Value::Entity(derives));
                }
            }
            Rule::Inductive(rule) => {
                update.dissociate(source_attr(), entity.clone(), body);
                update.dissociate(
                    induces_attr(),
                    entity.clone(),
                    Value::Entity(rule.conclusion().this()),
                );
                for on in on_entities(rule) {
                    update.dissociate(on_attr(), entity.clone(), Value::Entity(on));
                }
            }
        }
    }
}

impl Statement for StorableRule {
    fn assert(self, update: &mut impl Update) {
        (&self).assert(update);
    }

    fn retract(self, update: &mut impl Update) {
        (&self).retract(update);
    }
}

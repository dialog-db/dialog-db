//! Rules as [`Statement`]s: the `dialog.rule/*` storage vocabulary and
//! the lowering that installs a rule by plain assertion.
//!
//! A rule *is* its facts: asserting a [`DeductiveRule`] or an
//! [`InductiveRule`] into any [`Update`] target stages the
//! `dialog.rule/*` claims that persist it, and retracting the same rule
//! erases them — install and uninstall are ordinary writes, no dedicated
//! API. The kind decides the fact shape:
//!
//! - deductive: the `conclusion` discovery index, the `derives` head
//!   index (one claim per head attribute), the `source` body, and the
//!   `reads` reverse index over the body's attributes;
//! - inductive: the shared `source` body, the `induces` head index,
//!   and the `on` trigger index commit-time dispatch probes by touched
//!   attribute.
//!
//! The rule entity is the content address of its canonical body, which
//! is what makes these facts safe to accept from the ordinary write
//! path: a reader verifies the decoded body against the entity it was
//! stored under and treats mismatched entries as inert.

use std::collections::BTreeSet;

use crate::artifact::{Entity, Value};
use crate::attribute::Relation;
use crate::rule::{DeductiveRule, InductiveRule, Rule};
use crate::{Proposition, Statement, Update, the};
use dialog_artifacts::{Attribute, NameShape, Symbol};

/// The `dialog.rule/source` body attribute, validated at compile time.
/// Shared by both rule kinds — hydration dispatches on the decoded
/// descriptor's head field.
pub fn source_attr() -> Attribute {
    the!("dialog.rule/source").into()
}

/// The `dialog.rule/conclusion` index attribute, validated at compile time.
pub fn conclusion_attr() -> Attribute {
    the!("dialog.rule/conclusion").into()
}

/// The `dialog.rule/induces` index attribute — the inductive sibling of
/// `dialog.rule/conclusion`, kept separate so deductive resolution never
/// hydrates (and discards) inductive rules concluding a queried
/// concept.
pub fn induces_attr() -> Attribute {
    the!("dialog.rule/induces").into()
}

/// The `dialog.rule/on` trigger-index attribute: one claim per attribute an
/// inductive rule's concept premises name, valued `on:<domain>/<name>`.
/// This is the index commit-time dispatch probes by touched attribute.
pub fn on_attr() -> Attribute {
    the!("dialog.rule/on").into()
}

/// The `dialog.rule/reads` reverse index for *deductive* rules: one claim
/// per attribute the rule's body names, valued `on:<domain>/<name>`.
/// Commit-time dispatch composes these at probe time to close the
/// trigger footprint over derivation — a base-fact write reaches
/// inductive rules premised on the concepts it (transitively)
/// supports. Per-rule and derived from the rule's own immutable body,
/// so an entry is never stale; the closure itself is never stored.
pub fn reads_attr() -> Attribute {
    the!("dialog.rule/reads").into()
}

/// The `dialog.rule/derives` head index for *deductive* rules: one claim
/// per attribute the rule's head names, valued `on:<domain>/<name>`
/// (a keyed collection contributes its half's cover key). This is the
/// index attribute-level resolution discovers rules by: a query for a
/// concept probes one narrow slice per attribute of the concept, so a
/// rule whose head merely *overlaps* the concept is found, and nothing
/// enumerates all rules. See `notes/attribute-level-deduction.md`.
pub fn derives_attr() -> Attribute {
    the!("dialog.rule/derives").into()
}

/// The `on:<domain>/<name>` trigger-index entity for an attribute.
/// Derivable from a runtime instruction alone — no schema lookup —
/// which is what keeps dispatch probing cheap.
pub fn on_entity(attribute: &Attribute) -> Option<Entity> {
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
    /// One attribute.
    Attribute(Attribute),
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
    pub fn of(relation: &Relation) -> Reach {
        match relation {
            Relation::Attribute(the) => Reach::Attribute(the.into()),
            Relation::Collection { domain, keyed } => Reach::Domain {
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
    pub fn covers(&self, attribute: &Attribute) -> bool {
        match self {
            Reach::Attribute(the) => the == attribute,
            Reach::Domain { domain, shape } => attribute
                .split()
                .is_ok_and(|(of, name)| &of == domain && name.shape() == *shape),
        }
    }

    /// Whether the two reaches share an attribute.
    pub fn overlaps(&self, other: &Reach) -> bool {
        match (self, other) {
            (Reach::Attribute(the), other) | (other, Reach::Attribute(the)) => other.covers(the),
            (Reach::Domain { .. }, Reach::Domain { .. }) => self == other,
        }
    }

    /// The trigger-index entity a rule is filed under for this reach.
    pub fn on_entity(&self) -> Option<Entity> {
        match self {
            Reach::Attribute(the) => on_entity(the),
            Reach::Domain { domain, shape } => Self::cover_entity(domain.as_str(), *shape),
        }
    }

    /// The trigger-index entities a touched reach probes: an attribute
    /// probes its own key and its half's cover key, so both a rule
    /// reading that attribute and a rule reading the collection it
    /// belongs to are found; a domain half probes its cover key.
    pub fn probes(&self) -> Vec<Entity> {
        match self {
            Reach::Attribute(the) => {
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
                if let Some(entity) = Reach::of(field.descriptor().the()).on_entity() {
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

/// The head-index entities for a deductive rule: one per attribute
/// its conclusion names. Stored as `dialog.rule/derives` so a concept
/// query can discover, per attribute, every rule deriving that
/// attribute — whatever the rest of the rule's head looks like.
pub fn derives_entities(rule: &DeductiveRule) -> BTreeSet<Entity> {
    head_entities(rule.conclusion())
}

/// The `on:` entities of every field of a concept descriptor.
pub fn head_entities(concept: &crate::ConceptDescriptor) -> BTreeSet<Entity> {
    concept
        .with()
        .iter()
        .filter_map(|(_, field)| Reach::of(field.descriptor().the()).on_entity())
        .collect()
}

/// Asserting a [`DeductiveRule`] installs it as `dialog.rule/*` facts:
/// the `conclusion` discovery index, the `source` body, and the `reads`
/// reverse index over the body's attributes that lets commit-time
/// dispatch close its trigger footprint over derivation. Retracting the
/// same rule erases those facts, uninstalling it.
///
/// ```no_run
/// # use dialog_query::DeductiveRule;
/// # use dialog_query::Changes;
/// # fn example(rule: DeductiveRule) {
/// let mut changes = Changes::new();
/// changes.assert(&rule);
/// # }
/// ```
impl Statement for &DeductiveRule {
    fn assert(self, update: &mut impl Update) {
        let rule_entity = self.this();
        update.associate(
            conclusion_attr(),
            rule_entity.clone(),
            Value::Entity(self.conclusion().this()),
        );
        update.associate(
            source_attr(),
            rule_entity.clone(),
            Value::Bytes(self.encode()),
        );
        for reads in reads_entities(self) {
            update.associate(reads_attr(), rule_entity.clone(), Value::Entity(reads));
        }
        for derives in derives_entities(self) {
            update.associate(derives_attr(), rule_entity.clone(), Value::Entity(derives));
        }
    }

    fn retract(self, update: &mut impl Update) {
        let rule_entity = self.this();
        update.dissociate(
            conclusion_attr(),
            rule_entity.clone(),
            Value::Entity(self.conclusion().this()),
        );
        update.dissociate(
            source_attr(),
            rule_entity.clone(),
            Value::Bytes(self.encode()),
        );
        for reads in reads_entities(self) {
            update.dissociate(reads_attr(), rule_entity.clone(), Value::Entity(reads));
        }
        for derives in derives_entities(self) {
            update.dissociate(derives_attr(), rule_entity.clone(), Value::Entity(derives));
        }
    }
}

impl Statement for DeductiveRule {
    fn assert(self, update: &mut impl Update) {
        (&self).assert(update);
    }

    fn retract(self, update: &mut impl Update) {
        (&self).retract(update);
    }
}

/// Asserting an [`InductiveRule`] installs it as `dialog.rule/*` facts:
/// the `source` body (shared attribute with deductive rules — hydration
/// dispatches on the head field), the `induces` head index, and the
/// `on` trigger index dispatch probes by touched attribute. Retracting
/// the same rule erases those facts, uninstalling it.
impl Statement for &InductiveRule {
    fn assert(self, update: &mut impl Update) {
        let rule_entity = self.this();
        update.associate(
            source_attr(),
            rule_entity.clone(),
            Value::Bytes(self.encode()),
        );
        update.associate(
            induces_attr(),
            rule_entity.clone(),
            Value::Entity(self.conclusion().this()),
        );
        for on in on_entities(self) {
            update.associate(on_attr(), rule_entity.clone(), Value::Entity(on));
        }
    }

    fn retract(self, update: &mut impl Update) {
        let rule_entity = self.this();
        update.dissociate(
            source_attr(),
            rule_entity.clone(),
            Value::Bytes(self.encode()),
        );
        update.dissociate(
            induces_attr(),
            rule_entity.clone(),
            Value::Entity(self.conclusion().this()),
        );
        for on in on_entities(self) {
            update.dissociate(on_attr(), rule_entity.clone(), Value::Entity(on));
        }
    }
}

impl Statement for InductiveRule {
    fn assert(self, update: &mut impl Update) {
        (&self).assert(update);
    }

    fn retract(self, update: &mut impl Update) {
        (&self).retract(update);
    }
}

/// A kind-erased [`Rule`] lowers as whichever variant it carries.
impl Statement for &Rule {
    fn assert(self, update: &mut impl Update) {
        match self {
            Rule::Deductive(rule) => rule.assert(update),
            Rule::Inductive(rule) => rule.assert(update),
        }
    }

    fn retract(self, update: &mut impl Update) {
        match self {
            Rule::Deductive(rule) => rule.retract(update),
            Rule::Inductive(rule) => rule.retract(update),
        }
    }
}

impl Statement for Rule {
    fn assert(self, update: &mut impl Update) {
        (&self).assert(update);
    }

    fn retract(self, update: &mut impl Update) {
        (&self).retract(update);
    }
}

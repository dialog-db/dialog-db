//! Rules as [`Statement`]s: the `dialog.rule/*` storage vocabulary and
//! the lowering that installs a rule by plain assertion.
//!
//! A rule *is* its facts: asserting a [`DeductiveRule`] or an
//! [`InductiveRule`] into any [`Update`] target stages the
//! `dialog.rule/*` claims that persist it, and retracting the same rule
//! erases them — install and uninstall are ordinary writes, no dedicated
//! API. The kind decides the fact shape:
//!
//! - deductive: the `conclusion` discovery index, the `source` body,
//!   and the `reads` reverse index over the body's attributes;
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
use crate::rule::{DeductiveRule, InductiveRule, Rule};
use crate::{Proposition, Statement, Update, the};
use dialog_artifacts::Attribute;

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

/// The `dialog.rule/description` sidecar attribute: a rule's
/// human-readable prose, stored beside the canonical body rather than
/// inside it so editing the prose never moves the rule's
/// content-addressed identity (the same stance
/// `dialog.concept/transient` takes for concepts).
pub fn description_attr() -> Attribute {
    the!("dialog.rule/description").into()
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

/// The `on:<domain>/<name>` trigger-index entity for an attribute.
/// Derivable from a runtime instruction alone — no schema lookup —
/// which is what keeps dispatch probing cheap.
pub fn on_entity(attribute: &Attribute) -> Option<Entity> {
    format!("on:{attribute}").parse().ok()
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
                let attribute: Attribute = field.descriptor().the().clone().into();
                if let Some(entity) = on_entity(&attribute) {
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
        if let Some(description) = self.description() {
            update.associate(
                description_attr(),
                rule_entity.clone(),
                Value::String(description.to_owned()),
            );
        }
        for reads in reads_entities(self) {
            update.associate(reads_attr(), rule_entity.clone(), Value::Entity(reads));
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
        if let Some(description) = self.description() {
            update.dissociate(
                description_attr(),
                rule_entity.clone(),
                Value::String(description.to_owned()),
            );
        }
        for reads in reads_entities(self) {
            update.dissociate(reads_attr(), rule_entity.clone(), Value::Entity(reads));
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
        if let Some(description) = self.description() {
            update.associate(
                description_attr(),
                rule_entity.clone(),
                Value::String(description.to_owned()),
            );
        }
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
        if let Some(description) = self.description() {
            update.dissociate(
                description_attr(),
                rule_entity.clone(),
                Value::String(description.to_owned()),
            );
        }
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

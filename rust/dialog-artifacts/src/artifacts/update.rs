use crate::{Artifact, Attribute, Entity, Instruction, Value};
use futures_util::Stream;
use std::collections::HashMap;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::vec::IntoIter;

/// A single write operation on an `(entity, attribute)` pair.
#[derive(Debug, Clone, PartialEq)]
pub enum Change {
    /// Assert a value for an entity-attribute pair.
    Assert(Value),
    /// Retract a value from an entity-attribute pair.
    Retract(Value),
}

/// The write side of the triple store.
///
/// Implementors accumulate fact changes (associations and dissociations)
/// that can later be committed atomically.
pub trait Update {
    /// Assert that the `attribute` of `entity` is `value`.
    fn associate(&mut self, the: Attribute, of: Entity, is: Value);

    /// Assert with cardinality-one semantics: replaces any previous
    /// value for the same `(attribute, entity)` pair in this batch.
    fn associate_unique(&mut self, the: Attribute, of: Entity, is: Value) {
        self.associate(the, of, is);
    }

    /// Retract that the `attribute` of `entity` is `value`.
    fn dissociate(&mut self, the: Attribute, of: Entity, is: Value);
}

/// A domain-level write operation that can be asserted or retracted.
///
/// Types like concept structs and attribute expressions implement this
/// trait. Asserting a statement adds facts; retracting removes them.
pub trait Statement: Sized {
    /// Assert this statement into an update target.
    fn assert(self, update: &mut impl Update);

    /// Retract this statement from an update target.
    fn retract(self, update: &mut impl Update);
}

/// A batch of pending writes, organized by entity and attribute.
#[derive(Debug, Default)]
pub struct Changes(HashMap<Entity, HashMap<Attribute, Vec<Change>>>);

impl Changes {
    /// Create an empty changeset.
    pub fn new() -> Self {
        Self::default()
    }

    /// Assert a claim.
    pub fn assert<C: Statement>(&mut self, claim: C) -> &mut Self {
        claim.assert(self);
        self
    }

    /// Retract a claim.
    pub fn retract<C: Statement>(&mut self, claim: C) -> &mut Self {
        claim.retract(self);
        self
    }

    /// Check if empty.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Convert to an instruction stream.
    pub fn into_stream(self) -> ChangeStream {
        ChangeStream::from(self)
    }

    /// Convert to a vec of instructions.
    pub fn into_instructions(self) -> Vec<Instruction> {
        let mut instructions = Vec::new();
        for (entity, attributes) in self.0 {
            for (attribute, operations) in attributes {
                for operation in operations {
                    let instruction = match operation {
                        Change::Assert(value) => Instruction::Assert(Artifact {
                            the: attribute.clone(),
                            of: entity.clone(),
                            is: value,
                            cause: None,
                        }),
                        Change::Retract(value) => Instruction::Retract(Artifact {
                            the: attribute.clone(),
                            of: entity.clone(),
                            is: value,
                            cause: None,
                        }),
                    };
                    instructions.push(instruction);
                }
            }
        }
        instructions
    }
}

impl Update for Changes {
    fn associate(&mut self, the: Attribute, of: Entity, is: Value) {
        self.0
            .entry(of)
            .or_default()
            .entry(the)
            .or_default()
            .push(Change::Assert(is));
    }

    fn associate_unique(&mut self, the: Attribute, of: Entity, is: Value) {
        self.0
            .entry(of)
            .or_default()
            .insert(the, vec![Change::Assert(is)]);
    }

    fn dissociate(&mut self, the: Attribute, of: Entity, is: Value) {
        self.0
            .entry(of)
            .or_default()
            .entry(the)
            .or_default()
            .push(Change::Retract(is));
    }
}

impl IntoIterator for Changes {
    type Item = Instruction;
    type IntoIter = IntoIter<Instruction>;

    fn into_iter(self) -> Self::IntoIter {
        self.into_instructions().into_iter()
    }
}

/// A [`Stream`] adapter that drains [`Changes`] into [`Instruction`]s.
pub struct ChangeStream {
    iter: IntoIter<Instruction>,
}

impl From<Changes> for ChangeStream {
    fn from(changes: Changes) -> Self {
        Self {
            iter: changes.into_iter(),
        }
    }
}

impl Stream for ChangeStream {
    type Item = Instruction;

    fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Poll::Ready(self.iter.next())
    }
}

//! Transaction system for dialog-query
//!
//! Provides a more extensible and efficient alternative to the direct Statement -> Instruction conversion.
//! Statements can now add operations to a Transaction which accumulates changes and optimizes before committing.

mod change;
mod edit;
mod stream;

pub use change::*;
pub use edit::*;
pub use stream::*;

use crate::Statement;
use crate::artifact::{Artifact, Entity, Instruction};
use crate::association::{Association, Attribute};
use crate::attribute::The;
use std::collections::HashMap;

/// An in-memory buffer of pending writes that can be committed atomically.
///
/// A `Transaction` collects [`Change`] operations (assert / retract) grouped
/// by `(entity, attribute)`. When committed via [`Session::commit`](super::Session),
/// the transaction is converted into a stream of [`Instruction`]s and
/// written to the underlying store in one batch.
///
/// Build a transaction using the [`Edit`] trait — call
/// [`Session::edit`](super::Session) to get an empty transaction, then
/// assert or retract claims into it.
#[derive(Debug)]
pub struct Transaction {
    /// Changes organized by entity -> attribute -> operation
    changes: Changes,
}

impl Transaction {
    /// Create a new empty transaction
    pub fn new() -> Self {
        Self {
            changes: HashMap::new(),
        }
    }

    /// Assert a claim into this transaction
    pub fn assert<C: Statement>(&mut self, claim: C) -> &mut Self {
        claim.assert(self);
        self
    }

    /// Retract a claim from this transaction
    pub fn retract<C: Statement>(&mut self, claim: C) -> &mut Self {
        claim.retract(self);
        self
    }

    /// Assert a relation (entity-attribute-value triple).
    /// Multiple assertions for the same `(entity, attribute)` accumulate.
    pub fn associate(&mut self, association: Association) -> &mut Self {
        self.insert(
            association.the,
            association.of,
            Change::Assert(association.is),
        )
    }

    /// Retract an association (entity-attribute-value triple).
    pub fn dissociate(&mut self, association: Association) -> &mut Self {
        self.insert(
            association.the,
            association.of,
            Change::Retract(association.is),
        )
    }

    /// Assert with `Cardinality::One` semantics: if the same
    /// `(entity, attribute)` pair was already asserted in this transaction,
    /// the previous value is replaced.
    pub fn associate_unique(&mut self, association: Association) -> &mut Self {
        self.replace(
            association.the,
            association.of,
            Change::Assert(association.is),
        )
    }

    /// Add a change operation for an entity-attribute pair, accumulating with
    /// any existing changes.
    fn insert(&mut self, the: The, of: Entity, change: Change) -> &mut Self {
        self.changes
            .entry(of)
            .or_default()
            .entry(the)
            .or_default()
            .push(change);
        self
    }

    /// Replace any existing changes for an entity-attribute pair with a single
    /// new change. Used for `Cardinality::One` attributes where only the last
    /// write should survive within a transaction.
    fn replace(&mut self, the: The, of: Entity, change: Change) -> &mut Self {
        self.changes
            .entry(of)
            .or_default()
            .insert(the, vec![change]);
        self
    }

    /// Check if the transaction is empty
    pub fn is_empty(&self) -> bool {
        self.changes.is_empty()
    }

    /// Convert the transaction into a stream
    pub fn into_stream(self) -> TransactionStream {
        TransactionStream::from(self)
    }

    /// Convert the transaction into Instructions for committing
    ///
    /// This is where optimizations can be applied:
    /// - Eliminate redundant operations
    /// - Optimize batch operations
    /// - Reorder for better performance
    pub fn into_instructions(self) -> Vec<Instruction> {
        let mut instructions = Vec::new();

        for (entity, attributes) in self.changes {
            for (the, operations) in attributes {
                let attribute: Attribute = the.into();
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

impl Default for Transaction {
    fn default() -> Self {
        Self::new()
    }
}

/// Implement IntoIterator for Transaction to provide instruction iteration
impl IntoIterator for Transaction {
    type Item = Instruction;
    type IntoIter = std::vec::IntoIter<Instruction>;

    fn into_iter(self) -> Self::IntoIter {
        self.into_instructions().into_iter()
    }
}

/// Implement Edit for Transaction so transactions can be composed
impl Edit for Transaction {
    fn merge(self, transaction: &mut Transaction) {
        for (of, attributes) in self.changes {
            for (the, changes) in attributes {
                for change in changes {
                    transaction.insert(the.clone(), of.clone(), change);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Association;
    use crate::artifact::{Entity, Value};
    use crate::the;

    #[dialog_common::test]
    fn it_creates_and_asserts_associations() -> anyhow::Result<()> {
        let mut transaction = Transaction::new();
        let alice = Entity::new()?;

        // Test basic assert
        transaction.associate(Association {
            the: the!("user/name"),
            of: alice.clone(),
            is: Value::String("Alice".to_string()),
        });

        // Check transaction state
        assert!(!transaction.is_empty());

        // Convert to instructions
        let instructions = transaction.into_instructions();
        assert_eq!(instructions.len(), 1);

        Ok(())
    }

    #[dialog_common::test]
    fn it_accumulates_multiple_values() -> anyhow::Result<()> {
        let mut transaction = Transaction::new();
        let alice = Entity::new()?;
        let first_value = Value::String("Alice".to_string());
        let second_value = Value::String("Alice Smith".to_string());

        transaction.associate(Association {
            the: the!("user/name"),
            of: alice.clone(),
            is: first_value.clone(),
        });

        transaction.associate(Association {
            the: the!("user/name"),
            of: alice.clone(),
            is: second_value.clone(),
        });

        // Both values should be present as separate instructions
        let instructions = transaction.into_instructions();
        assert_eq!(instructions.len(), 2);

        let values: Vec<_> = instructions
            .iter()
            .map(|i| match i {
                Instruction::Assert(a) => &a.is,
                Instruction::Retract(a) => &a.is,
            })
            .collect();

        assert!(values.contains(&&first_value));
        assert!(values.contains(&&second_value));

        Ok(())
    }

    #[dialog_common::test]
    fn it_replaces_previous_value_with_unique() -> anyhow::Result<()> {
        let mut transaction = Transaction::new();
        let alice = Entity::new()?;
        let first_value = Value::String("Alice".to_string());
        let second_value = Value::String("Alice Smith".to_string());

        transaction.associate_unique(Association {
            the: the!("user/name"),
            of: alice.clone(),
            is: first_value.clone(),
        });

        transaction.associate_unique(Association {
            the: the!("user/name"),
            of: alice.clone(),
            is: second_value.clone(),
        });

        // Only the last value should survive
        let instructions = transaction.into_instructions();
        assert_eq!(
            instructions.len(),
            1,
            "associate_unique should replace, not accumulate"
        );

        if let Instruction::Assert(artifact) = &instructions[0] {
            assert_eq!(artifact.is, second_value);
        } else {
            panic!("Expected Assert instruction");
        }

        Ok(())
    }

    #[dialog_common::test]
    fn it_preserves_other_attributes_on_unique() -> anyhow::Result<()> {
        let mut transaction = Transaction::new();
        let alice = Entity::new()?;
        transaction.associate_unique(Association {
            the: the!("user/name"),
            of: alice.clone(),
            is: Value::String("Alice".to_string()),
        });

        transaction.associate_unique(Association {
            the: the!("user/age"),
            of: alice.clone(),
            is: Value::SignedInt(30),
        });

        // Two different attributes — both should be present
        let instructions = transaction.into_instructions();
        assert_eq!(instructions.len(), 2);

        Ok(())
    }

    #[dialog_common::test]
    fn it_converts_to_iterator() -> anyhow::Result<()> {
        let mut transaction = Transaction::new();
        let alice = Entity::new()?;
        let bob = Entity::new()?;

        transaction.associate(Association {
            the: the!("user/name"),
            of: alice,
            is: Value::String("Alice".to_string()),
        });

        transaction.associate(Association {
            the: the!("user/name"),
            of: bob,
            is: Value::String("Bob".to_string()),
        });

        // Test IntoIterator implementation
        let instructions: Vec<_> = transaction.into_iter().collect();
        assert_eq!(instructions.len(), 2);

        Ok(())
    }

    #[dialog_common::test]
    async fn it_streams_transaction_artifacts() -> anyhow::Result<()> {
        use futures_util::StreamExt;

        let mut transaction = Transaction::new();
        let alice = Entity::new()?;
        let bob = Entity::new()?;

        transaction.associate(Association {
            the: the!("user/name"),
            of: alice,
            is: Value::String("Alice".to_string()),
        });

        transaction.associate(Association {
            the: the!("user/name"),
            of: bob,
            is: Value::String("Bob".to_string()),
        });

        // Test streaming the transaction
        let mut stream: TransactionStream = transaction.into();
        let mut count = 0;

        while let Some(_instruction) = stream.next().await {
            count += 1;
        }

        assert_eq!(count, 2);
        Ok(())
    }
}

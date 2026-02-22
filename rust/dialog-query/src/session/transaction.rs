//! Transaction system for dialog-query
//!
//! Provides a more extensible and efficient alternative to the direct Claim -> Instruction conversion.
//! Claims can now add operations to a Transaction which accumulates changes and optimizes before committing.

use crate::Claim;
use crate::artifact::{Artifact, Attribute, DialogArtifactsError, Entity, Instruction, Value};
use crate::relation::Relation;
use futures_util::Stream;
use std::collections::HashMap;
use std::pin::Pin;
use std::task::{Context, Poll};

/// Error types that can occur during transaction operations
#[derive(Debug, thiserror::Error)]
pub enum TransactionError {
    /// The requested operation is not valid
    #[error("Invalid operation: {reason}")]
    InvalidOperation {
        /// Reason why the operation is invalid
        reason: String,
    },
    /// An error from the underlying storage layer
    #[error("Storage error: {0}")]
    Storage(#[from] DialogArtifactsError),
}

/// Changes organized by entity -> attribute -> operations.
/// Each `(entity, attribute)` pair may have multiple changes — for example
/// asserting several values on a `Cardinality::Many` attribute in one
/// transaction.
pub type Changes = HashMap<Entity, HashMap<Attribute, Vec<Change>>>;

/// Type of change
#[derive(Debug, Clone, PartialEq)]
pub enum Change {
    /// Assert a value for an entity-attribute pair
    Assert(Value),
    /// Retract a value from an entity-attribute pair
    Retract(Value),
}

/// A transaction accumulates changes before committing them as instructions.
///
/// Multiple values can be asserted for the same `(entity, attribute)` pair
/// within a single transaction — all are preserved as separate instructions.
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
    pub fn assert<C: Claim>(&mut self, claim: C) -> &mut Self {
        claim.assert(self);
        self
    }

    /// Retract a claim from this transaction
    pub fn retract<C: Claim>(&mut self, claim: C) -> &mut Self {
        claim.retract(self);
        self
    }

    /// Assert a relation (entity-attribute-value triple).
    /// Multiple assertions for the same `(entity, attribute)` accumulate.
    pub fn associate(&mut self, relation: Relation) -> &mut Self {
        self.insert(relation.the, relation.of, Change::Assert(relation.is))
    }

    /// Retract a relation (entity-attribute-value triple).
    pub fn dissociate(&mut self, relation: Relation) -> &mut Self {
        self.insert(relation.the, relation.of, Change::Retract(relation.is))
    }

    /// Assert a relation with `Cardinality::One` semantics: if the same
    /// `(entity, attribute)` pair was already asserted in this transaction,
    /// the previous value is replaced.
    pub fn associate_unique(&mut self, relation: Relation) -> &mut Self {
        self.replace(relation.the, relation.of, Change::Assert(relation.is))
    }

    /// Add a change operation for an entity-attribute pair, accumulating with
    /// any existing changes.
    fn insert(&mut self, the: Attribute, of: Entity, change: Change) -> &mut Self {
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
    fn replace(&mut self, the: Attribute, of: Entity, change: Change) -> &mut Self {
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

/// Implement Stream for Transaction to provide async instruction streaming
///
/// This delegates to the IntoIterator implementation for simplicity and consistency.
/// The transaction is converted to instructions and then streamed via the iterator.
///
/// # Examples
///
/// ```rs
/// use dialog_query::Transaction;
/// use futures_util::StreamExt;
///
/// let mut transaction = session.edit();
/// // ... add operations to transaction ...
///
/// // Stream the instructions asynchronously
/// let mut stream = Box::pin(transaction);
/// while let Some(instruction) = stream.next().await {
///     // Process each instruction
///     println!("Processing: {:?}", instruction);
/// }
/// ```
/// Stream adapter that yields instructions from a consumed transaction
pub struct TransactionStream {
    /// Iterator over the transaction's instructions
    iter: std::vec::IntoIter<Instruction>,
}

impl From<Transaction> for TransactionStream {
    fn from(transaction: Transaction) -> Self {
        Self {
            iter: transaction.into_iter(),
        }
    }
}

impl Stream for TransactionStream {
    type Item = Instruction;

    fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Poll::Ready(self.iter.next())
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

/// Trait for types that can merge operations into a Transaction
///
/// This is the key extensibility point - Claims and other types
/// implement this trait to merge their operations into a transaction
pub trait Edit {
    /// Merge this item's operations into the transaction
    fn merge(self, transaction: &mut Transaction);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Relation;
    use crate::artifact::{Attribute, Entity, Value};

    #[dialog_common::test]
    fn test_transaction_basic_operations() -> anyhow::Result<()> {
        let mut transaction = Transaction::new();
        let alice = Entity::new()?;

        // Test basic assert
        let name_attr: Attribute = "user/name".parse()?;
        transaction.associate(Relation {
            the: name_attr.clone(),
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
    fn test_transaction_accumulates_multiple_values() -> anyhow::Result<()> {
        let mut transaction = Transaction::new();
        let alice = Entity::new()?;
        let name_attr: Attribute = "user/name".parse()?;
        let first_value = Value::String("Alice".to_string());
        let second_value = Value::String("Alice Smith".to_string());

        transaction.associate(Relation {
            the: name_attr.clone(),
            of: alice.clone(),
            is: first_value.clone(),
        });

        transaction.associate(Relation {
            the: name_attr.clone(),
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
    fn test_associate_unique_replaces_previous_value() -> anyhow::Result<()> {
        let mut transaction = Transaction::new();
        let alice = Entity::new()?;
        let name_attr: Attribute = "user/name".parse()?;
        let first_value = Value::String("Alice".to_string());
        let second_value = Value::String("Alice Smith".to_string());

        transaction.associate_unique(Relation {
            the: name_attr.clone(),
            of: alice.clone(),
            is: first_value.clone(),
        });

        transaction.associate_unique(Relation {
            the: name_attr.clone(),
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
    fn test_associate_unique_does_not_affect_other_attributes() -> anyhow::Result<()> {
        let mut transaction = Transaction::new();
        let alice = Entity::new()?;
        let name_attr: Attribute = "user/name".parse()?;
        let age_attr: Attribute = "user/age".parse()?;

        transaction.associate_unique(Relation {
            the: name_attr.clone(),
            of: alice.clone(),
            is: Value::String("Alice".to_string()),
        });

        transaction.associate_unique(Relation {
            the: age_attr.clone(),
            of: alice.clone(),
            is: Value::SignedInt(30),
        });

        // Two different attributes — both should be present
        let instructions = transaction.into_instructions();
        assert_eq!(instructions.len(), 2);

        Ok(())
    }

    #[dialog_common::test]
    fn test_into_iterator() -> anyhow::Result<()> {
        let mut transaction = Transaction::new();
        let alice = Entity::new()?;
        let bob = Entity::new()?;

        let name_attr: Attribute = "user/name".parse()?;
        transaction.associate(Relation {
            the: name_attr.clone(),
            of: alice,
            is: Value::String("Alice".to_string()),
        });

        transaction.associate(Relation {
            the: name_attr,
            of: bob,
            is: Value::String("Bob".to_string()),
        });

        // Test IntoIterator implementation
        let instructions: Vec<_> = transaction.into_iter().collect();
        assert_eq!(instructions.len(), 2);

        Ok(())
    }

    #[dialog_common::test]
    async fn test_transaction_stream() -> anyhow::Result<()> {
        use futures_util::StreamExt;

        let mut transaction = Transaction::new();
        let alice = Entity::new()?;
        let bob = Entity::new()?;

        let name_attr: Attribute = "user/name".parse()?;
        transaction.associate(Relation {
            the: name_attr.clone(),
            of: alice,
            is: Value::String("Alice".to_string()),
        });

        transaction.associate(Relation {
            the: name_attr,
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

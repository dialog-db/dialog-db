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
    #[error("Invalid operation: {reason}")]
    InvalidOperation { reason: String },
    #[error("Storage error: {0}")]
    Storage(#[from] DialogArtifactsError),
}

/// Changes organized by entity -> attribute -> operation
pub type Changes = HashMap<Entity, HashMap<Attribute, Change>>;

/// Type of change
#[derive(Debug, Clone, PartialEq)]
pub enum Change {
    Assert(Value),
    Retract(Value),
}

/// A transaction accumulates changes before committing them as instructions
///
/// This provides a simple way to batch operations before committing them.
/// Mutations simply replace with the latest value for each entity-attribute pair.
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

    pub fn assert<C: Claim>(&mut self, claim: C) -> &mut Self {
        claim.assert(self);
        self
    }

    pub fn retract<C: Claim>(&mut self, claim: C) -> &mut Self {
        claim.retract(self);
        self
    }

    pub fn associate(&mut self, relation: Relation) -> &mut Self {
        self.insert(relation.the, relation.of, Change::Assert(relation.is))
    }

    pub fn dissociate(&mut self, relation: Relation) -> &mut Self {
        self.insert(relation.the, relation.of, Change::Retract(relation.is))
    }

    /// Add a change operation - mutations simply replace with the latest value
    fn insert(&mut self, the: Attribute, of: Entity, change: Change) -> &mut Self {
        self.changes.entry(of).or_default().insert(the, change);
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
            for (attribute, operation) in attributes {
                let instruction = match operation {
                    Change::Assert(value) => Instruction::Assert(Artifact {
                        the: attribute,
                        of: entity.clone(),
                        is: value,
                        cause: None,
                    }),
                    Change::Retract(value) => Instruction::Retract(Artifact {
                        the: attribute,
                        of: entity.clone(),
                        is: value,
                        cause: None,
                    }),
                };
                instructions.push(instruction);
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
/// ```ignore
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
/// Internal state for Transaction streaming
pub struct TransactionStream {
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
        for (of, changes) in self.changes {
            for (the, change) in changes {
                transaction.insert(the, of.clone(), change);
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
    fn test_transaction_mutation_replacement() -> anyhow::Result<()> {
        let mut transaction = Transaction::new();
        let alice = Entity::new()?;
        let name_attr: Attribute = "user/name".parse()?;
        let initial_value = Value::String("Alice".to_string());
        let updated_value = Value::String("Alice Smith".to_string());

        // First operation
        transaction.associate(Relation {
            the: name_attr.clone(),
            of: alice.clone(),
            is: initial_value.clone(),
        });

        // Second operation should replace the first
        transaction.associate(Relation {
            the: name_attr.clone(),
            of: alice.clone(),
            is: updated_value.clone(),
        });

        // Should have only one instruction with the latest value
        let instructions = transaction.into_instructions();
        assert_eq!(instructions.len(), 1);

        if let Instruction::Assert(artifact) = &instructions[0] {
            assert_eq!(artifact.is, updated_value);
        } else {
            panic!("Expected Assert instruction");
        }

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

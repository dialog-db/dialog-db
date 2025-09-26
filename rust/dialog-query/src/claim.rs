//! High-level representations for database changes
//!
//! This module provides the `Claim` enum for representing complex database operations.
//! Claims use the modern transaction-based API for efficient batching and streaming.
//!
//! ## Overview
//!
//! Claims provide a high-level interface for describing database changes. Different
//! types of claims (facts, concepts, etc.) implement the `Edit` trait to merge their
//! operations into transactions before committing to the database.
//!
//! ```text
//! ┌─────────────┐    ┌─────────────┐    ┌─────────────┐
//! │   Claims    │ -> │ Transaction │ -> │  Database   │
//! │ (High-level)│    │ (Batched)   │    │  (Storage)  │
//! └─────────────┘    └─────────────┘    └─────────────┘
//! ```
//!
//! ## Usage Examples
//!
//! ### Transaction-based API (Preferred)
//!
//! ```ignore
//! use dialog_query::{Fact, Session};
//!
//! // Create individual claims
//! let claim1 = Fact::assert("user/name".parse()?, entity, "Alice".to_string());
//! let claim2 = Fact::assert("user/age".parse()?, entity, 25u32);
//!
//! // Commit using transaction API
//! let mut session = Session::open(store);
//! session.transact(vec![claim1, claim2]).await?;
//! ```
//!
//! ### Transaction Builder API
//!
//! ```ignore
//! use dialog_query::{Session, Relation};
//!
//! let mut session = Session::open(store);
//! let mut transaction = session.edit();
//!
//! // Add operations to transaction
//! transaction.assert(Relation::new(attr, entity, value));
//! transaction.retract(Relation::new(attr2, entity, old_value));
//!
//! // Commit the transaction
//! session.commit(transaction).await?;
//! ```
//!

pub mod concept;
pub mod fact;
pub mod rule;

pub use self::fact::Relation;
pub use crate::artifact::{Artifact, Attribute, Instruction};
pub use crate::session::transaction::{Edit, Transaction, TransactionError};
use dialog_artifacts::Entity;

/// A high-level representation of database changes
///
/// Claims describe complex database operations that get converted to low-level
/// `Instruction`s for execution. Each claim can generate one or more instructions.
/// Different claim types (facts, concepts, etc.) can represent different kinds
/// of database operations.
///
/// # Examples
///
/// ```ignore
/// use dialog_query::Fact;
///
/// // Create an assertion claim
/// let claim = Fact::assert("user/name".parse()?, entity, "Alice".to_string());
///
/// // Claims automatically convert to a set of instructions
/// let instructions: Vec<Instruction> = claim.into();
/// ```
#[derive(Debug, Clone)]
pub enum Claim {
    /// A fact-based claim (assertion or retraction)
    Fact(fact::Claim),
    Concept(concept::ConceptClaim),
}

impl Claim {
    pub fn this(&self) -> &'_ Entity {
        match self {
            Self::Concept(claim) => claim.this(),
            Self::Fact(claim) => claim.of(),
        }
    }
}

impl Edit for Claim {
    fn merge(self, transaction: &mut Transaction) {
        match self {
            Self::Fact(claim) => claim.merge(transaction),
            Self::Concept(claim) => claim.merge(transaction),
        }
    }
}

impl From<fact::Claim> for Claim {
    fn from(claim: fact::Claim) -> Self {
        Claim::Fact(claim)
    }
}

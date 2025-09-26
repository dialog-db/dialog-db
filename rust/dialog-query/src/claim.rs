//! High-level representations for database changes
//!
//! This module provides the `Claim` enum for representing complex database operations
//! and the `Claims` collection for managing multiple changes. Claims are converted to
//! low-level `Instruction`s for execution.
//!
//! ## Overview
//!
//! Claims provide a high-level interface for describing database changes. Different
//! types of claims (facts, concepts, etc.) can generate different sets of instructions
//! when committed to the database.
//!
//! ```text
//! ┌─────────────┐    ┌─────────────┐    ┌─────────────┐
//! │   Claims    │ -> │ Instructions│ -> │  Database   │
//! │ (High-level)│    │ (Low-level) │    │  (Storage)  │
//! └─────────────┘    └─────────────┘    └─────────────┘
//! ```
//!
//! ## Usage Examples
//!
//! ### Basic Usage
//!
//! ```ignore
//! use dialog_query::{Fact, Claims, Session};
//!
//! // Create individual claims
//! let claim1 = Fact::assert("user/name".parse()?, entity, "Alice".to_string());
//! let claim2 = Fact::assert("user/age".parse()?, entity, 25u32);
//!
//! // Commit directly with Vec<Claim> (preferred API)
//! session.commit(vec![claim1, claim2]).await?;
//! ```
//!
//! ### Advanced Usage
//!
//! ```ignore
//! use dialog_query::{Claims, Fact};
//! use futures_util::StreamExt;
//!
//! // Create claims collection for streaming
//! let claims = Claims::from(vec![
//!     Fact::assert("user/name".parse()?, entity1, "Alice".to_string()),
//!     Fact::assert("user/name".parse()?, entity2, "Bob".to_string()),
//! ]);
//!
//! // Stream instructions asynchronously
//! let mut instruction_stream = claims;
//! while let Some(instruction) = instruction_stream.next().await {
//!     // Process each instruction
//! }
//! ```
//!

pub mod concept;
pub mod fact;
pub mod rule;

pub use crate::artifact::{Artifact, Attribute, Instruction};
pub use crate::session::transaction::{Edit, Transaction, TransactionError};
pub use self::fact::Relation;
use dialog_artifacts::Entity;
use futures_util::Stream;
use std::pin::Pin;
use std::task::{Context, Poll};

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

/// Convert a Claim into its constituent Instructions (legacy API)
///
/// **Deprecated**: Use the `Edit` trait with `claim.merge(&mut transaction)` instead.
/// This provides better performance and composability.
///
/// Transforms high-level claims into low-level instructions for database execution.
/// Each claim type determines how many instructions it generates.
impl From<Claim> for Vec<Instruction> {
    fn from(claim: Claim) -> Self {
        match claim {
            Claim::Fact(claim) => claim.into(),
            Claim::Concept(claim) => claim.into(),
        }
    }
}
/// Iterate over the instructions contained in a Claim
///
/// Allows processing each instruction individually when a claim represents
/// multiple changes.
///
/// # Examples
///
/// ```ignore
/// use dialog_query::Fact;
///
/// let claim = Fact::assert("user/name".parse()?, entity, "Alice".to_string());
///
/// // Iterate over all instructions in the claim
/// for instruction in claim {
///     println!("Instruction: {:?}", instruction);
/// }
/// ```
impl IntoIterator for Claim {
    type Item = Instruction;
    type IntoIter = std::vec::IntoIter<Instruction>;

    fn into_iter(self) -> Self::IntoIter {
        let instructions: Vec<Instruction> = self.into();
        instructions.into_iter()
    }
}

/// A collection of Claims for batch processing
///
/// `Claims` efficiently manages multiple claims and provides streaming and
/// iteration over their constituent instructions. Instructions are pre-computed
/// for predictable performance.
///
/// # Examples
///
/// ```ignore
/// use dialog_query::{Fact, Claims};
/// use futures_util::StreamExt;
///
/// let claims = vec![
///     Fact::assert("user/name".parse()?, entity1, "Alice".to_string()),
///     Fact::assert("user/name".parse()?, entity2, "Bob".to_string()),
/// ];
///
/// // Convert to Claims for streaming
/// let claims_stream = Claims::from(claims);
///
/// // Stream the instructions
/// let instructions: Vec<_> = claims_stream.collect().await;
/// ```
pub struct Claims {
    /// Pre-flattened instructions for efficient iteration
    inner: std::vec::IntoIter<Instruction>,
}

/// Create a Claims collection from multiple claims
///
/// # Examples
///
/// ```ignore
/// use dialog_query::{Fact, Claims};
///
/// let claims = vec![
///     Fact::assert("user/name".parse()?, entity1, "Alice".to_string()),
///     Fact::assert("user/age".parse()?, entity1, 25u32),
///     Fact::retract("user/email".parse()?, entity2, "old@example.com".to_string()),
/// ];
///
/// let claims_collection = Claims::from(claims);
/// ```
impl From<Vec<Claim>> for Claims {
    fn from(claims: Vec<Claim>) -> Self {
        let instructions: Vec<Instruction> = claims
            .into_iter()
            .flat_map(|claim| claim.into_iter())
            .collect();
        Claims {
            inner: instructions.into_iter(),
        }
    }
}

/// Create a Claims collection from a single claim
///
/// # Examples
///
/// ```ignore
/// use dialog_query::{Fact, Claims};
///
/// let claim = Fact::assert("user/name".parse()?, entity, "Alice".to_string());
/// let claims_collection = Claims::from(claim);
/// ```
impl From<Claim> for Claims {
    fn from(claim: Claim) -> Self {
        let instructions: Vec<Instruction> = claim.into_iter().collect();
        Claims {
            inner: instructions.into_iter(),
        }
    }
}

impl Claims {
    /// Create a Claims collection from pre-generated instructions
    /// 
    /// This is used internally by the Transaction system to convert
    /// instructions back to a streamable Claims collection.
    pub fn from_instructions(instructions: Vec<Instruction>) -> Self {
        Claims {
            inner: instructions.into_iter(),
        }
    }
}

/// Stream implementation for async iteration over instructions
///
/// # Examples
///
/// ```ignore
/// use dialog_query::{Claims, Fact};
/// use futures_util::StreamExt;
///
/// async fn process_claims(claims: Claims) {
///     let mut stream = claims;
///     while let Some(instruction) = stream.next().await {
///         // Process each instruction
///         println!("Processing: {:?}", instruction);
///     }
/// }
/// ```
impl Stream for Claims {
    type Item = Instruction;

    fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Poll::Ready(self.inner.next())
    }
}

/// Iterator implementation for synchronous iteration over instructions
///
/// # Examples
///
/// ```ignore
/// use dialog_query::{Claims, Fact};
///
/// let claims = Claims::from(vec![
///     Fact::assert("user/name".parse()?, entity, "Alice".to_string()),
/// ]);
///
/// // Collect all instructions
/// let instructions: Vec<_> = claims.into_iter().collect();
///
/// // Or iterate directly
/// for instruction in Claims::from(vec![claim]) {
///     println!("Instruction: {:?}", instruction);
/// }
/// ```
impl IntoIterator for Claims {
    type Item = Instruction;
    type IntoIter = std::vec::IntoIter<Instruction>;

    fn into_iter(self) -> Self::IntoIter {
        self.inner
    }
}

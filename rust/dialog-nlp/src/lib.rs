//! # Dialog NLP — Natural Language Parser via Dialog Discovery
//!
//! A natural language command parser inspired by Mozilla Ubiquity, where verbs
//! and nouns are discovered through dialog's own fact store and rule system.
//!
//! ## Core Ideas
//!
//! - **Nouns are derivation rules**: each noun type is a set of rules that
//!   recognize and extract typed values from text input or selection.
//! - **Verbs are effectful concepts**: each verb describes a command whose
//!   argument schema is stored as facts, and whose execution produces effects.
//! - **Discovery through dialog**: the parser queries the fact store for
//!   available verbs/nouns rather than maintaining a separate registry.
//! - **The parser pipeline is a rule cascade**: each stage derives new facts
//!   from the previous stage's output, making it inspectable and extensible.
//!
//! ## Architecture
//!
//! ```text
//! Input (facts) → Tokenize (formula) → Verb Match (query)
//!   → Segment by Role (rules) → Noun Resolution (rules)
//!     → Candidate Assembly (rules) → Scoring (formula)
//!       → Execution (effects)
//! ```

pub mod input;
pub mod noun;
pub mod verb;
pub mod role;
pub mod token;
pub mod segment;
pub mod parse;
pub mod score;
pub mod sentence;
pub mod effect;
pub mod error;

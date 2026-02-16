//! Parsed sentence — the output of the parser pipeline.
//!
//! A parsed sentence is a verb plus its resolved arguments. In dialog-query
//! terms, it would be a `Concept`:
//!
//! ```rust,ignore
//! #[derive(Concept)]
//! pub struct ParsedSentence {
//!     this: Entity,
//!     verb: parsed::VerbRef,
//!     score: parsed::Score,
//! }
//!
//! // Each resolved argument is a related fact:
//! #[derive(Concept)]
//! pub struct ResolvedArg {
//!     this: Entity,
//!     sentence: resolved_arg::SentenceRef,
//!     role: resolved_arg::Role,
//!     noun_type: resolved_arg::NounType,
//!     value: resolved_arg::Value,
//!     confidence: resolved_arg::Confidence,
//! }
//! ```

use crate::noun::NounMatch;
use crate::role::SemanticRole;
use crate::score::CandidateScore;
use crate::verb::VerbMatch;
use std::collections::HashMap;
use std::fmt;

/// A candidate parse — one possible interpretation of the user's input.
///
/// The parser may produce multiple candidates; they are scored and ranked.
/// The top candidate is offered for execution.
#[derive(Debug, Clone)]
pub struct Candidate {
    /// The matched verb.
    pub verb_match: VerbMatch,
    /// Resolved arguments: role → noun match.
    pub arguments: HashMap<SemanticRole, NounMatch>,
    /// The computed score for ranking.
    pub score: CandidateScore,
}

impl Candidate {
    /// Get the resolved value for a semantic role.
    pub fn argument_value(&self, role: &SemanticRole) -> Option<&str> {
        self.arguments.get(role).map(|m| m.value.as_str())
    }

    /// Get the verb name.
    pub fn verb_name(&self) -> &str {
        &self.verb_match.verb_name
    }
}

impl fmt::Display for Candidate {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.verb_match.verb_name)?;
        for (role, noun_match) in &self.arguments {
            write!(f, " [{}: {}]", role, noun_match)?;
        }
        write!(f, " (score: {:.2})", self.score.total())
    }
}

/// A ranked list of parse candidates, best first.
#[derive(Debug, Clone)]
pub struct ParseResult {
    /// Candidates sorted by score, highest first.
    pub candidates: Vec<Candidate>,
}

impl ParseResult {
    /// Get the top-ranked candidate, if any.
    pub fn best(&self) -> Option<&Candidate> {
        self.candidates.first()
    }

    /// Check if the parse produced any candidates.
    pub fn is_empty(&self) -> bool {
        self.candidates.is_empty()
    }

    /// Number of candidates.
    pub fn len(&self) -> usize {
        self.candidates.len()
    }
}

//! Scoring and ranking for parse candidates.
//!
//! Ubiquity uses a lexicographic scoring system where higher-priority
//! factors completely dominate lower ones. We adopt a similar approach:
//!
//! 1. **Verb match quality** (exact > prefix > substring)
//! 2. **Argument completeness** (all required args filled)
//! 3. **Noun confidence** (average confidence of recognized arguments)
//! 4. **Specificity** (prefer verbs with more matched arguments)
//!
//! In dialog-query terms, scoring is a Formula — a pure computation
//! that takes candidate attributes and produces a score:
//!
//! ```rust,ignore
//! #[derive(Formula)]
//! pub struct ScoreCandidate {
//!     pub verb_match_score: f64,
//!     pub completeness: f64,
//!     pub noun_confidence: f64,
//!     pub specificity: f64,
//!     #[derived]
//!     pub total_score: f64,
//! }
//!
//! impl ScoreCandidate {
//!     fn derive(input: Input<Self>) -> Vec<Self> {
//!         let total = input.verb_match_score * 1000.0
//!             + input.completeness * 100.0
//!             + input.noun_confidence * 10.0
//!             + input.specificity;
//!         vec![ScoreCandidate { total_score: total, ..input }]
//!     }
//! }
//! ```

/// A confidence score in the range [0.0, 1.0].
#[derive(Debug, Clone, Copy, PartialEq, PartialOrd)]
pub struct Confidence(pub f64);

impl Confidence {
    pub fn new(value: f64) -> Self {
        Confidence(value.clamp(0.0, 1.0))
    }
}

/// Composite score for a parse candidate.
///
/// Uses weighted lexicographic ordering: verb match quality dominates,
/// then completeness, then noun confidence, then specificity.
#[derive(Debug, Clone, PartialEq)]
pub struct CandidateScore {
    /// How well the verb name matched (0.0–1.0).
    pub verb_match: f64,
    /// Fraction of required arguments that were filled (0.0–1.0).
    pub completeness: f64,
    /// Average confidence of noun recognitions (0.0–1.0).
    pub noun_confidence: f64,
    /// Number of arguments matched / total arguments.
    pub specificity: f64,
}

impl CandidateScore {
    /// Compute a single total score for ordering.
    ///
    /// The weighting ensures lexicographic dominance: a better verb match
    /// always beats better noun confidence, regardless of magnitude.
    pub fn total(&self) -> f64 {
        self.verb_match * 1000.0
            + self.completeness * 100.0
            + self.noun_confidence * 10.0
            + self.specificity
    }
}

impl PartialOrd for CandidateScore {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.total().partial_cmp(&other.total())
    }
}

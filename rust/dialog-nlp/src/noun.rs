//! Noun types as derivation rules.
//!
//! A noun type is not a passive type tag — it is a **set of derivation rules**
//! that actively recognize and extract typed values from text fragments. In the
//! dialog-query model:
//!
//! - A noun type is a `Concept` in the store (facts about its label, description)
//! - Its recognizers are `DeductiveRule`s installed in the session
//! - Recognition produces `NounMatch` facts with typed values and confidence
//!
//! ## How nouns are rules
//!
//! Consider a "language" noun type. When registered, it installs rules like:
//!
//! ```rust,ignore
//! // Rule: recognize_language
//! // Derives: Language concept from text that matches known language names
//! fn recognize_language(lang: Match<Language>) -> impl When {
//!     (
//!         // Premise: there's a text segment to analyze
//!         Match::<Segment> {
//!             this: Term::var("segment"),
//!             text: lang.name.clone(),  // join: segment text = language name
//!         },
//!         // Premise: the text matches a known language entry
//!         Match::<KnownLanguage> {
//!             this: Term::var("entry"),
//!             name: lang.name.clone(),
//!             code: lang.code.clone(),
//!         },
//!     )
//! }
//! ```
//!
//! The known language entries (`KnownLanguage`) are themselves facts in the
//! store — a lookup table that can be extended by asserting new facts:
//!
//! ```rust,ignore
//! tx.assert(KnownLanguage { this: e1, name: "spanish", code: "es" });
//! tx.assert(KnownLanguage { this: e2, name: "french", code: "fr" });
//! tx.assert(KnownLanguage { this: e3, name: "español", code: "es" });  // alias
//! ```
//!
//! ## Selection and input as nouns
//!
//! Both the user's typed input and the current selection are facts. A noun
//! recognizer can operate on either. The "text" noun type is the simplest:
//! it recognizes any text fragment as a text value. The selection provides
//! an implicit text argument when the user says "this" or omits the object.

use crate::score::Confidence;
use std::fmt;

/// A registered noun type.
///
/// In the full dialog-query integration, this would be a `Concept`:
/// ```rust,ignore
/// #[derive(Concept)]
/// pub struct NounType {
///     this: Entity,
///     label: noun_type::Label,
///     description: noun_type::Description,
/// }
/// ```
///
/// Here we define the in-memory representation used during parsing.
#[derive(Debug, Clone)]
pub struct NounType {
    /// Unique identifier for this noun type (e.g., "language", "contact", "url").
    pub label: String,
    /// Human-readable description.
    pub description: String,
    /// The recognizer that can extract values of this type from text.
    /// In dialog-query, this would be one or more installed DeductiveRules.
    pub recognizer: Recognizer,
}

/// A recognizer extracts typed values from text fragments.
///
/// In the dialog-query model, each variant corresponds to a different kind
/// of derivation rule:
///
/// - `Lookup`: a rule that joins against a table of known values (facts)
/// - `Pattern`: a rule that uses a Formula for regex/pattern matching
/// - `External`: a rule whose premises include an async external query
/// - `Passthrough`: the trivial recognizer — any text is a valid value
#[derive(Debug, Clone)]
pub enum Recognizer {
    /// Match against a set of known values stored as facts.
    /// This is the most common pattern: a concept like `KnownLanguage`
    /// with `name` and `code` attributes. The recognizer rule joins
    /// the text segment against the `name` attribute.
    ///
    /// ```rust,ignore
    /// // Conceptually:
    /// fn recognize(output: Match<NounMatch>) -> impl When {
    ///     (Match::<Segment> { text: Term::var("text"), .. },
    ///      Match::<KnownValue> { name: Term::var("text"), value: Term::var("v"), .. })
    /// }
    /// ```
    Lookup {
        /// The set of known `(name, value)` pairs.
        /// In dialog-query these would be queried from facts.
        entries: Vec<LookupEntry>,
    },

    /// Match using a pattern (regex or similar).
    /// In dialog-query, this would be a Formula-based rule.
    ///
    /// ```rust,ignore
    /// #[derive(Formula)]
    /// pub struct MatchUrl {
    ///     pub text: String,
    ///     #[derived]
    ///     pub url: String,
    ///     #[derived]
    ///     pub confidence: f64,
    /// }
    /// ```
    Pattern {
        /// Description of the pattern for debugging.
        description: String,
        /// The matching function. In production this would be a Formula.
        matcher: fn(&str) -> Option<(String, Confidence)>,
    },

    /// Any text is accepted as a valid value of this noun type.
    /// Used for free-text arguments like search queries.
    Passthrough,
}

/// A single entry in a lookup-based recognizer.
#[derive(Debug, Clone)]
pub struct LookupEntry {
    /// The text that triggers this entry (e.g., "spanish", "español").
    pub name: String,
    /// The canonical value produced (e.g., "es").
    pub value: String,
}

/// The result of a noun recognizer matching a text fragment.
///
/// In dialog-query terms, this would be a `Concept` derived by a rule:
/// ```rust,ignore
/// #[derive(Concept)]
/// pub struct NounMatch {
///     this: Entity,
///     noun_type: noun_match::NounTypeLabel,  // which noun type matched
///     input_text: noun_match::InputText,      // what text was recognized
///     value: noun_match::Value,               // the extracted typed value
///     confidence: noun_match::Confidence,     // how confident the match is
/// }
/// ```
#[derive(Debug, Clone)]
pub struct NounMatch {
    /// Which noun type produced this match.
    pub noun_type: String,
    /// The original text fragment that was recognized.
    pub input_text: String,
    /// The extracted/normalized value.
    pub value: String,
    /// How confident the recognizer is in this match (0.0–1.0).
    pub confidence: Confidence,
}

impl fmt::Display for NounMatch {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}({:?} → {:?}, conf={:.2})",
            self.noun_type, self.input_text, self.value, self.confidence.0
        )
    }
}

impl NounType {
    /// Create a new lookup-based noun type.
    pub fn lookup(
        label: impl Into<String>,
        description: impl Into<String>,
        entries: Vec<LookupEntry>,
    ) -> Self {
        NounType {
            label: label.into(),
            description: description.into(),
            recognizer: Recognizer::Lookup { entries },
        }
    }

    /// Create a passthrough noun type (accepts any text).
    pub fn passthrough(label: impl Into<String>, description: impl Into<String>) -> Self {
        NounType {
            label: label.into(),
            description: description.into(),
            recognizer: Recognizer::Passthrough,
        }
    }

    /// Try to recognize a text fragment as this noun type.
    ///
    /// Returns all possible matches with confidence scores. A lookup
    /// recognizer may return multiple matches if the text is ambiguous.
    ///
    /// In the dialog-query model, this is not a method call — it's the
    /// result of evaluating the recognizer's derivation rules against
    /// the text segment fact. We model it as a method here for the sketch.
    pub fn recognize(&self, text: &str) -> Vec<NounMatch> {
        let normalized = text.to_lowercase();
        match &self.recognizer {
            Recognizer::Lookup { entries } => {
                let mut matches = Vec::new();
                for entry in entries {
                    let entry_lower = entry.name.to_lowercase();
                    if entry_lower == normalized {
                        // Exact match
                        matches.push(NounMatch {
                            noun_type: self.label.clone(),
                            input_text: text.to_string(),
                            value: entry.value.clone(),
                            confidence: Confidence(1.0),
                        });
                    } else if entry_lower.starts_with(&normalized) {
                        // Prefix match
                        matches.push(NounMatch {
                            noun_type: self.label.clone(),
                            input_text: text.to_string(),
                            value: entry.value.clone(),
                            confidence: Confidence(0.7),
                        });
                    }
                }
                matches
            }
            Recognizer::Pattern { matcher, .. } => {
                if let Some((value, confidence)) = matcher(text) {
                    vec![NounMatch {
                        noun_type: self.label.clone(),
                        input_text: text.to_string(),
                        value,
                        confidence,
                    }]
                } else {
                    vec![]
                }
            }
            Recognizer::Passthrough => {
                vec![NounMatch {
                    noun_type: self.label.clone(),
                    input_text: text.to_string(),
                    value: text.to_string(),
                    confidence: Confidence(0.5),
                }]
            }
        }
    }
}

/// Built-in noun type: free text (accepts anything).
pub fn noun_type_text() -> NounType {
    NounType::passthrough("text", "Arbitrary text input")
}

/// Built-in noun type: language names/codes.
pub fn noun_type_language() -> NounType {
    NounType::lookup(
        "language",
        "A human language",
        vec![
            LookupEntry { name: "english".into(), value: "en".into() },
            LookupEntry { name: "spanish".into(), value: "es".into() },
            LookupEntry { name: "french".into(), value: "fr".into() },
            LookupEntry { name: "german".into(), value: "de".into() },
            LookupEntry { name: "japanese".into(), value: "ja".into() },
            LookupEntry { name: "chinese".into(), value: "zh".into() },
            LookupEntry { name: "portuguese".into(), value: "pt".into() },
            LookupEntry { name: "italian".into(), value: "it".into() },
            LookupEntry { name: "korean".into(), value: "ko".into() },
            LookupEntry { name: "russian".into(), value: "ru".into() },
        ],
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lookup_exact_match() {
        let lang = noun_type_language();
        let matches = lang.recognize("spanish");
        assert_eq!(matches.len(), 1);
        assert_eq!(matches[0].value, "es");
        assert_eq!(matches[0].confidence.0, 1.0);
    }

    #[test]
    fn lookup_prefix_match() {
        let lang = noun_type_language();
        let matches = lang.recognize("span");
        assert_eq!(matches.len(), 1);
        assert_eq!(matches[0].value, "es");
        assert!(matches[0].confidence.0 < 1.0);
    }

    #[test]
    fn passthrough_accepts_anything() {
        let text = noun_type_text();
        let matches = text.recognize("anything at all");
        assert_eq!(matches.len(), 1);
        assert_eq!(matches[0].value, "anything at all");
    }
}

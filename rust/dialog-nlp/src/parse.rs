//! The parser pipeline — from raw text to ranked candidates.
//!
//! The parser is a cascade of stages, each deriving new facts from the
//! previous stage's output. In dialog-query, each stage would be a set
//! of derivation rules. Here we implement the pipeline as a function
//! that composes the stages explicitly.
//!
//! ## Pipeline as a rule cascade (dialog-query vision)
//!
//! In a full dialog-query integration, the parser would be expressed as
//! installed rules, not procedural code. The pipeline becomes:
//!
//! ```text
//! // Stage 1: Tokenize (Formula)
//! Rule: tokenize
//!   Given: Input { text: ?text }
//!   Then:  Token { value: ?word, position: ?pos, kind: ?kind }
//!
//! // Stage 2: Verb match (DeductiveRule)
//! Rule: match_verb
//!   Given: Token { value: ?name, position: ?pos }
//!          Verb { name: ?name }  // join against registered verbs
//!   Then:  VerbMatch { verb: ?name, quality: exact, position: ?pos }
//!
//! // Stage 3: Segment (DeductiveRule)
//! Rule: segment_by_marker
//!   Given: Token { value: ?prep, position: ?pos }
//!          RoleMarker { word: ?prep, role: ?role }
//!   Then:  Segment { role: ?role, start: ?pos + 1 }
//!
//! // Stage 4: Noun resolution (DeductiveRule per noun type)
//! Rule: recognize_language
//!   Given: Segment { text: ?text, role: ?role }
//!          KnownLanguage { name: ?text, code: ?code }
//!   Then:  NounMatch { noun_type: "language", value: ?code, confidence: 1.0 }
//!
//! // Stage 5: Candidate assembly (DeductiveRule)
//! Rule: assemble_candidate
//!   Given: VerbMatch { verb: ?v, quality: ?q }
//!          NounMatch { role: ?role, value: ?val }  // for each argument
//!   Then:  Candidate { verb: ?v, ... }
//!
//! // Stage 6: Scoring (Formula)
//! Formula: score_candidate
//!   Given: Candidate { verb_match_quality: ?q, completeness: ?c, ... }
//!   Then:  RankedCandidate { score: weighted_sum(?q, ?c, ...) }
//! ```
//!
//! The session would discover available verbs and noun types by querying
//! the fact store, and the installed rules would handle the derivation
//! chain automatically.

use std::collections::HashMap;

use crate::error::NlpError;
use crate::input::Input;
use crate::noun::{NounMatch, NounType};
use crate::role::{RoleMarker, SemanticRole};
use crate::score::{CandidateScore, Confidence};
use crate::segment::{segment_tokens, Segment};
use crate::sentence::{Candidate, ParseResult};
use crate::token::tokenize;
use crate::verb::{Verb, VerbMatch};

/// The parser — holds registered verbs, noun types, and role markers.
///
/// In the full dialog-query integration, this would not exist as a separate
/// struct. Instead, verbs and noun types would be facts in the session's
/// store, and the parser pipeline would be installed rules. The parser
/// would be a query: "give me all RankedCandidates for this input."
///
/// We model it explicitly here to sketch the algorithm.
pub struct Parser {
    verbs: Vec<Verb>,
    noun_types: HashMap<String, NounType>,
    role_markers: Vec<RoleMarker>,
}

impl Parser {
    pub fn new() -> Self {
        Parser {
            verbs: Vec::new(),
            noun_types: HashMap::new(),
            role_markers: Vec::new(),
        }
    }

    /// Register a verb. In dialog-query: assert Verb concept facts.
    pub fn register_verb(&mut self, verb: Verb) {
        self.verbs.push(verb);
    }

    /// Register a noun type. In dialog-query: assert NounType facts + install recognizer rules.
    pub fn register_noun_type(&mut self, noun_type: NounType) {
        self.noun_types
            .insert(noun_type.label.clone(), noun_type);
    }

    /// Register role markers. In dialog-query: assert RoleMarker facts.
    pub fn register_role_markers(&mut self, markers: Vec<RoleMarker>) {
        self.role_markers.extend(markers);
    }

    /// Parse input text into ranked candidates.
    ///
    /// This is the main entry point. In dialog-query, this would be:
    /// ```rust,ignore
    /// // Assert the input
    /// tx.assert(With { this: session_entity, has: input::Text(text) });
    /// session.commit(tx).await?;
    ///
    /// // Query for ranked candidates (rules cascade automatically)
    /// let candidates = Match::<RankedCandidate> {
    ///     this: Term::var("candidate"),
    ///     verb: Term::var("verb"),
    ///     score: Term::var("score"),
    ///     // ...
    /// }.query(&session).try_vec().await?;
    /// ```
    pub fn parse(&self, input: &Input) -> Result<ParseResult, NlpError> {
        // Stage 1: Tokenize
        let tokens = tokenize(&input.text);
        if tokens.is_empty() {
            return Err(NlpError::NoCandidates);
        }

        // Stage 2: Verb recognition — try each token against each verb
        let verb_matches = self.recognize_verbs(&tokens);
        if verb_matches.is_empty() {
            return Err(NlpError::NoVerbMatch);
        }

        // For each verb match, run stages 3-6
        let mut candidates = Vec::new();
        for (verb, verb_match) in &verb_matches {
            // Stage 3: Segment remaining tokens by role markers
            let segments =
                segment_tokens(&tokens, verb_match.token_position, &self.role_markers);

            // Stage 4: Noun resolution — apply recognizers to each segment
            let resolved = self.resolve_nouns(verb, &segments, input);

            // Stage 5+6: Assemble and score candidate
            let score = self.score_candidate(verb, &verb_match, &resolved);

            candidates.push(Candidate {
                verb_match: verb_match.clone(),
                arguments: resolved,
                score,
            });
        }

        // Sort by score, highest first
        candidates.sort_by(|a, b| {
            b.score
                .total()
                .partial_cmp(&a.score.total())
                .unwrap_or(std::cmp::Ordering::Equal)
        });

        Ok(ParseResult { candidates })
    }

    /// Stage 2: Try to match each token against registered verbs.
    ///
    /// In dialog-query, this is a rule that joins Token facts against Verb
    /// concept facts.
    fn recognize_verbs<'a>(
        &'a self,
        tokens: &[crate::token::Token],
    ) -> Vec<(&'a Verb, VerbMatch)> {
        let mut matches = Vec::new();
        // Ubiquity assumes verb is the first token, but we check all
        // positions for flexibility (with preference for position 0).
        for token in tokens {
            for verb in &self.verbs {
                if let Some(verb_match) = verb.match_token(&token.value, token.position) {
                    matches.push((verb, verb_match));
                }
            }
        }
        matches
    }

    /// Stage 4: Apply noun recognizers to segments.
    ///
    /// For each segment, look up the verb's expected noun type for that role,
    /// then apply the corresponding recognizer. If no specific noun type is
    /// expected, try all noun types and pick the best match.
    ///
    /// If the segment text is "this" and there's a selection, use the selection.
    fn resolve_nouns(
        &self,
        verb: &Verb,
        segments: &[Segment],
        input: &Input,
    ) -> HashMap<SemanticRole, NounMatch> {
        let mut resolved = HashMap::new();

        for segment in segments {
            // Resolve "this" to the selection if available
            let text = if segment.text.to_lowercase() == "this" {
                if let Some(ref sel) = input.selection {
                    sel.as_str()
                } else {
                    &segment.text
                }
            } else {
                &segment.text
            };

            // Find expected noun type for this role
            let expected_noun_type = verb
                .argument_for_role(&segment.role)
                .map(|arg| arg.noun_type.as_str());

            let noun_match = if let Some(expected) = expected_noun_type {
                // Try the expected noun type first
                if let Some(nt) = self.noun_types.get(expected) {
                    let matches = nt.recognize(text);
                    matches.into_iter().max_by(|a, b| {
                        a.confidence
                            .partial_cmp(&b.confidence)
                            .unwrap_or(std::cmp::Ordering::Equal)
                    })
                } else {
                    // Fall back to passthrough
                    Some(NounMatch {
                        noun_type: "text".to_string(),
                        input_text: text.to_string(),
                        value: text.to_string(),
                        confidence: Confidence(0.3),
                    })
                }
            } else {
                // No expected type — try all noun types, pick best
                self.try_all_noun_types(text)
            };

            if let Some(m) = noun_match {
                resolved.insert(segment.role.clone(), m);
            }
        }

        // If the verb has an object argument but no object was resolved,
        // and there's a selection, try to fill from selection.
        if !resolved.contains_key(&SemanticRole::Object) {
            if let Some(ref selection) = input.selection {
                if let Some(arg) = verb.argument_for_role(&SemanticRole::Object) {
                    if let Some(nt) = self.noun_types.get(&arg.noun_type) {
                        if let Some(m) = nt.recognize(selection).into_iter().next() {
                            resolved.insert(SemanticRole::Object, m);
                        }
                    }
                }
            }
        }

        resolved
    }

    /// Try all registered noun types against text, return best match.
    fn try_all_noun_types(&self, text: &str) -> Option<NounMatch> {
        let mut best: Option<NounMatch> = None;
        for nt in self.noun_types.values() {
            for m in nt.recognize(text) {
                if best
                    .as_ref()
                    .is_none_or(|b| m.confidence > b.confidence)
                {
                    best = Some(m);
                }
            }
        }
        best
    }

    /// Stages 5+6: Score a candidate.
    fn score_candidate(
        &self,
        verb: &Verb,
        verb_match: &VerbMatch,
        resolved: &HashMap<SemanticRole, NounMatch>,
    ) -> CandidateScore {
        // Verb match quality
        let verb_score = verb_match.quality.score();

        // Position bonus: verbs at position 0 (natural command position) get a boost
        let position_bonus = if verb_match.token_position == 0 {
            1.0
        } else {
            0.5
        };

        // Completeness: fraction of required args filled
        let required: Vec<_> = verb.required_arguments().collect();
        let required_filled = required
            .iter()
            .filter(|a| resolved.contains_key(&a.role))
            .count();
        let completeness = if required.is_empty() {
            1.0
        } else {
            required_filled as f64 / required.len() as f64
        };

        // Average noun confidence
        let noun_confidence = if resolved.is_empty() {
            0.0
        } else {
            resolved.values().map(|m| m.confidence.0).sum::<f64>() / resolved.len() as f64
        };

        // Specificity: matched args / total verb args
        let specificity = if verb.arguments.is_empty() {
            0.0
        } else {
            resolved.len() as f64 / verb.arguments.len() as f64
        };

        CandidateScore {
            verb_match: verb_score * position_bonus,
            completeness,
            noun_confidence,
            specificity,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::noun::{noun_type_language, noun_type_text};
    use crate::role::english_role_markers;
    use crate::verb::{verb_search, verb_translate, MatchQuality};

    fn test_parser() -> Parser {
        let mut parser = Parser::new();
        parser.register_verb(verb_translate());
        parser.register_verb(verb_search());
        parser.register_noun_type(noun_type_language());
        parser.register_noun_type(noun_type_text());
        parser.register_role_markers(english_role_markers());
        parser
    }

    #[test]
    fn parse_translate_command() {
        let parser = test_parser();
        let input = Input::new("translate hello to spanish");
        let result = parser.parse(&input).unwrap();

        let best = result.best().unwrap();
        assert_eq!(best.verb_name(), "translate");
        assert_eq!(
            best.argument_value(&SemanticRole::Object),
            Some("hello")
        );
        assert_eq!(
            best.argument_value(&SemanticRole::Goal),
            Some("es")
        );
    }

    #[test]
    fn parse_translate_with_source() {
        let parser = test_parser();
        let input = Input::new("translate hello to spanish from english");
        let result = parser.parse(&input).unwrap();

        let best = result.best().unwrap();
        assert_eq!(best.verb_name(), "translate");
        assert_eq!(best.argument_value(&SemanticRole::Goal), Some("es"));
        assert_eq!(best.argument_value(&SemanticRole::Source), Some("en"));
    }

    #[test]
    fn parse_search_command() {
        let parser = test_parser();
        let input = Input::new("search cats and dogs");
        let result = parser.parse(&input).unwrap();

        let best = result.best().unwrap();
        assert_eq!(best.verb_name(), "search");
        assert_eq!(
            best.argument_value(&SemanticRole::Object),
            Some("cats and dogs")
        );
    }

    #[test]
    fn parse_with_selection() {
        let parser = test_parser();
        let input = Input::new("translate this to spanish")
            .with_selection("bonjour le monde");
        let result = parser.parse(&input).unwrap();

        let best = result.best().unwrap();
        assert_eq!(best.verb_name(), "translate");
        // "this" should resolve to the selection
        assert_eq!(
            best.argument_value(&SemanticRole::Object),
            Some("bonjour le monde")
        );
    }

    #[test]
    fn parse_verb_prefix() {
        let parser = test_parser();
        // "trans" is an alias for translate, should match exactly
        let input = Input::new("trans hello to french");
        let result = parser.parse(&input).unwrap();

        let best = result.best().unwrap();
        assert_eq!(best.verb_name(), "translate");
    }

    #[test]
    fn parse_no_verb_match() {
        let parser = test_parser();
        let input = Input::new("xyzzy hello");
        let result = parser.parse(&input);
        assert!(matches!(result, Err(NlpError::NoVerbMatch)));
    }

    #[test]
    fn parse_ambiguous_prefers_exact() {
        let parser = test_parser();
        // "find" is an alias for search
        let input = Input::new("find cats");
        let result = parser.parse(&input).unwrap();

        let best = result.best().unwrap();
        assert_eq!(best.verb_name(), "search");
        assert_eq!(best.verb_match.quality, MatchQuality::Exact);
    }
}

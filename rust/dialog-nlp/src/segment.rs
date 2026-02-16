//! Argument segmentation — splitting tokens into role-tagged segments.
//!
//! After verb recognition, the remaining tokens are split into argument
//! segments based on preposition/role markers. Each segment is a contiguous
//! span of tokens assigned to a semantic role.
//!
//! In dialog-query terms, segmentation is a rule that derives `Segment`
//! concepts from `Token` facts and `RoleMarker` facts:
//!
//! ```rust,ignore
//! // Rule: segment_by_preposition
//! fn segment_by_preposition(seg: Match<Segment>) -> impl When {
//!     (
//!         Match::<Token> { position: seg.start_position.clone(), value: Term::var("prep") },
//!         Match::<RoleMarker> { word: Term::var("prep"), role: seg.role.clone() },
//!         // ... subsequent tokens until next preposition
//!     )
//! }
//! ```

use crate::role::{RoleMarker, SemanticRole};
use crate::token::Token;

/// A segment of tokens assigned to a semantic role.
///
/// ```rust,ignore
/// // As a dialog-query Concept:
/// #[derive(Concept)]
/// pub struct Segment {
///     this: Entity,
///     role: segment::Role,
///     text: segment::Text,          // the joined token text
///     start_position: segment::Start,
///     end_position: segment::End,
/// }
/// ```
#[derive(Debug, Clone)]
pub struct Segment {
    /// The semantic role assigned to this segment.
    pub role: SemanticRole,
    /// The text content of the segment (tokens joined by spaces).
    pub text: String,
    /// Token positions included in this segment.
    pub token_range: std::ops::Range<usize>,
}

/// Segment tokens into role-tagged spans.
///
/// Algorithm:
/// 1. The first token after the verb (if no preposition) becomes the Object.
/// 2. When a preposition token is encountered, look up its role.
/// 3. Subsequent tokens until the next preposition belong to that role.
///
/// In dialog-query, this entire function would be a set of derivation rules
/// operating on Token and RoleMarker facts.
pub fn segment_tokens(
    tokens: &[Token],
    verb_position: usize,
    role_markers: &[RoleMarker],
) -> Vec<Segment> {
    let mut segments = Vec::new();
    let remaining: Vec<&Token> = tokens
        .iter()
        .filter(|t| t.position != verb_position)
        .collect();

    if remaining.is_empty() {
        return segments;
    }

    let mut current_role = SemanticRole::Object;
    let mut current_tokens: Vec<&Token> = Vec::new();
    let mut segment_start = remaining.first().map(|t| t.position).unwrap_or(0);

    for token in &remaining {
        // Check if this token is a role marker (preposition)
        if let Some(marker) = role_markers
            .iter()
            .find(|m| m.word.to_lowercase() == token.value)
        {
            // Flush the current segment
            if !current_tokens.is_empty() {
                let text = current_tokens
                    .iter()
                    .map(|t| t.original.as_str())
                    .collect::<Vec<_>>()
                    .join(" ");
                let end = current_tokens.last().unwrap().position + 1;
                segments.push(Segment {
                    role: current_role.clone(),
                    text,
                    token_range: segment_start..end,
                });
                current_tokens.clear();
            }
            // Start a new segment with the preposition's role
            current_role = marker.role.clone();
            segment_start = token.position + 1;
        } else {
            current_tokens.push(token);
        }
    }

    // Flush the last segment
    if !current_tokens.is_empty() {
        let text = current_tokens
            .iter()
            .map(|t| t.original.as_str())
            .collect::<Vec<_>>()
            .join(" ");
        let end = current_tokens.last().unwrap().position + 1;
        segments.push(Segment {
            role: current_role,
            text,
            token_range: segment_start..end,
        });
    }

    segments
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::role::english_role_markers;
    use crate::token::tokenize;

    #[test]
    fn segment_translate_command() {
        let tokens = tokenize("translate hello world to spanish from english");
        let markers = english_role_markers();
        let segments = segment_tokens(&tokens, 0, &markers);

        assert_eq!(segments.len(), 3);

        assert_eq!(segments[0].role, SemanticRole::Object);
        assert_eq!(segments[0].text, "hello world");

        assert_eq!(segments[1].role, SemanticRole::Goal);
        assert_eq!(segments[1].text, "spanish");

        assert_eq!(segments[2].role, SemanticRole::Source);
        assert_eq!(segments[2].text, "english");
    }

    #[test]
    fn segment_object_only() {
        let tokens = tokenize("search cats and dogs");
        let markers = english_role_markers();
        let segments = segment_tokens(&tokens, 0, &markers);

        assert_eq!(segments.len(), 1);
        assert_eq!(segments[0].role, SemanticRole::Object);
        assert_eq!(segments[0].text, "cats and dogs");
    }

    #[test]
    fn segment_with_location() {
        let tokens = tokenize("search cats in images");
        let markers = english_role_markers();
        let segments = segment_tokens(&tokens, 0, &markers);

        assert_eq!(segments.len(), 2);
        assert_eq!(segments[0].role, SemanticRole::Object);
        assert_eq!(segments[0].text, "cats");
        assert_eq!(segments[1].role, SemanticRole::Location);
        assert_eq!(segments[1].text, "images");
    }
}

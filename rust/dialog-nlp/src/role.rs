//! Semantic roles for verb arguments.
//!
//! Ubiquity's Parser 2 used abstract semantic roles to categorize argument
//! types independently of language-specific syntax. We adopt the same approach:
//! roles are first-class values stored as facts, and the mapping from
//! prepositions/postpositions to roles is itself a queryable fact set.
//!
//! This means localization is achieved by asserting different preposition→role
//! mappings for different locales, without changing any parser logic.
//!
//! ## Roles as facts
//!
//! ```rust,ignore
//! // In dialog-query terms:
//! mod role_marker {
//!     #[derive(Attribute, Clone)]
//!     pub struct Word(pub String);        // "to", "from", "in", ...
//!
//!     #[derive(Attribute, Clone)]
//!     pub struct Role(pub String);        // "goal", "source", "location", ...
//!
//!     #[derive(Attribute, Clone)]
//!     pub struct Locale(pub String);      // "en", "es", "ja", ...
//! }
//!
//! #[derive(Concept)]
//! pub struct RoleMarker {
//!     this: Entity,
//!     word: role_marker::Word,
//!     role: role_marker::Role,
//!     locale: role_marker::Locale,
//! }
//!
//! // Assert English preposition→role mappings as facts:
//! tx.assert(RoleMarker { this: e1, word: "to".into(), role: "goal".into(), locale: "en".into() });
//! tx.assert(RoleMarker { this: e2, word: "from".into(), role: "source".into(), locale: "en".into() });
//! tx.assert(RoleMarker { this: e3, word: "in".into(), role: "location".into(), locale: "en".into() });
//! tx.assert(RoleMarker { this: e4, word: "with".into(), role: "instrument".into(), locale: "en".into() });
//! tx.assert(RoleMarker { this: e5, word: "on".into(), role: "time".into(), locale: "en".into() });
//! ```

use std::fmt;

/// The semantic role of an argument in a verb's parameter list.
///
/// Roles abstract away language-specific syntax (prepositions, word order)
/// into universal categories. The parser segments input by recognizing
/// role markers (prepositions in English) and assigning segments to roles.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum SemanticRole {
    /// The direct object — the default/primary argument.
    /// In "translate hello", "hello" is the object.
    /// No preposition required; determined by position.
    Object,

    /// The destination or target.
    /// Marked by: "to", "into", "toward" (en)
    /// In "translate hello to spanish", "spanish" is the goal.
    Goal,

    /// The origin or starting point.
    /// Marked by: "from", "by" (en)
    /// In "translate hello from english", "english" is the source.
    Source,

    /// A physical or logical location.
    /// Marked by: "in", "at", "near" (en)
    /// In "search cats in images", "images" is the location.
    Location,

    /// A tool or means used to perform the action.
    /// Marked by: "with", "using" (en)
    /// In "open file with editor", "editor" is the instrument.
    Instrument,

    /// A temporal reference.
    /// Marked by: "at", "on", "before", "after" (en)
    /// In "remind me at 5pm", "5pm" is the time.
    Time,

    /// An argument of the noun rather than the verb.
    /// Used for modifiers that refine another argument.
    Modifier,
}

impl fmt::Display for SemanticRole {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Object => write!(f, "object"),
            Self::Goal => write!(f, "goal"),
            Self::Source => write!(f, "source"),
            Self::Location => write!(f, "location"),
            Self::Instrument => write!(f, "instrument"),
            Self::Time => write!(f, "time"),
            Self::Modifier => write!(f, "modifier"),
        }
    }
}

/// A mapping from a marker word (preposition) to a semantic role, scoped
/// by locale. In the full system these are facts in the store; here we
/// define the in-memory representation.
#[derive(Debug, Clone)]
pub struct RoleMarker {
    pub word: String,
    pub role: SemanticRole,
    pub locale: String,
}

/// Default English preposition→role mappings.
pub fn english_role_markers() -> Vec<RoleMarker> {
    vec![
        RoleMarker { word: "to".into(), role: SemanticRole::Goal, locale: "en".into() },
        RoleMarker { word: "into".into(), role: SemanticRole::Goal, locale: "en".into() },
        RoleMarker { word: "toward".into(), role: SemanticRole::Goal, locale: "en".into() },
        RoleMarker { word: "from".into(), role: SemanticRole::Source, locale: "en".into() },
        RoleMarker { word: "by".into(), role: SemanticRole::Source, locale: "en".into() },
        RoleMarker { word: "in".into(), role: SemanticRole::Location, locale: "en".into() },
        RoleMarker { word: "at".into(), role: SemanticRole::Location, locale: "en".into() },
        RoleMarker { word: "near".into(), role: SemanticRole::Location, locale: "en".into() },
        RoleMarker { word: "on".into(), role: SemanticRole::Location, locale: "en".into() },
        RoleMarker { word: "with".into(), role: SemanticRole::Instrument, locale: "en".into() },
        RoleMarker { word: "using".into(), role: SemanticRole::Instrument, locale: "en".into() },
        RoleMarker { word: "before".into(), role: SemanticRole::Time, locale: "en".into() },
        RoleMarker { word: "after".into(), role: SemanticRole::Time, locale: "en".into() },
    ]
}

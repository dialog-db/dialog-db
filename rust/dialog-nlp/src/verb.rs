//! Verbs as effectful concepts.
//!
//! A verb is a command whose schema (name, arguments, roles) is stored as
//! facts in the dialog store, and whose execution produces an effect. This
//! separates "what commands exist" (queryable data) from "what commands do"
//! (installed handlers).
//!
//! ## Verbs as dialog-query Concepts
//!
//! ```rust,ignore
//! // The verb itself is a concept:
//! mod verb {
//!     #[derive(Attribute, Clone)]
//!     pub struct Name(pub String);
//!
//!     #[derive(Attribute, Clone)]
//!     pub struct Description(pub String);
//! }
//!
//! #[derive(Concept)]
//! pub struct Verb {
//!     this: Entity,
//!     name: verb::Name,
//!     description: verb::Description,
//! }
//!
//! // Each argument slot is a separate entity with attributes:
//! mod argument {
//!     #[derive(Attribute, Clone)]
//!     pub struct VerbRef(pub Entity);    // which verb this belongs to
//!
//!     #[derive(Attribute, Clone)]
//!     pub struct Role(pub String);        // semantic role
//!
//!     #[derive(Attribute, Clone)]
//!     pub struct NounTypeRef(pub Entity); // expected noun type
//!
//!     #[derive(Attribute, Clone)]
//!     pub struct Required(pub bool);
//!
//!     #[derive(Attribute, Clone)]
//!     #[cardinality(many)]
//!     pub struct Preposition(pub String); // marker words for this role
//! }
//!
//! #[derive(Concept)]
//! pub struct ArgumentSlot {
//!     this: Entity,
//!     verb: argument::VerbRef,
//!     role: argument::Role,
//!     noun_type: argument::NounTypeRef,
//!     required: argument::Required,
//! }
//! ```
//!
//! ## Effectful Rules
//!
//! A verb's handler is the effectful part. In dialog-query, rules are purely
//! deductive — they derive new facts. Verb handlers extend this with an
//! effect layer: when a parsed sentence's verb is resolved, the handler
//! receives the resolved arguments and returns an `Effect` descriptor.
//!
//! The effect is then executed through dialog-db's capability system,
//! which gates execution on the caller's capabilities. This keeps the
//! parser pure and the execution controlled.
//!
//! ```rust,ignore
//! // A verb handler is an effectful rule:
//! // When:  ParsedSentence { verb: "translate", object: ?text, goal: ?lang }
//! // Then:  Effect::Translate { text: ?text, target: ?lang }
//!
//! // In Rust:
//! session.install_verb("translate", |args: ResolvedArguments| {
//!     let text = args.get(SemanticRole::Object)?;
//!     let target = args.get(SemanticRole::Goal)?;
//!     Ok(Effect::custom("translate", vec![
//!         ("text", text),
//!         ("target_language", target),
//!     ]))
//! });
//! ```

use crate::role::SemanticRole;
use std::collections::HashMap;

/// A registered verb — a command the user can invoke.
///
/// The verb's schema is data: its name, description, argument slots, and
/// aliases. In dialog-query, all of these are facts. The handler is the
/// only non-data part — it's a function installed in the session.
#[derive(Debug, Clone)]
pub struct Verb {
    /// The primary trigger name (e.g., "translate", "email", "search").
    pub name: String,
    /// Human-readable description of what this verb does.
    pub description: String,
    /// Alternative names that also trigger this verb.
    /// In dialog-query, these would be additional `verb::Alias` facts.
    pub aliases: Vec<String>,
    /// The argument slots this verb accepts.
    pub arguments: Vec<ArgumentSlot>,
}

/// An argument slot in a verb's parameter list.
///
/// Each slot has a semantic role, an expected noun type, and whether it's
/// required. The slot's prepositions (stored as facts in dialog-query) are
/// used by the parser to segment input into arguments.
#[derive(Debug, Clone)]
pub struct ArgumentSlot {
    /// The semantic role of this argument (object, goal, source, ...).
    pub role: SemanticRole,
    /// The label of the expected noun type.
    /// In dialog-query, this would be an entity reference to the noun type.
    pub noun_type: String,
    /// Whether this argument is required for the verb to execute.
    pub required: bool,
}

/// The result of resolving a verb's arguments from parsed input.
///
/// Maps semantic roles to their resolved string values. In a full
/// dialog-query integration, these would be typed `Value`s with
/// provenance tracking from the `Answer` system.
#[derive(Debug, Clone, Default)]
pub struct ResolvedArguments {
    pub values: HashMap<SemanticRole, String>,
}

impl ResolvedArguments {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn set(&mut self, role: SemanticRole, value: impl Into<String>) {
        self.values.insert(role, value.into());
    }

    pub fn get(&self, role: &SemanticRole) -> Option<&str> {
        self.values.get(role).map(|s| s.as_str())
    }
}

/// A verb match — the result of recognizing a verb name in the input.
///
/// In dialog-query terms, this would be a derived `Concept`:
/// ```rust,ignore
/// #[derive(Concept)]
/// pub struct VerbMatch {
///     this: Entity,
///     verb_ref: verb_match::VerbRef,
///     match_quality: verb_match::MatchQuality,
///     token_position: verb_match::TokenPosition,
/// }
/// ```
#[derive(Debug, Clone)]
pub struct VerbMatch {
    /// The name of the matched verb.
    pub verb_name: String,
    /// How well the input matched the verb name.
    pub quality: MatchQuality,
    /// Which token position matched the verb.
    pub token_position: usize,
}

/// How well a token matched a verb name.
///
/// Ordered from best to worst. Ubiquity uses this for scoring:
/// exact matches beat prefix matches beat substring matches.
#[derive(Debug, Clone, Copy, PartialEq, PartialOrd)]
pub enum MatchQuality {
    /// The token exactly equals the verb name or an alias.
    Exact,
    /// The token is a prefix of the verb name (e.g., "trans" → "translate").
    Prefix,
    /// The token is a substring of the verb name.
    Substring,
}

impl MatchQuality {
    /// Convert to a numeric score for ranking (higher is better).
    pub fn score(&self) -> f64 {
        match self {
            MatchQuality::Exact => 1.0,
            MatchQuality::Prefix => 0.8,
            MatchQuality::Substring => 0.5,
        }
    }
}

impl Verb {
    /// Try to match a token against this verb's name and aliases.
    ///
    /// In dialog-query, this would be a rule:
    /// ```rust,ignore
    /// fn match_verb(vm: Match<VerbMatch>) -> impl When {
    ///     (
    ///         Match::<Token> { value: Term::var("token"), position: vm.token_position.clone() },
    ///         Match::<Verb> { name: Term::var("token"), this: vm.verb_ref.clone() },
    ///     )
    /// }
    /// ```
    pub fn match_token(&self, token: &str, position: usize) -> Option<VerbMatch> {
        let token_lower = token.to_lowercase();
        let all_names: Vec<&str> = std::iter::once(self.name.as_str())
            .chain(self.aliases.iter().map(|s| s.as_str()))
            .collect();

        for name in &all_names {
            let name_lower = name.to_lowercase();
            if name_lower == token_lower {
                return Some(VerbMatch {
                    verb_name: self.name.clone(),
                    quality: MatchQuality::Exact,
                    token_position: position,
                });
            }
        }

        for name in &all_names {
            let name_lower = name.to_lowercase();
            if name_lower.starts_with(&token_lower) && token_lower.len() >= 2 {
                return Some(VerbMatch {
                    verb_name: self.name.clone(),
                    quality: MatchQuality::Prefix,
                    token_position: position,
                });
            }
        }

        for name in &all_names {
            let name_lower = name.to_lowercase();
            if name_lower.contains(&token_lower) && token_lower.len() >= 3 {
                return Some(VerbMatch {
                    verb_name: self.name.clone(),
                    quality: MatchQuality::Substring,
                    token_position: position,
                });
            }
        }

        None
    }

    /// Get the argument slot for a given semantic role, if any.
    pub fn argument_for_role(&self, role: &SemanticRole) -> Option<&ArgumentSlot> {
        self.arguments.iter().find(|a| &a.role == role)
    }

    /// Get all required argument slots.
    pub fn required_arguments(&self) -> impl Iterator<Item = &ArgumentSlot> {
        self.arguments.iter().filter(|a| a.required)
    }
}

/// Builder for constructing verbs ergonomically.
pub struct VerbBuilder {
    name: String,
    description: String,
    aliases: Vec<String>,
    arguments: Vec<ArgumentSlot>,
}

impl VerbBuilder {
    pub fn new(name: impl Into<String>, description: impl Into<String>) -> Self {
        VerbBuilder {
            name: name.into(),
            description: description.into(),
            aliases: Vec::new(),
            arguments: Vec::new(),
        }
    }

    pub fn alias(mut self, alias: impl Into<String>) -> Self {
        self.aliases.push(alias.into());
        self
    }

    /// Add an argument slot.
    pub fn argument(
        mut self,
        role: SemanticRole,
        noun_type: impl Into<String>,
        required: bool,
    ) -> Self {
        self.arguments.push(ArgumentSlot {
            role,
            noun_type: noun_type.into(),
            required,
        });
        self
    }

    /// Shorthand: add a required object argument.
    pub fn object(self, noun_type: impl Into<String>) -> Self {
        self.argument(SemanticRole::Object, noun_type, true)
    }

    /// Shorthand: add a goal argument ("to ...").
    pub fn goal(self, noun_type: impl Into<String>, required: bool) -> Self {
        self.argument(SemanticRole::Goal, noun_type, required)
    }

    /// Shorthand: add a source argument ("from ...").
    pub fn source(self, noun_type: impl Into<String>, required: bool) -> Self {
        self.argument(SemanticRole::Source, noun_type, required)
    }

    pub fn build(self) -> Verb {
        Verb {
            name: self.name,
            description: self.description,
            aliases: self.aliases,
            arguments: self.arguments,
        }
    }
}

/// Example: build the "translate" verb.
pub fn verb_translate() -> Verb {
    VerbBuilder::new("translate", "Translate text between languages")
        .alias("trans")
        .object("text")
        .goal("language", true)
        .source("language", false)
        .build()
}

/// Example: build the "search" verb.
pub fn verb_search() -> Verb {
    VerbBuilder::new("search", "Search the web or a specific site")
        .alias("find")
        .alias("google")
        .object("text")
        .argument(SemanticRole::Location, "text", false) // "in <site>"
        .build()
}

/// Example: build the "email" verb.
pub fn verb_email() -> Verb {
    VerbBuilder::new("email", "Send an email")
        .alias("mail")
        .object("text")
        .goal("contact", true) // "to <contact>"
        .build()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exact_verb_match() {
        let verb = verb_translate();
        let m = verb.match_token("translate", 0).unwrap();
        assert_eq!(m.quality, MatchQuality::Exact);
    }

    #[test]
    fn alias_match() {
        let verb = verb_translate();
        let m = verb.match_token("trans", 0).unwrap();
        assert_eq!(m.quality, MatchQuality::Exact);
    }

    #[test]
    fn prefix_match() {
        let verb = verb_translate();
        let m = verb.match_token("transl", 0).unwrap();
        assert_eq!(m.quality, MatchQuality::Prefix);
    }

    #[test]
    fn no_match() {
        let verb = verb_translate();
        assert!(verb.match_token("email", 0).is_none());
    }
}

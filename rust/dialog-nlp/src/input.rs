//! Input layer — text input and selection as facts.
//!
//! Everything the parser operates on starts as a fact in the store.
//! The user's typed text, the current page selection, and contextual
//! metadata are all asserted as facts before parsing begins.
//!
//! In dialog-query terms, these would be Attributes on a session entity:
//!
//! ```rust,ignore
//! // Using dialog-query's #[derive(Attribute)] pattern:
//! mod input {
//!     #[derive(Attribute, Clone)]
//!     pub struct Text(pub String);
//!
//!     #[derive(Attribute, Clone)]
//!     pub struct Selection(pub String);
//!
//!     #[derive(Attribute, Clone)]
//!     pub struct Cursor(pub u32);
//!
//!     #[derive(Attribute, Clone)]
//!     pub struct Locale(pub String);
//! }
//!
//! // Asserting input into the session:
//! let session_entity = Entity::new()?;
//! let mut tx = session.edit();
//! tx.assert(With { this: session_entity, has: input::Text("translate hello to spanish".into()) });
//! tx.assert(With { this: session_entity, has: input::Selection("some selected text".into()) });
//! tx.assert(With { this: session_entity, has: input::Locale("en".into()) });
//! session.commit(tx).await?;
//! ```
//!
//! The parser then queries these input facts as the starting point for the
//! parsing pipeline. Because they are ordinary facts, other rules can also
//! react to them — enabling features like input history, auto-complete hints,
//! or context-dependent verb availability.

/// Represents raw input to the NLP parser.
///
/// This is the in-memory representation used during parsing. In a full
/// dialog-query integration, these fields correspond to attributes on
/// a session entity in the fact store.
#[derive(Debug, Clone)]
pub struct Input {
    /// The text the user typed into the command interface.
    pub text: String,

    /// The currently selected text on the page/document, if any.
    /// Noun recognizers can use this as an implicit argument
    /// (e.g., "translate this" → selection becomes the object).
    pub selection: Option<String>,

    /// The user's locale, used to select the correct preposition→role
    /// mappings and verb aliases. Defaults to "en".
    pub locale: String,
}

impl Input {
    pub fn new(text: impl Into<String>) -> Self {
        Input {
            text: text.into(),
            selection: None,
            locale: "en".into(),
        }
    }

    pub fn with_selection(mut self, selection: impl Into<String>) -> Self {
        self.selection = Some(selection.into());
        self
    }

    pub fn with_locale(mut self, locale: impl Into<String>) -> Self {
        self.locale = locale.into();
        self
    }
}

//! Error types for the NLP parser.

use thiserror::Error;

#[derive(Debug, Error)]
pub enum NlpError {
    #[error("no verbs matched input")]
    NoVerbMatch,

    #[error("no candidates produced for input")]
    NoCandidates,

    #[error("required argument '{role}' for verb '{verb}' was not resolved")]
    MissingArgument { verb: String, role: String },

    #[error("noun type '{noun_type}' has no recognizer rules installed")]
    NoRecognizer { noun_type: String },

    #[error("ambiguous parse: {count} candidates with equal score")]
    Ambiguous { count: usize },

    #[error("verb '{name}' has no effect handler installed")]
    NoHandler { name: String },
}

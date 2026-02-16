//! Effect descriptors — what verb execution produces.
//!
//! When a verb is executed, it doesn't directly perform side effects.
//! Instead, it produces an `Effect` descriptor that describes what should
//! happen. The effect is then executed through dialog-db's capability system.
//!
//! This separation keeps the parser and resolver pure, and gates execution
//! on the caller's capabilities.
//!
//! ## Mapping to dialog-db capabilities
//!
//! ```rust,ignore
//! // An effect maps to a capability invocation:
//! // Effect::Custom { name: "translate", params: { "text": "hello", "to": "es" } }
//! //   →
//! // Subject::from(did)
//! //   .attenuate(NlpVerbs)
//! //   .attenuate(VerbPolicy::new("translate"))
//! //   .invoke(Execute::new(params))
//! ```
//!
//! ## Effectful rules
//!
//! Where dialog-query's `DeductiveRule` derives new facts (purely), an
//! "effectful rule" derives an Effect descriptor. The handler function
//! has the signature:
//!
//! ```rust,ignore
//! // Conceptual: an effectful rule
//! // When: Candidate { verb: "translate", object: ?text, goal: ?lang }
//! // Then: Effect::Custom("translate", { text: ?text, target: ?lang })
//!
//! type VerbHandler = Box<dyn Fn(ResolvedArguments) -> Result<Effect, NlpError>>;
//! ```

use crate::verb::ResolvedArguments;
use std::collections::HashMap;
use std::fmt;

/// An effect descriptor — describes what a verb execution should do.
///
/// Effects are data, not actions. They must be explicitly executed
/// through the capability system after being produced by a verb handler.
#[derive(Debug, Clone)]
pub enum Effect {
    /// A named effect with arbitrary parameters.
    /// This is the general form — specific verb handlers produce these.
    Custom {
        /// The effect name (typically matches the verb name).
        name: String,
        /// Parameters for the effect, derived from resolved arguments.
        params: HashMap<String, String>,
    },

    /// A composite effect — multiple effects to execute in sequence.
    /// Enables verb composition if we choose to support it later.
    Sequence(Vec<Effect>),

    /// No-op — the verb was recognized but produces no effect.
    /// Useful for "help" or "list" verbs that only produce output.
    None,
}

impl fmt::Display for Effect {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Effect::Custom { name, params } => {
                write!(f, "{}(", name)?;
                for (i, (k, v)) in params.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{}={:?}", k, v)?;
                }
                write!(f, ")")
            }
            Effect::Sequence(effects) => {
                write!(f, "[")?;
                for (i, e) in effects.iter().enumerate() {
                    if i > 0 {
                        write!(f, " → ")?;
                    }
                    write!(f, "{}", e)?;
                }
                write!(f, "]")
            }
            Effect::None => write!(f, "none"),
        }
    }
}

/// Convenience: build a custom effect from resolved arguments.
pub fn effect_from_args(name: &str, args: &ResolvedArguments) -> Effect {
    let params = args
        .values
        .iter()
        .map(|(role, value)| (role.to_string(), value.clone()))
        .collect();
    Effect::Custom {
        name: name.to_string(),
        params,
    }
}

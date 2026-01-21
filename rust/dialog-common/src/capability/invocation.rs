//! Invocation trait and types for requesting effect execution.
//!
//! The `Invocation` trait defines what can be passed to `Provider::execute`.
//! Both direct capabilities (`Capability<Fx>`) and authorized capabilities
//! (`Authorized<Fx, A>`) implement this trait.

use super::capability::{Capability, Constraint, Effect};

/// Trait for types that can be invoked via a Provider.
///
/// This trait connects an invocation type to what the provider receives
/// as input and what it produces as output.
///
/// # Implementations
///
/// - `Fx: Effect` - direct execution, input is `Capability<Fx>`
/// - `Authorized<Fx, A>` - authorized execution, input is `Authorized<Fx, A>`
pub trait Invocation {
    /// The input type passed to Provider::execute.
    type Input;
    /// The output type returned from Provider::execute.
    type Output;
}

/// Effects can be invoked directly (without authorization).
impl<Fx> Invocation for Fx
where
    Fx: Effect,
    Fx::Of: Constraint,
{
    type Input = Capability<Fx>;
    type Output = Fx::Output;
}

/// Opaque proof bytes.
///
/// Contains serialized authorization proof (e.g., UCAN chain bytes).
/// Empty for self-issued authorizations where the invoker owns the resource.
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct Proof(pub Vec<u8>);

impl Proof {
    /// Create an empty proof (for self-issued authorizations).
    pub fn empty() -> Self {
        Self(Vec::new())
    }

    /// Create a proof from bytes.
    pub fn new(bytes: Vec<u8>) -> Self {
        Self(bytes)
    }

    /// Get the proof bytes.
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }

    /// Check if the proof is empty.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl From<Vec<u8>> for Proof {
    fn from(bytes: Vec<u8>) -> Self {
        Self(bytes)
    }
}

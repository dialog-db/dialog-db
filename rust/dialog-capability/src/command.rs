use crate::{Capability, Constraint, Effect};

/// Trait for types that can be executed via a Provider.
///
/// This trait connects a command type to what the provider receives
/// as input and what it produces as output.
pub trait Command {
    /// The input type passed to Provider::execute.
    type Input;
    /// The output type returned from Provider::execute.
    type Output;
}

/// Effects can be executed directly (without authorization).
impl<Fx> Command for Fx
where
    Fx: Effect,
    Fx::Of: Constraint,
{
    type Input = Capability<Fx>;
    type Output = Fx::Output;
}

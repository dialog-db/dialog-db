use crate::Fact;

#[cfg(doc)]
use crate::FactStoreMut;

/// The instruction variants that are accepted by [`FactStoreMut::commit`].
pub enum Instruction {
    /// Assert a [`Fact`], persisting it in the [`FactStoreMut`]
    Assert(Fact),
    /// Retract a [`Fact`], removing it from the [`FactStoreMut`]
    Retract(Fact),
}

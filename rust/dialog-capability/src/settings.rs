use serde::Serialize;

/// Builder for collecting constrains from a capability chain.
///
/// Implement this trait to collect constrains in your preferred format.
/// For example, UCAN implements this to serialize constrains to IPLD.
pub trait PolicyBuilder {
    /// Add a constrain to the collection.
    fn push<T: Serialize>(&mut self, constrain: &T);
}

/// Trait for types that can contribute constrains to capability invocations.
///
/// Caveats are conditions or restrictions attached to a capability delegation.
/// This trait is auto-implemented for all `Serialize` types via a blanket impl.
pub trait Caveat: Serialize {
    /// Push this constrain to the builder.
    fn constrain(&self, builder: &mut impl PolicyBuilder);
}

impl<T: Serialize> Caveat for T {
    fn constrain(&self, builder: &mut impl PolicyBuilder) {
        builder.push(self);
    }
}

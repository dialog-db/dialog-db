/// A type that has a static descriptor of type `T`.
///
/// Implemented by attribute and concept types to provide compile-time
/// metadata (e.g. [`AttributeDescriptor`](crate::AttributeDescriptor),
/// [`ConceptDescriptor`](crate::ConceptDescriptor)) via a shared
/// `OnceLock`-backed accessor.
pub trait Descriptor<T> {
    /// Returns a reference to the static descriptor for this type.
    fn descriptor() -> &'static T;
}

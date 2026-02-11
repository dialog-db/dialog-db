//! Unset values for typesafe builder types

/// An unset required value.
///
/// Replace these with the expected type to make
/// the builder convert to the built type.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct Unset;

//! Type system utilities for dialog-query
//!
//! This module provides the bridge between Rust's compile-time type system and
//! dialog-artifacts' runtime [`Type`] system. The core abstractions are:
//!
//! - [`TypeDescriptor`] — a trait implemented by named ZSTs (like [`Text`],
//!   [`Boolean`]) that carry a static `TYPE` constant and can report the
//!   runtime type of a value.
//! - [`Typed`] — maps a Rust type (e.g. `String`) to its [`TypeDescriptor`]
//!   (e.g. `Text`). Also implemented by the ZSTs themselves so that
//!   `Term<String>` and `Term<Text>` are interchangeable.
//! - [`Scalar`] — concrete types with bidirectional [`Value`] conversion.
//! - [`Any`] — a descriptor that carries a runtime `Option<Type>`, used for
//!   type-erased terms (`Term<Any>` replaces the old `Parameter`).

use dialog_common::ConditionalSend;
use std::fmt;
use std::hash::Hash;

pub use crate::artifact::{Attribute, Cause, Entity, Type, Value};

/// Trait implemented by type descriptors — named ZSTs that represent a
/// dialog-artifacts type at the Rust type level.
///
/// Each descriptor answers two questions:
/// 1. **What is the static type?** — via `TYPE` (compile-time constant).
///    `Some(Type::String)` for concrete types, `None` for [`Any`].
/// 2. **What is the runtime type of a given value?** — via `content_type()`.
///    For concrete descriptors this always returns `TYPE`. For [`Any`] it
///    inspects the wrapped `Option<Type>`.
pub trait TypeDescriptor:
    Clone + fmt::Debug + Default + PartialEq + Eq + Hash + Send + Sync + 'static
{
    /// The dialog-artifacts type, if statically known.
    /// `None` means "any type" — determined at runtime.
    const TYPE: Option<Type>;

    /// Report the runtime type this descriptor represents.
    ///
    /// For concrete descriptors (e.g. [`Text`]) this returns `Self::TYPE`.
    /// For [`Any`] this returns the wrapped `Option<Type>`.
    fn content_type(&self) -> Option<Type>;

    /// Reconstruct a descriptor from a runtime type tag.
    ///
    /// For concrete descriptors this ignores the input and returns `Self::default()`.
    /// For [`Any`] this wraps the type tag.
    fn from_content_type(_type: Option<Type>) -> Self {
        Self::default()
    }
}

/// Maps a Rust type to its [`TypeDescriptor`].
///
/// For concrete types like `String`, this maps to a named ZST (`Text`).
/// Each ZST also implements `Typed` mapping to itself, so `Term<String>`
/// and `Term<Text>` use the same internal representation.
pub trait Typed {
    /// The descriptor type that represents this type in the term system.
    type Descriptor: TypeDescriptor;
}

// Named ZST descriptors and their TypeDescriptor + Typed implementations.
// Each descriptor is a zero-sized type that carries type information at the
// Rust type level, enabling Term<T> to store type metadata without overhead.

macro_rules! define_descriptor {
    (
        $(#[$meta:meta])*
        $name:ident, $variant:expr
    ) => {
        $(#[$meta])*
        #[derive(Clone, Debug, Default, PartialEq, Eq, Hash)]
        pub struct $name;

        impl TypeDescriptor for $name {
            const TYPE: Option<Type> = Some($variant);

            fn content_type(&self) -> Option<Type> {
                Some($variant)
            }
        }

        impl Typed for $name {
            type Descriptor = Self;
        }
    };
}

define_descriptor!(
    /// Descriptor for string/text values.
    Text, Type::String
);

define_descriptor!(
    /// Descriptor for boolean values.
    Boolean, Type::Boolean
);

define_descriptor!(
    /// Descriptor for unsigned integer values (`u8`–`u128`, `usize`).
    UnsignedInteger, Type::UnsignedInt
);

define_descriptor!(
    /// Descriptor for signed integer values (`i8`–`i128`).
    SignedInteger, Type::SignedInt
);

define_descriptor!(
    /// Descriptor for floating-point values (`f32`, `f64`).
    Float, Type::Float
);

define_descriptor!(
    /// Descriptor for binary data (`Vec<u8>`).
    Bytes, Type::Bytes
);

define_descriptor!(
    /// Descriptor for entity references.
    EntityType, Type::Entity
);

define_descriptor!(
    /// Descriptor for attribute symbols.
    Symbol, Type::Symbol
);

/// Descriptor for dynamically-typed values — carries an optional runtime
/// type tag. `Term<Any>` is the unified replacement for the old `Parameter`.
#[derive(Clone, Debug, Default, PartialEq, Eq, Hash)]
pub struct Any(pub Option<Type>);

impl TypeDescriptor for Any {
    const TYPE: Option<Type> = None;

    fn content_type(&self) -> Option<Type> {
        self.0
    }

    fn from_content_type(typ: Option<Type>) -> Self {
        Any(typ)
    }
}

impl Typed for Any {
    type Descriptor = Self;
}

// Typed implementations for Rust primitive and dialog-artifacts types.
// Each maps to the appropriate named ZST descriptor.

macro_rules! impl_typed {
    ($rust_type:ty, $descriptor:ty) => {
        impl Typed for $rust_type {
            type Descriptor = $descriptor;
        }
    };
}

impl_typed!(String, Text);
impl_typed!(bool, Boolean);
impl_typed!(usize, UnsignedInteger);
impl_typed!(u128, UnsignedInteger);
impl_typed!(u64, UnsignedInteger);
impl_typed!(u32, UnsignedInteger);
impl_typed!(u16, UnsignedInteger);
impl_typed!(u8, UnsignedInteger);
impl_typed!(i128, SignedInteger);
impl_typed!(i64, SignedInteger);
impl_typed!(i32, SignedInteger);
impl_typed!(i16, SignedInteger);
impl_typed!(i8, SignedInteger);
impl_typed!(f64, Float);
impl_typed!(f32, Float);
impl_typed!(Vec<u8>, Bytes);
impl_typed!(Entity, EntityType);
impl_typed!(Attribute, Symbol);
impl_typed!(crate::attribute::The, Symbol);
impl_typed!(Cause, Bytes);
impl_typed!(Value, Any);

/// A concrete type that can be used as a term value with bidirectional Value conversion.
///
/// `Scalar` types have a known static [`TypeDescriptor`] (their `Tag` is `()`-like —
/// a ZST) and can convert to/from [`Value`]. Every `Scalar` type must implement
/// `Into<Value>` (typically via `From<T> for Value`) for the forward direction.
pub trait Scalar:
    Typed + Clone + fmt::Debug + Into<Value> + 'static + ConditionalSend + TryFrom<Value>
{
}

macro_rules! impl_scalar {
    ($($ty:ty),*) => {
        $(impl Scalar for $ty {})*
    }
}

impl_scalar!(
    bool,
    String,
    u16,
    u32,
    u64,
    u128,
    i16,
    i32,
    i64,
    i128,
    f32,
    f64,
    Entity,
    Attribute,
    Vec<u8>,
    Cause,
    crate::attribute::The
);

impl Scalar for usize {}

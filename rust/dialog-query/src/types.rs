//! Type system utilities for dialog-query
//!
//! This module contains traits and implementations for bridging between Rust types
//! and the dialog-artifacts Type system. The main purpose is to provide
//! compile-time type information that can be used for:
//!
//! - JSON serialization with type annotations
//! - Runtime type checking during pattern matching
//! - Query optimization based on type constraints
//!
//! The core insight is that Rust's type system provides static type information
//! that we can reflect into the dynamic Type enum used by dialog-artifacts.

use dialog_common::ConditionalSend;

pub use crate::artifact::{Attribute, Cause, Entity, Type, Value};

/// Trait for types that can provide Type metadata
///
/// This trait bridges between Rust's compile-time type system and dialog-artifacts'
/// runtime type system. It allows Term<T> to know what Type corresponds
/// to its type parameter T.
///
/// # Key Design Decision
/// Returns `Option<Type>` rather than `Type` to handle the special
/// case of `Value` type, which is itself a dynamic type that can hold any value.
/// For `Value`, we return `None` to indicate "any type".
///
/// # Usage
/// ```rust
/// use dialog_query::types::IntoType;
/// use dialog_query::artifact::Type;
///
/// // For concrete types, bring IntoType trait into scope
/// assert_eq!(<String as IntoType>::TYPE, Some(Type::String));
/// assert_eq!(<u32 as IntoType>::TYPE, Some(Type::UnsignedInt));
/// ```
pub trait IntoType {
    /// The corresponding runtime Type, or None for dynamically-typed Value
    const TYPE: Option<Type>;
}

/// Macro to implement IntoType for primitive types
///
/// This macro reduces boilerplate for implementing the trait on standard Rust types.
/// Each implementation returns Some(Type) for the appropriate variant.
macro_rules! impl_into_type {
    ($rust_type:ty, $value_data_type:expr) => {
        impl IntoType for $rust_type {
            const TYPE: Option<Type> = Some($value_data_type);
        }
    };
}

// Implement IntoType for all supported primitive and dialog-artifacts types
//
// These implementations provide the mapping between Rust types and Type variants.
// Note that all unsigned integer types map to UnsignedInt, and all signed integers map
// to SignedInt, regardless of their specific bit width.

// String type
impl_into_type!(String, Type::String);

// Boolean type
impl_into_type!(bool, Type::Boolean);

// Unsigned integer types (all map to UnsignedInt)
impl_into_type!(usize, Type::UnsignedInt);
impl_into_type!(u128, Type::UnsignedInt);
impl_into_type!(u64, Type::UnsignedInt);
impl_into_type!(u32, Type::UnsignedInt);
impl_into_type!(u16, Type::UnsignedInt);
impl_into_type!(u8, Type::UnsignedInt);

// Signed integer types (all map to SignedInt)
impl_into_type!(i128, Type::SignedInt);
impl_into_type!(i64, Type::SignedInt);
impl_into_type!(i32, Type::SignedInt);
impl_into_type!(i16, Type::SignedInt);
impl_into_type!(i8, Type::SignedInt);

// Floating point types (all map to Float)
impl_into_type!(f64, Type::Float);
impl_into_type!(f32, Type::Float);

// Binary data
impl_into_type!(Vec<u8>, Type::Bytes);

// Dialog-artifacts specific types
impl_into_type!(Entity, Type::Entity);
impl_into_type!(Attribute, Type::Symbol);

impl_into_type!(Cause, Type::Bytes);

/// Special implementation for Value type
///
/// Value is the dynamic type that can hold any of the other types at runtime.
/// Since it's not statically typed to any specific Type variant,
/// we return None to indicate "this can be any type".
///
/// This is used by Term<Value> to indicate untyped variables in JSON serialization.
impl IntoType for Value {
    const TYPE: Option<Type> = None;
}

/// A concrete type that can be used as a term value with bidirectional Value conversion
pub trait Scalar:
    IntoType + Clone + std::fmt::Debug + 'static + ConditionalSend + TryFrom<Value>
{
    /// Can be used to convert scalars into boxed value. It is intentionally
    /// different from `From<Scalar> impl Value` to avoid unintentional
    /// type erasure.
    fn as_value(&self) -> Value;
}

macro_rules! impl_scalar {
    ($($ty:ty),*) => {
        $(impl Scalar for $ty {
            fn as_value(&self) -> Value {
                Value::from(self.to_owned())
            }
        })*
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
    Cause
);

impl Scalar for usize {
    fn as_value(&self) -> Value {
        Value::UnsignedInt(self.to_owned() as u128)
    }
}

impl Scalar for Value {
    fn as_value(&self) -> Value {
        self.to_owned()
    }
}

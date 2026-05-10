//! Type system utilities for dialog-query
//!
//! This module bridges Rust's compile-time type system to the
//! query engine's runtime type system. The core abstractions are:
//!
//! - [`TypeDescriptor`] — a trait implemented by named ZSTs (like
//!   [`Text`], [`Boolean`]) that report a unified
//!   [`type_system::Type`] for a value via
//!   [`kind`](TypeDescriptor::kind).
//! - [`Typed`] — maps a Rust type (e.g. `String`) to its
//!   [`TypeDescriptor`] (e.g. `Text`).
//! - [`Scalar`] — concrete types with bidirectional [`Value`]
//!   conversion.
//! - [`Any`] — a descriptor that carries a runtime
//!   `Option<type_system::Type>`. Used for type-erased terms.
//! - [`OptionalOf`] — a wrapper descriptor for `Term<Option<U>>`.
//!   Lifts the inner descriptor's `Type::Definite(...)` into
//!   `Type::Optional(...)`.

use crate::type_system;
use dialog_common::ConditionalSend;
use std::fmt;
use std::hash::Hash;
use std::marker::PhantomData;

pub use crate::artifact::{ArtifactsAttribute, Cause, Entity, Type, Value};
use crate::attribute::The;

/// Trait implemented by type descriptors — named ZSTs that
/// represent a runtime type at the Rust type level.
///
/// Each descriptor reports its represented type via
/// [`Self::kind`] returning `Option<type_system::Type>`. `None`
/// means "unknown — the unifier decides at rule-compile time."
pub trait TypeDescriptor:
    Clone + fmt::Debug + Default + PartialEq + Eq + Hash + Send + Sync + 'static
{
    /// The legacy storage tag, if statically known. `None` means
    /// "any type."
    const TYPE: Option<Type>;

    /// Report the unified [`type_system::Type`] this descriptor
    /// represents. `None` means "no static info — leave to the
    /// unifier."
    ///
    /// Default implementation lifts [`Self::TYPE`]:
    /// `Some(vt) → Some(Type::primitive(vt))`, `None → None`.
    fn kind(&self) -> Option<type_system::Type> {
        Self::TYPE.map(type_system::Type::primitive)
    }

    /// Reconstruct a descriptor from a unified type kind.
    ///
    /// Concrete descriptors ignore the input. [`Any`] wraps it.
    fn from_kind(_kind: Option<type_system::Type>) -> Self {
        Self::default()
    }
}

/// Maps a Rust type to its [`TypeDescriptor`].
pub trait Typed {
    /// The descriptor type that represents this type in the term
    /// system.
    type Descriptor: TypeDescriptor;
}

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

define_descriptor!(
    /// Descriptor for opaque record values.
    Record, Type::Record
);

/// Descriptor for dynamically-typed values — carries an optional
/// runtime type kind. `Term<Any>` is the unified replacement for
/// the old `Parameter`.
#[derive(Clone, Debug, Default, PartialEq, Eq, Hash)]
pub struct Any(pub Option<type_system::Type>);

impl TypeDescriptor for Any {
    const TYPE: Option<Type> = None;

    fn kind(&self) -> Option<type_system::Type> {
        self.0.clone()
    }

    fn from_kind(kind: Option<type_system::Type>) -> Self {
        Any(kind)
    }
}

impl Typed for Any {
    type Descriptor = Self;
}

impl From<Option<Type>> for Any {
    /// Lift a legacy storage tag into an `Any` descriptor.
    /// `Some(vt) → Some(Type::primitive(vt))`, `None → None`.
    fn from(value: Option<Type>) -> Self {
        Any(value.map(type_system::Type::primitive))
    }
}

/// Wrapper descriptor lifting an inner [`TypeDescriptor`] into a
/// set-widened (Optional) shape.
///
/// Used by `Term<Option<U>>` to report its kind as
/// `Type::Optional(...)` based on the inner descriptor's
/// `Type::Definite(...)`.
#[derive(Clone, Debug, Default, PartialEq, Eq, Hash)]
pub struct OptionalOf<D: TypeDescriptor>(PhantomData<D>);

impl<D: TypeDescriptor> TypeDescriptor for OptionalOf<D> {
    const TYPE: Option<Type> = D::TYPE;

    /// Lift the inner descriptor's `kind()` into `Optional`.
    /// `Some(Type::Definite(d)) → Some(Type::Optional(d))`.
    /// `Some(Type::Optional(d))` passes through.
    /// `None → None`.
    fn kind(&self) -> Option<type_system::Type> {
        D::default().kind().map(|k| match k {
            type_system::Type::Definite(d) => type_system::Type::Optional(d),
            other => other,
        })
    }
}

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
impl_typed!(ArtifactsAttribute, Symbol);
impl_typed!(The, Symbol);
impl_typed!(Cause, Bytes);
impl_typed!(Value, Any);

/// `Option<U>: Typed` for any [`Scalar`] `U`. Maps to
/// [`OptionalOf<U::Descriptor>`].
impl<U: Scalar> Typed for Option<U> {
    type Descriptor = OptionalOf<<U as Typed>::Descriptor>;
}

/// A concrete type that can be used as a term value with
/// bidirectional Value conversion.
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
    ArtifactsAttribute,
    Vec<u8>,
    Cause,
    The
);

impl Scalar for usize {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::type_system::Definite;

    /// `Text::kind()` reports `Some(Type::Definite(Primitive(String)))`.
    #[test]
    fn text_descriptor_kind_is_definite_string() {
        let kind = Text.kind().expect("Text has a static kind");
        match kind {
            type_system::Type::Definite(d) => match *d {
                Definite::Primitive(set) => {
                    assert_eq!(set.as_singleton(), Some(Type::String));
                }
                other => panic!("expected Primitive, got {:?}", other),
            },
            other => panic!("expected Definite, got {:?}", other),
        }
    }

    /// Each named ZST descriptor reports the right primitive.
    #[test]
    fn named_descriptors_report_their_primitives() {
        let to_singleton = |k: Option<type_system::Type>| k.unwrap().shape().as_singleton();
        assert_eq!(to_singleton(Text.kind()), Some(Type::String));
        assert_eq!(to_singleton(Boolean.kind()), Some(Type::Boolean));
        assert_eq!(
            to_singleton(UnsignedInteger.kind()),
            Some(Type::UnsignedInt)
        );
        assert_eq!(to_singleton(SignedInteger.kind()), Some(Type::SignedInt));
        assert_eq!(to_singleton(Float.kind()), Some(Type::Float));
        assert_eq!(to_singleton(Bytes.kind()), Some(Type::Bytes));
        assert_eq!(to_singleton(EntityType.kind()), Some(Type::Entity));
        assert_eq!(to_singleton(Symbol.kind()), Some(Type::Symbol));
        assert_eq!(to_singleton(Record.kind()), Some(Type::Record));
    }

    /// `Any(Some(Type::primitive(vt)))` reports the wrapped kind.
    #[test]
    fn any_descriptor_with_tag_reports_definite() {
        let descriptor = Any(Some(type_system::Type::primitive(Type::Entity)));
        let kind = descriptor.kind().expect("kind present");
        assert!(!kind.is_optional());
        assert_eq!(kind.shape().as_singleton(), Some(Type::Entity));
    }

    /// `Any(None)` reports `None` — no static info.
    #[test]
    fn any_descriptor_without_tag_reports_none() {
        let descriptor = Any(None);
        assert!(descriptor.kind().is_none());
    }

    /// `Any::default()` yields `Any(None)`.
    #[test]
    fn any_default_is_none() {
        let descriptor = Any::default();
        assert_eq!(descriptor, Any(None));
        assert!(descriptor.kind().is_none());
    }

    /// `From<Option<Type>> for Any` lifts a legacy storage tag.
    #[test]
    fn from_option_value_type_lifts_into_any() {
        let a: Any = Some(Type::String).into();
        assert_eq!(a.kind(), Some(type_system::Type::primitive(Type::String)));
        let b: Any = None.into();
        assert_eq!(b, Any(None));
    }

    /// `OptionalOf<Text>::kind()` reports
    /// `Some(Type::Optional(Primitive(String)))`.
    #[test]
    fn optional_of_text_reports_optional_string() {
        let descriptor: OptionalOf<Text> = OptionalOf::default();
        let kind = descriptor.kind().expect("present");
        assert!(kind.is_optional());
        assert_eq!(kind.shape().as_singleton(), Some(Type::String));
    }

    /// `OptionalOf<EntityType>::kind()` reports
    /// `Some(Type::Optional(Primitive(Entity)))`.
    #[test]
    fn optional_of_entity_reports_optional_entity() {
        let descriptor: OptionalOf<EntityType> = OptionalOf::default();
        let kind = descriptor.kind().expect("present");
        assert!(kind.is_optional());
        assert_eq!(kind.shape().as_singleton(), Some(Type::Entity));
    }

    /// `OptionalOf<Any>` passes through `None` since `Any`'s
    /// default kind is `None`.
    #[test]
    fn optional_of_any_passes_through_none() {
        let descriptor: OptionalOf<Any> = OptionalOf::default();
        assert!(descriptor.kind().is_none());
    }
}

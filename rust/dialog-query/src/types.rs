//! Type system utilities for dialog-query
//!
//! This module bridges Rust's compile-time type system to the
//! query engine's runtime type system. The core abstractions are:
//!
//! - [`TypeDescriptor`]: a trait implemented by named ZSTs (like
//!   [`Text`], [`Boolean`]) that report a unified
//!   [`type_system::Type`] for a value via
//!   [`kind`](TypeDescriptor::kind).
//! - [`Typed`]: maps a Rust type (e.g. `String`) to its
//!   [`TypeDescriptor`] (e.g. `Text`).
//! - [`Scalar`]: concrete types with bidirectional [`Value`]
//!   conversion.
//! - [`Any`]: a descriptor that carries a runtime
//!   `Option<type_system::Type>`. Used for type-erased terms.
//! - [`OptionalOf`]: a wrapper descriptor for `Term<Option<U>>`.
//!   Widens the inner descriptor's kind with the `Nothing` atom via
//!   [`type_system::Type::optional`].

use crate::type_system;
use crate::type_system::Type as Kind;
use dialog_common::{ConditionalSend, ConditionalSync};
use std::fmt;
use std::hash::Hash;
use std::marker::PhantomData;

use crate::artifact::ArtifactTypeError;
pub use crate::artifact::{ArtifactsAttribute, Cause, Entity, Type, Value};
use crate::attribute::The;

/// Trait implemented by type descriptors: named ZSTs that
/// represent a runtime type at the Rust type level.
///
/// Each descriptor reports its represented type via
/// [`Self::kind`] returning `Option<type_system::Type>`. `None`
/// means "unknown; the unifier decides at rule-compile time."
pub trait TypeDescriptor:
    Clone + fmt::Debug + Default + PartialEq + Eq + Hash + ConditionalSend + ConditionalSync + 'static
{
    /// The legacy storage tag, if statically known. `None` means
    /// "any type."
    const TYPE: Option<Type>;

    /// Report the unified [`type_system::Type`] this descriptor
    /// represents. `None` means "no static info; leave to the
    /// unifier."
    ///
    /// Default implementation lifts [`Self::TYPE`]:
    /// `Some(vt) → Some(Type::from(vt))`, `None → None`.
    fn kind(&self) -> Option<type_system::Type> {
        Self::TYPE.map(Kind::from)
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
    /// Descriptor for unsigned integer values (`u8`-`u128`, `usize`).
    UnsignedInteger, Type::UnsignedInt
);

define_descriptor!(
    /// Descriptor for signed integer values (`i8`-`i128`).
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

/// Descriptors whose terms can carry a *narrowed* kind at plan
/// time — a refinement the rule proved about the variable (an
/// entity URI prefix, an attribute-name prefix) that the scan
/// boundary turns into index-range bounds. The stored kind is
/// `None` whenever it adds nothing over the static type, so terms
/// without narrowing compare, hash, and round-trip exactly like
/// the unit descriptors they replace.
macro_rules! define_refinable_descriptor {
    (
        $(#[$meta:meta])*
        $name:ident, $variant:expr
    ) => {
        $(#[$meta])*
        #[derive(Clone, Debug, Default, PartialEq, Eq, Hash)]
        pub struct $name(Option<type_system::Type>);

        impl TypeDescriptor for $name {
            const TYPE: Option<Type> = Some($variant);

            fn kind(&self) -> Option<type_system::Type> {
                self.0
                    .clone()
                    .or_else(|| Self::TYPE.map(Kind::from))
            }

            fn from_kind(kind: Option<type_system::Type>) -> Self {
                // Normalize: a kind that says no more than the
                // static type is not stored, so unnarrowed terms
                // stay equal to their pre-roundtrip selves.
                let stored = kind.filter(|k| Some(k) != Self::TYPE.map(Kind::from).as_ref());
                $name(stored)
            }
        }

        impl Typed for $name {
            type Descriptor = Self;
        }
    };
}

define_refinable_descriptor!(
    /// Descriptor for entity references.
    EntityType, Type::Entity
);

define_refinable_descriptor!(
    /// Descriptor for attribute symbols.
    Symbol, Type::Symbol
);

define_descriptor!(
    /// Descriptor for opaque record values.
    Record, Type::Record
);

/// Descriptor for dynamically-typed values: carries an optional
/// runtime type kind. `Term<Any>` is the type-erased term whose
/// kind is decided at runtime.
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
    /// `Some(vt) → Some(Type::from(vt))`, `None → None`.
    fn from(value: Option<Type>) -> Self {
        Any(value.map(Kind::from))
    }
}

/// Wrapper descriptor widening an inner [`TypeDescriptor`]'s kind
/// with the `Nothing` atom, used by `Term<Option<U>>`.
#[derive(Clone, Debug, Default, PartialEq, Eq, Hash)]
pub struct OptionalOf<D: TypeDescriptor>(PhantomData<D>);

impl<D: TypeDescriptor> TypeDescriptor for OptionalOf<D> {
    const TYPE: Option<Type> = D::TYPE;

    /// Widen the inner descriptor's `kind()` with the `Nothing`
    /// atom via [`type_system::Type::optional`]. `None → None`.
    fn kind(&self) -> Option<type_system::Type> {
        D::default().kind().map(|k| k.optional())
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
    The,
    RecordBytes
);

/// The opaque payload of a [`Value::Record`] fact, as a typed term
/// value.
///
/// `Vec<u8>` carries `Value::Bytes`; this wrapper is its record-typed
/// counterpart, so a term can bind a record-valued fact (e.g. a
/// version-control revision record — see the `dialog/revision`
/// formula) and hand its bytes to a formula for projection.
#[derive(Clone, Debug, Default, PartialEq, Eq, Hash)]
pub struct RecordBytes(pub Vec<u8>);

impl_typed!(RecordBytes, Record);

impl From<RecordBytes> for Value {
    fn from(value: RecordBytes) -> Self {
        Value::Record(value.0)
    }
}

impl TryFrom<Value> for RecordBytes {
    type Error = ArtifactTypeError;

    fn try_from(value: Value) -> Result<Self, Self::Error> {
        match value {
            Value::Record(bytes) => Ok(RecordBytes(bytes)),
            _ => Err(Self::Error::TypeMismatch(Type::Record, value.data_type())),
        }
    }
}

impl Scalar for usize {}

// Kind-marker trait family: classify types by *shape* for
// compile-time bounds, orthogonal to [`Typed`] (which is the
// mechanical "what descriptor represents this Rust type" map).
//
// Membership table:
// - `ScalarType`:  atomic value types ([`Scalar`] members).
// - `ProductType`: record/product shapes. Reserved.
// - `VariantType`: sum/variant shapes. Reserved.
// - `OptionalType`: `Option<T>` for `T: DefiniteType`.
// - `AnyType`:     the dynamic top, [`Any`].
// - `DefiniteType`: umbrella over `Scalar`/`Product`/`Variant`,
//   the canonical "any concrete shape, not optional, not dynamic"
//   predicate. Used as the bound on `Option<T>`'s `Typed` impl,
//   which is what structurally rejects `Option<Option<U>>` and
//   `Option<Any>` at compile time.

/// Kind marker for atomic (scalar) shapes: types like `String`,
/// `i32`, `Entity`. Every [`Scalar`] is `ScalarType` via the
/// blanket impl below.
///
/// New code that needs a pure kind bound (without the practical
/// `Clone + Into<Value> + ...` bag carried by [`Scalar`]) should
/// use `ScalarType`.
pub trait ScalarType: Typed {}

impl<T: Scalar> ScalarType for T {}

/// Kind marker for product (record) shapes. Reserved for future
/// use; no types impl this trait yet, but the slot exists so that
/// product-only generic code reads symmetrically with
/// [`ScalarType`] and [`VariantType`].
pub trait ProductType: Typed {}

/// Kind marker for variant (sum) shapes. Reserved for future
/// use; no types impl this trait yet.
pub trait VariantType: Typed {}

/// Kind marker for the dynamic top type, [`Any`].
///
/// `Any` is deliberately *not* a [`DefiniteType`]: it already
/// admits absence implicitly (via [`Binding`](crate::Binding))
/// and wrapping it in `Option` would be a category error. This
/// exclusion is what makes `Option<Any>` rejected at compile time.
pub trait AnyType: Typed {}

impl AnyType for Any {}

/// Kind marker for the `Option<T>` shape family.
///
/// `Option<T>` impls `OptionalType` for any `T: DefiniteType`.
/// The bound on `T` is the structural fence that makes:
///
/// - `Option<Option<U>>` reject (nested optionality is meaningless
///   under set-widening; `OptionalType` does not impl
///   `DefiniteType`).
/// - `Option<Any>` reject (`AnyType` does not impl `DefiniteType`).
///
/// The compile-fail doctests below prove both rejections.
///
/// Nested optionality is rejected:
///
/// ```compile_fail
/// # use dialog_query::Term;
/// // Option<Option<String>> does not satisfy Typed because
/// // Option<String> is not DefiniteType.
/// let _: Term<Option<Option<String>>> = Term::var("x");
/// ```
///
/// Optional-of-Any is rejected:
///
/// ```compile_fail
/// # use dialog_query::{Term, types::Any};
/// // Option<Any> does not satisfy Typed because Any is not
/// // DefiniteType.
/// let _: Term<Option<Any>> = Term::var("x");
/// ```
pub trait OptionalType: Typed {}

impl<T: Scalar> OptionalType for Option<T> {}

/// Umbrella supertrait for **definite** types: any concrete shape
/// that is not the dynamic top ([`AnyType`]) and not set-widened
/// ([`OptionalType`]).
///
/// **Membership.** Every [`Scalar`] is `DefiniteType` via a
/// blanket impl. Future products and variants will join via their
/// own `impl … for X` lines.
///
/// **Where the bound is used.** This is the bound on `T` in
/// `Option<T>`'s [`Typed`] impl, and the canonical "any concrete
/// value type, but not Any, not Optional" predicate. Rust trait
/// bounds compose by intersection (`T: A + B` = both), so an
/// umbrella supertrait is the right shape for the *union* of
/// `Scalar`/`Product`/`Variant`.
pub trait DefiniteType: Typed {}

impl<T: Scalar> DefiniteType for T {}

#[cfg(test)]
mod tests {
    use super::*;

    /// `Text::kind()` reports `Some(Type::Primitive({String}))`.
    #[dialog_common::test]
    fn text_descriptor_kind_is_primitive_string() {
        let kind = Text.kind().expect("Text has a static kind");
        assert_eq!(kind.as_value_type(), Some(Type::String));
        assert!(!kind.is_optional());
    }

    /// Each named ZST descriptor reports the right primitive.
    #[dialog_common::test]
    fn named_descriptors_report_their_primitives() {
        let to_singleton = |k: Option<Kind>| k.unwrap().as_value_type();
        assert_eq!(to_singleton(Text.kind()), Some(Type::String));
        assert_eq!(to_singleton(Boolean.kind()), Some(Type::Boolean));
        assert_eq!(
            to_singleton(UnsignedInteger.kind()),
            Some(Type::UnsignedInt)
        );
        assert_eq!(to_singleton(SignedInteger.kind()), Some(Type::SignedInt));
        assert_eq!(to_singleton(Float.kind()), Some(Type::Float));
        assert_eq!(to_singleton(Bytes.kind()), Some(Type::Bytes));
        assert_eq!(
            to_singleton(EntityType::default().kind()),
            Some(Type::Entity)
        );
        assert_eq!(to_singleton(Symbol::default().kind()), Some(Type::Symbol));
        assert_eq!(to_singleton(Record.kind()), Some(Type::Record));
    }

    /// `Any(Some(Type::from(vt)))` reports the wrapped kind.
    #[dialog_common::test]
    fn any_descriptor_with_tag_reports_primitive() {
        let descriptor = Any(Some(Kind::from(Type::Entity)));
        let kind = descriptor.kind().expect("kind present");
        assert!(!kind.is_optional());
        assert_eq!(kind.as_value_type(), Some(Type::Entity));
    }

    /// `Any(None)` reports `None`: no static info.
    #[dialog_common::test]
    fn any_descriptor_without_tag_reports_none() {
        let descriptor = Any(None);
        assert!(descriptor.kind().is_none());
    }

    /// `Any::default()` yields `Any(None)`.
    #[dialog_common::test]
    fn any_default_is_none() {
        let descriptor = Any::default();
        assert_eq!(descriptor, Any(None));
        assert!(descriptor.kind().is_none());
    }

    /// `From<Option<Type>> for Any` lifts a legacy storage tag.
    #[dialog_common::test]
    fn from_option_value_type_lifts_into_any() {
        let a: Any = Some(Type::String).into();
        assert_eq!(a.kind(), Some(Kind::from(Type::String)));
        let b: Any = None.into();
        assert_eq!(b, Any(None));
    }

    /// `OptionalOf<Text>::kind()` widens String with `Nothing`.
    #[dialog_common::test]
    fn optional_of_text_widens_with_nothing() {
        let descriptor: OptionalOf<Text> = OptionalOf::default();
        let kind = descriptor.kind().expect("present");
        assert!(kind.is_optional());
        assert!(kind.primitive_part().contains(Type::String));
    }

    /// `OptionalOf<EntityType>::kind()` widens Entity with `Nothing`.
    #[dialog_common::test]
    fn optional_of_entity_widens_with_nothing() {
        let descriptor: OptionalOf<EntityType> = OptionalOf::default();
        let kind = descriptor.kind().expect("present");
        assert!(kind.is_optional());
        assert!(kind.primitive_part().contains(Type::Entity));
    }

    /// `OptionalOf<Any>` passes through `None` since `Any`'s
    /// default kind is `None`.
    #[dialog_common::test]
    fn optional_of_any_passes_through_none() {
        let descriptor: OptionalOf<Any> = OptionalOf::default();
        assert!(descriptor.kind().is_none());
    }

    /// Kind-marker family is consistent: scalar types are
    /// `ScalarType` and `DefiniteType`; `Any` is `AnyType` but not
    /// `DefiniteType`; `Option<T>` is `OptionalType`.
    ///
    /// These are compile-time checks: the test body uses
    /// function-bound helpers that only accept types that satisfy
    /// the corresponding marker trait.
    #[dialog_common::test]
    fn kind_markers_classify_consistently() {
        fn requires_scalar<T: ScalarType>() {}
        fn requires_definite<T: DefiniteType>() {}
        fn requires_optional<T: OptionalType>() {}
        fn requires_any<T: AnyType>() {}

        requires_scalar::<String>();
        requires_scalar::<u32>();
        requires_scalar::<Entity>();
        requires_definite::<String>();
        requires_definite::<u32>();
        requires_optional::<Option<String>>();
        requires_optional::<Option<u32>>();
        requires_any::<Any>();
    }
}

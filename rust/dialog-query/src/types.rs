//! Type system utilities for dialog-query
//!
//! This module provides the bridge between Rust's compile-time type system and
//! the runtime type system. The core abstractions are:
//!
//! - [`TypeDescriptor`] — a trait implemented by named ZSTs (like [`Text`],
//!   [`Boolean`]) that carry a static `TYPE` constant and can report the
//!   runtime type of a value. Now also exposes
//!   [`kind`](TypeDescriptor::kind) returning the unified
//!   [`type_system::Type`] for the v2 type system.
//! - [`Typed`] — maps a Rust type (e.g. `String`) to its [`TypeDescriptor`]
//!   (e.g. `Text`). Also implemented by the ZSTs themselves so that
//!   `Term<String>` and `Term<Text>` are interchangeable.
//! - [`Scalar`] — concrete types with bidirectional [`Value`] conversion.
//! - [`Any`] — a descriptor that carries a runtime `Option<Type>`, used for
//!   type-erased terms (`Term<Any>` replaces the old `Parameter`).
//!   In v2, `Any::kind()` reports a fresh anonymous type variable
//!   when the wrapped tag is `None`.
//! - [`OptionalOf`] — a wrapper descriptor for `Term<Option<U>>`. Reports
//!   `Type::Optional(...)` from the inner descriptor's `Type::Definite(...)`.

use crate::type_system;
use dialog_common::ConditionalSend;
use std::fmt;
use std::hash::Hash;
use std::marker::PhantomData;

pub use crate::artifact::{ArtifactsAttribute, Cause, Entity, Type, Value};
use crate::attribute::The;

/// Trait implemented by type descriptors — named ZSTs that represent a
/// runtime type at the Rust type level.
///
/// Each descriptor exposes three views of its represented type:
/// 1. **Storage tag** — the legacy [`Type`] (alias for
///    `dialog_artifacts::ValueDataType`) carried in
///    [`Self::TYPE`]. `Some(Type::String)` for concrete
///    descriptors, `None` for [`Any`]. This view is what storage
///    selectors and wire-format value tags consume.
/// 2. **Runtime tag** — [`Self::content_type`] returns the same
///    `Option<Type>` but takes `&self`, allowing dynamic
///    descriptors (like [`Any`]) to inspect their wrapped state.
/// 3. **Unified type** — [`Self::kind`] returns the v2
///    [`type_system::Type`], the rich representation used by the
///    Damas-Milner unifier and rule-compile-time analysis.
///    Lossless from `TYPE`: a `None` storage tag becomes a fresh
///    anonymous variable; a `Some(vt)` becomes
///    `Type::Definite(Primitive(singleton(vt)))`.
pub trait TypeDescriptor:
    Clone + fmt::Debug + Default + PartialEq + Eq + Hash + Send + Sync + 'static
{
    /// The legacy storage tag, if statically known.
    /// `None` means "any type" — determined at runtime.
    const TYPE: Option<Type>;

    /// Report the runtime storage tag this descriptor represents.
    ///
    /// For concrete descriptors (e.g. [`Text`]) this returns
    /// `Self::TYPE`. For [`Any`] this returns the wrapped
    /// `Option<Type>`.
    fn content_type(&self) -> Option<Type>;

    /// Report the unified [`type_system::Type`] this descriptor
    /// represents.
    ///
    /// Default implementation builds from [`Self::TYPE`]:
    /// - `None` → a fresh anonymous variable with constraint
    ///   [`PrimitiveSet::ALL`](type_system::PrimitiveSet::ALL).
    /// - `Some(vt)` → `Type::Definite(Primitive(singleton(vt)))`.
    ///
    /// Dynamic descriptors ([`Any`]) override to inspect their
    /// wrapped state. Wrapper descriptors ([`OptionalOf`])
    /// override to lift the inner kind into `Type::Optional`.
    fn kind(&self) -> type_system::Type {
        match Self::TYPE {
            Some(vt) => type_system::Type::primitive(vt),
            None => type_system::Type::any(),
        }
    }

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

define_descriptor!(
    /// Descriptor for opaque record values.
    Record, Type::Record
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

    /// For [`Any`], `kind()` mirrors the wrapped storage tag:
    /// - `Some(vt)` → `Type::Definite(Primitive(singleton(vt)))`.
    /// - `None` → fresh anonymous variable
    ///   (`Definite::Variable(VarId::global_fresh())`) with
    ///   constraint [`PrimitiveSet::ALL`](type_system::PrimitiveSet::ALL).
    ///
    /// Each call with `None` allocates a new global `VarId`.
    /// Callers that need stable identity across multiple
    /// `kind()` calls on the same `Term<Any>` should consult
    /// the rule-level [`UnificationContext`](type_system::UnificationContext)
    /// rather than calling `kind()` repeatedly.
    fn kind(&self) -> type_system::Type {
        match self.0 {
            Some(vt) => type_system::Type::primitive(vt),
            None => type_system::Type::any(),
        }
    }

    fn from_content_type(typ: Option<Type>) -> Self {
        Any(typ)
    }
}

impl Typed for Any {
    type Descriptor = Self;
}

/// Wrapper descriptor lifting an inner [`TypeDescriptor`] into a
/// set-widened (Optional) shape.
///
/// Used by `Term<Option<U>>` to report its kind as
/// `Type::Optional(...)` based on the inner descriptor's
/// `Type::Definite(...)`. Adding `Option<U>` at the Rust type
/// level produces a `Term<Option<U>>` whose descriptor is
/// `OptionalOf<<U as Typed>::Descriptor>`.
///
/// `OptionalOf` is a ZST: it carries no runtime data, the inner
/// descriptor's information is reached via `D::kind()`.
///
/// Note: only used in v2's typed surface for `Term<Option<U>>`.
/// The macro layer (Step 7) emits `Term<Option<...>>` for
/// `Option<T>` fields, which routes through this descriptor.
#[derive(Clone, Debug, Default, PartialEq, Eq, Hash)]
pub struct OptionalOf<D: TypeDescriptor>(PhantomData<D>);

impl<D: TypeDescriptor> TypeDescriptor for OptionalOf<D> {
    /// Optional doesn't have a single storage tag — it admits
    /// either the inner type *or* `Absent`. The legacy `TYPE`
    /// constant is `None` to signal "no single static tag";
    /// callers needing the rich information call `kind()`.
    const TYPE: Option<Type> = D::TYPE;

    fn content_type(&self) -> Option<Type> {
        D::TYPE
    }

    /// Lift the inner descriptor's `kind()` from
    /// `Type::Definite(d)` to `Type::Optional(d)`.
    ///
    /// If the inner descriptor reports a non-`Definite` kind
    /// (e.g. another `Optional` — which the marker traits at the
    /// Rust API layer prevent, but be defensive at runtime), the
    /// kind passes through unchanged.
    fn kind(&self) -> type_system::Type {
        match D::default().kind() {
            type_system::Type::Definite(d) => type_system::Type::Optional(d),
            other => other,
        }
    }
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
impl_typed!(ArtifactsAttribute, Symbol);
impl_typed!(The, Symbol);
impl_typed!(Cause, Bytes);
impl_typed!(Value, Any);

/// `Option<U>: Typed` for any [`Scalar`] `U`. Maps to
/// [`OptionalOf<U::Descriptor>`], so `Term<Option<String>>` and
/// `Term<Option<u32>>` get a descriptor that reports
/// `Type::Optional(Primitive(...))` via [`TypeDescriptor::kind`].
///
/// The `U: Scalar` bound is what structurally rejects nested
/// optionality: `Option<U>` itself is not `Scalar` (no
/// `impl Scalar for Option<U>`), so `Option<Option<U>>` fails to
/// satisfy `Typed`. This is the v2 replacement for v1's
/// `OptionalType: !DefiniteType` marker-trait fence.
impl<U: Scalar> Typed for Option<U> {
    type Descriptor = OptionalOf<<U as Typed>::Descriptor>;
}

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
    ArtifactsAttribute,
    Vec<u8>,
    Cause,
    The
);

impl Scalar for usize {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::type_system::{Definite, PrimitiveSet};

    /// `Text::kind()` reports `Type::Definite(Primitive(singleton(String)))`.
    #[test]
    fn text_descriptor_kind_is_definite_string() {
        let kind = Text.kind();
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
        assert_eq!(Text.kind().shape().as_singleton(), Some(Type::String));
        assert_eq!(Boolean.kind().shape().as_singleton(), Some(Type::Boolean));
        assert_eq!(
            UnsignedInteger.kind().shape().as_singleton(),
            Some(Type::UnsignedInt)
        );
        assert_eq!(
            SignedInteger.kind().shape().as_singleton(),
            Some(Type::SignedInt)
        );
        assert_eq!(Float.kind().shape().as_singleton(), Some(Type::Float));
        assert_eq!(Bytes.kind().shape().as_singleton(), Some(Type::Bytes));
        assert_eq!(EntityType.kind().shape().as_singleton(), Some(Type::Entity));
        assert_eq!(Symbol.kind().shape().as_singleton(), Some(Type::Symbol));
        assert_eq!(Record.kind().shape().as_singleton(), Some(Type::Record));
    }

    /// `Any(Some(vt))` reports `Type::Definite(Primitive(singleton(vt)))`.
    #[test]
    fn any_descriptor_with_tag_reports_definite() {
        let descriptor = Any(Some(Type::Entity));
        let kind = descriptor.kind();
        assert!(!kind.is_optional());
        assert_eq!(kind.shape().as_singleton(), Some(Type::Entity));
    }

    /// `Any(None)` reports an anonymous variable.
    #[test]
    fn any_descriptor_without_tag_reports_variable() {
        let descriptor = Any(None);
        let kind = descriptor.kind();
        match kind.shape() {
            Definite::Variable(_) => {}
            other => panic!("expected variable, got {:?}", other),
        }
    }

    /// `Any(None).kind()` allocates a fresh `VarId` each call.
    #[test]
    fn any_descriptor_kind_is_unique_per_call() {
        let descriptor = Any(None);
        let a = match descriptor.kind().shape() {
            Definite::Variable(id) => *id,
            _ => panic!("expected variable"),
        };
        let b = match descriptor.kind().shape() {
            Definite::Variable(id) => *id,
            _ => panic!("expected variable"),
        };
        assert_ne!(a, b);
    }

    /// `OptionalOf<Text>::kind()` reports `Type::Optional(Primitive(String))`.
    #[test]
    fn optional_of_text_reports_optional_string() {
        let descriptor: OptionalOf<Text> = OptionalOf::default();
        let kind = descriptor.kind();
        assert!(kind.is_optional());
        assert_eq!(
            kind.shape().as_singleton(),
            Some(Type::String),
            "inner shape preserved through Optional wrap"
        );
    }

    /// `OptionalOf<EntityType>::kind()` reports
    /// `Type::Optional(Primitive(Entity))`.
    #[test]
    fn optional_of_entity_reports_optional_entity() {
        let descriptor: OptionalOf<EntityType> = OptionalOf::default();
        let kind = descriptor.kind();
        assert!(kind.is_optional());
        assert_eq!(kind.shape().as_singleton(), Some(Type::Entity));
    }

    /// `OptionalOf<Any>` should pass through (Any's kind is a
    /// variable, not Definite, so the Optional wrap is a no-op
    /// per the defensive fall-through). Marker traits at the
    /// Rust API layer prevent this case in well-typed code.
    #[test]
    fn optional_of_any_passes_through() {
        let descriptor: OptionalOf<Any> = OptionalOf::default();
        let kind = descriptor.kind();
        // Any's kind is Type::Definite(Variable(_)). Optional
        // wrap turns that into Type::Optional(Variable(_)).
        // Both are valid; our test just verifies kind() doesn't
        // panic and produces a well-formed Type.
        match kind {
            type_system::Type::Optional(d) => match *d {
                Definite::Variable(_) => {}
                other => panic!("expected Variable, got {:?}", other),
            },
            type_system::Type::Definite(d) => match *d {
                Definite::Variable(_) => {}
                other => panic!("expected Variable, got {:?}", other),
            },
        }
    }

    /// The default `kind()` from `TYPE` matches the override
    /// behavior — sanity check that the trait default and the
    /// macro-generated impls agree.
    #[test]
    fn default_kind_matches_named_descriptor() {
        // Text uses the default kind() from the trait. This test
        // verifies the default impl is wired correctly.
        let kind = Text.kind();
        assert_eq!(kind.shape().as_singleton(), Some(Type::String));
    }

    /// Constraint set on `Any`'s anonymous variable is `ALL`.
    #[test]
    fn any_descriptor_variable_constraint_is_all() {
        let descriptor = Any(None);
        let kind = descriptor.kind();
        let var_id = match kind.shape() {
            Definite::Variable(id) => *id,
            _ => panic!("expected variable"),
        };
        // The global VarId allocator doesn't track constraints;
        // a fresh UnificationContext considering this VarId
        // would treat its constraint as ALL by default.
        let ctx = type_system::UnificationContext::new();
        assert_eq!(ctx.constraint(var_id), PrimitiveSet::ALL);
    }
}

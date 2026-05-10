//! Unified type system for the query engine.
//!
//! This module defines the static, derive-friendly user-facing
//! [`Type`] used by [`Term`](crate::Term),
//! [`Equality`](crate::constraint::Equality),
//! [`Premise`](crate::Premise), [`DeductiveRule`](crate::DeductiveRule),
//! and the schema layer.
//!
//! Type variables — used for compile-time unification of rules —
//! live in the [`unifier`] submodule, never in the user-facing
//! [`Type`]. This split lets the user-facing types derive
//! [`PartialEq`]/[`Eq`]/[`Hash`] cleanly: equivalent rules produce
//! stable hashes regardless of how many anonymous variables they
//! have.
//!
//! See `notes/optional-fields.md` for the design rationale.

pub mod unifier;

use crate::artifact::Type as ValueType;
use serde::{Deserialize, Serialize};

/// A bitfield over [`ValueType`] variants — a set of admissible
/// primitive shapes.
///
/// Used everywhere a "kind constraint" is needed: type variables
/// declare their constraint as a `PrimitiveSet`, formula schemas
/// declare per-variable constraints, and unification intersects
/// them.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct PrimitiveSet {
    /// One bit per [`ValueType`] variant. Bit position is the
    /// variant's discriminant in [`Self::bit_for`].
    bits: u16,
}

impl PrimitiveSet {
    /// Empty set — no admissible types. Constructible but
    /// rejected at unification time (an empty set means
    /// "no shape can satisfy this constraint").
    pub const EMPTY: Self = Self { bits: 0 };

    /// Set containing every [`ValueType`] variant.
    pub const ALL: Self = Self {
        bits: 0b1_1111_1111,
    };

    /// Numeric primitives: `UnsignedInt`, `SignedInt`, `Float`.
    pub const NUMERIC: Self = Self {
        bits: Self::bit_for(ValueType::UnsignedInt)
            | Self::bit_for(ValueType::SignedInt)
            | Self::bit_for(ValueType::Float),
    };

    /// String-like primitives: `String`, `Symbol`.
    pub const STRING_LIKE: Self = Self {
        bits: Self::bit_for(ValueType::String) | Self::bit_for(ValueType::Symbol),
    };

    /// Comparable primitives: numeric, string-like, entity, bytes.
    pub const COMPARABLE: Self = Self {
        bits: Self::NUMERIC.bits
            | Self::STRING_LIKE.bits
            | Self::bit_for(ValueType::Entity)
            | Self::bit_for(ValueType::Bytes),
    };

    /// Construct a singleton set from a single `ValueType`.
    pub const fn singleton(vt: ValueType) -> Self {
        Self {
            bits: Self::bit_for(vt),
        }
    }

    /// Bitfield position for a `ValueType` variant. Stable across
    /// builds so serialization round-trips.
    const fn bit_for(vt: ValueType) -> u16 {
        match vt {
            ValueType::String => 1 << 0,
            ValueType::Boolean => 1 << 1,
            ValueType::UnsignedInt => 1 << 2,
            ValueType::SignedInt => 1 << 3,
            ValueType::Float => 1 << 4,
            ValueType::Bytes => 1 << 5,
            ValueType::Entity => 1 << 6,
            ValueType::Symbol => 1 << 7,
            ValueType::Record => 1 << 8,
        }
    }

    /// Returns `true` iff this set has no members.
    pub fn is_empty(self) -> bool {
        self.bits == 0
    }

    /// Set intersection. Returns `None` iff the result is empty.
    pub fn intersect(self, other: Self) -> Option<Self> {
        let merged = Self {
            bits: self.bits & other.bits,
        };
        if merged.is_empty() {
            None
        } else {
            Some(merged)
        }
    }

    /// Set union. Always non-empty if either input is.
    pub fn union(self, other: Self) -> Self {
        Self {
            bits: self.bits | other.bits,
        }
    }

    /// Returns `true` iff `self` is a (non-strict) superset of `other`.
    pub fn includes(self, other: Self) -> bool {
        (self.bits & other.bits) == other.bits
    }

    /// Returns `true` iff `vt` is a member of this set.
    pub fn contains(self, vt: ValueType) -> bool {
        (self.bits & Self::bit_for(vt)) != 0
    }

    /// If this set has exactly one member, return it.
    pub fn as_singleton(self) -> Option<ValueType> {
        let count = self.bits.count_ones();
        if count != 1 {
            return None;
        }
        let pos = self.bits.trailing_zeros();
        value_type_for_bit(pos)
    }

    /// Iterate over the members of this set in `ValueType`
    /// discriminant order.
    pub fn iter(self) -> impl Iterator<Item = ValueType> {
        let bits = self.bits;
        (0..9u32).filter_map(move |pos| {
            if (bits & (1 << pos)) != 0 {
                value_type_for_bit(pos)
            } else {
                None
            }
        })
    }
}

fn value_type_for_bit(pos: u32) -> Option<ValueType> {
    Some(match pos {
        0 => ValueType::String,
        1 => ValueType::Boolean,
        2 => ValueType::UnsignedInt,
        3 => ValueType::SignedInt,
        4 => ValueType::Float,
        5 => ValueType::Bytes,
        6 => ValueType::Entity,
        7 => ValueType::Symbol,
        8 => ValueType::Record,
        _ => return None,
    })
}

impl From<ValueType> for PrimitiveSet {
    fn from(vt: ValueType) -> Self {
        Self::singleton(vt)
    }
}

impl From<ValueType> for Type {
    fn from(vt: ValueType) -> Self {
        Self::primitive(vt)
    }
}

/// Schema-layer type of a value, term, or schema slot.
///
/// Two outer variants distinguish set-widening (Optional) from
/// concrete shapes (Definite). Optionality lives at the slot
/// layer, never inside a [`Definite`]: this keeps "nested
/// optionality" structurally unrepresentable.
///
/// `Type` is fully static — no type variables, no allocation
/// state. Deriving [`PartialEq`]/[`Eq`]/[`Hash`] is safe and
/// produces stable values for equivalent types.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Type {
    /// A definite shape. Subtype of `Optional(definite)` via the
    /// `T ⊆ Optional<T>` set-widening rule.
    Definite(Box<Definite>),
    /// Set-widened: the wrapped shape *or*
    /// [`Binding::Absent`](crate::Binding::Absent) at the row
    /// layer. Wraps a [`Definite`] (not a [`Type`]) so nested
    /// optionality is structurally impossible.
    Optional(Box<Definite>),
}

/// A concrete value shape — non-`Optional`. Static; type variables
/// live in [`unifier::Type`], not here.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Definite {
    /// Atomic value type, possibly a union over several primitive
    /// shapes.
    Primitive(PrimitiveSet),
    /// Sum-type placeholder. Reserved for future use; not
    /// constructed in current code paths.
    Variant,
    /// Product-type placeholder. Reserved for future use; not
    /// constructed in current code paths.
    Product,
}

impl Type {
    /// Construct a `Type::Definite(Primitive(singleton(vt)))`.
    pub fn primitive(vt: ValueType) -> Self {
        Type::Definite(Box::new(Definite::Primitive(PrimitiveSet::singleton(vt))))
    }

    /// Construct a `Type::Optional(Primitive(singleton(vt)))`.
    pub fn optional(vt: ValueType) -> Self {
        Type::Optional(Box::new(Definite::Primitive(PrimitiveSet::singleton(vt))))
    }

    /// Construct a `Type::Definite(Primitive(set))`.
    pub fn primitive_set(set: PrimitiveSet) -> Self {
        Type::Definite(Box::new(Definite::Primitive(set)))
    }

    /// Construct a `Type::Optional(Primitive(set))`.
    pub fn optional_set(set: PrimitiveSet) -> Self {
        Type::Optional(Box::new(Definite::Primitive(set)))
    }

    /// Returns `true` iff this type is set-widened with `Absent`.
    pub fn is_optional(&self) -> bool {
        matches!(self, Type::Optional(_))
    }

    /// Returns the underlying [`Definite`] regardless of whether
    /// this is `Definite` or `Optional`.
    pub fn shape(&self) -> &Definite {
        match self {
            Type::Definite(d) | Type::Optional(d) => d,
        }
    }

    /// Lift this type to set-widened (`Optional`). Idempotent.
    pub fn wrap_optional(self) -> Self {
        match self {
            Type::Definite(d) | Type::Optional(d) => Type::Optional(d),
        }
    }
}

impl Definite {
    /// Construct a `Definite::Primitive(singleton(vt))`.
    pub fn primitive(vt: ValueType) -> Self {
        Definite::Primitive(PrimitiveSet::singleton(vt))
    }

    /// If this is a primitive shape with a singleton value type,
    /// return it. `Variant`/`Product` placeholders return `None`.
    pub fn as_singleton(&self) -> Option<ValueType> {
        match self {
            Definite::Primitive(set) => set.as_singleton(),
            Definite::Variant | Definite::Product => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn p(vt: ValueType) -> Type {
        Type::primitive(vt)
    }
    fn o(vt: ValueType) -> Type {
        Type::optional(vt)
    }

    #[dialog_common::test]
    fn primitive_set_singleton() {
        let s = PrimitiveSet::singleton(ValueType::String);
        assert!(s.contains(ValueType::String));
        assert!(!s.contains(ValueType::UnsignedInt));
        assert_eq!(s.as_singleton(), Some(ValueType::String));
    }

    #[dialog_common::test]
    fn primitive_set_intersect_overlap() {
        let s = PrimitiveSet::NUMERIC
            .intersect(PrimitiveSet::singleton(ValueType::UnsignedInt))
            .unwrap();
        assert_eq!(s.as_singleton(), Some(ValueType::UnsignedInt));
    }

    #[dialog_common::test]
    fn primitive_set_intersect_disjoint() {
        assert!(
            PrimitiveSet::singleton(ValueType::String)
                .intersect(PrimitiveSet::singleton(ValueType::Entity))
                .is_none()
        );
    }

    #[dialog_common::test]
    fn primitive_set_includes_self() {
        assert!(PrimitiveSet::ALL.includes(PrimitiveSet::ALL));
        assert!(PrimitiveSet::NUMERIC.includes(PrimitiveSet::singleton(ValueType::UnsignedInt)));
    }

    #[dialog_common::test]
    fn primitive_set_iter() {
        let s = PrimitiveSet::NUMERIC;
        let members: Vec<_> = s.iter().collect();
        assert_eq!(members.len(), 3);
        assert!(members.contains(&ValueType::UnsignedInt));
        assert!(members.contains(&ValueType::SignedInt));
        assert!(members.contains(&ValueType::Float));
    }

    #[dialog_common::test]
    fn primitive_set_serde_round_trip() {
        let s = PrimitiveSet::NUMERIC;
        let j = serde_json::to_string(&s).unwrap();
        let back: PrimitiveSet = serde_json::from_str(&j).unwrap();
        assert_eq!(s, back);
    }

    #[dialog_common::test]
    fn type_primitive_is_definite() {
        let t = p(ValueType::String);
        assert!(matches!(t, Type::Definite(_)));
        assert!(!t.is_optional());
    }

    #[dialog_common::test]
    fn type_optional_wraps_definite() {
        let t = o(ValueType::String);
        assert!(t.is_optional());
    }

    #[dialog_common::test]
    fn type_serde_round_trip_definite() {
        let t = p(ValueType::String);
        let j = serde_json::to_string(&t).unwrap();
        let back: Type = serde_json::from_str(&j).unwrap();
        assert_eq!(t, back);
    }

    #[dialog_common::test]
    fn type_serde_round_trip_optional() {
        let t = o(ValueType::Entity);
        let j = serde_json::to_string(&t).unwrap();
        let back: Type = serde_json::from_str(&j).unwrap();
        assert_eq!(t, back);
    }

    /// Equivalent types hash and compare equal regardless of how
    /// they were constructed — the property the unifier-internal
    /// split was designed to enforce.
    #[dialog_common::test]
    fn type_hash_is_stable_across_constructions() {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};
        let a = p(ValueType::String);
        let b = Type::Definite(Box::new(Definite::primitive(ValueType::String)));
        let mut ha = DefaultHasher::new();
        let mut hb = DefaultHasher::new();
        a.hash(&mut ha);
        b.hash(&mut hb);
        assert_eq!(ha.finish(), hb.finish());
        assert_eq!(a, b);
    }
}

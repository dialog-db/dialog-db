//! Unified type system for the query engine.
//!
//! This module defines the static, derive-friendly user-facing
//! [`Type`] used by [`Term`](crate::Term),
//! [`Equality`](crate::constraint::Equality),
//! [`Premise`](crate::Premise), [`DeductiveRule`](crate::DeductiveRule),
//! and the schema layer.
//!
//! A [`Type`] is the set of admissible value shapes a slot may
//! bind. It is built from a [`Primitive`] bitfield (the atomic
//! shapes, plus a `Nothing` bit for set-widened optionality) and an
//! optional set of [`Composite`] shapes (Product records, Variant
//! tags). The two parts compose by set-union: a value satisfies
//! [`Type`] iff it inhabits at least one of the primitive bits or
//! one of the composite shapes.
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
use crate::artifact::Value;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::fmt::{Display, Formatter, Result as FmtResult};

/// A bitfield over [`ValueType`] variants plus a `Nothing` bit —
/// the set of admissible primitive shapes.
///
/// Used everywhere a "kind constraint" is needed: type variables
/// declare their constraint as a `Primitive`, formula schemas
/// declare per-variable constraints, and unification intersects
/// them. The `Nothing` bit (position 9) is a synthetic atom with
/// no corresponding [`ValueType`]; it marks "Absent" admissibility
/// at the row layer — the new home for what used to be the outer
/// `Optional` variant.
#[derive(
    Clone, Copy, Debug, Default, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize,
)]
pub struct Primitive {
    /// One bit per [`ValueType`] variant (positions 0-8), plus a
    /// `Nothing` bit at position 9.
    bits: u16,
}

impl Display for Primitive {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        if *self == Self::EMPTY {
            return write!(f, "Never");
        }
        if self.without_nothing() == Self::ALL {
            write!(f, "Value")?;
        } else {
            const ATOMS: [ValueType; 9] = [
                ValueType::String,
                ValueType::Boolean,
                ValueType::UnsignedInt,
                ValueType::SignedInt,
                ValueType::Float,
                ValueType::Bytes,
                ValueType::Entity,
                ValueType::Symbol,
                ValueType::Record,
            ];
            let mut first = true;
            for atom in ATOMS {
                if self.contains(atom) {
                    if !first {
                        write!(f, "|")?;
                    }
                    write!(f, "{atom}")?;
                    first = false;
                }
            }
            if first {
                // Only the Nothing bit is set.
                return write!(f, "Nothing");
            }
        }
        if self.contains_nothing() {
            write!(f, "|Nothing")?;
        }
        Ok(())
    }
}

/// Bit position for the synthetic `Nothing` atom.
const NOTHING_BIT: u16 = 1 << 9;

impl Primitive {
    /// Empty set — no admissible shapes. Rejected at unification
    /// time.
    pub const EMPTY: Self = Self { bits: 0 };

    /// Every concrete [`ValueType`] variant, without `Nothing`.
    /// This is the right default for constraint widening — a
    /// variable that "accepts anything" still demands a Present
    /// value.
    pub const ALL: Self = Self {
        bits: 0b1_1111_1111,
    };

    /// Every shape including the `Nothing` atom — the broadest
    /// possible set, including row-level absence.
    pub const ANY: Self = Self {
        bits: Self::ALL.bits | NOTHING_BIT,
    };

    /// Singleton set containing only the `Nothing` atom.
    pub const NOTHING: Self = Self { bits: NOTHING_BIT };

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

    /// Returns `true` iff `vt` is a member of this set.
    pub fn contains(self, vt: ValueType) -> bool {
        (self.bits & Self::bit_for(vt)) != 0
    }

    /// Returns `true` iff the `Nothing` atom is a member.
    pub fn contains_nothing(self) -> bool {
        (self.bits & NOTHING_BIT) != 0
    }

    /// Returns a copy of this set with the `Nothing` atom removed.
    /// Used to strip set-widening from an `Optional<T>` type back
    /// to its underlying `T` shape.
    pub fn without_nothing(self) -> Self {
        Self {
            bits: self.bits & !NOTHING_BIT,
        }
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

    /// If this set has exactly one `ValueType` member (and nothing
    /// else, not even `Nothing`), return it. A `Nothing`-only set
    /// returns `None` — it has no `ValueType`.
    pub fn as_singleton(self) -> Option<ValueType> {
        if self.bits.count_ones() != 1 {
            return None;
        }
        let pos = self.bits.trailing_zeros();
        value_type_for_bit(pos)
    }

    /// Iterate the [`ValueType`] members in discriminant order.
    /// Does **not** yield the `Nothing` atom.
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

impl From<ValueType> for Primitive {
    fn from(vt: ValueType) -> Self {
        Self::singleton(vt)
    }
}

impl From<ValueType> for Type {
    fn from(vt: ValueType) -> Self {
        Self::primitive(vt)
    }
}

/// Schema-layer type — the set of admissible value shapes a slot
/// can bind.
///
/// A [`Type`] is the union of two parts:
///
/// - A [`Primitive`] bitfield carrying atomic [`ValueType`] bits
///   plus an optional `Nothing` atom (row-level absence).
/// - An optional non-empty set of [`Composite`] shapes for
///   structured values (records, variants).
///
/// Smart constructors enforce the invariant that
/// `Composite(_, set)` always has `!set.is_empty()`; a request to
/// build a composite with an empty set collapses to a plain
/// `Primitive`.
///
/// Composites live in a [`BTreeSet`] rather than a [`HashSet`].
/// Two reasons:
///
impl Display for Type {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        match self {
            Type::Primitive(p) => write!(f, "{p}"),
            Type::Composite(p, c) => write!(f, "{p}+{} composite", c.len()),
        }
    }
}

/// 1. `Type` derives [`Hash`]. `BTreeSet` iterates in a stable
///    `Ord`-based order, so the derived hash is canonical:
///    `Type::Composite(p, {a, b})` and `Type::Composite(p, {b, a})`
///    have identical hashes. `HashSet` does not implement `Hash`
///    in std, and any hand-rolled order-independent hash would
///    have to sort internally anyway — `BTreeSet` is that already.
///
/// 2. Insertion order at construction never affects equality.
///    Building the same set via different paths (different unions,
///    different insert orders) yields equal [`Type`]s.
///
/// The cost is requiring [`Ord`] on [`Composite`], [`Type`], and
/// [`Primitive`]. The ordering is structural plumbing — it has no
/// semantic meaning (no claim that `u32 < String`); it exists only
/// to keep the set canonical.
///
/// `Type` is fully static — no type variables, no allocation
/// state. Deriving [`PartialEq`]/[`Eq`]/[`Hash`] is safe and
/// produces stable values for equivalent types.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Type {
    /// Pure primitive set (no composite shapes admitted).
    Primitive(Primitive),
    /// Primitive set together with at least one composite shape.
    /// Invariant: the composite set is never empty.
    Composite(Primitive, BTreeSet<Composite>),
}

/// A composite (structured) value shape. Ordered by derived
/// [`Ord`] for stable iteration and hashing inside a [`BTreeSet`].
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Composite {
    /// Product (record) type with named fields. Field names ordered
    /// by [`BTreeMap`] for stable equality and hashing.
    Product(BTreeMap<String, Type>),
    /// Sum-type variant carrying a single label and its payload.
    Variant {
        /// The discriminator label.
        label: String,
        /// The payload type for this label.
        value: Type,
    },
}

impl Type {
    /// Construct a [`Type::Primitive`] containing the single
    /// `ValueType`.
    pub fn primitive(vt: ValueType) -> Type {
        Type::Primitive(Primitive::singleton(vt))
    }

    /// Construct a [`Type::Primitive`] wrapping the given bitfield.
    pub fn primitive_set(p: Primitive) -> Type {
        Type::Primitive(p)
    }

    /// The type whose only inhabitant is the `Nothing` atom — the
    /// "absent" row-layer value.
    pub fn nothing() -> Type {
        Type::Primitive(Primitive::NOTHING)
    }

    /// Widen this type to also admit `Nothing` — the new home for
    /// what used to be the outer `Optional` variant. Idempotent.
    pub fn optional(self) -> Type {
        match self {
            Type::Primitive(p) => Type::Primitive(p.union(Primitive::NOTHING)),
            Type::Composite(p, c) => Type::Composite(p.union(Primitive::NOTHING), c),
        }
    }

    /// True when the given runtime value inhabits this type: the
    /// value's data type is a member of the primitive part.
    /// Composite refinements are not yet checked — no composite
    /// values flow through evaluation today.
    pub fn admits(&self, value: &Value) -> bool {
        self.primitive_part().contains(value.data_type())
    }

    /// The inverse of [`optional`]: strip the `Nothing` atom if
    /// present, yielding the underlying non-widened type.
    /// Idempotent.
    pub fn without_nothing(self) -> Type {
        match self {
            Type::Primitive(p) => Type::Primitive(p.without_nothing()),
            Type::Composite(p, c) => Type::Composite(p.without_nothing(), c),
        }
    }

    /// Smart constructor. Collapses `Composite(p, empty)` to
    /// `Primitive(p)`. Use this rather than building the
    /// `Composite` variant directly.
    pub fn composite(primitive: Primitive, composite: BTreeSet<Composite>) -> Type {
        if composite.is_empty() {
            Type::Primitive(primitive)
        } else {
            Type::Composite(primitive, composite)
        }
    }

    /// Build a record type from named fields.
    pub fn product(fields: BTreeMap<String, Type>) -> Type {
        let mut set = BTreeSet::new();
        set.insert(Composite::Product(fields));
        Type::Composite(Primitive::EMPTY, set)
    }

    /// Build a singleton variant type from a label and payload.
    pub fn variant(label: impl Into<String>, value: Type) -> Type {
        let mut set = BTreeSet::new();
        set.insert(Composite::Variant {
            label: label.into(),
            value,
        });
        Type::Composite(Primitive::EMPTY, set)
    }

    /// Set intersection over both primitive and composite parts.
    /// Returns `None` when the result is empty — no admissible
    /// shapes survive.
    ///
    /// Composite shapes narrow structurally: two
    /// `Composite::Product` values with the same field-name set
    /// intersect their field types recursively (if any field
    /// intersection is empty, the product is eliminated). Products
    /// with disjoint field-name sets do not intersect — they
    /// describe disjoint records. Variants intersect by matching
    /// label, recursing into payload types.
    pub fn intersect(&self, other: &Type) -> Option<Type> {
        let p_self = self.primitive_part();
        let p_other = other.primitive_part();
        let p = Primitive {
            bits: p_self.bits & p_other.bits,
        };

        let composite_intersection: BTreeSet<Composite> =
            match (self.composite_part(), other.composite_part()) {
                (Some(a), Some(b)) => intersect_composite_sets(a, b),
                _ => BTreeSet::new(),
            };

        if p.is_empty() && composite_intersection.is_empty() {
            None
        } else {
            Some(Type::composite(p, composite_intersection))
        }
    }

    /// Set union over both primitive and composite parts.
    pub fn union(&self, other: &Type) -> Type {
        let p = self.primitive_part().union(other.primitive_part());
        let mut composites: BTreeSet<Composite> = BTreeSet::new();
        if let Some(s) = self.composite_part() {
            composites.extend(s.iter().cloned());
        }
        if let Some(s) = other.composite_part() {
            composites.extend(s.iter().cloned());
        }
        Type::composite(p, composites)
    }

    /// Subtype check. Returns `true` iff every shape `other`
    /// admits is also admitted by `self`.
    ///
    /// For composites: each shape in `other` must be included by
    /// some shape in `self`. Inclusion is structural — a Product
    /// `{x: T1, y: T2}` includes a Product `{x: T1', y: T2'}` when
    /// the field-name sets match and each `Ti` includes `Ti'`. A
    /// product with extra fields includes one with fewer fields
    /// (width subtyping is not assumed; equal field sets only).
    pub fn includes(&self, other: &Type) -> bool {
        if !self.primitive_part().includes(other.primitive_part()) {
            return false;
        }
        match (self.composite_part(), other.composite_part()) {
            (_, None) => true,
            (None, Some(b)) => b.is_empty(),
            (Some(a), Some(b)) => b
                .iter()
                .all(|b_shape| a.iter().any(|a_shape| composite_includes(a_shape, b_shape))),
        }
    }

    /// Returns `true` iff this type admits the `Nothing` atom.
    pub fn is_optional(&self) -> bool {
        self.primitive_part().contains_nothing()
    }

    /// The primitive part of this type, regardless of variant.
    pub fn primitive_part(&self) -> Primitive {
        match self {
            Type::Primitive(p) => *p,
            Type::Composite(p, _) => *p,
        }
    }

    /// The composite part of this type, or `None` if the type has
    /// only a primitive part.
    pub fn composite_part(&self) -> Option<&BTreeSet<Composite>> {
        match self {
            Type::Primitive(_) => None,
            Type::Composite(_, c) => Some(c),
        }
    }

    /// Legacy storage-codec view: if this type reduces to exactly
    /// one [`ValueType`] (no `Nothing`, no composites), return it.
    pub fn as_value_type(&self) -> Option<ValueType> {
        match self {
            Type::Primitive(p) => p.as_singleton(),
            Type::Composite(_, _) => None,
        }
    }
}

/// Structurally intersect two sets of [`Composite`] shapes.
///
/// For each `Composite::Product` in `a`, look for a same-field-set
/// `Composite::Product` in `b` and intersect their field types
/// pairwise; if any field intersection is empty, the product is
/// eliminated. For each `Composite::Variant` in `a`, look for a
/// same-label `Composite::Variant` in `b` and intersect their
/// payload types. Anything in `a` or `b` without a structural
/// counterpart on the other side is dropped — set intersection
/// only keeps shapes admitted by both sides.
fn intersect_composite_sets(
    a: &BTreeSet<Composite>,
    b: &BTreeSet<Composite>,
) -> BTreeSet<Composite> {
    let mut result = BTreeSet::new();
    for a_shape in a {
        for b_shape in b {
            if let Some(merged) = intersect_composite(a_shape, b_shape) {
                result.insert(merged);
            }
        }
    }
    result
}

/// Pairwise intersection of two [`Composite`] shapes. Two
/// products intersect iff they share the same set of field names;
/// two variants iff they share the same label.
fn intersect_composite(a: &Composite, b: &Composite) -> Option<Composite> {
    match (a, b) {
        (Composite::Product(fa), Composite::Product(fb)) => {
            if fa.len() != fb.len() {
                return None;
            }
            let mut out = BTreeMap::new();
            for (name, ta) in fa {
                let tb = fb.get(name)?;
                let merged = ta.intersect(tb)?;
                out.insert(name.clone(), merged);
            }
            Some(Composite::Product(out))
        }
        (
            Composite::Variant {
                label: la,
                value: va,
            },
            Composite::Variant {
                label: lb,
                value: vb,
            },
        ) => {
            if la != lb {
                return None;
            }
            let merged = va.intersect(vb)?;
            Some(Composite::Variant {
                label: la.clone(),
                value: merged,
            })
        }
        _ => None,
    }
}

/// Structural inclusion check between two [`Composite`] shapes.
/// `a` includes `b` iff they are the same shape kind, have matching
/// keys/labels, and each of `a`'s recursive types includes `b`'s.
fn composite_includes(a: &Composite, b: &Composite) -> bool {
    match (a, b) {
        (Composite::Product(fa), Composite::Product(fb)) => {
            if fa.len() != fb.len() {
                return false;
            }
            for (name, ta) in fa {
                let Some(tb) = fb.get(name) else {
                    return false;
                };
                if !ta.includes(tb) {
                    return false;
                }
            }
            true
        }
        (
            Composite::Variant {
                label: la,
                value: va,
            },
            Composite::Variant {
                label: lb,
                value: vb,
            },
        ) => la == lb && va.includes(vb),
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash as HashTrait, Hasher};

    fn hash_of<T: HashTrait>(t: &T) -> u64 {
        let mut h = DefaultHasher::new();
        t.hash(&mut h);
        h.finish()
    }

    fn p(vt: ValueType) -> Type {
        Type::primitive(vt)
    }
    fn o(vt: ValueType) -> Type {
        Type::primitive(vt).optional()
    }

    #[dialog_common::test]
    fn primitive_singleton() {
        let s = Primitive::singleton(ValueType::String);
        assert!(s.contains(ValueType::String));
        assert!(!s.contains(ValueType::UnsignedInt));
        assert!(!s.contains_nothing());
        assert_eq!(s.as_singleton(), Some(ValueType::String));
    }

    #[dialog_common::test]
    fn primitive_intersect_overlap() {
        let s = Primitive::NUMERIC
            .intersect(Primitive::singleton(ValueType::UnsignedInt))
            .unwrap();
        assert_eq!(s.as_singleton(), Some(ValueType::UnsignedInt));
    }

    #[dialog_common::test]
    fn primitive_intersect_disjoint() {
        assert!(
            Primitive::singleton(ValueType::String)
                .intersect(Primitive::singleton(ValueType::Entity))
                .is_none()
        );
    }

    #[dialog_common::test]
    fn primitive_includes_self() {
        assert!(Primitive::ALL.includes(Primitive::ALL));
        assert!(Primitive::NUMERIC.includes(Primitive::singleton(ValueType::UnsignedInt)));
    }

    #[dialog_common::test]
    fn primitive_iter_skips_nothing() {
        let s = Primitive::NUMERIC.union(Primitive::NOTHING);
        let members: Vec<_> = s.iter().collect();
        assert_eq!(members.len(), 3);
        assert!(members.contains(&ValueType::UnsignedInt));
        assert!(members.contains(&ValueType::SignedInt));
        assert!(members.contains(&ValueType::Float));
        assert!(s.contains_nothing());
    }

    #[dialog_common::test]
    fn primitive_nothing_singleton_returns_none() {
        assert_eq!(Primitive::NOTHING.as_singleton(), None);
    }

    #[dialog_common::test]
    fn primitive_all_excludes_nothing() {
        assert!(!Primitive::ALL.contains_nothing());
        assert!(Primitive::ANY.contains_nothing());
        assert!(Primitive::ANY.includes(Primitive::ALL));
        assert!(Primitive::ANY.includes(Primitive::NOTHING));
    }

    #[dialog_common::test]
    fn primitive_serde_round_trip() {
        let s = Primitive::NUMERIC;
        let j = serde_json::to_string(&s).unwrap();
        let back: Primitive = serde_json::from_str(&j).unwrap();
        assert_eq!(s, back);
    }

    #[dialog_common::test]
    fn type_primitive_is_not_optional() {
        let t = p(ValueType::String);
        assert!(matches!(t, Type::Primitive(_)));
        assert!(!t.is_optional());
    }

    #[dialog_common::test]
    fn type_optional_widens_with_nothing() {
        let t = o(ValueType::String);
        assert!(t.is_optional());
        assert_eq!(t.as_value_type(), None);
        assert_eq!(t.primitive_part().as_singleton(), None);
        assert!(t.primitive_part().contains(ValueType::String));
        assert!(t.primitive_part().contains_nothing());
    }

    #[dialog_common::test]
    fn type_optional_is_idempotent() {
        let a = p(ValueType::String).optional();
        let b = a.clone().optional();
        assert_eq!(a, b);
    }

    #[dialog_common::test]
    fn type_serde_round_trip_primitive() {
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
    /// they were constructed.
    #[dialog_common::test]
    fn type_hash_is_stable_across_constructions() {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};
        let a = p(ValueType::String);
        let b = Type::primitive_set(Primitive::singleton(ValueType::String));
        let mut ha = DefaultHasher::new();
        let mut hb = DefaultHasher::new();
        a.hash(&mut ha);
        b.hash(&mut hb);
        assert_eq!(ha.finish(), hb.finish());
        assert_eq!(a, b);
    }

    #[dialog_common::test]
    fn product_constructor_round_trip() {
        let mut fields = BTreeMap::new();
        fields.insert("name".to_string(), Type::primitive(ValueType::String));
        fields.insert("age".to_string(), Type::primitive(ValueType::UnsignedInt));
        let a = Type::product(fields.clone());
        let b = Type::product(fields);
        assert_eq!(a, b);
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};
        let mut ha = DefaultHasher::new();
        let mut hb = DefaultHasher::new();
        a.hash(&mut ha);
        b.hash(&mut hb);
        assert_eq!(ha.finish(), hb.finish());
    }

    #[dialog_common::test]
    fn variant_constructor_carries_label_and_payload() {
        let some = Type::variant("Some", Type::primitive(ValueType::String));
        let composites = some.composite_part().unwrap();
        assert_eq!(composites.len(), 1);
        let first = composites.iter().next().unwrap();
        match first {
            Composite::Variant { label, value } => {
                assert_eq!(label, "Some");
                assert_eq!(*value, Type::primitive(ValueType::String));
            }
            other => panic!("expected Variant, got {:?}", other),
        }
    }

    #[dialog_common::test]
    fn intersect_two_records_same_fields() {
        let mut fields = BTreeMap::new();
        fields.insert("x".to_string(), Type::primitive(ValueType::String));
        let a = Type::product(fields.clone());
        let b = Type::product(fields);
        let c = a.intersect(&b).expect("intersection non-empty");
        assert_eq!(a, c);
    }

    /// Two products with the same field names but different field
    /// types narrow structurally: intersect each field's type
    /// recursively. If every field intersection is non-empty, the
    /// product survives with the narrowed fields.
    #[dialog_common::test]
    fn intersect_products_narrows_field_types() {
        let mut a_fields = BTreeMap::new();
        a_fields.insert("x".to_string(), Type::primitive_set(Primitive::NUMERIC));
        let a = Type::product(a_fields);

        let mut b_fields = BTreeMap::new();
        b_fields.insert("x".to_string(), Type::primitive(ValueType::UnsignedInt));
        let b = Type::product(b_fields);

        let merged = a.intersect(&b).expect("narrows to UnsignedInt");
        let composite = merged.composite_part().expect("composite present");
        let product = composite.iter().next().expect("one product");
        match product {
            Composite::Product(fields) => {
                let x_type = fields.get("x").expect("x field");
                assert_eq!(x_type.as_value_type(), Some(ValueType::UnsignedInt));
            }
            other => panic!("expected Product, got {other:?}"),
        }
    }

    /// Two products with disjoint field types fail to intersect.
    #[dialog_common::test]
    fn intersect_products_disjoint_fields_eliminated() {
        let mut a_fields = BTreeMap::new();
        a_fields.insert("x".to_string(), Type::primitive(ValueType::String));
        let a = Type::product(a_fields);

        let mut b_fields = BTreeMap::new();
        b_fields.insert("x".to_string(), Type::primitive(ValueType::UnsignedInt));
        let b = Type::product(b_fields);

        assert!(a.intersect(&b).is_none());
    }

    /// Two products with different field-name sets do not
    /// intersect — they describe disjoint record shapes.
    #[dialog_common::test]
    fn intersect_products_different_keys_eliminated() {
        let mut a_fields = BTreeMap::new();
        a_fields.insert("x".to_string(), Type::primitive(ValueType::String));
        let a = Type::product(a_fields);

        let mut b_fields = BTreeMap::new();
        b_fields.insert("y".to_string(), Type::primitive(ValueType::String));
        let b = Type::product(b_fields);

        assert!(a.intersect(&b).is_none());
    }

    /// Two variants with the same label intersect their payloads;
    /// different labels do not intersect.
    #[dialog_common::test]
    fn intersect_variants_by_label() {
        let a = Type::variant("Some", Type::primitive_set(Primitive::NUMERIC));
        let b = Type::variant("Some", Type::primitive(ValueType::UnsignedInt));
        let merged = a.intersect(&b).expect("same label narrows payload");
        let composite = merged.composite_part().expect("composite present");
        match composite.iter().next().expect("one variant") {
            Composite::Variant { label, value } => {
                assert_eq!(label, "Some");
                assert_eq!(value.as_value_type(), Some(ValueType::UnsignedInt));
            }
            other => panic!("expected Variant, got {other:?}"),
        }

        let c = Type::variant("Other", Type::primitive(ValueType::UnsignedInt));
        assert!(a.intersect(&c).is_none());
    }

    #[dialog_common::test]
    fn intersect_primitive_and_composite_keeps_primitive_only() {
        let mut fields = BTreeMap::new();
        fields.insert("x".to_string(), Type::primitive(ValueType::String));
        let composite = Type::product(fields);
        let prim = Type::primitive(ValueType::String);
        // primitive part of `composite` is EMPTY; composite part of `prim` is None.
        // Intersection of primitive parts: EMPTY ∩ {String} = EMPTY.
        // Intersection of composite parts: None ∩ {Product{...}} = empty.
        // Overall: empty → None.
        assert!(prim.intersect(&composite).is_none());

        // But if the composite carries a Record primitive bit, the
        // intersection has that bit alone.
        let composite_with_record = Type::composite(
            Primitive::singleton(ValueType::Record),
            composite
                .composite_part()
                .cloned()
                .unwrap_or_else(BTreeSet::new),
        );
        let record_prim = Type::primitive(ValueType::Record);
        let intersected = record_prim.intersect(&composite_with_record).unwrap();
        assert_eq!(
            intersected.primitive_part().as_singleton(),
            Some(ValueType::Record)
        );
        assert!(intersected.composite_part().is_none());
    }

    #[dialog_common::test]
    fn includes_product_subtype_extra_fields_in_subject() {
        // A type T "includes" U iff T admits every shape U admits.
        // For our set semantics, T includes U requires every
        // composite of U to be present in T's composite set. So
        // larger composite sets in T are fine; an extra Product in
        // U not in T breaks inclusion.
        let mut a_fields = BTreeMap::new();
        a_fields.insert("x".to_string(), Type::primitive(ValueType::String));
        let a = Type::product(a_fields.clone());

        let mut b_fields = BTreeMap::new();
        b_fields.insert("x".to_string(), Type::primitive(ValueType::String));
        b_fields.insert("y".to_string(), Type::primitive(ValueType::UnsignedInt));
        let b = Type::product(b_fields);

        // a and b are different products; neither includes the other.
        assert!(!a.includes(&b));
        assert!(!b.includes(&a));

        // a includes itself.
        assert!(a.includes(&a));

        // Union of {a, b} includes both.
        let union = a.union(&b);
        assert!(union.includes(&a));
        assert!(union.includes(&b));
    }

    #[dialog_common::test]
    fn smart_constructor_collapses_empty_composite_set() {
        let collapsed = Type::composite(Primitive::singleton(ValueType::String), BTreeSet::new());
        assert!(matches!(collapsed, Type::Primitive(_)));
        assert_eq!(collapsed.as_value_type(), Some(ValueType::String));
    }

    #[dialog_common::test]
    fn nothing_constructor_yields_only_nothing() {
        let t = Type::nothing();
        assert!(t.is_optional());
        assert_eq!(t.primitive_part().as_singleton(), None);
        assert_eq!(t.as_value_type(), None);
    }

    #[dialog_common::test]
    fn as_value_type_unique_singleton() {
        assert_eq!(
            p(ValueType::String).as_value_type(),
            Some(ValueType::String)
        );
        assert_eq!(o(ValueType::String).as_value_type(), None);
        assert_eq!(
            Type::primitive_set(Primitive::NUMERIC).as_value_type(),
            None
        );
    }

    /// Insertion order of composite elements does not affect
    /// equality or hash. The [`BTreeSet`] storage canonicalizes
    /// the set, so different paths to the same set of shapes
    /// produce equal `Type`s with equal hashes.
    #[dialog_common::test]
    fn composite_set_is_insertion_order_independent() {
        let a = Type::variant("A", p(ValueType::String));
        let b = Type::variant("B", p(ValueType::UnsignedInt));

        // Build the same multi-element set two different ways:
        // ({A, B}) via a.union(b), and ({B, A}) via b.union(a).
        let ab = a.union(&b);
        let ba = b.union(&a);

        assert_eq!(ab, ba, "set equality is order-independent");
        assert_eq!(hash_of(&ab), hash_of(&ba), "set hash is order-independent");
    }

    /// Field insertion order into a `BTreeMap<String, Type>` does
    /// not affect product equality or hash — the BTreeMap sorts
    /// by key. Same product built via `{x, y}` vs `{y, x}` insert
    /// orders must compare equal and hash equal.
    #[dialog_common::test]
    fn product_field_insertion_order_independent() {
        let mut a_fields = BTreeMap::new();
        a_fields.insert("x".to_string(), Type::primitive(ValueType::String));
        a_fields.insert("y".to_string(), Type::primitive(ValueType::UnsignedInt));
        let a = Type::product(a_fields);

        let mut b_fields = BTreeMap::new();
        b_fields.insert("y".to_string(), Type::primitive(ValueType::UnsignedInt));
        b_fields.insert("x".to_string(), Type::primitive(ValueType::String));
        let b = Type::product(b_fields);

        assert_eq!(
            a, b,
            "product equality is field-insertion-order independent"
        );
        assert_eq!(
            hash_of(&a),
            hash_of(&b),
            "product hash is field-insertion-order independent"
        );
    }

    /// Combining two products via union also produces identical
    /// hashes regardless of which product was unioned first.
    #[dialog_common::test]
    fn product_union_is_order_independent() {
        let mut a_fields = BTreeMap::new();
        a_fields.insert("x".to_string(), Type::primitive(ValueType::String));
        let a = Type::product(a_fields);

        let mut b_fields = BTreeMap::new();
        b_fields.insert("y".to_string(), Type::primitive(ValueType::UnsignedInt));
        let b = Type::product(b_fields);

        let ab = a.union(&b);
        let ba = b.union(&a);

        assert_eq!(ab, ba);
        assert_eq!(hash_of(&ab), hash_of(&ba));
    }
}

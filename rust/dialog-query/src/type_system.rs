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
//! Type variables (used for compile-time unification of rules)
//! live in the [`unifier`] submodule, never in the user-facing
//! [`Type`]. This split lets the user-facing types derive
//! [`PartialEq`]/[`Eq`]/[`Hash`] cleanly: equivalent rules produce
//! stable hashes regardless of how many anonymous variables they
//! have.

pub mod unifier;

use crate::artifact::Type as ValueType;
use crate::artifact::Value;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::fmt::{Display, Formatter, Result as FmtResult};

/// A bitfield over [`ValueType`] variants plus a `Nothing` bit:
/// the set of admissible primitive shapes.
///
/// Used everywhere a "kind constraint" is needed: type variables
/// declare their constraint as a `Primitive`, formula schemas
/// declare per-variable constraints, and unification intersects
/// them. The `Nothing` bit (position 9) is a synthetic atom with
/// no corresponding [`ValueType`]; it marks an admissible absent
/// value at the row layer.
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
        if self.required() == Self::ALL {
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
    /// Empty set: no admissible shapes. Rejected at unification
    /// time.
    pub const EMPTY: Self = Self { bits: 0 };

    /// Every concrete [`ValueType`] variant, without `Nothing`.
    /// This is the right default for constraint widening: a
    /// variable that "accepts anything" still demands a Present
    /// value.
    pub const ALL: Self = Self {
        bits: 0b1_1111_1111,
    };

    /// Every shape including the `Nothing` atom: the broadest
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

    /// Textual primitives: the types whose values have a lexical
    /// form a prefix predicate can range over — strings, symbols
    /// (attribute names), and entities (URIs). The bound for
    /// `starts-with`-style predicates: one predicate over TEXTUAL
    /// instead of per-type variants, with each member's lexical
    /// grammar deciding whether a given prefix can match it at all.
    pub const TEXTUAL: Self = Self {
        bits: Self::STRING_LIKE.bits | Self::bit_for(ValueType::Entity),
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

    /// Returns a copy of this set with the `Nothing` atom removed,
    /// leaving only the present-value shapes it admits.
    pub fn required(self) -> Self {
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
    /// returns `None`; it has no `ValueType`.
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
        Type::Primitive(Primitive::singleton(vt))
    }
}

impl From<Primitive> for Type {
    fn from(primitive: Primitive) -> Self {
        Type::Primitive(primitive)
    }
}

/// Schema-layer type: the set of admissible value shapes a slot
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
            Type::Refined(p, r) => write!(f, "{p}[starts-with {:?}]", r.prefix),
        }
    }
}

/// 1. `Type` derives [`Hash`]. `BTreeSet` iterates in a stable
///    `Ord`-based order, so the derived hash is canonical:
///    `Type::Composite(p, {a, b})` and `Type::Composite(p, {b, a})`
///    have identical hashes. `HashSet` does not implement `Hash`
///    in std, and any hand-rolled order-independent hash would
///    have to sort internally anyway, and `BTreeSet` is that already.
///
/// 2. Insertion order at construction never affects equality.
///    Building the same set via different paths (different unions,
///    different insert orders) yields equal [`Type`]s.
///
/// The cost is requiring [`Ord`] on [`Composite`], [`Type`], and
/// [`Primitive`]. The ordering is structural plumbing; it has no
/// semantic meaning (no claim that `u32 < String`); it exists only
/// to keep the set canonical.
///
/// `Type` is fully static: no type variables, no allocation
/// state. Deriving [`PartialEq`]/[`Eq`]/[`Hash`] is safe and
/// produces stable values for equivalent types.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Type {
    /// Pure primitive set (no composite shapes admitted).
    Primitive(Primitive),
    /// The union of a primitive set and a non-empty set of composite
    /// shapes: a value satisfies it if it inhabits one of the
    /// primitive bits OR matches one of the composite shapes (not
    /// the intersection of the two). The primitive set may be empty,
    /// giving a pure composite type. Invariant: the composite set is
    /// never empty (an empty one collapses to `Primitive`).
    Composite(Primitive, BTreeSet<Composite>),
    /// Primitive set narrowed by a [`Refinement`] on the *values*
    /// of the member types — not just which types are admitted, but
    /// which of their inhabitants. Produced by refinement
    /// predicates (`starts-with`); consumed by scan-range pushdown
    /// and, like every kind, by [`Type::admits`] at the data
    /// boundary. Admits no composite shapes (refinements constrain
    /// lexical forms, which composites do not have).
    Refined(Primitive, Refinement),
}

/// A value-level constraint layered onto a primitive membership
/// set. The meet of two refinements is their conjunction (both
/// constraints), the join their weakest common implication — see
/// [`Refinement::meet`] and [`Refinement::join`].
///
/// Today one refinement exists: a lexical prefix over the TEXTUAL
/// kinds. Numeric intervals and Entity concept-membership (M3)
/// extend this struct rather than adding lattice variants.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct Refinement {
    /// Lexical prefix every admitted value must begin with.
    /// Invariant: non-empty (an empty prefix is no refinement; the
    /// constructors collapse it).
    pub prefix: String,
}

impl Refinement {
    /// Meet: the conjunction of both constraints. Two prefixes are
    /// jointly satisfiable iff one extends the other, and the meet
    /// is the longer; disjoint prefixes admit nothing.
    fn meet(&self, other: &Refinement) -> Option<Refinement> {
        if self.prefix.starts_with(&other.prefix) {
            Some(self.clone())
        } else if other.prefix.starts_with(&self.prefix) {
            Some(other.clone())
        } else {
            None
        }
    }

    /// Join: the weakest constraint both sides imply — the longest
    /// common prefix. `None` when nothing is common (the join
    /// carries no refinement).
    fn join(&self, other: &Refinement) -> Option<Refinement> {
        let common: String = self
            .prefix
            .chars()
            .zip(other.prefix.chars())
            .take_while(|(a, b)| a == b)
            .map(|(a, _)| a)
            .collect();
        if common.is_empty() {
            None
        } else {
            Some(Refinement { prefix: common })
        }
    }

    /// True when the value's lexical form satisfies the refinement.
    /// Values without a lexical form satisfy no prefix.
    pub fn admits(&self, value: &Value) -> bool {
        lexical_form(value).is_some_and(|form| form.starts_with(&self.prefix))
    }
}

/// The lexical form of a value, if its kind has one: the string
/// content, the symbol's `namespace/predicate` name, or the
/// entity's URI. This is the form prefix refinements and the
/// `starts-with` predicate compare against.
pub fn lexical_form(value: &Value) -> Option<String> {
    match value {
        Value::String(content) => Some(content.clone()),
        Value::Symbol(symbol) => Some(String::from(symbol)),
        Value::Entity(entity) => Some(entity.as_str().to_string()),
        _ => None,
    }
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
    /// The type whose only inhabitant is the `Nothing` atom: the
    /// "absent" row-layer value.
    pub fn nothing() -> Type {
        Type::Primitive(Primitive::NOTHING)
    }

    /// Widen this type to also admit the `Nothing` atom, marking an
    /// admissible absent value. Idempotent.
    pub fn optional(self) -> Type {
        match self {
            Type::Primitive(p) => Type::Primitive(p.union(Primitive::NOTHING)),
            Type::Composite(p, c) => Type::Composite(p.union(Primitive::NOTHING), c),
            Type::Refined(p, r) => Type::Refined(p.union(Primitive::NOTHING), r),
        }
    }

    /// True when the given runtime value inhabits this type: the
    /// value's data type is a member of the primitive part, and the
    /// refinement (if any) admits the value itself.
    /// Composite refinements are not yet checked; no composite
    /// values flow through evaluation today.
    pub fn admits(&self, value: &Value) -> bool {
        if !self.primitive_part().contains(value.data_type()) {
            return false;
        }
        match self {
            Type::Refined(_, r) => r.admits(value),
            _ => true,
        }
    }

    /// The inverse of [`optional`]: strip the `Nothing` atom if
    /// present, yielding the type that requires a present value.
    /// Idempotent.
    pub fn required(self) -> Type {
        match self {
            Type::Primitive(p) => Type::Primitive(p.required()),
            Type::Composite(p, c) => Type::Composite(p.required(), c),
            Type::Refined(p, r) => Type::Refined(p.required(), r),
        }
    }

    /// Refine this type with a lexical prefix constraint.
    ///
    /// The membership is narrowed to the TEXTUAL kinds (the kinds
    /// with a lexical form; the `Nothing` bit, if present, rides
    /// along untouched — refinements constrain present values).
    /// Returns `None` when no member could carry a lexical form —
    /// an empty meet. An empty prefix is no constraint and returns
    /// the type unchanged; an existing prefix refinement is met
    /// against the new one.
    pub fn with_prefix(self, prefix: impl Into<String>) -> Option<Type> {
        let prefix = prefix.into();
        if prefix.is_empty() {
            return Some(self);
        }
        let membership = self
            .primitive_part()
            .intersect(Primitive::TEXTUAL.union(Primitive::NOTHING))?;
        if membership.required().is_empty() {
            return None;
        }
        let refinement = match &self {
            Type::Refined(_, existing) => existing.meet(&Refinement { prefix })?,
            _ => Refinement { prefix },
        };
        Some(Type::Refined(membership, refinement))
    }

    /// The refinement layered onto this type, if any.
    pub fn refinement(&self) -> Option<&Refinement> {
        match self {
            Type::Refined(_, r) => Some(r),
            _ => None,
        }
    }

    /// Rebuild this type around a replacement primitive part,
    /// preserving its composite or refinement structure. The
    /// unifier uses this so a resolution that narrows membership
    /// does not silently shed the rest of the type.
    pub(crate) fn with_primitive_part(&self, p: Primitive) -> Type {
        match self {
            Type::Primitive(_) => Type::Primitive(p),
            Type::Composite(_, c) => Type::composite(p, c.clone()),
            Type::Refined(_, r) => Type::Refined(p, r.clone()),
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
    /// Returns `None` when the result is empty: no admissible
    /// shapes survive.
    ///
    /// Composite shapes narrow structurally: two
    /// `Composite::Product` values with the same field-name set
    /// intersect their field types recursively (if any field
    /// intersection is empty, the product is eliminated). Products
    /// with disjoint field-name sets do not intersect; they
    /// describe disjoint records. Variants intersect by matching
    /// label, recursing into payload types.
    pub fn intersect(&self, other: &Type) -> Option<Type> {
        let p_self = self.primitive_part();
        let p_other = other.primitive_part();
        let p = Primitive {
            bits: p_self.bits & p_other.bits,
        };

        // Refinements are constraints: the meet carries the
        // conjunction. Two refined sides must have jointly
        // satisfiable refinements; a refined side admits no
        // composite shapes, so any composite part on the other
        // side is dropped.
        let refinement = match (self.refinement(), other.refinement()) {
            (Some(a), Some(b)) => Some(a.meet(b)?),
            (Some(r), None) | (None, Some(r)) => Some(r.clone()),
            (None, None) => None,
        };
        if let Some(refinement) = refinement {
            return if p.is_empty() {
                None
            } else {
                Some(Type::Refined(p, refinement))
            };
        }

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
    ///
    /// Refinements weaken at a join: two refined sides keep their
    /// weakest common implication (the longest common prefix), a
    /// refined side joined with an unrefined one sheds the
    /// refinement entirely — the union must admit everything either
    /// side admits.
    pub fn union(&self, other: &Type) -> Type {
        let p = self.primitive_part().union(other.primitive_part());

        if let (Some(a), Some(b)) = (self.refinement(), other.refinement())
            && let Some(joined) = a.join(b)
        {
            return Type::Refined(p, joined);
        }

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
    /// some shape in `self`. Inclusion is structural: a Product
    /// `{x: T1, y: T2}` includes a Product `{x: T1', y: T2'}` when
    /// the field-name sets match and each `Ti` includes `Ti'`. A
    /// product with extra fields includes one with fewer fields
    /// (width subtyping is not assumed; equal field sets only).
    pub fn includes(&self, other: &Type) -> bool {
        if !self.primitive_part().includes(other.primitive_part()) {
            return false;
        }
        // A refined type admits fewer values than its membership: it
        // includes `other` only if `other` is at least as
        // constrained (other's prefix extends ours). An unrefined
        // type over-approximates any refinement of its membership.
        match (self.refinement(), other.refinement()) {
            (Some(a), Some(b)) => {
                if !b.prefix.starts_with(&a.prefix) {
                    return false;
                }
            }
            (Some(_), None) => return false,
            (None, _) => {}
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
            Type::Refined(p, _) => *p,
        }
    }

    /// The composite part of this type, or `None` if the type has
    /// only a primitive part.
    pub fn composite_part(&self) -> Option<&BTreeSet<Composite>> {
        match self {
            Type::Primitive(_) => None,
            Type::Composite(_, c) => Some(c),
            Type::Refined(_, _) => None,
        }
    }

    /// Legacy storage-codec view: if this type reduces to exactly
    /// one [`ValueType`] (no `Nothing`, no composites), return it.
    /// A refinement does not change the storage type, so a refined
    /// singleton still reports its member.
    pub fn as_value_type(&self) -> Option<ValueType> {
        match self {
            Type::Primitive(p) => p.as_singleton(),
            Type::Composite(_, _) => None,
            Type::Refined(p, _) => p.as_singleton(),
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
/// counterpart on the other side is dropped: set intersection
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
    use std::hash::{Hash, Hash as HashTrait, Hasher};

    fn hash_of<T: HashTrait>(t: &T) -> u64 {
        let mut h = DefaultHasher::new();
        t.hash(&mut h);
        h.finish()
    }

    fn p(vt: ValueType) -> Type {
        Type::from(vt)
    }
    fn o(vt: ValueType) -> Type {
        Type::from(vt).optional()
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
        let a = p(ValueType::String);
        let b = Type::from(Primitive::singleton(ValueType::String));
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
        fields.insert("name".to_string(), Type::from(ValueType::String));
        fields.insert("age".to_string(), Type::from(ValueType::UnsignedInt));
        let a = Type::product(fields.clone());
        let b = Type::product(fields);
        assert_eq!(a, b);
        let mut ha = DefaultHasher::new();
        let mut hb = DefaultHasher::new();
        a.hash(&mut ha);
        b.hash(&mut hb);
        assert_eq!(ha.finish(), hb.finish());
    }

    #[dialog_common::test]
    fn variant_constructor_carries_label_and_payload() {
        let some = Type::variant("Some", Type::from(ValueType::String));
        let composites = some.composite_part().unwrap();
        assert_eq!(composites.len(), 1);
        let first = composites.iter().next().unwrap();
        match first {
            Composite::Variant { label, value } => {
                assert_eq!(label, "Some");
                assert_eq!(*value, Type::from(ValueType::String));
            }
            other => panic!("expected Variant, got {:?}", other),
        }
    }

    #[dialog_common::test]
    fn intersect_two_records_same_fields() {
        let mut fields = BTreeMap::new();
        fields.insert("x".to_string(), Type::from(ValueType::String));
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
        a_fields.insert("x".to_string(), Type::from(Primitive::NUMERIC));
        let a = Type::product(a_fields);

        let mut b_fields = BTreeMap::new();
        b_fields.insert("x".to_string(), Type::from(ValueType::UnsignedInt));
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
        a_fields.insert("x".to_string(), Type::from(ValueType::String));
        let a = Type::product(a_fields);

        let mut b_fields = BTreeMap::new();
        b_fields.insert("x".to_string(), Type::from(ValueType::UnsignedInt));
        let b = Type::product(b_fields);

        assert!(a.intersect(&b).is_none());
    }

    /// Two products with different field-name sets do not
    /// intersect; they describe disjoint record shapes.
    #[dialog_common::test]
    fn intersect_products_different_keys_eliminated() {
        let mut a_fields = BTreeMap::new();
        a_fields.insert("x".to_string(), Type::from(ValueType::String));
        let a = Type::product(a_fields);

        let mut b_fields = BTreeMap::new();
        b_fields.insert("y".to_string(), Type::from(ValueType::String));
        let b = Type::product(b_fields);

        assert!(a.intersect(&b).is_none());
    }

    /// Two variants with the same label intersect their payloads;
    /// different labels do not intersect.
    #[dialog_common::test]
    fn intersect_variants_by_label() {
        let a = Type::variant("Some", Type::from(Primitive::NUMERIC));
        let b = Type::variant("Some", Type::from(ValueType::UnsignedInt));
        let merged = a.intersect(&b).expect("same label narrows payload");
        let composite = merged.composite_part().expect("composite present");
        match composite.iter().next().expect("one variant") {
            Composite::Variant { label, value } => {
                assert_eq!(label, "Some");
                assert_eq!(value.as_value_type(), Some(ValueType::UnsignedInt));
            }
            other => panic!("expected Variant, got {other:?}"),
        }

        let c = Type::variant("Other", Type::from(ValueType::UnsignedInt));
        assert!(a.intersect(&c).is_none());
    }

    #[dialog_common::test]
    fn intersect_primitive_and_composite_keeps_primitive_only() {
        let mut fields = BTreeMap::new();
        fields.insert("x".to_string(), Type::from(ValueType::String));
        let composite = Type::product(fields);
        let prim = Type::from(ValueType::String);
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
        let record_prim = Type::from(ValueType::Record);
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
        a_fields.insert("x".to_string(), Type::from(ValueType::String));
        let a = Type::product(a_fields.clone());

        let mut b_fields = BTreeMap::new();
        b_fields.insert("x".to_string(), Type::from(ValueType::String));
        b_fields.insert("y".to_string(), Type::from(ValueType::UnsignedInt));
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
        assert_eq!(Type::from(Primitive::NUMERIC).as_value_type(), None);
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
    /// not affect product equality or hash: the BTreeMap sorts
    /// by key. Same product built via `{x, y}` vs `{y, x}` insert
    /// orders must compare equal and hash equal.
    #[dialog_common::test]
    fn product_field_insertion_order_independent() {
        let mut a_fields = BTreeMap::new();
        a_fields.insert("x".to_string(), Type::from(ValueType::String));
        a_fields.insert("y".to_string(), Type::from(ValueType::UnsignedInt));
        let a = Type::product(a_fields);

        let mut b_fields = BTreeMap::new();
        b_fields.insert("y".to_string(), Type::from(ValueType::UnsignedInt));
        b_fields.insert("x".to_string(), Type::from(ValueType::String));
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

    /// `with_prefix` narrows membership to the TEXTUAL kinds and
    /// attaches the refinement; non-textual membership is an empty
    /// meet.
    #[dialog_common::test]
    fn with_prefix_narrows_to_textual() {
        let refined = Type::from(Primitive::ALL)
            .with_prefix("did:")
            .expect("textual members remain");
        assert_eq!(refined.primitive_part(), Primitive::TEXTUAL);
        assert_eq!(refined.refinement().expect("refined").prefix, "did:");

        assert!(
            Type::from(Primitive::NUMERIC).with_prefix("did:").is_none(),
            "no numeric value has a lexical form"
        );
        assert_eq!(
            Type::from(ValueType::String).with_prefix(""),
            Some(Type::from(ValueType::String)),
            "an empty prefix is no refinement"
        );
    }

    /// The meet of two refined types takes the stronger prefix;
    /// disjoint prefixes are an empty meet.
    #[dialog_common::test]
    fn refined_intersect_takes_the_stronger_prefix() {
        let did = Type::from(ValueType::Entity).with_prefix("did:").unwrap();
        let did_key = Type::from(ValueType::Entity)
            .with_prefix("did:key:")
            .unwrap();
        let met = did.intersect(&did_key).expect("compatible prefixes");
        assert_eq!(met.refinement().unwrap().prefix, "did:key:");

        let http = Type::from(ValueType::Entity).with_prefix("http:").unwrap();
        assert!(
            did.intersect(&http).is_none(),
            "no value starts with both prefixes"
        );

        let unrefined = Type::from(Primitive::TEXTUAL);
        let met = did.intersect(&unrefined).expect("memberships overlap");
        assert_eq!(
            met.refinement().unwrap().prefix,
            "did:",
            "the refinement survives a meet with an unrefined side"
        );
        assert_eq!(met.primitive_part().as_singleton(), Some(ValueType::Entity));
    }

    /// The join weakens: common prefix when there is one, no
    /// refinement otherwise (including against an unrefined side).
    #[dialog_common::test]
    fn refined_union_weakens() {
        let a = Type::from(ValueType::String)
            .with_prefix("user/admin")
            .unwrap();
        let b = Type::from(ValueType::String)
            .with_prefix("user/guest")
            .unwrap();
        let joined = a.union(&b);
        assert_eq!(joined.refinement().unwrap().prefix, "user/");

        let unrefined = Type::from(ValueType::String);
        assert_eq!(
            a.union(&unrefined),
            Type::from(ValueType::String),
            "a join with an unrefined side sheds the refinement"
        );

        let disjoint = Type::from(ValueType::String).with_prefix("group/").unwrap();
        assert!(
            a.union(&disjoint).refinement().is_none(),
            "no common prefix, no refinement"
        );
    }

    /// Inclusion: the more-refined side is the subtype.
    #[dialog_common::test]
    fn refined_includes_is_constraint_ordered() {
        let did = Type::from(ValueType::Entity).with_prefix("did:").unwrap();
        let did_key = Type::from(ValueType::Entity)
            .with_prefix("did:key:")
            .unwrap();
        let unrefined = Type::from(ValueType::Entity);

        assert!(did.includes(&did_key), "longer prefix is more constrained");
        assert!(!did_key.includes(&did));
        assert!(unrefined.includes(&did), "unrefined over-approximates");
        assert!(!did.includes(&unrefined));
    }

    /// `admits` enforces the refinement against the value's lexical
    /// form — membership alone is not enough.
    #[dialog_common::test]
    fn refined_admits_checks_lexical_form() {
        let refined = Type::from(Primitive::TEXTUAL).with_prefix("user/").unwrap();
        assert!(refined.admits(&Value::String("user/name".into())));
        assert!(!refined.admits(&Value::String("group/name".into())));
        assert!(!refined.admits(&Value::UnsignedInt(7)));
    }

    /// Refined types survive serde.
    #[dialog_common::test]
    fn refined_serde_round_trip() {
        let t = Type::from(ValueType::Entity)
            .with_prefix("did:key:")
            .unwrap();
        let j = serde_json::to_string(&t).unwrap();
        let back: Type = serde_json::from_str(&j).unwrap();
        assert_eq!(t, back);
    }

    /// Combining two products via union also produces identical
    /// hashes regardless of which product was unioned first.
    #[dialog_common::test]
    fn product_union_is_order_independent() {
        let mut a_fields = BTreeMap::new();
        a_fields.insert("x".to_string(), Type::from(ValueType::String));
        let a = Type::product(a_fields);

        let mut b_fields = BTreeMap::new();
        b_fields.insert("y".to_string(), Type::from(ValueType::UnsignedInt));
        let b = Type::product(b_fields);

        let ab = a.union(&b);
        let ba = b.union(&a);

        assert_eq!(ab, ba);
        assert_eq!(hash_of(&ab), hash_of(&ba));
    }
}

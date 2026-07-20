//! Unified type system for the query engine.
//!
//! This module defines the static, derive-friendly user-facing
//! [`Type`] used by [`Term`](crate::Term),
//! [`Equality`](crate::constraint::Equality),
//! [`Premise`](crate::Premise), [`DeductiveRule`](crate::DeductiveRule),
//! and the schema layer.
//!
//! A [`Type`] is the set of admissible value shapes a slot may
//! bind: a [`Primitive`] bitfield (the atomic shapes, plus a
//! `Nothing` bit for set-widened optionality), optionally narrowed
//! by a value-level [`Refinement`]. A value satisfies [`Type`] iff
//! its data type is a member of the primitive set and the
//! refinement (if any) admits the value itself.
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
use crate::artifact::{decode_value, encode_value_owned};
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::collections::BTreeSet;
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

impl Display for Type {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        match self {
            Type::Primitive(p) => write!(f, "{p}"),
            Type::Refined(p, r) => write!(f, "{p}[{r}]"),
        }
    }
}

/// Schema-layer type: the set of admissible value shapes a slot
/// can bind.
///
/// A [`Type`] is a [`Primitive`] bitfield carrying atomic
/// [`ValueType`] bits plus an optional `Nothing` atom (row-level
/// absence), optionally narrowed by a value-level [`Refinement`].
///
/// `Type` is fully static: no type variables, no allocation
/// state. Deriving [`PartialEq`]/[`Eq`]/[`Hash`] is safe and
/// produces stable values for equivalent types.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Type {
    /// Pure primitive membership set.
    Primitive(Primitive),
    /// Primitive set narrowed by a [`Refinement`] on the *values*
    /// of the member types — not just which types are admitted, but
    /// which of their inhabitants. Produced by refinement
    /// predicates (`starts-with`); consumed by scan-range pushdown
    /// and, like every kind, by [`Type::admits`] at the data
    /// boundary.
    Refined(Primitive, Refinement),
}

/// Opaque identity of a concept an entity must conform to — the
/// concept's content-derived entity URI (`concept:{hash}`).
///
/// Deliberately opaque on the lattice: subsumption *between*
/// concepts (attribute-set inclusion) needs their descriptors,
/// which the lattice does not carry. The analyzer canonicalizes
/// conformance sets against the registry before refinements enter
/// unification; the lattice orders them by plain set inclusion.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct ConceptRef(pub String);

impl Display for ConceptRef {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        write!(f, "{}", self.0)
    }
}

/// A value-level constraint layered onto a primitive membership
/// set. The meet of two refinements is their conjunction (both
/// constraints), the join their weakest common implication — see
/// [`Refinement::meet`] and [`Refinement::join`].
///
/// Three constraints exist: a lexical prefix over the TEXTUAL kinds,
/// a conformance set over Entity, and an interval over the
/// COMPARABLE kinds proved by the comparison predicates.
///
/// Invariant: never empty (no prefix, no conformance, and no
/// interval is no refinement; the constructors collapse it to an
/// unrefined type).
#[derive(Clone, Debug, Default, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct Refinement {
    /// Lexical prefix every admitted value must begin with.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prefix: Option<String>,
    /// Concepts the value's entity must conform to — all of them
    /// (the meet unions this set). Carried as *information*: an
    /// entity's conformance is not a property of the scalar (it is
    /// "facts exist"), so enforcement is structural — the analyzer
    /// conjoins the target concept's premises on the field — while
    /// this set feeds inclusion ordering, demand covers, and
    /// diagnostics.
    #[serde(default, skip_serializing_if = "BTreeSet::is_empty")]
    pub conforms: BTreeSet<ConceptRef>,
    /// An interval a comparison predicate proved about the
    /// value. Carried as *information*, like `conforms`: enforcement
    /// stays with the comparison premise itself (whose literal-
    /// adaptation semantics [`Refinement::admits`] deliberately does
    /// not reproduce), while this feeds the scan-range pushdown.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub interval: Option<Box<Interval>>,
}

/// An interval over a single COMPARABLE value type, proved by
/// comparison predicates on a variable: at most one lower and one
/// upper bound.
///
/// Bounds are stored in the value's ORDER-PRESERVING encoding (see
/// `dialog_artifacts::encode_value_owned`), so within one
/// `value_type` the lattice operations are plain byte comparisons and
/// the struct stays `Eq`/`Ord`/`Hash` for the type lattice (a raw
/// `Value` would not, floats being only partially ordered).
///
/// The interval records the LITERAL's own type. A NUMERIC comparison
/// adapts its literal to each row's type, so an interval typed
/// `UnsignedInt` still admits floats; consumers that cannot honor
/// that (the scan pushdown) must gate on the variable's kind being
/// exactly one comparable type and adapt the bound, as the comparison
/// would.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct Interval {
    /// The numeric type the bound literals were written in.
    pub value_type: ValueType,
    /// The lower bound, if one was proved.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lower: Option<IntervalBound>,
    /// The upper bound, if one was proved.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub upper: Option<IntervalBound>,
}

/// One side of an [`Interval`]: the bound value's order-preserving
/// encoding and whether the bound itself is admitted.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct IntervalBound {
    /// The bound value's order-preserving encoding.
    pub encoded: Vec<u8>,
    /// Whether the bound is inclusive (`>=`/`<=`) rather than strict.
    pub inclusive: bool,
}

impl Interval {
    /// The interval a single comparison proves: one bound of one side.
    fn bound(value_type: ValueType, encoded: Vec<u8>, inclusive: bool, lower: bool) -> Interval {
        let bound = Some(IntervalBound { encoded, inclusive });
        Interval {
            value_type,
            lower: if lower { bound.clone() } else { None },
            upper: if lower { None } else { bound },
        }
    }

    /// Meet: the conjunction. Same-typed intervals intersect
    /// (byte order equals numeric order within one type: the higher
    /// lower bound and the lower upper bound win; equal encodings
    /// keep the STRICTER inclusivity). Differently-typed intervals
    /// cannot be compared without per-row adaptation, so the meet
    /// conservatively keeps neither — a weaker refinement is always
    /// sound, because enforcement lives with the comparison premises
    /// themselves.
    fn meet(&self, other: &Interval) -> Option<Interval> {
        if self.value_type != other.value_type {
            return None;
        }
        let lower = match (&self.lower, &other.lower) {
            (Some(a), Some(b)) => Some(pick(a, b, Ordering::Greater, true)),
            (Some(bound), None) | (None, Some(bound)) => Some(bound.clone()),
            (None, None) => None,
        };
        let upper = match (&self.upper, &other.upper) {
            (Some(a), Some(b)) => Some(pick(a, b, Ordering::Less, true)),
            (Some(bound), None) | (None, Some(bound)) => Some(bound.clone()),
            (None, None) => None,
        };
        Some(Interval {
            value_type: self.value_type,
            lower,
            upper,
        })
    }

    /// Join: the weakest interval both sides imply — same-typed
    /// intervals take the convex hull; differently-typed intervals
    /// share no interval.
    fn join(&self, other: &Interval) -> Option<Interval> {
        if self.value_type != other.value_type {
            return None;
        }
        let lower = match (&self.lower, &other.lower) {
            (Some(a), Some(b)) => Some(pick(a, b, Ordering::Less, false)),
            _ => None,
        };
        let upper = match (&self.upper, &other.upper) {
            (Some(a), Some(b)) => Some(pick(a, b, Ordering::Greater, false)),
            _ => None,
        };
        if lower.is_none() && upper.is_none() {
            return None;
        }
        Some(Interval {
            value_type: self.value_type,
            lower,
            upper,
        })
    }

    /// True when `other` implies `self`: same type, and `other`'s
    /// bounds sit within `self`'s. Conservative (differently-typed
    /// intervals are treated as unrelated), which only weakens
    /// subsumption, never soundness.
    fn implied_by(&self, other: &Interval) -> bool {
        if self.value_type != other.value_type {
            return false;
        }
        let lower_implied = match (&self.lower, &other.lower) {
            (Some(a), Some(b)) => match a.encoded.cmp(&b.encoded) {
                Ordering::Less => true,
                Ordering::Equal => a.inclusive || !b.inclusive,
                Ordering::Greater => false,
            },
            (Some(_), None) => false,
            (None, _) => true,
        };
        let upper_implied = match (&self.upper, &other.upper) {
            (Some(a), Some(b)) => match a.encoded.cmp(&b.encoded) {
                Ordering::Greater => true,
                Ordering::Equal => a.inclusive || !b.inclusive,
                Ordering::Less => false,
            },
            (Some(_), None) => false,
            (None, _) => true,
        };
        lower_implied && upper_implied
    }
}

/// Of two same-side bounds, the one whose encoding wins `prefer` (for
/// a meet: the greater lower bound / the lesser upper bound; reversed
/// for a join). Equal encodings resolve by inclusivity: a meet
/// (`strict`) admits the bound only if BOTH sides do; a join admits
/// it if EITHER does.
fn pick(a: &IntervalBound, b: &IntervalBound, prefer: Ordering, strict: bool) -> IntervalBound {
    match a.encoded.cmp(&b.encoded) {
        Ordering::Equal => IntervalBound {
            encoded: a.encoded.clone(),
            inclusive: if strict {
                a.inclusive && b.inclusive
            } else {
                a.inclusive || b.inclusive
            },
        },
        ordering if ordering == prefer => a.clone(),
        _ => b.clone(),
    }
}

impl Refinement {
    /// A prefix-only refinement.
    fn prefix(prefix: String) -> Refinement {
        Refinement {
            prefix: Some(prefix),
            conforms: BTreeSet::new(),
            interval: None,
        }
    }

    /// An interval-only refinement.
    fn interval(interval: Interval) -> Refinement {
        Refinement {
            prefix: None,
            conforms: BTreeSet::new(),
            // Boxed: the interval's bounds would otherwise dominate the
            // size of every `Type` (and every error carrying one).
            interval: Some(Box::new(interval)),
        }
    }

    /// True when the refinement constrains nothing — the shape the
    /// constructors collapse to an unrefined type.
    fn is_empty(&self) -> bool {
        self.prefix.is_none() && self.conforms.is_empty() && self.interval.is_none()
    }

    /// Meet: the conjunction of both constraints. Two prefixes are
    /// jointly satisfiable iff one extends the other (the meet is
    /// the longer; disjoint prefixes admit nothing); conformance
    /// sets union — the value must satisfy both sides' concepts;
    /// same-typed intervals intersect, and differently-typed ones
    /// conservatively drop (the interval is advisory — see the field
    /// doc — so a weaker meet is sound).
    fn meet(&self, other: &Refinement) -> Option<Refinement> {
        let prefix = match (&self.prefix, &other.prefix) {
            (Some(a), Some(b)) => {
                if a.starts_with(b.as_str()) {
                    Some(a.clone())
                } else if b.starts_with(a.as_str()) {
                    Some(b.clone())
                } else {
                    return None;
                }
            }
            (Some(p), None) | (None, Some(p)) => Some(p.clone()),
            (None, None) => None,
        };
        let conforms = self.conforms.union(&other.conforms).cloned().collect();
        let interval = match (&self.interval, &other.interval) {
            (Some(a), Some(b)) => a.meet(b).map(Box::new),
            (Some(interval), None) | (None, Some(interval)) => Some(interval.clone()),
            (None, None) => None,
        };
        Some(Refinement {
            prefix,
            conforms,
            interval,
        })
    }

    /// Join: the weakest constraint both sides imply — the longest
    /// common prefix (a side without one implies none) and the
    /// intersection of the conformance sets. `None` when nothing
    /// remains (the join carries no refinement).
    fn join(&self, other: &Refinement) -> Option<Refinement> {
        let prefix = match (&self.prefix, &other.prefix) {
            (Some(a), Some(b)) => {
                let common: String = a
                    .chars()
                    .zip(b.chars())
                    .take_while(|(x, y)| x == y)
                    .map(|(x, _)| x)
                    .collect();
                if common.is_empty() {
                    None
                } else {
                    Some(common)
                }
            }
            _ => None,
        };
        let conforms: BTreeSet<ConceptRef> = self
            .conforms
            .intersection(&other.conforms)
            .cloned()
            .collect();
        let interval = match (&self.interval, &other.interval) {
            (Some(a), Some(b)) => a.join(b).map(Box::new),
            _ => None,
        };
        let joined = Refinement {
            prefix,
            conforms,
            interval,
        };
        if joined.is_empty() {
            None
        } else {
            Some(joined)
        }
    }

    /// True when `other` is at least as constrained as `self` —
    /// every value satisfying `other` satisfies `self`. The
    /// inclusion check behind [`Type::includes`].
    fn implied_by(&self, other: &Refinement) -> bool {
        let prefix_implied = match (&self.prefix, &other.prefix) {
            (Some(a), Some(b)) => b.starts_with(a.as_str()),
            (Some(_), None) => false,
            (None, _) => true,
        };
        let interval_implied = match (&self.interval, &other.interval) {
            (Some(a), Some(b)) => a.implied_by(b),
            (Some(_), None) => false,
            (None, _) => true,
        };
        prefix_implied && interval_implied && self.conforms.is_subset(&other.conforms)
    }

    /// True when the value satisfies the row-locally checkable half
    /// of the refinement: the lexical prefix. Values without a
    /// lexical form satisfy no prefix. Conformance is deliberately
    /// not checked here — see the field doc; its enforcement is the
    /// desugared premises' job.
    pub fn admits(&self, value: &Value) -> bool {
        match &self.prefix {
            Some(prefix) => {
                lexical_form(value).is_some_and(|form| form.starts_with(prefix.as_str()))
            }
            None => true,
        }
    }
}

impl Display for Refinement {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        let mut separate = false;
        if let Some(prefix) = &self.prefix {
            write!(f, "starts-with {prefix:?}")?;
            separate = true;
        }
        for concept in &self.conforms {
            if separate {
                write!(f, " & ")?;
            }
            write!(f, "conforms-to {concept}")?;
            separate = true;
        }
        if let Some(interval) = &self.interval {
            let mut side = |bound: &Option<IntervalBound>,
                            strict: &str,
                            inclusive: &str,
                            separate: &mut bool|
             -> FmtResult {
                if let Some(bound) = bound {
                    if *separate {
                        write!(f, " & ")?;
                    }
                    let relation = if bound.inclusive { inclusive } else { strict };
                    match decode_value(interval.value_type, &bound.encoded) {
                        Some((value, [])) => write!(f, "{relation} {value:?}")?,
                        _ => write!(f, "{relation} <{} bytes>", bound.encoded.len())?,
                    }
                    *separate = true;
                }
                Ok(())
            };
            side(&interval.lower, ">", ">=", &mut separate)?;
            side(&interval.upper, "<", "<=", &mut separate)?;
        }
        Ok(())
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
            Type::Refined(p, r) => Type::Refined(p.union(Primitive::NOTHING), r),
        }
    }

    /// True when the given runtime value inhabits this type: the
    /// value's data type is a member of the primitive part, and the
    /// refinement (if any) admits the value itself.
    pub fn admits(&self, value: &Value) -> bool {
        if !self.primitive_part().contains(value.data_type()) {
            return false;
        }
        match self {
            Type::Refined(_, r) => r.admits(value),
            Type::Primitive(_) => true,
        }
    }

    /// The inverse of [`optional`]: strip the `Nothing` atom if
    /// present, yielding the type that requires a present value.
    /// Idempotent.
    pub fn required(self) -> Type {
        match self {
            Type::Primitive(p) => Type::Primitive(p.required()),
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
            Type::Refined(_, existing) => existing.meet(&Refinement::prefix(prefix))?,
            _ => Refinement::prefix(prefix),
        };
        Some(Type::Refined(membership, refinement))
    }

    /// Refine this type with one side of an interval, as a
    /// comparison predicate proves it: `lower` selects which side,
    /// `inclusive` whether the bound itself is admitted.
    ///
    /// The membership narrows to the COMPARABLE kinds (plus a riding
    /// `Nothing` bit); the interval records the LITERAL's own type,
    /// because a NUMERIC comparison adapts its literal to each row's
    /// type — an interval typed `UnsignedInt` still admits floats.
    /// (Non-numeric literals never adapt, but the interval stays
    /// advisory either way — see the field doc.) Returns `None` when
    /// no member could be comparable — an empty meet. A
    /// non-comparable bound value is no constraint and returns the
    /// type unchanged (the comparison itself will filter every row).
    pub fn with_interval(self, bound: &Value, inclusive: bool, lower: bool) -> Option<Type> {
        let value_type = bound.data_type();
        if !Primitive::COMPARABLE.contains(value_type) {
            return Some(self);
        }
        // A numeric literal adapts to each row's numeric type, so any
        // numeric member may still match; a non-numeric literal never
        // adapts, so only rows of the literal's own type can order
        // against it and the membership narrows to exactly that type.
        let admissible = if Primitive::NUMERIC.contains(value_type) {
            Primitive::NUMERIC
        } else {
            Primitive::singleton(value_type)
        };
        let membership = self
            .primitive_part()
            .intersect(admissible.union(Primitive::NOTHING))?;
        if membership.required().is_empty() {
            return None;
        }
        let interval = Interval::bound(value_type, encode_value_owned(bound), inclusive, lower);
        let refinement = match &self {
            Type::Refined(_, existing) => existing.meet(&Refinement::interval(interval))?,
            _ => Refinement::interval(interval),
        };
        Some(Type::Refined(membership, refinement))
    }

    /// Constrain this type's values to entities conforming to the
    /// given concept — the lattice form of a concept-typed field.
    ///
    /// The membership narrows to Entity (the `Nothing` bit, if
    /// present, rides along — an optional concept-typed field is
    /// still optional). Returns `None` when no entity could inhabit
    /// the type — an empty meet, the ordinary
    /// known-types-misalign conflict. Conformance accumulates: an
    /// existing refinement gains the concept.
    pub fn with_conformance(self, concept: ConceptRef) -> Option<Type> {
        let membership = self
            .primitive_part()
            .intersect(Primitive::from(ValueType::Entity).union(Primitive::NOTHING))?;
        if membership.required().is_empty() {
            return None;
        }
        let mut refinement = match self {
            Type::Refined(_, r) => r,
            Type::Primitive(_) => Refinement::default(),
        };
        refinement.conforms.insert(concept);
        Some(Type::Refined(membership, refinement))
    }

    /// The refinement layered onto this type, if any.
    pub fn refinement(&self) -> Option<&Refinement> {
        match self {
            Type::Refined(_, r) => Some(r),
            Type::Primitive(_) => None,
        }
    }

    /// Rebuild this type around a replacement primitive part,
    /// preserving its refinement structure. The unifier uses this
    /// so a resolution that narrows membership does not silently
    /// shed the rest of the type.
    pub(crate) fn with_primitive_part(&self, p: Primitive) -> Type {
        match self {
            Type::Primitive(_) => Type::Primitive(p),
            Type::Refined(_, r) => Type::Refined(p, r.clone()),
        }
    }

    /// Set intersection: the meet of both memberships and both
    /// refinements. Returns `None` when the result is empty: no
    /// admissible shapes survive.
    ///
    /// Refinements are constraints: the meet carries the
    /// conjunction. Two refined sides must have jointly
    /// satisfiable refinements.
    pub fn intersect(&self, other: &Type) -> Option<Type> {
        let p = self.primitive_part().intersect(other.primitive_part())?;
        let refinement = match (self.refinement(), other.refinement()) {
            (Some(a), Some(b)) => Some(a.meet(b)?),
            (Some(r), None) | (None, Some(r)) => Some(r.clone()),
            (None, None) => None,
        };
        Some(match refinement {
            // A meet can leave nothing behind (mixed-type intervals drop
            // conservatively); uphold the never-empty refinement invariant.
            Some(refinement) if !refinement.is_empty() => Type::Refined(p, refinement),
            _ => Type::Primitive(p),
        })
    }

    /// Set union of both memberships.
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
        Type::Primitive(p)
    }

    /// Subtype check. Returns `true` iff every shape `other`
    /// admits is also admitted by `self`.
    pub fn includes(&self, other: &Type) -> bool {
        if !self.primitive_part().includes(other.primitive_part()) {
            return false;
        }
        // A refined type admits fewer values than its membership: it
        // includes `other` only if `other` is at least as
        // constrained (other's constraints imply ours). An unrefined
        // type over-approximates any refinement of its membership.
        match (self.refinement(), other.refinement()) {
            (Some(a), Some(b)) => a.implied_by(b),
            (Some(_), None) => false,
            (None, _) => true,
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
            Type::Refined(p, _) => *p,
        }
    }

    /// Legacy storage-codec view: if this type reduces to exactly
    /// one [`ValueType`] (no `Nothing`), return it. A refinement
    /// does not change the storage type, so a refined singleton
    /// still reports its member.
    pub fn as_value_type(&self) -> Option<ValueType> {
        match self {
            Type::Primitive(p) => p.as_singleton(),
            Type::Refined(p, _) => p.as_singleton(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};

    fn p(vt: ValueType) -> Type {
        Type::from(vt)
    }
    fn o(vt: ValueType) -> Type {
        Type::from(vt).optional()
    }

    /// Interval refinements meet by intersection and join by convex hull
    /// within one literal type; the membership narrows to NUMERIC and a
    /// non-numeric member set is an empty meet.
    #[dialog_common::test]
    fn it_meets_and_joins_intervals() {
        let base = Type::from(Primitive::NUMERIC);
        let ge2 = base
            .clone()
            .with_interval(&Value::UnsignedInt(2), true, true)
            .expect("numeric admits an interval");
        let ge5 = base
            .clone()
            .with_interval(&Value::UnsignedInt(5), true, true)
            .unwrap();
        let le9 = base
            .clone()
            .with_interval(&Value::UnsignedInt(9), true, false)
            .unwrap();

        // meet(x >= 2, x >= 5) keeps the tighter lower bound.
        let met = ge2.clone().intersect(&ge5).expect("compatible");
        assert_eq!(
            met.refinement().unwrap().interval,
            ge5.refinement().unwrap().interval,
        );

        // meet of a lower with an upper carries both sides.
        let both = ge5.clone().intersect(&le9).expect("compatible");
        let interval = both.refinement().unwrap().interval.clone().unwrap();
        assert!(interval.lower.is_some() && interval.upper.is_some());

        // join(x >= 2, x >= 5) keeps the weaker bound.
        let joined = ge2.clone().union(&ge5);
        assert_eq!(
            joined.refinement().unwrap().interval,
            ge2.refinement().unwrap().interval,
        );

        // Inclusion: `x >= 5` implies `x >= 2`, not vice versa.
        assert!(ge2.includes(&ge5));
        assert!(!ge5.includes(&ge2));

        // A numeric bound on a known non-numeric membership is an
        // empty meet.
        assert!(
            Type::from(ValueType::String)
                .with_interval(&Value::UnsignedInt(1), true, true)
                .is_none()
        );
        // A non-numeric bound never adapts, so it narrows membership
        // to the literal's own type...
        let narrowed = Type::from(Primitive::ALL)
            .with_interval(&Value::String("x".into()), true, true)
            .expect("string rows remain");
        assert_eq!(
            narrowed.primitive_part(),
            Primitive::from(ValueType::String)
        );
        assert!(narrowed.refinement().expect("refined").interval.is_some());
        // ...and meets empty against a membership that excludes it.
        assert!(
            base.clone()
                .with_interval(&Value::String("x".into()), true, true)
                .is_none()
        );
        // A non-comparable bound value is no constraint at all.
        let unchanged = base
            .with_interval(&Value::Boolean(true), true, true)
            .unwrap();
        assert!(unchanged.refinement().is_none());
    }

    /// Differently-typed intervals cannot be compared without per-row
    /// literal adaptation, so the lattice treats them conservatively:
    /// the meet keeps neither (weaker is sound — enforcement lives with
    /// the comparison premises) and inclusion says "unrelated".
    #[dialog_common::test]
    fn it_drops_mixed_type_intervals_conservatively() {
        let base = Type::from(Primitive::NUMERIC);
        let ge_int = base
            .clone()
            .with_interval(&Value::UnsignedInt(1), true, true)
            .unwrap();
        let le_float = base
            .clone()
            .with_interval(&Value::Float(10.5), true, false)
            .unwrap();

        let met = ge_int.clone().intersect(&le_float).expect("compatible");
        assert!(
            met.refinement().is_none(),
            "mixed-type intervals drop rather than conflict: {met}"
        );
        assert!(!ge_int.includes(&le_float));
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

    /// Intersection and union reduce to primitive-part set algebra
    /// when no refinements are involved.
    #[dialog_common::test]
    fn intersect_and_union_are_primitive_set_algebra() {
        let numeric = Type::from(Primitive::NUMERIC);
        let uint = Type::from(ValueType::UnsignedInt);
        let met = numeric.intersect(&uint).expect("overlap");
        assert_eq!(met.as_value_type(), Some(ValueType::UnsignedInt));

        let string = Type::from(ValueType::String);
        assert!(uint.intersect(&string).is_none(), "disjoint memberships");

        let joined = uint.union(&string);
        assert!(joined.primitive_part().contains(ValueType::UnsignedInt));
        assert!(joined.primitive_part().contains(ValueType::String));
        assert!(joined.includes(&uint));
        assert!(joined.includes(&string));
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

    /// `with_prefix` narrows membership to the TEXTUAL kinds and
    /// attaches the refinement; non-textual membership is an empty
    /// meet.
    #[dialog_common::test]
    fn with_prefix_narrows_to_textual() {
        let refined = Type::from(Primitive::ALL)
            .with_prefix("did:")
            .expect("textual members remain");
        assert_eq!(refined.primitive_part(), Primitive::TEXTUAL);
        assert_eq!(
            refined.refinement().expect("refined").prefix.as_deref(),
            Some("did:")
        );

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
        assert_eq!(
            met.refinement().unwrap().prefix.as_deref(),
            Some("did:key:")
        );

        let http = Type::from(ValueType::Entity).with_prefix("http:").unwrap();
        assert!(
            did.intersect(&http).is_none(),
            "no value starts with both prefixes"
        );

        let unrefined = Type::from(Primitive::TEXTUAL);
        let met = did.intersect(&unrefined).expect("memberships overlap");
        assert_eq!(
            met.refinement().unwrap().prefix.as_deref(),
            Some("did:"),
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
        assert_eq!(
            joined.refinement().unwrap().prefix.as_deref(),
            Some("user/")
        );

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

    fn concept(uri: &str) -> ConceptRef {
        ConceptRef(uri.to_string())
    }

    /// `with_conformance` narrows membership to Entity (keeping the
    /// `Nothing` bit for optional fields) and rejects memberships
    /// with no entity in them.
    #[dialog_common::test]
    fn with_conformance_narrows_to_entity() {
        let person = concept("concept:person");
        let refined = Type::from(Primitive::ALL)
            .with_conformance(person.clone())
            .expect("Entity is a member");
        assert_eq!(
            refined.primitive_part().as_singleton(),
            Some(ValueType::Entity)
        );
        assert!(refined.refinement().unwrap().conforms.contains(&person));

        let optional = Type::from(ValueType::Entity)
            .optional()
            .with_conformance(person.clone())
            .expect("optional entity remains inhabited");
        assert!(
            optional.primitive_part().contains_nothing(),
            "the Nothing bit rides along"
        );

        assert!(
            Type::from(Primitive::NUMERIC)
                .with_conformance(person)
                .is_none(),
            "no numeric value is an entity"
        );
    }

    /// The meet unions conformance sets (the value must satisfy both
    /// sides); the join intersects them (only what both sides imply
    /// survives).
    #[dialog_common::test]
    fn conformance_meet_unions_join_intersects() {
        let person = concept("concept:person");
        let employee = concept("concept:employee");
        let a = Type::from(ValueType::Entity)
            .with_conformance(person.clone())
            .unwrap();
        let b = Type::from(ValueType::Entity)
            .with_conformance(employee.clone())
            .unwrap();

        let met = a.intersect(&b).expect("memberships overlap");
        let conforms = &met.refinement().unwrap().conforms;
        assert!(conforms.contains(&person) && conforms.contains(&employee));

        assert!(
            a.union(&b).refinement().is_none(),
            "disjoint conformance sets imply nothing in common"
        );
        let both = a.clone().with_conformance(employee).unwrap();
        assert_eq!(
            both.union(&a),
            a,
            "the join keeps exactly the shared concepts"
        );
    }

    /// Inclusion: a larger conformance set is more constrained, so
    /// the superset side is the subtype.
    #[dialog_common::test]
    fn conformance_includes_is_subset_ordered() {
        let person = concept("concept:person");
        let a = Type::from(ValueType::Entity)
            .with_conformance(person.clone())
            .unwrap();
        let both = a
            .clone()
            .with_conformance(concept("concept:employee"))
            .unwrap();
        let unrefined = Type::from(ValueType::Entity);

        assert!(a.includes(&both), "more concepts is more constrained");
        assert!(!both.includes(&a));
        assert!(unrefined.includes(&a), "unrefined over-approximates");
        assert!(!a.includes(&unrefined));

        let prefixed = a.clone().with_prefix("did:").unwrap();
        assert!(
            a.includes(&prefixed) && !prefixed.includes(&a),
            "both halves participate in the ordering"
        );
    }

    /// Conformance is not row-checkable ("facts exist" is not a
    /// property of the scalar): `admits` enforces only the prefix
    /// half, enforcement of conformance is the desugared premises'
    /// job.
    #[dialog_common::test]
    fn conformance_is_not_row_checked() {
        let refined = Type::from(ValueType::Entity)
            .with_conformance(concept("concept:person"))
            .unwrap();
        let entity = Value::Entity("did:key:z6Mk".parse().expect("valid entity"));
        assert!(
            refined.admits(&entity),
            "any entity passes the row-local check"
        );
        assert!(
            !refined.admits(&Value::UnsignedInt(7)),
            "membership still applies"
        );
    }

    /// Conformance survives serde, and the extended [`Refinement`]
    /// still reads the prefix-only wire shape older peers produce.
    #[dialog_common::test]
    fn conformance_serde_round_trip_and_wire_compat() {
        let t = Type::from(ValueType::Entity)
            .with_conformance(concept("concept:person"))
            .unwrap()
            .with_prefix("did:")
            .unwrap();
        let j = serde_json::to_string(&t).unwrap();
        let back: Type = serde_json::from_str(&j).unwrap();
        assert_eq!(t, back);

        let old = Type::from(ValueType::Entity).with_prefix("did:").unwrap();
        let old_wire = serde_json::to_string(&old).unwrap();
        assert!(
            !old_wire.contains("conforms"),
            "an empty conformance set stays off the wire"
        );
        let read: Type = serde_json::from_str(&old_wire).unwrap();
        assert_eq!(read, old);
    }
}

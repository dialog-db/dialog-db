//! Unified type system for the query engine.
//!
//! This module is the foundation of v2's type system. It defines:
//!
//! - [`Type`] / [`Definite`] — the runtime type representation,
//!   covering primitive value types, type variables, and
//!   set-widened (Optional) types. Designed to grow `Record` and
//!   `Variant` constructors in future PRs without reshape.
//! - [`PrimitiveSet`] — a bitfield over [`ValueType`] variants.
//!   Enables type variables to carry kind-level constraints
//!   (`NUMERIC`, `STRING_LIKE`, etc.) and lets unification narrow
//!   via set intersection.
//! - [`VarId`] / [`TypeScheme`] — rank-1 polymorphic type
//!   schemes, used by formula declarations to express generic
//!   signatures like `Sum<T: Numeric>(T, T) -> T`.
//! - [`UnificationContext`] — a Damas-Milner unifier with
//!   substitution, constraint registry, fresh-id allocator, and
//!   the [`unify`](UnificationContext::unify) algorithm itself.
//! - [`MatchOptionality`] — slot-level optionality for unification.
//!
//! See `notes/optional-fields.md` for the design rationale.

use crate::artifact::Type as ValueType;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, Ordering};

/// A unique identifier for a type variable within a unification
/// context. Allocated by [`UnificationContext::fresh`] or by
/// [`UnificationContext::instantiate`] when a [`TypeScheme`]'s
/// quantified variable is bound to a fresh runtime variable.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct VarId(u32);

/// Process-global counter for [`VarId`] allocation. Used as a
/// fallback when no [`UnificationContext`] is available — most
/// allocation goes through a context's per-context counter for
/// determinism.
static GLOBAL_VAR_COUNTER: AtomicU32 = AtomicU32::new(0);

impl VarId {
    /// Allocate a new globally-unique `VarId`. Used at descriptor
    /// boundaries where no context is available; rule-compile-time
    /// unification uses [`UnificationContext::fresh`] instead.
    pub fn global_fresh() -> Self {
        Self(GLOBAL_VAR_COUNTER.fetch_add(1, Ordering::Relaxed))
    }
}

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

    /// Set containing every [`ValueType`] variant. Equivalent to
    /// the v1 `Type::Any` for primitive shapes.
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

    /// Comparable primitives: numeric ∪ string-like ∪ entity ∪ bytes.
    /// Used as the constraint for ordering predicates (`<`, `<=`, etc.).
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
    /// builds so serialization round-trips. Adding a new variant
    /// requires extending this match and the `ALL` mask above.
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

    /// Set intersection. Returns `None` iff the result is empty —
    /// signals "no shape can satisfy both constraints," which
    /// callers (notably [`UnificationContext::unify`]) treat as a
    /// type error.
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

    /// Returns `true` iff `self` is a (non-strict) superset of
    /// `other`. Used by the `includes`-style subtype check.
    pub fn includes(self, other: Self) -> bool {
        (self.bits & other.bits) == other.bits
    }

    /// Returns `true` iff `vt` is a member of this set.
    pub fn contains(self, vt: ValueType) -> bool {
        (self.bits & Self::bit_for(vt)) != 0
    }

    /// If this set has exactly one member, return it. Useful for
    /// the common "I narrowed the constraint down to a single
    /// type" case.
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

impl From<Option<ValueType>> for Type {
    /// Lifts the legacy `Option<ValueType>` storage-tag
    /// representation into the unified type system.
    ///
    /// - `None` → an anonymous unconstrained variable
    ///   ([`Self::any`]). Each call allocates a fresh
    ///   [`VarId`], so multiple `None.into()` invocations
    ///   produce distinct variables. The unifier links them at
    ///   rule-compile time when the same rule-level variable
    ///   name is shared across slots.
    /// - `Some(vt)` → `Type::Definite(Primitive(singleton(vt)))`.
    fn from(value: Option<ValueType>) -> Self {
        match value {
            Some(vt) => Self::primitive(vt),
            None => Self::any(),
        }
    }
}

/// Schema-layer type of a value, term, or schema slot.
///
/// Two outer variants distinguish set-widening (Optional) from
/// concrete shapes (Definite). Optionality lives at the slot
/// layer, never on type variables — this keeps "nested
/// optionality" structurally unrepresentable.
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

/// A concrete value shape — non-`Optional`. Includes type
/// variables (which still represent "a single shape," just
/// unknown until unified).
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Definite {
    /// Atomic value type, possibly a union over several primitive
    /// shapes. `PrimitiveSet::singleton(ValueType::String)` is
    /// "exactly String"; `PrimitiveSet::NUMERIC` is "any of
    /// `UnsignedInt`, `SignedInt`, `Float`."
    Primitive(PrimitiveSet),
    /// A type variable. The variable's constraint
    /// (`PrimitiveSet`) lives in the
    /// [`UnificationContext`]'s constraint registry, keyed by
    /// `VarId`. The variable resolves to a concrete `Definite`
    /// once unified.
    Variable(VarId),
    // Future:
    // Record(BTreeMap<String, Type>),
    // Variant(BTreeMap<String, Definite>),
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

    /// Construct a `Type::Definite(Variable(id))`.
    pub fn variable(id: VarId) -> Self {
        Type::Definite(Box::new(Definite::Variable(id)))
    }

    /// Construct an "anonymous unconstrained variable" type — the
    /// v2 replacement for v1's `Type::Any`. Allocates a globally-
    /// unique [`VarId`]; for rule-compile-time anonymous
    /// allocation use
    /// [`UnificationContext::fresh`](UnificationContext::fresh)
    /// with `PrimitiveSet::ALL`.
    pub fn any() -> Self {
        Type::variable(VarId::global_fresh())
    }

    /// Returns `true` iff this type is set-widened with `Absent`.
    pub fn is_optional(&self) -> bool {
        matches!(self, Type::Optional(_))
    }

    /// Returns the underlying [`Definite`] regardless of whether
    /// this is `Definite` or `Optional`. Used by callers that
    /// need to inspect the shape independently of optionality.
    pub fn shape(&self) -> &Definite {
        match self {
            Type::Definite(d) | Type::Optional(d) => d,
        }
    }

    /// Lift this type to set-widened (`Optional`). Idempotent —
    /// applying twice has no further effect (`Optional(Optional)`
    /// would be structurally redundant; the outer enum prevents
    /// it by construction).
    ///
    /// Used by [`DynamicAttributeQuery`](crate::DynamicAttributeQuery)
    /// when its [`Resolution`](crate::attribute::query::Resolution) is
    /// `Optional`: the schema's `is` slot needs to advertise
    /// "this slot may bind to Absent at the row layer."
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
    /// return it.
    pub fn as_singleton(&self) -> Option<ValueType> {
        match self {
            Definite::Primitive(set) => set.as_singleton(),
            Definite::Variable(_) => None,
        }
    }
}

/// Slot-level optionality used during unification. Tracks whether
/// a use site of a variable is wrapped in `Optional` independently
/// of the variable's own shape.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum MatchOptionality {
    /// The slot demands a Present value — `Type::Definite(_)`.
    Required,
    /// The slot tolerates `Absent` — `Type::Optional(_)`.
    Optional,
}

impl MatchOptionality {
    /// Lift a `Type` to its slot-level optionality plus the
    /// underlying shape.
    pub fn split(ty: &Type) -> (Self, &Definite) {
        match ty {
            Type::Definite(d) => (Self::Required, d),
            Type::Optional(d) => (Self::Optional, d),
        }
    }

    /// "Strictest wins" combine: `Required` ∧ `Optional` =
    /// `Required`. Used when two slots demand the same variable
    /// but with different optionality.
    pub fn combine(self, other: Self) -> Self {
        match (self, other) {
            (Self::Required, _) | (_, Self::Required) => Self::Required,
            (Self::Optional, Self::Optional) => Self::Optional,
        }
    }
}

/// Errors raised by unification.
#[derive(Debug, Clone, PartialEq, thiserror::Error)]
pub enum UnifyError {
    /// Two types' primitive constraint sets have empty
    /// intersection — no shape can satisfy both.
    #[error("constraint conflict between {left:?} and {right:?}")]
    ConstraintConflict {
        /// The narrower side.
        left: PrimitiveSet,
        /// The other narrower side.
        right: PrimitiveSet,
    },
    /// Occurs check: unifying a variable with a type that
    /// transitively contains the variable would produce an
    /// infinite type. Today's `Definite` doesn't compose, so
    /// this is unreachable; reserved for future `Record` /
    /// `Variant` constructors.
    #[error("occurs check failed for variable {var:?}")]
    OccursCheck {
        /// The offending variable.
        var: VarId,
    },
}

/// A rank-1 polymorphic type scheme. Used by formula declarations
/// to express generic signatures like
/// `Sum<T: Numeric>(left: T, right: T) -> T`.
///
/// Schemes are static Rust-side metadata, not part of the wire
/// format. Each formula module defines a `LazyLock<TypeScheme>`
/// constant; the registry maps formula identifiers (e.g.
/// `"math/sum"`) to their schemes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TypeScheme {
    /// Quantified variables and their constraints. Names are
    /// scope-local to this scheme.
    pub quantified: Vec<(SchemeVarName, PrimitiveSet)>,
    /// The scheme body, referencing quantified variables by
    /// name. Instantiation replaces names with fresh `VarId`s.
    pub body: SchemeBody,
}

/// A name binding a quantified type variable in a scheme. Local
/// to the scheme; instantiation replaces it with a fresh
/// [`VarId`].
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SchemeVarName(pub String);

impl SchemeVarName {
    /// Construct a name. Convention: single uppercase letter (`T`,
    /// `U`) for the most common case, but any string works.
    pub fn new(name: impl Into<String>) -> Self {
        Self(name.into())
    }
}

/// The body of a [`TypeScheme`] — either a function-like signature
/// (formula schemas) or a single value type.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SchemeBody {
    /// A function-like signature: parameter names → slot types.
    Schema(Vec<(String, SchemeType)>),
    /// A single value type — for non-function schemes.
    Type(SchemeType),
}

/// A type expression inside a [`TypeScheme`]. References
/// quantified variables by name; instantiation replaces names
/// with fresh runtime [`VarId`]s.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SchemeType {
    /// `Definite(SchemeDefinite)`.
    Definite(Box<SchemeDefinite>),
    /// `Optional(SchemeDefinite)` — set-widened slot.
    Optional(Box<SchemeDefinite>),
}

/// `Definite` shape inside a [`TypeScheme`]. Variables reference
/// quantified names; instantiation replaces with [`VarId`]s.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SchemeDefinite {
    /// Concrete primitive set — no variable.
    Primitive(PrimitiveSet),
    /// Reference to a quantified variable by name.
    Variable(SchemeVarName),
}

/// Per-rule unification context. Tracks the substitution map,
/// per-variable constraints, and a fresh-id counter.
#[derive(Debug, Clone, Default)]
pub struct UnificationContext {
    /// Substitution: variable → resolved (or partially-resolved)
    /// `Definite`. Updated in-place by [`Self::unify`]. The
    /// substituted value may itself contain variables that haven't
    /// yet been resolved.
    substitution: HashMap<VarId, Definite>,
    /// Per-variable primitive constraint. Populated when a
    /// variable is allocated; intersected during unification.
    constraints: HashMap<VarId, PrimitiveSet>,
    /// Counter for fresh `VarId` allocation within this context.
    next_id: u32,
}

impl UnificationContext {
    /// Create an empty context.
    pub fn new() -> Self {
        Self::default()
    }

    /// Allocate a fresh `VarId` with the given constraint.
    pub fn fresh(&mut self, constraint: PrimitiveSet) -> VarId {
        let id = VarId(self.next_id);
        self.next_id += 1;
        self.constraints.insert(id, constraint);
        id
    }

    /// Look up the constraint for a variable. Returns
    /// [`PrimitiveSet::ALL`] if the variable is unknown to this
    /// context (e.g. one allocated via [`VarId::global_fresh`]
    /// outside any context).
    pub fn constraint(&self, var: VarId) -> PrimitiveSet {
        self.constraints
            .get(&var)
            .copied()
            .unwrap_or(PrimitiveSet::ALL)
    }

    /// Apply the current substitution to a `Type`, recursively
    /// resolving any variables that have been bound. Variables
    /// that haven't been bound stay as `Variable(id)`.
    pub fn apply(&self, ty: &Type) -> Type {
        match ty {
            Type::Definite(d) => Type::Definite(Box::new(self.apply_definite(d))),
            Type::Optional(d) => Type::Optional(Box::new(self.apply_definite(d))),
        }
    }

    fn apply_definite(&self, d: &Definite) -> Definite {
        match d {
            Definite::Primitive(_) => d.clone(),
            Definite::Variable(id) => {
                if let Some(resolved) = self.substitution.get(id) {
                    self.apply_definite(resolved)
                } else {
                    Definite::Variable(*id)
                }
            }
        }
    }

    /// Robinson unification with constraint propagation. Updates
    /// `self` in-place. Returns `Err` on constraint conflict or
    /// occurs check failure.
    pub fn unify(&mut self, a: &Type, b: &Type) -> Result<(), UnifyError> {
        let a = self.apply(a);
        let b = self.apply(b);
        let (_a_opt, a_def) = MatchOptionality::split(&a);
        let (_b_opt, b_def) = MatchOptionality::split(&b);

        // Slot-level optionality is informational at this layer:
        // the underlying definite shapes are unified regardless,
        // and "strictest wins" means a Definite consumer paired
        // with an Optional producer narrows to Definite. Higher
        // layers (RuleAnalysis, Slice 7 enforcement) read the
        // slot-level optionality from each use site directly.

        self.unify_definite(a_def, b_def)?;

        Ok(())
    }

    fn unify_definite(&mut self, a: &Definite, b: &Definite) -> Result<(), UnifyError> {
        match (a, b) {
            // Two same variables: nothing to do.
            (Definite::Variable(x), Definite::Variable(y)) if x == y => Ok(()),
            // Two distinct variables: bind one to the other,
            // intersecting constraints.
            (Definite::Variable(x), Definite::Variable(y)) => {
                let cx = self.constraint(*x);
                let cy = self.constraint(*y);
                let merged = cx.intersect(cy).ok_or(UnifyError::ConstraintConflict {
                    left: cx,
                    right: cy,
                })?;
                self.constraints.insert(*x, merged);
                self.constraints.insert(*y, merged);
                self.substitution.insert(*y, Definite::Variable(*x));
                Ok(())
            }
            // Variable vs primitive: check constraint, substitute.
            (Definite::Variable(x), Definite::Primitive(p))
            | (Definite::Primitive(p), Definite::Variable(x)) => {
                let cx = self.constraint(*x);
                let merged = cx.intersect(*p).ok_or(UnifyError::ConstraintConflict {
                    left: cx,
                    right: *p,
                })?;
                self.constraints.insert(*x, merged);
                self.substitution.insert(*x, Definite::Primitive(merged));
                Ok(())
            }
            // Two primitives: intersect their sets.
            (Definite::Primitive(p), Definite::Primitive(q)) => p
                .intersect(*q)
                .ok_or(UnifyError::ConstraintConflict {
                    left: *p,
                    right: *q,
                })
                .map(|_| ()),
        }
    }

    /// Instantiate a [`TypeScheme`]: allocate fresh `VarId`s for
    /// each quantified name, then materialize the body into a
    /// concrete signature with substitutions applied.
    ///
    /// Two calls to `instantiate` on the same scheme produce
    /// independent fresh variables — different uses of the same
    /// formula don't conflate their type variables.
    pub fn instantiate(&mut self, scheme: &TypeScheme) -> InstantiatedScheme {
        let mut substitution: HashMap<SchemeVarName, VarId> = HashMap::new();
        for (name, constraint) in &scheme.quantified {
            let id = self.fresh(*constraint);
            substitution.insert(name.clone(), id);
        }
        InstantiatedScheme {
            body: instantiate_body(&scheme.body, &substitution),
            variables: substitution,
        }
    }
}

fn instantiate_body(body: &SchemeBody, sub: &HashMap<SchemeVarName, VarId>) -> InstantiatedBody {
    match body {
        SchemeBody::Schema(fields) => InstantiatedBody::Schema(
            fields
                .iter()
                .map(|(name, ty)| (name.clone(), instantiate_type(ty, sub)))
                .collect(),
        ),
        SchemeBody::Type(ty) => InstantiatedBody::Type(instantiate_type(ty, sub)),
    }
}

fn instantiate_type(ty: &SchemeType, sub: &HashMap<SchemeVarName, VarId>) -> Type {
    match ty {
        SchemeType::Definite(d) => Type::Definite(Box::new(instantiate_definite(d, sub))),
        SchemeType::Optional(d) => Type::Optional(Box::new(instantiate_definite(d, sub))),
    }
}

fn instantiate_definite(d: &SchemeDefinite, sub: &HashMap<SchemeVarName, VarId>) -> Definite {
    match d {
        SchemeDefinite::Primitive(p) => Definite::Primitive(*p),
        SchemeDefinite::Variable(name) => Definite::Variable(
            *sub.get(name)
                .expect("scheme references unquantified variable"),
        ),
    }
}

/// The result of instantiating a [`TypeScheme`]. Carries the
/// fresh-`VarId` substitution alongside the body so callers can
/// look up which `VarId` represents which scheme variable.
#[derive(Debug, Clone)]
pub struct InstantiatedScheme {
    /// The instantiated body — concrete `Type`s with `VarId`
    /// references in place of scheme-variable names.
    pub body: InstantiatedBody,
    /// Maps each scheme variable name to the fresh `VarId`
    /// allocated for this instantiation.
    pub variables: HashMap<SchemeVarName, VarId>,
}

/// An instantiated [`SchemeBody`].
#[derive(Debug, Clone)]
pub enum InstantiatedBody {
    /// Function-like signature: `Vec<(name, slot_type)>`.
    Schema(Vec<(String, Type)>),
    /// A single value type.
    Type(Type),
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

    // ============== PrimitiveSet ==============

    #[test]
    fn primitive_set_singleton() {
        let s = PrimitiveSet::singleton(ValueType::String);
        assert!(s.contains(ValueType::String));
        assert!(!s.contains(ValueType::UnsignedInt));
        assert_eq!(s.as_singleton(), Some(ValueType::String));
    }

    #[test]
    fn primitive_set_intersect_overlap() {
        let s = PrimitiveSet::NUMERIC
            .intersect(PrimitiveSet::singleton(ValueType::UnsignedInt))
            .unwrap();
        assert_eq!(s.as_singleton(), Some(ValueType::UnsignedInt));
    }

    #[test]
    fn primitive_set_intersect_disjoint() {
        assert!(
            PrimitiveSet::singleton(ValueType::String)
                .intersect(PrimitiveSet::singleton(ValueType::Entity))
                .is_none()
        );
    }

    #[test]
    fn primitive_set_includes_self() {
        assert!(PrimitiveSet::ALL.includes(PrimitiveSet::ALL));
        assert!(PrimitiveSet::NUMERIC.includes(PrimitiveSet::singleton(ValueType::UnsignedInt)));
    }

    #[test]
    fn primitive_set_iter() {
        let s = PrimitiveSet::NUMERIC;
        let members: Vec<_> = s.iter().collect();
        assert_eq!(members.len(), 3);
        assert!(members.contains(&ValueType::UnsignedInt));
        assert!(members.contains(&ValueType::SignedInt));
        assert!(members.contains(&ValueType::Float));
    }

    #[test]
    fn primitive_set_serde_round_trip() {
        let s = PrimitiveSet::NUMERIC;
        let j = serde_json::to_string(&s).unwrap();
        let back: PrimitiveSet = serde_json::from_str(&j).unwrap();
        assert_eq!(s, back);
    }

    // ============== Type construction ==============

    #[test]
    fn type_primitive_is_definite() {
        let t = p(ValueType::String);
        assert!(matches!(t, Type::Definite(_)));
        assert!(!t.is_optional());
    }

    #[test]
    fn type_optional_wraps_definite() {
        let t = o(ValueType::String);
        assert!(t.is_optional());
    }

    #[test]
    fn type_any_is_anonymous_variable() {
        let t = Type::any();
        match t.shape() {
            Definite::Variable(_) => {}
            other => panic!("expected anonymous variable, got {:?}", other),
        }
    }

    #[test]
    fn type_serde_round_trip_definite() {
        let t = p(ValueType::String);
        let j = serde_json::to_string(&t).unwrap();
        let back: Type = serde_json::from_str(&j).unwrap();
        assert_eq!(t, back);
    }

    #[test]
    fn type_serde_round_trip_optional() {
        let t = o(ValueType::Entity);
        let j = serde_json::to_string(&t).unwrap();
        let back: Type = serde_json::from_str(&j).unwrap();
        assert_eq!(t, back);
    }

    // ============== UnificationContext: variable basics ==============

    #[test]
    fn fresh_variables_are_distinct() {
        let mut ctx = UnificationContext::new();
        let a = ctx.fresh(PrimitiveSet::ALL);
        let b = ctx.fresh(PrimitiveSet::ALL);
        assert_ne!(a, b);
    }

    #[test]
    fn fresh_records_constraint() {
        let mut ctx = UnificationContext::new();
        let a = ctx.fresh(PrimitiveSet::NUMERIC);
        assert_eq!(ctx.constraint(a), PrimitiveSet::NUMERIC);
    }

    // ============== UnificationContext: unify primitive vs primitive ==============

    #[test]
    fn unify_two_concrete_primitives_same_succeeds() {
        let mut ctx = UnificationContext::new();
        ctx.unify(&p(ValueType::String), &p(ValueType::String))
            .unwrap();
    }

    #[test]
    fn unify_two_concrete_primitives_different_fails() {
        let mut ctx = UnificationContext::new();
        let result = ctx.unify(&p(ValueType::String), &p(ValueType::Entity));
        assert!(matches!(result, Err(UnifyError::ConstraintConflict { .. })));
    }

    // ============== UnificationContext: unify variable vs primitive ==============

    #[test]
    fn unify_variable_with_concrete_substitutes() {
        let mut ctx = UnificationContext::new();
        let v = ctx.fresh(PrimitiveSet::NUMERIC);
        ctx.unify(&Type::variable(v), &p(ValueType::UnsignedInt))
            .unwrap();
        let resolved = ctx.apply(&Type::variable(v));
        match resolved.shape() {
            Definite::Primitive(set) => {
                assert_eq!(set.as_singleton(), Some(ValueType::UnsignedInt));
            }
            other => panic!("expected concrete primitive, got {:?}", other),
        }
    }

    #[test]
    fn unify_variable_with_concrete_outside_constraint_fails() {
        let mut ctx = UnificationContext::new();
        let v = ctx.fresh(PrimitiveSet::NUMERIC);
        let result = ctx.unify(&Type::variable(v), &p(ValueType::String));
        assert!(matches!(result, Err(UnifyError::ConstraintConflict { .. })));
    }

    #[test]
    fn unify_variable_constraint_propagates() {
        let mut ctx = UnificationContext::new();
        let v = ctx.fresh(PrimitiveSet::NUMERIC);
        let v_ty = Type::variable(v);
        let comparable = Type::primitive_set(PrimitiveSet::COMPARABLE);
        ctx.unify(&v_ty, &comparable).unwrap();
        let resolved = ctx.apply(&v_ty);
        match resolved.shape() {
            Definite::Primitive(set) => {
                assert_eq!(*set, PrimitiveSet::NUMERIC);
            }
            other => panic!("expected primitive, got {:?}", other),
        }
    }

    // ============== UnificationContext: unify variable vs variable ==============

    #[test]
    fn unify_two_variables_intersects_constraints() {
        let mut ctx = UnificationContext::new();
        let a = ctx.fresh(PrimitiveSet::NUMERIC);
        let b = ctx.fresh(PrimitiveSet::COMPARABLE);
        ctx.unify(&Type::variable(a), &Type::variable(b)).unwrap();
        assert_eq!(ctx.constraint(a), PrimitiveSet::NUMERIC);
        assert_eq!(ctx.constraint(b), PrimitiveSet::NUMERIC);
    }

    #[test]
    fn unify_two_variables_disjoint_fails() {
        let mut ctx = UnificationContext::new();
        let a = ctx.fresh(PrimitiveSet::NUMERIC);
        let b = ctx.fresh(PrimitiveSet::singleton(ValueType::String));
        let result = ctx.unify(&Type::variable(a), &Type::variable(b));
        assert!(matches!(result, Err(UnifyError::ConstraintConflict { .. })));
    }

    #[test]
    fn chained_unification_propagates() {
        let mut ctx = UnificationContext::new();
        let t = ctx.fresh(PrimitiveSet::NUMERIC);
        let u = ctx.fresh(PrimitiveSet::NUMERIC);
        ctx.unify(&Type::variable(t), &Type::variable(u)).unwrap();
        ctx.unify(&Type::variable(t), &p(ValueType::UnsignedInt))
            .unwrap();
        let t_resolved = ctx.apply(&Type::variable(t));
        let u_resolved = ctx.apply(&Type::variable(u));
        assert_eq!(
            t_resolved.shape().as_singleton(),
            Some(ValueType::UnsignedInt)
        );
        assert_eq!(
            u_resolved.shape().as_singleton(),
            Some(ValueType::UnsignedInt)
        );
    }

    // ============== UnificationContext: optionality interaction ==============

    #[test]
    fn unify_optional_with_definite_same_shape_succeeds() {
        let mut ctx = UnificationContext::new();
        ctx.unify(&p(ValueType::String), &o(ValueType::String))
            .unwrap();
    }

    #[test]
    fn unify_optional_with_definite_disjoint_fails() {
        let mut ctx = UnificationContext::new();
        let result = ctx.unify(&p(ValueType::String), &o(ValueType::Entity));
        assert!(matches!(result, Err(UnifyError::ConstraintConflict { .. })));
    }

    // ============== TypeScheme: instantiation ==============

    fn sum_scheme() -> TypeScheme {
        TypeScheme {
            quantified: vec![(SchemeVarName::new("T"), PrimitiveSet::NUMERIC)],
            body: SchemeBody::Schema(vec![
                (
                    "left".into(),
                    SchemeType::Definite(Box::new(SchemeDefinite::Variable(SchemeVarName::new(
                        "T",
                    )))),
                ),
                (
                    "right".into(),
                    SchemeType::Definite(Box::new(SchemeDefinite::Variable(SchemeVarName::new(
                        "T",
                    )))),
                ),
                (
                    "out".into(),
                    SchemeType::Definite(Box::new(SchemeDefinite::Variable(SchemeVarName::new(
                        "T",
                    )))),
                ),
            ]),
        }
    }

    #[test]
    fn instantiate_allocates_fresh_variables() {
        let scheme = sum_scheme();
        let mut ctx = UnificationContext::new();
        let i1 = ctx.instantiate(&scheme);
        let i2 = ctx.instantiate(&scheme);
        let t1 = i1.variables.get(&SchemeVarName::new("T")).copied().unwrap();
        let t2 = i2.variables.get(&SchemeVarName::new("T")).copied().unwrap();
        assert_ne!(t1, t2);
        assert_eq!(ctx.constraint(t1), PrimitiveSet::NUMERIC);
        assert_eq!(ctx.constraint(t2), PrimitiveSet::NUMERIC);
    }

    #[test]
    fn instantiate_shared_variable_in_body() {
        let scheme = sum_scheme();
        let mut ctx = UnificationContext::new();
        let inst = ctx.instantiate(&scheme);
        if let InstantiatedBody::Schema(fields) = &inst.body {
            let var_ids: Vec<VarId> = fields
                .iter()
                .map(|(_, ty)| match ty.shape() {
                    Definite::Variable(id) => *id,
                    other => panic!("expected variable, got {:?}", other),
                })
                .collect();
            assert_eq!(var_ids.len(), 3);
            assert_eq!(var_ids[0], var_ids[1]);
            assert_eq!(var_ids[1], var_ids[2]);
        } else {
            panic!("expected Schema body");
        }
    }

    #[test]
    fn instantiated_polymorphic_formula_unifies() {
        let scheme = sum_scheme();
        let mut ctx = UnificationContext::new();
        let inst = ctx.instantiate(&scheme);
        let fields = match &inst.body {
            InstantiatedBody::Schema(f) => f,
            _ => panic!(),
        };
        ctx.unify(&fields[0].1, &p(ValueType::UnsignedInt)).unwrap();
        for (_, ty) in fields {
            let resolved = ctx.apply(ty);
            assert_eq!(
                resolved.shape().as_singleton(),
                Some(ValueType::UnsignedInt),
                "expected u32 after unification"
            );
        }
    }

    #[test]
    fn instantiated_polymorphic_formula_rejects_mismatch() {
        let scheme = sum_scheme();
        let mut ctx = UnificationContext::new();
        let inst = ctx.instantiate(&scheme);
        let fields = match &inst.body {
            InstantiatedBody::Schema(f) => f,
            _ => panic!(),
        };
        ctx.unify(&fields[0].1, &p(ValueType::UnsignedInt)).unwrap();
        let result = ctx.unify(&fields[1].1, &p(ValueType::String));
        assert!(matches!(result, Err(UnifyError::ConstraintConflict { .. })));
    }

    #[test]
    fn two_instantiations_are_independent() {
        // First instantiation pins T to u32; second instantiation
        // should still be free to pin T to a different type.
        let scheme = sum_scheme();
        let mut ctx = UnificationContext::new();
        let i1 = ctx.instantiate(&scheme);
        let i2 = ctx.instantiate(&scheme);
        let i1_left = match &i1.body {
            InstantiatedBody::Schema(f) => f[0].1.clone(),
            _ => panic!(),
        };
        let i2_left = match &i2.body {
            InstantiatedBody::Schema(f) => f[0].1.clone(),
            _ => panic!(),
        };
        ctx.unify(&i1_left, &p(ValueType::UnsignedInt)).unwrap();
        // Independent — second instantiation can pin to a different
        // member of NUMERIC without conflict.
        ctx.unify(&i2_left, &p(ValueType::Float)).unwrap();
    }

    // ============== MatchOptionality ==============

    #[test]
    fn match_optionality_split() {
        let (opt, _) = MatchOptionality::split(&p(ValueType::String));
        assert_eq!(opt, MatchOptionality::Required);
        let (opt, _) = MatchOptionality::split(&o(ValueType::String));
        assert_eq!(opt, MatchOptionality::Optional);
    }

    #[test]
    fn match_optionality_combine_strictest_wins() {
        assert_eq!(
            MatchOptionality::Required.combine(MatchOptionality::Optional),
            MatchOptionality::Required
        );
        assert_eq!(
            MatchOptionality::Optional.combine(MatchOptionality::Required),
            MatchOptionality::Required
        );
        assert_eq!(
            MatchOptionality::Optional.combine(MatchOptionality::Optional),
            MatchOptionality::Optional
        );
    }
}

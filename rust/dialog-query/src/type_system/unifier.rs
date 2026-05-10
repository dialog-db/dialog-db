//! Unifier-internal type representation and Damas-Milner unifier.
//!
//! This submodule owns everything that needs to talk about
//! per-rule type variables: [`VarId`] allocation, the
//! [`Type`] enum with its `Static`/`Variable` distinction, the
//! [`Context`] that drives unification, and the [`TypeScheme`]
//! machinery for rank-1 polymorphic formula signatures.
//!
//! Variables never escape into the user-facing
//! [`StaticType`]. Lifting between layers happens via [`lift`]
//! and [`Context::infer`].

use crate::artifact::Type as ValueType;
use crate::type_system::Primitive;
use crate::type_system::Type as StaticType;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// A unique identifier for a type variable within a unification
/// context.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct VarId(u32);

/// Unifier-internal type — either a static
/// [user-facing type](StaticType) or a fresh type variable.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Type {
    /// A static, user-facing type lifted into the unifier domain.
    Static(StaticType),
    /// A type variable allocated by a [`Context`].
    Variable(VarId),
}

impl Type {
    /// Build a static primitive type, wrapped for unification.
    pub fn primitive(vt: ValueType) -> Self {
        Type::Static(StaticType::primitive(vt))
    }

    /// Build a static primitive set, wrapped for unification.
    pub fn primitive_set(set: Primitive) -> Self {
        Type::Static(StaticType::primitive_set(set))
    }

    /// Build a static optional primitive, wrapped for unification.
    pub fn optional(vt: ValueType) -> Self {
        Type::Static(StaticType::primitive(vt).optional())
    }
}

/// Lift a static [user-facing type](StaticType) into the unifier
/// domain. Always wraps in [`Type::Static`].
pub fn lift(ty: &StaticType) -> Type {
    Type::Static(ty.clone())
}

/// Slot-level optionality used during unification. Tracks whether
/// a use site of a variable is wrapped in `Optional` independently
/// of the variable's own shape.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum MatchOptionality {
    /// The slot demands a Present value.
    Required,
    /// The slot tolerates `Absent`.
    Optional,
}

impl MatchOptionality {
    /// "Strictest wins" combine: `Required` ∧ `Optional` =
    /// `Required`.
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
        left: Primitive,
        /// The other narrower side.
        right: Primitive,
    },
    /// Occurs check: unifying a variable with a type that
    /// transitively contains the variable would produce an
    /// infinite type. Reserved for future composite shapes.
    #[error("occurs check failed for variable {var:?}")]
    OccursCheck {
        /// The offending variable.
        var: VarId,
    },
    /// A `Coalesce` source type was expected to be set-widened
    /// with `Nothing` but is not. Raised by
    /// [`Coalesce::validate`](crate::constraint::Coalesce::validate).
    #[error("Coalesce source is not Optional")]
    SourceNotOptional,
}

/// A rank-1 polymorphic type scheme. Used by formula declarations
/// to express generic signatures.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TypeScheme {
    /// Quantified variables and their constraints.
    pub quantified: Vec<(SchemeVarName, Primitive)>,
    /// The scheme body, referencing quantified variables by name.
    pub body: SchemeBody,
}

/// A name binding a quantified type variable in a scheme.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SchemeVarName(pub String);

impl SchemeVarName {
    /// Construct a name.
    pub fn new(name: impl Into<String>) -> Self {
        Self(name.into())
    }
}

/// The body of a [`TypeScheme`] — either a function-like signature
/// or a single value type.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SchemeBody {
    /// A function-like signature: parameter names → slot types.
    Schema(Vec<(String, SchemeType)>),
    /// A single value type.
    Type(SchemeType),
}

/// A type expression inside a [`TypeScheme`]. The `optional` flag
/// requests set-widening (adds the `Nothing` bit) when the scheme
/// is instantiated to a static type.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SchemeType {
    /// Required slot — produces a plain primitive after
    /// instantiation.
    Required(Box<SchemeShape>),
    /// Optional slot — produces a primitive set widened with
    /// `Nothing` after instantiation.
    Optional(Box<SchemeShape>),
}

/// Primitive or variable shape inside a [`TypeScheme`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SchemeShape {
    /// Concrete primitive set — no variable.
    Primitive(Primitive),
    /// Reference to a quantified variable by name.
    Variable(SchemeVarName),
}

/// Per-rule unification context. Tracks the substitution map,
/// per-variable constraints, named variables in lexical scope,
/// and a fresh-id counter.
#[derive(Debug, Clone, Default)]
pub struct Context {
    /// Substitution: variable → resolved (or partially-resolved)
    /// type. Updated in-place by [`Self::unify`].
    substitution: HashMap<VarId, Type>,
    /// Per-variable primitive constraint.
    constraints: HashMap<VarId, Primitive>,
    /// Lexically-scoped name → `VarId` map; ensures that the same
    /// name within a scope refers to the same variable.
    names: HashMap<String, VarId>,
    /// Counter for fresh `VarId` allocation within this context.
    next_id: u32,
}

impl Context {
    /// Create an empty context.
    pub fn new() -> Self {
        Self::default()
    }

    /// Allocate a fresh `VarId` with the given constraint.
    pub fn fresh(&mut self, constraint: Primitive) -> VarId {
        let id = VarId(self.next_id);
        self.next_id += 1;
        self.constraints.insert(id, constraint);
        id
    }

    /// Allocate a fresh anonymous variable type with constraint
    /// [`Primitive::ALL`].
    pub fn fresh_var(&mut self) -> Type {
        Type::Variable(self.fresh(Primitive::ALL))
    }

    /// Look up or allocate the `VarId` associated with a name in
    /// this scope. The first call for a given name allocates;
    /// later calls return the same id.
    pub fn var_for_name(&mut self, name: &str) -> VarId {
        if let Some(id) = self.names.get(name) {
            return *id;
        }
        let id = self.fresh(Primitive::ALL);
        self.names.insert(name.to_string(), id);
        id
    }

    /// Look up the constraint for a variable. Returns
    /// [`Primitive::ALL`] if the variable is unknown to this
    /// context.
    pub fn constraint(&self, var: VarId) -> Primitive {
        self.constraints
            .get(&var)
            .copied()
            .unwrap_or(Primitive::ALL)
    }

    /// Materialize a [`Type`] for a slot from optional name and
    /// optional static kind:
    ///
    /// - `(Some(_), Some(t))` and `(None, Some(t))`:
    ///   wrap `t` as [`Type::Static`] (the user supplied a static
    ///   type, so no variable is needed).
    /// - `(Some(name), None)`: look up or allocate a variable for
    ///   the name; return [`Type::Variable`].
    /// - `(None, None)`: allocate a fresh anonymous variable;
    ///   return [`Type::Variable`].
    pub fn infer(&mut self, name: Option<&str>, kind: Option<StaticType>) -> Type {
        match (name, kind) {
            (_, Some(t)) => Type::Static(t),
            (Some(n), None) => Type::Variable(self.var_for_name(n)),
            (None, None) => self.fresh_var(),
        }
    }

    /// Apply the current substitution to a [`Type`], recursively
    /// resolving any variables that have been bound.
    pub fn apply(&self, ty: &Type) -> Type {
        match ty {
            Type::Static(s) => Type::Static(s.clone()),
            Type::Variable(id) => {
                if let Some(resolved) = self.substitution.get(id) {
                    self.apply(resolved)
                } else {
                    Type::Variable(*id)
                }
            }
        }
    }

    /// Robinson unification with constraint propagation. Updates
    /// `self` in-place. Returns `Err` on constraint conflict.
    pub fn unify(&mut self, a: &Type, b: &Type) -> Result<(), UnifyError> {
        let a = self.apply(a);
        let b = self.apply(b);
        match (a, b) {
            (Type::Variable(x), Type::Variable(y)) if x == y => Ok(()),
            (Type::Variable(x), Type::Variable(y)) => {
                let cx = self.constraint(x);
                let cy = self.constraint(y);
                let merged = cx.intersect(cy).ok_or(UnifyError::ConstraintConflict {
                    left: cx,
                    right: cy,
                })?;
                self.constraints.insert(x, merged);
                self.constraints.insert(y, merged);
                self.substitution.insert(y, Type::Variable(x));
                Ok(())
            }
            (Type::Variable(x), Type::Static(s)) | (Type::Static(s), Type::Variable(x)) => {
                let cx = self.constraint(x);
                let p = primitive_set_of(&s).ok_or(UnifyError::ConstraintConflict {
                    left: cx,
                    right: Primitive::EMPTY,
                })?;
                let merged = cx
                    .intersect(p)
                    .ok_or(UnifyError::ConstraintConflict { left: cx, right: p })?;
                self.constraints.insert(x, merged);
                self.substitution
                    .insert(x, Type::Static(StaticType::primitive_set(merged)));
                Ok(())
            }
            (Type::Static(a), Type::Static(b)) => {
                let pa = primitive_set_of(&a).ok_or(UnifyError::ConstraintConflict {
                    left: Primitive::EMPTY,
                    right: Primitive::EMPTY,
                })?;
                let pb = primitive_set_of(&b).ok_or(UnifyError::ConstraintConflict {
                    left: pa,
                    right: Primitive::EMPTY,
                })?;
                pa.intersect(pb)
                    .ok_or(UnifyError::ConstraintConflict {
                        left: pa,
                        right: pb,
                    })
                    .map(|_| ())
            }
        }
    }

    /// Instantiate a [`TypeScheme`]: allocate fresh `VarId`s for
    /// each quantified name, then materialize the body.
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

/// Extract the primitive set of a static type. For composite-only
/// types (no primitive bits and no admissible singleton) returns
/// `None`. The unifier currently operates over the primitive part
/// alone; composite-only constraints are reserved for future use.
fn primitive_set_of(ty: &StaticType) -> Option<Primitive> {
    let p = ty.primitive_part();
    if p.is_empty() { None } else { Some(p) }
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
        SchemeType::Required(d) => instantiate_shape(d, sub, false),
        SchemeType::Optional(d) => instantiate_shape(d, sub, true),
    }
}

fn instantiate_shape(d: &SchemeShape, sub: &HashMap<SchemeVarName, VarId>, optional: bool) -> Type {
    match d {
        SchemeShape::Primitive(p) => {
            let static_ty = if optional {
                StaticType::primitive_set(*p).optional()
            } else {
                StaticType::primitive_set(*p)
            };
            Type::Static(static_ty)
        }
        SchemeShape::Variable(name) => Type::Variable(
            *sub.get(name)
                .expect("scheme references unquantified variable"),
        ),
    }
}

/// The result of instantiating a [`TypeScheme`].
#[derive(Debug, Clone)]
pub struct InstantiatedScheme {
    /// The instantiated body.
    pub body: InstantiatedBody,
    /// Maps each scheme variable name to the fresh `VarId`
    /// allocated for this instantiation.
    pub variables: HashMap<SchemeVarName, VarId>,
}

/// An instantiated [`SchemeBody`].
#[derive(Debug, Clone)]
pub enum InstantiatedBody {
    /// Function-like signature.
    Schema(Vec<(String, Type)>),
    /// A single type.
    Type(Type),
}

#[cfg(test)]
mod tests {
    use super::*;

    fn p(vt: ValueType) -> Type {
        Type::primitive(vt)
    }

    #[dialog_common::test]
    fn fresh_variables_are_distinct() {
        let mut ctx = Context::new();
        let a = ctx.fresh(Primitive::ALL);
        let b = ctx.fresh(Primitive::ALL);
        assert_ne!(a, b);
    }

    #[dialog_common::test]
    fn fresh_records_constraint() {
        let mut ctx = Context::new();
        let a = ctx.fresh(Primitive::NUMERIC);
        assert_eq!(ctx.constraint(a), Primitive::NUMERIC);
    }

    #[dialog_common::test]
    fn unify_two_concrete_primitives_same_succeeds() {
        let mut ctx = Context::new();
        ctx.unify(&p(ValueType::String), &p(ValueType::String))
            .unwrap();
    }

    #[dialog_common::test]
    fn unify_two_concrete_primitives_different_fails() {
        let mut ctx = Context::new();
        let result = ctx.unify(&p(ValueType::String), &p(ValueType::Entity));
        assert!(matches!(result, Err(UnifyError::ConstraintConflict { .. })));
    }

    #[dialog_common::test]
    fn unify_variable_with_concrete_substitutes() {
        let mut ctx = Context::new();
        let v = ctx.fresh(Primitive::NUMERIC);
        ctx.unify(&Type::Variable(v), &p(ValueType::UnsignedInt))
            .unwrap();
        let resolved = ctx.apply(&Type::Variable(v));
        match resolved {
            Type::Static(s) => {
                assert_eq!(s.as_value_type(), Some(ValueType::UnsignedInt));
            }
            other => panic!("expected static, got {:?}", other),
        }
    }

    #[dialog_common::test]
    fn unify_variable_with_concrete_outside_constraint_fails() {
        let mut ctx = Context::new();
        let v = ctx.fresh(Primitive::NUMERIC);
        let result = ctx.unify(&Type::Variable(v), &p(ValueType::String));
        assert!(matches!(result, Err(UnifyError::ConstraintConflict { .. })));
    }

    #[dialog_common::test]
    fn unify_variable_constraint_propagates() {
        let mut ctx = Context::new();
        let v = ctx.fresh(Primitive::NUMERIC);
        let v_ty = Type::Variable(v);
        let comparable = Type::primitive_set(Primitive::COMPARABLE);
        ctx.unify(&v_ty, &comparable).unwrap();
        let resolved = ctx.apply(&v_ty);
        match resolved {
            Type::Static(s) => assert_eq!(s.primitive_part(), Primitive::NUMERIC),
            other => panic!("expected static, got {:?}", other),
        }
    }

    #[dialog_common::test]
    fn unify_two_variables_intersects_constraints() {
        let mut ctx = Context::new();
        let a = ctx.fresh(Primitive::NUMERIC);
        let b = ctx.fresh(Primitive::COMPARABLE);
        ctx.unify(&Type::Variable(a), &Type::Variable(b)).unwrap();
        assert_eq!(ctx.constraint(a), Primitive::NUMERIC);
        assert_eq!(ctx.constraint(b), Primitive::NUMERIC);
    }

    #[dialog_common::test]
    fn unify_two_variables_disjoint_fails() {
        let mut ctx = Context::new();
        let a = ctx.fresh(Primitive::NUMERIC);
        let b = ctx.fresh(Primitive::singleton(ValueType::String));
        let result = ctx.unify(&Type::Variable(a), &Type::Variable(b));
        assert!(matches!(result, Err(UnifyError::ConstraintConflict { .. })));
    }

    #[dialog_common::test]
    fn chained_unification_propagates() {
        let mut ctx = Context::new();
        let t = ctx.fresh(Primitive::NUMERIC);
        let u = ctx.fresh(Primitive::NUMERIC);
        ctx.unify(&Type::Variable(t), &Type::Variable(u)).unwrap();
        ctx.unify(&Type::Variable(t), &p(ValueType::UnsignedInt))
            .unwrap();
        let t_resolved = ctx.apply(&Type::Variable(t));
        let u_resolved = ctx.apply(&Type::Variable(u));
        match (t_resolved, u_resolved) {
            (Type::Static(t), Type::Static(u)) => {
                assert_eq!(t.as_value_type(), Some(ValueType::UnsignedInt));
                assert_eq!(u.as_value_type(), Some(ValueType::UnsignedInt));
            }
            other => panic!("unexpected: {:?}", other),
        }
    }

    #[dialog_common::test]
    fn unify_optional_with_definite_same_shape_succeeds() {
        let mut ctx = Context::new();
        ctx.unify(&p(ValueType::String), &Type::optional(ValueType::String))
            .unwrap();
    }

    #[dialog_common::test]
    fn unify_optional_with_definite_disjoint_fails() {
        let mut ctx = Context::new();
        let result = ctx.unify(&p(ValueType::String), &Type::optional(ValueType::Entity));
        assert!(matches!(result, Err(UnifyError::ConstraintConflict { .. })));
    }

    fn sum_scheme() -> TypeScheme {
        TypeScheme {
            quantified: vec![(SchemeVarName::new("T"), Primitive::NUMERIC)],
            body: SchemeBody::Schema(vec![
                (
                    "left".into(),
                    SchemeType::Required(Box::new(SchemeShape::Variable(SchemeVarName::new("T")))),
                ),
                (
                    "right".into(),
                    SchemeType::Required(Box::new(SchemeShape::Variable(SchemeVarName::new("T")))),
                ),
                (
                    "out".into(),
                    SchemeType::Required(Box::new(SchemeShape::Variable(SchemeVarName::new("T")))),
                ),
            ]),
        }
    }

    #[dialog_common::test]
    fn instantiate_allocates_fresh_variables() {
        let scheme = sum_scheme();
        let mut ctx = Context::new();
        let i1 = ctx.instantiate(&scheme);
        let i2 = ctx.instantiate(&scheme);
        let t1 = i1.variables.get(&SchemeVarName::new("T")).copied().unwrap();
        let t2 = i2.variables.get(&SchemeVarName::new("T")).copied().unwrap();
        assert_ne!(t1, t2);
        assert_eq!(ctx.constraint(t1), Primitive::NUMERIC);
        assert_eq!(ctx.constraint(t2), Primitive::NUMERIC);
    }

    #[dialog_common::test]
    fn instantiate_shared_variable_in_body() {
        let scheme = sum_scheme();
        let mut ctx = Context::new();
        let inst = ctx.instantiate(&scheme);
        if let InstantiatedBody::Schema(fields) = &inst.body {
            let var_ids: Vec<VarId> = fields
                .iter()
                .map(|(_, ty)| match ty {
                    Type::Variable(id) => *id,
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

    #[dialog_common::test]
    fn instantiated_polymorphic_formula_unifies() {
        let scheme = sum_scheme();
        let mut ctx = Context::new();
        let inst = ctx.instantiate(&scheme);
        let fields = match &inst.body {
            InstantiatedBody::Schema(f) => f,
            _ => panic!(),
        };
        ctx.unify(&fields[0].1, &p(ValueType::UnsignedInt)).unwrap();
        for (_, ty) in fields {
            let resolved = ctx.apply(ty);
            match resolved {
                Type::Static(s) => assert_eq!(
                    s.as_value_type(),
                    Some(ValueType::UnsignedInt),
                    "expected u32 after unification"
                ),
                other => panic!("expected static, got {:?}", other),
            }
        }
    }

    #[dialog_common::test]
    fn instantiated_polymorphic_formula_rejects_mismatch() {
        let scheme = sum_scheme();
        let mut ctx = Context::new();
        let inst = ctx.instantiate(&scheme);
        let fields = match &inst.body {
            InstantiatedBody::Schema(f) => f,
            _ => panic!(),
        };
        ctx.unify(&fields[0].1, &p(ValueType::UnsignedInt)).unwrap();
        let result = ctx.unify(&fields[1].1, &p(ValueType::String));
        assert!(matches!(result, Err(UnifyError::ConstraintConflict { .. })));
    }

    #[dialog_common::test]
    fn two_instantiations_are_independent() {
        let scheme = sum_scheme();
        let mut ctx = Context::new();
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
        ctx.unify(&i2_left, &p(ValueType::Float)).unwrap();
    }

    #[dialog_common::test]
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

    /// `infer` with no name and no kind allocates a fresh
    /// anonymous variable.
    #[dialog_common::test]
    fn infer_anonymous_unconstrained() {
        let mut ctx = Context::new();
        let a = ctx.infer(None, None);
        let b = ctx.infer(None, None);
        match (&a, &b) {
            (Type::Variable(x), Type::Variable(y)) => assert_ne!(x, y),
            _ => panic!("expected two distinct variables"),
        }
    }

    /// `infer` with a name (no kind) allocates once per name in
    /// scope; repeated calls return the same variable.
    #[dialog_common::test]
    fn infer_named_is_stable_within_scope() {
        let mut ctx = Context::new();
        let a = ctx.infer(Some("x"), None);
        let b = ctx.infer(Some("x"), None);
        match (&a, &b) {
            (Type::Variable(x), Type::Variable(y)) => assert_eq!(x, y),
            _ => panic!("expected variables"),
        }
    }

    /// `infer` with a kind always wraps statically — no variable
    /// is allocated even when a name is given.
    #[dialog_common::test]
    fn infer_with_kind_wraps_statically() {
        let mut ctx = Context::new();
        let a = ctx.infer(Some("x"), Some(StaticType::primitive(ValueType::String)));
        match a {
            Type::Static(_) => {}
            other => panic!("expected static, got {:?}", other),
        }
    }
}

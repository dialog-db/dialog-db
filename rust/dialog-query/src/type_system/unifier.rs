//! Unifier-internal type representation and unification.
//!
//! This submodule owns everything that needs to talk about
//! per-rule type variables: [`VarId`] allocation, the
//! [`Type`] enum with its `Static`/`Variable` distinction, and
//! the [`Context`] that drives unification.
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
    /// this scope. The first call for a given name allocates with
    /// the maximally-permissive constraint [`Primitive::ANY`] —
    /// includes `Nothing`, so optionality can survive unification.
    /// Narrowing happens when slot kinds without `Nothing` unify
    /// against the variable.
    /// Later calls return the same id.
    pub fn var_for_name(&mut self, name: &str) -> VarId {
        if let Some(id) = self.names.get(name) {
            return *id;
        }
        let id = self.fresh(Primitive::ANY);
        self.names.insert(name.to_string(), id);
        id
    }

    /// Iterate over `(name, VarId)` pairs for every named variable
    /// allocated in this context. The order is unspecified.
    pub fn named_vars(&self) -> impl Iterator<Item = (&String, &VarId)> {
        self.names.iter()
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

    /// Walk the substitution following variable-to-variable links
    /// (union-find style), returning the canonical [`VarId`] for
    /// the chain. Stops at the first non-variable substitution or
    /// at an unsubstituted variable.
    fn root(&self, mut id: VarId) -> VarId {
        while let Some(Type::Variable(next)) = self.substitution.get(&id) {
            if *next == id {
                break;
            }
            id = *next;
        }
        id
    }

    /// Robinson unification with constraint propagation. Updates
    /// `self` in-place and returns the *unified type* — the
    /// principal meet of the two sides — so the result of every
    /// unification flows to the caller instead of being discarded:
    ///
    /// - variable / variable: the canonical variable of the merged
    ///   chain (its constraint is the met constraint);
    /// - variable / static: the narrowed static the variable now
    ///   resolves to;
    /// - static / static: the meet of the two statics.
    ///
    /// Returns `Err` on constraint conflict (an empty meet).
    ///
    /// Static types intersect via [`StaticType::intersect`], which
    /// narrows both primitive and composite parts (products by
    /// shared field-name set, variants by label). Variables carry
    /// a [`Primitive`] constraint that's intersected with the
    /// primitive part of any concrete type they're unified with.
    pub fn unify(&mut self, a: &Type, b: &Type) -> Result<Type, UnifyError> {
        match (a, b) {
            (Type::Variable(x), Type::Variable(y)) => {
                let x = self.root(*x);
                let y = self.root(*y);
                if x == y {
                    return Ok(Type::Variable(x));
                }
                // If either side has already been resolved to a
                // static, unify that resolved static against the
                // other variable instead.
                let xs = self.substitution.get(&x).cloned();
                let ys = self.substitution.get(&y).cloned();
                match (xs, ys) {
                    (Some(Type::Static(sx)), _) => {
                        return self.unify(&Type::Static(sx), &Type::Variable(y));
                    }
                    (_, Some(Type::Static(sy))) => {
                        return self.unify(&Type::Variable(x), &Type::Static(sy));
                    }
                    _ => {}
                }
                let cx = self.constraint(x);
                let cy = self.constraint(y);
                let merged = cx.intersect(cy).ok_or(UnifyError::ConstraintConflict {
                    left: cx,
                    right: cy,
                })?;
                self.constraints.insert(x, merged);
                self.constraints.insert(y, merged);
                self.substitution.insert(y, Type::Variable(x));
                Ok(Type::Variable(x))
            }
            (Type::Variable(x), Type::Static(s)) | (Type::Static(s), Type::Variable(x)) => {
                let x = self.root(*x);
                // If `x` is already resolved to a static, intersect
                // the previous resolution with the new static — the
                // variable's resolved type is the narrowest static
                // satisfying every unify call that touched it.
                let prev = match self.substitution.get(&x) {
                    Some(Type::Static(prev)) => Some(prev.clone()),
                    _ => None,
                };
                let narrowed_static = match prev {
                    Some(prev) => {
                        prev.intersect(s)
                            .ok_or_else(|| UnifyError::ConstraintConflict {
                                left: prev.primitive_part(),
                                right: s.primitive_part(),
                            })?
                    }
                    None => s.clone(),
                };
                let cx = self.constraint(x);
                let p = narrowed_static.primitive_part();
                let merged = cx
                    .intersect(p)
                    .ok_or(UnifyError::ConstraintConflict { left: cx, right: p })?;
                self.constraints.insert(x, merged);
                // Rebuild around the merged membership without
                // shedding the static's composite or refinement
                // structure.
                let resolved = narrowed_static.with_primitive_part(merged);
                self.substitution.insert(x, Type::Static(resolved.clone()));
                Ok(Type::Static(resolved))
            }
            (Type::Static(a), Type::Static(b)) => {
                a.intersect(b)
                    .map(Type::Static)
                    .ok_or_else(|| UnifyError::ConstraintConflict {
                        left: a.primitive_part(),
                        right: b.primitive_part(),
                    })
            }
        }
    }
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

    /// Static-static unification returns the meet, not merely a
    /// compatibility verdict: the result of every unification flows
    /// to the caller. Foundation for scheme instantiation.
    #[dialog_common::test]
    fn unify_static_static_returns_the_meet() {
        let mut ctx = Context::new();
        let unified = ctx
            .unify(
                &Type::primitive_set(Primitive::NUMERIC),
                &Type::primitive_set(Primitive::COMPARABLE),
            )
            .unwrap();
        match unified {
            Type::Static(s) => assert_eq!(
                s.primitive_part(),
                Primitive::NUMERIC.intersect(Primitive::COMPARABLE).unwrap(),
                "the meet of the two statics is returned"
            ),
            other => panic!("expected static, got {other:?}"),
        }
    }

    /// Variable-static unification returns the narrowed static the
    /// variable now resolves to.
    #[dialog_common::test]
    fn unify_var_static_returns_the_narrowed_static() {
        let mut ctx = Context::new();
        let v = ctx.fresh(Primitive::NUMERIC);
        let unified = ctx
            .unify(
                &Type::Variable(v),
                &Type::primitive_set(Primitive::COMPARABLE),
            )
            .unwrap();
        match unified {
            Type::Static(s) => assert_eq!(
                s.primitive_part(),
                Primitive::NUMERIC.intersect(Primitive::COMPARABLE).unwrap(),
                "the variable's constraint participates in the returned meet"
            ),
            other => panic!("expected static, got {other:?}"),
        }
    }

    /// Variable-variable unification returns the canonical variable
    /// of the merged chain.
    #[dialog_common::test]
    fn unify_var_var_returns_the_canonical_variable() {
        let mut ctx = Context::new();
        let a = ctx.fresh(Primitive::NUMERIC);
        let b = ctx.fresh(Primitive::COMPARABLE);
        let unified = ctx.unify(&Type::Variable(a), &Type::Variable(b)).unwrap();
        match unified {
            Type::Variable(root) => assert_eq!(
                ctx.constraint(root),
                Primitive::NUMERIC.intersect(Primitive::COMPARABLE).unwrap(),
                "the canonical variable carries the met constraint"
            ),
            other => panic!("expected variable, got {other:?}"),
        }
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

    /// A variable resolved against a refined static keeps the
    /// refinement: resolution narrows membership without shedding
    /// the rest of the type's structure.
    #[dialog_common::test]
    fn unify_variable_with_refined_static_keeps_the_refinement() {
        let mut ctx = Context::new();
        let v = ctx.fresh(Primitive::ANY);
        let refined = StaticType::primitive_set(Primitive::TEXTUAL)
            .with_prefix("did:")
            .expect("textual members");
        ctx.unify(&Type::Variable(v), &Type::Static(refined))
            .unwrap();
        // A later premise narrows membership further; the prefix
        // must survive that narrowing too.
        ctx.unify(&Type::Variable(v), &Type::primitive(ValueType::Entity))
            .unwrap();
        let resolved = ctx.apply(&Type::Variable(v));
        match resolved {
            Type::Static(s) => {
                assert_eq!(s.primitive_part().as_singleton(), Some(ValueType::Entity));
                assert_eq!(s.refinement().expect("refinement preserved").prefix, "did:");
            }
            other => panic!("expected static, got {other:?}"),
        }
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

//! Type inference over a rule's premises.
//!
//! Every named variable a rule's positive premises mention places a
//! constraint on that variable's type: each slot it appears in claims
//! a kind (from the slot's schema), and the variable's rule-level
//! type is the unification of those claims.
//!
//! This module runs that inference using the [`Context`] from
//! [`crate::type_system::unifier`] and produces a [`TypeEnv`] —
//! a name-keyed map from each variable to its inferred type. The
//! result is carried on the [`Conjunction`](super::Plan) so
//! downstream evaluators can consult per-variable types instead of
//! re-deriving them from each term's local kind.
//!
//! Untyped slots (those with no static `content_type`) still
//! contribute to inference via their requirement shape:
//!
//! - `Required` slots contribute `Primitive::ALL` — "any present
//!   value." This excludes `Nothing` from the variable's type.
//! - `Optional` slots contribute `Primitive::ANY` — "any present or
//!   absent value." This lets `Nothing` survive unification when
//!   every slot the variable visits is optional.
//!
//! Negation premises do not contribute. They filter on existing
//! bindings rather than introducing them.

use crate::Premise;
use crate::planner::Plan;
use crate::schema::Requirement;
use crate::type_system::Type as Kind;
use crate::type_system::unifier::{Context, Type as Inferred, lift};
use std::collections::HashMap;

use super::super::type_system::Primitive;

/// Inferred types for every named variable referenced by a rule's
/// positive premises.
///
/// Built by [`TypeEnv::infer`] during planning; carried on
/// [`Conjunction`](crate::planner::Conjunction) so evaluators can
/// look up rule-level types without re-running inference.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TypeEnv {
    by_name: HashMap<String, Kind>,
}

impl TypeEnv {
    /// Empty environment — no variables inferred.
    pub fn new() -> Self {
        Self::default()
    }

    /// Build a `TypeEnv` by walking the given plan steps. For each
    /// named variable mentioned by any positive premise's slots,
    /// unify the slot kinds and record the resulting type.
    ///
    /// Returns the environment even if unification fails for some
    /// variable: failures collapse that variable's type to the
    /// narrower input. The compile-time check for required-head
    /// optionality reads from the environment, so a failure here
    /// surfaces as either a missing key or an unexpected
    /// `Nothing`-admitting result — both already handled by the
    /// rule compiler.
    pub fn infer(steps: &[Plan]) -> Self {
        let mut ctx = Context::new();
        for step in steps {
            // Negation premises don't contribute — they filter on
            // bindings rather than introducing them.
            let Premise::Assert(_) = &step.premise else {
                continue;
            };
            let schema = step.premise.schema();
            let params = step.premise.parameters();

            for (slot_name, field) in schema.iter() {
                let Some(param) = params.get(slot_name) else {
                    continue;
                };
                let Some(var_name) = param.name() else {
                    continue;
                };
                let slot_kind: Kind = match field.content_type() {
                    Some(t) => t.clone(),
                    None => match field.requirement {
                        Requirement::Required(_) => Kind::primitive_set(Primitive::ALL),
                        Requirement::Optional => Kind::primitive_set(Primitive::ANY),
                    },
                };
                let var = ctx.var_for_name(var_name);
                // Errors here mean the rule has contradictory slot
                // kinds for the same variable — leave that for the
                // compile-time check to surface.
                let _ = ctx.unify(&lift(&slot_kind), &Inferred::Variable(var));
            }
        }

        let mut by_name = HashMap::new();
        for (name, var_id) in ctx.named_vars() {
            if let Inferred::Static(kind) = ctx.apply(&Inferred::Variable(*var_id)) {
                by_name.insert(name.clone(), kind);
            } else {
                // Variable never resolved to a static type — record
                // its constraint as a primitive set. This is the
                // "no slot ever gave us a concrete shape" case; the
                // rule compiler treats it as fully unconstrained.
                by_name.insert(name.clone(), Kind::primitive_set(ctx.constraint(*var_id)));
            }
        }
        Self { by_name }
    }

    /// Look up the inferred type for a variable by name.
    pub fn get(&self, name: &str) -> Option<&Kind> {
        self.by_name.get(name)
    }

    /// Iterate over `(name, inferred kind)` pairs.
    pub fn iter(&self) -> impl Iterator<Item = (&String, &Kind)> {
        self.by_name.iter()
    }

    /// Number of variables inferred.
    pub fn len(&self) -> usize {
        self.by_name.len()
    }

    /// `true` if no variables were inferred.
    pub fn is_empty(&self) -> bool {
        self.by_name.is_empty()
    }
}

#[cfg(test)]
mod tests {
    #[cfg(target_arch = "wasm32")]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    use super::*;
    use crate::artifact::{Entity, Type as ValueType};
    use crate::attribute::query::AttributeQuery;
    use crate::planner::Planner;
    use crate::types::Any;
    use crate::{Cardinality, Environment, Term, the};

    /// A typed slot kind flows into the variable's inferred type.
    #[dialog_common::test]
    fn it_records_inferred_kind_for_typed_slot() {
        let typed_name: Term<Any> = Term::<String>::var("name").into();
        let premises = vec![
            AttributeQuery::new(
                Term::from(the!("person/name")),
                Term::<Entity>::var("this"),
                typed_name,
                Term::var("cause"),
                Some(Cardinality::One),
            )
            .into(),
        ];
        let plan = Planner::from(premises).plan(&Environment::new()).unwrap();
        let env = TypeEnv::infer(&plan.steps);
        let name_kind = env.get("name").expect("name inferred");
        assert_eq!(name_kind.as_value_type(), Some(ValueType::String));
    }

    /// A variable bound only by optional slots keeps the `Nothing`
    /// bit in its inferred type.
    #[dialog_common::test]
    fn it_preserves_nothing_when_only_optional_bindings_exist() {
        let optional_name: Term<Any> = Term::<Option<String>>::var("name").into();
        let premises = vec![
            AttributeQuery::new(
                Term::from(the!("person/name")),
                Term::<Entity>::var("this"),
                optional_name,
                Term::var("cause"),
                Some(Cardinality::One),
            )
            .into(),
        ];
        let plan = Planner::from(premises).plan(&Environment::new()).unwrap();
        let env = TypeEnv::infer(&plan.steps);
        let name_kind = env.get("name").expect("name inferred");
        assert!(
            name_kind.is_optional(),
            "single optional binding leaves Nothing in the inferred type"
        );
    }

    /// A variable bound by both an optional and a required slot has
    /// `Nothing` removed by the intersection — the required slot
    /// wins.
    #[dialog_common::test]
    fn it_strips_nothing_when_a_required_binding_also_exists() {
        let optional_name: Term<Any> = Term::<Option<String>>::var("name").into();
        let typed_name: Term<Any> = Term::<String>::var("name").into();
        let premises = vec![
            AttributeQuery::new(
                Term::from(the!("person/nickname")),
                Term::<Entity>::var("this"),
                optional_name,
                Term::var("cause1"),
                Some(Cardinality::One),
            )
            .into(),
            AttributeQuery::new(
                Term::from(the!("person/name")),
                Term::<Entity>::var("this"),
                typed_name,
                Term::var("cause2"),
                Some(Cardinality::One),
            )
            .into(),
        ];
        let plan = Planner::from(premises).plan(&Environment::new()).unwrap();
        let env = TypeEnv::infer(&plan.steps);
        let name_kind = env.get("name").expect("name inferred");
        assert!(
            !name_kind.is_optional(),
            "Required + Optional bindings strip Nothing from the inferred type"
        );
        assert_eq!(name_kind.as_value_type(), Some(ValueType::String));
    }
}

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
//! planner consumes the env to rewrite each premise's variable
//! terms so they carry the inferred kinds at evaluation time.
//!
//! Untyped slots (those with no static `content_type`) contribute
//! `Primitive::ALL` — "any present value" — regardless of their
//! requirement: a slot's `Requirement` speaks of *derivability*
//! (does the premise produce or demand the binding), never of
//! absence. Set-widening (admitting `Nothing`) is declared
//! exclusively through content types — a `OptionalAttributeQuery`'s value slot
//! or a concept's optional field.
//!
//! Negation premises do not contribute. They filter on existing
//! bindings rather than introducing them.

use crate::Premise;
use crate::formula::number::Numeric;
use crate::type_system::Primitive;
use crate::type_system::Type as Kind;
use crate::type_system::unifier::{Context, Type as Inferred, lift};
use std::collections::HashMap;

/// Errors raised by [`TypeEnv::infer`].
#[derive(Debug, Clone, PartialEq, thiserror::Error)]
pub enum InferenceError {
    /// A variable appears in slots whose declared kinds have no
    /// common type — unification produced a contradiction.
    #[error("variable {variable} has conflicting kinds across premises: {reason}")]
    Conflict {
        /// Name of the offending variable.
        variable: String,
        /// Underlying unifier error message.
        reason: String,
    },
}

/// Inferred types for every named variable referenced by a rule's
/// positive premises.
///
/// Built by [`TypeEnv::infer`] during planning. The planner uses
/// the result to narrow each premise's variable terms so they
/// carry rule-level kinds at evaluation time. Also carried on
/// [`AnalyzedRule`](super::AnalyzedRule) (wrapped in an `Arc`) for
/// later phases that want type-by-variable lookups.
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
    /// Returns `Err` if any variable's kind is contradictory across
    /// slots (e.g. `?x` is `String` in one premise and `Entity` in
    /// another). The variable name is returned with the error so
    /// the caller can surface a useful diagnostic.
    pub fn infer(premises: &[Premise]) -> Result<Self, InferenceError> {
        let mut ctx = Context::new();
        for premise in premises {
            // Negation premises don't contribute — they filter on
            // bindings rather than introducing them.
            let Premise::Assert(proposition) = premise else {
                continue;
            };

            // Formula premises may declare *schemes*: cells sharing a
            // label share one bounded type variable, instantiated
            // fresh per use of the formula. That linkage (and the
            // contribution of constant arguments, which narrow the
            // scheme) is expressed against the formula's cells
            // directly, so formulas take this arm instead of the
            // generic schema walk below.
            if let crate::Proposition::Formula(formula) = proposition {
                Self::infer_formula(&mut ctx, formula)?;
                continue;
            }

            let schema = premise.schema();
            let params = premise.parameters();

            for (slot_name, field) in schema.iter() {
                let Some(param) = params.get(slot_name) else {
                    continue;
                };
                let Some(var_name) = param.name() else {
                    continue;
                };
                // An untyped slot constrains the variable to "any
                // present value" regardless of its requirement.
                // `Requirement::Optional` means *derivable* (the
                // premise produces the binding rather than demanding
                // it) — derivability is not absence. Set-widening
                // (admitting `Nothing`) is declared exclusively
                // through content types: a `OptionalAttributeQuery`'s value slot
                // or a concept's optional field.
                let slot_kind: Kind = match field.content_type() {
                    Some(t) => t.clone(),
                    None => Kind::primitive_set(Primitive::ALL),
                };
                let var = ctx.var_for_name(var_name);
                if let Err(reason) = ctx.unify(&lift(&slot_kind), &Inferred::Variable(var)) {
                    return Err(InferenceError::Conflict {
                        variable: var_name.to_string(),
                        reason: reason.to_string(),
                    });
                }
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
        Ok(Self { by_name })
    }

    /// Contribute one formula premise to the unification context.
    ///
    /// Scheme-labeled cells sharing a label share one fresh type
    /// variable bounded by the cell's kind (the per-use
    /// instantiation of the formula's scheme); concrete cells
    /// contribute their kinds as before. Constant arguments unify
    /// too — a literal narrows the scheme it instantiates, and a
    /// literal outside a concrete cell's bound is a compile-time
    /// conflict.
    fn infer_formula(
        ctx: &mut Context,
        formula: &crate::FormulaQuery,
    ) -> Result<(), InferenceError> {
        use crate::type_system::unifier::Type as Inferred;
        let params = formula.parameters();
        let mut schemes: HashMap<String, Inferred> = HashMap::new();

        for (slot_name, cell) in formula.cells().iter() {
            let Some(param) = params.get(slot_name) else {
                continue;
            };

            let slot_ty = match cell.scheme_label() {
                Some(label) => schemes
                    .entry(label.to_string())
                    .or_insert_with(|| {
                        let bound = cell
                            .content_type()
                            .as_ref()
                            .map(|kind| kind.primitive_part())
                            .unwrap_or(Primitive::ALL);
                        Inferred::Variable(ctx.fresh(bound))
                    })
                    .clone(),
                None => match cell.content_type() {
                    Some(kind) => lift(kind),
                    None => lift(&Kind::primitive_set(Primitive::ALL)),
                },
            };

            let argument = match param.name() {
                Some(var_name) => Inferred::Variable(ctx.var_for_name(var_name)),
                None => match (param, cell.scheme_label()) {
                    // A numeric constant in a *scheme* slot is a
                    // polymorphic literal: it contributes the set of
                    // types it instantiates to losslessly, so `1`
                    // does not pin the scheme while `1.5` pins it to
                    // Float. (At evaluation the literal adapts to the
                    // row's instantiation; data never does.)
                    (crate::Term::Constant(value), Some(_)) => {
                        match Numeric::try_from(value.clone()) {
                            Ok(literal) => {
                                Inferred::Static(Kind::primitive_set(literal.admissible()))
                            }
                            // Not numeric: its singleton kind applies
                            // (and conflicts with a numeric bound).
                            Err(_) => match param.kind() {
                                Some(kind) => lift(&kind),
                                None => continue,
                            },
                        }
                    }
                    // A constant in a concrete slot: its kind narrows
                    // the slot.
                    _ => match param.kind() {
                        Some(kind) => lift(&kind),
                        // A blank contributes nothing.
                        None => continue,
                    },
                },
            };

            if let Err(reason) = ctx.unify(&slot_ty, &argument) {
                return Err(InferenceError::Conflict {
                    variable: param.name().unwrap_or(slot_name).to_string(),
                    reason: reason.to_string(),
                });
            }
        }
        Ok(())
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
    use crate::attribute::The;
    use crate::attribute::query::AttributeQuery;
    use crate::optional::OptionalAttributeQuery;
    use crate::planner::Planner;
    use crate::types::Any;
    use crate::{Cardinality, Environment, Term, the};

    /// Helper: an optional (set-widening) binding for `?name` — a
    /// `OptionalAttributeQuery` left-join whose schema admits `Nothing` for the
    /// value slot.
    fn optional_name_premise(the: Term<The>) -> Premise {
        OptionalAttributeQuery::new(
            the,
            Term::<Entity>::var("this"),
            Term::<String>::var("name").into(),
            Term::blank(),
            Some(Cardinality::One),
        )
        .into()
    }

    /// A type predicate narrows its subject rule-wide: occurrence
    /// typing as a premise.
    #[dialog_common::test]
    fn it_narrows_variables_via_type_predicates() -> anyhow::Result<()> {
        let scan = AttributeQuery::new(
            Term::from(the!("misc/tag")),
            Term::<Entity>::var("this"),
            Term::var("tag"),
            Term::var("cause"),
            Some(Cardinality::One),
        );
        let premises = vec![scan.into(), Term::<Any>::var("tag").number()];

        let env = TypeEnv::infer(&premises)?;
        let kind = env.get("tag").expect("inferred");
        assert_eq!(
            kind.primitive_part(),
            Primitive::NUMERIC,
            "the predicate narrowed the scan variable"
        );
        Ok(())
    }

    /// Conflicting type predicates are a compile-time error.
    #[dialog_common::test]
    fn it_rejects_conflicting_type_predicates() {
        let premises = vec![Term::<Any>::var("x").text(), Term::<Any>::var("x").number()];
        assert!(
            TypeEnv::infer(&premises).is_err(),
            "text and number have an empty meet"
        );
    }

    /// A formula scheme links its cells: one bounded type variable
    /// per label, shared by every cell carrying it, instantiated
    /// fresh for this use of the formula.
    #[dialog_common::test]
    fn it_instantiates_formula_schemes() -> anyhow::Result<()> {
        use crate::Proposition;
        use crate::formula::Formula;
        use crate::formula::math::Sum;

        let mut terms = crate::Parameters::new();
        terms.insert("of".to_string(), Term::var("a"));
        terms.insert("with".to_string(), Term::var("b"));
        terms.insert("is".to_string(), Term::var("c"));
        let premises = vec![Premise::Assert(Proposition::Formula(
            Sum::apply(terms)?.into(),
        ))];

        let env = TypeEnv::infer(&premises)?;
        for var in ["a", "b", "c"] {
            let kind = env.get(var).expect("inferred");
            assert_eq!(
                kind.primitive_part(),
                Primitive::NUMERIC,
                "{var} is bounded by the scheme"
            );
        }
        Ok(())
    }

    /// An *integer* literal is polymorphic: it fits every numeric
    /// type losslessly, so it does not pin the scheme — the linked
    /// variables stay bounded NUMERIC and the row's data decides.
    #[dialog_common::test]
    fn it_keeps_schemes_open_for_integer_literals() -> anyhow::Result<()> {
        use crate::Proposition;
        use crate::Value;
        use crate::formula::Formula;
        use crate::formula::math::Sum;

        let mut terms = crate::Parameters::new();
        terms.insert("of".to_string(), Term::var("a"));
        terms.insert(
            "with".to_string(),
            Term::<Any>::Constant(Value::UnsignedInt(1)),
        );
        terms.insert("is".to_string(), Term::var("c"));
        let premises = vec![Premise::Assert(Proposition::Formula(
            Sum::apply(terms)?.into(),
        ))];

        let env = TypeEnv::infer(&premises)?;
        for var in ["a", "c"] {
            let kind = env.get(var).expect("inferred");
            assert_eq!(
                kind.primitive_part(),
                Primitive::NUMERIC,
                "the integer literal must not pin {var}"
            );
        }
        Ok(())
    }

    /// A constant argument narrows the scheme it instantiates: a
    /// float literal makes every linked cell Float.
    #[dialog_common::test]
    fn it_narrows_schemes_by_constant_arguments() -> anyhow::Result<()> {
        use crate::Proposition;
        use crate::Value;
        use crate::formula::Formula;
        use crate::formula::math::Sum;

        let mut terms = crate::Parameters::new();
        terms.insert("of".to_string(), Term::var("a"));
        terms.insert("with".to_string(), Term::<Any>::Constant(Value::Float(1.5)));
        terms.insert("is".to_string(), Term::var("c"));
        let premises = vec![Premise::Assert(Proposition::Formula(
            Sum::apply(terms)?.into(),
        ))];

        let env = TypeEnv::infer(&premises)?;
        for var in ["a", "c"] {
            let kind = env.get(var).expect("inferred");
            assert_eq!(
                kind.as_value_type(),
                Some(ValueType::Float),
                "the literal narrowed {var} through the scheme"
            );
        }
        Ok(())
    }

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
        let env = TypeEnv::infer(&premises).unwrap();
        let name_kind = env.get("name").expect("name inferred");
        assert_eq!(name_kind.as_value_type(), Some(ValueType::String));
    }

    /// A variable bound only by optional slots keeps the `Nothing`
    /// bit in its inferred type.
    #[dialog_common::test]
    fn it_preserves_nothing_when_only_optional_bindings_exist() {
        let premises = vec![optional_name_premise(Term::from(the!("person/name")))];
        let env = TypeEnv::infer(&premises).unwrap();
        let name_kind = env.get("name").expect("name inferred");
        assert!(
            name_kind.is_optional(),
            "single optional binding leaves Nothing in the inferred type"
        );
    }

    /// Derivability is not absence: a variable bound only by an
    /// *untyped derivable* slot (a concept field with no declared
    /// type) infers "any present value" — no `Nothing` bit. Only
    /// set-widened content types introduce absence.
    #[dialog_common::test]
    fn it_does_not_widen_untyped_derivable_slots() {
        use crate::{AttributeDescriptor, ConceptDescriptor, ConceptQuery, Parameters};

        let concept = ConceptDescriptor::try_from(vec![(
            "tag",
            AttributeDescriptor::new(the!("misc/tag"), "", Cardinality::One, None),
        )])
        .unwrap();
        let mut terms = Parameters::new();
        terms.insert("this".to_string(), Term::var("entity"));
        terms.insert("tag".to_string(), Term::var("tag"));
        let premises = vec![Premise::Assert(crate::Proposition::Concept(ConceptQuery {
            terms,
            predicate: concept,
        }))];

        let env = TypeEnv::infer(&premises).unwrap();
        let tag_kind = env.get("tag").expect("tag inferred");
        assert!(
            !tag_kind.is_optional(),
            "an untyped derivable slot must not admit Nothing"
        );
    }

    /// The concept boundary declares set-widening: a variable bound
    /// by a concept's *optional* field admits `Nothing` in the
    /// consuming rule's TypeEnv, while a required sibling stays
    /// present-only.
    #[dialog_common::test]
    fn it_widens_variables_bound_by_concept_optional_fields() {
        use crate::{
            AttributeDescriptor, ConceptDescriptor, ConceptFieldDescriptor, ConceptQuery,
            Parameters,
        };

        let concept = ConceptDescriptor::try_from(vec![
            (
                "title".to_string(),
                ConceptFieldDescriptor::required(AttributeDescriptor::new(
                    the!("person/title"),
                    "",
                    Cardinality::One,
                    Some(ValueType::String),
                )),
            ),
            (
                "nickname".to_string(),
                ConceptFieldDescriptor::optional(AttributeDescriptor::new(
                    the!("person/nickname"),
                    "",
                    Cardinality::One,
                    Some(ValueType::String),
                )),
            ),
        ])
        .unwrap();
        let mut terms = Parameters::new();
        terms.insert("this".to_string(), Term::var("entity"));
        terms.insert("title".to_string(), Term::var("title"));
        terms.insert("nickname".to_string(), Term::var("nick"));
        let premises = vec![Premise::Assert(crate::Proposition::Concept(ConceptQuery {
            terms,
            predicate: concept,
        }))];

        let env = TypeEnv::infer(&premises).unwrap();
        assert!(
            env.get("nick").expect("nick inferred").is_optional(),
            "the optional field widens the consuming variable"
        );
        assert!(
            !env.get("title").expect("title inferred").is_optional(),
            "the required field stays present-only"
        );
    }

    /// A variable bound by both an optional and a required slot has
    /// `Nothing` removed by the intersection — the required slot
    /// wins.
    #[dialog_common::test]
    fn it_strips_nothing_when_a_required_binding_also_exists() {
        let typed_name: Term<Any> = Term::<String>::var("name").into();
        let premises = vec![
            optional_name_premise(Term::from(the!("person/nickname"))),
            AttributeQuery::new(
                Term::from(the!("person/name")),
                Term::<Entity>::var("this"),
                typed_name,
                Term::var("cause2"),
                Some(Cardinality::One),
            )
            .into(),
        ];
        let env = TypeEnv::infer(&premises).unwrap();
        let name_kind = env.get("name").expect("name inferred");
        assert!(
            !name_kind.is_optional(),
            "Required + Optional bindings strip Nothing from the inferred type"
        );
        assert_eq!(name_kind.as_value_type(), Some(ValueType::String));
    }

    /// Three premises, all referencing `?name`: two optional, one
    /// required. The required one alone is enough to strip
    /// `Nothing` from the inferred type. Verifies that
    /// inference is *intersection*, not union.
    #[dialog_common::test]
    fn it_strips_nothing_when_any_premise_is_required() {
        let opt_a: Term<Any> = Term::<Option<String>>::var("name").into();
        let opt_b: Term<Any> = Term::<Option<String>>::var("name").into();
        let req: Term<Any> = Term::<String>::var("name").into();
        let premises = vec![
            AttributeQuery::new(
                Term::from(the!("person/nickname")),
                Term::<Entity>::var("this"),
                opt_a,
                Term::var("c1"),
                Some(Cardinality::One),
            )
            .into(),
            AttributeQuery::new(
                Term::from(the!("person/alias")),
                Term::<Entity>::var("this"),
                opt_b,
                Term::var("c2"),
                Some(Cardinality::One),
            )
            .into(),
            AttributeQuery::new(
                Term::from(the!("person/name")),
                Term::<Entity>::var("this"),
                req,
                Term::var("c3"),
                Some(Cardinality::One),
            )
            .into(),
        ];
        let env = TypeEnv::infer(&premises).unwrap();
        let name_kind = env.get("name").expect("name inferred");
        assert!(
            !name_kind.is_optional(),
            "a single required binding among many optional ones strips Nothing"
        );
        assert_eq!(name_kind.as_value_type(), Some(ValueType::String));
    }

    /// Negation premises don't contribute to inference. A
    /// negation that mentions `?x` with kind `String` doesn't
    /// constrain the rule-level type of `?x`; the positive
    /// premise that binds `?x` is the sole contributor.
    #[dialog_common::test]
    fn it_ignores_negation_contributions_during_inference() {
        use crate::Proposition;
        use crate::negation::Negation;
        // Positive premise binds ?name optionally (a Maybe left-join).
        // Negation references ?name as Term<String> (non-optional).
        // If the negation contributed to inference, ?name's inferred
        // kind would be narrowed to String (no Nothing). But it
        // doesn't, so ?name stays set-widened.
        let strict_name: Term<Any> = Term::<String>::var("name").into();
        let neg_query = AttributeQuery::new(
            Term::from(the!("person/nickname")),
            Term::<Entity>::var("this"),
            strict_name,
            Term::blank(),
            Some(Cardinality::One),
        );
        let premises = vec![
            optional_name_premise(Term::from(the!("person/name"))),
            Premise::Unless(Negation(Proposition::Attribute(Box::new(neg_query)))),
        ];
        let env = TypeEnv::infer(&premises).unwrap();
        let name_kind = env.get("name").expect("name inferred");
        assert!(
            name_kind.is_optional(),
            "negation should not strip Nothing from `?name`'s inferred kind"
        );
    }

    /// A variable used as `Term<String>` in one premise and as
    /// `Term<u32>` in another has no consistent type — inference
    /// must report a conflict rather than silently producing one
    /// or the other. The planner surfaces this as
    /// `TypeError::TypeInference`.
    #[dialog_common::test]
    fn it_rejects_planning_when_variable_kinds_disagree() {
        use crate::error::TypeError;
        let as_string: Term<Any> = Term::<String>::var("x").into();
        let as_u32: Term<Any> = Term::<u32>::var("x").into();
        let premises = vec![
            AttributeQuery::new(
                Term::from(the!("thing/label")),
                Term::<Entity>::var("this"),
                as_string,
                Term::var("cause1"),
                Some(Cardinality::One),
            )
            .into(),
            AttributeQuery::new(
                Term::from(the!("thing/count")),
                Term::<Entity>::var("this"),
                as_u32,
                Term::var("cause2"),
                Some(Cardinality::One),
            )
            .into(),
        ];
        let err = Planner::from(premises)
            .plan(&Environment::new())
            .unwrap_err();
        match err {
            TypeError::TypeInference { reason } => {
                assert!(
                    reason.contains("x"),
                    "error mentions the conflicting variable name; got: {reason}"
                );
            }
            other => panic!("expected TypeInference, got {other:?}"),
        }
    }
}

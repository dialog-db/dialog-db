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
use crate::type_system::unifier::{Context, Type as Inferred, VarId, lift};
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

/// One narrowing step recorded during inference: `premise`
/// constrained `variable` to `to` (from `from`, or from nothing on
/// the variable's first contribution). The provenance behind
/// [`TypeEnv::explain`] — *which premise proved what* — and the
/// evidence the dead-optionality lint reads.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Narrowing {
    /// The variable whose kind changed.
    pub variable: String,
    /// Display form of the premise that narrowed it.
    pub premise: String,
    /// The kind before this premise; `None` on first contribution.
    pub from: Option<Kind>,
    /// The kind after this premise.
    pub to: Kind,
}

/// A dead-optionality finding: a premise declared the variable
/// optional (its slot admits `Nothing`), but the rule's other
/// premises require it present, so no result can ever carry it as
/// `Absent` — the declared optionality is unreachable. Not an
/// error (the rule is sound; the optional lookup is demoted to a
/// scalar scan), but almost certainly not what the author meant.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeadOptionality {
    /// The variable declared optional.
    pub variable: String,
    /// Display form of the premise that declared the optionality.
    pub declared_in: String,
    /// Display form of the premise that pinned the variable
    /// present, when one narrowing step is identifiable.
    pub required_by: Option<String>,
}

/// Inferred types for every named variable referenced by a rule's
/// positive premises.
///
/// Built by [`TypeEnv::infer`] during planning. The planner uses
/// the result to narrow each premise's variable terms so they
/// carry rule-level kinds at evaluation time. Also carried on
/// [`AnalyzedRule`](super::AnalyzedRule) (wrapped in an `Arc`) for
/// later phases that want type-by-variable lookups.
///
/// Alongside the final kinds, inference records its *provenance*:
/// every step where a premise changed a variable's kind
/// ([`Self::narrowings`]) and every declared optionality no result
/// can exercise ([`Self::dead_optionality`]). [`Self::explain`]
/// renders both as a human-readable account.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TypeEnv {
    by_name: HashMap<String, Kind>,
    narrowings: Vec<Narrowing>,
    dead_optionality: Vec<DeadOptionality>,
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
        let mut narrowings: Vec<Narrowing> = Vec::new();
        // Running per-variable kinds, diffed after each premise to
        // attribute narrowing steps to the premise that caused them.
        let mut current: HashMap<String, Kind> = HashMap::new();
        // The first premise to declare each variable optional (a
        // slot whose content type admits `Nothing`) — the lint's
        // evidence of *declared* optionality.
        let mut widened: Vec<(String, String)> = Vec::new();

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
                Self::record_narrowings(&ctx, premise, &mut current, &mut narrowings);
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
                if slot_kind.is_optional() && !widened.iter().any(|(name, _)| name == var_name) {
                    widened.push((var_name.to_string(), premise.to_string()));
                }
                let var = ctx.var_for_name(var_name);
                if let Err(reason) = ctx.unify(&lift(&slot_kind), &Inferred::Variable(var)) {
                    return Err(InferenceError::Conflict {
                        variable: var_name.to_string(),
                        reason: reason.to_string(),
                    });
                }
            }

            Self::record_narrowings(&ctx, premise, &mut current, &mut narrowings);
        }

        let mut by_name = HashMap::new();
        for (name, var_id) in ctx.named_vars() {
            by_name.insert(name.clone(), Self::resolve(&ctx, *var_id));
        }

        // Dead optionality: declared optional, inferred required.
        // The premise to blame is the first one whose contribution
        // left the variable without the `Nothing` atom.
        let mut dead_optionality = Vec::new();
        for (variable, declared_in) in widened {
            let Some(kind) = by_name.get(&variable) else {
                continue;
            };
            if kind.is_optional() {
                continue;
            }
            let required_by = narrowings
                .iter()
                .find(|step| step.variable == variable && !step.to.is_optional())
                .map(|step| step.premise.clone());
            dead_optionality.push(DeadOptionality {
                variable,
                declared_in,
                required_by,
            });
        }

        Ok(Self {
            by_name,
            narrowings,
            dead_optionality,
        })
    }

    /// Resolve a unifier variable to its current best kind: the
    /// static it resolved to, or its primitive constraint when no
    /// slot ever gave it a concrete shape.
    fn resolve(ctx: &Context, var: VarId) -> Kind {
        match ctx.apply(&Inferred::Variable(var)) {
            Inferred::Static(kind) => kind,
            _ => Kind::primitive_set(ctx.constraint(var)),
        }
    }

    /// Diff every named variable's kind against the running
    /// snapshot and attribute each change to `premise`. Names are
    /// visited in sorted order so the recorded steps are
    /// deterministic.
    fn record_narrowings(
        ctx: &Context,
        premise: &Premise,
        current: &mut HashMap<String, Kind>,
        narrowings: &mut Vec<Narrowing>,
    ) {
        let mut names: Vec<(&String, &VarId)> = ctx.named_vars().collect();
        names.sort_by_key(|(name, _)| name.as_str());
        for (name, var_id) in names {
            let kind = Self::resolve(ctx, *var_id);
            let previous = current.get(name);
            if previous == Some(&kind) {
                continue;
            }
            narrowings.push(Narrowing {
                variable: name.clone(),
                premise: premise.to_string(),
                from: previous.cloned(),
                to: kind.clone(),
            });
            current.insert(name.clone(), kind);
        }
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

    /// The narrowing steps recorded during inference, in premise
    /// order: which premise constrained which variable, from what
    /// to what.
    pub fn narrowings(&self) -> &[Narrowing] {
        &self.narrowings
    }

    /// Declared optionality no result can exercise: variables some
    /// premise set-widened with `Nothing` that the rule's other
    /// premises require present. Sound, but worth surfacing — the
    /// author wrote an optional that always filters.
    pub fn dead_optionality(&self) -> &[DeadOptionality] {
        &self.dead_optionality
    }

    /// Render a human-readable account of the inference: every
    /// variable's final kind, the premises that narrowed it there,
    /// and any dead-optionality findings.
    ///
    /// ```text
    /// ?age: UnsignedInt
    ///   introduced as UnsignedInt|Nothing by person { age?: ?age }
    ///   narrowed to UnsignedInt by 5 = sum(?age, 1)
    /// warning: ?age is declared optional by person { age?: ?age }
    ///   but required by 5 = sum(?age, 1); rows lacking it are
    ///   excluded, no result carries it as Absent
    /// ```
    pub fn explain(&self) -> String {
        let mut lines = Vec::new();
        let mut names: Vec<&String> = self.by_name.keys().collect();
        names.sort();
        for name in names {
            lines.push(format!("?{name}: {}", self.by_name[name]));
            for step in self.narrowings.iter().filter(|step| &step.variable == name) {
                match &step.from {
                    None => lines.push(format!("  introduced as {} by {}", step.to, step.premise)),
                    Some(from) => lines.push(format!(
                        "  narrowed from {} to {} by {}",
                        from, step.to, step.premise
                    )),
                }
            }
        }
        for finding in &self.dead_optionality {
            let mut line = format!(
                "warning: ?{} is declared optional by {} but can never be Absent",
                finding.variable, finding.declared_in
            );
            if let Some(required_by) = &finding.required_by {
                line.push_str(&format!(
                    " (required by {}; rows lacking it are excluded)",
                    required_by
                ));
            }
            lines.push(line);
        }
        lines.join("\n")
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
    use std::slice;

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

    /// Inference records its provenance: each premise's
    /// contribution to a variable's kind becomes a narrowing step
    /// naming the premise.
    #[dialog_common::test]
    fn it_records_narrowing_provenance() -> anyhow::Result<()> {
        let scan = AttributeQuery::new(
            Term::from(the!("misc/tag")),
            Term::<Entity>::var("this"),
            Term::var("tag"),
            Term::var("cause"),
            Some(Cardinality::One),
        );
        let predicate = Term::<Any>::var("tag").number();
        let premises = vec![scan.into(), predicate.clone()];

        let env = TypeEnv::infer(&premises)?;
        let steps: Vec<&Narrowing> = env
            .narrowings()
            .iter()
            .filter(|step| step.variable == "tag")
            .collect();
        assert_eq!(
            steps.len(),
            2,
            "introduced by the scan, narrowed by the predicate"
        );
        assert_eq!(steps[0].from, None, "first contribution has no prior kind");
        assert_eq!(
            steps[1].to.primitive_part(),
            Primitive::NUMERIC,
            "the predicate's step records the narrowed kind"
        );
        assert_eq!(
            steps[1].premise,
            predicate.to_string(),
            "the step names the premise that narrowed"
        );
        assert!(
            env.explain().contains("?tag:"),
            "explain renders the variable's account"
        );
        Ok(())
    }

    /// A declared optionality the rule's other premises make
    /// unreachable is reported: the variable can never be Absent in
    /// any result.
    #[dialog_common::test]
    fn it_lints_dead_optionality() -> anyhow::Result<()> {
        let optional = optional_name_premise(Term::from(the!("person/nickname")));
        let scan: Premise = AttributeQuery::new(
            Term::from(the!("person/name")),
            Term::<Entity>::var("this"),
            Term::<String>::var("name").into(),
            Term::var("cause"),
            Some(Cardinality::One),
        )
        .into();

        // Alone, the optionality is live: no lint.
        let env = TypeEnv::infer(slice::from_ref(&optional))?;
        assert!(
            env.dead_optionality().is_empty(),
            "an optional nothing requires is live"
        );

        // A sibling scan requires ?name present: the optionality is
        // dead and the lint names both premises.
        let env = TypeEnv::infer(&[optional.clone(), scan.clone()])?;
        let findings = env.dead_optionality();
        assert_eq!(findings.len(), 1);
        assert_eq!(findings[0].variable, "name");
        assert_eq!(findings[0].declared_in, optional.to_string());
        assert_eq!(
            findings[0].required_by.as_deref(),
            Some(scan.to_string().as_str())
        );
        assert!(
            env.explain().contains("warning:"),
            "explain surfaces the finding"
        );
        Ok(())
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

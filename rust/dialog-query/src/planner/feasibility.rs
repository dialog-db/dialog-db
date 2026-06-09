use crate::schema::Requirement;
use crate::{Environment, Parameters, Premise, Schema};
use std::collections::{BTreeSet, HashSet};

/// Why a premise cannot run yet under the current bindings — the
/// `Err` case of the SIPS binding function [`feasible`]. Names which
/// variables the premise is still waiting on, so the planner (and
/// later demand reification) knows what would unblock it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Infeasible {
    /// All of these still-unbound variables must be bound before the
    /// premise can run. A choice group already satisfied (by a
    /// constant or a bound variable) contributes nothing here.
    NeedsAll(BTreeSet<String>),
}

/// Categorize a premise's parameters against a set of already-bound
/// variables: which it will bind, and which it still requires.
///
/// This is the single definition of feasibility, shared by the planner
/// (via [`feasible`]) and the per-step SIPS function
/// ([`Plan::adorn`](super::Plan::adorn)). For each non-constant,
/// non-blank, not-yet-bound slot: a `Required(None)` slot is required;
/// a `Required(Some(group))` slot is bound if its choice group is
/// satisfied (by a constant or an already-bound member) and required
/// otherwise; an `Optional` slot is bound. A negation never binds —
/// its slots are requirements only.
pub(crate) fn categorize(
    schema: &Schema,
    params: &Parameters,
    is_negation: bool,
    bound: &Environment,
) -> (Environment, Environment) {
    // A choice group is satisfied if any member is a constant or is
    // already bound.
    let mut satisfied_groups = HashSet::new();
    for (name, field) in schema.iter() {
        if let Some(param) = params.get(name)
            && let Requirement::Required(Some(group)) = &field.requirement
            && (param.is_constant() || param.is_bound(bound))
        {
            satisfied_groups.insert(*group);
        }
    }

    let mut binds = Environment::new();
    let mut requires = Environment::new();
    for (name, field) in schema.iter() {
        let Some(param) = params.get(name) else {
            continue;
        };
        if param.is_constant() || param.is_blank() || param.is_bound(bound) {
            continue;
        }
        match &field.requirement {
            Requirement::Required(Some(group)) => {
                if satisfied_groups.contains(group) {
                    if !is_negation {
                        param.bind(&mut binds);
                    }
                } else {
                    param.bind(&mut requires);
                }
            }
            Requirement::Required(None) => {
                param.bind(&mut requires);
            }
            Requirement::Optional => {
                if !is_negation {
                    param.bind(&mut binds);
                }
            }
        }
    }

    (binds, requires)
}

/// Feasibility verdict for a premise under the given set of
/// already-bound variables.
///
/// `Ok(binds)` — the variables the premise will bind — when every
/// prerequisite is satisfied, or `Err(Infeasible)` naming the
/// variables it still requires. This is the premise-level entry the
/// planner orders by; [`Plan::adorn`](super::Plan::adorn) is the same
/// computation over a lowered step.
pub(crate) fn feasible(premise: &Premise, bound: &Environment) -> Result<Environment, Infeasible> {
    let is_negation = matches!(premise, Premise::Unless(_));
    let (binds, requires) =
        categorize(&premise.schema(), &premise.parameters(), is_negation, bound);
    if requires.is_empty() {
        Ok(binds)
    } else {
        Err(Infeasible::NeedsAll(
            requires.iter().map(String::from).collect(),
        ))
    }
}

#[cfg(test)]
mod tests {
    #[cfg(target_arch = "wasm32")]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    use super::super::Planner;
    use super::*;
    use crate::artifact::Entity;
    use crate::attribute::query::AttributeQuery;
    use crate::attribute::{Cardinality, The};
    use crate::formula::Formula;
    use crate::formula::string::Length;
    use crate::proposition::Proposition;
    use crate::schema::{INDEX_SCAN_COST, LOOKUP_COST, RANGE_SCAN_COST, VERIFICATION_COST};
    use crate::the;
    use crate::types::Any;
    use crate::{Parameters, Term};
    use std::collections::BTreeSet;

    fn attribute(value: Term<Any>, cardinality: Cardinality) -> Premise {
        Premise::Assert(Proposition::Attribute(Box::new(AttributeQuery::new(
            Term::from(the!("user/name")),
            Term::<Entity>::var("entity"),
            value,
            Term::var("cause"),
            Some(cardinality),
        ))))
    }

    fn scope(vars: &[&str]) -> Environment {
        let mut env = Environment::new();
        for v in vars {
            env.add(*v);
        }
        env
    }

    /// An attribute's slots share one choice group: with a constant
    /// `the` the group is satisfied, so every variable slot binds and
    /// nothing is required — feasible at the empty scope.
    #[dialog_common::test]
    fn attribute_with_constant_member_is_feasible() {
        let premise = attribute(Term::var("value"), Cardinality::One);
        let binds =
            feasible(&premise, &Environment::new()).expect("constant `the` satisfies group");
        assert!(binds.contains("value"));
        assert!(binds.contains("entity"));
        assert!(binds.contains("cause"));
    }

    /// A formula's required input must be bound before it is feasible;
    /// once bound, it is feasible and binds its output.
    #[dialog_common::test]
    fn formula_requires_input_then_binds_output() {
        let mut params = Parameters::new();
        params.insert("of".to_string(), Term::var("text"));
        params.insert("is".to_string(), Term::var("len"));
        let premise = Premise::from(Length::apply(params).unwrap());

        match feasible(&premise, &Environment::new()) {
            Err(Infeasible::NeedsAll(needs)) => assert!(needs.contains("text")),
            other => panic!("expected NeedsAll(text), got {:?}", other),
        }
        let binds = feasible(&premise, &scope(&["text"])).expect("input bound → feasible");
        assert!(binds.contains("len"));
    }

    /// A negation requires its variables bound but never binds —
    /// feasible once bound, with an empty bind set.
    #[dialog_common::test]
    fn negation_requires_but_never_binds() {
        let x = Term::<String>::var("x");
        let y = Term::<String>::var("y");
        let premise = !x.is(y);

        assert!(feasible(&premise, &Environment::new()).is_err());
        let binds = feasible(&premise, &scope(&["x", "y"])).expect("both bound → feasible");
        assert_eq!(binds.iter().count(), 0, "negation binds nothing");
    }

    /// Cost (via `estimate`) reflects how constrained the lookup is:
    /// all-constant is a point LOOKUP; one constant + variables is a
    /// scan; cardinality Many costs at least as much as One.
    #[dialog_common::test]
    fn estimate_reflects_binding_and_cardinality() {
        let all_const = Premise::Assert(Proposition::Attribute(Box::new(AttributeQuery::new(
            Term::from(the!("user/name")),
            Term::from(Entity::new().unwrap()),
            Term::constant("x".to_string()),
            Term::var("cause"),
            Some(Cardinality::One),
        ))));
        assert_eq!(
            all_const.estimate(&Environment::new()),
            Some(LOOKUP_COST),
            "all constants → point lookup"
        );

        let one = attribute(Term::var("value"), Cardinality::One);
        assert_eq!(
            one.estimate(&Environment::new()),
            Some(RANGE_SCAN_COST + VERIFICATION_COST),
            "constant `the` only → range scan + verification"
        );

        let many = attribute(Term::var("value"), Cardinality::Many);
        assert!(
            many.estimate(&Environment::new()).unwrap()
                >= one.estimate(&Environment::new()).unwrap(),
            "cardinality Many is at least as costly as One"
        );

        // Binding the entity narrows the cost to a lookup.
        assert_eq!(
            one.estimate(&scope(&["entity"])),
            Some(LOOKUP_COST),
            "entity bound → point lookup"
        );
    }

    /// A formula with no IO is cheaper than an attribute scan that
    /// binds the same variable.
    #[dialog_common::test]
    fn formula_is_cheaper_than_scan() {
        let mut params = Parameters::new();
        params.insert("of".to_string(), Term::constant("hello".to_string()));
        params.insert("is".to_string(), Term::var("len"));
        let formula = Premise::from(Length::apply(params).unwrap());

        let scan = attribute(Term::var("value"), Cardinality::Many);
        assert!(
            formula.estimate(&Environment::new()).unwrap()
                < scan.estimate(&Environment::new()).unwrap(),
            "a no-IO formula is cheaper than an index scan"
        );
        assert!(scan.estimate(&Environment::new()).unwrap() >= INDEX_SCAN_COST);
    }

    /// `feasible` is consistent with the planner's own output: for
    /// each planned step, asking feasibility with the variables bound
    /// when that step runs (`step.env()`) is `Ok` and reports exactly
    /// the step's `binds()`. Pins the SIPS binding function to the
    /// plan the planner emits.
    #[dialog_common::test]
    fn feasible_matches_planned_binds() {
        let premises = vec![
            attribute_for(the!("person/name"), "name"),
            attribute_for(the!("person/age"), "age"),
        ];
        let plan = Planner::from(premises).plan(&Environment::new()).unwrap();

        for step in &plan.steps {
            let binds =
                feasible(&step.as_premise(), step.env()).expect("planned step is feasible at env");
            let got: BTreeSet<String> = binds.iter().map(String::from).collect();
            let expected: BTreeSet<String> = step.binds().iter().map(String::from).collect();
            assert_eq!(got, expected, "feasible binds match the planner's binds");
        }
    }

    fn attribute_for(the: The, value: &str) -> Premise {
        Premise::Assert(Proposition::Attribute(Box::new(AttributeQuery::new(
            Term::from(the),
            Term::<Entity>::var("this"),
            Term::var(value),
            Term::var("cause"),
            Some(Cardinality::One),
        ))))
    }
}

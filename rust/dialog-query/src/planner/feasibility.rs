use crate::error::Infeasible;
use crate::pattern::Pattern;
use crate::schema::Requirement;
use crate::types::Any;
use crate::{Environment, Parameters, Premise, Schema, Term};
use std::collections::HashSet;

/// Categorize a premise's parameters against a set of already-bound
/// variables: which it will bind, and which it still requires.
///
/// This is the single definition of feasibility: the SIPS binding
/// function, shared by the planner (via [`feasible`]) and the rule
/// analyzer's dependency graph. For each non-constant,
/// non-blank, not-yet-bound slot: a `Required(None)` slot is required;
/// a `Required(Some(group))` slot is bound if its choice group is
/// satisfied (by a constant or an already-bound member) and required
/// otherwise; an `Optional` slot is bound. A negation never binds,
/// its slots are requirements only.
///
/// A slot that holds a map, a list or a tuple of terms is judged by the
/// variables inside it: it is bound when every one of them is, a slot
/// the premise binds binds each of them that is not, and a slot the
/// premise requires requires each of them that is not. A blank inside
/// is neither bound nor required, as a blank slot is.
pub(crate) fn categorize(
    schema: &Schema,
    params: &Parameters,
    is_negation: bool,
    environment: &Environment,
) -> (Environment, Environment) {
    // A choice group is satisfied if any member is a constant or is
    // already bound.
    let mut satisfied_groups = HashSet::new();
    for (name, field) in schema.iter() {
        if let Requirement::Required(Some(group)) = &field.requirement
            && let Some(bound) = Slot::of(params, name).map(|slot| slot.is_bound(environment))
            && bound
        {
            satisfied_groups.insert(*group);
        }
    }

    let mut binds = Environment::new();
    let mut requires = Environment::new();
    for (name, field) in schema.iter() {
        // If parameter is not set treat it as blank and move one
        let Some(slot) = Slot::of(params, name) else {
            continue;
        };
        if slot.is_blank() || slot.is_bound(environment) {
            continue;
        }
        match &field.requirement {
            Requirement::Required(Some(group)) => {
                // If paramater is already satisfied by a group add it to
                // binds otherwise put it into requirements.
                if satisfied_groups.contains(group) {
                    if !is_negation {
                        slot.bind(environment, &mut binds);
                    }
                } else {
                    slot.bind(environment, &mut requires);
                }
            }
            Requirement::Required(None) => {
                slot.bind(environment, &mut requires);
            }
            Requirement::Optional => {
                if !is_negation {
                    slot.bind(environment, &mut binds);
                }
            }
        }
    }

    (binds, requires)
}

/// What a parameter holds, as feasibility reads it: a term, or a map,
/// a list or a tuple of terms judged by the variables inside it.
enum Slot<'a> {
    Term(&'a Term<Any>),
    Shape(&'a Pattern),
}

impl<'a> Slot<'a> {
    fn of(params: &'a Parameters, name: &str) -> Option<Self> {
        params.slot(name).map(|(term, shape)| match shape {
            Some(pattern) => Slot::Shape(pattern),
            None => Slot::Term(term),
        })
    }

    fn is_blank(&self) -> bool {
        match self {
            Slot::Term(term) => term.is_blank(),
            Slot::Shape(_) => false,
        }
    }

    /// A constant, a bound variable, or a shape every variable of which
    /// is bound.
    fn is_bound(&self, environment: &Environment) -> bool {
        match self {
            Slot::Term(term) => term.is_constant() || term.is_bound(environment),
            Slot::Shape(pattern) => pattern.is_bound(environment),
        }
    }

    /// Add the variables this slot holds that `environment` does not
    /// bind yet to `into`.
    fn bind(&self, environment: &Environment, into: &mut Environment) {
        match self {
            Slot::Term(term) => term.bind(into),
            Slot::Shape(pattern) => {
                for term in pattern.terms() {
                    if !term.is_bound(environment) {
                        term.bind(into);
                    }
                }
            }
        }
    }
}

impl Premise {
    /// Feasibility verdict for this premise under the given set of
    /// already-bound variables.
    ///
    /// `Ok(binds)` (the variables the premise will bind) when every
    /// prerequisite is satisfied, or `Err(Infeasible)` naming the
    /// variables it still requires. This is the premise-level entry
    /// the planner orders by. The body lives here, next to
    /// [`categorize`], because it is the planner's notion of
    /// feasibility rather than general premise behavior.
    pub(crate) fn feasible(&self, environment: &Environment) -> Result<Environment, Infeasible> {
        let is_negation = matches!(self, Premise::Unless(_));
        let (binds, requires) =
            categorize(&self.schema(), &self.parameters(), is_negation, environment);
        if requires.is_empty() {
            Ok(binds)
        } else {
            Err(Infeasible::NeedsAll(
                requires.iter().map(String::from).collect(),
            ))
        }
    }
}

#[cfg(test)]
mod tests {
    #[cfg(target_arch = "wasm32")]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    use super::super::Planner;
    use super::*;
    use crate::attribute::query::AttributeQuery;
    use crate::attribute::{Cardinality, Relation};
    use crate::corpus::Entity;
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
    /// nothing is required: feasible at the empty scope.
    #[dialog_common::test]
    fn attribute_with_constant_member_is_feasible() {
        let premise = attribute(Term::var("value"), Cardinality::One);
        let binds = premise
            .feasible(&Environment::new())
            .expect("constant `the` satisfies group");
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

        match premise.feasible(&Environment::new()) {
            Err(Infeasible::NeedsAll(needs)) => assert!(needs.contains("text")),
            other => panic!("expected NeedsAll(text), got {:?}", other),
        }
        let binds = premise
            .feasible(&scope(&["text"]))
            .expect("input bound → feasible");
        assert!(binds.contains("len"));
    }

    /// A negation requires its variables bound but never binds,
    /// feasible once bound, with an empty bind set.
    #[dialog_common::test]
    fn negation_requires_but_never_binds() {
        let x = Term::<String>::var("x");
        let y = Term::<String>::var("y");
        let premise = !x.is(y);

        assert!(premise.feasible(&Environment::new()).is_err());
        let binds = premise
            .feasible(&scope(&["x", "y"]))
            .expect("both bound → feasible");
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
            let binds = step
                .as_premise()
                .feasible(step.env())
                .expect("planned step is feasible at env");
            let got: BTreeSet<String> = binds.iter().map(String::from).collect();
            let expected: BTreeSet<String> = step.binds().iter().map(String::from).collect();
            assert_eq!(got, expected, "feasible binds match the planner's binds");
        }
    }

    /// A slot that holds a map of terms is judged by the variables
    /// inside it. An equality between a variable and such a map is
    /// feasible once every variable inside the map is bound, and then
    /// binds the variable; or once the variable is bound, and then
    /// binds what inside the map is not. With neither it names what it
    /// still needs, and a variable inside the map that is bound already
    /// is not among them.
    #[dialog_common::test]
    fn a_pattern_is_feasible_by_the_variables_inside_it() {
        let pattern = Pattern::map([
            (
                "title".to_string(),
                Pattern::from(Term::<Any>::var("title")),
            ),
            ("body".to_string(), Pattern::from(Term::<Any>::var("body"))),
        ])
        .expect("a pattern");
        let premise = pattern.is(Term::<Any>::var("card"));

        match premise.feasible(&scope(&["title"])) {
            Err(Infeasible::NeedsAll(needs)) => {
                assert!(needs.contains("body"));
                assert!(needs.contains("card"));
                assert!(!needs.contains("title"), "the title is bound");
            }
            other => panic!("expected NeedsAll, got {other:?}"),
        }

        let binds = premise
            .feasible(&scope(&["title", "body"]))
            .expect("the map is a value");
        let bound: BTreeSet<&str> = binds.iter().collect();
        assert_eq!(bound, BTreeSet::from(["card"]));

        let binds = premise
            .feasible(&scope(&["card", "title"]))
            .expect("the value is bound");
        let bound: BTreeSet<&str> = binds.iter().collect();
        assert_eq!(bound, BTreeSet::from(["body"]), "what is not bound yet");
    }

    /// A negated premise binds nothing and needs every variable inside
    /// its pattern bound. A blank inside it is not a variable to bind.
    #[dialog_common::test]
    fn a_negated_pattern_requires_its_variables_and_not_its_blanks() {
        let Pattern::Map(named) = Pattern::map([(
            "title".to_string(),
            Pattern::from(Term::<Any>::var("title")),
        )])
        .expect("a pattern") else {
            panic!("a map pattern");
        };
        let premise = !Pattern::Map(named.with_rest(Term::blank())).is(Term::<Any>::var("card"));

        match premise.feasible(&scope(&["card"])) {
            Err(Infeasible::NeedsAll(needs)) => {
                assert_eq!(needs.len(), 1);
                assert!(needs.contains("title"));
            }
            other => panic!("expected NeedsAll, got {other:?}"),
        }
        let binds = premise
            .feasible(&scope(&["card", "title"]))
            .expect("everything named is bound");
        assert_eq!(binds.iter().count(), 0, "negation binds nothing");
    }

    fn attribute_for(the: Relation, value: &str) -> Premise {
        Premise::Assert(Proposition::Attribute(Box::new(AttributeQuery::new(
            Term::from(the),
            Term::<Entity>::var("this"),
            Term::var(value),
            Term::var("cause"),
            Some(Cardinality::One),
        ))))
    }
}

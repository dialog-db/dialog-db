use super::{Header, Plan};
use crate::error::TypeError;
use crate::{Environment, Parameters, Premise, Requirement, Schema};
use std::collections::HashSet;

/// A premise under consideration by the query planner, tracking whether it
/// can execute given the current variable bindings.
///
/// The planner examines each premise to determine whether it can execute
/// given the current set of bound variables. A `Candidate` captures this
/// determination along with cached schema and parameter data so it can be
/// re-evaluated against a new scope without re-deriving them.
///
/// A `Candidate` starts in either `Viable` (all required variables are bound)
/// or `Blocked` (some variables are still missing). As the planner selects
/// premises and their bindings flow into the environment, blocked candidates
/// are updated via [`Candidate::update`] and may transition to `Viable`.
///
/// Once planning is complete, viable candidates are converted into [`Plan`]
/// values (which drop the cached schema/params) for execution.
#[derive(Debug, Clone, PartialEq)]
pub enum Candidate {
    /// All prerequisites are satisfied — this premise can execute now.
    Viable {
        /// The premise to execute.
        premise: Premise,
        /// Estimated execution cost.
        cost: usize,
        /// Variables this premise will bind.
        binds: Environment,
        /// Variables already bound in the environment.
        env: Environment,
        /// Cached schema, reused when re-evaluating against a new scope.
        schema: Schema,
        /// Cached parameters, reused when re-evaluating against a new scope.
        params: Parameters,
    },
    /// One or more prerequisites are unsatisfied — this premise cannot
    /// execute until the missing variables are bound by earlier premises.
    Blocked {
        /// The premise that cannot yet execute.
        premise: Premise,
        /// Estimated execution cost once unblocked.
        cost: usize,
        /// Variables this premise will bind once it can execute.
        binds: Environment,
        /// Variables already bound in the environment.
        env: Environment,
        /// Variables that must be bound before this premise can execute.
        requires: Environment,
        /// Cached schema, reused when re-evaluating against a new scope.
        schema: Schema,
        /// Cached parameters, reused when re-evaluating against a new scope.
        params: Parameters,
    },
}

/// Categorize a premise's parameters against a set of already-bound
/// variables: which it will bind, and which it still requires.
///
/// This is the single definition of feasibility shared by the planner
/// (`Candidate`) and the per-step SIPS function ([`Plan::adorn`]). For
/// each non-constant, non-blank, not-yet-bound slot: a `Required(None)`
/// slot is required; a `Required(Some(group))` slot is bound if its
/// choice group is satisfied (by a constant or an already-bound
/// member) and required otherwise; an `Optional` slot is bound. A
/// negation never binds — its slots are requirements only.
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

impl Candidate {
    /// Analyzes a premise to determine whether it is viable or blocked,
    /// and computes its estimated cost in an empty environment.
    pub fn from(premise: Premise) -> Self {
        let schema = premise.schema();
        let params = premise.parameters();
        Self::build(premise, schema, params, &Environment::new())
    }

    /// Build a candidate for a premise at the given scope. Shared by
    /// [`from`](Self::from) (empty scope) and [`update`](Self::update)
    /// (a new scope): feasibility comes from [`categorize`], cost from
    /// the premise's estimate over the scope-bound variables.
    fn build(premise: Premise, schema: Schema, params: Parameters, scope: &Environment) -> Self {
        let is_negation = matches!(premise, Premise::Unless(_));

        // `env` is the subset of the scope this premise actually uses.
        let mut env = Environment::new();
        for (name, _) in schema.iter() {
            if let Some(param) = params.get(name)
                && let Some(var) = param.name()
                && scope.contains(var)
            {
                env.add(var);
            }
        }

        let (binds, requires) = categorize(&schema, &params, is_negation, scope);
        let cost = premise.estimate(&env).unwrap_or(usize::MAX);

        if requires.is_empty() {
            Candidate::Viable {
                premise,
                cost,
                binds,
                env,
                schema,
                params,
            }
        } else {
            Candidate::Blocked {
                premise,
                cost,
                binds,
                env,
                requires,
                schema,
                params,
            }
        }
    }

    /// Synchronize this candidate with the given target scope.
    ///
    /// Recomputes feasibility, binds, and cost for the new scope from
    /// scratch (via [`categorize`]) — there is no incremental delta to
    /// thread, because the planner always calls `update` with the full
    /// cumulative scope. May transition between Blocked and Viable.
    pub fn update(&mut self, target_scope: &Environment) {
        let (premise, schema, params) = match self {
            Candidate::Viable {
                premise,
                schema,
                params,
                ..
            }
            | Candidate::Blocked {
                premise,
                schema,
                params,
                ..
            } => (premise.clone(), schema.clone(), params.clone()),
        };
        *self = Self::build(premise, schema, params, target_scope);
    }

    /// Get the cost of this candidate (whether viable or blocked)
    pub fn cost(&self) -> usize {
        match self {
            Candidate::Viable { cost, .. } => *cost,
            Candidate::Blocked { cost, .. } => *cost,
        }
    }

    /// Check if this candidate is viable
    pub fn is_viable(&self) -> bool {
        matches!(self, Candidate::Viable { .. })
    }

    /// Get the premise this candidate is for
    pub fn premise(&self) -> &Premise {
        match self {
            Candidate::Viable { premise, .. } => premise,
            Candidate::Blocked { premise, .. } => premise,
        }
    }
}

impl From<Premise> for Candidate {
    fn from(premise: Premise) -> Self {
        Candidate::from(premise)
    }
}

impl TryFrom<Candidate> for Plan {
    type Error = TypeError;

    fn try_from(candidate: Candidate) -> Result<Self, Self::Error> {
        match candidate {
            Candidate::Viable {
                premise,
                cost,
                binds,
                env,
                ..
            } => {
                // Drop schema/params — they're only needed during
                // planning, not at evaluation time. Lower the
                // premise into its compiled `Plan` variant.
                Ok(Plan::lower(premise, Header { cost, binds, env }))
            }
            Candidate::Blocked { requires, .. } => {
                Err(TypeError::RequiredBindings { required: requires })
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::formula::Formula;
    use crate::formula::string::Length;

    use crate::{Environment, Parameters, Premise, Term};

    #[dialog_common::test]
    fn it_creates_candidate_all_derived() {
        let mut params = Parameters::new();
        params.insert("of".to_string(), Term::var("text"));
        params.insert("is".to_string(), Term::var("len"));

        let application = Length::apply(params).unwrap();
        let premise = Premise::from(application);

        let candidate = Candidate::from(premise);
        assert!(!candidate.is_viable());
    }

    #[dialog_common::test]
    fn it_creates_candidate_with_constant() {
        let mut params = Parameters::new();
        params.insert("of".to_string(), Term::constant("hello".to_string()));
        params.insert("is".to_string(), Term::var("len"));

        let application = Length::apply(params).unwrap();
        let premise = Premise::from(application);

        let candidate = Candidate::from(premise);
        assert!(candidate.is_viable());
    }

    #[dialog_common::test]
    fn it_transitions_to_viable_on_update() {
        let mut params = Parameters::new();
        params.insert("of".to_string(), Term::var("text"));
        params.insert("is".to_string(), Term::var("len"));

        let application = Length::apply(params).unwrap();
        let premise = Premise::from(application);

        let mut candidate = Candidate::from(premise);
        assert!(!candidate.is_viable());

        let mut env = Environment::new();
        env.add("text");
        candidate.update(&env);

        assert!(candidate.is_viable());
    }

    #[dialog_common::test]
    fn it_reduces_cost_when_derived_bound() {
        let mut params = Parameters::new();
        params.insert("of".to_string(), Term::constant("hello".to_string()));
        params.insert("is".to_string(), Term::var("len"));

        let application = Length::apply(params).unwrap();
        let premise = Premise::from(application);

        let mut candidate = Candidate::from(premise);
        let initial_cost = candidate.cost();
        assert!(candidate.is_viable());

        let mut env = Environment::new();
        env.add("len");
        candidate.update(&env);

        assert_eq!(
            candidate.cost(),
            initial_cost,
            "Formula cost should remain constant regardless of bound variables"
        );
    }

    #[dialog_common::test]
    fn it_converts_viable_candidate_to_plan() {
        let mut params = Parameters::new();
        params.insert("of".to_string(), Term::constant("hello".to_string()));
        params.insert("is".to_string(), Term::var("len"));

        let application = Length::apply(params).unwrap();
        let premise = Premise::from(application);

        let candidate = Candidate::from(premise);
        assert!(candidate.is_viable());

        let plan = Plan::try_from(candidate);
        assert!(plan.is_ok());
    }

    #[dialog_common::test]
    fn it_rejects_blocked_candidate_conversion() {
        let mut params = Parameters::new();
        params.insert("of".to_string(), Term::var("text"));
        params.insert("is".to_string(), Term::var("len"));

        let application = Length::apply(params).unwrap();
        let premise = Premise::from(application);

        let candidate = Candidate::from(premise);
        assert!(!candidate.is_viable());

        let plan = Plan::try_from(candidate);
        assert!(plan.is_err());
    }

    #[dialog_common::test]
    fn it_blocks_negated_constraint_with_unbound_variables() {
        let x = Term::<String>::var("x");
        let y = Term::<String>::var("y");
        let premise = !x.clone().is(y);
        let candidate = Candidate::from(premise);

        assert!(
            !candidate.is_viable(),
            "Negated constraint with unbound variables should be blocked"
        );
    }

    #[dialog_common::test]
    fn it_unblocks_negated_constraint_when_variables_bound() {
        let x = Term::<String>::var("x");
        let y = Term::<String>::var("y");
        let premise = !x.clone().is(y.clone());
        let mut candidate = Candidate::from(premise);

        assert!(!candidate.is_viable());

        let mut env = Environment::new();
        x.bind(&mut env);
        y.bind(&mut env);
        candidate.update(&env);

        assert!(
            candidate.is_viable(),
            "Negated constraint should become viable when all variables are bound"
        );
    }
}

#[cfg(test)]
mod cost_model_tests {
    use super::Candidate;
    use crate::artifact::Entity;
    use crate::attribute::Cardinality;
    use crate::attribute::query::AttributeQuery;
    use crate::concept::descriptor::ConceptDescriptor;
    use crate::concept::query::ConceptQuery;
    use crate::formula::Formula;
    use crate::formula::string::Length;
    use crate::proposition::Proposition;
    use crate::schema::{
        CONCEPT_OVERHEAD, INDEX_SCAN_COST, LOOKUP_COST, RANGE_READ_COST, RANGE_SCAN_COST,
        VERIFICATION_COST,
    };
    use crate::the;

    use crate::{AttributeDescriptor, Environment, Parameters, Premise, Term, Type, Value};

    #[dialog_common::test]
    fn it_excludes_constants_from_cost() {
        let entity_val: Entity = Entity::new().unwrap();

        let app = AttributeQuery::new(
            Term::from(the!("user/name")),
            Term::from(entity_val),
            Term::constant("test".to_string()),
            Term::var("cause"),
            Some(Cardinality::One),
        );
        let premise = Premise::Assert(Proposition::Attribute(Box::new(app)));
        let candidate = Candidate::from(premise);

        assert_eq!(
            candidate.cost(),
            LOOKUP_COST,
            "All constants should only cost LOOKUP_COST ({}), got {}",
            LOOKUP_COST,
            candidate.cost()
        );
    }

    #[dialog_common::test]
    fn it_costs_one_constant_two_variables() {
        let query = AttributeQuery::new(
            Term::from(the!("user/name")),
            Term::<Entity>::var("entity"),
            Term::var("value"),
            Term::var("cause"),
            Some(Cardinality::One),
        );
        let premise = Premise::Assert(Proposition::Attribute(Box::new(query)));
        let candidate = Candidate::from(premise);

        assert_eq!(
            candidate.cost(),
            RANGE_SCAN_COST + VERIFICATION_COST,
            "With 1 constraint (just constant 'the'), cost should be RANGE_SCAN_COST + VERIFICATION_COST ({}), got {}",
            RANGE_SCAN_COST + VERIFICATION_COST,
            candidate.cost()
        );
    }

    #[dialog_common::test]
    fn it_reduces_cost_for_env_variables() {
        let query = AttributeQuery::new(
            Term::from(the!("user/name")),
            Term::<Entity>::var("entity"),
            Term::var("value"),
            Term::var("cause"),
            Some(Cardinality::One),
        );
        let premise = Premise::Assert(Proposition::Attribute(Box::new(query)));

        let mut candidate = Candidate::from(premise);
        let initial_cost = candidate.cost();
        assert_eq!(initial_cost, RANGE_SCAN_COST + VERIFICATION_COST);

        let mut env = Environment::new();
        env.add("entity");
        candidate.update(&env);

        let after_entity = candidate.cost();
        assert_eq!(
            after_entity, LOOKUP_COST,
            "After binding entity, cost should decrease to LOOKUP_COST. Expected {}, got {}",
            LOOKUP_COST, after_entity
        );

        env.add("value");
        candidate.update(&env);

        let final_cost = candidate.cost();
        assert_eq!(
            final_cost, LOOKUP_COST,
            "After binding all variables, cost stays at LOOKUP_COST ({}), got {}",
            LOOKUP_COST, final_cost
        );
    }

    #[dialog_common::test]
    fn it_excludes_initial_env_variables_from_cost() {
        let query = AttributeQuery::new(
            Term::from(the!("user/name")),
            Term::<Entity>::var("entity"),
            Term::var("value"),
            Term::var("cause"),
            Some(Cardinality::One),
        );

        let mut env = Environment::new();
        env.add("entity");

        let cost = query.estimate(&env).unwrap_or(usize::MAX);

        assert_eq!(
            cost, LOOKUP_COST,
            "Variable already in env counts as bound. Expected LOOKUP_COST ({}), got {}",
            LOOKUP_COST, cost
        );
    }

    #[dialog_common::test]
    fn it_costs_more_for_cardinality_many() {
        let one = AttributeQuery::new(
            Term::from(the!("user/tags")),
            Term::<Entity>::var("entity"),
            Term::var("tag"),
            Term::var("cause"),
            Some(Cardinality::One),
        );

        let many = AttributeQuery::new(
            Term::from(the!("user/tags")),
            Term::<Entity>::var("entity"),
            Term::var("tag"),
            Term::var("cause"),
            Some(Cardinality::Many),
        );

        let one_candidate = Candidate::from(Premise::Assert(Proposition::Attribute(Box::new(one))));
        let many_candidate =
            Candidate::from(Premise::Assert(Proposition::Attribute(Box::new(many))));

        assert!(
            many_candidate.cost() > one_candidate.cost(),
            "Cardinality::Many should cost more than Cardinality::One. One: {}, Many: {}",
            one_candidate.cost(),
            many_candidate.cost()
        );

        assert_eq!(one_candidate.cost(), RANGE_SCAN_COST + VERIFICATION_COST);
        assert_eq!(many_candidate.cost(), INDEX_SCAN_COST);

        let expected_diff = INDEX_SCAN_COST - (RANGE_SCAN_COST + VERIFICATION_COST);
        let actual_diff = many_candidate.cost() - one_candidate.cost();

        assert_eq!(
            actual_diff, expected_diff,
            "Cost difference should be {} (INDEX_SCAN_COST - (RANGE_SCAN_COST + VERIFICATION_COST)), got {}",
            expected_diff, actual_diff
        );
    }

    #[dialog_common::test]
    fn it_costs_same_when_fully_bound() {
        let entity_val: Entity = Entity::new().unwrap();
        let value_val = Value::String("rust".to_string());

        let one_app = AttributeQuery::new(
            Term::from(the!("user/tags")),
            Term::from(entity_val.clone()),
            Term::Constant(value_val.clone()),
            Term::var("cause"),
            Some(Cardinality::One),
        );

        let many_app = AttributeQuery::new(
            Term::from(the!("user/tags")),
            Term::from(entity_val),
            Term::Constant(value_val),
            Term::var("cause"),
            Some(Cardinality::Many),
        );

        let one_candidate =
            Candidate::from(Premise::Assert(Proposition::Attribute(Box::new(one_app))));
        let many_candidate =
            Candidate::from(Premise::Assert(Proposition::Attribute(Box::new(many_app))));

        assert_eq!(one_candidate.cost(), LOOKUP_COST);
        assert_eq!(many_candidate.cost(), LOOKUP_COST);

        assert_eq!(
            one_candidate.cost(),
            many_candidate.cost(),
            "Fully bound queries have the same cost regardless of cardinality"
        );
    }

    #[dialog_common::test]
    fn it_costs_less_for_formula_without_io() {
        let mut formula_params = Parameters::new();
        formula_params.insert("of".to_string(), Term::constant("hello".to_string()));
        formula_params.insert("is".to_string(), Term::var("len"));

        let formula_app = Length::apply(formula_params).unwrap();
        let formula_candidate = Candidate::from(Premise::from(formula_app));

        let fact_app = AttributeQuery::new(
            Term::from(the!("user/name")),
            Term::<Entity>::var("entity"),
            Term::var("value"),
            Term::var("cause"),
            Some(Cardinality::One),
        );
        let fact_candidate =
            Candidate::from(Premise::Assert(Proposition::Attribute(Box::new(fact_app))));

        assert!(
            formula_candidate.cost() < fact_candidate.cost(),
            "Formula with no IO should be cheaper than AttributeQuery. Formula: {}, Fact: {}",
            formula_candidate.cost(),
            fact_candidate.cost()
        );
    }

    #[dialog_common::test]
    fn it_costs_more_for_formula_with_fact() {
        let mut formula_params = Parameters::new();
        formula_params.insert("of".to_string(), Term::var("text"));
        formula_params.insert("is".to_string(), Term::var("len"));

        let formula_app = Length::apply(formula_params).unwrap();
        let formula_premise = Premise::from(formula_app);
        let formula_candidate = Candidate::from(formula_premise);

        assert!(
            !formula_candidate.is_viable(),
            "Formula requiring unbound variable should be blocked"
        );
    }

    #[dialog_common::test]
    fn it_matches_fact_cost_nothing_bound() {
        let fact_app = AttributeQuery::new(
            Term::from(the!("user/name")),
            Term::<Entity>::var("entity"),
            Term::var("value"),
            Term::var("cause"),
            Some(Cardinality::One),
        );

        let concept = ConceptDescriptor::try_from([(
            "name",
            AttributeDescriptor::new(
                the!("user/name"),
                "User name",
                Cardinality::One,
                Some(Type::String),
            ),
        )])
        .unwrap();

        let mut terms = Parameters::new();
        terms.insert("this".to_string(), Term::var("entity"));
        terms.insert("name".to_string(), Term::var("value"));

        let concept_app = ConceptQuery {
            terms,
            predicate: concept,
        };

        let env = Environment::new();

        let fact_cost = fact_app.estimate(&env).expect("Should have cost");
        let concept_cost = concept_app.estimate(&env).expect("Should have cost");

        assert_eq!(fact_cost, RANGE_SCAN_COST + VERIFICATION_COST);
        assert_eq!(
            concept_cost,
            RANGE_SCAN_COST + VERIFICATION_COST + CONCEPT_OVERHEAD
        );

        assert!(
            concept_cost > fact_cost,
            "ConceptQuery should cost more than AttributeQuery due to rule overhead. \
             Fact: {}, Concept: {}",
            fact_cost,
            concept_cost
        );
    }

    #[dialog_common::test]
    fn it_matches_fact_cost_value_bound() {
        let fact_app = AttributeQuery::new(
            Term::from(the!("user/name")),
            Term::<Entity>::var("entity"),
            Term::var("value"),
            Term::var("cause"),
            Some(Cardinality::One),
        );

        let concept = ConceptDescriptor::try_from([(
            "name",
            AttributeDescriptor::new(
                the!("user/name"),
                "User name",
                Cardinality::One,
                Some(Type::String),
            ),
        )])
        .unwrap();

        let mut terms = Parameters::new();
        terms.insert("this".to_string(), Term::var("entity"));
        terms.insert("name".to_string(), Term::var("value"));

        let concept_app = ConceptQuery {
            terms,
            predicate: concept,
        };

        let mut env = Environment::new();
        env.add("value");

        let fact_cost = fact_app.estimate(&env).expect("Should have cost");
        let concept_cost = concept_app.estimate(&env).expect("Should have cost");

        assert_eq!(fact_cost, RANGE_READ_COST + VERIFICATION_COST);
        assert_eq!(
            concept_cost,
            RANGE_READ_COST + VERIFICATION_COST + CONCEPT_OVERHEAD
        );
    }

    #[dialog_common::test]
    fn it_matches_fact_cost_entity_bound() {
        let fact_app = AttributeQuery::new(
            Term::from(the!("user/name")),
            Term::<Entity>::var("entity"),
            Term::var("value"),
            Term::var("cause"),
            Some(Cardinality::One),
        );

        let concept = ConceptDescriptor::try_from([(
            "name",
            AttributeDescriptor::new(
                the!("user/name"),
                "User name",
                Cardinality::One,
                Some(Type::String),
            ),
        )])
        .unwrap();

        let mut terms = Parameters::new();
        terms.insert("this".to_string(), Term::var("entity"));
        terms.insert("name".to_string(), Term::var("value"));

        let concept_app = ConceptQuery {
            terms,
            predicate: concept,
        };

        let mut env = Environment::new();
        env.add("entity");

        let fact_cost = fact_app.estimate(&env).expect("Should have cost");
        let concept_cost = concept_app.estimate(&env).expect("Should have cost");

        assert_eq!(fact_cost, LOOKUP_COST);
        assert_eq!(concept_cost, LOOKUP_COST + CONCEPT_OVERHEAD);
    }

    #[dialog_common::test]
    fn it_matches_fact_cost_many_nothing_bound() {
        let fact_app = AttributeQuery::new(
            Term::from(the!("user/tags")),
            Term::<Entity>::var("entity"),
            Term::var("tag"),
            Term::var("cause"),
            Some(Cardinality::Many),
        );

        let tag = AttributeDescriptor::new(
            the!("user/tags"),
            "User tags",
            Cardinality::Many,
            Some(Type::String),
        );

        let concept = ConceptDescriptor::try_from([("tags", tag)]).unwrap();

        let mut terms = Parameters::new();
        terms.insert("this".to_string(), Term::var("entity"));
        terms.insert("tags".to_string(), Term::var("tag"));

        let concept_app = ConceptQuery {
            terms,
            predicate: concept,
        };

        let env = Environment::new();

        let fact_cost = fact_app.estimate(&env).expect("Should have cost");
        let concept_cost = concept_app.estimate(&env).expect("Should have cost");

        assert_eq!(fact_cost, INDEX_SCAN_COST);
        assert_eq!(concept_cost, INDEX_SCAN_COST + CONCEPT_OVERHEAD);
    }

    #[dialog_common::test]
    fn it_matches_fact_cost_many_value_bound() {
        let fact_app = AttributeQuery::new(
            Term::from(the!("user/tags")),
            Term::<Entity>::var("entity"),
            Term::var("tag"),
            Term::var("cause"),
            Some(Cardinality::Many),
        );

        let tag = AttributeDescriptor::new(
            the!("user/tags"),
            "User tags",
            Cardinality::Many,
            Some(Type::String),
        );

        let concept = ConceptDescriptor::try_from([("tags", tag)]).unwrap();

        let mut terms = Parameters::new();
        terms.insert("this".to_string(), Term::var("entity"));
        terms.insert("tags".to_string(), Term::var("tag"));

        let concept_app = ConceptQuery {
            terms,
            predicate: concept,
        };

        let mut env = Environment::new();
        env.add("tag");

        let fact_cost = fact_app.estimate(&env).expect("Should have cost");
        let concept_cost = concept_app.estimate(&env).expect("Should have cost");

        assert_eq!(fact_cost, RANGE_READ_COST);
        assert_eq!(concept_cost, RANGE_READ_COST + CONCEPT_OVERHEAD);
    }

    #[dialog_common::test]
    fn it_accumulates_cost_through_planning() {
        let p1 = AttributeQuery::new(
            Term::from(the!("user/name")),
            Term::<Entity>::var("entity"),
            Term::var("name"),
            Term::var("cause"),
            Some(Cardinality::One),
        );

        let p2 = AttributeQuery::new(
            Term::from(the!("user/age")),
            Term::<Entity>::var("entity"),
            Term::var("age"),
            Term::var("cause"),
            Some(Cardinality::One),
        );

        let a1 = Candidate::from(Premise::Assert(Proposition::Attribute(Box::new(p1))));
        let cost1 = a1.cost();

        assert_eq!(cost1, RANGE_SCAN_COST + VERIFICATION_COST);

        let mut a2 = Candidate::from(Premise::Assert(Proposition::Attribute(Box::new(
            p2.clone(),
        ))));
        let mut env = Environment::new();
        env.add("entity");
        a2.update(&env);
        let cost2 = a2.cost();

        assert_eq!(
            cost2, LOOKUP_COST,
            "Second premise with bound entity should cost LOOKUP_COST. Expected {}, got {}",
            LOOKUP_COST, cost2
        );

        let total = cost1 + cost2;
        let expected_total = RANGE_SCAN_COST + VERIFICATION_COST + LOOKUP_COST;
        assert_eq!(
            total, expected_total,
            "Total cost should be sum of individual costs. Expected {}, got {}",
            expected_total, total
        );
    }

    #[dialog_common::test]
    fn debug_update_cost() {
        let app = AttributeQuery::new(
            Term::from(the!("user/name")),
            Term::<Entity>::var("entity"),
            Term::var("value"),
            Term::var("cause"),
            Some(Cardinality::One),
        );

        let schema = app.schema();
        eprintln!("\nSchema:");
        for (name, constraint) in schema.iter() {
            eprintln!("  {}: {:?}", name, constraint.requirement);
        }

        let premise = Premise::Assert(Proposition::Attribute(Box::new(app)));
        let mut candidate = Candidate::from(premise);

        eprintln!("\nInitial state:");
        eprintln!("  Cost: {}", candidate.cost());
        if let Candidate::Viable { binds, .. } = &candidate {
            eprintln!("  Binds: {:?}", binds);
        }

        let mut env = Environment::new();
        env.add("entity");

        eprintln!("\nUpdating with entity bound...");
        candidate.update(&env);

        eprintln!("\nAfter update:");
        eprintln!("  Cost: {}", candidate.cost());
        if let Candidate::Viable { binds, env, .. } = &candidate {
            eprintln!("  Binds: {:?}", binds);
            eprintln!("  Env: {:?}", env);
        }
    }

    #[dialog_common::test]
    fn it_restores_cost_when_variable_leaves_scope() {
        let app = AttributeQuery::new(
            Term::from(the!("user/name")),
            Term::<Entity>::var("entity"),
            Term::var("value"),
            Term::var("cause"),
            Some(Cardinality::One),
        );
        let premise = Premise::Assert(Proposition::Attribute(Box::new(app)));
        let mut candidate = Candidate::from(premise);

        // Bind entity → cost should decrease
        let mut env_with_entity = Environment::new();
        env_with_entity.add("entity");
        candidate.update(&env_with_entity);

        assert_eq!(
            candidate.cost(),
            LOOKUP_COST,
            "After binding entity, cost should be LOOKUP_COST"
        );

        // Now update back to empty environment → cost should increase again
        let empty_env = Environment::new();
        candidate.update(&empty_env);

        assert_eq!(
            candidate.cost(),
            RANGE_SCAN_COST + VERIFICATION_COST,
            "After removing entity from scope, cost should return to RANGE_SCAN_COST + VERIFICATION_COST. \
             Without bidirectional update, stale env retains entity binding."
        );
    }

    #[dialog_common::test]
    fn it_restores_cost_for_cardinality_many_when_variable_leaves_scope() {
        // Cardinality::Many makes the cost difference more dramatic:
        //   1/3 constraints (just 'the'): INDEX_SCAN_COST = 5000
        //   2/3 constraints (the + of):   RANGE_READ_COST = 200
        let app = AttributeQuery::new(
            Term::from(the!("person/hobbies")),
            Term::<Entity>::var("entity"),
            Term::var("hobby"),
            Term::var("cause"),
            Some(Cardinality::Many),
        );
        let premise = Premise::Assert(Proposition::Attribute(Box::new(app)));
        let mut candidate = Candidate::from(premise);

        assert_eq!(
            candidate.cost(),
            INDEX_SCAN_COST,
            "With 1/3 constraints, Cardinality::Many should cost INDEX_SCAN_COST"
        );

        // Bind entity → 2/3 constraints
        let mut env_with_entity = Environment::new();
        env_with_entity.add("entity");
        candidate.update(&env_with_entity);

        assert_eq!(
            candidate.cost(),
            RANGE_READ_COST,
            "With 2/3 constraints, cost should drop to RANGE_READ_COST"
        );

        // Update back to empty → should revert to 1/3 constraints
        let empty_env = Environment::new();
        candidate.update(&empty_env);

        assert_eq!(
            candidate.cost(),
            INDEX_SCAN_COST,
            "After replanning back to empty env, cost should return to INDEX_SCAN. \
             Without bidirectional update, stale env would keep the lower cost."
        );
    }
}

/// Characterization tests pinning `Candidate`'s observable
/// categorization at construction and on scope *growth* — the exact
/// `binds` / `requires` / `env` set contents and viability for the
/// paths a forward planning pass actually exercises. These capture
/// the current behavior so a refactor of `Candidate::from` / `update`
/// can be proven behavior-preserving. The coarse plan-ordering tests
/// in `planner.rs` pin the end-to-end observable (plan order + cost
/// across replans).
#[cfg(test)]
mod characterization {
    #[cfg(target_arch = "wasm32")]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    use super::{Candidate, Header, Plan};
    use crate::artifact::Entity;
    use crate::attribute::The;
    use crate::attribute::query::AttributeQuery;
    use crate::formula::Formula;
    use crate::formula::string::Length;
    use crate::planner::{Binds, Infeasible};
    use crate::proposition::Proposition;
    use crate::the;
    use crate::{Cardinality, Environment, Parameters, Premise, Term};
    use std::collections::BTreeSet;

    /// Sorted variable names in a candidate's `binds` set.
    fn binds_of(c: &Candidate) -> Vec<String> {
        let env = match c {
            Candidate::Viable { binds, .. } => binds,
            Candidate::Blocked { binds, .. } => binds,
        };
        let mut v: Vec<String> = env.iter().map(String::from).collect();
        v.sort();
        v
    }

    /// Sorted variable names a candidate still requires (empty when
    /// viable).
    fn requires_of(c: &Candidate) -> Vec<String> {
        match c {
            Candidate::Viable { .. } => Vec::new(),
            Candidate::Blocked { requires, .. } => {
                let mut v: Vec<String> = requires.iter().map(String::from).collect();
                v.sort();
                v
            }
        }
    }

    /// Sorted variable names in a candidate's `env` (bound) set.
    fn env_of(c: &Candidate) -> Vec<String> {
        let env = match c {
            Candidate::Viable { env, .. } => env,
            Candidate::Blocked { env, .. } => env,
        };
        let mut v: Vec<String> = env.iter().map(String::from).collect();
        v.sort();
        v
    }

    fn scope(vars: &[&str]) -> Environment {
        let mut env = Environment::new();
        for v in vars {
            env.add(*v);
        }
        env
    }

    /// An all-variable attribute query: `the`/`of`/`is`/`cause` are
    /// all in one choice group, so binding any one satisfies the
    /// group and the rest become binds rather than requires.
    fn attribute_candidate() -> Candidate {
        let query = AttributeQuery::new(
            Term::<The>::var("the"),
            Term::<Entity>::var("of"),
            Term::var("is"),
            Term::var("cause"),
            Some(Cardinality::One),
        );
        Candidate::from(Premise::Assert(Proposition::Attribute(Box::new(query))))
    }

    /// At empty scope, no choice-group member is bound, so every
    /// non-blank slot is required and the candidate is blocked.
    #[dialog_common::test]
    fn attribute_empty_scope_requires_all_slots() {
        let candidate = attribute_candidate();
        assert!(!candidate.is_viable(), "no bound member → blocked");
        assert_eq!(
            requires_of(&candidate),
            vec!["cause", "is", "of", "the"],
            "all four grouped slots are required at empty scope"
        );
        assert_eq!(binds_of(&candidate), Vec::<String>::new());
        assert_eq!(env_of(&candidate), Vec::<String>::new());
    }

    /// Binding one choice-group member makes the candidate viable:
    /// `Candidate::from` with `of` already in scope is viable and
    /// requires nothing. (Constructed at the target scope rather than
    /// via incremental `update`, since `update` is an optimization
    /// whose internal state is not part of the contract — only the
    /// resulting viability/binds is.)
    #[dialog_common::test]
    fn attribute_with_one_member_in_scope_is_viable() {
        let query = AttributeQuery::new(
            Term::<The>::var("the"),
            Term::<Entity>::var("of"),
            Term::var("is"),
            Term::var("cause"),
            Some(Cardinality::One),
        );
        let mut candidate =
            Candidate::from(Premise::Assert(Proposition::Attribute(Box::new(query))));
        candidate.update(&scope(&["of"]));

        assert!(candidate.is_viable(), "one bound member satisfies group");
        assert_eq!(requires_of(&candidate), Vec::<String>::new());
    }

    // `update` is an incremental optimization — it avoids recomputing
    // categorization from scratch as scope grows during a forward
    // planning pass. Its internal state (and the Viable-arm shrink
    // asymmetry) is not part of the contract: replanning rebuilds
    // candidates fresh via `Conjunction::plan` ->
    // `Planner::from(Vec<Premise>)`, so the only observable that
    // matters is the plan that comes out. That is pinned by the
    // coarse plan-ordering tests in `planner.rs`. Tests here assert
    // `Candidate::from`'s construction-time categorization, the part
    // that defines feasibility.

    /// A constant choice-group member satisfies the group at
    /// construction: a query with a constant `the` is viable from
    /// the start, binding the variable slots.
    #[dialog_common::test]
    fn attribute_constant_member_satisfies_group_at_construction() {
        let query = AttributeQuery::new(
            Term::from(the!("person/name")),
            Term::<Entity>::var("of"),
            Term::var("is"),
            Term::var("cause"),
            Some(Cardinality::One),
        );
        let candidate = Candidate::from(Premise::Assert(Proposition::Attribute(Box::new(query))));

        assert!(candidate.is_viable(), "constant `the` satisfies the group");
        assert_eq!(
            binds_of(&candidate),
            vec!["cause", "is", "of"],
            "variable slots bind once the group is constant-satisfied"
        );
        assert_eq!(requires_of(&candidate), Vec::<String>::new());
    }

    /// A formula with an ungrouped `Required(None)` input is blocked
    /// at construction until that exact variable is bound; its output
    /// is a bind. With the input already in scope it is viable.
    #[dialog_common::test]
    fn formula_required_input_is_needed_then_binds_output() {
        let mut params = Parameters::new();
        params.insert("of".to_string(), Term::var("text"));
        params.insert("is".to_string(), Term::var("len"));
        let candidate = Candidate::from(Premise::from(Length::apply(params.clone()).unwrap()));

        assert!(!candidate.is_viable(), "unbound required input → blocked");
        assert_eq!(requires_of(&candidate), vec!["text"]);
        assert_eq!(binds_of(&candidate), vec!["len"]);

        let mut candidate = Candidate::from(Premise::from(Length::apply(params).unwrap()));
        candidate.update(&scope(&["text"]));
        assert!(candidate.is_viable(), "binding the input unblocks");
    }

    /// A negated premise never contributes binds — its slots are
    /// requirements only. With both bound it is viable.
    #[dialog_common::test]
    fn negation_requires_but_never_binds() {
        let x = Term::<String>::var("x");
        let y = Term::<String>::var("y");
        let candidate = Candidate::from(!x.clone().is(y.clone()));

        assert!(!candidate.is_viable());
        assert_eq!(
            binds_of(&candidate),
            Vec::<String>::new(),
            "negation binds nothing"
        );
        assert_eq!(requires_of(&candidate), vec!["x", "y"]);

        let mut candidate = Candidate::from(!x.is(y));
        candidate.update(&scope(&["x", "y"]));
        assert!(candidate.is_viable(), "both bound → viable");
    }

    /// Equivalence of the incremental `update` with a stateless
    /// recompute, under the planner's real call pattern.
    ///
    /// The planner always calls `update` with the *full* cumulative
    /// scope, and that scope only ever grows during a pass
    /// (`planner.rs`: `bound.extend(plan.binds())`). Under that
    /// pattern, the incremental candidate's observable categorization
    /// — viability and the `binds` set — must agree at every step
    /// with a fresh stateless `Plan::adorn(full_scope)`. This is the
    /// property a stateless refactor of `Candidate` relies on; it is
    /// tested directly so the refactor can be proven equivalent.
    #[dialog_common::test]
    fn incremental_update_matches_stateless_adorn_on_growth() {
        // Two attribute premises sharing `entity`/`cause`, plus a
        // formula over `name` → a mix of choice-group and ungrouped
        // requirements.
        let cases: Vec<Premise> = vec![
            AttributeQuery::new(
                Term::<The>::var("the"),
                Term::<Entity>::var("entity"),
                Term::var("name"),
                Term::var("cause"),
                Some(Cardinality::One),
            )
            .into(),
            {
                let mut params = Parameters::new();
                params.insert("of".to_string(), Term::var("name"));
                params.insert("is".to_string(), Term::var("len"));
                Premise::from(Length::apply(params).unwrap())
            },
        ];

        // A monotonically growing sequence of full scopes.
        let growth: [&[&str]; 4] = [
            &[],
            &["entity"],
            &["entity", "name"],
            &["entity", "name", "the"],
        ];

        for premise in cases {
            // Stateless reference: lower to a Plan once; `adorn` is a
            // pure function of the scope.
            let reference = Plan::lower(
                premise.clone(),
                Header {
                    cost: 0,
                    binds: Environment::new(),
                    env: Environment::new(),
                },
            );

            // Incremental candidate, driven through the growing scopes
            // with the full scope each time — the planner's pattern.
            let mut candidate = Candidate::from(premise);

            for step in growth {
                let full = scope(step);
                candidate.update(&full);

                let bound: BTreeSet<String> = step.iter().map(|s| s.to_string()).collect();
                let stateless = reference.adorn(&bound);

                match stateless {
                    Ok(Binds(expected_binds)) => {
                        assert!(
                            candidate.is_viable(),
                            "incremental must be viable when stateless adorn is Ok at scope {:?}",
                            step
                        );
                        let actual: BTreeSet<String> = binds_of(&candidate).into_iter().collect();
                        assert_eq!(
                            actual, expected_binds,
                            "binds must match stateless adorn at scope {:?}",
                            step
                        );
                    }
                    Err(Infeasible::NeedsAll(expected_needs)) => {
                        assert!(
                            !candidate.is_viable(),
                            "incremental must be blocked when stateless adorn is infeasible at scope {:?}",
                            step
                        );
                        let actual: BTreeSet<String> =
                            requires_of(&candidate).into_iter().collect();
                        assert_eq!(
                            actual, expected_needs,
                            "requires must match stateless adorn's needs at scope {:?}",
                            step
                        );
                    }
                }
            }
        }
    }
}

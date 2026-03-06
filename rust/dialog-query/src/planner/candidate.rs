use super::Plan;
use crate::error::TypeError;
use crate::{Environment, Parameters, Premise, Requirement, Schema};
use std::collections::HashSet;

/// A premise under consideration by the query planner, tracking whether it
/// can execute given the current variable bindings.
///
/// The planner examines each premise to determine whether it can execute
/// given the current set of bound variables. A `Candidate` captures this
/// determination along with cached schema and parameter data that allow
/// efficient incremental re-evaluation as the environment grows.
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
        /// Cached schema for efficient incremental updates.
        schema: Schema,
        /// Cached parameters for efficient incremental updates.
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
        /// Cached schema for efficient incremental updates.
        schema: Schema,
        /// Cached parameters for efficient incremental updates.
        params: Parameters,
    },
}

impl Candidate {
    /// Analyzes a premise to determine whether it is viable or blocked,
    /// and computes its estimated cost in an empty environment.
    pub fn from(premise: Premise) -> Self {
        let schema = premise.schema();
        let params = premise.parameters();
        let env = Environment::new();

        // Negations never bind variables - they only filter
        let is_negation = matches!(premise, Premise::Unless(_));

        // Use the premise's estimate() method to calculate cost
        // If None, the premise is unbound and should use a high cost
        let cost = premise.estimate(&env).unwrap_or(usize::MAX);
        let mut binds = Environment::new();
        let mut requires = Environment::new();

        // Track which choice groups are satisfied by constants
        let mut satisfied_groups = HashSet::new();

        // First pass: identify requirement groups satisfied by constants
        for (name, constraint) in schema.iter() {
            if let Some(param) = params.get(name)
                && let Requirement::Required(Some(group)) = &constraint.requirement
                && param.is_constant()
            {
                // If parameter is a constant, its group is satisfied
                satisfied_groups.insert(*group);
            }
        }

        // Second pass: categorize all parameters based on their requirement types
        for (name, constraint) in schema.iter() {
            if let Some(param) = params.get(name) {
                // Constants and variables already in env don't add cost - they're already satisfied
                if param.is_constant() || param.is_bound(&env) {
                    continue;
                }

                // Blank terms are wildcards - they match anything and don't need to be bound
                if param.is_blank() {
                    continue;
                }

                match &constraint.requirement {
                    Requirement::Required(Some(group)) => {
                        // If this group is satisfied, treat as desired (variable will be bound)
                        if satisfied_groups.contains(group) {
                            // Negations don't bind variables, so skip adding to binds
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
                        // Negations don't bind variables, so skip adding to binds
                        if !is_negation {
                            param.bind(&mut binds);
                        }
                    }
                }
            }
        }

        // If no requirements, create Viable candidate
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
    /// Variables present in `target_scope` are moved from `binds` to `env`,
    /// and variables absent from `target_scope` are moved from `env` back to
    /// `binds`. This handles both incremental updates (scope grows) and
    /// replanning scenarios (scope shrinks or differs).
    ///
    /// May transition from Blocked to Viable if requirements are satisfied.
    pub fn update(&mut self, target_scope: &Environment) {
        match self {
            Candidate::Viable {
                premise,
                cost,
                binds,
                env,
                schema,
                params,
            } => {
                for (name, _constraint) in schema.iter() {
                    if let Some(param) = params.get(name) {
                        if param.is_constant() || param.is_blank() {
                            continue;
                        }

                        let in_target = param.is_bound(target_scope);
                        let in_env = param.is_bound(env);
                        let in_binds = param.is_bound(binds);

                        match (in_target, in_env, in_binds) {
                            // Variable is in target but only in binds → move to env
                            (true, false, true) => {
                                param.bind(env);
                                param.unbind(binds);
                            }
                            // Variable is in target but tracked nowhere → add to env
                            (true, false, false) => {
                                param.bind(env);
                            }
                            // Variable left target but is in env → move back to binds
                            (false, true, false) => {
                                param.unbind(env);
                                param.bind(binds);
                            }
                            // Variable not in target and not tracked → add to binds
                            (false, false, false) => {
                                param.bind(binds);
                            }
                            // Already consistent: (true, true, _) or (false, false, true)
                            _ => {}
                        }
                    }
                }

                // Re-estimate cost based on updated environment
                *cost = premise.estimate(env).unwrap_or(usize::MAX);
            }
            Candidate::Blocked {
                premise,
                cost,
                binds,
                env,
                requires,
                schema,
                params,
            } => {
                let is_negation = matches!(premise, Premise::Unless(_));

                // Track which choice groups now have at least one bound parameter
                let mut satisfied_groups = HashSet::new();

                // First pass: synchronize each parameter with the target scope
                for (name, constraint) in schema.iter() {
                    if let Some(param) = params.get(name) {
                        if param.is_constant() || param.is_blank() {
                            continue;
                        }

                        let in_target = param.is_bound(target_scope);
                        let in_env = param.is_bound(env);

                        if in_target && !in_env {
                            // Variable entered the scope → move from requires/binds to env
                            let was_required = param.unbind(requires);
                            let was_bound = param.unbind(binds);

                            if was_required || was_bound {
                                param.bind(env);

                                if let Requirement::Required(Some(group)) = &constraint.requirement
                                {
                                    satisfied_groups.insert(*group);
                                }
                            }
                        } else if !in_target && in_env {
                            // Variable left the scope → move from env back to
                            // requires or binds depending on its schema requirement
                            param.unbind(env);

                            match &constraint.requirement {
                                Requirement::Required(Some(group)) => {
                                    // Will be resolved in second pass once we
                                    // know which groups are still satisfied
                                    param.bind(requires);
                                    // Check if the group might still be satisfied
                                    // by another bound parameter
                                    let group_still_satisfied =
                                        schema.iter().any(|(other_name, other_constraint)| {
                                            other_name != name
                                                && matches!(
                                                    &other_constraint.requirement,
                                                    Requirement::Required(Some(g)) if *g == *group
                                                )
                                                && params.get(other_name).is_some_and(|p| {
                                                    p.is_bound(env) || p.is_constant()
                                                })
                                        });
                                    if group_still_satisfied {
                                        satisfied_groups.insert(*group);
                                    }
                                }
                                Requirement::Required(None) => {
                                    param.bind(requires);
                                }
                                Requirement::Optional => {
                                    if !is_negation {
                                        param.bind(binds);
                                    }
                                }
                            }
                        }
                    }
                }

                // Second pass: for satisfied choice groups, convert required params to desired
                if !satisfied_groups.is_empty() {
                    for (name, constraint) in schema.iter() {
                        if let Requirement::Required(Some(group)) = &constraint.requirement
                            && satisfied_groups.contains(group)
                            && let Some(param) = params.get(name)
                            && param.unbind(requires)
                            && !param.is_bound(env)
                            && !is_negation
                        {
                            param.bind(binds);
                        }
                    }
                }

                // Re-estimate cost based on updated environment
                *cost = premise.estimate(env).unwrap_or(usize::MAX);

                // If no requirements remain, transition to Viable
                if requires.is_empty() {
                    *self = Candidate::Viable {
                        premise: premise.clone(),
                        cost: *cost,
                        binds: binds.clone(),
                        env: env.clone(),
                        schema: schema.clone(),
                        params: params.clone(),
                    };
                }
            }
        }
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

impl From<Plan> for Candidate {
    fn from(plan: Plan) -> Self {
        Self::Viable {
            schema: plan.premise.schema(),
            params: plan.premise.parameters(),
            premise: plan.premise,
            cost: plan.cost,
            binds: plan.binds,
            env: plan.env,
        }
    }
}

impl From<&Plan> for Candidate {
    fn from(plan: &Plan) -> Self {
        Self::Viable {
            schema: plan.premise.schema(),
            params: plan.premise.parameters(),
            premise: plan.premise.clone(),
            cost: plan.cost,
            binds: plan.binds.clone(),
            env: plan.env.clone(),
        }
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
                // Drop schema/params - don't need them in the final plan
                Ok(Plan {
                    premise,
                    cost,
                    binds,
                    env,
                })
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

        let concept = ConceptDescriptor::from([(
            "name",
            AttributeDescriptor::new(
                the!("user/name"),
                "User name",
                Cardinality::One,
                Some(Type::String),
            ),
        )]);

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

        let concept = ConceptDescriptor::from([(
            "name",
            AttributeDescriptor::new(
                the!("user/name"),
                "User name",
                Cardinality::One,
                Some(Type::String),
            ),
        )]);

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

        let concept = ConceptDescriptor::from([(
            "name",
            AttributeDescriptor::new(
                the!("user/name"),
                "User name",
                Cardinality::One,
                Some(Type::String),
            ),
        )]);

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

        let concept = ConceptDescriptor::from([("tags", tag)]);

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

        let concept = ConceptDescriptor::from([("tags", tag)]);

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

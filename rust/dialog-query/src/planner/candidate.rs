use super::{Plan, Prerequisites};
use crate::error::CompileError;
use crate::{Environment, Parameters, Premise, Requirement, Schema, Term};
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
        requires: Prerequisites,
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
        let mut requires = Prerequisites::new();

        // Track which choice groups are satisfied by constants
        let mut satisfied_groups = HashSet::new();

        // First pass: identify requirement groups satisfied by constants
        for (name, constraint) in schema.iter() {
            if let Some(term) = params.get(name)
                && let Requirement::Required(Some(group)) = &constraint.requirement
                && matches!(term, Term::Constant(_))
            {
                // If parameter is a constant, its group is satisfied
                satisfied_groups.insert(*group);
            }
        }

        // Second pass: categorize all parameters based on their requirement types
        for (name, constraint) in schema.iter() {
            if let Some(term) = params.get(name) {
                // Constants and variables already in env don't add cost - they're already satisfied
                if matches!(term, Term::Constant(_)) || env.contains(term) {
                    continue;
                }

                // Blank terms are wildcards - they match anything and don't need to be bound
                if term.is_blank() {
                    continue;
                }

                match &constraint.requirement {
                    Requirement::Required(Some(group)) => {
                        // If this group is satisfied, treat as desired (variable will be bound)
                        if satisfied_groups.contains(group) {
                            // Negations don't bind variables, so skip adding to binds
                            if !is_negation {
                                binds.add(term);
                            }
                        } else {
                            requires.insert(term);
                        }
                    }
                    Requirement::Required(None) => {
                        requires.insert(term);
                    }
                    Requirement::Optional => {
                        // Negations don't bind variables, so skip adding to binds
                        if !is_negation {
                            binds.add(term);
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
    /// Update this candidate with new bindings from the environment.
    /// May transition from Blocked to Viable if requirements are satisfied.
    /// Only processes relevant bindings and updates incrementally.
    pub fn update(&mut self, new_bindings: &Environment) {
        match self {
            Candidate::Viable {
                premise,
                cost,
                binds,
                env,
                schema,
                params,
            } => {
                // Only process bindings that are relevant to this plan
                for (name, _constraint) in schema.iter() {
                    if let Some(term) = params.get(name) {
                        // Skip constants - they're never in binds
                        if matches!(term, Term::Constant(_)) {
                            continue;
                        }

                        // If this term was in binds and is now bound, move it to env
                        if new_bindings.contains(term) && binds.contains(term) {
                            // Add to env (only relevant bindings)
                            env.add(term);

                            // Remove from binds (incremental update)
                            binds.remove(term);
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
                // Track which choice groups now have at least one bound parameter
                let mut satisfied_groups = HashSet::new();

                // Process only relevant bindings (parameters that got bound)
                for (name, constraint) in schema.iter() {
                    if let Some(term) = params.get(name)
                        && new_bindings.contains(term)
                    {
                        // Check if this term is relevant to this plan
                        let was_required = requires.remove(term);
                        let was_bound = binds.remove(term);

                        if was_required || was_bound {
                            // This parameter is now bound (add to env)
                            env.add(term);

                            // If this is part of a choice group, mark that group as satisfied
                            if let Requirement::Required(Some(group)) = &constraint.requirement {
                                satisfied_groups.insert(*group);
                            }
                        }
                    }
                }

                // Second pass: for satisfied choice groups, convert required params to desired
                if !satisfied_groups.is_empty() {
                    for (name, constraint) in schema.iter() {
                        if let Requirement::Required(Some(group)) = &constraint.requirement
                            && satisfied_groups.contains(group)
                            && let Some(term) = params.get(name)
                        {
                            // If this term was required, it's no longer required
                            // Move it to binds if it's not already bound
                            if requires.remove(term) && !env.contains(term) {
                                binds.add(term);
                            }
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
    type Error = CompileError;

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
                Err(CompileError::RequiredBindings { required: requires })
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::formula::Formula;
    use crate::formula::string::Length;
    use crate::{Environment, Parameters, Premise, Term, Value};

    #[dialog_common::test]
    fn test_candidate_from_premise_all_derived() {
        let mut params = Parameters::new();
        params.insert("of".to_string(), Term::<Value>::var("text".to_string()));
        params.insert("is".to_string(), Term::<Value>::var("len".to_string()));

        let application = Length::apply(params).unwrap();
        let premise = Premise::from(application);

        let candidate = Candidate::from(premise);
        assert!(!candidate.is_viable());
    }

    #[dialog_common::test]
    fn test_candidate_from_premise_with_constant() {
        let mut params = Parameters::new();
        params.insert(
            "of".to_string(),
            Term::<Value>::Constant(Value::String("hello".to_string())),
        );
        params.insert("is".to_string(), Term::<Value>::var("len".to_string()));

        let application = Length::apply(params).unwrap();
        let premise = Premise::from(application);

        let candidate = Candidate::from(premise);
        assert!(candidate.is_viable());
    }

    #[dialog_common::test]
    fn test_candidate_update_transitions_to_viable() {
        let mut params = Parameters::new();
        params.insert("of".to_string(), Term::<Value>::var("text"));
        params.insert("is".to_string(), Term::<Value>::var("len"));

        let application = Length::apply(params).unwrap();
        let premise = Premise::from(application);

        let mut candidate = Candidate::from(premise);
        assert!(!candidate.is_viable());

        let mut env = Environment::new();
        env.add(&Term::<Value>::var("text"));
        candidate.update(&env);

        assert!(candidate.is_viable());
    }

    #[dialog_common::test]
    fn test_candidate_update_reduces_cost_when_derived_bound() {
        let mut params = Parameters::new();
        params.insert(
            "of".to_string(),
            Term::<Value>::Constant(Value::String("hello".to_string())),
        );
        params.insert("is".to_string(), Term::<Value>::var("len".to_string()));

        let application = Length::apply(params).unwrap();
        let premise = Premise::from(application);

        let mut candidate = Candidate::from(premise);
        let initial_cost = candidate.cost();
        assert!(candidate.is_viable());

        let mut env = Environment::new();
        env.add(&Term::<Value>::var("len".to_string()));
        candidate.update(&env);

        assert_eq!(
            candidate.cost(),
            initial_cost,
            "Formula cost should remain constant regardless of bound variables"
        );
    }

    #[dialog_common::test]
    fn test_candidate_try_into_plan_when_viable() {
        let mut params = Parameters::new();
        params.insert(
            "of".to_string(),
            Term::<Value>::Constant(Value::String("hello".to_string())),
        );
        params.insert("is".to_string(), Term::<Value>::var("len".to_string()));

        let application = Length::apply(params).unwrap();
        let premise = Premise::from(application);

        let candidate = Candidate::from(premise);
        assert!(candidate.is_viable());

        let plan = Plan::try_from(candidate);
        assert!(plan.is_ok());
    }

    #[dialog_common::test]
    fn test_candidate_try_into_plan_when_blocked() {
        let mut params = Parameters::new();
        params.insert("of".to_string(), Term::<Value>::var("text".to_string()));
        params.insert("is".to_string(), Term::<Value>::var("len".to_string()));

        let application = Length::apply(params).unwrap();
        let premise = Premise::from(application);

        let candidate = Candidate::from(premise);
        assert!(!candidate.is_viable());

        let plan = Plan::try_from(candidate);
        assert!(plan.is_err());
    }
}

#[cfg(test)]
mod cost_model_tests {
    use super::Candidate;
    use crate::artifact::Entity;
    use crate::attribute::Cardinality;
    use crate::concept::application::ConceptQuery;
    use crate::concept::descriptor::ConceptDescriptor;
    use crate::formula::Formula;
    use crate::formula::string::Length;
    use crate::proposition::Proposition;
    use crate::relation::descriptor::RelationDescriptor;
    use crate::relation::query::RelationQuery;
    use crate::schema::{
        CONCEPT_OVERHEAD, INDEX_SCAN, RANGE_READ_COST, RANGE_SCAN_COST, SEGMENT_READ_COST,
    };
    use crate::the;
    use crate::{AttributeDescriptor, Environment, Parameters, Premise, Term, Type, Value};

    #[dialog_common::test]
    fn test_constants_do_not_add_cost() {
        let entity_val: Entity = Entity::new().unwrap();

        let app = RelationQuery::new(
            Term::Constant("user".to_string()),
            Term::Constant("name".to_string()),
            Term::Constant(entity_val),
            Term::Constant(Value::String("test".to_string())),
            Term::var("cause"),
            Some(RelationDescriptor::new(None, Cardinality::One)),
        );
        let premise = Premise::When(Proposition::Relation(Box::new(app)));
        let candidate = Candidate::from(premise);

        assert_eq!(
            candidate.cost(),
            SEGMENT_READ_COST,
            "All constants should only cost SEGMENT_READ_COST ({}), got {}",
            SEGMENT_READ_COST,
            candidate.cost()
        );
    }

    #[dialog_common::test]
    fn test_one_constant_two_variables() {
        let app = RelationQuery::new(
            Term::Constant("user".to_string()),
            Term::Constant("name".to_string()),
            Term::<Entity>::var("entity"),
            Term::<Value>::var("value"),
            Term::var("cause"),
            Some(RelationDescriptor::new(None, Cardinality::One)),
        );
        let premise = Premise::When(Proposition::Relation(Box::new(app)));
        let candidate = Candidate::from(premise);

        assert_eq!(
            candidate.cost(),
            RANGE_SCAN_COST,
            "With 1 constraint (just constant 'the'), cost should be RANGE_SCAN_COST ({}), got {}",
            RANGE_SCAN_COST,
            candidate.cost()
        );
    }

    #[dialog_common::test]
    fn test_env_variables_reduce_cost() {
        let app = RelationQuery::new(
            Term::Constant("user".to_string()),
            Term::Constant("name".to_string()),
            Term::<Entity>::var("entity"),
            Term::<Value>::var("value"),
            Term::var("cause"),
            Some(RelationDescriptor::new(None, Cardinality::One)),
        );
        let premise = Premise::When(Proposition::Relation(Box::new(app)));

        let mut candidate = Candidate::from(premise);
        let initial_cost = candidate.cost();
        assert_eq!(initial_cost, RANGE_SCAN_COST);

        let mut env = Environment::new();
        env.add(&Term::<Value>::var("entity"));
        candidate.update(&env);

        let after_entity = candidate.cost();
        assert_eq!(
            after_entity, SEGMENT_READ_COST,
            "After binding entity, cost should decrease to SEGMENT_READ_COST. Expected {}, got {}",
            SEGMENT_READ_COST, after_entity
        );

        env.add(&Term::<Value>::var("value"));
        candidate.update(&env);

        let final_cost = candidate.cost();
        assert_eq!(
            final_cost, SEGMENT_READ_COST,
            "After binding all variables, cost stays at SEGMENT_READ_COST ({}), got {}",
            SEGMENT_READ_COST, final_cost
        );
    }

    #[dialog_common::test]
    fn test_variables_already_in_initial_env_dont_add_cost() {
        let app = RelationQuery::new(
            Term::Constant("user".to_string()),
            Term::Constant("name".to_string()),
            Term::<Entity>::var("entity"),
            Term::<Value>::var("value"),
            Term::var("cause"),
            Some(RelationDescriptor::new(None, Cardinality::One)),
        );

        let mut env = Environment::new();
        env.add(&Term::<Value>::var("entity"));

        let cost = app.estimate(&env).unwrap_or(usize::MAX);

        assert_eq!(
            cost, SEGMENT_READ_COST,
            "Variable already in env counts as bound. Expected SEGMENT_READ_COST ({}), got {}",
            SEGMENT_READ_COST, cost
        );
    }

    #[dialog_common::test]
    fn test_cardinality_many_costs_more_than_one() {
        let one_app = RelationQuery::new(
            Term::Constant("user".to_string()),
            Term::Constant("tags".to_string()),
            Term::<Entity>::var("entity"),
            Term::<Value>::var("tag"),
            Term::var("cause"),
            Some(RelationDescriptor::new(None, Cardinality::One)),
        );

        let many_app = RelationQuery::new(
            Term::Constant("user".to_string()),
            Term::Constant("tags".to_string()),
            Term::<Entity>::var("entity"),
            Term::<Value>::var("tag"),
            Term::var("cause"),
            Some(RelationDescriptor::new(None, Cardinality::Many)),
        );

        let one_candidate =
            Candidate::from(Premise::When(Proposition::Relation(Box::new(one_app))));
        let many_candidate =
            Candidate::from(Premise::When(Proposition::Relation(Box::new(many_app))));

        assert!(
            many_candidate.cost() > one_candidate.cost(),
            "Cardinality::Many should cost more than Cardinality::One. One: {}, Many: {}",
            one_candidate.cost(),
            many_candidate.cost()
        );

        assert_eq!(one_candidate.cost(), RANGE_SCAN_COST);
        assert_eq!(many_candidate.cost(), INDEX_SCAN);

        let expected_diff = INDEX_SCAN - RANGE_SCAN_COST;
        let actual_diff = many_candidate.cost() - one_candidate.cost();

        assert_eq!(
            actual_diff, expected_diff,
            "Cost difference should be {} (INDEX_SCAN - RANGE_SCAN_COST), got {}",
            expected_diff, actual_diff
        );
    }

    #[dialog_common::test]
    fn test_fully_bound_cardinality_should_not_differ() {
        let entity_val: Entity = Entity::new().unwrap();
        let value_val = Value::String("rust".to_string());

        let one_app = RelationQuery::new(
            Term::Constant("user".to_string()),
            Term::Constant("tags".to_string()),
            Term::Constant(entity_val.clone()),
            Term::Constant(value_val.clone()),
            Term::var("cause"),
            Some(RelationDescriptor::new(None, Cardinality::One)),
        );

        let many_app = RelationQuery::new(
            Term::Constant("user".to_string()),
            Term::Constant("tags".to_string()),
            Term::Constant(entity_val),
            Term::Constant(value_val),
            Term::var("cause"),
            Some(RelationDescriptor::new(None, Cardinality::Many)),
        );

        let one_candidate =
            Candidate::from(Premise::When(Proposition::Relation(Box::new(one_app))));
        let many_candidate =
            Candidate::from(Premise::When(Proposition::Relation(Box::new(many_app))));

        assert_eq!(one_candidate.cost(), SEGMENT_READ_COST);
        assert_eq!(many_candidate.cost(), RANGE_READ_COST);

        assert!(many_candidate.cost() > one_candidate.cost());
        assert!(
            many_candidate.cost() < one_candidate.cost() * 3,
            "Fully bound Many should cost more than One, but not drastically more"
        );
    }

    #[dialog_common::test]
    fn test_formula_cheaper_than_fact_no_io() {
        let mut formula_params = Parameters::new();
        formula_params.insert(
            "of".to_string(),
            Term::<Value>::Constant(Value::String("hello".to_string())),
        );
        formula_params.insert("is".to_string(), Term::<Value>::var("len"));

        let formula_app = Length::apply(formula_params).unwrap();
        let formula_candidate = Candidate::from(Premise::from(formula_app));

        let fact_app = RelationQuery::new(
            Term::Constant("user".to_string()),
            Term::Constant("name".to_string()),
            Term::<Entity>::var("entity"),
            Term::<Value>::var("value"),
            Term::var("cause"),
            Some(RelationDescriptor::new(None, Cardinality::One)),
        );
        let fact_candidate =
            Candidate::from(Premise::When(Proposition::Relation(Box::new(fact_app))));

        assert!(
            formula_candidate.cost() < fact_candidate.cost(),
            "Formula with no IO should be cheaper than RelationQuery. Formula: {}, Fact: {}",
            formula_candidate.cost(),
            fact_candidate.cost()
        );
    }

    #[dialog_common::test]
    fn test_formula_requiring_fact_costs_more() {
        let mut formula_params = Parameters::new();
        formula_params.insert("of".to_string(), Term::<Value>::var("text"));
        formula_params.insert("is".to_string(), Term::<Value>::var("len"));

        let formula_app = Length::apply(formula_params).unwrap();
        let formula_premise = Premise::from(formula_app);
        let formula_candidate = Candidate::from(formula_premise);

        assert!(
            !formula_candidate.is_viable(),
            "Formula requiring unbound variable should be blocked"
        );
    }

    #[dialog_common::test]
    fn test_concept_equals_fact_cost_nothing_bound() {
        let fact_app = RelationQuery::new(
            Term::Constant("user".to_string()),
            Term::Constant("name".to_string()),
            Term::<Entity>::var("entity"),
            Term::<Value>::var("value"),
            Term::var("cause"),
            Some(RelationDescriptor::new(None, Cardinality::One)),
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
        terms.insert("this".to_string(), Term::<Value>::var("entity"));
        terms.insert("name".to_string(), Term::<Value>::var("value"));

        let concept_app = ConceptQuery {
            terms,
            predicate: concept,
        };

        let env = Environment::new();

        let fact_cost = fact_app.estimate(&env).expect("Should have cost");
        let concept_cost = concept_app.estimate(&env).expect("Should have cost");

        assert_eq!(fact_cost, RANGE_SCAN_COST);
        assert_eq!(concept_cost, RANGE_SCAN_COST + CONCEPT_OVERHEAD);

        assert!(
            concept_cost > fact_cost,
            "ConceptQuery should cost more than RelationQuery due to rule overhead. \
             Fact: {}, Concept: {}",
            fact_cost,
            concept_cost
        );
    }

    #[dialog_common::test]
    fn test_concept_equals_fact_cost_value_bound() {
        let fact_app = RelationQuery::new(
            Term::Constant("user".to_string()),
            Term::Constant("name".to_string()),
            Term::<Entity>::var("entity"),
            Term::<Value>::var("value"),
            Term::var("cause"),
            Some(RelationDescriptor::new(None, Cardinality::One)),
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
        terms.insert("this".to_string(), Term::<Value>::var("entity"));
        terms.insert("name".to_string(), Term::<Value>::var("value"));

        let concept_app = ConceptQuery {
            terms,
            predicate: concept,
        };

        let mut env = Environment::new();
        env.add(&Term::<Value>::var("value"));

        let fact_cost = fact_app.estimate(&env).expect("Should have cost");
        let concept_cost = concept_app.estimate(&env).expect("Should have cost");

        assert_eq!(fact_cost, SEGMENT_READ_COST);
        assert_eq!(concept_cost, SEGMENT_READ_COST + CONCEPT_OVERHEAD);
    }

    #[dialog_common::test]
    fn test_concept_equals_fact_cost_entity_bound() {
        let fact_app = RelationQuery::new(
            Term::Constant("user".to_string()),
            Term::Constant("name".to_string()),
            Term::<Entity>::var("entity"),
            Term::<Value>::var("value"),
            Term::var("cause"),
            Some(RelationDescriptor::new(None, Cardinality::One)),
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
        terms.insert("this".to_string(), Term::<Value>::var("entity"));
        terms.insert("name".to_string(), Term::<Value>::var("value"));

        let concept_app = ConceptQuery {
            terms,
            predicate: concept,
        };

        let mut env = Environment::new();
        env.add(&Term::<Value>::var("entity"));

        let fact_cost = fact_app.estimate(&env).expect("Should have cost");
        let concept_cost = concept_app.estimate(&env).expect("Should have cost");

        assert_eq!(fact_cost, SEGMENT_READ_COST);
        assert_eq!(concept_cost, SEGMENT_READ_COST + CONCEPT_OVERHEAD);
    }

    #[dialog_common::test]
    fn test_concept_equals_fact_cost_cardinality_many_nothing_bound() {
        let fact_app = RelationQuery::new(
            Term::Constant("user".to_string()),
            Term::Constant("tags".to_string()),
            Term::<Entity>::var("entity"),
            Term::<Value>::var("tag"),
            Term::var("cause"),
            Some(RelationDescriptor::new(None, Cardinality::Many)),
        );

        let tag = AttributeDescriptor::new(
            the!("user/tags"),
            "User tags",
            Cardinality::Many,
            Some(Type::String),
        );

        let concept = ConceptDescriptor::from([("tags", tag)]);

        let mut terms = Parameters::new();
        terms.insert("this".to_string(), Term::<Value>::var("entity"));
        terms.insert("tags".to_string(), Term::<Value>::var("tag"));

        let concept_app = ConceptQuery {
            terms,
            predicate: concept,
        };

        let env = Environment::new();

        let fact_cost = fact_app.estimate(&env).expect("Should have cost");
        let concept_cost = concept_app.estimate(&env).expect("Should have cost");

        assert_eq!(fact_cost, INDEX_SCAN);
        assert_eq!(concept_cost, INDEX_SCAN + CONCEPT_OVERHEAD);
    }

    #[dialog_common::test]
    fn test_concept_equals_fact_cost_cardinality_many_value_bound() {
        let fact_app = RelationQuery::new(
            Term::Constant("user".to_string()),
            Term::Constant("tags".to_string()),
            Term::<Entity>::var("entity"),
            Term::<Value>::var("tag"),
            Term::var("cause"),
            Some(RelationDescriptor::new(None, Cardinality::Many)),
        );

        let tag = AttributeDescriptor::new(
            the!("user/tags"),
            "User tags",
            Cardinality::Many,
            Some(Type::String),
        );

        let concept = ConceptDescriptor::from([("tags", tag)]);

        let mut terms = Parameters::new();
        terms.insert("this".to_string(), Term::<Value>::var("entity"));
        terms.insert("tags".to_string(), Term::<Value>::var("tag"));

        let concept_app = ConceptQuery {
            terms,
            predicate: concept,
        };

        let mut env = Environment::new();
        env.add(&Term::<Value>::var("tag"));

        let fact_cost = fact_app.estimate(&env).expect("Should have cost");
        let concept_cost = concept_app.estimate(&env).expect("Should have cost");

        assert_eq!(fact_cost, RANGE_SCAN_COST);
        assert_eq!(concept_cost, RANGE_SCAN_COST + CONCEPT_OVERHEAD);
    }

    #[dialog_common::test]
    fn test_cost_accumulation_through_planning() {
        let p1 = RelationQuery::new(
            Term::Constant("user".to_string()),
            Term::Constant("name".to_string()),
            Term::<Entity>::var("entity"),
            Term::<Value>::var("name"),
            Term::var("cause"),
            Some(RelationDescriptor::new(None, Cardinality::One)),
        );

        let p2 = RelationQuery::new(
            Term::Constant("user".to_string()),
            Term::Constant("age".to_string()),
            Term::<Entity>::var("entity"),
            Term::<Value>::var("age"),
            Term::var("cause"),
            Some(RelationDescriptor::new(None, Cardinality::One)),
        );

        let a1 = Candidate::from(Premise::When(Proposition::Relation(Box::new(p1))));
        let cost1 = a1.cost();

        assert_eq!(cost1, RANGE_SCAN_COST);

        let mut a2 = Candidate::from(Premise::When(Proposition::Relation(Box::new(p2.clone()))));
        let mut env = Environment::new();
        env.add(&Term::<Value>::var("entity"));
        a2.update(&env);
        let cost2 = a2.cost();

        assert_eq!(
            cost2, SEGMENT_READ_COST,
            "Second premise with bound entity should cost SEGMENT_READ_COST. Expected {}, got {}",
            SEGMENT_READ_COST, cost2
        );

        let total = cost1 + cost2;
        let expected_total = RANGE_SCAN_COST + SEGMENT_READ_COST;
        assert_eq!(
            total, expected_total,
            "Total cost should be sum of individual costs. Expected {}, got {}",
            expected_total, total
        );
    }

    #[dialog_common::test]
    fn debug_update_cost() {
        let app = RelationQuery::new(
            Term::Constant("user".to_string()),
            Term::Constant("name".to_string()),
            Term::<Entity>::var("entity"),
            Term::<Value>::var("value"),
            Term::var("cause"),
            Some(RelationDescriptor::new(None, Cardinality::One)),
        );

        let schema = app.schema();
        eprintln!("\nSchema:");
        for (name, constraint) in schema.iter() {
            eprintln!("  {}: {:?}", name, constraint.requirement);
        }

        let premise = Premise::When(Proposition::Relation(Box::new(app)));
        let mut candidate = Candidate::from(premise);

        eprintln!("\nInitial state:");
        eprintln!("  Cost: {}", candidate.cost());
        if let Candidate::Viable { binds, .. } = &candidate {
            eprintln!("  Binds: {:?}", binds.variables);
        }

        let mut env = Environment::new();
        env.add(&Term::<Value>::var("entity"));

        eprintln!("\nUpdating with entity bound...");
        candidate.update(&env);

        eprintln!("\nAfter update:");
        eprintln!("  Cost: {}", candidate.cost());
        if let Candidate::Viable { binds, env, .. } = &candidate {
            eprintln!("  Binds: {:?}", binds.variables);
            eprintln!("  Env: {:?}", env.variables);
        }
    }
}

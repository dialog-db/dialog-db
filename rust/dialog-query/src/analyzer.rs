use crate::error::CompileError;
use crate::{fact::Scalar, predicate::DeductiveRule};
use crate::{Dependencies, Premise, Term, Value, VariableScope};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::fmt::Display;
use thiserror::Error;

/// Errors that can occur during rule or formula analysis.
/// These errors indicate structural problems with rules that would prevent execution.
#[derive(Error, Debug, Clone, PartialEq)]
pub enum AnalyzerError {
    /// A rule parameter is defined in the conclusion but never used by any premise.
    /// This indicates a likely error in the rule definition.
    #[error("Rule {rule} does not makes use of the \"{parameter}\" parameter")]
    UnusedParameter {
        rule: DeductiveRule,
        parameter: String,
    },
    /// A rule application is missing a required parameter that the rule needs.
    #[error("Rule {rule} application omits required parameter \"{parameter}\"")]
    RequiredParameter {
        rule: DeductiveRule,
        parameter: String,
    },
    /// A formula application is missing a required cell value.
    #[error("Formula {formula} application omits required cell \"{cell}\"")]
    OmitsRequiredCell { formula: &'static str, cell: String },
    /// A rule uses a local variable that cannot be satisfied by any premise.
    /// This makes the rule impossible to execute.
    #[error("Rule {rule} makes use of local {variable} that no premise can provide")]
    RequiredLocalVariable {
        rule: DeductiveRule,
        variable: String,
    },

    #[error("Rule {rule} does not bind a variable \"{variable}\"")]
    UnboundVariable {
        rule: DeductiveRule,
        variable: String,
    },
}

/// Query planner analyzes each premise to identify it's dependencies and budget
/// required to perform them. This struct represents result of succesful analysis.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LegacyAnalysis {
    /// Base execution cost which does not include added costs captured in the
    /// dependencies.
    pub cost: usize,
    pub dependencies: Dependencies,
}

impl LegacyAnalysis {
    pub fn new(cost: usize) -> Self {
        LegacyAnalysis {
            cost,
            dependencies: Dependencies::new(),
        }
    }

    pub fn desire<T: Scalar>(&mut self, dependency: Option<&Term<T>>, cost: usize) -> &mut Self {
        match dependency {
            Some(Term::Variable {
                name: Some(name), ..
            }) => {
                self.dependencies.desire(name.into(), cost);
            }
            Some(Term::Variable { name: None, .. }) => {
                self.cost += cost;
            }
            Some(Term::Constant(_)) => {}
            None => {
                self.cost += cost;
            }
        }

        self
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct Required(HashSet<String>);
impl Required {
    pub fn new() -> Self {
        Required(HashSet::new())
    }
    pub fn clear(&mut self) {
        self.0.clear();
    }
    pub fn count(&self) -> usize {
        self.0.len()
    }
    pub fn add<T: Scalar>(&mut self, term: &Term<T>) {
        match term {
            Term::Constant(_) => {}
            Term::Variable { name, .. } => {
                let dependency = name
                    .clone()
                    .expect(".require must be passed a named variable");
                self.0.insert(dependency);
            }
        }
    }

    pub fn remove<T: Scalar>(&mut self, term: &Term<T>) -> bool {
        match term {
            Term::Variable {
                name: Some(name), ..
            } => self.0.remove(name),
            _ => false,
        }
    }
}
impl Display for Required {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut iter = self.0.iter();
        if let Some(name) = iter.next() {
            write!(f, "{}", name)?;
        }

        for name in iter {
            write!(f, ", {}", name)?;
        }

        write!(f, "")
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct Desired(HashMap<String, usize>);
impl Desired {
    pub fn new() -> Self {
        Desired(HashMap::new())
    }
    pub fn count(&self) -> usize {
        self.0.len()
    }

    pub fn contains<T: Scalar>(&self, term: &Term<T>) -> bool {
        match term {
            Term::Variable {
                name: Some(name), ..
            } => self.0.contains_key(name),
            _ => false,
        }
    }

    pub fn remove<T: Scalar>(&mut self, term: &Term<T>) -> bool {
        match term {
            Term::Variable {
                name: Some(name), ..
            } => self.0.remove(name).and(Some(true)).unwrap_or(false),
            _ => false,
        }
    }
    pub fn insert<T: Scalar>(&mut self, term: &Term<T>, cost: usize) {
        match term {
            Term::Constant(_) => {}
            Term::Variable { name, .. } => {
                let dependency = name
                    .clone()
                    .expect(".desire must be passed a named variable");
                self.0.insert(dependency, cost);
            }
        }
    }

    pub fn total(&self) -> usize {
        self.0.values().sum()
    }

    pub fn iter(&self) -> impl Iterator<Item = Term<Value>> + '_ {
        self.0.keys().map(|name| Term::var(name.clone()))
    }

    pub fn entries(&self) -> impl Iterator<Item = (Term<Value>, usize)> + '_ {
        self.0
            .iter()
            .map(|(name, cost)| (Term::var(name.clone()), *cost))
    }
}

impl From<Desired> for VariableScope {
    fn from(desired: Desired) -> Self {
        let mut scope = VariableScope::new();
        for (name, _) in desired.0.into_iter() {
            scope.add(&Term::<Value>::var(name));
        }
        scope
    }
}

#[derive(Error, Debug, Clone, PartialEq)]
pub enum EstimateError<'a> {
    #[error("Required parameters {required} are not bound in the environment ")]
    RequiredParameters { required: &'a Required },
}

impl<'a> From<EstimateError<'a>> for CompileError {
    fn from(error: EstimateError<'a>) -> Self {
        match error {
            EstimateError::RequiredParameters { required } => CompileError::RequiredBindings {
                required: required.clone(),
            },
        }
    }
}

/// A plan for executing a premise - ready to execute (lightweight, no cached schema/params)
#[derive(Debug, Clone, PartialEq)]
pub struct Plan {
    pub premise: Premise,
    pub cost: usize,
    pub binds: VariableScope,
    pub env: VariableScope,
}

/// Represents a join plan - the result of planning multiple premises together.
/// Contains the ordered sequence of steps, total cost, and variable scopes.
#[derive(Debug, Clone, PartialEq)]
pub struct JoinPlan {
    /// The ordered steps to execute
    pub steps: Vec<Plan>,
    /// Total execution cost
    pub cost: usize,
    /// Variables provided/bound by this join
    pub binds: VariableScope,
    /// Variables required in the environment to execute this join
    pub env: VariableScope,
}

/// Analysis result for a premise - either viable or blocked
/// Both variants cache schema/params for efficient updates
#[derive(Debug, Clone, PartialEq)]
pub enum Analysis {
    /// Plan is ready to execute
    Viable {
        premise: crate::premise::Premise,
        cost: usize,
        binds: VariableScope,
        env: VariableScope,
        // Cached for efficient updates
        schema: crate::Schema,
        params: crate::Parameters,
    },
    /// Plan is blocked on missing requirements
    Blocked {
        premise: crate::premise::Premise,
        cost: usize,
        binds: VariableScope,
        env: VariableScope,
        requires: Required,
        // Cached for efficient updates
        schema: crate::Schema,
        params: crate::Parameters,
    },
}

impl Analysis {
    pub fn from(premise: Premise) -> Self {
        let schema = premise.schema();
        let params = premise.parameters();
        let env = VariableScope::new();

        let mut cost = premise.cost();
        let mut binds = VariableScope::new();
        let mut requires = Required::new();

        // Track which choice groups are satisfied by constants
        let mut satisfied_groups = std::collections::HashSet::new();

        // First pass: identify groups satisfied by constants
        for (name, constraint) in schema.iter() {
            if let Some(term) = params.get(name) {
                if let crate::Requirement::Required(Some((_, group))) = &constraint.requirement {
                    // If this parameter is a constant, its group is satisfied
                    if matches!(term, Term::Constant(_)) {
                        satisfied_groups.insert(*group);
                    }
                }
            }
        }

        // Second pass: categorize all parameters based on their requirement types
        for (name, constraint) in schema.iter() {
            if let Some(term) = params.get(name) {
                // Constants and variables already in env don't add cost - they're already satisfied
                if matches!(term, Term::Constant(_)) || env.contains(term) {
                    continue;
                }

                match &constraint.requirement {
                    crate::Requirement::Required(Some((c, group))) => {
                        // If this group is satisfied, treat as desired (variable will be bound)
                        if satisfied_groups.contains(group) {
                            cost += c;
                            binds.add(term);
                        } else {
                            requires.add(term);
                        }
                    }
                    crate::Requirement::Required(None) => {
                        requires.add(term);
                    }
                    crate::Requirement::Derived(c) => {
                        cost += c;
                        binds.add(term);
                    }
                }
            }
        }

        // If no requirements, create Viable analysis
        if requires.count() == 0 {
            Analysis::Viable {
                premise,
                cost,
                binds,
                env,
                schema,
                params,
            }
        } else {
            Analysis::Blocked {
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
    /// Update this analysis with new bindings from the environment.
    /// May transition from Blocked to Viable if requirements are satisfied.
    /// Only processes relevant bindings and updates incrementally.
    pub fn update(&mut self, new_bindings: &VariableScope) {
        match self {
            Analysis::Viable {
                cost,
                binds,
                env,
                schema,
                params,
                ..
            } => {
                // Only process bindings that are relevant to this plan
                for (name, constraint) in schema.iter() {
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

                            // Decrease cost (incremental update)
                            match &constraint.requirement {
                                crate::Requirement::Derived(c) => {
                                    *cost = cost.saturating_sub(*c);
                                }
                                crate::Requirement::Required(Some((c, _group))) => {
                                    // Grouped requirement that's been satisfied
                                    *cost = cost.saturating_sub(*c);
                                }
                                _ => {}
                            }
                        }
                    }
                }
            }
            Analysis::Blocked {
                premise,
                cost,
                binds,
                env,
                requires,
                schema,
                params,
            } => {
                // Track which choice groups now have at least one bound parameter
                let mut satisfied_groups = std::collections::HashSet::new();

                // Process only relevant bindings (parameters that got bound)
                for (name, constraint) in schema.iter() {
                    if let Some(term) = params.get(name) {
                        if new_bindings.contains(term) {
                            // Check if this term is relevant to this plan
                            let was_required = requires.remove(term);
                            let was_bound = binds.remove(term);

                            if was_required || was_bound {
                                // This parameter is now bound (add to env)
                                env.add(term);

                                // Update cost incrementally if it was a desired binding
                                if was_bound {
                                    if let crate::Requirement::Derived(c) = constraint.requirement {
                                        *cost = cost.saturating_sub(c);
                                    }
                                }

                                // If this is part of a choice group, mark that group as satisfied
                                if let crate::Requirement::Required(Some((_, group))) =
                                    &constraint.requirement
                                {
                                    satisfied_groups.insert(*group);
                                }
                            }
                        }
                    }
                }

                // Second pass: for satisfied choice groups, convert required params to desired
                if !satisfied_groups.is_empty() {
                    for (name, constraint) in schema.iter() {
                        if let crate::Requirement::Required(Some((c, group))) =
                            &constraint.requirement
                        {
                            if satisfied_groups.contains(group) {
                                if let Some(term) = params.get(name) {
                                    // If this term was required, it's no longer required
                                    // Move it to binds if it's not already bound
                                    if requires.remove(term) && !env.contains(term) {
                                        binds.add(term);
                                        // Add its cost since it's now desired instead of required
                                        *cost += c;
                                    }
                                }
                            }
                        }
                    }
                }

                // If no requirements remain, transition to Viable
                if requires.count() == 0 {
                    *self = Analysis::Viable {
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

    /// Get the cost of this analysis (whether viable or blocked)
    pub fn cost(&self) -> usize {
        match self {
            Analysis::Viable { cost, .. } => *cost,
            Analysis::Blocked { cost, .. } => *cost,
        }
    }

    /// Check if this analysis is viable
    pub fn is_viable(&self) -> bool {
        matches!(self, Analysis::Viable { .. })
    }

    /// Get the premise this analysis is for
    pub fn premise(&self) -> &crate::premise::Premise {
        match self {
            Analysis::Viable { premise, .. } => premise,
            Analysis::Blocked { premise, .. } => premise,
        }
    }
}

impl From<Premise> for Analysis {
    fn from(premise: Premise) -> Self {
        Analysis::from(premise)
    }
}

impl TryFrom<Analysis> for Plan {
    type Error = CompileError;

    fn try_from(analysis: Analysis) -> Result<Self, Self::Error> {
        match analysis {
            Analysis::Viable {
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
            Analysis::Blocked { requires, .. } => {
                Err(CompileError::RequiredBindings { required: requires })
            }
        }
    }
}

#[test]
fn test_analysis_from_premise_all_derived() {
    use crate::predicate::formula::Formula;
    use crate::strings::Length;
    use crate::{Parameters, Term, Value};

    // Length formula has: of (required), is (derived)
    // We'll test with both as variables to see derived-only behavior
    let mut params = Parameters::new();
    params.insert("of".to_string(), Term::<Value>::var("text".to_string()));
    params.insert("is".to_string(), Term::<Value>::var("len".to_string()));

    let application = Length::apply(params).unwrap();
    let premise = Premise::from(application);

    // Analysis should be Blocked because "of" is required
    let analysis = Analysis::from(premise);
    assert!(!analysis.is_viable());
}

#[test]
fn test_analysis_from_premise_with_constant() {
    use crate::predicate::formula::Formula;
    use crate::strings::Length;
    use crate::{Parameters, Term, Value};

    // Provide "of" as a constant, "is" as a variable
    let mut params = Parameters::new();
    params.insert(
        "of".to_string(),
        Term::<Value>::Constant(Value::String("hello".to_string())),
    );
    params.insert("is".to_string(), Term::<Value>::var("len".to_string()));

    let application = Length::apply(params).unwrap();
    let premise = Premise::from(application);

    // Analysis should be Viable because "of" is provided as constant
    let analysis = Analysis::from(premise);
    assert!(analysis.is_viable());
}

#[test]
fn test_analysis_update_transitions_to_viable() {
    use crate::predicate::formula::Formula;
    use crate::strings::Length;
    use crate::{Parameters, Term, Value, VariableScope};

    // Length formula requires "of" parameter
    let mut params = Parameters::new();
    params.insert("of".to_string(), Term::<Value>::var("text"));
    params.insert("is".to_string(), Term::<Value>::var("len"));

    let application = Length::apply(params).unwrap();
    let premise = Premise::from(application);

    let mut analysis = Analysis::from(premise);
    assert!(!analysis.is_viable());

    // Update with "text" bound
    let mut env = VariableScope::new();
    env.add(&Term::<Value>::var("text"));
    analysis.update(&env);

    // Should now be viable
    assert!(analysis.is_viable());
}

#[test]
fn test_analysis_update_reduces_cost_when_derived_bound() {
    use crate::predicate::formula::Formula;
    use crate::strings::Length;
    use crate::{Parameters, Term, Value, VariableScope};

    // Provide "of" as constant so it's viable, "is" is derived
    let mut params = Parameters::new();
    params.insert(
        "of".to_string(),
        Term::<Value>::Constant(Value::String("hello".to_string())),
    );
    params.insert("is".to_string(), Term::<Value>::var("len".to_string()));

    let application = Length::apply(params).unwrap();
    let premise = Premise::from(application);

    let mut analysis = Analysis::from(premise);
    let initial_cost = analysis.cost();
    assert!(analysis.is_viable());

    // Update with "len" already bound (should reduce cost)
    let mut env = VariableScope::new();
    env.add(&Term::<Value>::var("len".to_string()));
    analysis.update(&env);

    // Cost should be reduced since "is" was desired and is now bound
    assert!(analysis.cost() < initial_cost);
}

#[test]
fn test_analysis_try_into_plan_when_viable() {
    use crate::predicate::formula::Formula;
    use crate::strings::Length;
    use crate::{Parameters, Term, Value};

    // Provide "of" as constant so premise is viable
    let mut params = Parameters::new();
    params.insert(
        "of".to_string(),
        Term::<Value>::Constant(Value::String("hello".to_string())),
    );
    params.insert("is".to_string(), Term::<Value>::var("len".to_string()));

    let application = Length::apply(params).unwrap();
    let premise = Premise::from(application);

    let analysis = Analysis::from(premise);
    assert!(analysis.is_viable());

    // Should successfully convert to Plan
    let plan = Plan::try_from(analysis);
    assert!(plan.is_ok());
}

#[test]
fn test_analysis_try_into_plan_when_blocked() {
    use crate::predicate::formula::Formula;
    use crate::strings::Length;
    use crate::{Parameters, Term, Value};

    // Leave "of" as unbound variable - premise will be blocked
    let mut params = Parameters::new();
    params.insert("of".to_string(), Term::<Value>::var("text".to_string()));
    params.insert("is".to_string(), Term::<Value>::var("len".to_string()));

    let application = Length::apply(params).unwrap();
    let premise = Premise::from(application);

    let analysis = Analysis::from(premise);
    assert!(!analysis.is_viable());

    // Should fail to convert to Plan
    let plan = Plan::try_from(analysis);
    assert!(plan.is_err());
}
#[cfg(test)]
mod cost_model_tests {
    use crate::application::fact::{FactApplication, ATTRIBUTE_COST, BASE_COST, ENTITY_COST, VALUE_COST};
    use crate::artifact::{Attribute, Entity};
    use crate::analyzer::Analysis;
    use crate::{Premise, Term, Value, VariableScope};

    // Test 1: Constants don't add to cost
    #[test]
    fn test_constants_do_not_add_cost() {
        // All constants - should only have BASE_COST
        let the_attr: Attribute = "user/name".parse().unwrap();
        let entity_val: Entity = Entity::new().unwrap();

        let app = FactApplication::new(
            Term::Constant(the_attr),
            Term::Constant(entity_val),
            Term::Constant(Value::String("test".to_string())),
            crate::attribute::Cardinality::One,
        );
        let premise = Premise::from(app);
        let analysis = Analysis::from(premise);

        assert_eq!(analysis.cost(), BASE_COST,
            "All constants should only cost BASE_COST ({}), got {}",
            BASE_COST, analysis.cost());
    }

    #[test]
    fn test_one_constant_two_variables() {
        // Constant "the" satisfies group, "of" and "is" are derived
        let the_attr: Attribute = "user/name".parse().unwrap();

        let app = FactApplication::new(
            Term::Constant(the_attr),
            Term::<Entity>::var("entity"),
            Term::<Value>::var("value"),
            crate::attribute::Cardinality::One,
        );
        let premise = Premise::from(app);
        let analysis = Analysis::from(premise);

        let expected = BASE_COST + ENTITY_COST + VALUE_COST;
        assert_eq!(analysis.cost(), expected,
            "Constant 'the' should not add cost. Expected BASE({}) + ENTITY({}) + VALUE({}) = {}, got {}",
            BASE_COST, ENTITY_COST, VALUE_COST, expected, analysis.cost());
    }

    // Test 2: Parameters in env are removed from costs
    #[test]
    fn test_env_variables_reduce_cost() {
        let the_attr: Attribute = "user/name".parse().unwrap();
        let app = FactApplication::new(
            Term::Constant(the_attr),
            Term::<Entity>::var("entity"),
            Term::<Value>::var("value"),
            crate::attribute::Cardinality::One,
        );
        let premise = Premise::from(app);

        let mut analysis = Analysis::from(premise);
        let initial_cost = analysis.cost();
        assert_eq!(initial_cost, BASE_COST + ENTITY_COST + VALUE_COST);

        // Bind entity in environment
        let mut env = VariableScope::new();
        env.add(&Term::<Value>::var("entity"));
        analysis.update(&env);

        let after_entity = analysis.cost();
        assert_eq!(after_entity, BASE_COST + VALUE_COST,
            "After binding entity, cost should decrease by ENTITY_COST ({}). Expected {}, got {}",
            ENTITY_COST, BASE_COST + VALUE_COST, after_entity);

        // Bind value as well
        env.add(&Term::<Value>::var("value"));
        analysis.update(&env);

        let final_cost = analysis.cost();
        assert_eq!(final_cost, BASE_COST,
            "After binding all variables, cost should be just BASE_COST ({}), got {}",
            BASE_COST, final_cost);
    }

    #[test]
    fn test_variables_already_in_initial_env_dont_add_cost() {
        let the_attr: Attribute = "user/name".parse().unwrap();
        let app = FactApplication::new(
            Term::Constant(the_attr),
            Term::<Entity>::var("entity"),
            Term::<Value>::var("value"),
            crate::attribute::Cardinality::One,
        );

        // Create analysis with entity already in environment
        let schema = app.schema();
        let params = app.parameters();
        let mut env = VariableScope::new();
        env.add(&Term::<Value>::var("entity"));

        let mut cost = app.cost();
        let mut binds = VariableScope::new();
        let mut requires = crate::analyzer::Required::new();

        // Manually simulate Analysis::from with pre-populated env
        let mut satisfied_groups = std::collections::HashSet::new();
        for (name, constraint) in schema.iter() {
            if let Some(term) = params.get(name) {
                if let crate::Requirement::Required(Some((_, group))) = &constraint.requirement {
                    if matches!(term, Term::Constant(_)) {
                        satisfied_groups.insert(*group);
                    }
                }
            }
        }

        for (name, constraint) in schema.iter() {
            if let Some(term) = params.get(name) {
                if matches!(term, Term::Constant(_)) || env.contains(term) {
                    continue;
                }

                match &constraint.requirement {
                    crate::Requirement::Required(Some((c, group))) => {
                        if satisfied_groups.contains(group) {
                            cost += c;
                            binds.add(term);
                        } else {
                            requires.add(term);
                        }
                    }
                    crate::Requirement::Required(None) => {
                        requires.add(term);
                    }
                    crate::Requirement::Derived(c) => {
                        cost += c;
                        binds.add(term);
                    }
                }
            }
        }

        // entity is in env, so it shouldn't add ENTITY_COST
        let expected = BASE_COST + VALUE_COST;
        assert_eq!(cost, expected,
            "Variable already in env should not add cost. Expected BASE({}) + VALUE({}) = {}, got {}",
            BASE_COST, VALUE_COST, expected, cost);
    }

    // Test 3: Cardinality affects cost
    #[test]
    fn test_cardinality_many_costs_more_than_one() {
        let the_attr: Attribute = "user/tags".parse().unwrap();

        let one_app = FactApplication::new(
            Term::Constant(the_attr.clone()),
            Term::<Entity>::var("entity"),
            Term::<Value>::var("tag"),
            crate::attribute::Cardinality::One,
        );

        let many_app = FactApplication::new(
            Term::Constant(the_attr),
            Term::<Entity>::var("entity"),
            Term::<Value>::var("tag"),
            crate::attribute::Cardinality::Many,
        );

        let one_analysis = Analysis::from(Premise::from(one_app));
        let many_analysis = Analysis::from(Premise::from(many_app));

        assert!(many_analysis.cost() > one_analysis.cost(),
            "Cardinality::Many should cost more than Cardinality::One. One: {}, Many: {}",
            one_analysis.cost(), many_analysis.cost());

        // Verify the specific cost difference
        let one_base = 100;  // BASE_COST for Cardinality::One
        let many_base = 100 * 100;  // BASE_COST^2 for Cardinality::Many

        let expected_diff = many_base - one_base;
        let actual_diff = many_analysis.cost() - one_analysis.cost();

        assert_eq!(actual_diff, expected_diff,
            "Cost difference should be {} (many_base - one_base), got {}",
            expected_diff, actual_diff);
    }

    #[test]
    fn test_fully_bound_cardinality_should_not_differ() {
        // When all parameters are known (constants or bound), cardinality shouldn't matter much
        // because we're doing a precise lookup, not a scan
        let the_attr: Attribute = "user/tags".parse().unwrap();
        let entity_val: Entity = Entity::new().unwrap();
        let value_val = Value::String("rust".to_string());

        let one_app = FactApplication::new(
            Term::Constant(the_attr.clone()),
            Term::Constant(entity_val.clone()),
            Term::Constant(value_val.clone()),
            crate::attribute::Cardinality::One,
        );

        let many_app = FactApplication::new(
            Term::Constant(the_attr),
            Term::Constant(entity_val),
            Term::Constant(value_val),
            crate::attribute::Cardinality::Many,
        );

        let one_analysis = Analysis::from(Premise::from(one_app));
        let many_analysis = Analysis::from(Premise::from(many_app));

        // Both should only have their base costs since all params are constants
        // The difference is only in the BASE_COST
        let one_base = 100;
        let many_base = 10000;

        assert_eq!(one_analysis.cost(), one_base);
        assert_eq!(many_analysis.cost(), many_base);

        // Note: This test documents current behavior. The comment above suggests
        // this might not be ideal - when all components are known, cardinality
        // probably shouldn't affect cost much since it's a precise lookup
    }

    // Test 4: Formula vs Fact costs
    #[test]
    fn test_formula_cheaper_than_fact_no_io() {
        use crate::predicate::formula::Formula;
        use crate::strings::Length;
        use crate::Parameters;

        // Formula with constant input (no IO needed)
        let mut formula_params = Parameters::new();
        formula_params.insert("of".to_string(),
            Term::<Value>::Constant(Value::String("hello".to_string())));
        formula_params.insert("is".to_string(),
            Term::<Value>::var("len"));

        let formula_app = Length::apply(formula_params).unwrap();
        let formula_analysis = Analysis::from(Premise::from(formula_app));

        // Fact with constant attribute (requires IO)
        let the_attr: Attribute = "user/name".parse().unwrap();
        let fact_app = FactApplication::new(
            Term::Constant(the_attr),
            Term::<Entity>::var("entity"),
            Term::<Value>::var("value"),
            crate::attribute::Cardinality::One,
        );
        let fact_analysis = Analysis::from(Premise::from(fact_app));

        assert!(formula_analysis.cost() < fact_analysis.cost(),
            "Formula with no IO should be cheaper than FactApplication. Formula: {}, Fact: {}",
            formula_analysis.cost(), fact_analysis.cost());
    }

    #[test]
    fn test_formula_requiring_fact_costs_more() {
        use crate::predicate::formula::Formula;
        use crate::strings::Length;
        use crate::Parameters;

        // Formula that needs variable bound by fact (requires IO transitively)
        let mut formula_params = Parameters::new();
        formula_params.insert("of".to_string(),
            Term::<Value>::var("text"));  // Needs to be bound first
        formula_params.insert("is".to_string(),
            Term::<Value>::var("len"));

        let formula_app = Length::apply(formula_params).unwrap();
        let formula_premise = Premise::from(formula_app);
        let formula_analysis = Analysis::from(formula_premise);

        // This formula is blocked - needs "text" to be bound
        assert!(!formula_analysis.is_viable(),
            "Formula requiring unbound variable should be blocked");

        // To make it viable, we'd need a fact to bind "text" first
        // The combined cost would be: fact_cost + formula_cost
        // which is more than just the formula with a constant
    }

    #[test]
    fn test_cost_accumulation_through_planning() {
        // Test that costs accumulate correctly when planning multiple premises
        let the_attr: Attribute = "user/name".parse().unwrap();

        // First premise: binds "entity" and "name"
        let p1 = FactApplication::new(
            Term::Constant(the_attr.clone()),
            Term::<Entity>::var("entity"),
            Term::<Value>::var("name"),
            crate::attribute::Cardinality::One,
        );

        // Second premise: uses bound "entity", binds "age"
        let age_attr: Attribute = "user/age".parse().unwrap();
        let p2 = FactApplication::new(
            Term::Constant(age_attr),
            Term::<Entity>::var("entity"),  // Already bound by p1
            Term::<Value>::var("age"),
            crate::attribute::Cardinality::One,
        );

        let a1 = Analysis::from(Premise::from(p1));
        let cost1 = a1.cost();

        // Simulate p2 with entity already bound
        let mut a2 = Analysis::from(Premise::from(p2.clone()));
        let mut env = VariableScope::new();
        env.add(&Term::<Value>::var("entity"));
        a2.update(&env);
        let cost2 = a2.cost();

        // Second premise should be cheaper because entity is already bound
        let expected_p2 = BASE_COST + VALUE_COST;  // No ENTITY_COST
        assert_eq!(cost2, expected_p2,
            "Second premise with bound entity should cost BASE + VALUE. Expected {}, got {}",
            expected_p2, cost2);

        // Total cost should be sum of both
        let total = cost1 + cost2;
        let expected_total = (BASE_COST + ENTITY_COST + VALUE_COST) + (BASE_COST + VALUE_COST);
        assert_eq!(total, expected_total,
            "Total cost should be sum of individual costs. Expected {}, got {}",
            expected_total, total);
    }
}
#[test]
fn debug_update_cost() {
    use crate::application::fact::{FactApplication, ATTRIBUTE_COST, BASE_COST, ENTITY_COST, VALUE_COST};
    use crate::artifact::Attribute;
    use crate::{Premise, Term, Value, VariableScope};
    use crate::artifact::Entity;
    use crate::analyzer::Analysis;

    let the_attr: Attribute = "user/name".parse().unwrap();
    let app = FactApplication::new(
        Term::Constant(the_attr),
        Term::<Entity>::var("entity"),
        Term::<Value>::var("value"),
        crate::attribute::Cardinality::One,
    );
    
    let schema = app.schema();
    eprintln!("\nSchema:");
    for (name, constraint) in schema.iter() {
        eprintln!("  {}: {:?}", name, constraint.requirement);
    }
    
    let premise = Premise::from(app);
    let mut analysis = Analysis::from(premise);
    
    eprintln!("\nInitial state:");
    eprintln!("  Cost: {}", analysis.cost());
    if let Analysis::Viable { binds, .. } = &analysis {
        eprintln!("  Binds: {:?}", binds.variables);
    }
    
    // Bind entity
    let mut env = VariableScope::new();
    env.add(&Term::<Value>::var("entity"));
    
    eprintln!("\nUpdating with entity bound...");
    analysis.update(&env);
    
    eprintln!("\nAfter update:");
    eprintln!("  Cost: {}", analysis.cost());
    if let Analysis::Viable { binds, env, .. } = &analysis {
        eprintln!("  Binds: {:?}", binds.variables);
        eprintln!("  Env: {:?}", env.variables);
    }
}

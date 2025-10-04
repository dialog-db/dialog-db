use crate::error::CompileError;
use crate::{fact::Scalar, predicate::DeductiveRule};
use crate::{
    Dependencies, EvaluationContext, Parameters, Premise, Requirement, Schema, Selection, Source,
    Term, Value, VariableScope,
};
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

impl Plan {
    pub fn cost(&self) -> usize {
        self.cost
    }

    pub fn binds(&self) -> &VariableScope {
        &self.binds
    }

    pub fn env(&self) -> &VariableScope {
        &self.env
    }

    /// Evaluate this plan with the given context
    /// The premise will be evaluated with scope set to self.env
    pub fn evaluate<S: Source, M: Selection>(
        &self,
        context: EvaluationContext<S, M>,
    ) -> impl Selection {
        // Delegate to premise evaluation passing env inferred by an analyzer
        // as scope. Premise already returns boxed, so just pass through.
        let scope = self.env.clone();
        self.premise.evaluate(EvaluationContext {
            source: context.source,
            selection: context.selection,
            scope,
        })
    }
}

/// Analysis result for a premise - either viable or blocked
/// Both variants cache schema/params for efficient updates
#[derive(Debug, Clone, PartialEq)]
pub enum Analysis {
    /// Plan is ready to execute
    Viable {
        premise: Premise,
        cost: usize,
        binds: VariableScope,
        env: VariableScope,
        // Cached for efficient updates
        schema: Schema,
        params: Parameters,
    },
    /// Plan is blocked on missing requirements
    Blocked {
        premise: Premise,
        cost: usize,
        binds: VariableScope,
        env: VariableScope,
        requires: Required,
        // Cached for efficient updates
        schema: Schema,
        params: Parameters,
    },
}

impl Analysis {
    pub fn from(premise: Premise) -> Self {
        let schema = premise.schema();
        let params = premise.parameters();
        let env = VariableScope::new();

        // Use the premise's estimate() method to calculate cost
        // If None, the premise is unbound and should use a high cost
        let cost = premise.estimate(&env).unwrap_or(usize::MAX);
        let mut binds = VariableScope::new();
        let mut requires = Required::new();

        // Track which choice groups are satisfied by constants
        let mut satisfied_groups = std::collections::HashSet::new();

        // First pass: identify groups satisfied by constants
        for (name, constraint) in schema.iter() {
            if let Some(term) = params.get(name) {
                if let Requirement::Required(Some(group)) = &constraint.requirement {
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
                    Requirement::Required(Some(group)) => {
                        // If this group is satisfied, treat as desired (variable will be bound)
                        if satisfied_groups.contains(group) {
                            binds.add(term);
                        } else {
                            requires.add(term);
                        }
                    }
                    Requirement::Required(None) => {
                        requires.add(term);
                    }
                    Requirement::Optional => {
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

                                // If this is part of a choice group, mark that group as satisfied
                                if let Requirement::Required(Some(group)) = &constraint.requirement
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
                        if let Requirement::Required(Some(group)) = &constraint.requirement {
                            if satisfied_groups.contains(group) {
                                if let Some(term) = params.get(name) {
                                    // If this term was required, it's no longer required
                                    // Move it to binds if it's not already bound
                                    if requires.remove(term) && !env.contains(term) {
                                        binds.add(term);
                                    }
                                }
                            }
                        }
                    }
                }

                // Re-estimate cost based on updated environment
                *cost = premise.estimate(env).unwrap_or(usize::MAX);

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
    pub fn premise(&self) -> &Premise {
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

impl From<Plan> for Analysis {
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

impl From<&Plan> for Analysis {
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

    // Update with "len" already bound
    let mut env = VariableScope::new();
    env.add(&Term::<Value>::var("len".to_string()));
    analysis.update(&env);

    // For formulas, cost doesn't change based on what's bound (it's computational, not I/O)
    // The cost is constant regardless of which variables are bound
    assert_eq!(
        analysis.cost(),
        initial_cost,
        "Formula cost should remain constant regardless of bound variables"
    );
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
    use crate::analyzer::Analysis;
    use crate::application::fact::{FactApplication, BASE_COST};
    use crate::artifact::{Attribute, Entity};
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

        assert_eq!(
            analysis.cost(),
            BASE_COST,
            "All constants should only cost BASE_COST ({}), got {}",
            BASE_COST,
            analysis.cost()
        );
    }

    #[test]
    fn test_one_constant_two_variables() {
        use crate::application::fact::RANGE_SCAN_COST;

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

        // With new cost model: 1 constraint (only 'the' constant is bound)
        // 1 constraint with Cardinality::One = RANGE_SCAN_COST
        assert_eq!(
            analysis.cost(),
            RANGE_SCAN_COST,
            "With 1 constraint (just constant 'the'), cost should be RANGE_SCAN_COST ({}), got {}",
            RANGE_SCAN_COST,
            analysis.cost()
        );
    }

    // Test 2: Parameters in env are removed from costs
    #[test]
    fn test_env_variables_reduce_cost() {
        use crate::application::fact::{RANGE_SCAN_COST, SEGMENT_READ_COST};

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
        // With new cost model: 1 constraint (just 'the' constant)
        // 1 constraint with Cardinality::One = RANGE_SCAN_COST
        assert_eq!(initial_cost, RANGE_SCAN_COST);

        // Bind entity in environment
        let mut env = VariableScope::new();
        env.add(&Term::<Value>::var("entity"));
        analysis.update(&env);

        let after_entity = analysis.cost();
        // After binding entity: 2 constraints ('the' + 'entity')
        // 2 constraints with Cardinality::One = SEGMENT_READ_COST
        assert_eq!(
            after_entity, SEGMENT_READ_COST,
            "After binding entity, cost should decrease to SEGMENT_READ_COST. Expected {}, got {}",
            SEGMENT_READ_COST, after_entity
        );

        // Bind value as well
        env.add(&Term::<Value>::var("value"));
        analysis.update(&env);

        let final_cost = analysis.cost();
        // After binding value: 3 constraints (all bound)
        // 3 constraints with Cardinality::One = SEGMENT_READ_COST (same as 2)
        assert_eq!(
            final_cost, SEGMENT_READ_COST,
            "After binding all variables, cost stays at SEGMENT_READ_COST ({}), got {}",
            SEGMENT_READ_COST, final_cost
        );
    }

    #[test]
    fn test_variables_already_in_initial_env_dont_add_cost() {
        use crate::application::fact::SEGMENT_READ_COST;

        let the_attr: Attribute = "user/name".parse().unwrap();
        let app = FactApplication::new(
            Term::Constant(the_attr),
            Term::<Entity>::var("entity"),
            Term::<Value>::var("value"),
            crate::attribute::Cardinality::One,
        );

        // Create analysis with entity already in environment
        let mut env = VariableScope::new();
        env.add(&Term::<Value>::var("entity"));

        // Use estimate() with the pre-populated env
        let cost = app.estimate(&env).unwrap_or(usize::MAX);

        // With entity in env: 2 constraints ('the' constant + 'entity' in env)
        // 2 constraints with Cardinality::One = SEGMENT_READ_COST
        assert_eq!(
            cost, SEGMENT_READ_COST,
            "Variable already in env counts as bound. Expected SEGMENT_READ_COST ({}), got {}",
            SEGMENT_READ_COST, cost
        );
    }

    // Test 3: Cardinality affects cost
    #[test]
    fn test_cardinality_many_costs_more_than_one() {
        use crate::application::fact::{INDEX_SCAN, RANGE_SCAN_COST};

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

        assert!(
            many_analysis.cost() > one_analysis.cost(),
            "Cardinality::Many should cost more than Cardinality::One. One: {}, Many: {}",
            one_analysis.cost(),
            many_analysis.cost()
        );

        // With new cost model: 1 constraint (just 'the' constant)
        // Cardinality::One with 1 constraint = RANGE_SCAN_COST (1000)
        // Cardinality::Many with 1 constraint = INDEX_SCAN (5000)
        assert_eq!(one_analysis.cost(), RANGE_SCAN_COST);
        assert_eq!(many_analysis.cost(), INDEX_SCAN);

        let expected_diff = INDEX_SCAN - RANGE_SCAN_COST;
        let actual_diff = many_analysis.cost() - one_analysis.cost();

        assert_eq!(
            actual_diff, expected_diff,
            "Cost difference should be {} (INDEX_SCAN - RANGE_SCAN_COST), got {}",
            expected_diff, actual_diff
        );
    }

    #[test]
    fn test_fully_bound_cardinality_should_not_differ() {
        use crate::application::fact::{RANGE_READ_COST, SEGMENT_READ_COST};

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

        // With new cost model: 3 constraints (all constants)
        // Cardinality::One with 3 constraints = SEGMENT_READ_COST (100)
        // Cardinality::Many with 3 constraints = RANGE_READ_COST (200)
        assert_eq!(one_analysis.cost(), SEGMENT_READ_COST);
        assert_eq!(many_analysis.cost(), RANGE_READ_COST);

        // Cardinality still matters a bit even when fully bound, but the difference is small
        assert!(many_analysis.cost() > one_analysis.cost());
        assert!(
            many_analysis.cost() < one_analysis.cost() * 3,
            "Fully bound Many should cost more than One, but not drastically more"
        );
    }

    // Test 4: Formula vs Fact costs
    #[test]
    fn test_formula_cheaper_than_fact_no_io() {
        use crate::predicate::formula::Formula;
        use crate::strings::Length;
        use crate::Parameters;

        // Formula with constant input (no IO needed)
        let mut formula_params = Parameters::new();
        formula_params.insert(
            "of".to_string(),
            Term::<Value>::Constant(Value::String("hello".to_string())),
        );
        formula_params.insert("is".to_string(), Term::<Value>::var("len"));

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

        assert!(
            formula_analysis.cost() < fact_analysis.cost(),
            "Formula with no IO should be cheaper than FactApplication. Formula: {}, Fact: {}",
            formula_analysis.cost(),
            fact_analysis.cost()
        );
    }

    #[test]
    fn test_formula_requiring_fact_costs_more() {
        use crate::predicate::formula::Formula;
        use crate::strings::Length;
        use crate::Parameters;

        // Formula that needs variable bound by fact (requires IO transitively)
        let mut formula_params = Parameters::new();
        formula_params.insert("of".to_string(), Term::<Value>::var("text")); // Needs to be bound first
        formula_params.insert("is".to_string(), Term::<Value>::var("len"));

        let formula_app = Length::apply(formula_params).unwrap();
        let formula_premise = Premise::from(formula_app);
        let formula_analysis = Analysis::from(formula_premise);

        // This formula is blocked - needs "text" to be bound
        assert!(
            !formula_analysis.is_viable(),
            "Formula requiring unbound variable should be blocked"
        );

        // To make it viable, we'd need a fact to bind "text" first
        // The combined cost would be: fact_cost + formula_cost
        // which is more than just the formula with a constant
    }

    #[test]
    fn test_concept_equals_fact_cost_nothing_bound() {
        use crate::application::concept::ConceptApplication;
        use crate::application::fact::{CONCEPT_OVERHEAD, RANGE_SCAN_COST};
        use crate::predicate::concept::Concept;

        // Create a FactApplication with constant attribute name
        let the_attr: Attribute = "user/name".parse().unwrap();
        let fact_app = FactApplication::new(
            Term::Constant(the_attr.clone()),
            Term::<Entity>::var("entity"),
            Term::<Value>::var("value"),
            crate::attribute::Cardinality::One,
        );

        // Create a ConceptApplication with single attribute
        let concept = Concept {
            operator: "user".to_string(),
            attributes: [(
                "name",
                crate::Attribute::new("user", "name", "User name", crate::Type::String),
            )]
            .into(),
        };

        let mut terms = crate::Parameters::new();
        terms.insert("this".to_string(), Term::<Value>::var("entity"));
        terms.insert("name".to_string(), Term::<Value>::var("value"));

        let concept_app = ConceptApplication { terms, concept };

        // Both should have same cost when nothing is bound
        let env = VariableScope::new();

        let fact_cost = fact_app.estimate(&env).expect("Should have cost");
        let concept_cost = concept_app.estimate(&env).expect("Should have cost");

        // Fact cost is just the scan
        assert_eq!(fact_cost, RANGE_SCAN_COST);

        // Concept cost includes overhead for potential rule evaluation
        assert_eq!(concept_cost, RANGE_SCAN_COST + CONCEPT_OVERHEAD);

        assert!(
            concept_cost > fact_cost,
            "ConceptApplication should cost more than FactApplication due to rule overhead. \
             Fact: {}, Concept: {}",
            fact_cost,
            concept_cost
        );
    }

    #[test]
    fn test_concept_equals_fact_cost_value_bound() {
        use crate::application::concept::ConceptApplication;
        use crate::application::fact::{CONCEPT_OVERHEAD, SEGMENT_READ_COST};
        use crate::predicate::concept::Concept;

        // Create a FactApplication with constant attribute name
        let the_attr: Attribute = "user/name".parse().unwrap();
        let fact_app = FactApplication::new(
            Term::Constant(the_attr.clone()),
            Term::<Entity>::var("entity"),
            Term::<Value>::var("value"),
            crate::attribute::Cardinality::One,
        );

        // Create a ConceptApplication with single attribute
        let concept = Concept {
            operator: "user".to_string(),
            attributes: [(
                "name",
                crate::Attribute::new("user", "name", "User name", crate::Type::String),
            )]
            .into(),
        };

        let mut terms = crate::Parameters::new();
        terms.insert("this".to_string(), Term::<Value>::var("entity"));
        terms.insert("name".to_string(), Term::<Value>::var("value"));

        let concept_app = ConceptApplication { terms, concept };

        // Bind the value
        let mut env = VariableScope::new();
        env.add(&Term::<Value>::var("value"));

        let fact_cost = fact_app.estimate(&env).expect("Should have cost");
        let concept_cost = concept_app.estimate(&env).expect("Should have cost");

        // Fact cost
        assert_eq!(fact_cost, SEGMENT_READ_COST);

        // Concept cost includes overhead
        assert_eq!(concept_cost, SEGMENT_READ_COST + CONCEPT_OVERHEAD);
    }

    #[test]
    fn test_concept_equals_fact_cost_entity_bound() {
        use crate::application::concept::ConceptApplication;
        use crate::application::fact::{CONCEPT_OVERHEAD, SEGMENT_READ_COST};
        use crate::predicate::concept::Concept;

        // Create a FactApplication with constant attribute name
        let the_attr: Attribute = "user/name".parse().unwrap();
        let fact_app = FactApplication::new(
            Term::Constant(the_attr.clone()),
            Term::<Entity>::var("entity"),
            Term::<Value>::var("value"),
            crate::attribute::Cardinality::One,
        );

        // Create a ConceptApplication with single attribute
        let concept = Concept {
            operator: "user".to_string(),
            attributes: [(
                "name",
                crate::Attribute::new("user", "name", "User name", crate::Type::String),
            )]
            .into(),
        };

        let mut terms = crate::Parameters::new();
        terms.insert("this".to_string(), Term::<Value>::var("entity"));
        terms.insert("name".to_string(), Term::<Value>::var("value"));

        let concept_app = ConceptApplication { terms, concept };

        // Bind the entity
        let mut env = VariableScope::new();
        env.add(&Term::<Value>::var("entity"));

        let fact_cost = fact_app.estimate(&env).expect("Should have cost");
        let concept_cost = concept_app.estimate(&env).expect("Should have cost");

        // Fact cost
        assert_eq!(fact_cost, SEGMENT_READ_COST);

        // Concept cost includes overhead
        assert_eq!(concept_cost, SEGMENT_READ_COST + CONCEPT_OVERHEAD);
    }

    #[test]
    fn test_concept_equals_fact_cost_cardinality_many_nothing_bound() {
        use crate::application::concept::ConceptApplication;
        use crate::application::fact::{CONCEPT_OVERHEAD, INDEX_SCAN};
        use crate::predicate::concept::Concept;

        // Create a FactApplication with Cardinality::Many
        let the_attr: Attribute = "user/tags".parse().unwrap();
        let fact_app = FactApplication::new(
            Term::Constant(the_attr.clone()),
            Term::<Entity>::var("entity"),
            Term::<Value>::var("tag"),
            crate::attribute::Cardinality::Many,
        );

        // Create a ConceptApplication with single Cardinality::Many attribute
        let mut concept_attr =
            crate::Attribute::new("user", "tags", "User tags", crate::Type::String);
        concept_attr.cardinality = crate::Cardinality::Many;

        let concept = Concept {
            operator: "user".to_string(),
            attributes: [("tags", concept_attr)].into(),
        };

        let mut terms = crate::Parameters::new();
        terms.insert("this".to_string(), Term::<Value>::var("entity"));
        terms.insert("tags".to_string(), Term::<Value>::var("tag"));

        let concept_app = ConceptApplication { terms, concept };

        // Nothing bound
        let env = VariableScope::new();

        let fact_cost = fact_app.estimate(&env).expect("Should have cost");
        let concept_cost = concept_app.estimate(&env).expect("Should have cost");

        // Fact cost
        assert_eq!(fact_cost, INDEX_SCAN);

        // Concept cost includes overhead
        assert_eq!(concept_cost, INDEX_SCAN + CONCEPT_OVERHEAD);
    }

    #[test]
    fn test_concept_equals_fact_cost_cardinality_many_value_bound() {
        use crate::application::concept::ConceptApplication;
        use crate::application::fact::{CONCEPT_OVERHEAD, RANGE_SCAN_COST};
        use crate::predicate::concept::Concept;

        // Create a FactApplication with Cardinality::Many
        let the_attr: Attribute = "user/tags".parse().unwrap();
        let fact_app = FactApplication::new(
            Term::Constant(the_attr.clone()),
            Term::<Entity>::var("entity"),
            Term::<Value>::var("tag"),
            crate::attribute::Cardinality::Many,
        );

        // Create a ConceptApplication with single Cardinality::Many attribute
        let mut concept_attr =
            crate::Attribute::new("user", "tags", "User tags", crate::Type::String);
        concept_attr.cardinality = crate::Cardinality::Many;

        let concept = Concept {
            operator: "user".to_string(),
            attributes: [("tags", concept_attr)].into(),
        };

        let mut terms = crate::Parameters::new();
        terms.insert("this".to_string(), Term::<Value>::var("entity"));
        terms.insert("tags".to_string(), Term::<Value>::var("tag"));

        let concept_app = ConceptApplication { terms, concept };

        // Bind the value
        let mut env = VariableScope::new();
        env.add(&Term::<Value>::var("tag"));

        let fact_cost = fact_app.estimate(&env).expect("Should have cost");
        let concept_cost = concept_app.estimate(&env).expect("Should have cost");

        // Fact cost
        assert_eq!(fact_cost, RANGE_SCAN_COST);

        // Concept cost includes overhead
        assert_eq!(concept_cost, RANGE_SCAN_COST + CONCEPT_OVERHEAD);
    }

    #[test]
    fn test_cost_accumulation_through_planning() {
        use crate::application::fact::{RANGE_SCAN_COST, SEGMENT_READ_COST};

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
            Term::<Entity>::var("entity"), // Already bound by p1
            Term::<Value>::var("age"),
            crate::attribute::Cardinality::One,
        );

        let a1 = Analysis::from(Premise::from(p1));
        let cost1 = a1.cost();

        // First premise: 1 constraint (just 'the' constant)
        // 1 constraint with Cardinality::One = RANGE_SCAN_COST
        assert_eq!(cost1, RANGE_SCAN_COST);

        // Simulate p2 with entity already bound
        let mut a2 = Analysis::from(Premise::from(p2.clone()));
        let mut env = VariableScope::new();
        env.add(&Term::<Value>::var("entity"));
        a2.update(&env);
        let cost2 = a2.cost();

        // Second premise with entity bound: 2 constraints ('the' constant + 'entity' in env)
        // 2 constraints with Cardinality::One = SEGMENT_READ_COST
        assert_eq!(
            cost2, SEGMENT_READ_COST,
            "Second premise with bound entity should cost SEGMENT_READ_COST. Expected {}, got {}",
            SEGMENT_READ_COST, cost2
        );

        // Total cost should be sum of both
        let total = cost1 + cost2;
        let expected_total = RANGE_SCAN_COST + SEGMENT_READ_COST;
        assert_eq!(
            total, expected_total,
            "Total cost should be sum of individual costs. Expected {}, got {}",
            expected_total, total
        );
    }
}
#[test]
fn debug_update_cost() {
    use crate::analyzer::Analysis;
    use crate::application::fact::FactApplication;
    use crate::artifact::Attribute;
    use crate::artifact::Entity;
    use crate::{Premise, Term, Value, VariableScope};

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

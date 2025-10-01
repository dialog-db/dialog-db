use crate::error::CompileError;
use crate::{fact::Scalar, predicate::DeductiveRule};
use crate::{Dependencies, Term, Value, VariableScope};
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
    pub premise: crate::premise::Premise,
    pub cost: usize,
    pub binds: VariableScope,
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
                        // If this term was in binds and is now bound, move it to env
                        if new_bindings.contains(term) && binds.contains(term) {
                            // Add to env (only relevant bindings)
                            env.add(term);

                            // Remove from binds (incremental update)
                            binds.remove(term);

                            // Decrease cost (incremental update)
                            if let crate::Requirement::Derived(c) = constraint.requirement {
                                *cost = cost.saturating_sub(c);
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

                // Process only relevant bindings
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Term, Value};

    #[test]
    fn test_syntax_analysis_new() {
        let analysis = Analysis::new(100);

        match analysis {
            Analysis::Candidate {
                cost,
                desired,
                depends,
            } => {
                assert_eq!(cost, 100);
                assert_eq!(desired.count(), 0);
                assert_eq!(depends.size(), 0);
            }
            _ => panic!("Expected Candidate variant"),
        }
    }

    #[test]
    fn test_plan_context_from_candidate() {
        let mut analysis = Analysis::new(100);
        let term = Term::<Value>::var("y");
        analysis.desire(&term, 10);

        let context: PlanContext = analysis.try_into().expect("Should convert to PlanContext");

        assert_eq!(context.cost, 100);
        assert_eq!(context.desired.count(), 1);
        assert_eq!(context.depends.size(), 0);
    }

    #[test]
    fn test_plan_context_from_incomplete_fails() {
        let mut analysis = Analysis::new(100);
        let term = Term::<Value>::var("z");

        analysis.require(&term);

        let result: Result<PlanContext, _> = analysis.try_into();
        assert!(result.is_err());
    }

    #[test]
    fn test_depend_marks_variable_as_bound() {
        let mut analysis = Analysis::new(100);
        let term = Term::<Value>::var("bound_var");

        analysis.depend(&term);

        // Should be in depends ONLY
        assert_eq!(analysis.depends().size(), 1);
        assert!(analysis.depends().contains(&term));

        // Should NOT be in desired (mutual exclusivity)
        assert_eq!(analysis.desired().count(), 0);
        assert!(!analysis.desired().contains(&term));
    }

    #[test]
    fn test_depend_removes_from_desired() {
        let mut analysis = Analysis::new(50);
        let term = Term::<Value>::var("var");

        // First, desire it
        analysis.desire(&term, 15);
        assert_eq!(analysis.desired().count(), 1);
        assert_eq!(analysis.depends().size(), 0);

        // Then mark as dependent
        analysis.depend(&term);

        // Should move from desired to depends
        assert_eq!(analysis.depends().size(), 1);
        assert!(analysis.depends().contains(&term));
        assert_eq!(analysis.desired().count(), 0);
        assert!(!analysis.desired().contains(&term));
    }

    #[test]
    fn test_depend_removes_from_required_and_transitions() {
        let mut analysis = Analysis::new(90);
        let term = Term::<Value>::var("will_be_bound");

        // First, require it
        analysis.require(&term);

        match &analysis {
            Analysis::Incomplete { required, .. } => {
                assert_eq!(required.count(), 1);
            }
            _ => panic!("Should be Incomplete after require"),
        }

        // Then mark as dependent
        analysis.depend(&term);

        // Should transition to Candidate (no required left)
        match analysis {
            Analysis::Candidate {
                depends, desired, ..
            } => {
                assert_eq!(depends.size(), 1);
                assert!(depends.contains(&term));
                assert_eq!(desired.count(), 0);
            }
            _ => panic!("Should transition to Candidate after satisfying requirement"),
        }
    }

    #[test]
    fn test_mutual_exclusivity_of_categories() {
        let mut analysis = Analysis::new(100);
        let term1 = Term::<Value>::var("a");
        let term2 = Term::<Value>::var("b");
        let term3 = Term::<Value>::var("c");

        // term1: desired
        analysis.desire(&term1, 10);
        // term2: required
        analysis.require(&term2);
        // term3: depends
        analysis.depend(&term3);

        match analysis {
            Analysis::Incomplete {
                depends,
                desired,
                required,
                ..
            } => {
                // Each term should be in exactly one category
                assert_eq!(desired.count(), 1);
                assert!(desired.contains(&term1));

                assert_eq!(required.count(), 1);

                assert_eq!(depends.size(), 1);
                assert!(depends.contains(&term3));

                // Verify mutual exclusivity
                assert!(!desired.contains(&term2));
                assert!(!desired.contains(&term3));
                assert!(!depends.contains(&term1));
                assert!(!depends.contains(&term2));
            }
            _ => panic!("Expected Incomplete state"),
        }
    }
}

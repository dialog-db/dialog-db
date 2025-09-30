use crate::application::{ConceptApplication, FactApplication, FormulaApplication};
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
pub struct Analysis {
    /// Base execution cost which does not include added costs captured in the
    /// dependencies.
    pub cost: usize,
    pub dependencies: Dependencies,
}

impl Analysis {
    pub fn new(cost: usize) -> Self {
        Analysis {
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

/// Syntax forms for our datalog notation.
pub trait Syntax: Sized {
    /// Performs analysis of this syntax form in the provided environment.
    fn analyze<'a>(&'a self, env: &Environment) -> Stats<'a, Self>;

    fn update<'a>(&'a self, stats: &mut Stats<'a, Self>, extension: &VariableScope);
}

pub struct Environment {
    pub locals: VariableScope,
}

pub struct AnalyzedSyntax<'a, Form: Syntax> {
    pub syntax: &'a Form,

    /// Estimated cost of evaluating this syntax form.
    pub cost: usize,

    /// Set of variables that need to be bound in the evaluation
    /// context in order to evaluate this syntax form.
    pub requires: VariableScope,

    /// Set of variables that will be bound by the evaluation of this
    /// syntax form
    pub provides: VariableScope,
}

pub enum AnalysisStatus<'a, Form: Syntax> {
    Blocked {
        syntax: &'a Form,
        requires: VariableScope,
    },
    Candidate {
        syntax: &'a Form,
        cost: usize,
        requires: VariableScope,
        provides: VariableScope,
    },
}

pub enum Plan<'a> {
    Fact(&'a FactApplication),
    Concept(&'a ConceptApplication),
    Formula(&'a FormulaApplication),
}

pub struct Stats<'a, Form: Syntax> {
    pub syntax: &'a Form,
    /// Base cost of evaluating this syntax form regardless of
    /// all the desired variables.
    pub cost: usize,
    /// Set of variable names that are required.
    pub required: Required,
    /// Set of variable names mapped to corresponding costs.
    pub desired: Desired,
}

impl<'a, Form: Syntax> Stats<'a, Form> {
    pub fn new(syntax: &'a Form, cost: usize) -> Self {
        Stats {
            syntax,
            cost,
            required: Required::new(),
            desired: Desired::new(),
        }
    }

    pub fn expense(&mut self, cost: usize) {
        self.cost += cost;
    }

    pub fn require<T: Scalar>(&mut self, term: &Term<T>) {
        self.required.add(term);
    }

    pub fn desire<T: Scalar>(&mut self, term: &Term<T>, cost: usize) {
        match term {
            Term::Variable { name: None, .. } => {
                self.cost += cost;
            }
            Term::Variable {
                name: Some(name), ..
            } => {
                self.desired.0.insert(name.into(), cost);
            }
            _ => {}
        }
    }

    /// Calculates the total cost of all derived dependencies.
    /// Required dependencies don't contribute to cost as they must be provided.
    pub fn estimate(&'a self) -> Result<usize, EstimateError<'a>> {
        if self.required.count() == 0 {
            Ok(self.cost + self.desired.total())
        } else {
            Err(EstimateError::RequiredParameters {
                required: &self.required,
            })
        }
    }

    pub fn require_all(&mut self) {
        for variable in self.desired.0.keys() {
            self.required.0.insert(variable.clone());
        }
        self.desired.0.clear();
    }

    /// updates analysis by adding new variables to the scope
    pub fn update(&mut self, scope: &VariableScope) {
        for variable in scope.into_iter() {
            self.required.remove(&variable);
            self.desired.remove(&variable);
        }
    }
}
impl<'a, Form: Syntax> Stats<'a, Form> {
    pub fn provides(&self) -> VariableScope {
        let mut provides = VariableScope::new();
        for variable in self.desired.iter() {
            provides.add(&variable);
        }
        provides
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

// trait Status {}

// #[derive(Debug, Clone)]
// pub struct Blocked;
// impl Status for Blocked {}

// #[derive(Debug, Clone)]
// pub struct Ready;
// impl Status for Ready {}

// #[derive(Debug, Clone)]
// pub struct SyntaxAnalysis<Status> {
//     /// Base cost of evaluating this syntax form regardless of
//     /// all the desired variables.
//     pub cost: usize,
//     /// Set of variable names that are required.
//     pub required: Required,
//     /// Set of variable names mapped to corresponding costs.
//     pub desired: Desired,

//     status: PhantomData<Status>,
// }

/// Context for a successfully planned premise
/// Unlike SyntaxAnalysis, this can never be in Incomplete state
#[derive(Debug, Clone, PartialEq)]
pub struct PlanContext {
    pub cost: usize,
    pub desired: Desired,
    pub depends: VariableScope,
}

impl PlanContext {
    pub fn provides(&self) -> VariableScope {
        self.desired.clone().into()
    }

    pub fn depends(&self) -> &VariableScope {
        &self.depends
    }
}

impl TryFrom<SyntaxAnalysis> for PlanContext {
    type Error = &'static str;

    fn try_from(analysis: SyntaxAnalysis) -> Result<Self, Self::Error> {
        match analysis {
            SyntaxAnalysis::Candidate {
                cost,
                desired,
                depends,
            } => Ok(PlanContext {
                cost,
                desired,
                depends,
            }),
            SyntaxAnalysis::Incomplete { .. } => {
                Err("Cannot convert Incomplete SyntaxAnalysis to PlanContext")
            }
        }
    }
}

impl From<PlanContext> for SyntaxAnalysis {
    fn from(context: PlanContext) -> Self {
        SyntaxAnalysis::Candidate {
            cost: context.cost,
            desired: context.desired,
            depends: context.depends,
        }
    }
}

#[derive(Clone)]
pub enum SyntaxAnalysis {
    /// Plan that can not be evaluated because it has unsatisfied requirements.
    Incomplete {
        cost: usize,
        required: Required,
        desired: Desired,
        depends: VariableScope,
    },
    Candidate {
        cost: usize,
        desired: Desired,
        depends: VariableScope,
    },
}
impl SyntaxAnalysis {
    pub fn new(cost: usize) -> Self {
        SyntaxAnalysis::Candidate {
            cost,
            desired: Desired::new(),
            depends: VariableScope::new(),
        }
    }

    pub fn desired(&self) -> &Desired {
        match self {
            SyntaxAnalysis::Incomplete { desired, .. } => desired,
            SyntaxAnalysis::Candidate { desired, .. } => desired,
        }
    }

    pub fn cost(&self) -> &usize {
        match self {
            SyntaxAnalysis::Incomplete { cost, .. } => cost,
            SyntaxAnalysis::Candidate { cost, .. } => cost,
        }
    }

    pub fn depends(&self) -> &VariableScope {
        match self {
            SyntaxAnalysis::Incomplete { depends, .. } => depends,
            SyntaxAnalysis::Candidate { depends, .. } => depends,
        }
    }

    pub fn provides(&self) -> &VariableScope {
        match self {
            SyntaxAnalysis::Incomplete { depends, .. } => depends,
            SyntaxAnalysis::Candidate { depends, .. } => depends,
        }
    }

    pub fn require<T: Scalar>(&mut self, term: &Term<T>) {
        match self {
            SyntaxAnalysis::Incomplete {
                required, desired, ..
            } => {
                desired.remove(term);
                required.add(term);
            }
            SyntaxAnalysis::Candidate {
                cost,
                desired,
                depends,
            } => {
                desired.remove(term);
                let mut required = Required::new();
                required.add(term);
                *self = SyntaxAnalysis::Incomplete {
                    cost: *cost,
                    desired: desired.to_owned(),
                    required,
                    depends: depends.to_owned(),
                };
            }
        }
    }

    pub fn desire<T: Scalar>(&mut self, term: &Term<T>, cost: usize) {
        match term {
            // if terms is not a named variable we add inflate base cost
            Term::Variable { name: None, .. } => match self {
                SyntaxAnalysis::Incomplete {
                    cost: total,
                    required,
                    desired,
                    depends,
                } => {
                    *self = SyntaxAnalysis::Incomplete {
                        cost: *total + cost,
                        desired: desired.to_owned(),
                        required: required.to_owned(),
                        depends: depends.to_owned(),
                    };
                }
                SyntaxAnalysis::Candidate {
                    cost: total,
                    desired,
                    depends,
                } => {
                    *self = SyntaxAnalysis::Candidate {
                        cost: *total + cost,
                        desired: desired.to_owned(),
                        depends: depends.to_owned(),
                    };
                }
            },
            // if term is named variable we update required and desired
            Term::Variable { name: Some(_), .. } => match self {
                SyntaxAnalysis::Incomplete {
                    cost: total,
                    required,
                    desired,
                    depends,
                } => {
                    required.remove(term);
                    desired.insert(term, cost);

                    // if none of the requirements are left we transition it to
                    // candidate state.
                    if required.count() == 0 {
                        *self = SyntaxAnalysis::Candidate {
                            cost: *total,
                            desired: desired.to_owned(),
                            depends: depends.to_owned(),
                        };
                    }
                }
                SyntaxAnalysis::Candidate { desired, .. } => {
                    desired.insert(term, cost);
                }
            },
            _ => {}
        }
    }

    pub fn require_all(&mut self) {
        let terms: Vec<_> = self
            .desired()
            .entries()
            .filter(|(_, cost)| *cost > 0)
            .map(|(term, _)| term)
            .collect();
        for term in terms {
            self.require(&term);
        }
    }

    /// Mark a variable as bound (already available in the environment)
    /// This removes it from desired/required and adds it to depends
    /// Variables should be in exactly one category: required, desired, or depends
    pub fn depend<T: Scalar>(&mut self, term: &Term<T>) {
        match term {
            Term::Constant(_) => {}
            Term::Variable { name: Some(_), .. } => {
                match self {
                    SyntaxAnalysis::Incomplete {
                        depends,
                        required,
                        desired,
                        cost,
                    } => {
                        // Remove from required and desired
                        required.remove(term);
                        desired.remove(term);
                        // Add to depends
                        depends.add(term);

                        // If no required left, transition to Candidate
                        if required.count() == 0 {
                            *self = SyntaxAnalysis::Candidate {
                                cost: *cost,
                                desired: desired.to_owned(),
                                depends: depends.to_owned(),
                            };
                        }
                    }
                    SyntaxAnalysis::Candidate {
                        depends, desired, ..
                    } => {
                        // Remove from desired
                        desired.remove(term);
                        // Add to depends
                        depends.add(term);
                    }
                }
            }
            Term::Variable { name: None, .. } => {}
        }
    }

    /// Bindings availabile in this context
    pub fn bindings(&self) -> impl Iterator<Item = Term<Value>> + '_ {
        self.desired()
            .entries()
            .filter_map(|(term, cost)| if cost == 0 { Some(term) } else { None })
    }
}

/// Syntax forms for our datalog notation.
pub trait Planner: Sized {
    /// Performs analysis of this syntax form in the provided environment.
    fn init(&self, plan: &mut SyntaxAnalysis, env: &VariableScope);
    fn update(&self, plan: &mut SyntaxAnalysis, env: &VariableScope);

    fn plan(&self, env: &VariableScope) -> SyntaxAnalysis {
        let mut plan = SyntaxAnalysis::new(0);
        self.init(&mut plan, env);

        // If plan has no required variables it is a candidate.
        if let SyntaxAnalysis::Incomplete {
            cost,
            required,
            desired,
            depends,
        } = &plan
        {
            if required.count() == 0 {
                plan = SyntaxAnalysis::Candidate {
                    cost: *cost,
                    desired: desired.clone(),
                    depends: depends.clone(),
                };
            }
        }

        plan
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Term, Value};

    #[test]
    fn test_syntax_analysis_new() {
        let analysis = SyntaxAnalysis::new(100);

        match analysis {
            SyntaxAnalysis::Candidate {
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
        let mut analysis = SyntaxAnalysis::new(100);
        let term = Term::<Value>::var("y");
        analysis.desire(&term, 10);

        let context: PlanContext = analysis.try_into().expect("Should convert to PlanContext");

        assert_eq!(context.cost, 100);
        assert_eq!(context.desired.count(), 1);
        assert_eq!(context.depends.size(), 0);
    }

    #[test]
    fn test_plan_context_from_incomplete_fails() {
        let mut analysis = SyntaxAnalysis::new(100);
        let term = Term::<Value>::var("z");

        analysis.require(&term);

        let result: Result<PlanContext, _> = analysis.try_into();
        assert!(result.is_err());
    }

    #[test]
    fn test_depend_marks_variable_as_bound() {
        let mut analysis = SyntaxAnalysis::new(100);
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
        let mut analysis = SyntaxAnalysis::new(50);
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
        let mut analysis = SyntaxAnalysis::new(90);
        let term = Term::<Value>::var("will_be_bound");

        // First, require it
        analysis.require(&term);

        match &analysis {
            SyntaxAnalysis::Incomplete { required, .. } => {
                assert_eq!(required.count(), 1);
            }
            _ => panic!("Should be Incomplete after require"),
        }

        // Then mark as dependent
        analysis.depend(&term);

        // Should transition to Candidate (no required left)
        match analysis {
            SyntaxAnalysis::Candidate {
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
        let mut analysis = SyntaxAnalysis::new(100);
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
            SyntaxAnalysis::Incomplete {
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

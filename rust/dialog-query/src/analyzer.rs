use crate::application::{ConceptApplication, FactApplication, FormulaApplication};
use crate::error::CompileError;
use crate::{fact::Scalar, predicate::DeductiveRule};
use crate::{Dependencies, Term, Value, VariableScope};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::fmt::Display;
use std::marker::PhantomData;
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
            provides.add(variable.clone());
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

    pub fn iter(&self) -> impl Iterator<Item = Term<Value>> {
        self.0.keys().map(|name| Term::var(name.clone()))
    }

    pub fn entries(&self) -> impl Iterator<Item = (&Term<Value>, &usize)> {
        self.0
            .iter()
            .map(|(name, cost)| (&Term::var(name.clone()), cost))
    }
}

impl From<Desired> for VariableScope {
    fn from(desired: Desired) -> Self {
        let mut scope = VariableScope::new();
        for (name, _) in desired.0.into_iter() {
            scope.add(&Term::var(name));
        }
        scope
    }
}

#[derive(Error, Debug, Clone, PartialEq)]
pub enum EstimateError<'a> {
    #[error("Required parameters {required} are not bound in the environment ")]
    RequiredParameters { required: &'a Required },
}

impl<'a> From<EstimateError<'a>> for CompileError<'a> {
    fn from(error: EstimateError<'a>) -> Self {
        match error {
            EstimateError::RequiredParameters { required } => {
                CompileError::RequiredBindings { required }
            }
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

pub enum SyntaxAnalysis {
    /// Plan that can not be evaluated because it has unsatisfied requirements.
    Incomplete {
        cost: usize,
        required: Required,
        desired: Desired,
    },
    Candidate {
        cost: usize,
        desired: Desired,
    },
}
impl SyntaxAnalysis {
    pub fn new(cost: usize) -> Self {
        SyntaxAnalysis::Candidate {
            cost,
            desired: Desired::new(),
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

    pub fn require<T: Scalar>(&mut self, term: &Term<T>) {
        match self {
            SyntaxAnalysis::Incomplete {
                required, desired, ..
            } => {
                desired.remove(term);
                required.add(term);
            }
            SyntaxAnalysis::Candidate { cost, desired } => {
                desired.remove(term);
                let mut required = Required::new();
                required.add(term);
                *self = SyntaxAnalysis::Incomplete {
                    cost: *cost,
                    desired: desired.to_owned(),
                    required,
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
                } => {
                    *self = SyntaxAnalysis::Incomplete {
                        cost: total + *cost,
                        desired: desired.to_owned(),
                        required: required.to_owned(),
                    };
                }
                SyntaxAnalysis::Candidate {
                    cost: total,
                    desired,
                } => {
                    *self = SyntaxAnalysis::Candidate {
                        cost: total + *cost,
                        desired: desired.to_owned(),
                    };
                }
            },
            // if term is named variable we update required and desired
            Term::Variable { name: Some(_), .. } => match self {
                SyntaxAnalysis::Incomplete {
                    cost: total,
                    required,
                    desired,
                } => {
                    required.remove(term);
                    desired.insert(term, cost);

                    // if none of the requirements are left we transition it to
                    // candidate state.
                    if (required.count() == 0) {
                        *self = SyntaxAnalysis::Candidate {
                            cost: *total,
                            desired: desired.to_owned(),
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
        for (term, cost) in self.desired().entries() {
            if cost > &0 {
                self.require(term);
            }
        }
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
        } = plan
        {
            if required.count() == 0 {
                *plan = SyntaxAnalysis::Candidate { cost, desired };
            }
        }

        plan
    }
}

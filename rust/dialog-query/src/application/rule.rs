use super::{Application, Join};
use crate::analyzer::{Analysis, AnalyzerError};
use crate::error::PlanError;
use crate::plan::RuleApplicationPlan;
use crate::predicate::DeductiveRule;
use crate::{Dependencies, Parameters, Requirement, Term, VariableScope};
use std::fmt::Display;

/// Represents a rule application with the terms applied to corresponding
/// rule parameters.
#[derive(Debug, Clone, PartialEq)]
pub struct RuleApplication {
    /// Applied terms
    pub terms: Parameters,
    /// Rule being applied
    pub rule: DeductiveRule,
}

impl RuleApplication {
    /// Creates a new rule application with the given rule and term bindings.
    pub fn new(rule: DeductiveRule, terms: Parameters) -> Self {
        RuleApplication { rule, terms }
    }

    /// Analyzes this rule application to validate term bindings and compute dependencies.
    /// Ensures all required parameters are provided and propagates variable dependencies.
    pub fn analyze(&self) -> Result<Analysis, AnalyzerError> {
        // First we analyze the rule itself identifying its dependencies and
        // execution budget.
        let analysis = self.rule.analyze()?;
        let mut dependencies = Dependencies::new();

        for (parameter, requirement) in analysis.dependencies.iter() {
            match requirement {
                // If some of the parameters is a required dependency of the
                // rule, but it was not applied rule application is invalid.
                Requirement::Required => {
                    self.terms
                        .get(parameter)
                        .ok_or_else(|| AnalyzerError::RequiredParameter {
                            rule: self.rule.clone(),
                            parameter: parameter.to_string(),
                        })?;
                }
                // If dependency is not required and applied term is not a
                // constant we propagate it into dependencies.
                Requirement::Derived(desire) => {
                    if let Some(Term::Variable { .. }) = self.terms.get(parameter) {
                        dependencies.desire(parameter.to_string(), desire);
                    }
                }
            }
        }

        Ok(Analysis {
            dependencies,
            cost: analysis.cost,
        })
    }
    /// Creates an execution plan for this rule application.
    /// Validates that all required variables are in scope and plans execution
    /// of all rule premises in optimal order.
    pub fn plan(&self, scope: &VariableScope) -> Result<RuleApplicationPlan, PlanError> {
        let mut provides = VariableScope::new();
        let analysis = self.analyze().map_err(PlanError::from)?;
        // analyze dependencies and make sure that all required dependencies
        // are provided
        for (name, requirement) in analysis.dependencies.iter() {
            let parameter = self.terms.get(name);
            match requirement {
                Requirement::Required => {
                    if let Some(term) = parameter {
                        if scope.contains(&term) {
                            Ok(())
                        } else {
                            Err(PlanError::UnboundRuleParameter {
                                rule: self.rule.clone(),
                                parameter: name.into(),
                                term: term.clone(),
                            })
                        }
                    } else {
                        Err(PlanError::OmitsRequiredParameter {
                            rule: self.rule.clone(),
                            parameter: name.into(),
                        })
                    }?;
                }
                Requirement::Derived(_) => {
                    // If requirement can be derived and was not provided
                    // we add it to the provided set
                    if let Some(term) = parameter {
                        if !scope.contains(&term) {
                            provides.add(&term);
                        }
                    }
                }
            }
        }

        let mut planner = Join::new(&self.rule.premises);
        let (cost, conjuncts) = planner.plan(scope)?;

        Ok(RuleApplicationPlan {
            cost,
            provides,
            conjuncts,
            terms: self.terms.clone(),
            rule: self.rule.clone(),
        })
    }
}

impl Display for RuleApplication {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} {{", self.rule.conclusion.operator)?;
        for (name, term) in self.terms.iter() {
            write!(f, "{}: {},", name, term)?;
        }
        write!(f, "}}")
    }
}

impl From<RuleApplication> for Application {
    fn from(application: RuleApplication) -> Self {
        Application::ApplyRule(application)
    }
}

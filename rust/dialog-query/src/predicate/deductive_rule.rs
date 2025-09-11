pub use crate::analyzer::{Analysis, AnalyzerError};
pub use crate::application::RuleApplication;
pub use crate::premise::Premise;
pub use crate::{Attribute, Dependencies, Parameters, Requirement, Value};
use std::collections::{HashMap, HashSet};
use std::fmt::Display;

/// Represents a conclusion of the rule as a set of attribute descriptors keyed
/// by the rule parameter name. It is effectively describes decomposition into
/// facts with a shared entity.
#[derive(Debug, Clone, PartialEq)]
pub struct Conclusion {
    /// Map of all attributes this entity should have to reach this conclusion.
    pub attributes: HashMap<String, Attribute<Value>>,
}
impl Conclusion {
    /// Checks if the conclusion includes the given parameter name.
    /// The special "this" parameter is always considered present as it represents
    /// the entity that the conclusion applies to.
    pub fn contains(&self, name: &str) -> bool {
        name == "this" || self.attributes.contains_key(name)
    }

    /// Finds a parameter that is absent from the provided dependencies.
    pub fn absent(&self, dependencies: &Dependencies) -> Option<&str> {
        if !dependencies.contains("this") {
            Some("this")
        } else {
            self.attributes
                .keys()
                .find(|name| !dependencies.contains(name))
                .map(|name| name.as_str())
        }
    }
}

/// Represents a deductive rule that can be applied creating a premise.
#[derive(Debug, Clone, PartialEq)]
pub struct DeductiveRule {
    /// Rule identifier used to look rules up by.
    pub operator: String,
    /// Conclusion that this rule reaches if all premises hold. This is
    /// typically what datalog calls rule head.
    pub conclusion: Conclusion,
    /// Premises that must hold for rule to reach it's conclusion. Typically
    /// datalog calls these rule body.
    pub premises: Vec<Premise>,
}
impl DeductiveRule {
    /// Returns the names of the parameters for this rule.
    pub fn parameters(&self) -> HashSet<String> {
        let Conclusion { attributes, .. } = &self.conclusion;
        let mut params = HashSet::new();
        for (name, _) in attributes.iter() {
            params.insert(name.clone());
        }
        params.insert("this".to_string());
        params
    }

    /// Analyzes this rule identifying its dependencies and estimated execution
    /// budget. It also verifies that all rule parameters are utilized by the
    /// rule premises and returns an error if any are not.
    pub fn analyze(&self) -> Result<Analysis, AnalyzerError> {
        let conclusion = &self.conclusion;
        // We will collect rule dependencies and compute their levels based on
        // their use in the rule premises.
        let mut dependencies = Dependencies::new();
        // We will collect all internal dependencies which correspond to
        // variable terms that are not shared with outside scope. We do so
        // in order to identify if there are any unresolvable dependencies
        // and in the local rule budget.
        let mut variables = Dependencies::new();

        let mut cost: usize = 0;
        // Analyze each premise and account their dependencies into the rule's
        // dependencies and budget.
        for premise in self.premises.iter() {
            let analysis = premise.analyze()?;
            cost += analysis.cost;

            // Go over every dependency of every premise and estimate their
            // cost for the rule. If dependency is a parameter of the rule
            // it updates rule dependency levels accordingly, otherwise it
            // captures them in the internal dependencies in order to reflect
            // it in the budget.
            for (name, dependency) in analysis.dependencies.iter() {
                if conclusion.contains(name) {
                    dependencies.update(name.to_string(), dependency);
                } else {
                    variables.update(name.to_string(), dependency);
                }
            }
        }

        // Now that we have processed all premises we expect all the
        // parameters to be in the dependencies. If there is a parameter
        // not listed in the dependencies, we raise an error because this rule
        // is considered invalid - it would imply that parameter is required
        // input and even then it is completely ignored, suggesting an error in
        // the rule definition. We can introduce `discard` operator in the
        // future where rule author may intentionally require a parameter it is
        // not utilizing.
        conclusion
            .absent(&dependencies)
            .map_or(Ok(()), |parameter| {
                Err(AnalyzerError::UnusedParameter {
                    rule: self.clone(),
                    parameter: parameter.to_string(),
                })
            })?;

        // Next we check if there is a required local variable and if so we
        // raise an error. If we have such variable it implies that we have a
        // premise(s) that require this variable, but there is no premise that
        // can provide it, which makes it impossible to execute such a rule.
        variables
            .iter()
            .find(|(_, level)| matches!(level, Requirement::Required))
            .map_or(Ok(()), |(variable, _)| {
                Err(AnalyzerError::RequiredLocalVariable {
                    rule: self.clone(),
                    variable: variable.to_string(),
                })
            })?;

        // If we got this far we know all the dependencies and have an estimate
        // cost of executions.
        Ok(Analysis {
            cost: cost + variables.cost(),
            dependencies,
        })
    }

    /// Creates a rule application by binding the provided terms to this rule's parameters.
    /// Validates that all required parameters are provided and returns an error if the
    /// application would be invalid.
    pub fn apply(&self, terms: Parameters) -> Result<RuleApplication, AnalyzerError> {
        let application = RuleApplication::new(self.clone(), terms);
        application.analyze().and(Ok(application))
    }
}
impl Display for DeductiveRule {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} {{", self.operator)?;
        for (name, attribute) in self.conclusion.attributes.iter() {
            write!(f, "{}: {},", name, attribute.data_type)?;
        }
        write!(f, "}}")
    }
}

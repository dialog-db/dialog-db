pub use crate::analyzer::{AnalyzerError, LegacyAnalysis, Plan};
pub use crate::application::{FactApplication, RuleApplication};
use crate::error::{CompileError, SchemaError};
pub use crate::planner::Join;
pub use crate::predicate::Concept;
pub use crate::premise::Premise;
pub use crate::{Application, Attribute, Dependencies, Parameters, Requirement, Value};
use crate::{Term, Type, VariableScope};
use std::fmt::Display;

/// Represents a deductive rule that can be applied creating a premise.
#[derive(Debug, Clone, PartialEq)]
pub struct DeductiveRule {
    /// Conclusion that this rule reaches if all premises hold. This is
    /// typically what datalog calls rule head.
    pub conclusion: Concept,
    /// Premises that must hold for rule to reach it's conclusion. Typically
    /// datalog calls these rule body. These are guaranteed to be viable plans
    /// after compilation.
    pub premises: Vec<Plan>,
}
impl DeductiveRule {
    /// Create a new uncompiled rule from a conclusion and premises
    pub fn new(conclusion: Concept, premises: Vec<Premise>) -> Result<Self, CompileError> {
        // Convert premises to an intermediate form, then compile
        let uncompiled = UncompiledDeductiveRule {
            conclusion,
            premises,
        };
        uncompiled.compile()
    }

    pub fn operator(&self) -> &str {
        &self.conclusion.operator()
    }
    pub fn operands(&self) -> impl Iterator<Item = &str> {
        self.conclusion.operands()
    }
    /// Returns the names of the parameters for this rule.
    pub fn parameters(&self) -> impl Iterator<Item = &str> {
        self.conclusion.operands()
    }

    /// Creates a rule application by binding the provided terms to this rule's parameters.
    /// Validates that all required parameters are provided and returns an error if the
    /// application would be invalid.
    pub fn apply(&self, parameters: Parameters) -> Result<Application, SchemaError> {
        self.conclusion.apply(parameters)
    }
}

/// Internal helper for rules before compilation
struct UncompiledDeductiveRule {
    conclusion: Concept,
    premises: Vec<Premise>,
}

impl UncompiledDeductiveRule {
    pub fn compile(self) -> Result<DeductiveRule, CompileError> {
        // We attempt to plan the order of premises in a scope where none of the
        // rule parameters are bound in order to identify most optimal execution
        // order in such scenario or to discover that some premise in the rule
        // is not satisfiable e.g. if formula uses rule parameter in the required
        // cell which is not derived from any other premise.
        let (premises, derived) = Join::new(self.premises.clone()).plan(&VariableScope::new())?;

        // We also verify that every rule parameter was derived by one of the
        // rule premises, otherwise we produce an error since rule evaluation
        // would not be able to bind such parameter.
        for name in self.conclusion.operands() {
            if !derived.contains(&Term::<Value>::var(name)) {
                // Create a temporary rule for the error message
                let temp_rule = DeductiveRule {
                    conclusion: self.conclusion.clone(),
                    premises: premises.clone(),
                };
                Err(CompileError::UnboundVariable {
                    rule: temp_rule,
                    variable: name.to_string(),
                })?;
            }
        }

        Ok(DeductiveRule {
            conclusion: self.conclusion,
            premises,
        })
    }

    // /// Analyzes this rule identifying its dependencies and estimated execution
    // /// budget. It also verifies that all rule parameters are utilized by the
    // /// rule premises and returns an error if any are not.
    // pub fn analyze(&self) -> Result<Analysis, AnalyzerError> {
    //     let conclusion = &self.conclusion;
    //     // We will collect all internal dependencies which correspond to
    //     // variable terms that are not shared with outside scope. We do so
    //     // in order to identify if there are any unresolvable dependencies
    //     // and in the local rule budget.
    //     let mut variables = Dependencies::new();

    //     let mut cost: usize = 0;
    //     // Analyze each premise and account their dependencies into the rule's
    //     // dependencies and budget.
    //     for premise in self.premises.iter() {
    //         let analysis = premise.analyze()?;
    //         cost += analysis.cost;

    //         // Go over every dependency of every premise and estimate their
    //         // cost for the rule. If dependency is a parameter of the rule
    //         // updates dependency cost, otherwise it capture in the local
    //         // variables to reflect in the total cost
    //         for (name, dependency) in analysis.dependencies.iter() {
    //             variables.merge(name.into(), dependency);
    //         }
    //     }

    //     // Now that we have processed all premises we expect all the
    //     // parameters to be in the dependencies and all should be derived.
    //     // If some parameter is not in the dependencies that implies that
    //     // parameter was not used which is an error because it could not be
    //     // derived by the rule. If some parameter is required dependency that
    //     // implies that formula makes use of it, but there is no premise that
    //     // binds it.
    //     let mut dependencies = Dependencies::new();
    //     for name in conclusion.parameters() {
    //         if let Some(dependency) = variables.lookup(name) {
    //             match dependency {
    //                 // If rule attribute is a required dependency it implies that
    //                 // no premise derives it while some formula utilizes it which
    //                 // is not allowed.
    //                 Requirement::Required => Err(AnalyzerError::UnboundVariable {
    //                     rule: self.clone(),
    //                     variable: name.to_string(),
    //                 }),
    //                 // Otherwise add a variable to the dependencies
    //                 Requirement::Derived(cost) => {
    //                     dependencies.desire(name.into(), cost.clone());
    //                     Ok(())
    //                 }
    //             }
    //         }
    //         // If there is no dependency on the rule parameter it can not be
    //         // derived which indicates that rule definition is invalid.
    //         else {
    //             Err(AnalyzerError::UnusedParameter {
    //                 rule: self.clone(),
    //                 parameter: name.to_string(),
    //             })
    //         }?;
    //     }

    //     // Next we check if there is any required variable if so we
    //     // raise an error because it can not be derived by this rule.
    //     variables
    //         .iter()
    //         .find(|(_, level)| matches!(level, Requirement::Required))
    //         .map_or(Ok(()), |(variable, _)| {
    //             Err(AnalyzerError::RequiredLocalVariable {
    //                 rule: self.clone(),
    //                 variable: variable.to_string(),
    //             })
    //         })?;

    //     // If we got this far we know all the dependencies and have an estimate
    //     // cost of executions.
    //     Ok(Analysis {
    //         cost: cost + variables.cost(),
    //         dependencies,
    //     })
    // }
}

#[derive(Debug, Clone, PartialEq)]
pub struct DeductiveRuleAnalysis {
    analysis: LegacyAnalysis,
}

impl Display for DeductiveRule {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} {{", self.operator())?;
        write!(f, "this: {},", Type::Entity)?;
        for (name, attribute) in self.conclusion.attributes.iter() {
            write!(f, "{}: {},", name, attribute.content_type)?;
        }
        write!(f, "}}")
    }
}

impl TryFrom<&Concept> for DeductiveRule {
    type Error = CompileError;

    fn try_from(concept: &Concept) -> Result<Self, Self::Error> {
        use crate::artifact::Entity;

        let mut premises = Vec::new();

        let this = Term::<Entity>::var("this");
        for (name, attribute) in concept.attributes.iter() {
            let attr_str = attribute.the();
            let the = Term::Constant(
                attr_str
                    .parse::<crate::artifact::Attribute>()
                    .expect("Failed to parse attribute name"),
            );
            premises.push(
                FactApplication::new(the, this.clone(), Term::var(name), attribute.cardinality)
                    .into(),
            );
        }

        DeductiveRule::new(concept.clone(), premises)
    }
}

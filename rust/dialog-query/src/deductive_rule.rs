use crate::artifact::Value;
use crate::attribute::Attribute;
use crate::fact_selector::FactSelector;
// use crate::plan::{EvaluationContext, EvaluationPlan};
// use crate::query::Store;
// use crate::stream::fork_stream;
use crate::term::Term;
use dialog_artifacts::{Entity, ValueDataType};
use std::collections::{HashMap, HashSet};
use std::fmt::Display;
use thiserror::Error;

/// Represents set of bindings used in the rule or formula applications. It is
/// effectively a map of terms (constant or variable) keyed by parameter names.
#[derive(Debug, Clone, PartialEq)]
pub struct Terms(HashMap<String, Term<Value>>);
impl Terms {
    pub fn new() -> Self {
        Self(HashMap::new())
    }
    /// Returns the term associated with the given parameter name, if has one.
    pub fn get(&self, name: &str) -> Option<&Term<Value>> {
        self.0.get(name)
    }
}

/// Represents a conclusion of the rule as a set of attribute descriptors keyed
/// by the rule parameter name. It is effectively describes decomposition into
/// facts with a shared entity.
#[derive(Debug, Clone, PartialEq)]
pub struct Conclusion {
    /// Every conclusion is required to have an entity to associate attributes
    /// with.
    this: Attribute<Entity>,
    /// Map of all attributes this entity should have to reach this conclusion.
    attributes: HashMap<String, Attribute<Value>>,
}

/// Query planner analyzes each premise to identify it's dependencies and budget
/// required to perform them. This struct represents result of succesful analysis.
#[derive(Debug, Clone, PartialEq)]
pub struct Analysis {
    dependencies: Dependencies,
    budget: usize,
}

/// Represents a deductive rule that can be applied creating a premise.
#[derive(Debug, Clone, PartialEq)]
pub struct DeductiveRule {
    /// Rule identifier used to look rules up by.
    name: String,
    /// Conclusion that this rule reaches if all premises hold. This is
    /// typically what datalog calls rule head.
    conclusion: Conclusion,
    /// Premises that must hold for rule to reach it's conclusion. Typically
    /// datalog calls these rule body.
    premises: Vec<Premise>,
}
impl DeductiveRule {
    /// Returns the names of the parameters for this rule.
    fn parameters(&self) -> HashSet<String> {
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
    fn analyze(&self) -> Result<Analysis, AnalyzerError> {
        // We will collect rule dependencies and compute their levels based on
        // their use in the rule premises.
        let mut dependencies = Dependencies::new();
        // We will collect all internal dependencies which correspond to
        // variable terms that are not shared with outside scope. We do so
        // in order to identify if there are any unresolvable dependencies
        // and in the local rule budget.
        let mut variables = Dependencies::new();
        let parameters = self.parameters();

        let mut budget: usize = 0;
        // Analyze each premise and account their dependencies into the rule's
        // dependencies and budget.
        for premise in self.premises.iter() {
            let analysis = premise.analyze()?;
            budget += analysis.budget;

            // Go over every dependency of every premise and estimate their
            // cost for the rule. If dependency is a parameter of the rule
            // it updates rule dependency levels accordingly, otherwise it
            // captures them in the internal dependencies in order to reflect
            // it in the budget.
            for (name, dependency) in analysis.dependencies.iter() {
                if parameters.contains(name) {
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
        parameters
            .iter()
            .find(|parameter| !dependencies.contains(parameter))
            .map_or(Ok(()), |parameter| {
                Err(AnalyzerError::UnusedParameter {
                    rule: self.clone(),
                    parameter: parameter.clone(),
                })
            })?;

        // Next we check if there is a required local variable and if so we
        // raise an error. If we have such variable it implies that we have a
        // premise(s) that require this variable, but there is no premise that
        // can provide it, which makes it impossible to execute such a rule.
        variables
            .iter()
            .find(|(_, level)| matches!(level, Level::Required))
            .map_or(Ok(()), |(variable, _)| {
                Err(AnalyzerError::RequiredLocalVariable {
                    rule: self.clone(),
                    variable: variable.to_string(),
                })
            })?;

        // If we got this far we know all the dependencies and can estimate a
        // overall budget for the rule execution.
        Ok(Analysis {
            budget: budget + variables.cost(),
            dependencies,
        })
    }
}
impl Display for DeductiveRule {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} {{", self.name);
        for (name, attribute) in self.conclusion.attributes.iter() {
            write!(f, "{}: {},", name, attribute.data_type)?;
        }
        write!(f, "}}")
    }
}

#[derive(Error, Debug, Clone, PartialEq)]
pub enum AnalyzerError {
    #[error("Rule {rule} does not makes use of the \"{parameter}\" parameter")]
    UnusedParameter {
        rule: DeductiveRule,
        parameter: String,
    },
    #[error("Rule {rule} application omits required parameter \"{parameter}\"")]
    RequiredParameter {
        rule: DeductiveRule,
        parameter: String,
    },
    #[error("Rule {rule} makes use of local {variable} that no premise can provide")]
    RequiredLocalVariable {
        rule: DeductiveRule,
        variable: String,
    },
}

/// Represents a rule application with the terms applied to corresponding
/// rule parameters.
#[derive(Debug, Clone, PartialEq)]
pub struct RuleApplication {
    /// Applied terms
    terms: Terms,
    /// Rule being applied
    rule: DeductiveRule,
}

impl RuleApplication {
    fn analyze(&self) -> Result<Analysis, AnalyzerError> {
        // First we analyze the rule itself identifying its dependencies and
        // execution budget.
        let analysis = self.rule.analyze()?;
        let mut dependencies = Dependencies::new();

        for (parameter, level) in analysis.dependencies.iter() {
            match level {
                // If some of the parameters is a required dependency of the
                // rule, but it was not applied rule application is invalid.
                Level::Required => {
                    self.terms
                        .get(parameter)
                        .ok_or_else(|| AnalyzerError::RequiredParameter {
                            rule: self.rule.clone(),
                            parameter: parameter.to_string(),
                        })?;
                }
                // If dependency is not required and applied term is not a
                // constant we propagate it into dependencies.
                Level::Desired(desire) => {
                    if let Some(Term::Variable { .. }) = self.terms.get(parameter) {
                        dependencies.desire(parameter.to_string(), *desire);
                    }
                }
            }
        }

        Ok(Analysis {
            dependencies,
            budget: analysis.budget,
        })
    }
    fn plan(&self) -> Plan {
        let _constants = self.terms.constants();
        Plan::None
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct Formula {
    operator: String,
    cells: Cells,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Cells(HashMap<String, Cell>);

#[derive(Debug, Clone, PartialEq)]
pub enum Cell {
    /// Reads from this cell
    Input { data_type: ValueDataType },
    /// Writes to this cell
    Output { data_type: ValueDataType },
    /// Reads if provided, writes if not provided
    Modal { data_type: ValueDataType },
}

#[derive(Debug, Clone, PartialEq)]
pub struct Dependencies(HashMap<String, Level>);
impl Dependencies {
    fn new() -> Self {
        Dependencies(HashMap::new())
    }
    fn cost(&self) -> usize {
        self.0
            .values()
            .filter_map(|d| match d {
                Level::Desired(cost) => Some(*cost),
                _ => None,
            })
            .sum()
    }

    pub fn iter(&self) -> impl Iterator<Item = (&str, &Level)> {
        self.0.iter().map(|(k, v)| (k.as_str(), v))
    }

    pub fn desire(&mut self, dependency: String, cost: usize) {
        let Dependencies(content) = self;
        if let Some(existing) = content.get(&dependency) {
            if let Level::Desired(prior) = existing {
                content.insert(dependency, Level::Desired(cost.max(*prior)));
            }
        } else {
            content.insert(dependency, Level::Desired(cost));
        }
    }

    pub fn require(&mut self, dependency: String) {
        self.0.insert(dependency, Level::Required);
    }

    /// Alters the dependency level to the lowest between current and provided
    /// levels. If dependency does not exist yet it is added. General idea
    /// behind picking lower ranking level is that if some premise is able to
    /// fulfill the requirement with a lower budget it will likely be picked
    /// to execute ahead of the ones that are more expensive, hence actual level
    /// is lower (🤔 perhaps average would be more accurate).
    pub fn update(&mut self, dependency: String, level: &Level) {
        let Dependencies(content) = self;
        if let Some(existing) = content.get(&dependency) {
            if let Level::Desired(prior) = existing {
                if let Level::Desired(desire) = level {
                    content.insert(dependency, Level::Desired(*prior.min(desire)));
                }
            } else {
                content.insert(dependency, level.clone());
            }
        }
        // If dependency was previously assumed to be required it is no longer
        else {
            content.insert(dependency, level.clone());
        }
    }

    pub fn contains(&self, dependency: &str) -> bool {
        let Dependencies(content) = self;
        content.contains_key(dependency)
    }

    pub fn required(&self) -> impl Iterator<Item = (&str, &Level)> {
        self.0.iter().filter_map(|(k, v)| match v {
            Level::Required => Some((k.as_str(), v)),
            Level::Desired(_) => None,
        })
    }
}

#[derive(Debug, Clone, PartialEq)]
enum Level {
    /// Required dependency must be satisfied
    Required,
    /// Desired dependency, the higher the number the higher the
    /// cost of omitting it.
    Desired(usize),
}

#[derive(Debug, Clone, PartialEq)]
pub struct FormulaApplication {
    operator: Formula,
    terms: Terms,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Premise {
    /// Statement who's evaluation results are included in the output. This is
    /// basically every possible statement that is not a negation.
    Include(Statement),
    /// Statement that exclude matches from the selection. This is basically
    /// a negetated statement.
    Exclude(Statement),
}

impl Premise {
    fn analyze(&self) -> Result<Analysis, AnalyzerError> {
        match self {
            Premise::Include(statement) => statement.analyze(),
            Premise::Exclude(statement) => statement.analyze(),
        }
    }
}

/// Statements that can be used by the rules.
#[derive(Debug, Clone, PartialEq)]
pub enum Statement {
    /// Fact selection.
    Select(FactSelector),
    /// Rule application
    ApplyRule(RuleApplication),
}

impl Statement {
    fn analyze(&self) -> Result<Analysis, AnalyzerError> {
        match self {
            Statement::Select(selector) => {
                let mut dependencies = Dependencies::new();

                if let Some(Term::Variable {
                    name: Some(name), ..
                }) = &selector.the
                {
                    dependencies.desire(name.clone(), 200)
                }

                if let Some(Term::Variable {
                    name: Some(name), ..
                }) = &selector.of
                {
                    dependencies.desire(name.clone(), 500)
                }

                if let Some(Term::Variable {
                    name: Some(name), ..
                }) = &selector.is
                {
                    dependencies.desire(name.clone(), 300)
                }

                Ok(Analysis {
                    dependencies,
                    budget: 100,
                })
            }
            Statement::ApplyRule(application) => application.analyze(),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum Operator {
    Rule(DeductiveRule),
    Formula(Formula),
}

impl Conclusion {}

impl Terms {
    fn constants(&self) -> HashMap<String, Value> {
        let Terms(terms) = self;
        let mut constants = HashMap::new();
        for (name, term) in terms.iter() {
            if let Term::Constant(value) = term {
                constants.insert(name.clone(), value.clone());
            }
        }
        constants
    }
}

enum Plan {
    None,
}

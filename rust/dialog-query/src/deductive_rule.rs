use crate::attribute::Attribute;
use crate::error::InconsistencyError;
use crate::fact_selector::FactSelector;
use crate::selection::Match;
use crate::VariableScope;
use crate::{EvaluationContext, Selection, Store, Term, Value};
use async_stream::try_stream;
use dialog_artifacts::ValueDataType;
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

    pub fn insert(&mut self, name: String, term: Term<Value>) {
        self.0.insert(name, term);
    }

    pub fn contains(&self, name: &str) -> bool {
        self.0.contains_key(name)
    }

    pub fn iter(&self) -> impl Iterator<Item = (&String, &Term<Value>)> {
        self.0.iter()
    }
}

/// Represents a conclusion of the rule as a set of attribute descriptors keyed
/// by the rule parameter name. It is effectively describes decomposition into
/// facts with a shared entity.
#[derive(Debug, Clone, PartialEq)]
pub struct Conclusion {
    /// Map of all attributes this entity should have to reach this conclusion.
    attributes: HashMap<String, Attribute<Value>>,
}
impl Conclusion {
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

/// Query planner analyzes each premise to identify it's dependencies and budget
/// required to perform them. This struct represents result of succesful analysis.
#[derive(Debug, Clone, PartialEq)]
pub struct Analysis {
    /// Base execution cost which does not include added costs captured in the
    /// dependencies.
    cost: usize,
    dependencies: Dependencies,
}

/// Represents a deductive rule that can be applied creating a premise.
#[derive(Debug, Clone, PartialEq)]
pub struct DeductiveRule {
    /// Rule identifier used to look rules up by.
    operator: String,
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
        let conclusion = &self.conclusion;
        // We will collect rule dependencies and compute their levels based on
        // their use in the rule premises.
        let mut dependencies = Dependencies::new();
        // We will collect all internal dependencies which correspond to
        // variable terms that are not shared with outside scope. We do so
        // in order to identify if there are any unresolvable dependencies
        // and in the local rule budget.
        let mut variables = Dependencies::new();

        let mut budget: usize = 0;
        // Analyze each premise and account their dependencies into the rule's
        // dependencies and budget.
        for premise in self.premises.iter() {
            let analysis = premise.analyze()?;
            budget += analysis.cost;

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

        // If we got this far we know all the dependencies and can estimate a
        // overall budget for the rule execution.
        Ok(Analysis {
            cost: budget + variables.cost(),
            dependencies,
        })
    }

    fn apply(&self, terms: Terms) -> Result<RuleApplication, AnalyzerError> {
        let application = RuleApplication::new(self.clone(), terms);
        application.analyze().and(Ok(application))
    }
}
impl Display for DeductiveRule {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} {{", self.operator);
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
    #[error("Formula {formula} application omits required cell \"{cell}\"")]
    RequiredCell { formula: Formula, cell: String },
    #[error("Rule {rule} makes use of local {variable} that no premise can provide")]
    RequiredLocalVariable {
        rule: DeductiveRule,
        variable: String,
    },
}

#[derive(Error, Debug, Clone, PartialEq)]
pub enum PlanError {
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
    #[error("Formula {formula} application omits required cell \"{cell}\"")]
    RequiredCell { formula: Formula, cell: String },
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
    fn new(rule: DeductiveRule, terms: Terms) -> Self {
        RuleApplication { rule, terms }
    }
    fn analyze(&self) -> Result<Analysis, AnalyzerError> {
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
                        dependencies.desire(parameter.to_string(), *desire);
                    }
                }
            }
        }

        Ok(Analysis {
            dependencies,
            cost: analysis.cost,
        })
    }
    fn plan(&self) -> Plan {
        let _constants = self.terms.constants();
        Plan::None
    }
}

/// Represents a set of named cells that formula operates on. Each cell also
/// describes whether it is required or optional and cost of it's omission.
#[derive(Debug, Clone, PartialEq)]
pub struct Cells(HashMap<String, Cell>);

/// Describes a cell of the formula.
#[derive(Debug, Clone, PartialEq)]
pub struct Cell {
    pub description: &'static str,
    pub requirement: Requirement,
    pub data_type: ValueDataType,
}

impl Cells {
    fn new() -> Self {
        Cells(HashMap::new())
    }

    pub fn iter(&self) -> impl Iterator<Item = (&str, &Cell)> {
        self.0.iter().map(|(k, v)| (k.as_str(), v))
    }

    pub fn add(&mut self, name: String, cell: Cell) -> &mut Self {
        self.0.insert(name, cell);
        self
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct Dependencies(HashMap<String, Requirement>);
impl Dependencies {
    fn new() -> Self {
        Dependencies(HashMap::new())
    }
    fn cost(&self) -> usize {
        self.0
            .values()
            .filter_map(|d| match d {
                Requirement::Derived(cost) => Some(*cost),
                _ => None,
            })
            .sum()
    }

    pub fn iter(&self) -> impl Iterator<Item = (&str, &Requirement)> {
        self.0.iter().map(|(k, v)| (k.as_str(), v))
    }

    pub fn desire(&mut self, dependency: String, cost: usize) {
        let Dependencies(content) = self;
        if let Some(existing) = content.get(&dependency) {
            if let Requirement::Derived(prior) = existing {
                content.insert(dependency, Requirement::Derived(cost.max(*prior)));
            }
        } else {
            content.insert(dependency, Requirement::Derived(cost));
        }
    }

    pub fn provide(&mut self, dependency: String) {
        self.desire(dependency, 0);
    }

    pub fn require(&mut self, dependency: String) {
        self.0.insert(dependency, Requirement::Required);
    }

    /// Alters the dependency level to the lowest between current and provided
    /// levels. If dependency does not exist yet it is added. General idea
    /// behind picking lower ranking level is that if some premise is able to
    /// fulfill the requirement with a lower budget it will likely be picked
    /// to execute ahead of the ones that are more expensive, hence actual level
    /// is lower (🤔 perhaps average would be more accurate).
    pub fn update(&mut self, dependency: String, requirement: &Requirement) {
        let Dependencies(content) = self;
        if let Some(existing) = content.get(&dependency) {
            if let Requirement::Derived(prior) = existing {
                if let Requirement::Derived(desire) = requirement {
                    content.insert(dependency, Requirement::Derived(*prior.min(desire)));
                }
            } else {
                content.insert(dependency, requirement.clone());
            }
        }
        // If dependency was previously assumed to be required it is no longer
        else {
            content.insert(dependency, requirement.clone());
        }
    }

    pub fn contains(&self, dependency: &str) -> bool {
        let Dependencies(content) = self;
        content.contains_key(dependency)
    }

    pub fn required(&self) -> impl Iterator<Item = (&str, &Requirement)> {
        self.0.iter().filter_map(|(k, v)| match v {
            Requirement::Required => Some((k.as_str(), v)),
            Requirement::Derived(_) => None,
        })
    }

    pub fn resolve(&self, name: &str) -> Requirement {
        match self.0.get(name) {
            Some(requirement) => requirement.clone(),
            None => Requirement::Derived(0),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
enum Requirement {
    /// Dependency that must be provided
    Required,
    /// Dependency that could be provided. If not provided it will be derived.
    /// Number represents cost of the deriviation.
    Derived(usize),
}

impl Requirement {
    pub fn is_required(&self) -> bool {
        matches!(self, Requirement::Required)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct Formula {
    /// Unique identifier for the formula.
    operator: String,
    /// Set of operands this formula operates on.
    operands: Cells,

    /// Base cost of applying this formula.
    cost: usize,
}
impl Formula {
    pub fn new(operator: &str) -> Self {
        Formula {
            operator: operator.to_string(),
            operands: Cells::new(),
            cost: 0,
        }
    }

    fn with_cell(&mut self, name: &str, cell: Cell) -> &mut Self {
        self.operands.add(name.into(), cell);
        self
    }

    pub fn apply(&self, terms: Terms) -> Result<FormulaApplication, AnalyzerError> {
        let application = FormulaApplication::new(self.clone(), terms);
        application.analyze().and(Ok(application))
    }

    pub fn provide<Apply: FormulaImplementation>(&self, apply: Apply) -> FormulaProvider<Apply> {
        FormulaProvider {
            formula: self.clone(),
            provider: apply,
        }
    }
}

trait FormulaImplementation: Fn(&mut FormulaContext) -> Result<(), FormulaEvaluationError> {}
impl<Apply: FormulaImplementation> FormulaImplementation for Apply {}

pub struct FormulaProvider<Apply>
where
    Apply: FormulaImplementation,
{
    formula: Formula,
    provider: Apply,
}

impl Display for Formula {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} {{", self.operator);
        for (name, cell) in self.operands.iter() {
            write!(f, "{}: {},", name, cell.data_type)?;
        }
        write!(f, "}}")
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct FormulaApplication {
    formula: Formula,
    terms: Terms,
}
impl FormulaApplication {
    pub fn new(formula: Formula, terms: Terms) -> Self {
        FormulaApplication { formula, terms }
    }

    pub fn analyze(&self) -> Result<Analysis, AnalyzerError> {
        let mut dependencies = Dependencies::new();

        // Iterate over all the operands of the formula and to capture
        // requirements. If required operand is not provided in this
        // application we raise an error, because this is not a valid
        // application.
        for (name, cell) in self.formula.operands.iter() {
            match cell.requirement {
                // If cell is required but not provided we raise an error
                // Otherwise we capture requirement in our dependencies
                Requirement::Required => {
                    if !self.terms.contains(name) {
                        Err(AnalyzerError::RequiredCell {
                            formula: self.formula.clone(),
                            cell: name.into(),
                        })
                    } else {
                        dependencies.update(name.into(), &cell.requirement);
                        Ok(())
                    }
                }
                // If cell can be derived we simply update our dependencies with
                // the cell's requirement.
                Requirement::Derived(_) => {
                    dependencies.update(name.into(), &cell.requirement);
                    Ok(())
                }
            }?;

            dependencies.update(name.to_string(), &cell.requirement);
        }

        Ok(Analysis {
            dependencies,
            cost: self.formula.cost,
        })
    }

    pub fn plan(&self, scope: VariableScope) -> Result<Plan, PlanError> {
        Ok(Plan::None)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct FormulaApplicationPlan {
    analysis: Analysis,
    terms: Terms,
    scope: VariableScope,
}
impl FormulaApplicationPlan {
    /// Resolves Terms from the given source. When term already a constant it
    /// ensures that it is consistent with the constant value in the source.
    /// When term is a variable it attempts to resolve the constant from the
    /// source. If source does not contain a value for the parameter it copies
    /// variable into returned terms unless it is a required parameter. If
    /// parameter is required and value is not present in source
    /// InconsistencyError is returned.
    pub fn resolve(&self, source: Match) -> Result<Terms, InconsistencyError> {
        let mut parameters = self.terms.clone();
        let dependencies = &self.analysis.dependencies;
        for (name, parameter) in self.terms.iter() {
            match parameter {
                Term::Constant(_) => {
                    parameters.insert(name.clone(), parameter.clone());
                }
                Term::Variable { .. } => match source.get(parameter) {
                    Ok(value) => {
                        parameters.insert(name.clone(), Term::Constant(value));
                    }
                    Err(error) => {
                        let requirement = dependencies.resolve(name);
                        match requirement {
                            Requirement::Required => {
                                Err(error)?;
                            }
                            Requirement::Derived(_) => {
                                parameters.insert(name.clone(), parameter.clone());
                            }
                        }
                    }
                },
            }
        }

        Ok(parameters)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct FormulaContext(HashMap<String, Value>);
impl FormulaContext {
    fn read(&self, name: &str) -> Result<Value, FormulaEvaluationError> {
        self.0
            .get(name)
            .cloned()
            .ok_or(FormulaEvaluationError::ReadError {
                name: name.to_string(),
            })
    }
    fn write(&mut self, name: &str, value: Value) -> Result<(), FormulaEvaluationError> {
        self.0.insert(name.to_string(), value);
        Ok(())
    }
}

#[derive(Error, Debug, Clone, PartialEq)]
enum FormulaEvaluationError {
    #[error("Required cell '{name}' has no value")]
    ReadError { name: String },
}

#[test]
fn define_inc_formula() {
    let formula = Formula::new("inc")
        .with_cell(
            "of",
            Cell {
                description: &"Increment operation",
                requirement: Requirement::Required,
                data_type: ValueDataType::SignedInt,
            },
        )
        .with_cell(
            "is",
            Cell {
                description: &"Incremented result",
                requirement: Requirement::Derived(0),
                data_type: ValueDataType::SignedInt,
            },
        );

    formula.provide(|cells| {
        let of = cells.read("of")?;

        cells.write("is", of + 1)
    });
}

#[derive(Debug, Clone, PartialEq)]
pub enum Premise {
    /// Fact selection.
    Select(FactSelector),
    /// Rule application
    ApplyRule(RuleApplication),
    /// Formula application
    ApplyFormula(FormulaApplication),
    /// Statement that exclude matches from the selection. This is basically
    /// a negetated statement.
    Exclude(Application),
}

impl Premise {
    fn analyze(&self) -> Result<Analysis, AnalyzerError> {
        match self {
            Premise::Select(selector) => selector.analyze(),
            Premise::ApplyFormula(application) => application.analyze(),
            Premise::ApplyRule(application) => application.analyze(),
            // Negation requires that all of the underlying dependencies to be
            // derived before the execution. That is why we mark all of the
            // underlying dependencies as required.
            Premise::Exclude(statement) => {
                let mut dependencies = Dependencies::new();
                let analysis = statement.analyze()?;
                for (name, _) in analysis.dependencies.iter() {
                    dependencies.require(name.into());
                }

                Ok(Analysis {
                    dependencies,
                    cost: analysis.cost,
                })
            }
        }
    }
}

impl From<FormulaApplication> for Premise {
    fn from(application: FormulaApplication) -> Self {
        Premise::ApplyFormula(application)
    }
}

impl From<RuleApplication> for Premise {
    fn from(application: RuleApplication) -> Self {
        Premise::ApplyRule(application)
    }
}

impl From<FactSelector> for Premise {
    fn from(selector: FactSelector) -> Self {
        Premise::Select(selector)
    }
}

impl FactSelector {
    fn analyze(&self) -> Result<Analysis, AnalyzerError> {
        let mut dependencies = Dependencies::new();

        if let Some(Term::Variable {
            name: Some(name), ..
        }) = &self.the
        {
            dependencies.desire(name.clone(), 200)
        }

        if let Some(Term::Variable {
            name: Some(name), ..
        }) = &self.of
        {
            dependencies.desire(name.clone(), 500)
        }

        if let Some(Term::Variable {
            name: Some(name), ..
        }) = &self.is
        {
            dependencies.desire(name.clone(), 300)
        }

        Ok(Analysis {
            dependencies,
            cost: 100,
        })
    }
}

/// Statements that can be used by the rules.
#[derive(Debug, Clone, PartialEq)]
pub enum Application {
    /// Fact selection.
    Select(FactSelector),
    /// Rule application
    ApplyRule(RuleApplication),
    /// Formula application
    ApplyFormula(FormulaApplication),
}

impl Application {
    fn analyze(&self) -> Result<Analysis, AnalyzerError> {
        match self {
            Application::Select(selector) => selector.analyze(),
            Application::ApplyRule(application) => application.analyze(),
            Application::ApplyFormula(application) => application.analyze(),
        }
    }

    fn not(&self) -> Premise {
        Premise::Exclude(self.clone())
    }
}

impl From<RuleApplication> for Application {
    fn from(application: RuleApplication) -> Self {
        Application::ApplyRule(application)
    }
}

impl From<FormulaApplication> for Application {
    fn from(application: FormulaApplication) -> Self {
        Application::ApplyFormula(application)
    }
}

impl From<FactSelector> for Application {
    fn from(selector: FactSelector) -> Self {
        Application::Select(selector)
    }
}

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
    Formula(FormulaApplicationPlan),
}

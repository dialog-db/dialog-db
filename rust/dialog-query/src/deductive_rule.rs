use crate::artifact::{Entity, ValueDataType};
use crate::attribute::Attribute;
use crate::fact_selector::FactSelector;
use crate::fact_selector::{BASE_COST, ENTITY_COST, VALUE_COST};
use crate::formula::{FormulaApplication, FormulaApplicationPlan};
use crate::join::Join;
use crate::plan::EvaluationPlan;
use crate::{try_stream, QueryError};
use crate::{EvaluationContext, Selection, Store, Term, Type, Value};
use crate::{FactSelectorPlan, VariableScope};
use core::cmp::Ordering;
use futures_util::{stream, TryStreamExt};
use std::collections::{HashMap, HashSet};
use std::fmt::{Debug, Display};
use std::usize;
use thiserror::Error;
use tokio;

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

    /// Inserts a new term binding for the given parameter name.
    /// If the parameter already exists, it will be overwritten.
    pub fn insert(&mut self, name: String, term: Term<Value>) {
        self.0.insert(name, term);
    }

    /// Checks if a term binding exists for the given parameter name.
    pub fn contains(&self, name: &str) -> bool {
        self.0.contains_key(name)
    }

    /// Returns an iterator over all parameter-term pairs in this binding set.
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

/// Query planner analyzes each premise to identify it's dependencies and budget
/// required to perform them. This struct represents result of succesful analysis.
#[derive(Debug, Clone, PartialEq)]
pub struct Analysis {
    /// Base execution cost which does not include added costs captured in the
    /// dependencies.
    pub cost: usize,
    pub dependencies: Dependencies,
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
    pub fn apply(&self, terms: Terms) -> Result<RuleApplication, AnalyzerError> {
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

/// Represents a concept which is a set of attributes that define an entity type.
/// Concepts are similar to tables in relational databases but are more flexible
/// as they can be derived from rules rather than just stored directly.
#[derive(Debug, Clone, PartialEq)]
pub struct Concept {
    /// Concept identifier used to look concepts up by.
    pub operator: String,
    /// Map of attribute names to their definitions for this concept.
    pub attributes: HashMap<String, Attribute<Value>>,
}

/// Represents an application of a concept with specific term bindings.
/// This is used when querying for entities that match a concept pattern.
/// Note: The name has a typo (should be ConceptApplication) but is kept for compatibility.
#[derive(Debug, Clone, PartialEq)]
pub struct ConcetApplication {
    /// The term bindings for this concept application.
    pub terms: Terms,
    /// The concept being applied.
    pub concept: Concept,
}

impl ConcetApplication {
    /// Analyzes this concept application to determine its dependencies and execution cost.
    /// All concept applications require the "this" entity parameter and desire all
    /// concept attributes as dependencies.
    fn analyze(&self) -> Result<Analysis, AnalyzerError> {
        let mut dependencies = Dependencies::new();
        dependencies.desire("this".into(), ENTITY_COST);

        for (name, _) in self.concept.attributes.iter() {
            dependencies.desire(name.to_string(), VALUE_COST);
        }

        Ok(Analysis {
            cost: BASE_COST,
            dependencies,
        })
    }

    /// Creates an execution plan for this concept application.
    /// Converts the concept application into a set of fact selector premises
    /// that can be executed to find matching entities.
    fn plan(&self, scope: &VariableScope) -> Result<ConceptPlan, PlanError> {
        let mut provides = VariableScope::new();
        let mut cost = 0;
        if let Some(this) = self.terms.get("this") {
            if !scope.contains(&this) {
                provides.add(&this);
                cost += ENTITY_COST
            }
        }

        // Convert the "this" term from Term<Value> to Term<Entity>
        let this_entity: Term<Entity> = if let Some(this_value) = self.terms.get("this") {
            match this_value {
                Term::Variable { name, .. } => Term::<Entity>::Variable {
                    name: name.clone(),
                    _type: Type::default(),
                },
                Term::Constant(value) => {
                    // If it's a constant, it should be an Entity value
                    if let Value::Entity(entity) = value {
                        Term::Constant(entity.clone())
                    } else {
                        // Fallback to a variable if not an entity
                        Term::<Entity>::var(&format!("this_{}", self.concept.operator))
                    }
                }
            }
        } else {
            // Create a unique variable if "this" is not provided
            Term::<Entity>::var(&format!("this_{}", self.concept.operator))
        };

        let mut premises = vec![];

        // go over dependencies to add all the terms that will be derived
        // by the application to the `provides` list.
        for (name, attribute) in self.concept.attributes.iter() {
            let parameter = self.terms.get(name);
            // If parameter was not provided we add it to the provides set
            if let Some(term) = parameter {
                if !scope.contains(&term) {
                    provides.add(&term);
                    cost += VALUE_COST
                }

                let select = FactSelector::new()
                    .the(attribute.the())
                    .of(this_entity.clone())
                    .is(term.clone());

                premises.push(select.into());
            }
        }

        let mut planner = Planner::new(&premises);
        let (added_cost, conjuncts) = planner.plan(scope)?;

        Ok(ConceptPlan {
            cost: cost + added_cost,
            provides,
            conjuncts,
        })
    }
}

impl Display for ConcetApplication {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} {{", self.concept.operator)?;
        for (name, term) in self.terms.iter() {
            write!(f, "{}: {},", name, term)?;
        }

        write!(f, "}}")
    }
}

/// Execution plan for a concept application.
/// Contains the cost estimate, variables that will be provided by execution,
/// and the individual sub-plans that need to be executed and joined.
#[derive(Debug, Clone, PartialEq)]
pub struct ConceptPlan {
    /// Estimated execution cost for this plan.
    pub cost: usize,
    /// Variables that will be bound by executing this plan.
    pub provides: VariableScope,
    /// Individual sub-plans that must all succeed for the concept to match.
    pub conjuncts: Vec<Plan>,
}
impl EvaluationPlan for ConceptPlan {
    fn cost(&self) -> usize {
        self.cost
    }
    fn provides(&self) -> &VariableScope {
        &self.provides
    }
    fn evaluate<S: Store, M: Selection>(&self, context: EvaluationContext<S, M>) -> impl Selection {
        let join = Join::from(self.conjuncts.clone());
        join.evaluate(context)
    }
}

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
}

impl From<AnalyzerError> for PlanError {
    fn from(error: AnalyzerError) -> Self {
        match error {
            AnalyzerError::UnusedParameter { rule, parameter } => {
                PlanError::UnusedParameter { rule, parameter }
            }
            AnalyzerError::RequiredParameter { rule, parameter } => {
                PlanError::OmitsRequiredParameter { rule, parameter }
            }
            AnalyzerError::OmitsRequiredCell { formula, cell } => {
                PlanError::OmitsRequiredCell { formula, cell }
            }
            AnalyzerError::RequiredLocalVariable { rule, variable } => {
                PlanError::RequiredLocalVariable { rule, variable }
            }
        }
    }
}

/// Errors that can occur during query planning.
/// These errors indicate problems that prevent creating a valid execution plan.
#[derive(Error, Debug, Clone, PartialEq)]
pub enum PlanError {
    #[error("Rule {rule} does not makes use of the \"{parameter}\" parameter")]
    UnusedParameter {
        rule: DeductiveRule,
        parameter: String,
    },
    #[error("Rule {rule} application omits required parameter \"{parameter}\"")]
    OmitsRequiredParameter {
        rule: DeductiveRule,
        parameter: String,
    },
    #[error("Rule {rule} makes use of local {variable} that no premise can provide")]
    RequiredLocalVariable {
        rule: DeductiveRule,
        variable: String,
    },
    #[error(
        "Rule {rule} application passes unbound {term} into a required parameter \"{parameter}\""
    )]
    UnboundRuleParameter {
        rule: DeductiveRule,
        parameter: String,
        term: Term<Value>,
    },

    #[error(
        "Premise {application} passes unbound variable in a required parameter \"{parameter}\""
    )]
    UnboundParameter {
        application: Application,
        parameter: String,
        term: Term<Value>,
    },

    #[error("Formula {formula} application omits required cell \"{cell}\"")]
    OmitsRequiredCell { formula: &'static str, cell: String },
    #[error(
        "Formula {formula} application can not pass blank '_' variable in required cell \"{cell}\""
    )]
    BlankRequiredCell { formula: &'static str, cell: String },

    #[error(
        "Formula {formula} application passes '{variable}' unbound variable into a required cell \"{cell}\""
    )]
    UnboundRequiredCell {
        formula: &'static str,
        cell: String,
        variable: String,
    },

    #[error(
        "Formula {formula} application passes unbound {parameter} into a required cell \"{cell}\""
    )]
    UnboundFormulaParameter {
        formula: &'static str,
        cell: String,
        parameter: Term<Value>,
    },

    #[error("Fact application {selector} requires at least one bound parameter")]
    UnconstrainedSelector { selector: FactSelector },

    #[error("Unexpected error occured while planning a rule")]
    UnexpectedError,
}

impl From<PlanError> for QueryError {
    fn from(error: PlanError) -> Self {
        QueryError::PlanningError {
            message: error.to_string(),
        }
    }
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
    /// Creates a new rule application with the given rule and term bindings.
    pub fn new(rule: DeductiveRule, terms: Terms) -> Self {
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
    /// Creates an execution plan for this rule application.
    /// Validates that all required variables are in scope and plans execution
    /// of all rule premises in optimal order.
    fn plan(&self, scope: &VariableScope) -> Result<RuleApplicationPlan, PlanError> {
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

        let mut planner = Planner::new(&self.rule.premises);
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
        write!(f, "{} {{", self.rule.operator)?;
        for (name, term) in self.terms.iter() {
            write!(f, "{}: {},", name, term)?;
        }
        write!(f, "}}")
    }
}

/// Query planner that optimizes the order of premise execution based on cost
/// and dependency analysis. Uses a state machine approach to iteratively
/// select the best premise to execute next.
pub enum Planner<'a> {
    /// Initial state with unprocessed premises.
    Idle { premises: &'a Vec<Premise> },
    /// Processing state with cached candidates and current scope.
    Active {
        candidates: Vec<PlanCandidate<'a>>,
        scope: VariableScope,
    },
}

impl<'a> Planner<'a> {
    /// Creates a new planner for the given premises.
    pub fn new(premises: &'a Vec<Premise>) -> Self {
        Self::Idle { premises }
    }

    /// Helper to create a planning error from failed candidates.
    /// Returns the first error found, or UnexpectedError if none.
    fn fail(candidates: &[PlanCandidate]) -> Result<Plan, PlanError> {
        for candidate in candidates {
            match &candidate.result {
                Err(error) => {
                    return Err(error.clone());
                }
                _ => {}
            }
        }

        return Err(PlanError::UnexpectedError);
    }

    /// Checks if planning is complete (all premises have been planned).
    fn done(&self) -> bool {
        match self {
            Self::Idle { .. } => false,
            Self::Active { candidates, .. } => candidates.len() == 0,
        }
    }

    /// Creates an optimized execution plan for all premises.
    /// Returns the total cost and ordered list of sub-plans to execute.
    pub fn plan(&mut self, scope: &VariableScope) -> Result<(usize, Vec<Plan>), PlanError> {
        let plan = self.top(scope)?;
        let mut cost = plan.cost();

        let mut scope = scope.clone();
        let mut delta = scope.extend(plan.provides());
        let mut conjuncts = vec![plan];

        while !self.done() {
            let plan = self.top(&delta)?;

            cost += plan.cost();
            delta = scope.extend(plan.provides());

            conjuncts.push(plan);
        }

        Ok((cost, conjuncts))
    }
    /// Selects and returns the best premise to execute next based on cost.
    /// Updates the planner state by removing the selected premise from candidates.
    fn top(&mut self, differential: &VariableScope) -> Result<Plan, PlanError> {
        match self {
            Planner::Idle { premises } => {
                let mut best: Option<(Plan, usize)> = None;
                let mut candidates = vec![];
                for (index, premise) in premises.iter().enumerate() {
                    let analysis = premise.analyze()?;
                    let result = premise.plan(differential);

                    // Check if this is the best plan so far
                    if let Ok(plan) = &result {
                        if let Some((top, _)) = &best {
                            if plan < top {
                                best = Some((plan.clone(), index));
                            }
                        } else {
                            best = Some((plan.clone(), index));
                        }
                    }

                    let mut dependencies = VariableScope::new();

                    for (name, _) in analysis.dependencies.iter() {
                        dependencies.variables.insert(name.into());
                    }

                    candidates.push(PlanCandidate {
                        premise,
                        dependencies,
                        result,
                    });
                }

                if let Some((plan, index)) = best {
                    candidates.remove(index);
                    *self = Planner::Active {
                        candidates,
                        scope: differential.clone(),
                    };

                    Ok(plan)
                } else {
                    Self::fail(&candidates)
                }
            }
            Planner::Active {
                candidates, scope, ..
            } => {
                let mut best: Option<(Plan, usize)> = None;
                for (index, candidate) in candidates.iter_mut().enumerate() {
                    // Check if we need to recompute based on delta
                    if candidate.dependencies.intersects(&differential) {
                        candidate.plan(&scope);
                    }

                    if let Ok(plan) = &candidate.result {
                        if let Some((top, _)) = &best {
                            if plan < top {
                                best = Some((plan.clone(), index));
                            }
                        } else {
                            best = Some((plan.clone(), index));
                        }
                    }
                }

                if let Some((plan, index)) = best {
                    candidates.remove(index);

                    Ok(plan)
                } else {
                    Self::fail(&candidates)
                }
            }
        }
    }
}

/// Represents a premise candidate during query planning.
/// Caches the premise's dependencies and planning result to avoid recomputation.
#[derive(Debug, Clone)]
pub struct PlanCandidate<'a> {
    /// Reference to the premise being planned.
    pub premise: &'a Premise,
    /// Variables that this premise depends on.
    pub dependencies: VariableScope,
    /// Cached planning result for this premise.
    pub result: Result<Plan, PlanError>,
}

impl<'a> PlanCandidate<'a> {
    /// Re-plans this premise with the given scope and updates the cached result.
    fn plan(&mut self, scope: &VariableScope) -> &Self {
        self.result = self.premise.plan(scope);
        self
    }
}

/// Execution plan for a rule application.
/// Contains all information needed to execute the rule and produce results.
#[derive(Debug, Clone, PartialEq)]
pub struct RuleApplicationPlan {
    /// Total estimated execution cost.
    pub cost: usize,
    /// Term bindings for the rule parameters.
    pub terms: Terms,
    /// Ordered list of sub-plans to execute.
    pub conjuncts: Vec<Plan>,
    /// Variables that will be provided by this plan.
    pub provides: VariableScope,
    /// The rule being executed.
    pub rule: DeductiveRule,
}

impl RuleApplicationPlan {
    /// Evaluates this rule application plan against the provided context.
    pub fn eval<S: Store, M: Selection>(&self, context: EvaluationContext<S, M>) -> impl Selection {
        Self::eval_helper(context.store, context.selection, self.conjuncts.clone())
    }

    /// Helper function that recursively evaluates conjuncts in order.
    pub fn eval_helper<S: Store, M: Selection>(
        store: S,
        source: M,
        conjuncts: Vec<Plan>,
    ) -> impl Selection {
        try_stream! {
            match conjuncts.as_slice() {
                [] => {
                    for await each in source {
                        yield each?;
                    }
                }
                [plan, rest @ ..] => {
                    let selection = plan.evaluate(EvaluationContext {
                        store: store.clone(),
                        selection: source
                    });



                    let output = Self::eval_helper(
                        store,
                        selection,
                        rest.to_vec()
                    );

                    for await each in output {
                        yield each?;
                    }
                }
            }
        }
    }
}

impl EvaluationPlan for RuleApplicationPlan {
    fn cost(&self) -> usize {
        self.cost
    }
    fn provides(&self) -> &VariableScope {
        &self.provides
    }
    fn evaluate<S: Store, M: Selection>(&self, context: EvaluationContext<S, M>) -> impl Selection {
        let join = Join::from(self.conjuncts.clone());
        join.evaluate(context)
    }
}

// The evaluate method is now part of the EvaluationPlan trait implementation above

// yield_all function removed as it's no longer needed

/// Represents a set of named cells that formula operates on. Each cell also
/// describes whether it is required or optional and cost of its omission.
#[derive(Debug, Clone, PartialEq)]
pub struct Cells(HashMap<String, Cell>);

/// Describes a cell of the formula - a named parameter with type and requirement info.
#[derive(Debug, Clone, PartialEq)]
pub struct Cell {
    /// Name of the cell parameter.
    pub name: &'static str,
    /// Human-readable description of what this cell represents.
    pub description: &'static str,
    /// Whether this cell is required or can be derived.
    pub requirement: Requirement,
    /// Expected data type for values in this cell.
    pub data_type: ValueDataType,
}

impl Cells {
    /// Creates a new empty cell collection.
    pub fn new() -> Self {
        Cells(HashMap::new())
    }

    /// Returns an iterator over all cells as (name, cell) pairs.
    pub fn iter(&self) -> impl Iterator<Item = (&str, &Cell)> {
        self.0.iter().map(|(k, v)| (k.as_str(), v))
    }

    /// Gets a cell by name if it exists.
    pub fn get(&self, name: &str) -> Option<&Cell> {
        self.0.get(name)
    }

    /// Adds a new cell to this collection.
    pub fn add(&mut self, name: String, cell: Cell) -> &mut Self {
        self.0.insert(name, cell);
        self
    }
}

/// Tracks dependencies and their requirement levels for rules and formulas.
/// Used during analysis to determine execution costs and validate requirements.
#[derive(Debug, Clone, PartialEq)]
pub struct Dependencies(HashMap<String, Requirement>);

impl Dependencies {
    /// Creates a new empty dependency set.
    pub fn new() -> Self {
        Dependencies(HashMap::new())
    }

    /// Calculates the total cost of all derived dependencies.
    /// Required dependencies don't contribute to cost as they must be provided.
    pub fn cost(&self) -> usize {
        self.0
            .values()
            .filter_map(|d| match d {
                Requirement::Derived(cost) => Some(*cost),
                _ => None,
            })
            .sum()
    }

    /// Returns an iterator over all dependencies as (name, requirement) pairs.
    pub fn iter(&self) -> impl Iterator<Item = (&str, &Requirement)> {
        self.0.iter().map(|(k, v)| (k.as_str(), v))
    }

    /// Adds or updates a derived dependency with the given cost.
    /// If dependency already exists as derived, keeps the maximum cost.
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

    /// Marks a dependency as provided (zero cost derived).
    pub fn provide(&mut self, dependency: String) {
        self.desire(dependency, 0);
    }

    /// Marks a dependency as required - must be provided externally.
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

    /// Checks if a dependency exists in this set.
    pub fn contains(&self, dependency: &str) -> bool {
        let Dependencies(content) = self;
        content.contains_key(dependency)
    }

    /// Returns an iterator over only the required dependencies.
    pub fn required(&self) -> impl Iterator<Item = (&str, &Requirement)> {
        self.0.iter().filter_map(|(k, v)| match v {
            Requirement::Required => Some((k.as_str(), v)),
            Requirement::Derived(_) => None,
        })
    }

    /// Gets the requirement level for a dependency, defaulting to Derived(0) if not present.
    pub fn resolve(&self, name: &str) -> Requirement {
        match self.0.get(name) {
            Some(requirement) => requirement.clone(),
            None => Requirement::Derived(0),
        }
    }
}

/// Represents the requirement level for a dependency in a rule or formula.
#[derive(Debug, Clone, PartialEq)]
pub enum Requirement {
    /// Dependency that must be provided externally - cannot be derived.
    Required,
    /// Dependency that could be provided. If not provided it will be derived.
    /// Number represents cost of the derivation.
    Derived(usize),
}

impl Requirement {
    /// Checks if this is a required (non-derivable) dependency.
    pub fn is_required(&self) -> bool {
        matches!(self, Requirement::Required)
    }
}

/// Represents a premise in a rule - a condition that must be satisfied.
/// Can be either a positive application or a negated exclusion.
#[derive(Debug, Clone, PartialEq)]
pub enum Premise {
    /// A positive premise that produces matches.
    Apply(Application),
    /// A negated premise that excludes matches from the selection.
    Exclude(Negation),
}

impl Premise {
    /// Creates an execution plan for this premise within the given variable scope.
    pub fn plan(&self, scope: &VariableScope) -> Result<Plan, PlanError> {
        match self {
            Premise::Apply(application) => application.plan(scope).map(Plan::Application),
            Premise::Exclude(negation) => negation.plan(scope).map(Plan::Negation),
        }
    }

    /// Analyzes this premise to determine its dependencies and cost.
    fn analyze(&self) -> Result<Analysis, AnalyzerError> {
        match self {
            Premise::Apply(application) => application.analyze(),
            // Negation requires that all of the underlying dependencies to be
            // derived before the execution. That is why we mark all of the
            // underlying dependencies as required.
            Premise::Exclude(negation) => negation.analyze(),
        }
    }
}

impl Display for Premise {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Premise::Apply(application) => Display::fmt(&application, f),
            Premise::Exclude(negation) => Display::fmt(&negation, f),
        }
    }
}

impl From<FormulaApplication> for Premise {
    fn from(application: FormulaApplication) -> Self {
        Premise::Apply(Application::ApplyFormula(application))
    }
}

impl From<RuleApplication> for Premise {
    fn from(application: RuleApplication) -> Self {
        Premise::Apply(Application::ApplyRule(application))
    }
}

impl From<FactSelector> for Premise {
    fn from(selector: FactSelector) -> Self {
        Premise::Apply(Application::Select(selector))
    }
}

impl From<&FactSelector> for Premise {
    fn from(selector: &FactSelector) -> Self {
        Premise::Apply(Application::Select(selector.clone()))
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

// FactSelectorPlan's EvaluationPlan implementation is in fact_selector.rs

/// Represents a negated application that excludes matching results.
/// Used in rules to specify conditions that must NOT hold.
#[derive(Debug, Clone, PartialEq)]
pub struct Negation(Application);

impl Negation {
    /// Analyzes this negation to determine dependencies and cost.
    /// All dependencies become required since negation must fully evaluate its condition.
    pub fn analyze(&self) -> Result<Analysis, AnalyzerError> {
        let Negation(application) = self;
        let mut dependencies = Dependencies::new();
        let analysis = application.analyze()?;
        for (name, _) in analysis.dependencies.iter() {
            dependencies.require(name.into());
        }

        Ok(Analysis {
            dependencies,
            cost: analysis.cost,
        })
    }
    /// Creates an execution plan for this negation within the given variable scope.
    fn plan(&self, scope: &VariableScope) -> Result<NegationPlan, PlanError> {
        let Negation(application) = self;
        let plan = application.plan(&scope)?;

        Ok(plan.not())
    }
}

impl Display for Negation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let Negation(application) = self;
        write!(f, "! {}", application)
    }
}

/// Execution plan for a negated application.
/// Does not provide any variables since negation only filters matches.
#[derive(Debug, Clone, PartialEq)]
pub struct NegationPlan {
    /// The underlying application plan that will be negated
    pub application: ApplicationPlan,
    /// Variables provided by this plan (always empty for negation)
    pub provides: VariableScope,
}

impl NegationPlan {
    /// Creates a new negation plan from an application plan.
    pub fn not(application: ApplicationPlan) -> Self {
        Self {
            application,
            provides: VariableScope::new(),
        }
    }
    // evaluate method is now part of the EvaluationPlan trait implementation
}

/// Represents different types of applications that can be used as premises in rules.
/// Each variant corresponds to a different kind of query operation.
#[derive(Debug, Clone, PartialEq)]
pub enum Application {
    /// Direct fact selection from the knowledge base
    Select(FactSelector),
    /// Concept realization - matching entities against concept patterns
    Realize(ConcetApplication),
    /// Application of another deductive rule
    ApplyRule(RuleApplication),
    /// Application of a formula for computation
    ApplyFormula(FormulaApplication),
}

impl Application {
    /// Analyzes this application to determine its dependencies and base cost.
    fn analyze(&self) -> Result<Analysis, AnalyzerError> {
        match self {
            Application::Select(selector) => selector.analyze(),
            Application::Realize(concept) => concept.analyze(),
            Application::ApplyRule(application) => application.analyze(),
            Application::ApplyFormula(application) => application.analyze(),
        }
    }

    /// Creates an execution plan for this application within the given variable scope.
    fn plan(&self, scope: &VariableScope) -> Result<ApplicationPlan, PlanError> {
        match self {
            Application::Select(select) => select.plan(&scope).map(ApplicationPlan::Select),
            Application::Realize(concept) => concept.plan(&scope).map(ApplicationPlan::Concept),
            Application::ApplyRule(application) => {
                application.plan(scope).map(ApplicationPlan::Rule)
            }

            Application::ApplyFormula(application) => {
                application.plan(scope).map(ApplicationPlan::Formula)
            }
        }
    }

    /// Creates a negated premise from this application.
    pub fn not(&self) -> Premise {
        Premise::Exclude(Negation(self.clone()))
    }
}

impl Display for Application {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Application::Select(application) => Display::fmt(application, f),
            Application::Realize(application) => Display::fmt(application, f),
            Application::ApplyFormula(application) => Display::fmt(application, f),
            Application::ApplyRule(application) => Display::fmt(application, f),
        }
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

/// Execution plan for different types of applications.
/// Contains the optimized execution strategy for each application type.
#[derive(Debug, Clone, PartialEq)]
pub enum ApplicationPlan {
    /// Plan for fact selection operations
    Select(FactSelectorPlan),
    /// Plan for concept realization operations
    Concept(ConceptPlan),
    /// Plan for rule application operations
    Rule(RuleApplicationPlan),
    /// Plan for formula application operations
    Formula(FormulaApplicationPlan),
}

impl ApplicationPlan {
    /// Converts this application plan into a negated plan.
    pub fn not(self) -> NegationPlan {
        NegationPlan::not(self)
    }
    // evaluate method is now part of the EvaluationPlan trait implementation
}

/// Top-level execution plan that can be either a positive application or a negation.
/// Used by the query planner to organize premise execution.
#[derive(Debug, Clone, PartialEq)]
pub enum Plan {
    /// Positive application that produces matches
    Application(ApplicationPlan),
    /// Negative application that filters out matches
    Negation(NegationPlan),
}

// evaluate method is now part of the EvaluationPlan trait implementation

impl PartialOrd for Plan {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match (self, other) {
            (Plan::Application(_), Plan::Negation(_)) => Some(core::cmp::Ordering::Less),
            (Plan::Negation(_), Plan::Application(_)) => Some(core::cmp::Ordering::Greater),
            (Plan::Application(left), Plan::Application(right)) => left.partial_cmp(right),
            (Plan::Negation(left), Plan::Negation(right)) => left.partial_cmp(right),
        }
    }
}

impl PartialOrd for ApplicationPlan {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.cost().partial_cmp(&other.cost())
    }
}

impl PartialOrd for NegationPlan {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.cost().partial_cmp(&other.cost())
    }
}

// EvaluationPlan trait is imported from crate::plan module

impl EvaluationPlan for Plan {
    fn cost(&self) -> usize {
        match self {
            Plan::Application(plan) => plan.cost(),
            Plan::Negation(plan) => plan.cost(),
        }
    }

    fn provides(&self) -> &VariableScope {
        match self {
            Plan::Application(plan) => plan.provides(),
            Plan::Negation(plan) => plan.provides(),
        }
    }

    fn evaluate<S: Store, M: Selection>(&self, context: EvaluationContext<S, M>) -> impl Selection {
        let source = self.clone();
        try_stream! {
            match source {
                Plan::Application(plan) => {
                    for await output in plan.evaluate(context) {
                        yield output?
                    }
                },
                Plan::Negation(plan) => {
                    for await output in plan.evaluate(context) {
                        yield output?
                    }
                }
            }
        }
    }
}

impl EvaluationPlan for ApplicationPlan {
    fn cost(&self) -> usize {
        match self {
            ApplicationPlan::Select(plan) => plan.cost(),
            ApplicationPlan::Concept(plan) => plan.cost(),
            ApplicationPlan::Formula(plan) => plan.cost(),
            ApplicationPlan::Rule(plan) => plan.cost(),
        }
    }
    fn provides(&self) -> &VariableScope {
        match self {
            ApplicationPlan::Select(plan) => plan.provides(),
            ApplicationPlan::Concept(plan) => plan.provides(),
            ApplicationPlan::Formula(plan) => plan.provides(),
            ApplicationPlan::Rule(plan) => plan.provides(),
        }
    }

    fn evaluate<S: Store, M: Selection>(&self, context: EvaluationContext<S, M>) -> impl Selection {
        let source = self.clone();
        try_stream! {
            match source {
                ApplicationPlan::Select(plan) => {
                    for await each in plan.evaluate(context) {
                        yield each?;
                    }
                }
                ApplicationPlan::Concept(plan) => {
                    for await each in plan.evaluate(context) {
                        yield each?;
                    }
                },
                ApplicationPlan::Formula(plan) => {
                    for await each in plan.evaluate(context) {
                        yield each?;
                    }
                }
                ApplicationPlan::Rule(plan) => {
                    for await each in plan.evaluate(context) {
                        yield each?;
                    }
                }
            }
        }
    }
}

impl EvaluationPlan for NegationPlan {
    fn cost(&self) -> usize {
        let Self { application, .. } = self;
        application.cost()
    }
    fn provides(&self) -> &VariableScope {
        &self.provides
    }

    fn evaluate<S: Store, M: Selection>(&self, context: EvaluationContext<S, M>) -> impl Selection {
        let plan = self.application.clone();
        try_stream! {
            for await each in context.selection {
                let frame = each?;
                let not = frame.clone();
                let output = plan.evaluate(EvaluationContext {
                    selection: stream::once(async move { Ok(not)}),
                    store: context.store.clone()
                });

                tokio::pin!(output);

                if let Ok(Some(_)) = output.try_next().await {
                    continue;
                }

                yield frame;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::artifact::ValueDataType;
    use crate::attribute::Attribute;
    use crate::term::Term;

    #[test]
    fn test_terms_basic_operations() {
        let mut terms = Terms::new();

        // Test insertion and retrieval
        let name_term = Term::var("name");
        terms.insert("name".to_string(), name_term.clone());

        assert_eq!(terms.get("name"), Some(&name_term));
        assert_eq!(terms.get("nonexistent"), None);
        assert!(terms.contains("name"));
        assert!(!terms.contains("nonexistent"));

        // Test iteration
        let collected: Vec<_> = terms.iter().collect();
        assert_eq!(collected.len(), 1);
        assert_eq!(collected[0].0, &"name".to_string());
        assert_eq!(collected[0].1, &name_term);
    }

    #[test]
    fn test_conclusion_operations() {
        let mut attributes = HashMap::new();
        attributes.insert(
            "name".to_string(),
            Attribute::new("person", "name", "Person name", ValueDataType::String),
        );
        attributes.insert(
            "age".to_string(),
            Attribute::new("person", "age", "Person age", ValueDataType::UnsignedInt),
        );

        let conclusion = Conclusion { attributes };

        // Test contains method - should include "this" parameter
        assert!(conclusion.contains("this"));
        assert!(conclusion.contains("name"));
        assert!(conclusion.contains("age"));
        assert!(!conclusion.contains("height"));

        // Test absent method
        let mut dependencies = Dependencies::new();
        dependencies.desire("name".into(), 100);

        // Should find "this" as absent since it's not in dependencies
        assert_eq!(conclusion.absent(&dependencies), Some("this"));

        dependencies.desire("this".into(), 100);
        // Now should find "age" as absent
        assert_eq!(conclusion.absent(&dependencies), Some("age"));

        dependencies.desire("age".into(), 100);
        // Now nothing should be absent
        assert_eq!(conclusion.absent(&dependencies), None);
    }

    #[test]
    fn test_dependencies_operations() {
        let mut deps = Dependencies::new();

        // Test basic operations
        assert!(!deps.contains("test"));
        assert_eq!(deps.resolve("test"), Requirement::Derived(0)); // Default value

        // Test desire
        deps.desire("test".into(), 100);
        assert!(deps.contains("test"));
        assert_eq!(deps.resolve("test"), Requirement::Derived(100));

        // Test require
        deps.require("required".into());
        assert_eq!(deps.resolve("required"), Requirement::Required);

        // Test provide
        deps.provide("provided".into());
        assert_eq!(deps.resolve("provided"), Requirement::Derived(0));

        // Test iteration
        let items: Vec<_> = deps.iter().collect();
        assert_eq!(items.len(), 3);
    }

    #[test]
    fn test_dependencies_update_logic() {
        let mut deps = Dependencies::new();

        // Test updating derived with derived - should take minimum cost
        deps.desire("cost".into(), 50);
        deps.update("cost".into(), &Requirement::Derived(200));
        assert_eq!(deps.resolve("cost"), Requirement::Derived(50)); // Takes minimum

        // Test updating derived with lower cost - should take the new lower cost
        deps.update("cost".into(), &Requirement::Derived(25));
        assert_eq!(deps.resolve("cost"), Requirement::Derived(25));

        // Test that Required dependency gets overridden when updated with Derived
        deps.require("required_test".into());
        deps.update("required_test".into(), &Requirement::Derived(100));
        assert_eq!(deps.resolve("required_test"), Requirement::Derived(100));

        // Test adding new dependency via update
        deps.update("new_dep".into(), &Requirement::Derived(75));
        assert_eq!(deps.resolve("new_dep"), Requirement::Derived(75));
    }

    #[test]
    fn test_concept_creation() {
        let mut attributes = HashMap::new();
        attributes.insert(
            "name".to_string(),
            Attribute::new("person", "name", "Person name", ValueDataType::String),
        );

        let concept = Concept {
            operator: "person".to_string(),
            attributes,
        };

        assert_eq!(concept.operator, "person");
        assert_eq!(concept.attributes.len(), 1);
        assert!(concept.attributes.contains_key("name"));
    }

    #[test]
    fn test_concept_application_analysis() {
        let mut attributes = HashMap::new();
        attributes.insert(
            "name".to_string(),
            Attribute::new("person", "name", "Person name", ValueDataType::String),
        );
        attributes.insert(
            "age".to_string(),
            Attribute::new("person", "age", "Person age", ValueDataType::UnsignedInt),
        );

        let concept = Concept {
            operator: "person".to_string(),
            attributes,
        };

        let mut terms = Terms::new();
        terms.insert("name".to_string(), Term::var("person_name"));
        terms.insert("age".to_string(), Term::var("person_age"));

        let concept_app = ConcetApplication { terms, concept };

        let analysis = concept_app.analyze().expect("Analysis should succeed");

        assert_eq!(analysis.cost, BASE_COST);
        assert!(analysis.dependencies.contains("this"));
        assert!(analysis.dependencies.contains("name"));
        assert!(analysis.dependencies.contains("age"));
        // Check that we have the expected dependencies
        let deps_count = analysis.dependencies.iter().count();
        assert_eq!(deps_count, 3);
    }

    #[test]
    fn test_deductive_rule_parameters() {
        let mut conclusion_attributes = HashMap::new();
        conclusion_attributes.insert(
            "name".to_string(),
            Attribute::new("person", "name", "Person name", ValueDataType::String),
        );
        conclusion_attributes.insert(
            "age".to_string(),
            Attribute::new("person", "age", "Person age", ValueDataType::UnsignedInt),
        );

        let rule = DeductiveRule {
            operator: "adult".to_string(),
            conclusion: Conclusion {
                attributes: conclusion_attributes,
            },
            premises: vec![],
        };

        let params = rule.parameters();
        assert!(params.contains("this"));
        assert!(params.contains("name"));
        assert!(params.contains("age"));
        assert_eq!(params.len(), 3);
    }

    #[test]
    fn test_requirement_properties() {
        let required = Requirement::Required;
        let derived = Requirement::Derived(100);

        assert!(required.is_required());
        assert!(!derived.is_required());
    }

    #[test]
    fn test_premise_construction() {
        let fact_selector = FactSelector::new()
            .the("person/name")
            .of(Term::var("person"))
            .is(crate::artifact::Value::String("Alice".to_string()));

        let premise = Premise::from(fact_selector);

        match premise {
            Premise::Apply(Application::Select(_)) => {
                // Expected case
            }
            _ => panic!("Expected Select application"),
        }
    }

    #[test]
    fn test_analysis_structure() {
        let mut deps = Dependencies::new();
        deps.desire("test".into(), 50);

        let analysis = Analysis {
            cost: 100,
            dependencies: deps,
        };

        assert_eq!(analysis.cost, 100);
        assert!(analysis.dependencies.contains("test"));
    }

    #[test]
    fn test_planner_creation() {
        let premises = vec![];
        let planner = Planner::new(&premises);

        match planner {
            Planner::Idle { premises: p } => {
                assert_eq!(p.len(), 0);
            }
            _ => panic!("Expected Idle state"),
        }
    }

    #[test]
    fn test_plan_candidate_structure() {
        let fact_selector = FactSelector::new().the("test/attr");
        let premise = Premise::from(fact_selector);

        let candidate = PlanCandidate {
            premise: &premise,
            dependencies: VariableScope::new(),
            result: Err(PlanError::UnexpectedError),
        };

        // Test that the structure exists and can be created
        assert!(matches!(candidate.result, Err(PlanError::UnexpectedError)));
    }

    #[test]
    fn test_error_types() {
        // Test AnalyzerError creation
        let rule = DeductiveRule {
            operator: "test".to_string(),
            conclusion: Conclusion {
                attributes: HashMap::new(),
            },
            premises: vec![],
        };

        let analyzer_error = AnalyzerError::UnusedParameter {
            rule: rule.clone(),
            parameter: "test_param".to_string(),
        };

        // Test conversion to PlanError
        let plan_error: PlanError = analyzer_error.into();
        match &plan_error {
            PlanError::UnusedParameter { rule: r, parameter } => {
                assert_eq!(r.operator, "test");
                assert_eq!(parameter, "test_param");
            }
            _ => panic!("Expected UnusedParameter variant"),
        }

        // Test conversion to QueryError
        let query_error: QueryError = plan_error.into();
        match query_error {
            QueryError::PlanningError { .. } => {
                // Expected
            }
            _ => panic!("Expected PlanningError variant"),
        }
    }

    #[test]
    fn test_application_variants() {
        // Test Select application
        let selector = FactSelector::new().the("test/attr");
        let app = Application::Select(selector);

        match app {
            Application::Select(_) => {
                // Expected
            }
            _ => panic!("Expected Select variant"),
        }

        // Test other variants exist
        let mut terms = Terms::new();
        terms.insert("test".to_string(), Term::var("test_var"));
        let concept = Concept {
            operator: "test".to_string(),
            attributes: HashMap::new(),
        };
        let concept_app = Application::Realize(ConcetApplication { terms, concept });

        match concept_app {
            Application::Realize(_) => {
                // Expected
            }
            _ => panic!("Expected Realize variant"),
        }
    }

    #[test]
    fn test_join_operations() {
        let join = Join::new();
        match join {
            Join::Identity => {
                // Expected initial state
            }
            _ => panic!("Expected Identity variant"),
        }

        // Test building joins
        let plans = vec![];
        let join_from_plans = Join::from(plans);
        match join_from_plans {
            Join::Identity => {
                // Expected for empty vec
            }
            _ => panic!("Expected Identity for empty plans"),
        }
    }

    #[test]
    fn test_negation_construction() {
        let selector = FactSelector::new().the("test/attr");
        let app = Application::Select(selector);
        let negation = Negation(app);

        // Test that negation wraps the application
        match negation {
            Negation(Application::Select(_)) => {
                // Expected
            }
            _ => panic!("Expected wrapped Select application"),
        }
    }
}

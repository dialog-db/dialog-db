use crate::artifact::ValueDataType;
use crate::attribute::Attribute;
use crate::fact_selector::FactSelector;
use crate::fact_selector::{BASE_COST, ENTITY_COST, VALUE_COST};
use crate::formula::{FormulaApplication, FormulaApplicationPlan};
use crate::plan::EvaluationPlan;
use crate::Entity;
use crate::{try_stream, QueryError};
use crate::{EvaluationContext, Selection, Store, Term, Value};
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

/// Represents a deductive rule that can be applied creating a premise.
#[derive(Debug, Clone, PartialEq)]
pub struct Concept {
    /// Concept identifier uset to look concepts up by.
    pub operator: String,
    pub attributes: HashMap<String, Attribute<Value>>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ConcetApplication {
    pub terms: Terms,
    pub concept: Concept,
}

impl ConcetApplication {
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

    fn plan(&self, scope: &VariableScope) -> Result<ConceptPlan, PlanError> {
        let mut provides = VariableScope::new();
        let mut cost = 0;
        if let Some(this) = self.terms.get("this") {
            if !scope.contains(&this) {
                provides.add(&this);
                cost += ENTITY_COST
            }
        }

        let this = self
            .terms
            .get("this")
            // TODO: We need properly unique identifier here, but we can pun
            // on that for now.
            .unwrap_or(&Term::var(&format!("this_{}", self.concept.operator)));

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
                    .of(&this.into())
                    .is(term.clone());

                premises.push(select.into());
            }
        }

        let mut planner = Planner::new(&premises);
        let (cost, conjuncts) = planner.plan(scope)?;

        Ok(ConceptPlan {
            cost,
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

#[derive(Debug, Clone, PartialEq)]
pub struct ConceptPlan {
    pub cost: usize,
    pub provides: VariableScope,
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
    OmitsRequiredCell { formula: &'static str, cell: String },
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
    pub fn new(rule: DeductiveRule, terms: Terms) -> Self {
        RuleApplication { rule, terms }
    }
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

pub enum Planner<'a> {
    Idle {
        premises: &'a Vec<Premise>,
    },
    Active {
        candidates: Vec<PlanCandidate<'a>>,
        scope: VariableScope,
    },
}

impl<'a> Planner<'a> {
    pub fn new(premises: &'a Vec<Premise>) -> Self {
        Self::Idle { premises }
    }
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

    fn done(&self) -> bool {
        match self {
            Self::Idle { .. } => false,
            Self::Active { candidates, .. } => candidates.len() > 0,
        }
    }

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

/// Cached premise with all computed data
#[derive(Debug, Clone)]
struct PlanCandidate<'a> {
    premise: &'a Premise,
    dependencies: VariableScope,
    result: Result<Plan, PlanError>,
}

impl<'a> PlanCandidate<'a> {
    fn plan(&mut self, scope: &VariableScope) -> &Self {
        self.result = self.premise.plan(scope);
        self
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct RuleApplicationPlan {
    pub cost: usize,
    pub terms: Terms,
    pub conjuncts: Vec<Plan>,
    pub provides: VariableScope,
    pub rule: DeductiveRule,
}

impl RuleApplicationPlan {
    fn eval<S: Store, M: Selection>(&self, context: EvaluationContext<S, M>) -> impl Selection {
        Self::eval_helper(context.store, context.selection, self.conjuncts.clone())
    }
    fn eval_helper<S: Store, M: Selection>(
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

#[derive(Debug, Clone)]
pub enum Join {
    Identity,
    Join(Box<Join>, Plan),
}

impl Join {
    fn new() -> Self {
        Join::Identity
    }
    fn from(plans: Vec<Plan>) -> Self {
        plans
            .into_iter()
            .fold(Join::Identity, |join, plan| join.and(plan))
    }
    fn and(self, plan: Plan) -> Self {
        Join::Join(Box::new(self), plan)
    }

    fn evaluate<S: Store, M: Selection>(self, context: EvaluationContext<S, M>) -> impl Selection {
        try_stream! {
            match self {
                Join::Identity => {
                    for await each in context.selection {
                        yield each?;
                    }
                }
                Join::Join(left, right) => {
                    let store = context.store.clone();
                    let selection = left.evaluate(context);
                    let output = right.evaluate(EvaluationContext { selection, store });
                    for await each in output {
                        yield each?;
                    }
                }
            }
        }
    }
}

// The evaluate method is now part of the EvaluationPlan trait implementation above

// yield_all function removed as it's no longer needed

/// Represents a set of named cells that formula operates on. Each cell also
/// describes whether it is required or optional and cost of it's omission.
#[derive(Debug, Clone, PartialEq)]
pub struct Cells(HashMap<String, Cell>);

/// Describes a cell of the formula.
#[derive(Debug, Clone, PartialEq)]
pub struct Cell {
    pub name: &'static str,
    pub description: &'static str,
    pub requirement: Requirement,
    pub data_type: ValueDataType,
}

impl Cells {
    pub fn new() -> Self {
        Cells(HashMap::new())
    }

    pub fn iter(&self) -> impl Iterator<Item = (&str, &Cell)> {
        self.0.iter().map(|(k, v)| (k.as_str(), v))
    }

    pub fn get(&self, name: &str) -> Option<&Cell> {
        self.0.get(name)
    }

    pub fn add(&mut self, name: String, cell: Cell) -> &mut Self {
        self.0.insert(name, cell);
        self
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct Dependencies(HashMap<String, Requirement>);
impl Dependencies {
    pub fn new() -> Self {
        Dependencies(HashMap::new())
    }
    pub fn cost(&self) -> usize {
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
pub enum Requirement {
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
pub enum Premise {
    Apply(Application),
    /// Statement that exclude matches from the selection. This is basically
    /// a negetated statement.
    Exclude(Negation),
}

impl Premise {
    pub fn plan(&self, scope: &VariableScope) -> Result<Plan, PlanError> {
        match self {
            Premise::Apply(application) => application.plan(scope).map(Plan::Application),
            Premise::Exclude(negation) => negation.plan(scope).map(Plan::Negation),
        }
    }
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

/// Statements that can be used by the rules.
#[derive(Debug, Clone, PartialEq)]
pub struct Negation(Application);

impl Negation {
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

#[derive(Debug, Clone, PartialEq)]
pub struct NegationPlan {
    pub application: ApplicationPlan,
    pub provides: VariableScope,
}

impl NegationPlan {
    pub fn not(application: ApplicationPlan) -> Self {
        Self {
            application,
            provides: VariableScope::new(),
        }
    }
    // evaluate method is now part of the EvaluationPlan trait implementation
}

/// Statements that can be used by the rules.
#[derive(Debug, Clone, PartialEq)]
pub enum Application {
    /// Fact selection.
    Select(FactSelector),
    /// Concept selection
    Realize(ConcetApplication),
    /// Rule application
    ApplyRule(RuleApplication),
    /// Formula application
    ApplyFormula(FormulaApplication),
}

impl Application {
    fn analyze(&self) -> Result<Analysis, AnalyzerError> {
        match self {
            Application::Select(selector) => selector.analyze(),
            Application::Realize(concept) => concept.analyze(),
            Application::ApplyRule(application) => application.analyze(),
            Application::ApplyFormula(application) => application.analyze(),
        }
    }

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

impl Terms {
    pub fn constants(&self) -> HashMap<String, Value> {
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

#[derive(Debug, Clone, PartialEq)]
pub enum ApplicationPlan {
    Select(FactSelectorPlan),
    Concept(ConceptPlan),
    Rule(RuleApplicationPlan),
    Formula(FormulaApplicationPlan),
}

impl ApplicationPlan {
    pub fn not(self) -> NegationPlan {
        NegationPlan::not(self)
    }
    // evaluate method is now part of the EvaluationPlan trait implementation
}

#[derive(Debug, Clone, PartialEq)]
pub enum Plan {
    Application(ApplicationPlan),
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

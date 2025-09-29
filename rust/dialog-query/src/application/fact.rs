pub use super::{Analysis, Application};
use crate::analyzer::{
    AnalysisStatus, Desired, Environment, Plan as SyntaxPlan, Planner, Required, Stats, Syntax,
    SyntaxAnalysis,
};
pub use crate::artifact::Attribute;
pub use crate::artifact::{ArtifactSelector, Constrained};
use crate::dependencies;
pub use crate::error::AnalyzerError;
use crate::error::PlanError;
pub use crate::fact_selector::{ATTRIBUTE_COST, BASE_COST, ENTITY_COST, UNBOUND_COST, VALUE_COST};
pub use crate::plan::Plan;
use crate::Cardinality;
pub use crate::FactSelector;
pub use crate::VariableScope;
use crate::{try_stream, EvaluationContext, Match, Selection, Source};
pub use crate::{Dependencies, Entity, Fact, QueryError, Term, Value};
use serde::{Deserialize, Serialize};
use serde_json::de;
use std::fmt::Display;
use std::os::macos::raw::stat;
use std::path::MAIN_SEPARATOR_STR;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FactApplication {
    cardinality: Cardinality,
    the: Term<Attribute>,
    of: Term<Entity>,
    is: Term<Value>,
}

impl FactApplication {
    pub fn many(&self) -> Self {
        Self {
            cardinality: Cardinality::Many,
            the: self.the.clone(),
            of: self.of.clone(),
            is: self.is.clone(),
        }
    }
    pub fn new(
        the: Term<Attribute>,
        of: Term<Entity>,
        is: Term<Value>,
        cardinality: Cardinality,
    ) -> Self {
        Self {
            cardinality,
            the,
            of,
            is,
        }
    }

    pub fn analyze(&self) -> Analysis {
        Analysis::new(0)
            .desire(Some(&self.the), ATTRIBUTE_COST)
            .desire(Some(&self.of), ENTITY_COST)
            .desire(Some(&self.is), VALUE_COST)
            .to_owned()
    }

    pub fn plan(&self, scope: &VariableScope) -> Result<FactApplicationPlan, PlanError> {
        // We start with a cost estimate that assumes nothing is known.
        let mut cost = UNBOUND_COST;
        let mut provides = VariableScope::new();

        // If self.of is in scope we subtract ENTITY_COST from estimate

        if scope.contains(&self.of) {
            cost -= ENTITY_COST;
            provides.add(&self.of);
        }

        // If self.the is in scope we subtract ATTRIBUTE_COST from the cost
        if scope.contains(&self.the) {
            cost -= ATTRIBUTE_COST;
            provides.add(&self.the);
        }

        if scope.contains(&self.is) {
            cost -= VALUE_COST;
            provides.add(&self.is);
        }

        // if cost is below UNBOUND_COST we have some term in the selector &
        // during evaluation we will be able to produce constrained selector
        // in this case we can return a plan, otherwise we return an error
        if cost < UNBOUND_COST {
            Ok(FactApplicationPlan {
                selector: self.clone(),
                provides,
                cost,
            })
        } else {
            let selector = FactSelector {
                the: Some(self.the.clone()),
                of: Some(self.of.clone()),
                is: Some(self.is.clone()),
                fact: None,
            };
            Err(PlanError::UnconstrainedSelector { selector })
        }
    }
}

impl Syntax for FactApplication {
    fn analyze<'a>(&'a self, env: &Environment) -> Stats<'a, Self> {
        let mut stats = Stats::new(self, BASE_COST);
        let (the, of, is) = (
            env.locals.contains(&self.the),
            env.locals.contains(&self.of),
            env.locals.contains(&self.is),
        );

        // if no parameter is provided mark all as required
        if !(the && of && is) {
            stats.require(&self.the);
            stats.require(&self.of);
            stats.require(&self.is);
        }
        // otherwise we mark fields that are not in the local scope
        // as desired
        else {
            if !the {
                stats.desire(&self.the, ATTRIBUTE_COST);
            }
            if !of {
                stats.desire(&self.of, ENTITY_COST);
            }
            if !is {
                stats.desire(&self.is, VALUE_COST);
            }
        }

        stats
    }

    fn update<'a>(&'a self, stats: &mut Stats<'a, Self>, extension: VariableScope) {
        // If analyzer is not blocked update may reduce it's cost estimate
        if stats.required.count() == 0 {
            if extension.contains(&stats.syntax.the) {
                stats.desired.remove(&stats.syntax.the);
            }
            if extension.contains(&stats.syntax.of) {
                stats.desired.remove(&stats.syntax.of);
            }
            if extension.contains(&stats.syntax.is) {
                stats.desired.remove(&stats.syntax.is);
            }
        }
        // If analysis are blocked update may unblock it.
        else {
            let (the, of, is) = (
                extension.contains(&stats.syntax.the),
                extension.contains(&stats.syntax.of),
                extension.contains(&stats.syntax.is),
            );

            // if we have one of the parameters
            if the || of || is {
                stats.required.remove(&stats.syntax.the);
                stats.required.remove(&stats.syntax.of);
                stats.required.remove(&stats.syntax.is);

                if !the {
                    stats.desire(&stats.syntax.the, ATTRIBUTE_COST);
                    stats.desire(&stats.syntax.of, ENTITY_COST);
                    stats.desire(&stats.syntax.is, VALUE_COST);
                }
            }
        }
    }
}

impl Planner for FactApplication {
    fn init(&'a self, plan: &mut SyntaxAnalysis, env: &VariableScope) {
        // add base cost of execution
        plan.desire(&Term::blank(), BASE_COST);

        let (the, of, is) = (
            env.contains(&self.the),
            env.contains(&self.of),
            env.contains(&self.is),
        );

        // if no parameter is provided mark all as required
        if !(the && of && is) {
            plan.require(&self.the);
            plan.require(&self.of);
            plan.require(&self.is);
        }
        // otherwise we mark fields that are not in the local scope
        // as desired
        else {
            let desired = plan.desired();
            if !the {
                desired.insert(&self.the, ATTRIBUTE_COST);
            }
            if !of {
                desired.insert(&self.of, ENTITY_COST);
            }
            if !is {
                desired.insert(&self.is, VALUE_COST);
            }

            *plan = SyntaxAnalysis::Candidate {
                cost: BASE_COST,
                desired: desired.to_owned(),
            }
        }
    }

    fn update(&self, plan: &mut SyntaxAnalysis, env: &VariableScope) {
        match plan {
            SyntaxAnalysis::Incomplete { cost, desired, .. } => {
                let (the, of, is) = (
                    env.contains(&self.the),
                    env.contains(&self.of),
                    env.contains(&self.is),
                );

                if the || of || is {
                    if !the {
                        desired.insert(&self.the, ATTRIBUTE_COST);
                    }

                    if !of {
                        desired.insert(&self.of, ENTITY_COST);
                    }

                    if !is {
                        desired.insert(&self.is, VALUE_COST);
                    }

                    *plan = SyntaxAnalysis::Candidate {
                        desired: desired.to_owned(),
                        cost: *cost,
                    };
                }
            }
            SyntaxAnalysis::Candidate { desired, .. } => {
                if env.contains(&self.the) {
                    desired.remove(&self.the);
                }
                if env.contains(&self.of) {
                    desired.remove(&self.of);
                }
                if env.contains(&self.is) {
                    desired.remove(&self.is);
                }
            }
        }
    }
}

struct FactApplicationAnalysis {
    pub form: FactApplication,
    pub cost: usize,
    pub required: Required,
    pub desired: Desired,
}

impl FactApplicationAnalysis {
    pub fn update(&mut self, scope: &VariableScope) {
        if scope.contains(&self.form.the) {
            self.required.remove(&self.form.the);
            self.desired.remove(&self.form.the);
        }

        if scope.contains(&self.form.of) {
            self.required.remove(&self.form.of);
            self.desired.remove(&self.form.of);
        }

        if scope.contains(&self.form.is) {
            self.required.remove(&self.form.is);
            self.desired.remove(&self.form.is);
        }
    }
}

impl Planner for FactApplication {
    fn init(&self, analysis: &mut SyntaxAnalysis, env: &VariableScope) {
        let (the, of, is) = (
            if env.contains(&self.the) {
                0
            } else {
                ATTRIBUTE_COST
            },
            if env.contains(&self.of) {
                0
            } else {
                ENTITY_COST
            },
            if env.contains(&self.is) {
                0
            } else {
                VALUE_COST
            },
        );

        analysis.desire(&Term::blank(), BASE_COST);

        // if any variable is bound in the given environment we can mark
        // all as desired since we only need one to derive the others.
        if the + of + is < ATTRIBUTE_COST + ENTITY_COST + VALUE_COST {
            analysis.desire(&self.the, the);
            analysis.desire(&self.of, of);
            analysis.desire(&self.is, is);
        }
        // if none of the variables are bound we can mark them all as required
        else {
            analysis.require(&self.the);
            analysis.require(&self.of);
            analysis.require(&self.is);
        }
    }
    fn update(&self, analysis: &mut SyntaxAnalysis, env: &VariableScope) {
        // update all the bound variable costs to 0
        if env.contains(&self.the) {
            analysis.desire(&self.the, 0);
        }

        if env.contains(&self.of) {
            analysis.desire(&self.of, 0);
        }

        if env.contains(&self.is) {
            analysis.desire(&self.is, 0);
        }

        // if we desired count is above 0 we mark all the other
        // variables as desired also because we only need one to
        // derive the rest.
        let desired = analysis.desired();
        if desired.count() > 0 {
            if !desired.contains(&self.the) {
                analysis.desire(&self.the, ATTRIBUTE_COST);
            }
            if !desired.contains(&self.of) {
                analysis.desire(&self.of, ATTRIBUTE_COST);
            }
            if !desired.contains(&self.is) {
                analysis.desire(&self.is, ATTRIBUTE_COST);
            }
        }
    }
}

impl FactApplication {
    /// Resolves variables from the given selection match.
    pub fn resolve(&self, source: &Match) -> Self {
        let the = source.resolve(&self.the);
        let of = source.resolve(&self.of);
        let is = source.resolve(&self.is);

        Self {
            the,
            of,
            is,
            cardinality: self.cardinality,
        }
    }
    pub fn evaluate<S: Source, M: Selection>(
        &self,
        context: EvaluationContext<S, M>,
    ) -> impl Selection {
        let selector = self.clone();
        try_stream! {
            for await each in context.selection {
                let input = each?;
                let selection = selector.resolve(&input);
                for await artifact in context.source.select((&selection).try_into()?) {
                    let artifact = artifact?;

                    // Create a new frame by unifying the artifact with our pattern
                    let mut output = input.clone();

                    // Unify entity if we have an entity variable using type-safe unify
                    output = output.unify(selection.of.clone(), Value::Entity(artifact.of)).map_err(|e| QueryError::FactStore(e.to_string()))?;


                    // Unify attribute if we have an attribute variable using type-safe unify
                    output = output.unify(selection.the.clone(), Value::Symbol(artifact.the)).map_err(|e| QueryError::FactStore(e.to_string()))?;


                    // Unify value if we have a value variable using type-safe unify
                    output = output.unify(selection.is.clone(), artifact.is).map_err(|e| QueryError::FactStore(e.to_string()))?;

                    yield output;
                }
            }
        }
    }
}

impl TryFrom<&FactApplication> for ArtifactSelector<Constrained> {
    type Error = QueryError;

    fn try_from(from: &FactApplication) -> Result<Self, Self::Error> {
        let mut selector: Option<ArtifactSelector<Constrained>> = None;

        // Convert attribute (the)
        match &from.the {
            Term::Constant(the) => {
                selector = Some(match selector {
                    None => ArtifactSelector::new().the(the.to_owned()),
                    Some(s) => s.the(the.to_owned()),
                });
            }
            Term::Variable { .. } => {}
        }

        // Convert entity (of)
        match &from.of {
            Term::Constant(of) => {
                selector = Some(match selector {
                    None => ArtifactSelector::new().of(of.to_owned()),
                    Some(s) => s.of(of.to_owned()),
                });
            }
            Term::Variable { .. } => {}
        }

        // Convert value (is)
        match &from.is {
            Term::Constant(value) => {
                selector = Some(match selector {
                    None => ArtifactSelector::new().is(value.clone()),
                    Some(s) => s.is(value.clone()),
                });
            }
            Term::Variable { .. } => {}
        }

        selector.ok_or_else(|| QueryError::EmptySelector {
            message: "At least one field must be constrained".to_string(),
        })
    }
}

impl Display for FactApplication {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Fact {{")?;

        write!(f, "the: {},", self.the)?;

        write!(f, "the: {},", self.of)?;

        write!(f, "the: {},", self.is)?;

        write!(f, "}}")
    }
}

pub struct FactApplicationPlan {
    pub selector: FactApplication,
    pub provides: VariableScope,
    pub cost: usize,
}

impl From<FactApplication> for Application {
    fn from(selector: FactApplication) -> Self {
        Application::Fact(selector)
    }
}

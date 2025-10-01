pub use super::{Application, LegacyAnalysis};
pub use crate::artifact::Attribute;
pub use crate::artifact::{ArtifactSelector, Constrained};
pub use crate::error::AnalyzerError;
use crate::error::PlanError;
pub use crate::fact_selector::{ATTRIBUTE_COST, BASE_COST, ENTITY_COST, UNBOUND_COST, VALUE_COST};
pub use crate::plan::Plan;
use crate::Cardinality;
pub use crate::FactSelector;
pub use crate::VariableScope;
use crate::{try_stream, EvaluationContext, Match, Selection, Source};
use crate::{
    Constraint, Dependencies, Dependency, Entity, Parameters, QueryError, Schema, Term, Type, Value,
};
use serde::{Deserialize, Serialize};
use std::fmt::Display;
use std::sync::OnceLock;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FactApplication {
    cardinality: Cardinality,
    the: Term<Attribute>,
    of: Term<Entity>,
    is: Term<Value>,
}

impl FactApplication {
    /// Returns the schema for fact selectors
    /// Defines the "the", "of", "is" parameters with choice constraint
    pub fn schema(&self) -> Schema {
        Self::static_schema().clone()
    }

    /// Returns the static schema for fact selectors
    fn static_schema() -> &'static Schema {
        static FACT_SCHEMA: OnceLock<Schema> = OnceLock::new();
        FACT_SCHEMA.get_or_init(|| {
            let constraint = Dependency::some();
            let mut schema = Schema::new();

            schema.insert(
                "the".to_string(),
                Constraint {
                    description: "Attribute of the fact".to_string(),
                    content_type: Some(Type::Symbol),
                    requirement: constraint.derive(ATTRIBUTE_COST),
                    cardinality: Cardinality::One,
                },
            );

            schema.insert(
                "of".to_string(),
                Constraint {
                    description: "Entity of the fact".to_string(),
                    content_type: Some(Type::Entity),
                    requirement: constraint.derive(ENTITY_COST),
                    cardinality: Cardinality::One,
                },
            );

            schema.insert(
                "is".to_string(),
                Constraint {
                    description: "Value of the fact".to_string(),
                    content_type: None, // Can be any type
                    requirement: constraint.derive(VALUE_COST),
                    cardinality: Cardinality::One,
                },
            );

            schema
        })
    }

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

    pub fn cost(&self) -> usize {
        match self.cardinality {
            Cardinality::One => BASE_COST,
            Cardinality::Many => usize::pow(BASE_COST, 2),
        }
    }

    /// Returns the parameters for this fact application
    /// Note: This allocates since fact parameters are stored as separate fields
    pub fn parameters(&self) -> Parameters {
        let mut params = Parameters::new();
        params.insert("the".to_string(), self.the.as_unknown());
        params.insert("of".to_string(), self.of.as_unknown());
        params.insert("is".to_string(), self.is.clone());
        params
    }

    pub fn dependencies(&self) -> Dependencies {
        let mut dependencies = Dependencies::new();
        dependencies.desire("the".into(), ATTRIBUTE_COST);
        dependencies.desire("of".into(), ENTITY_COST);
        dependencies.desire("is".into(), VALUE_COST);
        dependencies
    }

    pub fn analyze(&self) -> LegacyAnalysis {
        LegacyAnalysis::new(0)
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

#[derive(Debug, Clone, PartialEq)]
pub struct FactApplicationPlan {
    pub selector: FactApplication,
    pub provides: VariableScope,
    pub cost: usize,
}

impl FactApplicationPlan {
    pub fn cost(&self) -> usize {
        self.cost
    }

    pub fn provides(&self) -> &VariableScope {
        &self.provides
    }

    // TODO: Phase 3 - Implement proper evaluate() method
    pub fn evaluate<S: crate::Source, M: crate::Selection>(
        &self,
        context: crate::EvaluationContext<S, M>,
    ) -> impl crate::Selection {
        // Return the input selection unchanged as a placeholder
        context.selection
    }
}

impl From<FactApplication> for Application {
    fn from(selector: FactApplication) -> Self {
        Application::Fact(selector)
    }
}

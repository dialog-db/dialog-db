pub use super::Application;
pub use crate::artifact::Attribute;
pub use crate::artifact::{ArtifactSelector, Constrained};
pub use crate::context::new_context;
pub use crate::error::{AnalyzerError, QueryResult};
pub use crate::query::Output;
use crate::query::{Circuit, Query};
use crate::Cardinality;
pub use crate::Environment;

use crate::Fact;
use crate::{try_stream, EvaluationContext, Match, Selection, Source};
use crate::{Constraint, Dependency, Entity, Parameters, QueryError, Schema, Term, Type, Value};
use dialog_artifacts::Cause;
use serde::{Deserialize, Serialize};
use std::fmt::Display;
use std::sync::OnceLock;

pub const BASE_COST: usize = 100;

/// Cost of a segment read for Cardinality::One with 3/3 or 2/3 constraints.
/// This is a direct lookup that reads from a single segment.
pub const SEGMENT_READ_COST: usize = 100;

/// Cost of a range read for Cardinality::Many with 3/3 constraints.
/// This read could potentially span multiple segments but is bounded.
pub const RANGE_READ_COST: usize = 200;

/// Cost of a range scan for Cardinality::Many with 2/3 constraints,
/// or Cardinality::One with 1/3 constraints.
/// This scan is likely to span multiple segments.
pub const RANGE_SCAN_COST: usize = 1_000;

/// Cost of an index scan for Cardinality::Many with 1/3 constraints.
/// This is the most expensive query pattern - scanning with minimal constraints.
pub const INDEX_SCAN: usize = 5_000;

/// Overhead cost for concept queries due to potential rule evaluation.
/// Concepts may have associated deductive rules that need to be checked and evaluated.
pub const CONCEPT_OVERHEAD: usize = 1_000;

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
                    requirement: constraint.member(),
                    cardinality: Cardinality::One,
                },
            );

            schema.insert(
                "of".to_string(),
                Constraint {
                    description: "Entity of the fact".to_string(),
                    content_type: Some(Type::Entity),
                    requirement: constraint.member(),
                    cardinality: Cardinality::One,
                },
            );

            schema.insert(
                "is".to_string(),
                Constraint {
                    description: "Value of the fact".to_string(),
                    content_type: None, // Can be any type
                    requirement: constraint.member(),
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

    /// Estimate cost based on how many parameters are constrained and cardinality.
    /// More constrained = lower cost. Cardinality matters for partially constrained queries.
    pub fn estimate(&self, env: &Environment) -> Option<usize> {
        // Check which parameters are bound (constants or in env)
        let the = env.contains(&self.the);
        let of = env.contains(&self.of);
        let is = env.contains(&self.is);

        self.cardinality.estimate(the, of, is)
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

                    // For each field, if it's a blank variable, assign it an internal name so we can retrieve it later
                    let of_term = match &selection.of {
                        Term::Variable { name: None, .. } => Term::Variable { name: Some("__of".to_string()), content_type: Default::default() },
                        term => term.clone(),
                    };
                    let the_term = match &selection.the {
                        Term::Variable { name: None, .. } => Term::Variable { name: Some("__the".to_string()), content_type: Default::default() },
                        term => term.clone(),
                    };
                    let is_term = match &selection.is {
                        Term::Variable { name: None, .. } => Term::Variable { name: Some("__is".to_string()), content_type: Default::default() },
                        term => term.clone(),
                    };

                    // Unify entity if we have an entity variable using type-safe unify
                    output = output.unify(of_term, Value::Entity(artifact.of)).map_err(|e| QueryError::FactStore(e.to_string()))?;

                    // Unify attribute if we have an attribute variable using type-safe unify
                    output = output.unify(the_term, Value::Symbol(artifact.the)).map_err(|e| QueryError::FactStore(e.to_string()))?;

                    // Unify value if we have a value variable using type-safe unify
                    output = output.unify(is_term, artifact.is).map_err(|e| QueryError::FactStore(e.to_string()))?;

                    yield output;
                }
            }
        }
    }

    pub fn realize(&self, source: crate::selection::Match) -> Result<Fact<Value>, QueryError> {
        // Convert blank variables to internal names for retrieval
        let the_term = match &self.the {
            Term::Variable { name: None, .. } => Term::Variable {
                name: Some("__the".to_string()),
                content_type: Default::default(),
            },
            term => term.clone(),
        };
        let of_term = match &self.of {
            Term::Variable { name: None, .. } => Term::Variable {
                name: Some("__of".to_string()),
                content_type: Default::default(),
            },
            term => term.clone(),
        };
        let is_term = match &self.is {
            Term::Variable { name: None, .. } => Term::Variable {
                name: Some("__is".to_string()),
                content_type: Default::default(),
            },
            term => term.clone(),
        };

        Ok(Fact::Assertion {
            the: source.get(&the_term)?,
            of: source.get(&of_term)?,
            is: source.get(&is_term)?,
            // TODO: We actually need to capture causes, but for now we fake it.
            cause: Cause([0; 32]),
        })
    }

    pub fn query<S: Source>(&self, source: &S) -> impl Output<Fact>
    where
        Self: Sized,
    {
        // TODO: This logic should be added perhaps to .compile method. In
        // practice we want concept to be used for queries which we know
        // can be evaluated due to attributes being known.
        //
        // // Validate that at least one parameter is constrained (not a variable)
        // // This prevents completely unconstrained queries
        // let has_constant = matches!(&self.the, Term::Constant(_))
        //     || matches!(&self.of, Term::Constant(_))
        //     || matches!(&self.is, Term::Constant(_));

        // if !has_constant {
        //     return Err(QueryError::EmptySelector {
        //         message:
        //             "At least one bound parameter required (the, of, or is must be a constant)"
        //                 .to_string(),
        //     });
        // }

        // Inline the trait implementation to avoid lifetime issues
        let context = new_context(source.clone());
        let selection = self.evaluate(context);
        let query = self.clone();
        try_stream! {
            for await each in selection {
                yield query.realize(each?)?;
            }
        }
    }
}

impl Circuit for FactApplication {
    fn evaluate<S: Source, M: Selection>(
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

                    // For each field, if it's a blank variable, assign it an internal name so we can retrieve it later
                    let of_term = match &selection.of {
                        Term::Variable { name: None, .. } => Term::Variable { name: Some("__of".to_string()), content_type: Default::default() },
                        term => term.clone(),
                    };
                    let the_term = match &selection.the {
                        Term::Variable { name: None, .. } => Term::Variable { name: Some("__the".to_string()), content_type: Default::default() },
                        term => term.clone(),
                    };
                    let is_term = match &selection.is {
                        Term::Variable { name: None, .. } => Term::Variable { name: Some("__is".to_string()), content_type: Default::default() },
                        term => term.clone(),
                    };

                    // Unify entity if we have an entity variable using type-safe unify
                    output = output.unify(of_term, Value::Entity(artifact.of)).map_err(|e| QueryError::FactStore(e.to_string()))?;

                    // Unify attribute if we have an attribute variable using type-safe unify
                    output = output.unify(the_term, Value::Symbol(artifact.the)).map_err(|e| QueryError::FactStore(e.to_string()))?;

                    // Unify value if we have a value variable using type-safe unify
                    output = output.unify(is_term, artifact.is).map_err(|e| QueryError::FactStore(e.to_string()))?;

                    yield output;
                }
            }
        }
    }
}

impl Query<Fact> for FactApplication {
    fn realize(&self, input: crate::selection::Match) -> Result<Fact, QueryError> {
        self.realize(input)
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
    pub provides: Environment,
    pub cost: usize,
}

impl FactApplicationPlan {
    pub fn cost(&self) -> usize {
        self.cost
    }

    pub fn provides(&self) -> &Environment {
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

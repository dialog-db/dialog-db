pub use super::Application;
pub use crate::artifact::Attribute;
pub use crate::artifact::{ArtifactSelector, Constrained};
pub use crate::context::new_context;
pub use crate::error::{AnalyzerError, QueryResult};
pub use crate::query::Output;
use crate::query::{Circuit, Query};
use crate::selection::{Answer, Answers, Evidence};
use crate::Cardinality;
pub use crate::Environment;
use crate::Fact;
use crate::{try_stream, EvaluationContext, Source};
use crate::{Entity, Field, Parameters, QueryError, Requirement, Schema, Term, Type, Value};
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

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct FactApplication {
    cardinality: Cardinality,
    the: Term<Attribute>,
    of: Term<Entity>,
    is: Term<Value>,
    cause: Term<Cause>,
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
            let requirement = Requirement::new();
            let mut schema = Schema::new();

            schema.insert(
                "the".to_string(),
                Field {
                    description: "Attribute of the fact".to_string(),
                    content_type: Some(Type::Symbol),
                    requirement: requirement.required(),
                    cardinality: Cardinality::One,
                },
            );

            schema.insert(
                "of".to_string(),
                Field {
                    description: "Entity of the fact".to_string(),
                    content_type: Some(Type::Entity),
                    requirement: requirement.required(),
                    cardinality: Cardinality::One,
                },
            );

            schema.insert(
                "is".to_string(),
                Field {
                    description: "Value of the fact".to_string(),
                    content_type: None, // Can be any type
                    requirement: requirement.required(),
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
            cause: self.cause.clone(),
        }
    }

    pub fn new(
        the: Term<Attribute>,
        of: Term<Entity>,
        is: Term<Value>,
        cause: Term<Cause>,
        cardinality: Cardinality,
    ) -> Self {
        Self {
            cardinality,
            the,
            of,
            is,
            cause,
        }
    }

    /// Get the 'the' (attribute) term
    pub fn the(&self) -> &Term<Attribute> {
        &self.the
    }

    /// Get the 'of' (entity) term
    pub fn of(&self) -> &Term<Entity> {
        &self.of
    }

    /// Get the 'is' (value) term
    pub fn is(&self) -> &Term<Value> {
        &self.is
    }

    pub fn cause(&self) -> &Term<Cause> {
        &self.cause
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
    /// Resolves variables from the given answer.
    pub fn resolve_from_answer(&self, source: &Answer) -> Self {
        let the = source.resolve_term(&self.the);
        let of = source.resolve_term(&self.of);
        let is = source.resolve_term(&self.is);
        let cause = source.resolve_term(&self.cause);

        Self {
            the,
            of,
            is,
            cause,
            cardinality: self.cardinality,
        }
    }
    /// Evaluate with fact provenance tracking - returns Answers instead of Match-based Selection
    pub fn evaluate_with_provenance<S: Source, M: Answers>(
        &self,
        source: S,
        answers: M,
    ) -> impl Answers {
        let selector = self.clone();
        try_stream! {
            for await each in answers {
                let input = each?;
                let selection = selector.resolve_from_answer(&input);

                // NOTE: We do not capture provenance for facts used to resolve the selector.
                // For example, if we query Fact { the: ?attr, of: alice, is: ?value }
                // and 'alice' came from a previous fact, we don't track that previous fact here.
                // We only track the facts that directly match this FactApplication pattern.
                // This may need reconsideration in the future for complete provenance tracking.

                for await artifact in source.select((&selection).try_into()?) {
                    let artifact = artifact?;

                    // Create fact for provenance tracking
                    let fact = Fact::Assertion {
                        the: artifact.the.clone(),
                        of: artifact.of.clone(),
                        is: artifact.is.clone(),
                        cause: artifact.cause.unwrap_or(Cause([0; 32])),
                    };

                    // Create a new answer by concluding variables and recording the application
                    let mut answer = input.clone();
                    answer.merge(Evidence::Selected {
                        application: &selector,
                        fact: &fact,
                    })?;


                    yield answer;
                }
            }
        }
    }

    pub fn realize(&self, source: Answer) -> Result<Fact<Value>, QueryError> {
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
        use futures_util::stream::once;

        // Use the Answer-based approach for proper provenance tracking
        let initial_answer = once(async move { Ok(Answer::new()) });
        let answers = self.evaluate_with_provenance(source.clone(), initial_answer);
        let query = self.clone();

        try_stream! {
            for await answer in answers {
                yield answer?.realize(&query)?;
            }
        }
    }
}

impl Circuit for FactApplication {
    fn evaluate<S: Source, M: Answers>(&self, context: EvaluationContext<S, M>) -> impl Answers {
        // Use the Answer-based implementation
        self.evaluate_with_provenance(context.source, context.selection)
    }
}

impl Query<Fact> for FactApplication {
    fn realize(&self, input: Answer) -> Result<Fact, QueryError> {
        input.realize(self)
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

        write!(f, "of: {},", self.of)?;

        write!(f, "is: {},", self.is)?;

        write!(f, "cause: {},", self.cause)?;

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
}

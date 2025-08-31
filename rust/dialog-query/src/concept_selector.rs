//! ConceptSelector for querying concepts
//!
//! This module implements the ConceptSelector type which represents a query
//! pattern for matching concepts in the knowledge base. It's similar to
//! FactSelector but specifically designed for concept queries.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::artifact::{Entity, Value};
use crate::error::{QueryError, QueryResult};
use crate::fact_selector::{FactSelector, FactSelectorPlan};
use crate::plan::{EvaluationContext, EvaluationPlan};
use crate::premise::Premise;
use crate::selection::{Match, Selection as SelectionTrait};
use crate::syntax::VariableScope;
use crate::term::Term;
use async_stream::try_stream;
use futures_util::StreamExt;

/// A selector for querying concepts in the knowledge base
///
/// ConceptSelector represents a pattern for matching concepts by specifying:
/// - The concept name
/// - The entity the concept applies to
/// - A map of attribute constraints
///
/// # Usage
///
/// ConceptSelectors are typically created when querying for concept instances:
/// ```rust,ignore
/// ConceptSelector {
///     concept: "Person".to_string(),
///     entity: Term::var("person"),
///     attributes: BTreeMap::from([
///         ("name".to_string(), Term::from("Alice")),
///         ("age".to_string(), Term::var("age")),
///     ]),
/// }
/// ```
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ConceptSelector {
    /// The name of the concept to query
    pub concept: String,

    /// The entity term that the concept applies to
    pub entity: Term<Entity>,

    /// Map of attribute names to their value terms
    pub attributes: BTreeMap<String, Term<Value>>,
}

impl ConceptSelector {
    /// Create a new ConceptSelector with the given concept name and entity
    ///
    /// Initializes with an empty attributes map that can be populated
    /// using the builder pattern or by direct manipulation.
    pub fn new<C: Into<String>, E: Into<Term<Entity>>>(concept: C, entity: E) -> Self {
        Self {
            concept: concept.into(),
            entity: entity.into(),
            attributes: BTreeMap::new(),
        }
    }

    /// Add an attribute constraint to this selector
    ///
    /// Builder method for fluently adding attribute constraints:
    /// ```rust,ignore
    /// ConceptSelector::new("Person", Term::var("p"))
    ///     .with_attribute("name", Term::from("Alice"))
    ///     .with_attribute("age", Term::var("age"))
    /// ```
    pub fn with_attribute<N: Into<String>, V: Into<Term<Value>>>(
        mut self,
        name: N,
        value: V,
    ) -> Self {
        self.attributes.insert(name.into(), value.into());
        self
    }

    /// Check if this selector has any attribute constraints
    pub fn has_attributes(&self) -> bool {
        !self.attributes.is_empty()
    }

    /// Get the number of attribute constraints
    pub fn attribute_count(&self) -> usize {
        self.attributes.len()
    }
}

impl Default for ConceptSelector {
    fn default() -> Self {
        Self {
            concept: String::new(),
            entity: Term::default(),
            attributes: BTreeMap::new(),
        }
    }
}

/// Execution plan for a concept selector
///
/// This plan converts the concept selector into fact selectors:
/// - One for the concept classification (e.g., "Person/is-a")
/// - One for each attribute constraint
#[derive(Debug, Clone)]
pub struct ConceptSelectorPlan {
    /// The original concept selector
    pub selector: ConceptSelector,
    /// The fact selectors that need to be executed
    pub fact_selectors: Vec<FactSelector<Value>>,
    /// The corresponding plans for each fact selector
    pub fact_plans: Vec<FactSelectorPlan<Value>>,
    /// Cost estimate for this operation
    pub cost: f64,
}

impl ConceptSelectorPlan {
    /// Create a new concept selector plan
    pub fn new(selector: ConceptSelector, scope: &VariableScope) -> QueryResult<Self> {
        let mut fact_selectors = Vec::new();
        let mut fact_plans = Vec::new();
        let mut total_cost = 0.0;

        // Create fact selector for concept classification
        // Pattern: attribute = "{concept}/is-a", of = entity, is = true
        let classification_attribute = format!("{}/is-a", selector.concept);
        let classification_selector = FactSelector::new()
            .the(classification_attribute)
            .of(selector.entity.clone())
            .is(Value::Boolean(true));

        let classification_plan = classification_selector.plan(scope)?;
        total_cost += classification_plan.cost();
        fact_selectors.push(classification_selector);
        fact_plans.push(classification_plan);

        // Create fact selectors for each attribute constraint
        // Pattern: attribute = "{concept}/{attribute_name}", of = entity, is = value
        for (attr_name, value_term) in &selector.attributes {
            let attribute = format!("{}/{}", selector.concept, attr_name);
            let attr_selector = FactSelector::new()
                .the(attribute)
                .of(selector.entity.clone())
                .is(value_term.clone());

            let attr_plan = attr_selector.plan(scope)?;
            total_cost += attr_plan.cost();
            fact_selectors.push(attr_selector);
            fact_plans.push(attr_plan);
        }

        Ok(ConceptSelectorPlan {
            selector,
            fact_selectors,
            fact_plans,
            cost: total_cost,
        })
    }
}

impl Plan for ConceptSelectorPlan {}

impl EvaluationPlan for ConceptSelectorPlan {
    fn cost(&self) -> f64 {
        self.cost
    }

    fn evaluate<S, M>(&self, context: EvaluationContext<S, M>) -> impl SelectionTrait + '_
    where
        S: crate::artifact::ArtifactStore + Clone + Send + 'static,
        M: SelectionTrait + 'static,
    {
        let store = context.store;
        let selection = context.selection;
        let fact_plans = self.fact_plans.clone();

        try_stream! {
            // Process each frame from the input selection
            for await frame in selection {
                let mut current_frame = frame?;
                let mut all_matched = true;

                // Apply each fact selector plan in sequence
                // All must match for the concept to match
                for plan in &fact_plans {
                    // Create a single-frame selection for this plan
                    let single_frame_selection = futures_util::stream::iter(vec![Ok(current_frame.clone())]);
                    let plan_context = EvaluationContext::single(store.clone(), single_frame_selection);

                    // Evaluate the plan and collect results
                    let results: Vec<Match> = plan.evaluate(plan_context)
                        .collect::<Vec<Result<Match, QueryError>>>()
                        .await
                        .into_iter()
                        .collect::<Result<Vec<_>, _>>()
                        .map_err(|e| QueryError::from(e))?;

                    if results.is_empty() {
                        // This fact selector didn't match, so the concept doesn't match
                        all_matched = false;
                        break;
                    } else {
                        // Use the first match (there should only be one since we started with one frame)
                        current_frame = results.into_iter().next().unwrap();
                    }
                }

                // If all fact selectors matched, yield the final frame
                if all_matched {
                    yield current_frame;
                }
            }
        }
    }
}

impl Premise for ConceptSelector {
    type Plan = ConceptSelectorPlan;

    fn plan(&self, scope: &VariableScope) -> QueryResult<Self::Plan> {
        ConceptSelectorPlan::new(self.clone(), scope)
    }
}

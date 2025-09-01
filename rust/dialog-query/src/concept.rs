use crate::artifact::{Entity, Value};
use crate::attribute::Attribute;
use crate::error::{QueryError, QueryResult};
use crate::plan::{Cost, EvaluationContext, EvaluationPlan};
use crate::Selection;
use crate::premise::Premise;
use crate::selection::Selection as SelectionTrait;
use crate::statement::Statement;
use crate::term::Term;
use crate::VariableScope;
use crate::{FactSelector, FactSelectorPlan, Statements};
use dialog_artifacts::Instruction;
use std::collections::{HashMap, HashSet};

/// Concept is a set of attributes associated with entity representing an
/// abstract idea. It is a tool for the domain modeling and in some regard
/// similar to a table in relational database or a collection in the document
/// database, but unlike them it is disconnected from how information is
/// organized, in that sense it is more like view into which you can also insert.
///
/// Concepts are used to describe conclusions of the rules, providing a mapping
/// between conclusions and facts. In that sense you concepts are on-demand
/// cache of all the conclusions from the associated rules.
pub trait Concept: Clone + std::fmt::Debug {
    type Instance: Instance;
    /// Type describing attributes of this concept.
    type Attributes: Attributes;
    /// Type representing a query of this concept. It is a set of terms
    /// corresponding to the set of attributes defined by this concept.
    /// It is used as premise of the rule.
    type Match: Match<Instance = Self::Instance, Attributes = Self::Attributes>;
    /// Type representing an assertion of this concept. It is used in the
    /// inductive rules that describe how state of the concept changes
    /// (or persists) over time.
    type Assert;
    /// Type representing a retraction of this concept. It is used in the
    /// inductive rules to describe conditions for the of the concepts lifecycle.
    type Retract;

    fn name() -> &'static str;

    /// Returns the static list of attributes defined for this concept
    ///
    /// This is a convenience method that delegates to the associated Attributes type.
    /// It provides easy access to concept attributes without having to explicitly
    /// reference the Attributes associated type.
    fn attributes() -> &'static [(&'static str, Attribute<Value>)] {
        Self::Attributes::attributes()
    }

    /// Create an attributes pattern for querying this concept
    ///
    /// This method enables fluent query building with .is() and .not() methods:
    /// ```rust,ignore
    /// use dialog_query::Term;
    ///
    /// let person_query = Person::r#match(Term::var("entity"));
    /// // person_query.name.is("John").age.not(25);  // Future API
    /// ```
    fn r#match<T: Into<Term<Entity>>>(this: T) -> Self::Attributes;
}

/// Every assertion or retraction can be decomposed into a set of
/// assertion / retraction.
///
/// This trait enables us to define each Concpet::Assert and Concpet::Retract
/// such that it could be decomposed into a set of instructions which can be
/// then be committed.
pub trait Instructions {
    type IntoIter: IntoIterator<Item = Instruction>;
    fn instructions(self) -> Self::IntoIter;
}

/// Concepts can be matched and this trait describes an abstract match for the
/// concept. Each match should be translatable into a set of statements making
/// it possible to spread it into a query.
pub trait Match {
    /// Instance of the concept that this match can produce.
    type Instance: Instance;
    /// Attributes describing the mapping between concept and it's instance.
    type Attributes: Attributes;

    /// Provides term for a given property name in the corresponding concept
    fn term_for(&self, name: &str) -> Option<&Term<Value>>;

    fn this(&self) -> Term<Entity>;
}

impl<T: Match + Clone + std::fmt::Debug> Premise for T {
    type Plan = Plan;

    fn plan(&self, scope: &VariableScope) -> QueryResult<Self::Plan> {
        // Step 1: Create all conjunct plans
        let mut all_conjuncts: Vec<FactSelectorPlan<Value>> = vec![];
        let entity = self.this();

        for (name, attribute) in T::Attributes::attributes() {
            let term = self.term_for(name).unwrap();
            let select = FactSelector::new()
                .the(attribute.the())
                .of(entity.clone())
                .is(term.clone());
            let conjunct = select.plan(&scope)?;
            all_conjuncts.push(conjunct);
        }

        // Step 2: Implement optimal join ordering (like Join.plan in JS)
        let ordered_conjuncts = optimal_join_ordering(all_conjuncts, scope)?;

        // Step 3: Calculate total cost
        let mut total_cost = Cost::Estimate(0);
        for conjunct in &ordered_conjuncts {
            total_cost.join(conjunct.cost());
        }

        Ok(Plan {
            cost: total_cost,
            conjuncts: ordered_conjuncts,
        })
    }
}

#[derive(Debug, Clone)]
pub struct Plan {
    cost: Cost,
    conjuncts: Vec<FactSelectorPlan<Value>>,
}

impl EvaluationPlan for Plan {
    fn cost(&self) -> &Cost {
        &self.cost
    }
    fn evaluate<S, M>(&self, context: EvaluationContext<S, M>) -> impl Selection
    where
        S: crate::artifact::ArtifactStore + Clone + Send + 'static,
        M: SelectionTrait + 'static,
    {
        let store = context.store;

        // This implementation needs to be redesigned for the new trait signature
        // For now, return the input selection unchanged
        context.selection
    }
}

/// Describes an instance of a concept. It is expected that each concept is
/// can be materialized from the selection::Match.
pub trait Instance {
    /// Each instance has a corresponding entity and this method
    /// returns a reference to it.
    fn this(&self) -> Entity;
}

// Schema describes mapping between concept properties and attributes that
// correspond to those properties.
pub trait Attributes {
    fn attributes() -> &'static [(&'static str, Attribute<Value>)];
}

/// Implements optimal join ordering algorithm inspired by Join.plan in JS
///
/// This function takes a list of conjunct plans and orders them for optimal execution
/// using a cost-based greedy algorithm that respects data dependencies.
fn optimal_join_ordering(
    conjuncts: Vec<FactSelectorPlan<Value>>,
    scope: &VariableScope,
) -> QueryResult<Vec<FactSelectorPlan<Value>>> {
    if conjuncts.is_empty() {
        return Ok(vec![]);
    }

    // Step 1: Analyze dependencies and costs
    let mut bound_variables: HashSet<String> = HashSet::new();
    let mut ready: Vec<(usize, &FactSelectorPlan<Value>)> = Vec::new();
    let mut blocked: HashMap<String, Vec<usize>> = HashMap::new();

    // Initialize bound variables from the current scope
    // TODO: Extract variable names from scope - for now assume none are bound

    // Step 2: Categorize conjuncts as ready or blocked
    for (index, conjunct) in conjuncts.iter().enumerate() {
        let required_vars = extract_required_variables(conjunct);
        let unbound_vars: Vec<String> = required_vars
            .iter()
            .filter(|var| !bound_variables.contains(*var))
            .cloned()
            .collect();

        if unbound_vars.is_empty() {
            // This conjunct can execute immediately
            ready.push((index, conjunct));
        } else {
            // This conjunct is blocked waiting for variables
            for var in unbound_vars {
                blocked.entry(var).or_insert_with(Vec::new).push(index);
            }
        }
    }

    // Step 3: Greedy selection algorithm
    let mut ordered_indices: Vec<usize> = Vec::new();
    let mut processed: HashSet<usize> = HashSet::new();

    while !ready.is_empty() {
        // Find the ready conjunct with the lowest estimated cost
        let best_index = find_lowest_cost_ready(&ready);
        let (conjunct_index, conjunct) = ready.remove(best_index);

        // Add to ordered execution plan
        ordered_indices.push(conjunct_index);
        processed.insert(conjunct_index);

        // Step 4: Update bound variables and check for newly ready conjuncts
        let output_vars = extract_output_variables(conjunct);
        for var in output_vars {
            bound_variables.insert(var.clone());

            // Check if any blocked conjuncts can now become ready
            if let Some(blocked_indices) = blocked.remove(&var) {
                for blocked_index in blocked_indices {
                    if processed.contains(&blocked_index) {
                        continue;
                    }

                    let blocked_conjunct = &conjuncts[blocked_index];
                    let still_required: Vec<String> = extract_required_variables(blocked_conjunct)
                        .iter()
                        .filter(|v| !bound_variables.contains(*v))
                        .cloned()
                        .collect();

                    if still_required.is_empty() {
                        ready.push((blocked_index, blocked_conjunct));
                    } else {
                        // Still blocked on other variables
                        for still_blocked_var in still_required {
                            blocked
                                .entry(still_blocked_var)
                                .or_insert_with(Vec::new)
                                .push(blocked_index);
                        }
                    }
                }
            }
        }
    }

    // Step 5: Check for unresolvable dependencies
    if ordered_indices.len() != conjuncts.len() {
        let unprocessed: Vec<usize> = (0..conjuncts.len())
            .filter(|i| !processed.contains(i))
            .collect();
        return Err(QueryError::PlanningError {
            message: format!(
                "Cannot resolve dependencies for conjuncts at indices: {:?}. \
                These conjuncts have circular dependencies or depend on unbound variables.",
                unprocessed
            ),
        });
    }

    // Step 6: Return conjuncts in optimal order
    let ordered_conjuncts = ordered_indices
        .into_iter()
        .map(|i| conjuncts[i].clone())
        .collect();

    Ok(ordered_conjuncts)
}

/// Find the index of the ready conjunct with the lowest estimated cost
fn find_lowest_cost_ready(ready: &[(usize, &FactSelectorPlan<Value>)]) -> usize {
    let mut best_index = 0;
    let mut best_cost = estimate_cost(ready[0].1);

    for (i, (_, conjunct)) in ready.iter().enumerate().skip(1) {
        let cost = estimate_cost(conjunct);
        if cost < best_cost {
            best_cost = cost;
            best_index = i;
        }
    }

    best_index
}

/// Estimate execution cost of a conjunct plan
fn estimate_cost(plan: &FactSelectorPlan<Value>) -> u32 {
    match plan.cost() {
        Cost::Infinity => u32::MAX,
        Cost::Estimate(cost) => *cost as u32,
    }
}

/// Extract variables that must be bound for this conjunct to execute
/// Based on the FactSelector pattern, looks for variable terms
fn extract_required_variables(plan: &FactSelectorPlan<Value>) -> Vec<String> {
    let mut vars = Vec::new();
    let selector = &plan.selector;

    // Check entity variable (of)
    if let Some(term) = &selector.of {
        if let Some(var_name) = term.name() {
            vars.push(var_name.to_string());
        }
    }

    // Check attribute variable (the) - less common but possible
    if let Some(term) = &selector.the {
        if let Some(var_name) = term.name() {
            vars.push(var_name.to_string());
        }
    }

    // Check value variable (is)
    if let Some(term) = &selector.is {
        if let Some(var_name) = term.name() {
            vars.push(var_name.to_string());
        }
    }

    vars
}

/// Extract variables that will be bound after this conjunct executes
/// These are the variables that appear in the conjunct's output
fn extract_output_variables(plan: &FactSelectorPlan<Value>) -> Vec<String> {
    // For fact selectors, the output variables are the same as input variables
    // since fact selection binds the variables it matches
    extract_required_variables(plan)
}

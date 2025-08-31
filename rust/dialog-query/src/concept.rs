use crate::artifact::Entity;
use crate::artifact::Value;
use crate::attribute::Attribute;
use crate::error::QueryResult;
use crate::plan::Cost;
use crate::plan::{EvaluationContext, EvaluationPlan};
use crate::premise::Premise;
use crate::query::Query;
use crate::selection::Selection as SelectionTrait;
use crate::statement::Statement;
use crate::term::Term;
use crate::Fact;
use crate::FactSelector;
use crate::Statements;
use crate::VariableScope;
use async_stream::try_stream;
use dialog_artifacts::Instruction;
use dialog_common::ConditionalSend;
use futures_util::StreamExt;

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

    // /// Returns the static list of attributes defined for this concept
    // ///
    // /// Each attribute is represented as an Attribute<Value> to provide a uniform
    // /// collection while preserving type information through the data_type() method.
    // fn attributes() -> &'static [Attribute<Value>];

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

impl<T: Match> Statements for T {
    type IntoIter = Vec<Statement>;

    fn statements(self) -> Self::IntoIter {
        let entity = self.this();
        let st: Vec<Statement> = T::Attributes::attributes()
            .into_iter()
            .map(|(name, attr)| {
                Statement::select(attr.of(&entity).is(self.term_for(name).unwrap()))
            })
            .collect();

        st // Return the Vec directly, not st.into_iter()
    }
}

enum Estimate {
    Infinity,
    Cost(f64),
}

impl<T: Match + Clone + std::fmt::Debug> Premise for T {
    type Plan = Plan;

    fn plan(&self, scope: &VariableScope) -> QueryResult<Self::Plan> {
        let conjuncts = [];
        let cost = Cost::Estimate(0);

        let entity = self.this();
        for (name, attribute) in T::Attributes::attributes() {
            let term = self.term_for(name).unwrap();
            // Select the attribute for the entity, plan select and
            // add it to the conjuncts vector
            let select = Fact::select().the(attribute.the()).of(&entity).is(&term);
            let conjuct = select.plan(&scope)?;
            // account for the cost of the conjunct
            cost.add(&conjuct.cost);

            conjuncts.push(select.plan(&scope)?);
        }

        let bindings = scope.clone();

        Ok(Plan {})
    }
}

#[derive(Debug, Clone)]
pub struct Plan {}

impl EvaluationPlan for Plan {
    fn cost(&self) -> f64 {
        0.0
    }
    fn evaluate<S, M>(&self, context: EvaluationContext<S, M>) -> impl SelectionTrait + '_
    where
        S: crate::artifact::ArtifactStore + Clone + Send + 'static,
        M: SelectionTrait + 'static,
    {
        let store = context.store;
        let selection = context.selection;

        try_stream! {
            for await frame in selection {
                let mut current_frame = frame?;
                yield current_frame;
            }

        }
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

use dialog_artifacts::Instruction;

use crate::artifact::Entity;
use crate::term::Term;
use crate::Statements;

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
    /// Type representing a query of this concept. It is a set of terms
    /// corresponding to the set of attributes defined by this concept.
    /// It is used as premise of the rule.
    type Match: Statements;
    /// Type representing an assertion of this concept. It is used in the
    /// inductive rules that describe how state of the concept changes
    /// (or persists) over time.
    type Assert;
    /// Type representing a retraction of this concept. It is used in the
    /// inductive rules to describe conditions for the of the concepts lifecycle.
    type Retract;
    /// Type describing attributes of this concept.
    type Attributes;

    fn name() -> &'static str;

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

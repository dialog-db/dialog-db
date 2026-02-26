use std::collections::HashMap;

use crate::Relation;
use crate::Term;
use crate::artifact::Value;
use crate::formula::application::FormulaApplication;
use crate::relation::application::RelationApplication;

use super::Factors;

/// Describes how a value binding was obtained, used when merging new
/// bindings into an [`Answer`](super::Answer).
///
/// When a premise produces results, each variable binding is accompanied
/// by an `Evidence` value that explains its origin. The answer converts
/// this evidence into a persistent [`Factor`](super::Factor) that becomes
/// part of the binding's provenance record.
pub enum Evidence<'a> {
    /// Selected from a relation query.
    Relation {
        /// The relation application that produced this match.
        application: &'a RelationApplication,
        /// The matched relation.
        fact: &'a Relation,
    },
    /// Derived using formula application.
    Derived {
        /// The term being bound.
        term: &'a Term<Value>,
        /// The computed value.
        value: Box<Value>,
        /// The facts that were read to produce this derived value, keyed by parameter name.
        from: HashMap<String, Factors>,
        /// The formula application that produced this value.
        formula: &'a FormulaApplication,
    },
    /// Applied parameter.
    Parameter {
        /// The term being bound.
        term: &'a Term<Value>,
        /// The parameter value.
        value: &'a Value,
    },
}

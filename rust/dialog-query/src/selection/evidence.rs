use std::collections::HashMap;

use crate::Claim;
use crate::artifact::Value;
use crate::formula::query::FormulaQuery;
use crate::parameter::Parameter;
use crate::relation::query::RelationQuery;

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
        application: &'a RelationQuery,
        /// The matched relation.
        fact: &'a Claim,
    },
    /// Derived using formula application.
    Derived {
        /// The parameter being bound.
        term: &'a Parameter,
        /// The computed value.
        value: Box<Value>,
        /// The facts that were read to produce this derived value, keyed by parameter name.
        from: HashMap<String, Factors>,
        /// The formula application that produced this value.
        formula: &'a FormulaQuery,
    },
    /// Applied parameter.
    Parameter {
        /// The parameter being bound.
        term: &'a Parameter,
        /// The parameter value.
        value: &'a Value,
    },
}

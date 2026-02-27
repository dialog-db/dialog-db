use std::collections::HashMap;
use std::sync::Arc;

use crate::Relation;
use crate::artifact::Value;
use crate::formula::query::FormulaQuery;
use crate::relation::query::RelationQuery;

use super::Factors;
use super::Selector;

/// Records the origin of a single variable binding inside an [`Answer`](super::Answer).
///
/// Every bound variable in an answer is backed by at least one `Factor`
/// explaining where its value came from. This provenance information
/// flows all the way to the query consumer, enabling features like
/// change tracking and incremental re-evaluation.
///
/// Factors come in three flavours:
/// - `Selected` — the value was read directly from a stored fact.
/// - `Derived` — the value was computed by a formula, with references
///   to the input facts that fed the computation.
/// - `Parameter` — the value was supplied as a query-time constant.
#[derive(Clone, Debug)]
pub enum Factor {
    /// A value selected directly from a matched fact.
    Selected {
        /// Which fact component this value came from.
        selector: Selector,
        /// The relation application that matched this fact.
        application: Arc<RelationQuery>,
        /// The matched fact itself.
        fact: Arc<Relation>,
    },
    /// Derived from a formula computation - tracks the input facts and formula used.
    Derived {
        /// The computed value.
        value: Value,
        /// The facts that were read to produce this derived value, keyed by parameter name.
        from: HashMap<String, Factors>,
        /// The formula application that produced this value.
        formula: Arc<FormulaQuery>,
    },
    /// A value provided externally as a query parameter.
    Parameter {
        /// The parameter value.
        value: Value,
    },
}

impl Factor {
    /// Get the underlying relation if this factor is directly from a relation (not derived)
    pub fn fact(&self) -> Option<&Relation> {
        match self {
            Factor::Selected { fact, .. } => Some(fact.as_ref()),
            Factor::Derived { .. } => None,
            Factor::Parameter { .. } => None,
        }
    }

    pub(crate) fn content(&self) -> Value {
        match self {
            Factor::Selected { selector, fact, .. } => match selector {
                Selector::The => Value::Symbol(fact.the()),
                Selector::Of => Value::Entity(fact.of().clone()),
                Selector::Is => fact.is().clone(),
                Selector::Cause => Value::Bytes(fact.cause().clone().0.into()),
            },
            Factor::Derived { value, .. } => value.clone(),
            Factor::Parameter { value, .. } => value.clone(),
        }
    }
}

// Implement Hash and Eq based on Arc pointer identity for fact variants,
// and value/from/formula for Derived variant
impl std::hash::Hash for Factor {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        // Hash the discriminant first to distinguish between variants
        std::mem::discriminant(self).hash(state);

        match self {
            Factor::Selected {
                selector,
                fact,
                application,
            } => {
                selector.hash(state);
                // Hash based on the Arc pointer address, not the content
                let fact_ptr = Arc::as_ptr(fact) as *const ();
                fact_ptr.hash(state);
                let app_ptr = Arc::as_ptr(application) as *const ();
                app_ptr.hash(state);
            }
            Factor::Parameter { value } => {
                value.hash(state);
            }
            Factor::Derived {
                value,
                from,
                formula,
            } => {
                // For derived factors, hash the value, input factors, and formula pointer
                value.hash(state);

                // Hash the from map (order-independent by using sorted keys)
                let mut keys: Vec<_> = from.keys().collect();
                keys.sort();
                for key in keys {
                    key.hash(state);
                    if let Some(factors) = from.get(key) {
                        // Hash the factors content
                        factors.content().hash(state);
                    }
                }

                // Hash the formula pointer
                let ptr = Arc::as_ptr(formula) as *const ();
                ptr.hash(state);
            }
        }
    }
}

impl PartialEq for Factor {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (
                Factor::Selected {
                    selector: s1,
                    fact: a,
                    application: app1,
                },
                Factor::Selected {
                    selector: s2,
                    fact: b,
                    application: app2,
                },
            ) => s1 == s2 && Arc::ptr_eq(a, b) && Arc::ptr_eq(app1, app2),
            (
                Factor::Derived {
                    value: v1,
                    from: f1,
                    formula: formula1,
                },
                Factor::Derived {
                    value: v2,
                    from: f2,
                    formula: formula2,
                },
            ) => {
                // Compare values, input factors, and formula pointer
                v1 == v2
                    && f1.len() == f2.len()
                    && f1
                        .iter()
                        .all(|(k, factors1)| f2.get(k).is_some_and(|factors2| factors1 == factors2))
                    && Arc::ptr_eq(formula1, formula2)
            }
            (Factor::Parameter { value: v1 }, Factor::Parameter { value: v2 }) => v1 == v2,
            _ => false,
        }
    }
}

impl Eq for Factor {}

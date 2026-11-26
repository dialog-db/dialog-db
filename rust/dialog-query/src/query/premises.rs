//! A query of premises: what a rule's body is, asked with no rule.

use std::collections::BTreeMap;
use std::iter;
use std::sync::Arc;

use dialog_corpus::Entity;

use crate::Value;
use crate::concept::Conclusion;
use crate::corpus::Type as ValueType;
use crate::environment::Environment;
use crate::error::{EvaluationError, TypeError};
use crate::planner::{Conjunction, Planner};
use crate::premise::Premise;
use crate::selection::{self, Match};

use super::Application;

/// A query of premises, planned as a rule's body is and asked with no
/// rule: each row is the entity `this` binds and the values of the
/// variables it selects.
///
/// It is where a formula an embedder supplies
/// ([`Extension`](crate::formula::Extension)) is evaluated, since no rule
/// may hold one. It is planned when it is built, so a premise it cannot
/// plan, or a selected variable no premise binds, is refused then. A
/// subscription over it evaluates it again whenever what its premises
/// read changes: it is never restricted to one entity.
#[derive(Clone, Debug)]
pub struct PremiseQuery {
    plan: Arc<Conjunction>,
    this: String,
    select: Vec<String>,
}

impl PremiseQuery {
    /// The query of `premises` whose rows are the entity `this` binds
    /// and the values of `select`.
    pub fn new(
        premises: Vec<Premise>,
        this: impl Into<String>,
        select: impl IntoIterator<Item = impl Into<String>>,
    ) -> Result<Self, TypeError> {
        let this = this.into();
        let select: Vec<String> = select.into_iter().map(Into::into).collect();
        let plan = Planner::from(premises).plan(&Environment::new())?;
        if let Some(variable) = iter::once(&this)
            .chain(&select)
            .find(|variable| !plan.binds.contains(variable))
        {
            return Err(TypeError::UnselectedVariable {
                variable: variable.clone(),
            });
        }
        Ok(Self {
            plan: Arc::new(plan),
            this,
            select,
        })
    }
}

/// A row of a [`PremiseQuery`]: the entity and the selected values.
#[derive(Clone, Debug, PartialEq)]
pub struct PremiseRow {
    /// The entity `this` bound.
    pub this: Entity,
    /// Each selected variable's value, where the row binds one.
    pub values: BTreeMap<String, Value>,
}

impl Conclusion for PremiseRow {
    fn this(&self) -> &Entity {
        &self.this
    }
}

impl Application for PremiseQuery {
    type Conclusion = PremiseRow;

    fn evaluate<'a, Env, M: selection::Selection + 'a>(
        self,
        selection: M,
        env: &'a Env,
    ) -> impl selection::Selection + 'a
    where
        Env: crate::Scope<'a>,
    {
        (*self.plan).clone().evaluate(selection, env)
    }

    fn realize(&self, input: Match) -> Result<PremiseRow, EvaluationError> {
        let this = match input.value_of(&self.this) {
            Some(Value::Entity(entity)) => entity.clone(),
            Some(other) => {
                return Err(EvaluationError::TypeMismatch {
                    expected: ValueType::Entity,
                    actual: other.data_type(),
                });
            }
            None => {
                return Err(EvaluationError::UnboundVariable {
                    variable_name: self.this.clone(),
                });
            }
        };
        let values = self
            .select
            .iter()
            .filter_map(|variable| Some((variable.clone(), input.value_of(variable)?.clone())))
            .collect();
        Ok(PremiseRow { this, values })
    }
}

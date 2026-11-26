//! A query of premises: what a rule's body is, asked with no rule.

use std::collections::BTreeMap;
use std::sync::{Arc, LazyLock};

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

/// The entity every row of a [`PremiseQuery`] with no entity of its own
/// stands for, as a fold's row with no entity among its groups does.
/// What keeps a subscription's rows indexes them by an entity; a premise
/// query is never restricted to one, so they all share this one.
const UNKEYED: &str = "premises:";

static UNKEYED_ENTITY: LazyLock<Entity> =
    LazyLock::new(|| UNKEYED.parse().expect("a bare scheme is an entity"));

/// A query of premises, planned as a rule's body is and asked with no
/// rule: each row is the values of the variables it selects, and the
/// entity a variable binds when it is asked for one ([`this`](Self::this)).
///
/// It needs no entity: `notation/read` of a constant text, alone, is a
/// premise query, and so is `notation/analyze` of a constant document.
/// It is where a formula or resolver the engine declares and a crate of
/// dialog's own supplies ([`crate::supplied`]) is asked, since no rule
/// may hold one. It is planned when it is built, so a premise it cannot
/// plan, or a selected variable no premise binds, is refused then. A
/// subscription over it evaluates it again whenever what its premises
/// read changes: it is never restricted to one entity.
#[derive(Clone, Debug)]
pub struct PremiseQuery {
    plan: Arc<Conjunction>,
    this: Option<String>,
    select: Vec<String>,
}

impl PremiseQuery {
    /// The query of `premises` whose rows are the values of `select`.
    pub fn new(
        premises: Vec<Premise>,
        select: impl IntoIterator<Item = impl Into<String>>,
    ) -> Result<Self, TypeError> {
        let select: Vec<String> = select.into_iter().map(Into::into).collect();
        let plan = Planner::from(premises).plan(&Environment::new())?;
        let query = Self {
            plan: Arc::new(plan),
            this: None,
            select,
        };
        if let Some(variable) = query.select.iter().find(|variable| !query.binds(variable)) {
            return Err(TypeError::UnselectedVariable {
                variable: variable.clone(),
            });
        }
        Ok(query)
    }

    /// This query, its rows carrying also the entity `variable` binds:
    /// a row whose `variable` is bound to another value than an entity
    /// fails the query as it is answered.
    pub fn this(mut self, variable: impl Into<String>) -> Result<Self, TypeError> {
        let variable = variable.into();
        if !self.binds(&variable) {
            return Err(TypeError::UnselectedVariable { variable });
        }
        self.this = Some(variable);
        Ok(self)
    }

    fn binds(&self, variable: &str) -> bool {
        self.plan.binds.contains(variable)
    }
}

/// A row of a [`PremiseQuery`]: the selected values, and the entity when
/// the query asked for one.
#[derive(Clone, Debug, PartialEq)]
pub struct PremiseRow {
    /// The entity the query's [`this`](PremiseQuery::this) variable
    /// bound, when it named one.
    pub this: Option<Entity>,
    /// Each selected variable's value, where the row binds one: a
    /// variable the row binds to no value, an optional field the entity
    /// has no claim of, is absent here.
    pub values: BTreeMap<String, Value>,
}

impl Conclusion for PremiseRow {
    fn this(&self) -> &Entity {
        self.this.as_ref().unwrap_or(&UNKEYED_ENTITY)
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
        let this = match self
            .this
            .as_deref()
            .map(|this| (this, input.value_of(this)))
        {
            None => None,
            Some((_, Some(Value::Entity(entity)))) => Some(entity.clone()),
            Some((_, Some(other))) => {
                return Err(EvaluationError::TypeMismatch {
                    expected: ValueType::Entity,
                    actual: other.data_type(),
                });
            }
            Some((this, None)) => {
                return Err(EvaluationError::UnboundVariable {
                    variable_name: this.to_string(),
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

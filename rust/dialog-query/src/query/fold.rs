//! A query that folds: the rows of one query, grouped and folded with a
//! [`Reduce`], each group one row.
//!
//! It is not a rule. Nothing is installed under a name, no rule can read
//! it, so nothing stored can stand beside its value and it cannot close
//! a cycle. `select` and `subscribe` take it as they take any
//! [`Application`].
//!
//! The groups come from a query of their own, and the folded inputs from
//! another, read as optional: a group whose inputs match nothing is still
//! a group, its inputs all absent. So `count` and `sum` give their
//! identity, 0:
//!
//! ```no_run
//! # use dialog_query::reduce::{Aggregator, Reduce, ReduceEntry};
//! # use dialog_query::{Fold, Term};
//! # fn example<G: dialog_query::Application, Q: dialog_query::Application>(todos: G, tags: Q) {
//! // Each to-do of `todos` (binding ?t), with how many tags `tags`
//! // (binding ?t and ?g) gives it, none counted 0.
//! let counted = Fold::new(
//!     todos,
//!     tags,
//!     Reduce::new(
//!         vec!["t".into()],
//!         vec![ReduceEntry::new("tags", Aggregator::Count, Term::var("g"))],
//!     ),
//! );
//! # }
//! ```

use std::collections::{BTreeMap, HashMap};

use dialog_corpus::Entity;

use crate::Term;
use crate::concept::Conclusion;
use crate::corpus::Value;
use crate::error::EvaluationError;
use crate::reduce::Reduce;
use crate::selection::{Binding, Match, Selection};
use crate::try_stream;
use crate::types::Any;

use super::Application;

/// The entity a folded row stands for where its group has none: every
/// row of a fold with no entity among its groups is of this one.
const UNGROUPED: &str = "fold:";

/// A folded row: the value of each grouping field and each fold.
#[derive(Clone, Debug, PartialEq)]
pub struct FoldConclusion {
    this: Entity,
    values: BTreeMap<String, Option<Value>>,
}

impl FoldConclusion {
    /// The value of a grouping field or a fold, `None` where it is
    /// absent.
    pub fn get(&self, field: &str) -> Option<&Value> {
        self.values.get(field).and_then(Option::as_ref)
    }

    /// Every grouping field and fold and its value.
    pub fn values(&self) -> &BTreeMap<String, Option<Value>> {
        &self.values
    }
}

impl Conclusion for FoldConclusion {
    fn this(&self) -> &Entity {
        &self.this
    }
}

/// A query that folds the rows of `inputs`, grouped by the rows of
/// `groups`.
#[derive(Clone, Debug)]
pub struct Fold<G, Q> {
    groups: G,
    inputs: Q,
    reduce: Reduce,
}

impl<G, Q> Fold<G, Q> {
    /// Fold the rows of `inputs` with `reduce`, one row for each row of
    /// `groups`. The grouping fields of `reduce` are variables both
    /// queries bind; an input row joins the group row it agrees with on
    /// them. A group no input row joins is folded over inputs that are
    /// all absent.
    pub fn new(groups: G, inputs: Q, reduce: Reduce) -> Self {
        Self {
            groups,
            inputs,
            reduce,
        }
    }

    /// What it folds with.
    pub fn reduce(&self) -> &Reduce {
        &self.reduce
    }

    /// The query whose rows are the groups.
    pub fn groups(&self) -> &G {
        &self.groups
    }

    /// The query whose rows are folded.
    pub fn inputs(&self) -> &Q {
        &self.inputs
    }

    /// The key a row of either query is grouped under: the values it
    /// binds the grouping fields of [`reduce`](Self::reduce) to, written
    /// as their dag-cbor bytes. A group row and an input row with the
    /// same key are of one group.
    pub fn key(&self, row: &Match) -> Result<Vec<u8>, EvaluationError> {
        key(row, &self.reduce.groups)
    }

    /// Fold the group rows `groups`, each joined with the input rows
    /// `inputs` gives for its [`key`](Self::key), or, where it gives
    /// none, with each input absent, by [`Reduce::fold`]: one folded row
    /// for each key among `groups` that some joined row is left for.
    ///
    /// This is the whole of a fold: [`evaluate`](Application::evaluate)
    /// calls it once with every group, and a subscription of a fold
    /// calls it with the group rows of one key to fold that group again.
    /// [`Reduce::fold`] groups by key and folds each group's values in an
    /// order of their own, never the order of the rows, so a group folded
    /// alone gives the row it gives folded among every other.
    pub fn fold_groups<'r, I>(
        &self,
        groups: impl IntoIterator<Item = &'r Match>,
        inputs: impl Fn(&[u8]) -> I,
    ) -> Result<Vec<Match>, EvaluationError>
    where
        I: IntoIterator<Item = &'r Match>,
    {
        let mut rows = Vec::new();
        for group in groups {
            let mut found = inputs(&self.key(group)?).into_iter().peekable();
            if found.peek().is_none() {
                let mut row = group.clone();
                for entry in &self.reduce.entries {
                    if !row.contains(&entry.input) {
                        row.bind_absent(&entry.input)?;
                    }
                }
                rows.push(row);
            } else {
                for input in found {
                    if let Some(row) = group.clone().combine(input) {
                        rows.push(row);
                    }
                }
            }
        }
        self.reduce.clone().fold_rows(&rows)
    }
}

/// How a group's key is written, to look its inputs up by.
fn key(row: &Match, groups: &[String]) -> Result<Vec<u8>, EvaluationError> {
    let values: Vec<Option<Value>> = groups
        .iter()
        .map(|name| match row.lookup(&Term::<Any>::var(name.as_str()))? {
            Binding::Present(value) => Ok(Some(value)),
            Binding::Absent => Ok(None),
        })
        .collect::<Result<_, EvaluationError>>()?;
    serde_ipld_dagcbor::to_vec(&values).map_err(|error| EvaluationError::Serialization {
        message: error.to_string(),
    })
}

impl<G, Q> Application for Fold<G, Q>
where
    G: Application,
    Q: Application,
{
    type Conclusion = FoldConclusion;

    fn evaluate<'a, Env, M: Selection + 'a>(self, selection: M, env: &'a Env) -> impl Selection + 'a
    where
        Env: crate::Scope<'a>,
    {
        try_stream! {
            // The inputs of each group, by its key.
            let mut joined: HashMap<Vec<u8>, Vec<Match>> = HashMap::new();
            for row in self.inputs.clone().evaluate(Match::new().seed(), env).try_vec().await? {
                joined.entry(self.key(&row)?).or_default().push(row);
            }
            // Every group, with its inputs, or with each input absent.
            let groups = self.groups.clone().evaluate(Match::new().seed(), env).try_vec().await?;
            let folded = self.fold_groups(&groups, |key| joined.get(key).into_iter().flatten())?;
            // The caller's rows, each joined to the folded rows it
            // agrees with.
            for caller in selection.try_vec().await? {
                for row in &folded {
                    if let Some(row) = caller.clone().combine(row) {
                        yield row;
                    }
                }
            }
        }
    }

    fn realize(&self, input: Match) -> Result<FoldConclusion, EvaluationError> {
        let mut values = BTreeMap::new();
        let mut this = None;
        let names = self
            .reduce
            .groups
            .iter()
            .chain(self.reduce.entries.iter().map(|entry| &entry.field));
        for name in names {
            let value = match input.lookup(&Term::<Any>::var(name.as_str()))? {
                Binding::Present(value) => Some(value),
                Binding::Absent => None,
            };
            if this.is_none()
                && let Some(Value::Entity(entity)) = &value
            {
                this = Some(entity.clone());
            }
            values.insert(name.clone(), value);
        }
        let this = match this {
            Some(this) => this,
            None => UNGROUPED
                .parse()
                .map_err(|_| EvaluationError::Store("the ungrouped entity".into()))?,
        };
        Ok(FoldConclusion { this, values })
    }

    /// Each grouping field and fold bound to its value, absent where
    /// it is absent: what [`realize`](Application::realize) read.
    fn bindings(&self, conclusion: &FoldConclusion) -> Option<Match> {
        let mut row = Match::new();
        for (name, value) in &conclusion.values {
            let term = Term::<Any>::var(name.as_str());
            match value {
                Some(value) => row.bind(&term, value.clone()).ok()?,
                None => row.bind_absent(&term).ok()?,
            }
        }
        Some(row)
    }
}

#[cfg(test)]
mod tests {
    #[cfg(target_arch = "wasm32")]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    use super::*;
    use crate::attribute::Relation;
    use crate::attribute::query::AttributeQuery;
    use crate::query::Output as _;
    use crate::reduce::{Aggregator, ReduceEntry};
    use crate::session::RuleRegistry;
    use crate::source::test::TestEnv;
    use crate::the;
    use anyhow::Result;
    use dialog_peer::helpers::{test_repo, test_session_with_peer};
    use dialog_storage::provider::storage::VolatileSpace;

    type Operator = dialog_peer::Peer<VolatileSpace, dialog_peer::Session>;

    /// To-do 1 tagged `errand` and `home`, to-do 2 with no tag.
    async fn todos() -> Result<(Operator, dialog_repository::Branch, Entity, Entity)> {
        let (operator, profile) = test_session_with_peer().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;
        let one: Entity = "id:todo-1".parse()?;
        let two: Entity = "id:todo-2".parse()?;
        branch
            .transaction()
            .assert(
                the!("todo/title")
                    .of(one.clone())
                    .is("Buy milk".to_string()),
            )
            .assert(the!("todo/tag").of(one.clone()).is("home".to_string()))
            .assert(the!("todo/tag").of(one.clone()).is("errand".to_string()))
            .assert(
                the!("todo/title")
                    .of(two.clone())
                    .is("Call mum".to_string()),
            )
            .commit()
            .publish()
            .perform(&operator)
            .await?;
        let branch = repo.branch("main").open().perform(&operator).await?;
        Ok((operator, branch, one, two))
    }

    fn read(the: Term<Relation>, of: &str, is: &str, cause: &str) -> AttributeQuery {
        AttributeQuery::new(
            the,
            Term::<Entity>::var(of),
            Term::<Any>::var(is),
            Term::var(cause),
            None,
        )
    }

    fn folded(aggregator: Aggregator) -> Fold<AttributeQuery, AttributeQuery> {
        Fold::new(
            read(Term::from(the!("todo/title")), "t", "title", "c1"),
            read(Term::from(the!("todo/tag")), "t", "g", "c2"),
            Reduce::new(
                vec!["t".into()],
                vec![ReduceEntry::new("tags", aggregator, Term::var("g"))],
            ),
        )
    }

    async fn fold(aggregator: Aggregator) -> Result<(BTreeMap<Entity, Value>, Entity, Entity)> {
        let (operator, branch, one, two) = todos().await?;
        let source = TestEnv::new(&branch, &operator, RuleRegistry::new());
        let rows = folded(aggregator).perform(&source).try_vec().await?;
        let folded = rows
            .iter()
            .map(|row| (row.this().clone(), row.get("tags").cloned().unwrap()))
            .collect();
        Ok((folded, one, two))
    }

    /// `count` gives a row for each group, and 0 for a group whose
    /// inputs match nothing: a to-do with no tag counts 0.
    #[dialog_common::test]
    async fn it_counts_each_group_and_zero_for_none() -> Result<()> {
        let (counted, one, two) = fold(Aggregator::Count).await?;
        assert_eq!(
            counted,
            BTreeMap::from([(one, Value::UnsignedInt(2)), (two, Value::UnsignedInt(0))])
        );
        Ok(())
    }

    /// A caller's row joins only the folded rows it agrees with: a caller
    /// that binds the to-do gets that to-do's count alone.
    #[dialog_common::test]
    async fn it_joins_the_caller_after_folding() -> Result<()> {
        let (operator, branch, _, two) = todos().await?;
        let source = TestEnv::new(&branch, &operator, RuleRegistry::new());
        let mut caller = Match::new();
        caller.bind(&Term::<Any>::var("t"), Value::Entity(two.clone()))?;
        let rows =
            Selection::try_vec(folded(Aggregator::Count).evaluate(caller.seed(), &source)).await?;
        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0].lookup(&Term::<Any>::var("tags"))?,
            Binding::Present(Value::UnsignedInt(0))
        );
        Ok(())
    }
}

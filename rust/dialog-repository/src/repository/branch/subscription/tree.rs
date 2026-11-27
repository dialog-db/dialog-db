//! A tree subscription uses independent standing queries for the
//! levels of a projection. Only changed rows go through the projection.

use dialog_common::ConditionalSync;
use dialog_query::{Application, Conclusion, EvaluationError};
use thiserror::Error;

use super::{Delta, Subscription};
use crate::{Branch, OpenStackEnv, ProjectedNode, Stack, TreeEdit, TreeEditError, TreeProjection};

type Project<Q, V> = Box<
    dyn Fn(
            &<Q as Application>::Conclusion,
            &mut Vec<ProjectedNode<V>>,
        ) -> Result<(), EvaluationError>
        + Send
        + Sync,
>;

/// A standing query and the nodes one answer contributes. A projector
/// must describe a node the same way when a row arrives and leaves.
pub struct TreeSource<Q: Application, V> {
    query: Q,
    project: Project<Q, V>,
}

impl<Q: Application, V> TreeSource<Q, V> {
    /// Define how each changed row contributes local nodes to the tree.
    /// The queries together must form a forest, with stable IDs.
    pub fn new(
        query: Q,
        project: impl Fn(&Q::Conclusion, &mut Vec<ProjectedNode<V>>) -> Result<(), EvaluationError>
        + Send
        + Sync
        + 'static,
    ) -> Self {
        Self {
            query,
            project: Box::new(project),
        }
    }
}

/// Failure to obtain one consistent batch of edits.
#[derive(Debug, Error)]
pub enum TreeSubscriptionError {
    /// Reading or projecting a changed row failed.
    #[error(transparent)]
    Query(#[from] EvaluationError),
    /// The resulting tree would violate its identity or parent invariants.
    #[error(transparent)]
    Projection(#[from] TreeEditError),
    /// The branch or stack advanced throughout this poll. Retry to catch
    /// up. Changes already read are retained.
    #[error("tree subscription could not catch a stable snapshot")]
    Busy,
}

type Feed<Q, V> = (Subscription<Q>, Project<Q, V>);
type Pending<Q> = (usize, Delta<<Q as Application>::Conclusion>);

/// A standing projection which emits ordered tree edits.
pub struct TreeSubscription<Q: Application, V> {
    feeds: Vec<Feed<Q, V>>,
    pending: Vec<Pending<Q>>,
    projection: TreeProjection<V>,
}

impl Branch {
    /// Subscribe to a projection built from queries over this branch.
    pub fn subscribe_tree<Q: Application, V>(
        &self,
        sources: impl IntoIterator<Item = TreeSource<Q, V>>,
    ) -> TreeSubscription<Q, V> {
        TreeSubscription::new(sources, |query| self.subscribe(query))
    }
}

impl Stack {
    /// Subscribe to a projection built from queries over this stack.
    pub fn tree_subscription<Q: Application, V>(
        &self,
        sources: impl IntoIterator<Item = TreeSource<Q, V>>,
    ) -> TreeSubscription<Q, V> {
        TreeSubscription::new(sources, |query| self.subscription(query))
    }
}

impl<Q: Application, V> TreeSubscription<Q, V> {
    fn new(
        sources: impl IntoIterator<Item = TreeSource<Q, V>>,
        subscribe: impl Fn(Q) -> Subscription<Q>,
    ) -> Self {
        Self {
            feeds: sources
                .into_iter()
                .map(|source| (subscribe(source.query), source.project))
                .collect(),
            pending: Vec::new(),
            projection: TreeProjection::default(),
        }
    }
}

impl<Q, V> TreeSubscription<Q, V>
where
    Q: Application + ConditionalSync,
    Q::Conclusion: Conclusion + PartialEq + Clone + ConditionalSync,
    V: Clone + Eq,
{
    /// Whole evaluations performed by the source subscriptions, including
    /// their initial reads and any fallbacks from incremental maintenance.
    pub fn recomputes(&self) -> usize {
        self.feeds.iter().map(|(query, _)| query.recomputes()).sum()
    }

    /// The first poll seeds an empty receiver. Later polls emit only
    /// edits since the last successful poll. The queries must all read
    /// one stable stack stamp before any edits are published.
    pub async fn poll<Env: OpenStackEnv>(
        &mut self,
        env: &Env,
    ) -> Result<Vec<TreeEdit<V>>, TreeSubscriptionError> {
        const ROUNDS: usize = 8;
        for _ in 0..ROUNDS {
            let Some((first, _)) = self.feeds.first() else {
                return Ok(Vec::new());
            };
            let stamp = first.stamp();
            for (feed, (subscription, _)) in self.feeds.iter_mut().enumerate() {
                if let Some(delta) = subscription.poll(env).await?
                    && !delta.is_empty()
                {
                    self.pending.push((feed, delta));
                }
            }
            if self.feeds[0].0.stamp() == stamp {
                let mut rows = Vec::new();
                let mut changes = Vec::new();
                for (feed, delta) in &self.pending {
                    let project = &self.feeds[*feed].1;
                    for (row, diff) in delta
                        .retracted
                        .iter()
                        .map(|row| (row, -1))
                        .chain(delta.asserted.iter().map(|row| (row, 1)))
                    {
                        project(row, &mut rows)?;
                        changes.extend(rows.drain(..).map(|node| (node, diff)));
                    }
                }
                let edits = self.projection.apply_changes(changes)?;
                self.pending.clear();
                return Ok(edits);
            }
        }
        Err(TreeSubscriptionError::Busy)
    }
}

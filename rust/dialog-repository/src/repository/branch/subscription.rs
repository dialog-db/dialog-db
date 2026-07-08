//! Standing query subscriptions, incrementally gated by demand.
//!
//! A [`Subscription`] registers a query once and is then *polled*:
//! each poll compares the branch's current revision against the
//! revision the subscription last evaluated at. Re-evaluation is
//! gated by the subscription's **demand cover** — the set of index
//! key ranges the previous evaluation read, recorded at the `Select`
//! boundary — intersected with the tree diff between the pinned root
//! and the new one:
//!
//! - no root change → nothing to do;
//! - root changed but no diff entry falls inside the cover → the
//!   result cannot have changed; the pin advances without
//!   re-evaluating (this is the point: unrelated writes are free);
//! - a diff entry falls inside the cover → re-evaluate, emit the
//!   result [`Delta`], re-record the cover, advance the pin.
//!
//! The cover is the *demanded* range, not the touched data: a scan
//! that came back empty still recorded its range, so absence reads
//! (a fact that wasn't there yet, a rule that wasn't installed yet)
//! are invalidated by later writes into the demanded range. Rule
//! discovery reads (`db.rule/*` scans) record the same way, so
//! committing a new rule for a subscribed concept re-triggers too.
//!
//! Deliberately pull-driven: nothing here retains operator state or
//! integrated inputs. Demand transformation over the existing
//! top-down engine is the architecture; the diff is the signal, the
//! cover is the gate, and re-evaluation is the (current) recompute
//! step. Per-delta DRed/FBF maintenance and dynamically maintained
//! demand (cones that grow with data) build on this same surface.

use std::ops::RangeInclusive;
use std::sync::{Arc, Mutex};

use dialog_artifacts::selector::Constrained;
use dialog_artifacts::tree::{TreeStorageBridge, selector_range};
use dialog_artifacts::{ArtifactSelector, KeyBytes};
use dialog_capability::{Fork, Provider};
use dialog_common::Blake3Hash as NodeHash;
use dialog_common::ConditionalSync;
use dialog_effects::archive::prelude::ArchiveSubjectExt as _;
use dialog_effects::archive::{Get, Put};
use dialog_effects::authority::Identify;
use dialog_effects::memory::Resolve;
use dialog_query::error::EvaluationError;
use dialog_query::query::{Application, Output as _};
use dialog_search_tree::ContentAddressedStorage;
use dialog_storage::Blake3Hash;
use futures_util::TryStreamExt as _;

use super::session::{QueryEnv, QueryLayer};
use crate::layer::tombstones_from;
use crate::{
    Branch, EMPTY_TREE_HASH, Index, NetworkedIndex, RemoteSite, RepositoryArchiveExt as _, Revision,
};

/// The demand cover of one evaluation: every index key range the
/// evaluation's selects read, recorded at the `Select` boundary.
/// Shared (cheaply clonable) so the query environment can append
/// while the evaluation streams.
#[derive(Clone, Debug, Default)]
pub struct Demand {
    ranges: Arc<Mutex<Vec<RangeInclusive<KeyBytes>>>>,
}

impl Demand {
    /// An empty cover.
    pub fn new() -> Self {
        Self::default()
    }

    /// Record a selector's demanded range. The range covers
    /// everything the selector's scan would touch — including where
    /// no entries exist, so misses are demanded too.
    pub(crate) fn record(&self, selector: &ArtifactSelector<Constrained>) {
        let range = selector_range(selector);
        let mut ranges = self.ranges.lock().expect("demand lock");
        if !ranges.contains(&range) {
            ranges.push(range);
        }
    }

    /// Whether the key falls inside any recorded range.
    pub fn covers(&self, key: &KeyBytes) -> bool {
        self.ranges
            .lock()
            .expect("demand lock")
            .iter()
            .any(|range| range.contains(key))
    }

    /// A snapshot of the recorded ranges: the scope a cover-gated
    /// tree diff walks.
    pub(crate) fn ranges(&self) -> Vec<RangeInclusive<KeyBytes>> {
        self.ranges.lock().expect("demand lock").clone()
    }

    /// Number of distinct recorded ranges.
    pub fn len(&self) -> usize {
        self.ranges.lock().expect("demand lock").len()
    }

    /// Whether nothing was demanded.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// The change to a subscription's result set between two polls.
#[derive(Clone, Debug, PartialEq)]
pub struct Delta<T> {
    /// Rows in the new result that were not in the previous one.
    pub asserted: Vec<T>,
    /// Rows of the previous result that are gone from the new one.
    pub retracted: Vec<T>,
}

impl<T> Delta<T> {
    /// Whether the result set did not change.
    pub fn is_empty(&self) -> bool {
        self.asserted.is_empty() && self.retracted.is_empty()
    }
}

/// A standing query over a branch. Created by
/// [`Branch::subscribe`]; driven by [`poll`](Subscription::poll).
pub struct Subscription<Q: Application> {
    branch: Branch,
    query: Q,
    /// The revision the retained results were evaluated at. `None`
    /// until the first poll.
    revision: Option<Revision>,
    /// The demand cover recorded during the last evaluation.
    demand: Demand,
    /// The last evaluation's full result, retained to compute the
    /// next delta.
    results: Vec<Q::Conclusion>,
    initialized: bool,
}

impl Branch {
    /// Register a standing query over this branch. The subscription
    /// evaluates on its first [`poll`](Subscription::poll) and is
    /// incrementally gated afterwards.
    pub fn subscribe<Q: Application>(&self, query: Q) -> Subscription<Q> {
        Subscription {
            branch: self.clone(),
            query,
            revision: None,
            demand: Demand::new(),
            results: Vec::new(),
            initialized: false,
        }
    }
}

/// The index root a revision pins, or the empty tree for an
/// unborn branch.
fn tree_hash(revision: &Option<Revision>) -> Blake3Hash {
    revision
        .as_ref()
        .map(|revision| *revision.tree.hash())
        .unwrap_or(EMPTY_TREE_HASH)
}

impl<Q> Subscription<Q>
where
    Q: Application + Clone,
    Q::Conclusion: PartialEq + Clone,
{
    /// The retained result of the last evaluation.
    pub fn results(&self) -> &[Q::Conclusion] {
        &self.results
    }

    /// The demand cover recorded by the last evaluation.
    pub fn demand(&self) -> &Demand {
        &self.demand
    }

    /// Poll the subscription against the branch's current state.
    ///
    /// Returns `Ok(None)` when the result is known unchanged: the
    /// branch is at the pinned revision, or it moved but no change
    /// intersects the demand cover (the pin advances silently).
    /// Returns `Ok(Some(delta))` after a (re-)evaluation — the first
    /// poll always evaluates, reporting the initial result as
    /// `asserted` rows.
    ///
    /// The revision is snapshotted before evaluating; a commit that
    /// lands mid-evaluation re-triggers on the next poll (the diff
    /// from the pinned root is a superset), so changes are never
    /// missed, at worst re-checked.
    pub async fn poll<Env>(
        &mut self,
        env: &Env,
    ) -> Result<Option<Delta<Q::Conclusion>>, EvaluationError>
    where
        Env: Provider<Get>
            + Provider<Put>
            + Provider<Resolve>
            + Provider<Identify>
            + Provider<Fork<RemoteSite, Get>>
            + Provider<Fork<RemoteSite, Resolve>>
            + ConditionalSync
            + 'static,
    {
        let current = self.branch.revision();
        if self.initialized {
            if current == self.revision {
                return Ok(None);
            }
            if !self.intersects(env, &current).await? {
                self.revision = current;
                return Ok(None);
            }
        }

        let demand = Demand::new();
        let results = self.evaluate(env, &demand).await?;

        let delta = Delta {
            asserted: results
                .iter()
                .filter(|row| !self.results.contains(row))
                .cloned()
                .collect(),
            retracted: self
                .results
                .iter()
                .filter(|row| !results.contains(row))
                .cloned()
                .collect(),
        };

        self.results = results;
        self.demand = demand;
        self.revision = current;
        self.initialized = true;
        Ok(Some(delta))
    }

    /// Whether any change between the pinned root and `current`
    /// falls inside the demand cover.
    ///
    /// The diff itself is *scoped to the cover*
    /// ([`differentiate_within`]): subtrees whose key span misses
    /// every demanded range are dropped from the comparison without
    /// being loaded, so on a partial replica the poll never fetches
    /// subtrees the subscription didn't demand — the walk is bounded
    /// by the changes *within the cover*, not the full delta between
    /// the roots. The first in-scope change decides.
    ///
    /// [`differentiate_within`]: dialog_search_tree::PersistentTree::differentiate_within
    async fn intersects<Env>(
        &self,
        env: &Env,
        current: &Option<Revision>,
    ) -> Result<bool, EvaluationError>
    where
        Env: Provider<Get>
            + Provider<Put>
            + Provider<Resolve>
            + Provider<Fork<RemoteSite, Get>>
            + Provider<Fork<RemoteSite, Resolve>>
            + ConditionalSync
            + 'static,
    {
        let pinned = tree_hash(&self.revision);
        let target = tree_hash(current);
        if pinned == target {
            return Ok(false);
        }
        let scope = self.demand.ranges();
        if scope.is_empty() {
            return Ok(false);
        }

        let store = NetworkedIndex::new(env, self.branch.subject().archive().index(), None);
        let storage = ContentAddressedStorage::new(TreeStorageBridge(store));
        let previous =
            Index::from_hash_with_cache(NodeHash::from(pinned), self.branch.node_cache());
        let next = Index::from_hash_with_cache(NodeHash::from(target), self.branch.node_cache());

        let changes = previous.differentiate_within(&next, &scope, &storage, &storage);
        let mut changes = Box::pin(changes);
        Ok(changes
            .try_next()
            .await
            .map_err(|error| EvaluationError::Store(format!("subscription diff: {error}")))?
            .is_some())
    }

    /// Evaluate the query against the branch, recording every
    /// demanded range into `demand`. Mirrors the ordinary
    /// `branch.select(query).perform(env)` path with a
    /// demand-recording environment.
    async fn evaluate<Env>(
        &self,
        env: &Env,
        demand: &Demand,
    ) -> Result<Vec<Q::Conclusion>, EvaluationError>
    where
        Env: Provider<Get>
            + Provider<Put>
            + Provider<Resolve>
            + Provider<Identify>
            + Provider<Fork<RemoteSite, Get>>
            + Provider<Fork<RemoteSite, Resolve>>
            + ConditionalSync
            + 'static,
    {
        let operator = Identify
            .perform(env)
            .await
            .map_err(|error| EvaluationError::Store(format!("identify: {error}")))?;
        let layer = QueryLayer::from(&self.branch);
        let overlay = layer.overlay(&operator);
        let tombstones = tombstones_from(&overlay);
        let query_env = QueryEnv::new(layer.branches().to_vec(), overlay, tombstones, env)
            .with_demand(demand.clone());
        self.query.clone().perform(&query_env).try_vec().await
    }
}

#[cfg(test)]
mod tests {
    #[cfg(target_arch = "wasm32")]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    use crate::helpers::{test_operator_with_profile, test_repo};
    use dialog_artifacts::{Entity, Value};
    use dialog_query::attribute::The;
    use dialog_query::{AttributeQuery, Claim, Term, the};

    /// A standing query over every `person/name` fact.
    fn names_query() -> AttributeQuery {
        AttributeQuery::from(
            Term::<The>::from(the!("person/name"))
                .of(Term::<Entity>::var("e"))
                .is(Term::<String>::var("v")),
        )
    }

    /// Project claims to comparable `(entity, name)` pairs; the
    /// `cause` provenance hash is commit-dependent and irrelevant
    /// to what the subscription observed.
    fn names(claims: &[Claim]) -> Vec<(Entity, String)> {
        claims
            .iter()
            .map(|claim| {
                let Value::String(name) = claim.is.clone() else {
                    panic!("expected a string value, got {:?}", claim.is)
                };
                (claim.of.clone(), name)
            })
            .collect()
    }

    /// The first poll evaluates and reports the initial result;
    /// polling again without a commit is a no-op.
    #[dialog_common::test]
    async fn it_evaluates_on_first_poll_and_idles_after() -> anyhow::Result<()> {
        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;

        let alice = Entity::new()?;
        branch
            .transaction()
            .assert(
                the!("person/name")
                    .of(alice.clone())
                    .is("Alice".to_string()),
            )
            .commit()
            .perform(&operator)
            .await?;

        let mut subscription = branch.subscribe(names_query());
        let delta = subscription
            .poll(&operator)
            .await?
            .expect("first poll evaluates");
        assert_eq!(
            names(&delta.asserted),
            vec![(alice.clone(), "Alice".to_string())]
        );
        assert!(delta.retracted.is_empty());
        assert!(
            !subscription.demand().is_empty(),
            "the evaluation recorded its demand cover"
        );

        assert!(
            subscription.poll(&operator).await?.is_none(),
            "no commit, no work"
        );
        Ok(())
    }

    /// A commit outside the demand cover advances the pin without
    /// re-evaluating: unrelated writes are free.
    #[dialog_common::test]
    async fn it_ignores_writes_outside_the_demand_cover() -> anyhow::Result<()> {
        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;

        let alice = Entity::new()?;
        branch
            .transaction()
            .assert(
                the!("person/name")
                    .of(alice.clone())
                    .is("Alice".to_string()),
            )
            .commit()
            .perform(&operator)
            .await?;

        let mut subscription = branch.subscribe(names_query());
        subscription.poll(&operator).await?.expect("initial");

        // An unrelated attribute: outside the person/name cover.
        branch
            .transaction()
            .assert(
                the!("misc/tag")
                    .of(Entity::new()?)
                    .is("unrelated".to_string()),
            )
            .commit()
            .perform(&operator)
            .await?;

        assert!(
            subscription.poll(&operator).await?.is_none(),
            "a write outside the cover must not re-evaluate"
        );
        assert_eq!(
            names(subscription.results()),
            vec![(alice.clone(), "Alice".to_string())],
            "results retained across the gated poll"
        );

        // The pin advanced: polling again is a revision-equality
        // no-op, not another diff.
        assert!(subscription.poll(&operator).await?.is_none());
        Ok(())
    }

    /// A commit inside the cover re-evaluates and emits the delta:
    /// asserted rows on assert, retracted rows on retract.
    #[dialog_common::test]
    async fn it_emits_deltas_for_writes_inside_the_cover() -> anyhow::Result<()> {
        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;

        let alice = Entity::new()?;
        branch
            .transaction()
            .assert(
                the!("person/name")
                    .of(alice.clone())
                    .is("Alice".to_string()),
            )
            .commit()
            .perform(&operator)
            .await?;

        let mut subscription = branch.subscribe(names_query());
        subscription.poll(&operator).await?.expect("initial");

        let bob = Entity::new()?;
        branch
            .transaction()
            .assert(the!("person/name").of(bob.clone()).is("Bob".to_string()))
            .commit()
            .perform(&operator)
            .await?;

        let delta = subscription
            .poll(&operator)
            .await?
            .expect("covered write re-evaluates");
        assert_eq!(
            names(&delta.asserted),
            vec![(bob.clone(), "Bob".to_string())]
        );
        assert!(delta.retracted.is_empty());

        branch
            .transaction()
            .retract(
                the!("person/name")
                    .of(alice.clone())
                    .is("Alice".to_string()),
            )
            .commit()
            .perform(&operator)
            .await?;

        let delta = subscription
            .poll(&operator)
            .await?
            .expect("retraction re-evaluates");
        assert!(delta.asserted.is_empty());
        assert_eq!(
            names(&delta.retracted),
            vec![(alice.clone(), "Alice".to_string())]
        );
        assert_eq!(
            names(subscription.results()),
            vec![(bob.clone(), "Bob".to_string())]
        );
        Ok(())
    }

    /// The cover is the *demanded* range, not the touched data: a
    /// subscription whose query currently matches nothing still
    /// re-triggers when a fact lands in the demanded range.
    #[dialog_common::test]
    async fn it_invalidates_absence_reads() -> anyhow::Result<()> {
        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;

        // Something unrelated so the branch has a first commit.
        branch
            .transaction()
            .assert(the!("misc/tag").of(Entity::new()?).is("seed".to_string()))
            .commit()
            .perform(&operator)
            .await?;

        let mut subscription = branch.subscribe(names_query());
        let delta = subscription.poll(&operator).await?.expect("initial");
        assert!(delta.is_empty(), "nothing matches yet");
        assert!(
            !subscription.demand().is_empty(),
            "the miss was still demanded"
        );

        let alice = Entity::new()?;
        branch
            .transaction()
            .assert(
                the!("person/name")
                    .of(alice.clone())
                    .is("Alice".to_string()),
            )
            .commit()
            .perform(&operator)
            .await?;

        let delta = subscription
            .poll(&operator)
            .await?
            .expect("a write into the demanded (empty) range re-triggers");
        assert_eq!(
            names(&delta.asserted),
            vec![(alice.clone(), "Alice".to_string())]
        );
        Ok(())
    }
}

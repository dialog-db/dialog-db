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
//! - a diff entry falls inside a *fact* range → maintain
//!   incrementally when the query supports it (see below), emit the
//!   result [`Delta`], advance the pin;
//! - a diff entry falls inside a *rule-discovery* range → the rule
//!   set may have changed, which can affect any row: full
//!   re-evaluation, cover re-recorded.
//!
//! The cover is the *demanded* range, not the touched data: a scan
//! that came back empty still recorded its range, so absence reads
//! (a fact that wasn't there yet, a rule that wasn't installed yet)
//! are invalidated by later writes into the demanded range.
//!
//! # Incremental maintenance (DRed / FBF)
//!
//! For fact changes, the delta is derived without re-evaluating the
//! whole query: the changed datums name their subject entities, and
//! for each touched entity the retained rows are over-deleted and
//! re-derived by evaluating the query *restricted to that entity*
//! ([`Application::restrict`]) — DRed's delete / re-derive / insert,
//! with the goal-directed re-evaluation playing both the re-derive
//! and insert steps. A row with surviving alternate derivations is
//! simply re-derived, which handles the multi-derivation retraction
//! case (FBF's concern) exactly, without counting.
//!
//! For attribute queries the affected entities are the changed
//! facts' subjects. For concept queries they are discovered against
//! the resolved rule set
//! ([`affected_entities`]): entity-local rules
//! ([`AnalyzedRule::is_entity_local`]) contribute the changed
//! subjects, and non-local rules — concept premises reading *other*
//! entities' facts (a conformance check, a variant's negation) —
//! contribute delta-join heads: the changed fact bound into the
//! premise it matches, the remaining premises joined sideways, the
//! head variable projected. Recursion and shapes the discovery does
//! not handle fall back to a full recompute, which is always sound.
//! [`recomputes`](Subscription::recomputes) and
//! [`maintenances`](Subscription::maintenances) expose which path
//! each poll took.
//!
//! Deliberately pull-driven: nothing here retains operator state or
//! integrated inputs. Demand transformation over the existing
//! top-down engine is the architecture; the diff is the signal, the
//! cover is the gate, and per-entity re-derivation is the
//! maintenance step. Dynamically maintained demand (cones that grow
//! with data) builds on this same surface.
//!
//! [`AnalyzedRule::is_entity_local`]: dialog_query::rule::analyzer::AnalyzedRule::is_entity_local

use std::collections::BTreeSet;
use std::ops::RangeInclusive;
use std::sync::{Arc, Mutex};

use dialog_artifacts::selector::Constrained;
use dialog_artifacts::tree::{TreeStorageBridge, selector_range};
use dialog_artifacts::{Artifact, ArtifactSelector, Entity, KeyBytes, State};
use dialog_capability::{Fork, Provider};
use dialog_common::Blake3Hash as NodeHash;
use dialog_common::ConditionalSync;
use dialog_effects::archive::prelude::ArchiveSubjectExt as _;
use dialog_effects::archive::{Get, Put};
use dialog_effects::authority::Identify;
use dialog_effects::memory::Resolve;
use dialog_query::Conclusion;
use dialog_query::concept::query::affected::affected_entities;
use dialog_query::concept::query::fixpoint::{Continuation, InMemoryAnswerTable};
use dialog_query::error::EvaluationError;
use dialog_query::query::{Application, Output as _, Restriction};
use dialog_query::source::SelectRules;
use dialog_search_tree::{Change, ContentAddressedStorage};
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
    /// Ranges read by fact scans: the query's data demand.
    facts: Arc<Mutex<Vec<RangeInclusive<KeyBytes>>>>,
    /// Ranges read by rule-discovery scans (`db.rule/*`). Kept
    /// apart because a change here can install a rule, which can
    /// affect any row — it invalidates the whole result, not one
    /// entity's slice.
    rules: Arc<Mutex<Vec<RangeInclusive<KeyBytes>>>>,
}

fn record_range(ranges: &Mutex<Vec<RangeInclusive<KeyBytes>>>, range: RangeInclusive<KeyBytes>) {
    let mut ranges = ranges.lock().expect("demand lock");
    if !ranges.contains(&range) {
        ranges.push(range);
    }
}

impl Demand {
    /// An empty cover.
    pub fn new() -> Self {
        Self::default()
    }

    /// Record a fact scan's demanded range. The range covers
    /// everything the selector's scan would touch — including where
    /// no entries exist, so misses are demanded too.
    pub(crate) fn record(&self, selector: &ArtifactSelector<Constrained>) {
        record_range(&self.facts, selector_range(selector));
    }

    /// Record a rule-discovery scan's demanded range.
    pub(crate) fn record_rules(&self, selector: &ArtifactSelector<Constrained>) {
        record_range(&self.rules, selector_range(selector));
    }

    /// Whether the key falls inside any recorded range.
    pub fn covers(&self, key: &KeyBytes) -> bool {
        self.covers_facts(key) || self.covers_rules(key)
    }

    fn covers_facts(&self, key: &KeyBytes) -> bool {
        self.facts
            .lock()
            .expect("demand lock")
            .iter()
            .any(|range| range.contains(key))
    }

    fn covers_rules(&self, key: &KeyBytes) -> bool {
        self.rules
            .lock()
            .expect("demand lock")
            .iter()
            .any(|range| range.contains(key))
    }

    /// A snapshot of every recorded range (facts and rules): the
    /// scope a cover-gated tree diff walks.
    pub(crate) fn ranges(&self) -> Vec<RangeInclusive<KeyBytes>> {
        let mut ranges = self.facts.lock().expect("demand lock").clone();
        ranges.extend(self.rules.lock().expect("demand lock").iter().cloned());
        ranges
    }

    /// Number of distinct recorded ranges.
    pub fn len(&self) -> usize {
        self.facts.lock().expect("demand lock").len()
            + self.rules.lock().expect("demand lock").len()
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
    /// The retained fixpoint answer table when the subscribed
    /// concept is recursive: additions extend it semi-naively;
    /// recomputes rebuild into it.
    fixpoint: Arc<Mutex<Option<InMemoryAnswerTable>>>,
    initialized: bool,
    /// Full evaluations performed (first poll + fallbacks).
    recomputes: usize,
    /// Polls maintained incrementally (per-entity re-derivation).
    maintenances: usize,
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
            fixpoint: Arc::new(Mutex::new(None)),
            initialized: false,
            recomputes: 0,
            maintenances: 0,
        }
    }
}

/// What the in-cover changes between two roots touched.
enum Touched {
    /// Nothing inside the cover changed (or the changes cannot
    /// alter any read: tombstones over never-asserted keys).
    Nothing,
    /// A rule-discovery range changed: the rule set may differ, so
    /// any row may be affected.
    Rules,
    /// Only fact ranges changed: the changed facts (deduplicated
    /// across the three index orders) and their subject entities.
    Facts {
        /// Subjects of the changed facts.
        subjects: BTreeSet<Entity>,
        /// The changed facts themselves, for delta-join discovery.
        facts: Vec<Artifact>,
        /// Whether every change was an addition (no fact left the
        /// index and no tombstone was written): the precondition
        /// for semi-naive fixpoint continuation.
        additions_only: bool,
    },
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
    Q::Conclusion: Conclusion + PartialEq + Clone,
{
    /// The retained result of the last evaluation.
    pub fn results(&self) -> &[Q::Conclusion] {
        &self.results
    }

    /// The demand cover recorded by the last evaluation.
    pub fn demand(&self) -> &Demand {
        &self.demand
    }

    /// Full evaluations performed so far (the first poll plus every
    /// fallback from incremental maintenance).
    pub fn recomputes(&self) -> usize {
        self.recomputes
    }

    /// Polls that were maintained incrementally: the delta was
    /// derived by re-evaluating only the touched entities (DRed's
    /// delete/re-derive, goal-directed per subject) instead of the
    /// whole query.
    pub fn maintenances(&self) -> usize {
        self.maintenances
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
            match self.touched(env, &current).await? {
                Touched::Nothing => {
                    self.revision = current;
                    return Ok(None);
                }
                Touched::Facts {
                    subjects,
                    facts,
                    additions_only,
                } => {
                    if let Some(delta) = self
                        .maintain(env, &subjects, &facts, additions_only)
                        .await?
                    {
                        self.maintenances += 1;
                        self.revision = current;
                        return Ok(Some(delta));
                    }
                    // Not maintainable for this query/rule shape:
                    // fall through to a full recompute.
                }
                // A rule-range change can install or change a rule,
                // which can affect any row: recompute.
                Touched::Rules => {}
            }
        }

        let demand = Demand::new();
        let results = self.evaluate(env, &demand, &self.query).await?;
        self.recomputes += 1;

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

    /// Classify what the changes between the pinned root and
    /// `current` touched within the demand cover.
    ///
    /// The diff is *scoped to the cover*
    /// ([`differentiate_within`]): subtrees whose key span misses
    /// every demanded range are dropped from the comparison without
    /// being loaded, so on a partial replica the poll never fetches
    /// subtrees the subscription didn't demand — the walk is bounded
    /// by the changes *within the cover*, not the full delta between
    /// the roots.
    ///
    /// A change inside a rule-discovery range short-circuits to
    /// [`Touched::Rules`]. Fact changes collect the subject entities
    /// of the changed datums; changes with no datum on either side
    /// (a tombstone written where nothing was asserted, or a
    /// tombstone entry disappearing) never alter what a scan reads
    /// and are skipped.
    ///
    /// [`differentiate_within`]: dialog_search_tree::PersistentTree::differentiate_within
    async fn touched<Env>(
        &self,
        env: &Env,
        current: &Option<Revision>,
    ) -> Result<Touched, EvaluationError>
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
            return Ok(Touched::Nothing);
        }
        let scope = self.demand.ranges();
        if scope.is_empty() {
            return Ok(Touched::Nothing);
        }

        let store = NetworkedIndex::new(env, self.branch.subject().archive().index(), None);
        let storage = ContentAddressedStorage::new(TreeStorageBridge(store));
        let previous =
            Index::from_hash_with_cache(NodeHash::from(pinned), self.branch.node_cache());
        let next = Index::from_hash_with_cache(NodeHash::from(target), self.branch.node_cache());

        let changes = previous.differentiate_within(&next, &scope, &storage, &storage);
        let mut changes = Box::pin(changes);
        let mut subjects = BTreeSet::new();
        let mut facts = Vec::new();
        let mut additions_only = true;
        // A fact change surfaces once per index order; dedup on the
        // datum triple.
        let mut seen = BTreeSet::new();
        while let Some(change) = changes
            .try_next()
            .await
            .map_err(|error| EvaluationError::Store(format!("subscription diff: {error}")))?
        {
            let entry = match &change {
                Change::Add(entry) => entry,
                Change::Remove(entry) => entry,
            };
            if self.demand.covers_rules(&entry.key) {
                return Ok(Touched::Rules);
            }
            // An entry leaving the index, or a tombstone arriving,
            // means a fact stopped being readable: not an addition.
            match (&change, &entry.value) {
                (Change::Remove(_), State::Added(_)) => additions_only = false,
                (Change::Add(_), State::Removed) => additions_only = false,
                _ => {}
            }
            if let State::Added(datum) = &entry.value {
                if !seen.insert((
                    datum.entity.clone(),
                    datum.attribute.clone(),
                    datum.value.clone(),
                )) {
                    continue;
                }
                let fact = Artifact::try_from(datum.clone())
                    .map_err(|error| EvaluationError::Store(format!("changed datum: {error:?}")))?;
                subjects.insert(fact.of.clone());
                facts.push(fact);
            }
        }

        if subjects.is_empty() {
            Ok(Touched::Nothing)
        } else {
            Ok(Touched::Facts {
                subjects,
                facts,
                additions_only,
            })
        }
    }

    /// Maintain the retained result incrementally: for each touched
    /// entity, over-delete its retained rows and re-derive them with
    /// the query restricted to that entity (DRed's delete /
    /// re-derive / insert, with the goal-directed re-evaluation
    /// playing both the re-derive and insert steps — a row with
    /// surviving alternate derivations is simply re-derived, which
    /// is what makes multi-derivation retractions exact without
    /// counting).
    ///
    /// Returns `Ok(None)` when the affected set cannot be bounded —
    /// the query is not restrictable, the concept is recursive, or
    /// the delta-join discovery hit a shape it does not handle — in
    /// which case the caller falls back to a full recompute.
    async fn maintain<Env>(
        &mut self,
        env: &Env,
        subjects: &BTreeSet<Entity>,
        facts: &[Artifact],
        additions_only: bool,
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
        // For a concept query the affected heads are discovered
        // against the resolved rule set: entity-local rules
        // contribute the changed subjects, non-local rules (concept
        // premises: conformance checks, variant negations)
        // contribute delta-join heads — the changed fact bound into
        // the premise it matches, remaining premises joined
        // sideways, head projected. `None` (recursion, unhandled
        // shape) falls back to full recompute. For a plain attribute
        // query the affected heads are just the changed subjects.
        //
        // The discovery evaluates against the demand-recording
        // environment, so the reads it depends on join the cover.
        let entities: BTreeSet<Entity> = if let Some(concept) = self.query.concept() {
            let operator = Identify
                .perform(env)
                .await
                .map_err(|error| EvaluationError::Store(format!("identify: {error}")))?;
            let layer = QueryLayer::from(&self.branch);
            let overlay = layer.overlay(&operator);
            let tombstones = tombstones_from(&overlay);
            let query_env = QueryEnv::new(layer.branches().to_vec(), overlay, tombstones, env)
                .with_demand(self.demand.clone());
            let rules = Provider::<SelectRules>::execute(&query_env, concept.clone()).await?;
            if rules.recursion().is_some() {
                // Fixpoint continuation: additions extend the
                // retained answer table semi-naively (rebuilding
                // when the rule set is not additively extendable);
                // deletions fall back to a recompute, which rebuilds
                // the table.
                if !additions_only {
                    return Ok(None);
                }
                drop(query_env);
                let results = self
                    .evaluate_with_continuation(env, Some(Arc::new(facts.to_vec())))
                    .await?;
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
                return Ok(Some(delta));
            }
            match affected_entities(concept, facts, &query_env).await? {
                Some(entities) => entities,
                None => return Ok(None),
            }
        } else {
            subjects.clone()
        };

        let mut asserted = Vec::new();
        let mut retracted = Vec::new();
        for entity in &entities {
            let scoped = match self.query.restrict(entity) {
                Restriction::Scoped(query) => query,
                Restriction::Unaffected => continue,
                Restriction::Unsupported => return Ok(None),
            };
            // Over-delete: every retained row for this entity...
            let before: Vec<Q::Conclusion> = self
                .results
                .iter()
                .filter(|row| row.this() == entity)
                .cloned()
                .collect();
            // ...re-derive + insert: goal-directed re-evaluation,
            // recording into the existing cover (the standing
            // demand only ever grows between recomputes).
            let after = self.evaluate(env, &self.demand.clone(), &scoped).await?;
            for row in &after {
                if !before.contains(row) {
                    asserted.push(row.clone());
                }
            }
            for row in &before {
                if !after.contains(row) {
                    retracted.push(row.clone());
                }
            }
            self.results.retain(|row| row.this() != entity);
            self.results.extend(after);
        }
        Ok(Some(Delta {
            asserted,
            retracted,
        }))
    }

    /// Evaluate the query against the branch, recording every
    /// demanded range into `demand`. Mirrors the ordinary
    /// `branch.select(query).perform(env)` path with a
    /// demand-recording environment.
    async fn evaluate<Env>(
        &self,
        env: &Env,
        demand: &Demand,
        query: &Q,
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
        let mut query_env = QueryEnv::new(layer.branches().to_vec(), overlay, tombstones, env)
            .with_demand(demand.clone());
        // Recursive concept subscriptions retain their fixpoint
        // across polls: a recompute rebuilds into the retained
        // table so a later additions-only poll can extend it.
        if let Some(concept) = query.concept() {
            query_env = query_env.with_fixpoint(
                concept.this(),
                Continuation::new(self.fixpoint.clone(), None),
            );
        }
        query.clone().perform(&query_env).try_vec().await
    }

    /// Evaluate the standing query with the retained fixpoint
    /// attached, seeding a semi-naive continuation from `additions`
    /// when present. Demand keeps recording into the standing
    /// cover.
    async fn evaluate_with_continuation<Env>(
        &self,
        env: &Env,
        additions: Option<Arc<Vec<Artifact>>>,
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
        let concept = self
            .query
            .concept()
            .expect("continuation evaluation requires a concept query");
        let operator = Identify
            .perform(env)
            .await
            .map_err(|error| EvaluationError::Store(format!("identify: {error}")))?;
        let layer = QueryLayer::from(&self.branch);
        let overlay = layer.overlay(&operator);
        let tombstones = tombstones_from(&overlay);
        let query_env = QueryEnv::new(layer.branches().to_vec(), overlay, tombstones, env)
            .with_demand(self.demand.clone())
            .with_fixpoint(
                concept.this(),
                Continuation::new(self.fixpoint.clone(), additions),
            );
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
    use dialog_query::types::Any;
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

    /// Covered writes are maintained incrementally: after the first
    /// full evaluation, deltas come from per-entity re-derivation,
    /// never a whole-query recompute.
    #[dialog_common::test]
    async fn it_maintains_covered_writes_without_recompute() -> anyhow::Result<()> {
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
        assert_eq!(subscription.recomputes(), 1);
        assert_eq!(subscription.maintenances(), 0);

        let bob = Entity::new()?;
        branch
            .transaction()
            .assert(the!("person/name").of(bob.clone()).is("Bob".to_string()))
            .commit()
            .perform(&operator)
            .await?;

        let delta = subscription.poll(&operator).await?.expect("covered write");
        assert_eq!(
            names(&delta.asserted),
            vec![(bob.clone(), "Bob".to_string())]
        );
        assert_eq!(
            subscription.recomputes(),
            1,
            "the delta came from per-entity re-derivation, not a recompute"
        );
        assert_eq!(subscription.maintenances(), 1);

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

        let delta = subscription.poll(&operator).await?.expect("retraction");
        assert_eq!(
            names(&delta.retracted),
            vec![(alice.clone(), "Alice".to_string())]
        );
        assert!(delta.asserted.is_empty());
        assert_eq!(subscription.recomputes(), 1);
        assert_eq!(subscription.maintenances(), 2);
        assert_eq!(
            names(subscription.results()),
            vec![(bob, "Bob".to_string())],
            "retained results track the maintained state"
        );
        Ok(())
    }

    /// The multi-derivation retraction case (what FBF solves without
    /// counting): an entity carries two values for the same
    /// attribute; retracting one must retract exactly that row and
    /// keep the other, because the survivor re-derives during the
    /// per-entity re-evaluation.
    #[dialog_common::test]
    async fn it_rederives_surviving_rows_on_partial_retraction() -> anyhow::Result<()> {
        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;

        let alice = Entity::new()?;
        branch
            .transaction()
            .assert(the!("person/name").of(alice.clone()).is("Ali".to_string()))
            .assert(
                the!("person/name")
                    .of(alice.clone())
                    .is("Alice".to_string()),
            )
            .commit()
            .perform(&operator)
            .await?;

        let mut subscription = branch.subscribe(names_query());
        let initial = subscription.poll(&operator).await?.expect("initial");
        assert_eq!(initial.asserted.len(), 2, "both values surface");

        branch
            .transaction()
            .retract(the!("person/name").of(alice.clone()).is("Ali".to_string()))
            .commit()
            .perform(&operator)
            .await?;

        let delta = subscription.poll(&operator).await?.expect("retraction");
        assert_eq!(
            names(&delta.retracted),
            vec![(alice.clone(), "Ali".to_string())],
            "only the retracted value goes"
        );
        assert!(delta.asserted.is_empty());
        assert_eq!(
            names(subscription.results()),
            vec![(alice.clone(), "Alice".to_string())],
            "the surviving value re-derived"
        );
        assert_eq!(subscription.recomputes(), 1);
        assert_eq!(subscription.maintenances(), 1);
        Ok(())
    }

    /// A write touching only *other* entities' facts inside the
    /// cover maintains just those entities: the untouched entity's
    /// rows are never re-derived, and the delta is scoped to what
    /// changed.
    #[dialog_common::test]
    async fn it_scopes_maintenance_to_touched_entities() -> anyhow::Result<()> {
        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;

        let alice = Entity::new()?;
        let bob = Entity::new()?;
        branch
            .transaction()
            .assert(
                the!("person/name")
                    .of(alice.clone())
                    .is("Alice".to_string()),
            )
            .assert(the!("person/name").of(bob.clone()).is("Bob".to_string()))
            .commit()
            .perform(&operator)
            .await?;

        let mut subscription = branch.subscribe(names_query());
        subscription.poll(&operator).await?.expect("initial");

        branch
            .transaction()
            .assert(the!("person/name").of(bob.clone()).is("Bobby".to_string()))
            .commit()
            .perform(&operator)
            .await?;

        let delta = subscription.poll(&operator).await?.expect("covered write");
        assert_eq!(
            names(&delta.asserted),
            vec![(bob.clone(), "Bobby".to_string())]
        );
        assert!(
            delta.retracted.is_empty(),
            "cardinality-many: the prior value stays"
        );
        let mut retained = names(subscription.results());
        retained.sort();
        let mut expected = vec![
            (alice.clone(), "Alice".to_string()),
            (bob.clone(), "Bob".to_string()),
            (bob.clone(), "Bobby".to_string()),
        ];
        expected.sort();
        assert_eq!(retained, expected);
        assert_eq!(subscription.maintenances(), 1);
        Ok(())
    }

    mod concepts {
        //! Derived concepts + attributes shared by the incremental
        //! maintenance tests below.

        use dialog_artifacts::Entity;
        use dialog_query::{Attribute, Concept};

        /// A badge number (`credential/badge`).
        #[derive(Attribute, Clone, PartialEq)]
        #[domain("credential")]
        pub struct Badge(pub String);

        /// A report's display name (`report/name`).
        #[derive(Attribute, Clone, PartialEq)]
        #[domain("report")]
        pub struct Name(pub String);

        /// The report's manager (`report/manager`).
        #[derive(Attribute, Clone, PartialEq)]
        #[domain("report")]
        pub struct Manager(pub Entity);

        /// Someone holding a badge.
        #[derive(Concept, Debug, Clone, PartialEq)]
        pub struct BadgeHolder {
            /// The badge holder entity.
            pub this: Entity,
            /// Their badge number.
            pub badge: Badge,
        }

        /// Someone reporting to a badge holder.
        #[derive(Concept, Debug, Clone, PartialEq)]
        pub struct Report {
            /// The report entity.
            pub this: Entity,
            /// Their display name.
            pub name: Name,
            /// Their manager, who must hold a badge.
            #[dialog(conforms = BadgeHolder)]
            pub manager: Manager,
        }

        /// An email handle (`comm/email`).
        #[derive(Attribute, Clone, PartialEq)]
        #[domain("comm")]
        pub struct Email(pub String);

        /// A phone handle (`comm/phone`).
        #[derive(Attribute, Clone, PartialEq)]
        #[domain("comm")]
        pub struct Phone(pub String);

        /// A contact handle (`contact/handle`) — the variant
        /// conclusion.
        #[derive(Attribute, Clone, PartialEq)]
        #[domain("contact")]
        pub struct Handle(pub String);

        /// A user with an email address.
        #[derive(Concept, Debug, Clone, PartialEq)]
        pub struct WithEmail {
            /// The user entity.
            pub this: Entity,
            /// Their email handle.
            pub handle: Email,
        }

        /// A user with a phone number.
        #[derive(Concept, Debug, Clone, PartialEq)]
        pub struct WithPhone {
            /// The user entity.
            pub this: Entity,
            /// Their phone handle.
            pub handle: Phone,
        }

        /// The preferred way to reach a user: email if they have
        /// one, otherwise phone.
        #[derive(Concept, Debug, Clone, PartialEq)]
        pub struct Contact {
            /// The user entity.
            pub this: Entity,
            /// The winning handle.
            pub handle: Handle,
        }

        /// A parent edge (`family/parent`).
        #[derive(Attribute, Clone, PartialEq)]
        #[domain("family")]
        pub struct Parent(pub Entity);

        /// An ancestor edge (`family/ancestor`) — the recursive
        /// conclusion.
        #[derive(Attribute, Clone, PartialEq)]
        #[domain("family")]
        pub struct Ancestor(pub Entity);

        /// Direct parenthood.
        #[derive(Concept, Debug, Clone, PartialEq)]
        pub struct HasParent {
            /// The child entity.
            pub this: Entity,
            /// Their parent.
            pub parent: Parent,
        }

        /// The transitive closure of parenthood.
        #[derive(Concept, Debug, Clone, PartialEq)]
        pub struct HasAncestor {
            /// The descendant entity.
            pub this: Entity,
            /// One of their ancestors.
            pub ancestor: Ancestor,
        }
    }

    /// Stage the `db.rule/*` facts that persist a deductive rule
    /// durably (the storage shape from `crate::rules`).
    fn with_rule<'t>(
        transaction: crate::Transaction<'t>,
        rule: &dialog_query::DeductiveRule,
    ) -> crate::Transaction<'t> {
        let entity = rule.this();
        transaction
            .assert(
                the!("db.rule/conclusion")
                    .of(entity.clone())
                    .is(rule.conclusion().this()),
            )
            .assert(the!("db.rule/source").of(entity).is(rule.encode()))
    }

    /// A rule `conclusion :- Concept(target, terms)` built from
    /// derived concept descriptors, storable as a durable rule.
    fn concept_rule(
        conclusion: &dialog_query::ConceptDescriptor,
        premises: Vec<dialog_query::Premise>,
    ) -> dialog_query::DeductiveRule {
        dialog_query::DeductiveRule::new(conclusion.clone(), premises).expect("rule compiles")
    }

    fn concept_premise(
        target: &dialog_query::ConceptDescriptor,
        bindings: &[(&str, &str)],
    ) -> dialog_query::Premise {
        let mut terms = dialog_query::Parameters::new();
        for (param, variable) in bindings {
            terms.insert((*param).to_string(), Term::<Any>::var(*variable));
        }
        dialog_query::Premise::Assert(dialog_query::Proposition::Concept(
            dialog_query::ConceptQuery {
                terms,
                predicate: target.clone(),
            },
        ))
    }

    fn negated_concept_premise(
        target: &dialog_query::ConceptDescriptor,
        bindings: &[(&str, &str)],
    ) -> dialog_query::Premise {
        let mut terms = dialog_query::Parameters::new();
        for (param, variable) in bindings {
            terms.insert((*param).to_string(), Term::<Any>::var(*variable));
        }
        dialog_query::Premise::Unless(dialog_query::Negation(dialog_query::Proposition::Concept(
            dialog_query::ConceptQuery {
                terms,
                predicate: target.clone(),
            },
        )))
    }

    /// Piece 1: a derived `Query<C>` subscription over an
    /// entity-local concept is maintained incrementally.
    #[dialog_common::test]
    async fn it_maintains_derived_concept_subscriptions() -> anyhow::Result<()> {
        use concepts::{Badge, BadgeHolder};
        use dialog_query::Query;

        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;

        let alice = Entity::new()?;
        branch
            .transaction()
            .assert(Badge::of(alice.clone()).is("A-1"))
            .commit()
            .perform(&operator)
            .await?;

        let mut subscription = branch.subscribe(Query::<BadgeHolder>::default());
        let initial = subscription.poll(&operator).await?.expect("initial");
        assert_eq!(
            initial.asserted,
            vec![BadgeHolder {
                this: alice.clone(),
                badge: Badge("A-1".into()),
            }]
        );
        assert_eq!(subscription.recomputes(), 1);

        let bob = Entity::new()?;
        branch
            .transaction()
            .assert(Badge::of(bob.clone()).is("B-2"))
            .commit()
            .perform(&operator)
            .await?;

        let delta = subscription.poll(&operator).await?.expect("covered write");
        assert_eq!(
            delta.asserted,
            vec![BadgeHolder {
                this: bob.clone(),
                badge: Badge("B-2".into()),
            }]
        );
        assert_eq!(
            subscription.recomputes(),
            1,
            "the derived query restricted to the touched entity"
        );
        assert_eq!(subscription.maintenances(), 1);
        Ok(())
    }

    /// Piece 2, conformance: a badge change for a *manager* affects
    /// the *report* entity's rows (cross-entity), discovered by the
    /// delta-join and maintained without a recompute.
    #[dialog_common::test]
    async fn it_maintains_cross_entity_conformance() -> anyhow::Result<()> {
        use concepts::{Badge, Manager, Name, Report};
        use dialog_query::Query;

        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;

        let alice = Entity::new()?; // manager WITH a badge
        let carol = Entity::new()?; // manager WITHOUT a badge (yet)
        let bob = Entity::new()?; // reports to alice
        let mallory = Entity::new()?; // reports to carol

        branch
            .transaction()
            .assert(Badge::of(alice.clone()).is("A-1"))
            .assert(Name::of(bob.clone()).is("Bob"))
            .assert(Manager::of(bob.clone()).is(alice.clone()))
            .assert(Name::of(mallory.clone()).is("Mallory"))
            .assert(Manager::of(mallory.clone()).is(carol.clone()))
            .commit()
            .perform(&operator)
            .await?;

        let mut subscription = branch.subscribe(Query::<Report>::default());
        let initial = subscription.poll(&operator).await?.expect("initial");
        assert_eq!(
            initial.asserted,
            vec![Report {
                this: bob.clone(),
                name: Name("Bob".into()),
                manager: Manager(alice.clone()),
            }],
            "only the report whose manager holds a badge conforms"
        );

        // Carol gets a badge: mallory's row appears, though the
        // changed fact's subject is carol, not mallory.
        branch
            .transaction()
            .assert(Badge::of(carol.clone()).is("C-3"))
            .commit()
            .perform(&operator)
            .await?;

        let delta = subscription
            .poll(&operator)
            .await?
            .expect("cross-entity effect");
        assert_eq!(
            delta.asserted,
            vec![Report {
                this: mallory.clone(),
                name: Name("Mallory".into()),
                manager: Manager(carol.clone()),
            }]
        );
        assert!(delta.retracted.is_empty());
        assert_eq!(
            subscription.recomputes(),
            1,
            "the affected report was discovered by the delta-join, not a recompute"
        );
        assert_eq!(subscription.maintenances(), 1);

        // And the retraction flows back the same way.
        branch
            .transaction()
            .retract(Badge::of(carol.clone()).is("C-3"))
            .commit()
            .perform(&operator)
            .await?;

        let delta = subscription.poll(&operator).await?.expect("retraction");
        assert_eq!(
            delta.retracted,
            vec![Report {
                this: mallory.clone(),
                name: Name("Mallory".into()),
                manager: Manager(carol.clone()),
            }]
        );
        assert_eq!(subscription.recomputes(), 1);
        assert_eq!(subscription.maintenances(), 2);
        Ok(())
    }

    /// Piece 2, variants: committing a rule re-triggers via the
    /// rule-discovery range (full recompute — the rule set changed);
    /// afterwards a fact write that flips a negated variant is
    /// maintained incrementally through the delta-join.
    #[dialog_common::test]
    async fn it_maintains_variant_negation_flips() -> anyhow::Result<()> {
        use concepts::{Contact, Email, Handle, Phone, WithEmail, WithPhone};
        use dialog_query::Query;

        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;

        let contact = Contact::descriptor().clone();
        let email = WithEmail::descriptor().clone();
        let phone = WithPhone::descriptor().clone();

        let email_rule = concept_rule(
            &contact,
            vec![concept_premise(
                &email,
                &[("this", "this"), ("handle", "handle")],
            )],
        );
        let phone_rule = concept_rule(
            &contact,
            vec![
                concept_premise(&phone, &[("this", "this"), ("handle", "handle")]),
                negated_concept_premise(&email, &[("this", "this")]),
            ],
        );

        let bob = Entity::new()?;
        let transaction = branch
            .transaction()
            .assert(Phone::of(bob.clone()).is("555-0100"));
        with_rule(transaction, &email_rule)
            .commit()
            .perform(&operator)
            .await?;

        let mut subscription = branch.subscribe(Query::<Contact>::default());
        let initial = subscription.poll(&operator).await?.expect("initial");
        assert!(
            initial.asserted.is_empty(),
            "only the email rule is installed and bob has no email"
        );

        // Installing the phone rule lands in the rule-discovery
        // range: recompute.
        with_rule(branch.transaction(), &phone_rule)
            .commit()
            .perform(&operator)
            .await?;

        let delta = subscription.poll(&operator).await?.expect("rule installed");
        assert_eq!(
            delta.asserted,
            vec![Contact {
                this: bob.clone(),
                handle: Handle("555-0100".into()),
            }]
        );
        assert_eq!(subscription.recomputes(), 2, "a rule-set change recomputes");

        // An email for bob flips the negated variant: the phone row
        // retracts and the email row asserts — maintained, not
        // recomputed.
        branch
            .transaction()
            .assert(Email::of(bob.clone()).is("bob@mail"))
            .commit()
            .perform(&operator)
            .await?;

        let delta = subscription.poll(&operator).await?.expect("variant flip");
        assert_eq!(
            delta.asserted,
            vec![Contact {
                this: bob.clone(),
                handle: Handle("bob@mail".into()),
            }]
        );
        assert_eq!(
            delta.retracted,
            vec![Contact {
                this: bob.clone(),
                handle: Handle("555-0100".into()),
            }]
        );
        assert_eq!(subscription.recomputes(), 2, "the flip was maintained");
        assert_eq!(subscription.maintenances(), 1);
        Ok(())
    }

    /// Piece 3, dynamic demand: a subscription over a recursive
    /// concept keeps answering as its demand cone grows with the
    /// data — each edge extending the chain re-triggers and derives
    /// exactly the new closure pairs. (Recursive closures re-derive
    /// via the fixpoint; incremental fixpoint continuation is a
    /// recorded follow-up.)
    #[dialog_common::test]
    async fn it_grows_the_demand_cone_with_recursive_rules() -> anyhow::Result<()> {
        use concepts::{Ancestor, HasAncestor, HasParent, Parent};
        use dialog_query::Query;

        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;

        let ancestor = HasAncestor::descriptor().clone();
        let parent = HasParent::descriptor().clone();

        let base = concept_rule(
            &ancestor,
            vec![concept_premise(
                &parent,
                &[("this", "this"), ("parent", "ancestor")],
            )],
        );
        let step = concept_rule(
            &ancestor,
            vec![
                concept_premise(&parent, &[("this", "this"), ("parent", "p")]),
                concept_premise(&ancestor, &[("this", "p"), ("ancestor", "ancestor")]),
            ],
        );

        let a = Entity::new()?;
        let b = Entity::new()?;
        let c = Entity::new()?;
        let d = Entity::new()?;
        let transaction = branch
            .transaction()
            .assert(Parent::of(b.clone()).is(a.clone()));
        with_rule(with_rule(transaction, &base), &step)
            .commit()
            .perform(&operator)
            .await?;

        let mut subscription = branch.subscribe(Query::<HasAncestor>::default());
        let initial = subscription.poll(&operator).await?.expect("initial");
        assert_eq!(
            initial.asserted,
            vec![HasAncestor {
                this: b.clone(),
                ancestor: Ancestor(a.clone()),
            }]
        );

        // Extend the chain: the closure grows by two pairs.
        branch
            .transaction()
            .assert(Parent::of(c.clone()).is(b.clone()))
            .commit()
            .perform(&operator)
            .await?;
        assert_eq!(subscription.recomputes(), 1);
        let delta = subscription.poll(&operator).await?.expect("cone grows");
        assert_eq!(
            subscription.recomputes(),
            1,
            "the closure extended via fixpoint continuation, not a re-fixpoint"
        );
        assert_eq!(subscription.maintenances(), 1);
        let mut asserted = delta.asserted.clone();
        asserted.sort_by_key(|row| format!("{row:?}"));
        let mut expected = vec![
            HasAncestor {
                this: c.clone(),
                ancestor: Ancestor(b.clone()),
            },
            HasAncestor {
                this: c.clone(),
                ancestor: Ancestor(a.clone()),
            },
        ];
        expected.sort_by_key(|row| format!("{row:?}"));
        assert_eq!(asserted, expected);
        assert!(delta.retracted.is_empty());

        // And again: the frontier extended by the previous poll is
        // itself demanded, so the next extension re-triggers too.
        branch
            .transaction()
            .assert(Parent::of(d.clone()).is(c.clone()))
            .commit()
            .perform(&operator)
            .await?;
        let delta = subscription
            .poll(&operator)
            .await?
            .expect("cone grows again");
        assert_eq!(delta.asserted.len(), 3, "(d,c), (d,b), (d,a)");
        assert!(delta.retracted.is_empty());
        assert_eq!(subscription.recomputes(), 1);
        assert_eq!(subscription.maintenances(), 2);

        // A deletion shrinks the closure: not additively
        // extendable, so the poll rebuilds the fixpoint (and the
        // retained table), retracting everything derived through
        // the removed edge.
        branch
            .transaction()
            .retract(Parent::of(c.clone()).is(b.clone()))
            .commit()
            .perform(&operator)
            .await?;
        let delta = subscription.poll(&operator).await?.expect("cone shrinks");
        assert!(delta.asserted.is_empty());
        assert_eq!(
            delta.retracted.len(),
            4,
            "everything derived through the removed edge goes: \
             (c,b), (c,a), (d,b), (d,a); (d,c) survives"
        );
        assert_eq!(subscription.recomputes(), 2, "deletions rebuild");

        // And the rebuilt table continues extending afterwards.
        let e = Entity::new()?;
        branch
            .transaction()
            .assert(Parent::of(e.clone()).is(d.clone()))
            .commit()
            .perform(&operator)
            .await?;
        let delta = subscription.poll(&operator).await?.expect("extends again");
        let mut asserted = delta.asserted.clone();
        asserted.sort_by_key(|row| format!("{row:?}"));
        let mut expected = vec![
            HasAncestor {
                this: e.clone(),
                ancestor: Ancestor(d.clone()),
            },
            HasAncestor {
                this: e.clone(),
                ancestor: Ancestor(c.clone()),
            },
        ];
        expected.sort_by_key(|row| format!("{row:?}"));
        assert_eq!(
            asserted, expected,
            "e's ancestors follow the surviving chain"
        );
        assert_eq!(subscription.recomputes(), 2);
        assert_eq!(subscription.maintenances(), 3);
        Ok(())
    }
}

//! A standing fold: a [`Fold`] kept current from a subscription of each
//! of its two queries, each group folded again in memory when a row of
//! it moves.
//!
//! A [`Subscription`] of a fold asks the whole fold again on every poll
//! that touches it, since a fold cannot be restricted to one entity: a
//! change to one group reads every group. A [`FoldSubscription`] keeps
//! what the fold is made of instead:
//!
//! - a subscription of the groups query and one of the inputs query,
//!   each maintained as any subscription is;
//! - the bindings of every input row, by the key of its group, whether
//!   or not that group holds;
//! - the bindings of every group row that holds, by its key;
//! - the folded row each group last gave, to report it retracted when
//!   it changes.
//!
//! A poll polls both subscriptions, applies what each reports to the
//! rows it keeps, and folds each group a moved row is of again with
//! [`Fold::fold_groups`], the function [`Fold`]'s own evaluation folds
//! with, over the rows it keeps: no read. A group whose folded row
//! changed reports the old row retracted and the new one asserted, one
//! that started to hold is asserted, one that stopped is retracted, and
//! one whose row is unchanged reports nothing. Its keys and its rows are
//! made by [`Fold::key`] and [`Application::realize`] as a
//! [`select`](crate::Branch::query) of the fold makes them, so a
//! maintained row and a selected one are the same.
//!
//! The inner subscriptions report conclusions, and a fold joins and
//! folds bindings: each conclusion is turned back into the bindings it
//! was realized from by [`Application::bindings`], which attribute
//! queries, concept queries and folds give. A poll that meets a
//! conclusion without them fails.
//!
//! An inner subscription reports a row once, however many derivations
//! it has, so the rows kept here are a set: a query whose evaluation
//! yields one row twice is folded once here and twice by a `select`.
//! Attribute and concept queries yield each row once.

use std::collections::{BTreeSet, HashMap};
use std::mem;

use dialog_capability::{Fork, Provider};
use dialog_common::{ConditionalSync, Holds};
use dialog_corpus::tree::CorpusNode;
use dialog_effects::archive::Load as ArchiveLoad;
use dialog_effects::archive::{Get, Put};
use dialog_effects::authority::Identify;
use dialog_effects::blob::Load as BlobLoad;
use dialog_effects::blob::Read as BlobRead;
use dialog_effects::memory::Resolve;
use dialog_query::error::EvaluationError;
use dialog_query::query::Application;
use dialog_query::selection::Match;
use dialog_query::{Conclusion, Fold, FoldConclusion};
use dialog_search_tree::nodes;

use super::{Delta, Subscription};
use crate::{Branch, RemoteSite, Stack};

/// How many times one poll polls both inner subscriptions again when
/// something they read moved while they were polled. Past it the poll
/// reports what it folded; the move is a change the inner subscriptions
/// report on the next poll, which folds the groups it touches again.
const ROUNDS: usize = 8;

/// The rows of one query a fold keeps, by group key: each conclusion
/// with the bindings it was realized from.
type Rows<C> = HashMap<Vec<u8>, Vec<(C, Match)>>;

/// A standing [`Fold`] over a branch, kept current from a subscription
/// of its groups query and one of its inputs query (see the [module
/// docs](self)). Created by [`Branch::subscribe_fold`] or
/// [`Stack::fold_subscription`]; driven by [`poll`](Self::poll).
pub struct FoldSubscription<G: Application, Q: Application> {
    fold: Fold<G, Q>,
    groups: Subscription<G>,
    inputs: Subscription<Q>,
    /// The group rows that hold, by key.
    held: Rows<G::Conclusion>,
    /// Every input row, by the key of its group.
    rows: Rows<Q::Conclusion>,
    /// The folded row each group last reported, by key.
    folded: HashMap<Vec<u8>, FoldConclusion>,
    /// The keys of groups a moved row is of, not yet folded again: kept
    /// across a poll that failed, so the next one folds them.
    pending: BTreeSet<Vec<u8>>,
    initialized: bool,
    /// Groups folded again in memory so far.
    refolds: usize,
}

impl Branch {
    /// Register a standing fold over this branch: a
    /// [`FoldSubscription`], which keeps `fold` current from a
    /// subscription of each of its two queries and folds again only the
    /// groups a change moved a row of. The queries read the branch as
    /// [`Branch::subscribe`] does, its session overlay included, and so
    /// the rules laid on it.
    pub fn subscribe_fold<G: Application, Q: Application>(
        &self,
        fold: Fold<G, Q>,
    ) -> FoldSubscription<G, Q> {
        FoldSubscription::new(
            self.subscribe(fold.groups().clone()),
            self.subscribe(fold.inputs().clone()),
            fold,
        )
    }
}

impl Stack {
    /// A standing fold over this stack, the stack's alone, as
    /// [`Stack::subscription`] is of a query: each of the fold's two
    /// queries is such a subscription.
    pub fn fold_subscription<G: Application, Q: Application>(
        &self,
        fold: Fold<G, Q>,
    ) -> FoldSubscription<G, Q> {
        FoldSubscription::new(
            self.subscription(fold.groups().clone()),
            self.subscription(fold.inputs().clone()),
            fold,
        )
    }
}

impl<G: Application, Q: Application> FoldSubscription<G, Q> {
    fn new(groups: Subscription<G>, inputs: Subscription<Q>, fold: Fold<G, Q>) -> Self {
        Self {
            fold,
            groups,
            inputs,
            held: HashMap::new(),
            rows: HashMap::new(),
            folded: HashMap::new(),
            pending: BTreeSet::new(),
            initialized: false,
            refolds: 0,
        }
    }

    /// The fold this subscription keeps.
    pub fn fold(&self) -> &Fold<G, Q> {
        &self.fold
    }

    /// The subscription of the fold's groups query.
    pub fn groups(&self) -> &Subscription<G> {
        &self.groups
    }

    /// The subscription of the fold's inputs query.
    pub fn inputs(&self) -> &Subscription<Q> {
        &self.inputs
    }

    /// The folded rows as the last poll left them, one for each group.
    pub fn results(&self) -> impl Iterator<Item = &FoldConclusion> {
        self.folded.values()
    }

    /// How many groups have been folded so far: every group on the
    /// first poll, and on each later one the groups a moved row is of.
    pub fn refolds(&self) -> usize {
        self.refolds
    }
}

/// Add a conclusion's bindings to `rows` under its group's key, or take
/// them away, and note the key.
fn apply<C: PartialEq>(
    rows: &mut Rows<C>,
    touched: &mut BTreeSet<Vec<u8>>,
    key: impl Fn(&Match) -> Result<Vec<u8>, EvaluationError>,
    bindings: impl Fn(&C) -> Option<Match>,
    delta: Delta<C>,
) -> Result<(), EvaluationError> {
    let unbound = || EvaluationError::Planning {
        message: "a fold under a subscription folds the bindings behind each row of its \
                  queries, and this query does not give them (Application::bindings)"
            .to_string(),
    };
    for conclusion in delta.retracted {
        let row = bindings(&conclusion).ok_or_else(unbound)?;
        let key = key(&row)?;
        if let Some(kept) = rows.get_mut(&key) {
            if let Some(at) = kept.iter().position(|(held, _)| *held == conclusion) {
                kept.swap_remove(at);
            }
            if kept.is_empty() {
                rows.remove(&key);
            }
        }
        touched.insert(key);
    }
    for conclusion in delta.asserted {
        let row = bindings(&conclusion).ok_or_else(unbound)?;
        let key = key(&row)?;
        rows.entry(key.clone()).or_default().push((conclusion, row));
        touched.insert(key);
    }
    Ok(())
}

impl<G, Q> FoldSubscription<G, Q>
where
    G: Application + Clone + ConditionalSync,
    G::Conclusion: Conclusion + PartialEq + Clone + ConditionalSync,
    Q: Application + Clone + ConditionalSync,
    Q::Conclusion: Conclusion + PartialEq + Clone + ConditionalSync,
{
    /// Full evaluations of the inner queries so far, of both together:
    /// each one's first poll and every fallback from its maintenance.
    pub fn recomputes(&self) -> usize {
        self.groups.recomputes() + self.inputs.recomputes()
    }

    /// Polls of the inner queries maintained incrementally so far, of
    /// both together.
    pub fn maintenances(&self) -> usize {
        self.groups.maintenances() + self.inputs.maintenances()
    }

    /// Poll the fold against the branch's current state, as
    /// [`Subscription::poll`] polls a query.
    ///
    /// Returns `Ok(None)` when neither inner subscription reports a
    /// change and no group is waiting to be folded again. Otherwise
    /// returns `Ok(Some(delta))`: the first poll asserts every group's
    /// row, and a later one reports each group whose row moved.
    ///
    /// The two inner subscriptions read one state. Each pins what it
    /// reads at the state it finds when its poll begins, so the poll
    /// notes where the branch, its overlay and the stack's layers stand
    /// before polling both and checks they stand there still after: when
    /// something moved in between, it polls both again, and their
    /// reports add up, until a pair of polls begins and ends at one
    /// stamp, or for [`ROUNDS`] pairs.
    ///
    /// What one inner subscription reported is applied to the rows kept
    /// before the other is polled, and the groups it moved are kept until
    /// they are folded, so a poll that fails loses nothing: the next one
    /// folds them, and reports their rows against the ones last reported.
    pub async fn poll<'a, Env>(
        &'a mut self,
        env: &'a Env,
    ) -> Result<Option<Delta<FoldConclusion>>, EvaluationError>
    where
        Env: Provider<BlobRead>
            + Provider<Get>
            + Provider<Put>
            + Provider<Resolve>
            + Provider<Identify>
            + Provider<ArchiveLoad>
            + Provider<nodes::Write<CorpusNode>>
            + ConditionalSync
            + Provider<nodes::Read<CorpusNode>>
            + Provider<nodes::Preload<CorpusNode>>
            + ConditionalSync
            + Provider<BlobLoad>
            + Provider<dialog_corpus::Preload>
            + Provider<dialog_corpus::Speculation>
            + Provider<Fork<RemoteSite, Resolve>>
            + Holds
            + ConditionalSync
            + 'static,
    {
        let mut reported = false;
        for _ in 0..ROUNDS {
            let stamp = self.groups.stamp();
            if let Some(delta) = self.groups.poll(env).await? {
                reported = true;
                let (fold, groups) = (&self.fold, &self.groups);
                apply(
                    &mut self.held,
                    &mut self.pending,
                    |row| fold.key(row),
                    |conclusion| groups.query.bindings(conclusion),
                    delta,
                )?;
            }
            if let Some(delta) = self.inputs.poll(env).await? {
                reported = true;
                let (fold, inputs) = (&self.fold, &self.inputs);
                apply(
                    &mut self.rows,
                    &mut self.pending,
                    |row| fold.key(row),
                    |conclusion| inputs.query.bindings(conclusion),
                    delta,
                )?;
            }
            if self.groups.stamp() == stamp {
                break;
            }
        }
        if self.initialized && !reported && self.pending.is_empty() {
            return Ok(None);
        }

        // Every fallible step runs before what was reported changes, so
        // a fold that fails leaves the last reported rows in place.
        let mut now = Vec::with_capacity(self.pending.len());
        let mut refolds = 0;
        for key in &self.pending {
            let row = match self.held.get(key) {
                Some(groups) => {
                    refolds += 1;
                    let folded = self
                        .fold
                        .fold_groups(groups.iter().map(|(_, row)| row), |key| {
                            self.rows.get(key).into_iter().flatten().map(|(_, row)| row)
                        })?;
                    match folded.into_iter().next() {
                        Some(row) => Some(self.fold.realize(row)?),
                        None => None,
                    }
                }
                None => None,
            };
            now.push(row);
        }
        self.refolds += refolds;

        let mut delta = Delta {
            asserted: Vec::new(),
            retracted: Vec::new(),
        };
        for (key, row) in mem::take(&mut self.pending).into_iter().zip(now) {
            let before = match row {
                Some(row) => {
                    if self.folded.get(&key) == Some(&row) {
                        continue;
                    }
                    delta.asserted.push(row.clone());
                    self.folded.insert(key, row)
                }
                None => self.folded.remove(&key),
            };
            if let Some(before) = before {
                delta.retracted.push(before);
            }
        }
        self.initialized = true;
        Ok(Some(delta))
    }
}

#[cfg(test)]
mod tests;

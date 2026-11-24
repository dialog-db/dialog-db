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
//! A group of ten thousand rows folded again for each row that moved
//! costs ten thousand rows a change. Where every entry of the fold has a
//! [`Running`] value (`count`, `count-distinct`, `sum`, `min`, `max`; see
//! [`Reduce::runs`]), a group is kept by one once it has moved: the poll
//! notes each input row it adds to a group and takes away, and the
//! group's running value takes those rows in, joined with its group row,
//! and gives its row; no fold. It keeps exactly what a fold of the
//! group's rows needs, so it gives the row such a fold gives, and the
//! poll reports it as it reports a folded one. A group is read whole
//! again instead, its running value made again from all its rows (and
//! its rows folded where none can be made), when the
//! value cannot take a change in (an integer `sum` met by a float, the
//! last row holding a `max` taken away, a value a `min` cannot order),
//! when a row of its groups query moved, when it has no input row or
//! more than one group row, and on the first poll, which gives no group
//! a running value: a group gets one when it first moves.
//!
//! What it keeps, beside the rows: for each group that has a running
//! value, a number for `count` and `sum` and the chosen value for `min`
//! and `max`, and for `count-distinct` the bytes of each distinct value
//! with the number of rows holding it, no more entries than the group
//! has input rows.
//!
//! A fold with an entry that orders by standing (`last`, `top`; see
//! [`Aggregator::orders_by_standing`]) is not folded again from the
//! rows kept here, which carry no standing: a poll in which a row of
//! either query moved evaluates the whole fold afresh, as a `select`
//! does, and reports how its rows differ from the ones last reported.
//! Each such evaluation counts as a recompute.
//!
//! [`Aggregator::orders_by_standing`]: dialog_query::reduce::Aggregator::orders_by_standing
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
use dialog_query::reduce::{Reduce, Running};
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

/// The changes to one group not yet taken into its running value.
#[derive(Default)]
struct Journal {
    /// The bindings of each input row added (`true`) or taken away, in
    /// the order the inner subscription reported them.
    inputs: Vec<(bool, Match)>,
    /// Whether a row of the groups query moved under this key.
    regrouped: bool,
}

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
    /// Whether every entry of the fold has a running value (see
    /// [`Reduce::runs`]), so that a group can be kept by one.
    runs: bool,
    /// The running value of each group that has one, by key: what its
    /// kept input rows fold to, as of the rows kept before the changes
    /// [`journal`](Self::journal) holds for it.
    running: HashMap<Vec<u8>, Running>,
    /// Each pending group's changes not yet taken into its running
    /// value: the input rows added (`true`) and taken away, in the
    /// order they were, and whether a group row of its key moved.
    journal: HashMap<Vec<u8>, Journal>,
    /// Groups whose row was given by their running value so far.
    steps: usize,
    /// Whether an entry of the fold orders by standing, so that a poll
    /// that moved a row evaluates the fold afresh (see the [module
    /// docs](self)).
    afresh: bool,
    /// Fresh evaluations of the whole fold so far.
    evaluations: usize,
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
        let afresh = fold
            .reduce()
            .entries
            .iter()
            .any(|entry| entry.aggregator.orders_by_standing());
        let runs = !afresh && fold.reduce().runs();
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
            runs,
            running: HashMap::new(),
            journal: HashMap::new(),
            steps: 0,
            afresh,
            evaluations: 0,
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

    /// How many times a group's row has been given by its running value
    /// instead of folded again (see the [module docs](self)).
    pub fn steps(&self) -> usize {
        self.steps
    }
}

/// Which query's rows a delta [`apply`] applies is of.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Side {
    Groups,
    Inputs,
}

/// Add a conclusion's bindings to `rows` under its group's key, or take
/// them away, and note the key; with a journal, note there too that a
/// group row of the key moved, or each input row added and taken away.
fn apply<C: PartialEq>(
    rows: &mut Rows<C>,
    touched: &mut BTreeSet<Vec<u8>>,
    mut journal: Option<(&mut HashMap<Vec<u8>, Journal>, Side)>,
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
                let (_, row) = kept.swap_remove(at);
                if let Some((journal, side)) = journal.as_mut() {
                    note(journal, *side, &key, false, row);
                }
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
        if let Some((journal, side)) = journal.as_mut() {
            note(journal, *side, &key, true, row.clone());
        }
        rows.entry(key.clone()).or_default().push((conclusion, row));
        touched.insert(key);
    }
    Ok(())
}

/// Note in `journal` a row of `side` added under `key`, or taken away.
fn note(journal: &mut HashMap<Vec<u8>, Journal>, side: Side, key: &[u8], added: bool, row: Match) {
    let entry = journal.entry(key.to_vec()).or_default();
    match side {
        Side::Groups => entry.regrouped = true,
        Side::Inputs => entry.inputs.push((added, row)),
    }
}

/// Take a group's input changes into its running value, as joined with
/// its one group row, and give the row the value now folds to; `None`
/// when it cannot, having dropped what it could not keep: the group is
/// then folded again.
fn step(
    reduce: &Reduce,
    group: &Match,
    running: &mut Running,
    journal: Journal,
) -> Result<Option<Match>, EvaluationError> {
    if journal.regrouped {
        return Ok(None);
    }
    for (added, input) in journal.inputs {
        // An input row that does not join the group row is no row of
        // the group, as a fold of the group drops it.
        let Some(row) = group.clone().combine(&input) else {
            continue;
        };
        let took = if added {
            running.add(reduce, &row)
        } else {
            running.remove(reduce, &row)
        };
        if !took {
            return Ok(None);
        }
    }
    running.finish(reduce)
}

/// The running value of a group, from its one group row and every input
/// row it keeps, joined as a fold of the group joins them; `None` when
/// a row is one it cannot take, or none joins.
fn build<'r>(
    reduce: &Reduce,
    group: &Match,
    inputs: impl IntoIterator<Item = &'r Match>,
) -> Option<Running> {
    let mut running = Running::new(reduce)?;
    for input in inputs {
        if let Some(row) = group.clone().combine(input)
            && !running.add(reduce, &row)
        {
            return None;
        }
    }
    (running.rows() > 0).then_some(running)
}

impl<G, Q> FoldSubscription<G, Q>
where
    G: Application + Clone + ConditionalSync,
    G::Conclusion: Conclusion + PartialEq + Clone + ConditionalSync,
    Q: Application + Clone + ConditionalSync,
    Q::Conclusion: Conclusion + PartialEq + Clone + ConditionalSync,
{
    /// Full evaluations so far: of the inner queries, both together,
    /// each one's first poll and every fallback from its maintenance;
    /// and of the whole fold, for one that orders by standing.
    pub fn recomputes(&self) -> usize {
        self.groups.recomputes() + self.inputs.recomputes() + self.evaluations
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
                    self.runs.then_some((&mut self.journal, Side::Groups)),
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
                    self.runs.then_some((&mut self.journal, Side::Inputs)),
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
        if self.afresh {
            return self.evaluate_afresh(env).await.map(Some);
        }

        // Every fallible step runs before what was reported changes, so
        // a fold that fails leaves the last reported rows in place.
        // A group's running value takes its changes in as it gives its
        // row, and is consistent with the rows kept whether or not a
        // later group's fold fails: a key it has taken in is still
        // pending, with nothing left to take in.
        let mut now = Vec::with_capacity(self.pending.len());
        let (mut refolds, mut steps) = (0, 0);
        let reduce = self.fold.reduce();
        for key in &self.pending {
            let journal = self.journal.remove(key).unwrap_or_default();
            let row = match self.held.get(key) {
                Some(groups) => {
                    let inputs = self.rows.get(key).filter(|rows| !rows.is_empty());
                    // A running value keeps a group of one group row with
                    // inputs: one of no inputs is folded over inputs all
                    // absent, and one of two group rows over each.
                    let one = match (groups.as_slice(), inputs) {
                        ([(_, group)], Some(_)) => Some(group),
                        _ => None,
                    };
                    let stepped = match (one, self.running.get_mut(key)) {
                        (Some(group), Some(running)) => step(reduce, group, running, journal)?,
                        _ => None,
                    };
                    let folded = match stepped {
                        Some(row) => {
                            steps += 1;
                            Some(row)
                        }
                        None => {
                            self.running.remove(key);
                            refolds += 1;
                            // Past the first poll a group folded again is
                            // one that moved: it gets a running value for
                            // its next change, made from its rows, which
                            // gives its row as a fold of them would. Where
                            // none can be made the rows are folded.
                            let built = match (one, inputs) {
                                (Some(group), Some(inputs)) if self.runs && self.initialized => {
                                    build(reduce, group, inputs.iter().map(|(_, row)| row))
                                }
                                _ => None,
                            };
                            let row = match &built {
                                Some(running) => running.finish(reduce)?,
                                None => None,
                            };
                            match (built, row) {
                                (Some(running), Some(row)) => {
                                    self.running.insert(key.clone(), running);
                                    Some(row)
                                }
                                _ => self
                                    .fold
                                    .fold_groups(groups.iter().map(|(_, row)| row), |key| {
                                        self.rows.get(key).into_iter().flatten().map(|(_, row)| row)
                                    })?
                                    .into_iter()
                                    .next(),
                            }
                        }
                    };
                    match folded {
                        Some(row) => Some(self.fold.realize(row)?),
                        None => None,
                    }
                }
                None => {
                    self.running.remove(key);
                    None
                }
            };
            now.push(row);
        }
        self.refolds += refolds;
        self.steps += steps;

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

    /// Evaluate the whole fold afresh, over what the inner queries read,
    /// and report how its rows differ from the ones last reported: what
    /// a poll of a fold that orders by standing does.
    async fn evaluate_afresh<'a, Env>(
        &'a mut self,
        env: &'a Env,
    ) -> Result<Delta<FoldConclusion>, EvaluationError>
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
        // A subscription's first poll is a whole evaluation, read as the
        // inner subscriptions read: the same branch, and the same stack.
        let mut whole = Subscription::over(
            self.inputs.branch.clone(),
            self.inputs.stack.clone(),
            self.fold.clone(),
        );
        let rows = whole
            .poll(env)
            .await?
            .map(|delta| delta.asserted)
            .unwrap_or_default();
        let mut now = HashMap::with_capacity(rows.len());
        for row in rows {
            let bindings = self
                .fold
                .bindings(&row)
                .ok_or_else(|| EvaluationError::Planning {
                    message: "a folded row gives its bindings".to_string(),
                })?;
            now.insert(self.fold.key(&bindings)?, row);
        }
        self.evaluations += 1;
        let mut delta = Delta {
            asserted: Vec::new(),
            retracted: Vec::new(),
        };
        for (key, before) in &self.folded {
            if now.get(key) != Some(before) {
                delta.retracted.push(before.clone());
            }
        }
        for (key, row) in &now {
            if self.folded.get(key) != Some(row) {
                delta.asserted.push(row.clone());
            }
        }
        self.folded = now;
        self.pending.clear();
        self.initialized = true;
        Ok(delta)
    }
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod running_tests;

//! Graph subscriptions reuse ordinary demand-gated query subscriptions.
use std::collections::{BTreeMap, BTreeSet};

use dialog_capability::{Fork, Provider};
use dialog_common::{ConditionalSync, Holds};
use dialog_effects::archive::{Get, Put};
use dialog_effects::authority::Identify;
use dialog_effects::blob::Read as BlobRead;
use dialog_effects::memory::Resolve;
use dialog_query::graph::{GraphDelta, GraphError, GraphQuery, GraphReader, GraphResult};
use dialog_query::{ConceptDescriptor, ConceptQuery, Entity, Value};

use super::subscription::DiffBlockCache;
use crate::{Branch, RemoteSite, Subscription};

// The constraint is serialized with its tagged Value representation, so distinct
// typed constants cannot alias one another in the retained demand cache.
type FieldKey = (Entity, String, String, String);

/// A pull-driven graph query. Field reads retain the existing subscription's
/// demand cover, including empty reads. Shaping cycles retain no recursive rules
/// or evaluator state. Losing the last path removes a node and its field reads.
pub struct GraphSubscription {
    branch: Branch,
    query: GraphQuery,
    root: Subscription<ConceptQuery>,
    fields: BTreeMap<FieldKey, Subscription<ConceptQuery>>,
    used: BTreeSet<FieldKey>,
    results: GraphResult,
    initialized: bool,
    revision: Option<crate::Revision>,
    overlay_sequence: u64,
    retired_recomputes: usize,
    retired_maintenances: usize,
}

impl Branch {
    /// Register a graph query. Evaluation starts at the first poll.
    pub fn subscribe_graph(&self, query: GraphQuery) -> Result<GraphSubscription, GraphError> {
        query.validate()?;
        Ok(GraphSubscription {
            branch: self.clone(),
            root: self.subscribe(query.root.clone()),
            query,
            fields: BTreeMap::new(),
            used: BTreeSet::new(),
            results: GraphResult::default(),
            initialized: false,
            revision: None,
            overlay_sequence: 0,
            retired_recomputes: 0,
            retired_maintenances: 0,
        })
    }
}

impl GraphSubscription {
    /// The last successfully read normalized graph.
    pub fn results(&self) -> &GraphResult {
        &self.results
    }

    /// Ordinary subscriptions retained, including root matching. This exposes
    /// composition costs rather than hiding them behind a single graph handle.
    pub fn retained_subscriptions(&self) -> usize {
        self.fields.len() + 1
    }

    /// Full query evaluations, including first reads of newly reachable fields.
    pub fn recomputes(&self) -> usize {
        self.retired_recomputes
            + self.root.recomputes()
            + self
                .fields
                .values()
                .map(Subscription::recomputes)
                .sum::<usize>()
    }

    /// Per-entity incremental maintenances performed by underlying queries.
    pub fn maintenances(&self) -> usize {
        self.retired_maintenances
            + self.root.maintenances()
            + self
                .fields
                .values()
                .map(Subscription::maintenances)
                .sum::<usize>()
    }

    /// Refresh the graph. New reachability is read in this poll, cycles are
    /// pruned from roots, and shared nodes produce one field delta per shape.
    /// Concurrent branch or overlay changes discard the assembled result and
    /// retry. After eight interrupted attempts this returns `UnstableRead`,
    /// keeping the last published graph intact so callers can retry later.
    pub async fn poll<Env>(&mut self, env: &Env) -> Result<Option<GraphDelta>, GraphError>
    where
        Env: Provider<BlobRead>
            + Provider<Get>
            + Provider<Put>
            + Provider<Resolve>
            + Provider<Identify>
            + Provider<crate::Hydrate>
            + Provider<dialog_artifacts::Preload>
            + Provider<dialog_artifacts::Speculation>
            + Provider<Fork<RemoteSite, Resolve>>
            + Holds
            + ConditionalSync
            + 'static,
    {
        for _ in 0..8 {
            // Publish only an attempt whose branch and overlay did not move while
            // root matching and field reads awaited independently. Retained query
            // pins still advance on discarded attempts; retrying brings each to the
            // new state through its ordinary maintenance path.
            let edition = self.branch.revision.edition();
            let revision = edition.as_ref().map(|edition| edition.content.clone());
            let sequence = self.branch.overlay().revision().sequence;
            if self.initialized && self.revision == revision && self.overlay_sequence == sequence {
                return Ok(None);
            }
            self.used.clear();
            let query = self.query.clone();
            let diff_cache = DiffBlockCache::default();
            let read = query
                .read(&mut SubscriptionReader {
                    graph: self,
                    env,
                    diff_cache: &diff_cache,
                })
                .await;
            if self.branch.revision.edition() != edition
                || self.branch.overlay().revision().sequence != sequence
            {
                continue;
            }
            let next = read?;
            self.fields.retain(|key, subscription| {
                if self.used.contains(key) {
                    true
                } else {
                    self.retired_recomputes += subscription.recomputes();
                    self.retired_maintenances += subscription.maintenances();
                    false
                }
            });
            let delta = self.results.delta(&next);
            self.results = next;
            let initial = !self.initialized;
            self.initialized = true;
            self.revision = revision;
            self.overlay_sequence = sequence;
            return Ok((initial || !delta.is_empty()).then_some(delta));
        }
        Err(GraphError::UnstableRead)
    }
}

struct SubscriptionReader<'a, Env> {
    graph: &'a mut GraphSubscription,
    env: &'a Env,
    diff_cache: &'a DiffBlockCache,
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl<Env> GraphReader for SubscriptionReader<'_, Env>
where
    Env: Provider<BlobRead>
        + Provider<Get>
        + Provider<Put>
        + Provider<Resolve>
        + Provider<Identify>
        + Provider<crate::Hydrate>
        + Provider<dialog_artifacts::Preload>
        + Provider<dialog_artifacts::Speculation>
        + Provider<Fork<RemoteSite, Resolve>>
        + Holds
        + ConditionalSync
        + 'static,
{
    async fn roots(&mut self, _query: &ConceptQuery) -> Result<Vec<Entity>, GraphError> {
        self.graph
            .root
            .poll_using(self.env, Some(self.diff_cache))
            .await?;
        Ok(self
            .graph
            .root
            .results()
            .iter()
            .map(|row| row.entity().clone())
            .collect())
    }

    async fn field(
        &mut self,
        shape: &str,
        field: &str,
        entity: &Entity,
        equal: Option<&Value>,
    ) -> Result<Vec<Value>, GraphError> {
        let constraint = serde_json::to_string(&equal)
            .map_err(|error| GraphError::Invalid(error.to_string()))?;
        let key = (
            entity.clone(),
            shape.to_string(),
            field.to_string(),
            constraint,
        );
        self.graph.used.insert(key.clone());
        if !self.graph.fields.contains_key(&key) {
            let query = self.graph.query.schema.shapes[shape].field_query(field, entity, equal)?;
            self.graph
                .fields
                .insert(key.clone(), self.graph.branch.subscribe(query));
        }
        let subscription = self
            .graph
            .fields
            .get_mut(&key)
            .expect("inserted field subscription");
        subscription
            .poll_using(self.env, Some(self.diff_cache))
            .await?;
        let mut values = Vec::new();
        for row in subscription.results() {
            let value = row.value(ConceptDescriptor::VALUE)?;
            if !values.contains(&value) {
                values.push(value);
            }
        }
        Ok(values)
    }
}

#[cfg(test)]
mod tests;

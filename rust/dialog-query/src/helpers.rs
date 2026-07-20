//! Benchmark helpers for the query engine.
//!
//! [`BenchEnv`] builds a real query environment (operator + repository +
//! branch) and runs queries through the actual branch-select path. It is
//! designed for benchmarks that need three signals:
//!
//! 1. **Read count** — the number of block fetches a query triggers,
//!    recorded via [`JournaledStorage`]. This is the planner's true
//!    objective (minimize round-trips) and is deterministic and
//!    machine-independent.
//! 2. **In-memory wall-clock** — engine CPU isolation, via a volatile
//!    [`BenchEnv::volatile`] environment.
//! 3. **On-disk wall-clock** — real-world latency where I/O dominates,
//!    via an [`BenchEnv::temp`] environment backed by the platform temp
//!    directory.

// The runtime imports below use `::dialog_query::…`, but the
// `#[derive(Concept)]`/`#[derive(Attribute)]` expansions emit bare
// `dialog_query::…` paths, so the name must also resolve without the
// leading `::`. No declaration is needed here in either build context: in
// the crate's own (test) build the lib root already aliases itself with
// `extern crate self as dialog_query`, and in the bench build this file is
// `#[path]`-included into a separate target where Cargo links the package's
// own lib under the `dialog_query` name (its extern prelude), so bare
// `dialog_query::…` resolves to the real crate in both.

use anyhow::Result;
use async_trait::async_trait;
use dialog_artifacts::selector::Constrained;
use dialog_artifacts::{
    Artifact, ArtifactSelector, ArtifactStream, Attribute, DialogArtifactsError, Instruction,
    Select,
};
use dialog_capability::{Fork, Provider, Subject};
use dialog_common::ConditionalSync;
use dialog_effects::archive::{Get, Import, Put};
use dialog_effects::authority::{Attest, Identify};
use dialog_effects::memory::{Publish, Resolve};
use dialog_effects::space::{Create as SpaceCreate, Load as SpaceLoad};
use dialog_network::Network;
use dialog_operator::helpers::{generate_data, unique_name};
use dialog_operator::{Operator, Profile};
use dialog_repository::{Branch, NetworkedIndex, RemoteSite, Repository, RepositoryExt as _};
use dialog_storage::provider::storage::{Storage, VolatileSpace};
use dialog_storage::{Blake3Hash, DialogStorageError, JournaledStorage, StorageBackend};
// The platform temp filesystem (and the on-disk `BenchEnv::temp` variant
// built on it) only exists off wasm — there is no native temp directory in
// the browser, so the whole on-disk path is gated to non-wasm targets.
#[cfg(not(target_arch = "wasm32"))]
use dialog_storage::NativeTempSpace;
use futures_util::{TryStreamExt as _, stream};
use std::collections::{BTreeMap, HashSet};
use std::env;
use std::sync::{Arc, Mutex};
use std::time::Instant;
use tracing::span::Attributes;
use tracing::{Id, Subscriber};
use tracing_subscriber::Layer;
use tracing_subscriber::layer::Context as LayerContext;
use tracing_subscriber::registry::LookupSpan;

use ::dialog_query::concept::query::ConceptRules;
use ::dialog_query::error::EvaluationError;
use ::dialog_query::source::SelectRules;
use ::dialog_query::{Concept, ConceptDescriptor, Entity, Output, Query, RuleRegistry, Term};

/// Attribute markers backing the [`Stuff`] concept. Each derives the
/// query-engine `Attribute` trait, giving it a stable `stuff/<field>`
/// attribute identifier the derived concept and its implicit rule use.
mod stuff {
    use ::dialog_query::Attribute;

    /// The `stuff/name` attribute.
    #[derive(Attribute, Clone, PartialEq)]
    pub struct Name(pub String);

    /// The `stuff/role` attribute.
    #[derive(Attribute, Clone, PartialEq)]
    pub struct Role(pub String);
}

/// Attribute markers for the [`Bug`] concept, a realistic 7-field record
/// mirroring the `squash.bug/*` schema of the tonk bug tracker. The
/// `#[domain("squash.bug")]` override gives each the exact real attribute
/// identifier (e.g. `squash.bug/status`), so a query over this concept is the
/// same shape the real app issues.
pub mod bug {
    use ::dialog_query::Attribute;

    /// Bug status (triage / todo / in-progress / done / canceled).
    #[derive(Attribute, Clone, PartialEq)]
    #[domain("squash.bug")]
    pub struct Status(pub String);

    /// Bug priority (low / medium / high / urgent).
    #[derive(Attribute, Clone, PartialEq)]
    #[domain("squash.bug")]
    pub struct Priority(pub String);

    /// Bug assignee (a DID string, or empty for unassigned).
    #[derive(Attribute, Clone, PartialEq)]
    #[domain("squash.bug")]
    pub struct Assignee(pub String);

    /// Bug title.
    #[derive(Attribute, Clone, PartialEq)]
    #[domain("squash.bug")]
    pub struct Title(pub String);

    /// LexoRank-style ordering key. Float in the real schema — the field that
    /// would have tripped the float-key width bug in a concept join.
    #[derive(Attribute, Clone, PartialEq)]
    #[domain("squash.bug")]
    pub struct Ordering(pub f64);
}

/// A realistic bug-tracker record: the seven-way join the tonk bug app runs.
/// Fewer fields than the real seven (detail/ident omitted) is enough to
/// exercise a real multi-premise concept join with a `Float` field.
#[derive(Clone, Debug, PartialEq, Concept)]
pub struct Bug {
    /// The bug entity.
    pub this: Entity,
    /// Its status.
    pub status: bug::Status,
    /// Its priority.
    pub priority: bug::Priority,
    /// Its assignee.
    pub assignee: bug::Assignee,
    /// Its title.
    pub title: bug::Title,
    /// Its ordering key (Float).
    pub ordering: bug::Ordering,
}

/// The fields of a brand-new issue for [`BenchEnv::create_bug`] — the
/// queryable concept fields plus the free-text `detail` description and the
/// human-facing `ident` label. Grouped into one struct so the create-issue
/// transaction reads like the record it commits.
#[derive(Clone, Debug)]
pub struct NewBug<'a> {
    /// Initial status (e.g. `triage`).
    pub status: &'a str,
    /// Priority (e.g. `high`).
    pub priority: &'a str,
    /// Assignee DID, or empty for unassigned.
    pub assignee: &'a str,
    /// Issue title.
    pub title: &'a str,
    /// Ordering key.
    pub ordering: f64,
    /// Human-facing label (e.g. `BUG-42`).
    pub ident: &'a str,
    /// The specific free-text description.
    pub detail: &'a str,
}

/// The source concept seeded into the branch and queried in the join
/// benchmark: each entity carries a `stuff/name` and a `stuff/role`.
///
/// Querying [`Stuff`] with both fields free drives the planner's implicit
/// two-attribute (`name` + `role`) rule, joined on the shared `this`
/// entity — the ordering choice that makes this exercise round-trip
/// optimization. No rule needs to be registered: the [`RuleRegistry`]
/// synthesizes the default rule for an unregistered concept on demand.
#[derive(Clone, Debug, PartialEq, Concept)]
pub struct Stuff {
    /// The entity the stuff facts hang off.
    pub this: Entity,
    /// Name of the stuff member.
    pub name: stuff::Name,
    /// Role of the stuff member.
    pub role: stuff::Role,
}

/// Attributes of the bug-tracker benchmark concept.
/// Per-span accumulated `(micros, calls)`, shared between the subscriber
/// layer that records them and the report that prints them.
#[cfg(not(target_arch = "wasm32"))]
type PhaseTotals = Arc<Mutex<BTreeMap<&'static str, (u128, u64)>>>;

/// Installs a tracing subscriber that totals time per span name, so a
/// benchmark can attribute cost to phases without hand-rolled timers.
///
/// Enabled by `DIALOG_TRACE=1`. Reports on drop of the returned guard.
#[cfg(not(target_arch = "wasm32"))]
pub fn trace_phases() -> Option<PhaseReport> {
    if env::var("DIALOG_TRACE").is_err() {
        return None;
    }
    let totals: PhaseTotals = Arc::default();
    let layer = PhaseLayer {
        totals: totals.clone(),
    };
    use tracing_subscriber::prelude::*;
    let _ = tracing_subscriber::registry().with(layer).try_init();
    Some(PhaseReport { totals })
}

/// Accumulated per-span timings, printed when dropped.
#[cfg(not(target_arch = "wasm32"))]
pub struct PhaseReport {
    totals: PhaseTotals,
}

#[cfg(not(target_arch = "wasm32"))]
impl Drop for PhaseReport {
    fn drop(&mut self) {
        let totals = self.totals.lock().expect("phase totals");
        for (name, (micros, count)) in totals.iter() {
            eprintln!(
                "TRACE {name:<24} total={micros:>10}us calls={count:<7} mean={:>8}us",
                micros / (*count).max(1) as u128
            );
        }
    }
}

#[cfg(not(target_arch = "wasm32"))]
struct PhaseLayer {
    totals: PhaseTotals,
}

#[cfg(not(target_arch = "wasm32"))]
struct SpanStart(Instant);

#[cfg(not(target_arch = "wasm32"))]
impl<S> Layer<S> for PhaseLayer
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    fn on_new_span(&self, _attrs: &Attributes<'_>, id: &Id, ctx: LayerContext<'_, S>) {
        if let Some(span) = ctx.span(id) {
            span.extensions_mut().insert(SpanStart(Instant::now()));
        }
    }

    fn on_close(&self, id: Id, ctx: LayerContext<'_, S>) {
        if let Some(span) = ctx.span(&id)
            && let Some(start) = span.extensions().get::<SpanStart>()
        {
            let elapsed = start.0.elapsed().as_micros();
            let mut totals = self.totals.lock().expect("phase totals");
            let entry = totals.entry(span.name()).or_insert((0, 0));
            entry.0 += elapsed;
            entry.1 += 1;
        }
    }
}

/// The outcome of running a single query through [`BenchEnv::run_query`].
#[derive(Debug, Clone)]
pub struct QueryRun {
    /// The artifacts the query produced.
    pub results: Vec<Artifact>,
    /// Total number of block reads (including repeats) the query triggered.
    pub reads: usize,
    /// Number of distinct block keys the query read.
    pub unique_reads: usize,
}

impl QueryRun {
    /// The number of artifacts the query produced.
    pub fn len(&self) -> usize {
        self.results.len()
    }

    /// Whether the query produced no artifacts.
    pub fn is_empty(&self) -> bool {
        self.results.is_empty()
    }
}

/// The outcome of running a multi-premise concept query (a join)
/// through [`BenchEnv::run_join`].
///
/// Unlike [`QueryRun`], the read counts here aggregate every block fetch
/// the planned rule issues across *all* premises and *all* scopes — the
/// signal the planner's round-trip optimization actually moves.
#[derive(Debug, Clone)]
pub struct JoinRun {
    /// The number of derived results the query produced.
    pub results_len: usize,
    /// Total block reads (including repeats) across every premise select.
    pub reads: usize,
    /// Number of distinct block keys read across every premise select.
    pub unique_reads: usize,
}

/// A read journal shared across the many `Provider<Select>` calls a
/// multi-premise query issues.
///
/// A planned join evaluates each premise (and re-evaluates inner premises
/// once per outer binding) through a *separate* `Provider<Select>::execute`
/// call, and each call builds its own [`NetworkedIndex`] over the borrowed
/// operator. To attribute every one of those block reads to a single
/// query we cannot reuse one [`JournaledStorage`] instance (its backend
/// would have to outlive each per-call index borrow). Instead the journal
/// is this small shared accumulator: each call wraps its fresh index in a
/// [`CountingStore`] that holds a clone of the same `Arc`, so all reads
/// fold into one counter cleared once before the query and read once after.
#[derive(Clone, Default)]
pub struct ReadJournal {
    state: Arc<Mutex<JournalState>>,
}

#[derive(Default)]
struct JournalState {
    reads: usize,
    keys: HashSet<Blake3Hash>,
}

impl ReadJournal {
    /// Reset the journal so warm-up reads are excluded from the next run.
    pub fn clear(&self) {
        let mut state = self.state.lock().unwrap();
        state.reads = 0;
        state.keys.clear();
    }

    /// Total reads (including repeats) recorded since the last [`clear`].
    ///
    /// [`clear`]: ReadJournal::clear
    pub fn reads(&self) -> usize {
        self.state.lock().unwrap().reads
    }

    /// Distinct block keys recorded since the last [`clear`].
    ///
    /// [`clear`]: ReadJournal::clear
    pub fn unique_reads(&self) -> usize {
        self.state.lock().unwrap().keys.len()
    }

    fn record(&self, key: &Blake3Hash) {
        let mut state = self.state.lock().unwrap();
        state.reads += 1;
        state.keys.insert(*key);
    }
}

/// A [`StorageBackend`] that records every successful read into a shared
/// [`ReadJournal`] before delegating to the wrapped backend.
///
/// Built fresh per `Provider<Select>` call but parameterized over a cloned
/// `ReadJournal`, so reads from every premise select accumulate together.
#[derive(Clone)]
pub struct CountingStore<Backend> {
    backend: Backend,
    journal: ReadJournal,
}

impl<Backend> CountingStore<Backend> {
    fn new(backend: Backend, journal: ReadJournal) -> Self {
        Self { backend, journal }
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<Backend> StorageBackend for CountingStore<Backend>
where
    Backend: StorageBackend<Key = Blake3Hash, Value = Vec<u8>, Error = DialogStorageError>
        + ConditionalSync,
{
    type Key = Blake3Hash;
    type Value = Vec<u8>;
    type Error = DialogStorageError;

    async fn set(&mut self, key: Self::Key, value: Self::Value) -> Result<(), Self::Error> {
        self.backend.set(key, value).await
    }

    async fn get(&self, key: &Self::Key) -> Result<Option<Self::Value>, Self::Error> {
        let value = self.backend.get(key).await?;
        if value.is_some() {
            self.journal.record(key);
        }
        Ok(value)
    }
}

/// A journaled query environment for multi-premise concept queries.
///
/// Implements both `Provider<Select<'a>>` (routing every premise scan
/// through a [`CountingStore`] over the shared [`ReadJournal`]) and
/// `Provider<SelectRules>` (delegating to the [`RuleRegistry`] so the
/// planner can order the join). Constructed by [`BenchEnv::run_join`].
pub struct JoinEnv<'a, Env> {
    branch: &'a Branch,
    operator: &'a Env,
    rules: RuleRegistry,
    journal: ReadJournal,
}

impl<'a, Env> JoinEnv<'a, Env> {
    /// The shared read journal accumulating block reads across premises.
    pub fn journal(&self) -> &ReadJournal {
        &self.journal
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<'a, Env> Provider<Select<'a>> for JoinEnv<'a, Env>
where
    Env: Provider<Get>
        + Provider<Put>
        + Provider<Resolve>
        + Provider<Fork<RemoteSite, Get>>
        + Provider<Fork<RemoteSite, Resolve>>
        + ConditionalSync
        + 'static,
{
    async fn execute(
        &self,
        input: ArtifactSelector<Constrained>,
    ) -> Result<ArtifactStream<'a>, DialogArtifactsError> {
        let select = self.branch.claims().select(input);
        let store = NetworkedIndex::new(self.operator, select.catalog(), None);
        let counting = CountingStore::new(store, self.journal.clone());
        let stream = select.execute(counting).await?;
        Ok(Box::pin(stream))
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<Env: ConditionalSync> Provider<SelectRules> for JoinEnv<'_, Env> {
    async fn execute(&self, input: ConceptDescriptor) -> Result<ConceptRules, EvaluationError> {
        self.rules.acquire(&input)
    }
}

/// A benchmark environment wrapping an operator, repository, and branch.
///
/// Generic over the operator's space type `Env` so the same seeding and
/// query logic serves both the volatile (in-memory) and temp (on-disk)
/// variants. Construct via [`BenchEnv::volatile`] or [`BenchEnv::temp`].
pub struct BenchEnv<Env> {
    operator: Env,
    repo: Repository,
    branch: String,
}

impl BenchEnv<Operator<VolatileSpace>> {
    /// Build a volatile (in-memory) benchmark environment.
    ///
    /// Use for CPU/memory-read isolated signals — no disk I/O.
    pub async fn volatile() -> Result<Self> {
        let storage = Storage::volatile();
        Self::with_storage(storage).await
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl BenchEnv<Operator<NativeTempSpace>> {
    /// Build an on-disk benchmark environment rooted in the platform
    /// temp directory.
    ///
    /// Use for real-world latency signals where I/O dominates.
    pub async fn temp() -> Result<Self> {
        let storage = Storage::temp();
        Self::with_storage(storage).await
    }
}

#[cfg(all(target_arch = "wasm32", feature = "browser-bench"))]
impl BenchEnv<Operator<::dialog_storage::provider::storage::WebSpace>> {
    /// Build an IndexedDB-backed benchmark environment (the real browser
    /// backend). Reads are async IndexedDB round-trips, so this is the wasm
    /// analogue of the on-disk backend — the read-count reduction shows up as
    /// fewer store round-trips.
    pub async fn web() -> Result<Self> {
        let storage = Storage::<::dialog_storage::provider::storage::WebSpace>::default();
        Self::with_storage(storage).await
    }
}

impl<Env> BenchEnv<Env>
where
    Env: Provider<Get>
        + Provider<Put>
        + Provider<Import>
        + Provider<Resolve>
        + Provider<Publish>
        + Provider<Identify>
        + Provider<Attest>
        + Provider<SpaceLoad>
        + Provider<SpaceCreate>
        + Provider<Fork<RemoteSite, Get>>
        + Provider<Fork<RemoteSite, Resolve>>
        + ConditionalSync
        + 'static,
{
    /// The operator backing this environment.
    pub fn operator(&self) -> &Env {
        &self.operator
    }

    /// Seed `entity_count` entities' worth of deterministic facts into
    /// the branch via a single transaction.
    ///
    /// Seeding is intentionally not part of any measured path. The facts
    /// come from [`generate_data`], which produces several artifacts per
    /// entity across a handful of attributes.
    pub async fn seed(&self, entity_count: usize) -> Result<usize> {
        let data = generate_data(entity_count)?;
        let count = data.len();
        let branch = self
            .repo
            .branch(&self.branch)
            .open()
            .perform(&self.operator)
            .await?;

        let instructions: Vec<Instruction> = data.into_iter().map(Instruction::Assert).collect();
        branch
            .commit(stream::iter(instructions))
            .perform(&self.operator)
            .await?;
        Ok(count)
    }

    /// Run a select-by-attribute query against the seeded branch,
    /// recording block reads via a [`JournaledStorage`] wrapper.
    ///
    /// The query scans the branch's index for every fact carrying the
    /// given attribute. The journal is cleared immediately before the
    /// scan so warm-up reads (e.g. the eager root probe inside
    /// `execute`) are excluded from the recorded counts.
    pub async fn run_query(&self, attribute: &str) -> Result<QueryRun> {
        let the: Attribute = attribute.parse()?;
        let branch = self
            .repo
            .branch(&self.branch)
            .load()
            .perform(&self.operator)
            .await?;

        let select = branch.claims().select(ArtifactSelector::new().the(the));
        let store = NetworkedIndex::new(&self.operator, select.catalog(), None);
        let journaled = JournaledStorage::new(store);
        journaled.clear_journal();

        let stream = select.execute(journaled.clone()).await?;
        let results: Vec<Artifact> = stream.try_collect().await?;

        let reads = journaled.read_count();
        let unique_reads = journaled.unique_keys_read_count();

        Ok(QueryRun {
            results,
            reads,
            unique_reads,
        })
    }

    /// Seed `entity_count` stuff entities, each with a `stuff/name` and a
    /// `stuff/role`, in a single transaction.
    ///
    /// Like [`seed`](Self::seed) this is intentionally off the measured
    /// path; it exists so [`query_stuff`](Self::query_stuff) has a fact base
    /// the implicit two-attribute rule can join over. Exposed so benches can
    /// seed once at setup and time only the query.
    pub async fn seed_stuff(&self, entity_count: usize) -> Result<()> {
        let branch = self
            .repo
            .branch(&self.branch)
            .open()
            .perform(&self.operator)
            .await?;

        let mut transaction = branch.transaction();
        for index in 0..entity_count {
            transaction = transaction.assert(Stuff {
                this: Entity::new()?,
                name: stuff::Name(format!("name-{index}")),
                role: stuff::Role(format!("role-{}", index % 8)),
            });
        }
        transaction.commit().perform(&self.operator).await?;
        Ok(())
    }

    /// Run the public [`Stuff`] concept query and report the block reads of
    /// the *whole* query. Does not seed — the caller seeds via
    /// [`seed_stuff`](Self::seed_stuff) first (so benches can seed once at
    /// setup and time only this query).
    ///
    /// The query is the ordinary public surface: a [`Query<Stuff>`] with
    /// both fields free, driven through [`Application::perform`]. With two
    /// free attributes the planner has a join-ordering choice — the whole
    /// reason this query exercises round-trip optimization. No rule is
    /// registered: the [`RuleRegistry`] synthesizes the implicit default
    /// rule for the unregistered concept on demand. Every premise scan the
    /// plan issues routes through a [`CountingStore`] over one shared
    /// [`ReadJournal`], so the reported counts aggregate all reads across
    /// the join. The journal is cleared once before evaluation and read
    /// once after, excluding the rule-registry warm-up.
    ///
    /// [`Query<Stuff>`]: Query
    /// [`Application::perform`]: ::dialog_query::Application::perform
    pub async fn query_stuff(&self) -> Result<JoinRun> {
        let branch = self
            .repo
            .branch(&self.branch)
            .load()
            .perform(&self.operator)
            .await?;

        let env = JoinEnv {
            branch: &branch,
            operator: &self.operator,
            rules: RuleRegistry::new(),
            journal: ReadJournal::default(),
        };

        env.journal().clear();
        let results = Query::<Stuff> {
            this: Term::var("this"),
            name: Term::var("name"),
            role: Term::var("role"),
        }
        .perform(&env)
        .try_vec()
        .await?;

        Ok(JoinRun {
            results_len: results.len(),
            reads: env.journal().reads(),
            unique_reads: env.journal().unique_reads(),
        })
    }

    /// Seed `entity_count` stuff entities and run the public concept query
    /// in one call, reporting the join's block reads.
    ///
    /// Convenience over [`seed_stuff`](Self::seed_stuff) +
    /// [`query_stuff`](Self::query_stuff) for one-shot reporting.
    pub async fn run_join(&self, entity_count: usize) -> Result<JoinRun> {
        self.seed_stuff(entity_count).await?;
        self.query_stuff().await
    }

    /// Seed a realistic bug-tracker fact base: `count` bugs, each a seven-fact
    /// [`Bug`] record, with status/priority/assignee drawn from the same
    /// distribution as the real tonk data (mostly-done/triage, medium priority,
    /// a few assignees plus many unassigned). Returns the seeded entities so a
    /// transaction benchmark can update specific bugs. Off the measured path.
    /// Import the real bug records from a `tonk export` CSV (the
    /// `the,of,as,is,cause` layout) into the branch, asserting every
    /// `squash.bug/*` fact — including `detail` (the long free-text
    /// description) and `ident`, which the [`Bug`] concept join does not read
    /// but which are real facts that shape the tree. Returns each bug entity
    /// alongside its status and assignee so the query benchmarks can pin real
    /// values. Off the measured path.
    ///
    /// Only rows whose attribute begins with `squash.bug/` are imported; the
    /// CSV parser handles quoted multi-line fields (the `detail` column holds
    /// multi-line text).
    #[cfg(not(target_arch = "wasm32"))]
    #[allow(clippy::absolute_paths)]
    pub async fn import_bugs_from_csv(
        &self,
        csv_path: &str,
    ) -> Result<Vec<(Entity, String, String)>> {
        fn parse_csv(text: &str) -> Vec<Vec<String>> {
            let mut records = Vec::new();
            let mut record = Vec::new();
            let mut field = String::new();
            let mut in_quotes = false;
            let mut chars = text.chars().peekable();
            while let Some(ch) = chars.next() {
                match ch {
                    '"' if in_quotes && chars.peek() == Some(&'"') => {
                        field.push('"');
                        chars.next();
                    }
                    '"' => in_quotes = !in_quotes,
                    ',' if !in_quotes => record.push(std::mem::take(&mut field)),
                    '\n' if !in_quotes => {
                        record.push(std::mem::take(&mut field));
                        records.push(std::mem::take(&mut record));
                    }
                    '\r' if !in_quotes => {}
                    _ => field.push(ch),
                }
            }
            if !field.is_empty() || !record.is_empty() {
                record.push(field);
                records.push(record);
            }
            records
        }

        let text = std::fs::read_to_string(csv_path)?;
        // Group the flat (the, of, is) rows by entity DID so each bug is a
        // record. The DID strings map to fresh Entities (stable per DID within
        // this import) so the concept join has real, distinct entities.
        let mut by_did: std::collections::BTreeMap<
            String,
            std::collections::HashMap<String, String>,
        > = std::collections::BTreeMap::new();
        for row in parse_csv(&text).into_iter().skip(1) {
            if row.len() < 4 {
                continue;
            }
            let the = &row[0];
            if !the.starts_with("squash.bug/") {
                continue;
            }
            let did = row[1].clone();
            let value = row[3].clone();
            by_did.entry(did).or_default().insert(the.clone(), value);
        }

        let branch = self
            .repo
            .branch(&self.branch)
            .open()
            .perform(&self.operator)
            .await?;

        let mut transaction = branch.transaction();
        let mut index = Vec::new();
        for fields in by_did.values() {
            let entity = Entity::new()?;
            let status = fields.get("squash.bug/status").cloned().unwrap_or_default();
            let assignee = fields
                .get("squash.bug/assignee")
                .cloned()
                .unwrap_or_default();
            let priority = fields
                .get("squash.bug/priority")
                .cloned()
                .unwrap_or_default();
            let title = fields.get("squash.bug/title").cloned().unwrap_or_default();
            let ordering = fields
                .get("squash.bug/ordering")
                .and_then(|value| value.parse::<f64>().ok())
                .unwrap_or(0.0);
            let ident = fields.get("squash.bug/ident").cloned().unwrap_or_default();
            let detail = fields.get("squash.bug/detail").cloned().unwrap_or_default();

            // The queryable concept fields.
            transaction = transaction.assert(Bug {
                this: entity.clone(),
                status: bug::Status(status.clone()),
                priority: bug::Priority(priority),
                assignee: bug::Assignee(assignee.clone()),
                title: bug::Title(title),
                ordering: bug::Ordering(ordering),
            });
            // The extra real facts (detail is the long description; ident the
            // human-facing BUG-N label). Not part of the join, but real bytes.
            transaction = transaction
                .assert(
                    ::dialog_query::the!("squash.bug/detail")
                        .of(entity.clone())
                        .is(detail),
                )
                .assert(
                    ::dialog_query::the!("squash.bug/ident")
                        .of(entity.clone())
                        .is(ident),
                );

            index.push((entity, status, assignee));
        }
        transaction.commit().perform(&self.operator).await?;
        Ok(index)
    }

    /// Run the [`Bug`] concept join, optionally pinning `status` and/or
    /// `assignee` to constants; any field left `None` stays a free variable.
    /// This is the exact query surface the app issues for the board views
    /// (all bugs, bugs of a given status) and the "assigned to member X"
    /// filter. Reports the join's aggregate block reads.
    pub async fn query_bugs(
        &self,
        status: Option<&str>,
        assignee: Option<&str>,
    ) -> Result<JoinRun> {
        let branch = self
            .repo
            .branch(&self.branch)
            .load()
            .perform(&self.operator)
            .await?;

        let env = JoinEnv {
            branch: &branch,
            operator: &self.operator,
            rules: RuleRegistry::new(),
            journal: ReadJournal::default(),
        };

        let status_term = match status {
            Some(value) => Term::from(value.to_string()),
            None => Term::var("status"),
        };
        let assignee_term = match assignee {
            Some(value) => Term::from(value.to_string()),
            None => Term::var("assignee"),
        };

        env.journal().clear();
        let results = Query::<Bug> {
            this: Term::var("this"),
            status: status_term,
            priority: Term::var("priority"),
            assignee: assignee_term,
            title: Term::var("title"),
            ordering: Term::var("ordering"),
        }
        .perform(&env)
        .try_vec()
        .await?;

        Ok(JoinRun {
            results_len: results.len(),
            reads: env.journal().reads(),
            unique_reads: env.journal().unique_reads(),
        })
    }

    /// The "open issues" board query: the union of the not-closed statuses.
    /// The concept query pins one value per field, so an open/closed *set*
    /// is the union of the per-status joins — the same way the app renders a
    /// board column per status. Returns the summed results and reads.
    pub async fn query_bugs_in_statuses(&self, statuses: &[&str]) -> Result<JoinRun> {
        let mut total = JoinRun {
            results_len: 0,
            reads: 0,
            unique_reads: 0,
        };
        for status in statuses {
            let run = self.query_bugs(Some(status), None).await?;
            total.results_len += run.results_len;
            total.reads += run.reads;
            total.unique_reads += run.unique_reads;
        }
        Ok(total)
    }

    /// Create a brand-new issue in a single transaction: the full concept
    /// record plus a specific free-text `detail` description and a `ident`
    /// label — the "file a bug with a specific description" flow.
    pub async fn create_bug(&self, new: NewBug<'_>) -> Result<Entity> {
        let entity = Entity::new()?;
        let branch = self
            .repo
            .branch(&self.branch)
            .open()
            .perform(&self.operator)
            .await?;
        branch
            .transaction()
            .assert(Bug {
                this: entity.clone(),
                status: bug::Status(new.status.to_string()),
                priority: bug::Priority(new.priority.to_string()),
                assignee: bug::Assignee(new.assignee.to_string()),
                title: bug::Title(new.title.to_string()),
                ordering: bug::Ordering(new.ordering),
            })
            .assert(
                ::dialog_query::the!("squash.bug/ident")
                    .of(entity.clone())
                    .is(new.ident.to_string()),
            )
            .assert(
                ::dialog_query::the!("squash.bug/detail")
                    .of(entity.clone())
                    .is(new.detail.to_string()),
            )
            .commit()
            .perform(&self.operator)
            .await?;
        Ok(entity)
    }

    /// Run the realistic bug-tracker benchmark against this environment
    /// (in-memory or on-disk, whichever it was built with): seed `count` bugs,
    /// then time the board queries and the file/close/reassign transactions,
    /// printing `BUGBENCH` lines. Generic over the backend so the same workload
    /// runs on both.
    #[allow(clippy::absolute_paths)]
    pub async fn run_bug_bench(&self, count: usize) -> Result<()> {
        let seed_start = std::time::Instant::now();
        let entities = self.seed_bugs(count).await?;
        let seed_elapsed = seed_start.elapsed();

        let bench = |label: &'static str, run: JoinRun, elapsed: std::time::Duration| {
            eprintln!(
                "BUGBENCH {label:<22} results={:<5} reads={:<6} unique_reads={:<5} time={elapsed:?}",
                run.results_len, run.reads, run.unique_reads
            );
        };

        let start = std::time::Instant::now();
        let all = self.query_bugs_by_status(None).await?;
        bench("all-bugs", all, start.elapsed());

        let start = std::time::Instant::now();
        let done = self.query_bugs_by_status(Some("done")).await?;
        bench("status=done", done, start.elapsed());

        let start = std::time::Instant::now();
        let triage = self.query_bugs_by_status(Some("triage")).await?;
        bench("status=triage", triage, start.elapsed());

        // File a new bug (a whole record).
        let start = std::time::Instant::now();
        let filed = Entity::new()?;
        {
            let branch = self
                .repo
                .branch(&self.branch)
                .open()
                .perform(&self.operator)
                .await?;
            branch
                .transaction()
                .assert(Bug {
                    this: filed,
                    status: bug::Status("triage".to_string()),
                    priority: bug::Priority("high".to_string()),
                    assignee: bug::Assignee(String::new()),
                    title: bug::Title("A newly filed bug".to_string()),
                    ordering: bug::Ordering(count as f64 * 1000.0),
                })
                .commit()
                .perform(&self.operator)
                .await?;
        }
        eprintln!("BUGBENCH {:<22} time={:?}", "file-bug", start.elapsed());

        let start = std::time::Instant::now();
        self.update_bug_status(&entities[3], "done").await?;
        eprintln!("BUGBENCH {:<22} time={:?}", "close-bug", start.elapsed());

        let start = std::time::Instant::now();
        self.reassign_bug(&entities[5], "did:key:zNewAssignee", "in-progress")
            .await?;
        eprintln!("BUGBENCH {:<22} time={:?}", "reassign-bug", start.elapsed());

        eprintln!("BUGBENCH seeded {count} bugs in {seed_elapsed:?}");
        Ok(())
    }

    /// Seed a realistic bug-tracker fact base: `count` bugs, each a seven-fact
    /// [`Bug`] record, with status/priority/assignee drawn from the same
    /// distribution as the real tonk data (mostly-done/triage, medium priority,
    /// a few assignees plus many unassigned). Returns the seeded entities so a
    /// transaction benchmark can update specific bugs. Off the measured path.
    pub async fn seed_bugs(&self, count: usize) -> Result<Vec<Entity>> {
        const STATUSES: &[&str] = &["done", "triage", "todo", "canceled", "in-progress"];
        const PRIORITIES: &[&str] = &["medium", "high", "low", "urgent"];
        const ASSIGNEES: &[&str] = &[
            "",
            "did:key:z6MkDQtgLHmp664Wf8wn32G9MT79GpKncnQkcJmLYYu6HEJz",
            "did:key:z6MkAoFSTzm7XMv6wc1X9H5iND4YSfEaHw2LYWiTR2xDPfu8",
            "did:key:z6MkGSesqrS3iyekKGrhMCmHyp82RxJaohuvnNMmdQXG9kza",
        ];

        let branch = self
            .repo
            .branch(&self.branch)
            .open()
            .perform(&self.operator)
            .await?;

        let mut entities = Vec::with_capacity(count);
        let mut transaction = branch.transaction();
        for index in 0..count {
            let entity = Entity::new()?;
            entities.push(entity.clone());
            transaction = transaction.assert(Bug {
                this: entity,
                status: bug::Status(STATUSES[index % STATUSES.len()].to_string()),
                priority: bug::Priority(PRIORITIES[index % PRIORITIES.len()].to_string()),
                assignee: bug::Assignee(ASSIGNEES[index % ASSIGNEES.len()].to_string()),
                title: bug::Title(format!("Bug #{index}: something is off")),
                ordering: bug::Ordering(index as f64 * 1000.0),
            });
        }
        transaction.commit().perform(&self.operator).await?;
        Ok(entities)
    }

    /// Run the public [`Bug`] concept query, optionally pinning `status` to a
    /// constant (the "bugs with status X" query the app issues; `None` leaves
    /// status free for an all-bugs join). Reports the join's block reads.
    pub async fn query_bugs_by_status(&self, status: Option<&str>) -> Result<JoinRun> {
        let branch = self
            .repo
            .branch(&self.branch)
            .load()
            .perform(&self.operator)
            .await?;

        let env = JoinEnv {
            branch: &branch,
            operator: &self.operator,
            rules: RuleRegistry::new(),
            journal: ReadJournal::default(),
        };

        let status_term = match status {
            Some(value) => Term::from(value.to_string()),
            None => Term::var("status"),
        };

        env.journal().clear();
        let results = Query::<Bug> {
            this: Term::var("this"),
            status: status_term,
            priority: Term::var("priority"),
            assignee: Term::var("assignee"),
            title: Term::var("title"),
            ordering: Term::var("ordering"),
        }
        .perform(&env)
        .try_vec()
        .await?;

        Ok(JoinRun {
            results_len: results.len(),
            reads: env.journal().reads(),
            unique_reads: env.journal().unique_reads(),
        })
    }

    /// Update a bug's status (the "close a bug" transaction): assert a new
    /// status on the entity. Cardinality-one means the assertion supersedes the
    /// prior status. Returns the committed revision hash's presence.
    pub async fn update_bug_status(&self, entity: &Entity, status: &str) -> Result<()> {
        let branch = self
            .repo
            .branch(&self.branch)
            .open()
            .perform(&self.operator)
            .await?;
        branch
            .transaction()
            .assert(
                ::dialog_query::the!("squash.bug/status")
                    .of(entity.clone())
                    .is(status.to_string()),
            )
            .commit()
            .perform(&self.operator)
            .await?;
        Ok(())
    }

    /// Reassign a bug and set its status in one transaction (the "update
    /// assignee + status" flow).
    pub async fn reassign_bug(&self, entity: &Entity, assignee: &str, status: &str) -> Result<()> {
        let branch = self
            .repo
            .branch(&self.branch)
            .open()
            .perform(&self.operator)
            .await?;
        branch
            .transaction()
            .assert(
                ::dialog_query::the!("squash.bug/assignee")
                    .of(entity.clone())
                    .is(assignee.to_string()),
            )
            .assert(
                ::dialog_query::the!("squash.bug/status")
                    .of(entity.clone())
                    .is(status.to_string()),
            )
            .commit()
            .perform(&self.operator)
            .await?;
        Ok(())
    }

    /// per bug, then a status change and a reassignment on a share of them.
    ///
    /// The single-transaction [`seed_bugs`](Self::seed_bugs) leaves a history
    /// two commits deep, which makes every per-commit history cost (skip-table
    /// construction, ancestry walks, context derivation) look free because
    /// there is no ancestry to walk. A tracker that has seen a few hundred
    /// edits has a few hundred revisions, and those costs grow with depth.
    pub async fn seed_bugs_staged(&self, count: usize) -> Result<Vec<Entity>> {
        const STATUSES: &[&str] = &["done", "triage", "todo", "canceled", "in-progress"];
        const PRIORITIES: &[&str] = &["medium", "high", "low", "urgent"];
        const ASSIGNEES: &[&str] = &[
            "",
            "did:key:z6MkDQtgLHmp664Wf8wn32G9MT79GpKncnQkcJmLYYu6HEJz",
            "did:key:z6MkAoFSTzm7XMv6wc1X9H5iND4YSfEaHw2LYWiTR2xDPfu8",
            "did:key:z6MkGSesqrS3iyekKGrhMCmHyp82RxJaohuvnNMmdQXG9kza",
        ];

        let mut entities = Vec::with_capacity(count);

        // One commit per filed bug.
        for index in 0..count {
            let entity = Entity::new()?;
            entities.push(entity.clone());
            let branch = self
                .repo
                .branch(&self.branch)
                .open()
                .perform(&self.operator)
                .await?;
            branch
                .transaction()
                .assert(Bug {
                    this: entity,
                    status: bug::Status(STATUSES[index % STATUSES.len()].to_string()),
                    priority: bug::Priority(PRIORITIES[index % PRIORITIES.len()].to_string()),
                    assignee: bug::Assignee(ASSIGNEES[index % ASSIGNEES.len()].to_string()),
                    title: bug::Title(format!("Bug #{index}: something is off")),
                    ordering: bug::Ordering(index as f64 * 1000.0),
                })
                .commit()
                .perform(&self.operator)
                .await?;
        }

        // A status change on every third bug, and a reassignment on every
        // fifth: the edits a tracker actually accumulates after filing.
        for (index, entity) in entities.iter().enumerate() {
            if index % 3 == 0 {
                self.update_bug_status(entity, "in-progress").await?;
            }
            if index % 5 == 0 {
                self.reassign_bug(entity, ASSIGNEES[1], "todo").await?;
            }
        }

        Ok(entities)
    }

    /// Commit `depth` times in sequence, reporting how per-commit cost grows
    /// with history depth.
    ///
    /// Skip tables grow as log2(depth), so the per-commit history work (skip
    /// construction, ancestry lookups) grows with the log of how much history
    /// precedes the commit. A benchmark seeded in one transaction cannot see
    /// this at all; this walks the curve directly.
    pub async fn probe_commit_depth(&self, depth: usize) -> Result<Vec<(usize, u128)>> {
        let mut samples = Vec::new();
        let mut window = Instant::now();
        // Reuse one branch handle across commits when asked, so the
        // branch-owned record/node memos survive: this isolates how much
        // of the depth curve is cold skip-table fetches.
        let reuse = env::var("DIALOG_REUSE_BRANCH").is_ok();
        let mut held: Option<_> = None;
        for index in 0..depth {
            let entity = Entity::new()?;
            if held.is_none() || !reuse {
                held = Some(
                    self.repo
                        .branch(&self.branch)
                        .open()
                        .perform(&self.operator)
                        .await?,
                );
            }
            let branch = held.as_ref().expect("branch handle");
            branch
                .transaction()
                .assert(Bug {
                    this: entity,
                    status: bug::Status("triage".to_string()),
                    priority: bug::Priority("medium".to_string()),
                    assignee: bug::Assignee(String::new()),
                    title: bug::Title(format!("Bug #{index}")),
                    ordering: bug::Ordering(index as f64),
                })
                .commit()
                .perform(&self.operator)
                .await?;

            // Sample the average commit cost over each power-of-two window, so
            // the curve is read at the depths where a skip level is added.
            let at = index + 1;
            if at.is_power_of_two() && at >= 8 {
                let span = window.elapsed().as_micros();
                samples.push((at, span / (at as u128 / 2)));
                window = Instant::now();
            }
        }
        Ok(samples)
    }

    /// Open the repository under `profile` and assemble the environment.
    async fn assemble(operator: Env, profile: &Profile) -> Result<Self> {
        let repo = profile
            .repository(unique_name("repo"))
            .open()
            .perform(&operator)
            .await?;
        Ok(Self {
            operator,
            repo,
            branch: "main".to_string(),
        })
    }
}

impl BenchEnv<Operator<VolatileSpace>> {
    async fn with_storage(storage: Storage<VolatileSpace>) -> Result<Self> {
        let profile = Profile::open(unique_name("bench"))
            .perform(&storage)
            .await?;
        let operator = profile
            .derive(b"bench")
            .allow(Subject::any())
            .network(Network::default())
            .build(storage)
            .await?;
        Self::assemble(operator, &profile).await
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl BenchEnv<Operator<NativeTempSpace>> {
    async fn with_storage(storage: Storage<NativeTempSpace>) -> Result<Self> {
        let profile = Profile::open(unique_name("bench"))
            .perform(&storage)
            .await?;
        let operator = profile
            .derive(b"bench")
            .allow(Subject::any())
            .network(Network::default())
            .build(storage)
            .await?;
        Self::assemble(operator, &profile).await
    }
}

#[cfg(all(target_arch = "wasm32", feature = "browser-bench"))]
impl BenchEnv<Operator<::dialog_storage::provider::storage::WebSpace>> {
    async fn with_storage(
        storage: Storage<::dialog_storage::provider::storage::WebSpace>,
    ) -> Result<Self> {
        let profile = Profile::open(unique_name("bench"))
            .perform(&storage)
            .await?;
        let operator = profile
            .derive(b"bench")
            .allow(Subject::any())
            .network(Network::default())
            .build(storage)
            .await?;
        Self::assemble(operator, &profile).await
    }
}

#[cfg(test)]
mod test {

    /// How does per-commit cost grow with history depth? `#[ignore]`d unless
    /// `DIALOG_DEPTH_BENCH` is set.
    #[cfg_attr(not(target_arch = "wasm32"), tokio::test)]
    #[cfg(not(target_arch = "wasm32"))]
    #[allow(clippy::absolute_paths)]
    async fn it_probes_commit_depth() -> Result<()> {
        if env::var("DIALOG_DEPTH_BENCH").is_err() {
            return Ok(());
        }
        let depth: usize = env::var("DIALOG_DEPTH")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(2048);
        let _trace = crate::helpers::trace_phases();
        let env = BenchEnv::temp().await?;
        for (at, per_commit) in env.probe_commit_depth(depth).await? {
            let levels = (at as f64).log2().floor() as u32;
            eprintln!(
                "DEPTHBENCH depth={at:<7} skip_levels~={levels:<3} per_commit={per_commit}us"
            );
        }
        Ok(())
    }

    // A gated, on-demand benchmark: fully-qualified std paths keep it
    // self-contained without adding imports the rest of the module doesn't use.
    #[cfg_attr(not(target_arch = "wasm32"), tokio::test)]
    #[cfg(not(target_arch = "wasm32"))]
    #[allow(clippy::absolute_paths)]
    async fn it_benchmarks_bug_tracker() -> Result<()> {
        if env::var("DIALOG_BUG_BENCH").is_err() {
            eprintln!("DIALOG_BUG_BENCH not set; skipping bug-tracker benchmark");
            return Ok(());
        }
        let count: usize = env::var("DIALOG_BUG_COUNT")
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(300);

        // On-disk is the realistic case: in-memory hides read cost, since
        // extra reads become near-free RAM hits instead of real I/O.
        let on_disk = env::var("DIALOG_BUG_DISK").is_ok();
        let seed_start = Instant::now();
        // The two arms differ only in storage; each has its own concrete
        // `BenchEnv` type, so the shared body below is generic over the
        // provider rather than boxed.
        let staged = env::var("DIALOG_BUG_STAGED").is_ok();
        if on_disk {
            let env = BenchEnv::temp().await?;
            let entities = if staged {
                env.seed_bugs_staged(count).await?
            } else {
                env.seed_bugs(count).await?
            };
            return run_bug_bench_body(env, entities, count, seed_start).await;
        }
        let env = BenchEnv::volatile().await?;
        let entities = if staged {
            env.seed_bugs_staged(count).await?
        } else {
            env.seed_bugs(count).await?
        };
        run_bug_bench_body(env, entities, count, seed_start).await
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[allow(clippy::absolute_paths)]
    async fn run_bug_bench_body<Env>(
        env: BenchEnv<Env>,
        entities: Vec<Entity>,
        count: usize,
        seed_start: Instant,
    ) -> Result<()>
    where
        Env: Provider<Get>
            + Provider<Put>
            + Provider<Import>
            + Provider<Resolve>
            + Provider<Publish>
            + Provider<Identify>
            + Provider<Attest>
            + Provider<SpaceLoad>
            + Provider<SpaceCreate>
            + Provider<Fork<RemoteSite, Get>>
            + Provider<Fork<RemoteSite, Resolve>>
            + ConditionalSync
            + 'static,
    {
        let seed_elapsed = seed_start.elapsed();

        let bench = |label: &'static str, run: JoinRun, elapsed: std::time::Duration| {
            eprintln!(
                "BUGBENCH {label:<22} results={:<5} reads={:<6} unique_reads={:<5} time={elapsed:?}",
                run.results_len, run.reads, run.unique_reads
            );
        };

        // Query: all bugs (the board view — a full six-field concept join).
        let start = Instant::now();
        let all = env.query_bugs_by_status(None).await?;
        bench("all-bugs", all, start.elapsed());

        // Query: bugs with status = done (a common filter).
        let start = Instant::now();
        let done = env.query_bugs_by_status(Some("done")).await?;
        bench("status=done", done, start.elapsed());

        // Query: bugs with status = triage (the "open" board column).
        let start = Instant::now();
        let triage = env.query_bugs_by_status(Some("triage")).await?;
        bench("status=triage", triage, start.elapsed());

        // Transaction: file a new bug (a whole seven-fact record).
        let start = Instant::now();
        let filed = Entity::new()?;
        {
            let branch = env
                .repo
                .branch(&env.branch)
                .open()
                .perform(&env.operator)
                .await?;
            branch
                .transaction()
                .assert(Bug {
                    this: filed.clone(),
                    status: bug::Status("triage".to_string()),
                    priority: bug::Priority("high".to_string()),
                    assignee: bug::Assignee(String::new()),
                    title: bug::Title("A newly filed bug".to_string()),
                    ordering: bug::Ordering(count as f64 * 1000.0),
                })
                .commit()
                .perform(&env.operator)
                .await?;
        }
        eprintln!("BUGBENCH {:<22} time={:?}", "file-bug", start.elapsed());

        // Transaction: close a bug (supersede its status).
        let start = Instant::now();
        env.update_bug_status(&entities[3], "done").await?;
        eprintln!("BUGBENCH {:<22} time={:?}", "close-bug", start.elapsed());

        // Transaction: reassign + set status.
        let start = Instant::now();
        env.reassign_bug(&entities[5], "did:key:zNewAssignee", "in-progress")
            .await?;
        eprintln!("BUGBENCH {:<22} time={:?}", "reassign-bug", start.elapsed());

        eprintln!("BUGBENCH seeded {count} bugs in {seed_elapsed:?}");
        Ok(())
    }

    use super::*;

    #[cfg(target_arch = "wasm32")]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    /// Bug-tracker concept queries over the real browser backend
    /// (IndexedDB). Reads are async IndexedDB round-trips, so this reports the
    /// block-read counts — the deterministic signal the format change moves
    /// (each avoided read is an avoided store round-trip). A smaller bug count
    /// keeps the headless-browser run quick.
    ///
    /// Behind the `browser-bench` feature (and wasm only), so it is NOT part of
    /// the default web test suite / CI. Run it on demand:
    /// `wasm-pack test --headless --chrome --lib --features helpers,browser-bench`.
    #[cfg(all(target_arch = "wasm32", feature = "browser-bench"))]
    #[dialog_common::test]
    async fn it_queries_bugs_on_indexeddb() -> Result<()> {
        let env = BenchEnv::web().await?;
        env.seed_bugs(60).await?;

        let all = env.query_bugs_by_status(None).await?;
        assert_eq!(all.results_len, 60, "every bug joins over IndexedDB");
        assert!(all.reads > 0);
        assert_eq!(all.reads, all.unique_reads, "no redundant reads");
        wasm_bindgen_test::console_log!(
            "IDBBENCH all-bugs results={} reads={} unique_reads={}",
            all.results_len,
            all.reads,
            all.unique_reads
        );

        let done = env.query_bugs_by_status(Some("done")).await?;
        wasm_bindgen_test::console_log!(
            "IDBBENCH status=done results={} reads={} unique_reads={}",
            done.results_len,
            done.reads,
            done.unique_reads
        );
        Ok(())
    }

    #[dialog_common::test]
    async fn it_runs_attribute_query_with_non_zero_reads() -> Result<()> {
        let env = BenchEnv::volatile().await?;
        env.seed(50).await?;

        let run = env.run_query("item/name").await?;
        // `generate_data` emits one `item/name` fact per entity.
        assert_eq!(run.len(), 50);
        // A non-empty branch scan must fetch at least the root node.
        assert!(run.reads > 0, "expected non-zero reads, got {}", run.reads);
        assert!(run.unique_reads > 0);
        assert!(run.unique_reads <= run.reads);
        Ok(())
    }

    #[dialog_common::test]
    async fn it_runs_join_query_with_aggregated_reads() -> Result<()> {
        let env = BenchEnv::volatile().await?;

        let run = env.run_join(50).await?;
        // Every seeded `Stuff` entity matches the concept query exactly once.
        assert_eq!(run.results_len, 50);
        // A two-premise join must fetch blocks across both premise scans.
        assert!(run.reads > 0, "expected non-zero reads, got {}", run.reads);
        assert!(run.unique_reads > 0);
        assert!(run.unique_reads <= run.reads);
        Ok(())
    }

    /// A realistic bug-tracker concept query, over a fact base large enough to
    /// span multiple leaves, must both round-trip and filter correctly. The
    /// [`Bug`] concept joins six attributes including a `Float` `ordering`
    /// field, so this is the concept-layer analogue of the artifact-layer float
    /// key-width regression: before that fix, committing enough float-valued
    /// bugs to fill a leaf failed outright, and no bug query could run.
    #[dialog_common::test]
    async fn it_queries_bugs_including_a_float_field() -> Result<()> {
        let env = BenchEnv::volatile().await?;
        // Enough bugs to force the index past a single leaf, so the float
        // `ordering` keys must re-split correctly inside a shared leaf.
        env.seed_bugs(300).await?;

        // All bugs (status free): every seeded bug is a full six-field record,
        // so each joins exactly once.
        let all = env.query_bugs_by_status(None).await?;
        assert_eq!(all.results_len, 300, "every bug joins across all fields");

        // Bugs with a specific status: the seeding cycles five statuses, so
        // "done" is every fifth bug.
        let done = env.query_bugs_by_status(Some("done")).await?;
        assert_eq!(done.results_len, 300 / 5, "one fifth of bugs are done");
        assert!(done.reads > 0);

        Ok(())
    }

    /// On-demand benchmark of the real bug-tracker workload against the
    /// **imported** tonk data on the **disk** backend. Skipped unless
    /// `DIALOG_BUG_CSV` points at a `tonk export` CSV. Imports every real
    /// `squash.bug/*` record, then times and reports (`REALBUG` lines) the
    /// exact query and transaction shapes the app issues:
    ///
    /// Queries: open issues (todo+triage+in-progress), closed issues
    /// (done+canceled), and issues assigned to a specific member (a join
    /// pinning the assignee). Transactions: change a bug's status, assign a
    /// bug to a member, and create a new issue with a specific description.
    ///
    /// Run on this revision and on `main` to compare formats on the real
    /// data. Native only (reads a file, uses the disk backend).
    #[cfg_attr(not(target_arch = "wasm32"), tokio::test)]
    #[cfg(not(target_arch = "wasm32"))]
    #[allow(clippy::absolute_paths)]
    async fn it_benchmarks_real_bug_workload() -> Result<()> {
        let Ok(csv_path) = std::env::var("DIALOG_BUG_CSV") else {
            eprintln!("DIALOG_BUG_CSV not set; skipping real bug workload benchmark");
            return Ok(());
        };

        let env = BenchEnv::temp().await?;
        let index = env.import_bugs_from_csv(&csv_path).await?;
        eprintln!("REALBUG imported {} bugs (disk backend)", index.len());

        // Pick the member with the most assigned bugs for the assignee join.
        let mut per_assignee: std::collections::HashMap<String, usize> =
            std::collections::HashMap::new();
        for (_, _, assignee) in &index {
            if !assignee.is_empty() {
                *per_assignee.entry(assignee.clone()).or_default() += 1;
            }
        }
        let top_assignee = per_assignee
            .into_iter()
            .max_by_key(|(_, count)| *count)
            .map(|(did, _)| did)
            .unwrap_or_default();

        let report = |label: &str, run: &JoinRun, elapsed: std::time::Duration| {
            eprintln!(
                "REALBUG {label:<28} results={:<4} reads={:<5} unique_reads={:<4} time={elapsed:?}",
                run.results_len, run.reads, run.unique_reads
            );
        };

        // Query: open issues.
        let start = std::time::Instant::now();
        let open = env
            .query_bugs_in_statuses(&["todo", "triage", "in-progress"])
            .await?;
        report("open-issues", &open, start.elapsed());

        // Query: closed issues.
        let start = std::time::Instant::now();
        let closed = env.query_bugs_in_statuses(&["done", "canceled"]).await?;
        report("closed-issues", &closed, start.elapsed());

        // Query: issues assigned to member X (join pinning the assignee).
        let start = std::time::Instant::now();
        let assigned = env.query_bugs(None, Some(&top_assignee)).await?;
        report("assigned-to-member", &assigned, start.elapsed());

        // Transaction: change a bug's status (close it).
        let target = index[3].0.clone();
        let start = std::time::Instant::now();
        env.update_bug_status(&target, "done").await?;
        eprintln!(
            "REALBUG {:<28} time={:?}",
            "txn-change-status",
            start.elapsed()
        );

        // Transaction: assign a bug to a member.
        let start = std::time::Instant::now();
        env.reassign_bug(&index[5].0, &top_assignee, "in-progress")
            .await?;
        eprintln!("REALBUG {:<28} time={:?}", "txn-assign", start.elapsed());

        // Transaction: create a new issue with a specific description.
        let detail = "A newly filed issue with a specific, realistic multi-sentence \
             description that spells out the reproduction steps, the expected \
             behavior, and the observed behavior in enough detail to be useful."
            .to_string();
        let start = std::time::Instant::now();
        env.create_bug(NewBug {
            status: "triage",
            priority: "high",
            assignee: &top_assignee,
            title: "Newly created issue",
            ordering: 999_000.0,
            ident: "BUG-NEW",
            detail: &detail,
        })
        .await?;
        eprintln!(
            "REALBUG {:<28} time={:?}",
            "txn-create-issue",
            start.elapsed()
        );

        Ok(())
    }

    /// The bug-tracker transactions round-trip: file a bug (a whole [`Bug`]
    /// record), close it (supersede its status), and reassign it (assignee +
    /// status). Each is the shape the real app commits.
    #[dialog_common::test]
    async fn it_transacts_bug_updates() -> Result<()> {
        let env = BenchEnv::volatile().await?;
        let entities = env.seed_bugs(20).await?;
        let target = entities[3].clone();

        // Close: supersede the status.
        env.update_bug_status(&target, "done").await?;
        // Reassign + set status in one transaction.
        env.reassign_bug(&target, "did:key:zAssignee", "in-progress")
            .await?;

        // The updated bug still joins (all six fields present) and now carries
        // the reassigned status.
        let in_progress = env.query_bugs_by_status(Some("in-progress")).await?;
        assert!(
            in_progress.results_len >= 1,
            "the reassigned bug shows up under its new status"
        );
        Ok(())
    }
}

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

// The query types below are imported through `::dialog_query::…` so the
// same source resolves in both build contexts: in the bench build this file
// is `#[path]`-included into a separate bench crate where `dialog-query` is
// an ordinary dependency, and in the crate's own (test) build the self-alias
// makes `::dialog_query` name the crate itself.
#[cfg(test)]
extern crate self as dialog_query;

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
use dialog_effects::authority::Identify;
use dialog_effects::memory::{Publish, Resolve};
use dialog_effects::space::{Create as SpaceCreate, Load as SpaceLoad};
use dialog_network::Network;
use dialog_operator::helpers::{generate_data, unique_name};
use dialog_operator::{Operator, Profile};
use dialog_repository::{Branch, NetworkedIndex, RemoteSite, Repository, RepositoryExt as _};
use dialog_storage::provider::storage::{Storage, VolatileSpace};
use dialog_storage::{
    Blake3Hash, DialogStorageError, JournaledStorage, NativeTempSpace, StorageBackend,
};
use futures_util::{TryStreamExt as _, stream};
use std::collections::HashSet;
use std::sync::{Arc, Mutex};

use ::dialog_query::concept::query::ConceptRules;
use ::dialog_query::error::EvaluationError;
use ::dialog_query::source::SelectRules;
use ::dialog_query::{
    AttributeDescriptor, AttributeQuery, Cardinality, ConceptDescriptor, DeductiveRule,
    Environment, Match, Parameters, Planner, Premise, RuleRegistry, Term, Type, the,
};

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

// The join concepts are built through the runtime descriptor API rather
// than `#[derive(Concept)]`. The derive expands to bare `dialog_query::…`
// paths, which the crate's own (test) build resolves via the self-alias but
// a separate bench crate cannot (the package can't dev-depend on itself to
// put the lib in its extern prelude as a bare name). The runtime API needs
// only ordinary `::dialog_query::…` imports and produces the same two-premise
// `DeductiveRule` the planner must order.

/// The source concept seeded into the branch: each entity carries a
/// `stuff/name` and a `stuff/role`.
fn stuff_descriptor() -> ConceptDescriptor {
    ConceptDescriptor::try_from([
        (
            "name",
            AttributeDescriptor::new(
                the!("stuff/name"),
                "Name of the stuff member",
                Cardinality::One,
                Some(Type::String),
            ),
        ),
        (
            "role",
            AttributeDescriptor::new(
                the!("stuff/role"),
                "Role of the stuff member",
                Cardinality::One,
                Some(Type::String),
            ),
        ),
    ])
    .expect("stuff descriptor is well-formed")
}

/// The derived concept queried in the benchmark. No `Employee` facts are
/// stored — every result is inferred by the two-premise join over `Stuff`.
fn employee_descriptor() -> ConceptDescriptor {
    ConceptDescriptor::try_from([
        (
            "name",
            AttributeDescriptor::new(
                the!("employee/name"),
                "Employee name",
                Cardinality::One,
                Some(Type::String),
            ),
        ),
        (
            "job",
            AttributeDescriptor::new(
                the!("employee/job"),
                "The job title of the employee",
                Cardinality::One,
                Some(Type::String),
            ),
        ),
    ])
    .expect("employee descriptor is well-formed")
}

/// Build the two-premise rule deriving an employee from stuff.
///
/// Two `AttributeQuery` premises (`stuff/name` then `stuff/role`) joined on
/// the shared `this` entity. With two premises the planner has an ordering
/// choice — the whole reason this query exercises round-trip optimization.
fn employee_from_stuff() -> Result<DeductiveRule> {
    let rule = DeductiveRule::new(
        employee_descriptor(),
        vec![
            AttributeQuery::new(
                Term::from(the!("stuff/name")),
                Term::var("this"),
                Term::var("name"),
                Term::blank(),
                None,
            )
            .into(),
            AttributeQuery::new(
                Term::from(the!("stuff/role")),
                Term::var("this"),
                Term::var("job"),
                Term::blank(),
                None,
            )
            .into(),
        ],
    )?;
    Ok(rule)
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

impl<Env> BenchEnv<Env>
where
    Env: Provider<Get>
        + Provider<Put>
        + Provider<Import>
        + Provider<Resolve>
        + Provider<Publish>
        + Provider<Identify>
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
    /// path; it exists so [`query_employees`](Self::query_employees) has a
    /// fact base the two-premise rule can join over. Exposed so benches can
    /// seed once at setup and time only the query.
    pub async fn seed_stuff(&self, entity_count: usize) -> Result<()> {
        let descriptor = stuff_descriptor();
        let branch = self
            .repo
            .branch(&self.branch)
            .open()
            .perform(&self.operator)
            .await?;

        let mut transaction = branch.transaction();
        for index in 0..entity_count {
            let statement = descriptor
                .create()
                .with("name", format!("name-{index}"))
                .with("role", format!("role-{}", index % 8))
                .build()?;
            transaction = transaction.assert(statement);
        }
        transaction.commit().perform(&self.operator).await?;
        Ok(())
    }

    /// Register the two-premise `employee_from_stuff` join rule, then run
    /// the planned employee concept query and report the block reads of the
    /// *whole* query. Does not seed — the caller seeds via
    /// [`seed_stuff`](Self::seed_stuff) first (so benches can seed once at
    /// setup and time only this query).
    ///
    /// The employee concept is applied with free `name`/`job` parameters
    /// and lowered through the planner — the same operator IR production
    /// uses — so the planner picks the join order. Every premise scan it
    /// issues routes through a [`CountingStore`] over one shared
    /// [`ReadJournal`], so the reported counts aggregate all reads across
    /// the join. The journal is cleared once before evaluation and read
    /// once after, excluding the rule-registry warm-up.
    pub async fn query_employees(&self) -> Result<JoinRun> {
        let mut rules = RuleRegistry::new();
        rules.register(employee_from_stuff()?)?;

        let branch = self
            .repo
            .branch(&self.branch)
            .load()
            .perform(&self.operator)
            .await?;

        let env = JoinEnv {
            branch: &branch,
            operator: &self.operator,
            rules,
            journal: ReadJournal::default(),
        };

        let mut parameters = Parameters::new();
        parameters.insert("name".into(), Term::var("name"));
        parameters.insert("job".into(), Term::var("job"));
        let proposition = employee_descriptor().apply(parameters)?;
        let plan = Planner::from(vec![Premise::Assert(proposition)]).plan(&Environment::new())?;

        env.journal().clear();
        let results: Vec<Match> = plan
            .evaluate(Match::new().seed(), &env)
            .try_collect()
            .await?;

        Ok(JoinRun {
            results_len: results.len(),
            reads: env.journal().reads(),
            unique_reads: env.journal().unique_reads(),
        })
    }

    /// Seed `entity_count` stuff entities and run the planned employee
    /// concept query in one call, reporting the join's block reads.
    ///
    /// Convenience over [`seed_stuff`](Self::seed_stuff) +
    /// [`query_employees`](Self::query_employees) for one-shot reporting.
    pub async fn run_join(&self, entity_count: usize) -> Result<JoinRun> {
        self.seed_stuff(entity_count).await?;
        self.query_employees().await
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

#[cfg(test)]
mod test {
    use super::*;

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
        // Every seeded `Stuff` entity derives exactly one `Employee`.
        assert_eq!(run.results_len, 50);
        // A two-premise join must fetch blocks across both premise scans.
        assert!(run.reads > 0, "expected non-zero reads, got {}", run.reads);
        assert!(run.unique_reads > 0);
        assert!(run.unique_reads <= run.reads);
        Ok(())
    }
}

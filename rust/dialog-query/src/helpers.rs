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
use dialog_effects::authority::Identify;
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
use std::collections::HashSet;
use std::sync::{Arc, Mutex};

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

    /// On-demand realistic bug-tracker benchmark. Skipped unless
    /// `DIALOG_BUG_BENCH` is set. Seeds a bug fact base and reports reads +
    /// wall-clock for the queries the app issues (all bugs, bugs by status,
    /// open bugs) and the transactions (file a bug, close, reassign) — run it
    /// on this revision and the old tag to compare formats on a realistic
    /// concept-join workload. Native only.
    // A gated, on-demand benchmark: fully-qualified std paths keep it
    // self-contained without adding imports the rest of the module doesn't use.
    #[cfg_attr(not(target_arch = "wasm32"), tokio::test)]
    #[cfg(not(target_arch = "wasm32"))]
    #[allow(clippy::absolute_paths)]
    async fn it_benchmarks_bug_tracker() -> Result<()> {
        if std::env::var("DIALOG_BUG_BENCH").is_err() {
            eprintln!("DIALOG_BUG_BENCH not set; skipping bug-tracker benchmark");
            return Ok(());
        }
        let count: usize = std::env::var("DIALOG_BUG_COUNT")
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(300);

        // `DIALOG_BUG_DISK=1` runs against a real on-disk filesystem backend
        // (the realistic case: reads are actual I/O); the default is the
        // in-memory backend (engine-CPU isolation).
        if std::env::var("DIALOG_BUG_DISK").is_ok() {
            eprintln!("BUGBENCH backend=disk");
            BenchEnv::temp().await?.run_bug_bench(count).await
        } else {
            eprintln!("BUGBENCH backend=memory");
            BenchEnv::volatile().await?.run_bug_bench(count).await
        }
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

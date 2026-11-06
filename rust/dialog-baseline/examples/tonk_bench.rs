//! Runs a tonk space's workload from dialog alone, against a store.
//!
//! ```sh
//! cargo run --release -p dialog-baseline --example tonk_bench -- \
//!   dataset.jsonl fs path/to/scratch
//! ```
//!
//! The space's commands are inductive rules it stores, its views' queries
//! are concept queries over descriptors it stores, so neither needs tonk:
//! a command is its facts committed (the space's rules do what it does),
//! a view is its query. The bench replays the dataset's history into the
//! store, installs the space's rules, and measures from there.
//!
//! It uses only what main and this branch share, so the same file builds
//! in a worktree of `origin/main` for the baseline.

#[cfg(not(target_arch = "wasm32"))]
#[path = "tonk/dataset.rs"]
// Shared with `tonk_dataset`, which writes what this reads.
#[allow(dead_code)]
mod dataset;

#[cfg(not(target_arch = "wasm32"))]
fn main() -> anyhow::Result<()> {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?
        .block_on(native::main())
}

#[cfg(target_arch = "wasm32")]
fn main() {}

#[cfg(not(target_arch = "wasm32"))]
mod native {
    use std::collections::BTreeMap;
    use std::path::{Path, PathBuf};
    use std::str::FromStr as _;
    use std::time::Instant;

    use dialog_artifacts::{Changes, Entity, Pick, Relation, Statement, Update, Value};
    use dialog_capability::Subject;
    use dialog_effects::storage::{Directory as At, Location};
    use dialog_peer::helpers::{Seed, open_peer_as, test_owned};
    use dialog_query::query::Output as _;
    use dialog_query::{
        ConceptDescriptor, ConceptQuery, DeductiveRule, InductiveRule, Parameters, Term,
    };
    use dialog_repository::{Branch, RepositoryExt as _, Transient};
    use dialog_storage::provider::storage::NativeSpace;
    use dialog_storage::provider::{FileSystem, Space, Storage};

    use crate::dataset::{self, Change, Line, Op, Phase};

    /// Opens the `main` branch of the bench's repository in a space of
    /// `$space` under `$at`, creating it when asked, with the same keys
    /// every time.
    macro_rules! open {
        ($space:ty, $at:expr, $create:expr) => {{
            let seed = Seed::new(733);
            let storage = test_owned(Storage::<$space>::new()).await;
            let profile = open_peer_as(
                storage,
                Location::new($at.clone(), "peer".to_string()),
                seed.credential("profile").await?,
            )
            .await?;
            let operator = profile
                .session(b"tonk-bench")
                .space(profile.state())
                .allow(Subject::any())
                .await?;
            let space = profile.space("space".to_string());
            let branch = if $create {
                space
                    .create()
                    .with_credential(seed.signer("space").await?)
                    .perform(&operator)
                    .await?
                    .branch("main")
                    .open()
                    .perform(&operator)
                    .await?
            } else {
                space
                    .open()
                    .perform(&operator)
                    .await?
                    .branch("main")
                    .open()
                    .perform(&operator)
                    .await?
            };
            (operator, branch)
        }};
    }

    /// The workload over `$lines`, through `$operator` on `$branch`: a
    /// macro, as each store's operator is a type of its own.
    macro_rules! run {
        ($lines:expr, $operator:expr, $branch:expr) => {{
            let (lines, operator, branch): (&[Line], _, &Branch) = ($lines, &$operator, &$branch);
            let mut concepts: BTreeMap<String, ConceptDescriptor> = BTreeMap::new();
        let mut sources: BTreeMap<String, serde_json::Value> = BTreeMap::new();
            let started = Instant::now();
            let mut commits = 0usize;
            for line in lines {
                match line {
                    Line::Concepts { concepts: found } => {
                        for (name, json) in found {
                            concepts.insert(name.clone(), serde_json::from_value(json.clone())?);
                        sources.insert(name.clone(), json.clone());
                        }
                    }
                    Line::Commit {
                        phase: Phase::Setup,
                        changes,
                    } => {
                        branch
                            .transaction()
                            .integrate(changes_of(changes)?)
                            .commit()
                            .publish()
                            .perform(operator)
                            .await?;
                        commits += 1;
                    }
                    Line::Schema {
                        inductive,
                        deductive,
                        transient,
                    } => {
                        let mut transaction = branch.transaction();
                        for bytes in inductive {
                            transaction = transaction
                                .assert(InductiveRule::decode(bytes).map_err(anyhow::Error::msg)?);
                        }
                        for bytes in deductive {
                            transaction = transaction
                                .assert(DeductiveRule::decode(bytes).map_err(anyhow::Error::msg)?);
                        }
                        for concept in transient {
                            transaction = transaction.assert(Transient(Entity::from_str(concept)?));
                        }
                        transaction.commit().publish().perform(operator).await?;
                    }
                    _ => {}
                }
            }
            eprintln!(
                "replayed {commits} commits and the schema in {:.1}s",
                started.elapsed().as_secs_f64()
            );
            let bug = concepts
                .get("v2/issue")
                .ok_or_else(|| anyhow::anyhow!("no v2/issue concept"))?;
            let started = Instant::now();
            let rows = branch
                .select(query_all(bug))
                .perform(operator)
                .try_vec()
                .await?;
            eprintln!(
                "v2/issue: {} rows in {:.1} ms",
                rows.len(),
                started.elapsed().as_secs_f64() * 1e3
            );
            eprintln!(
            "v2/issue-create is {} in this build",
            concepts["v2/issue-create"].this()
        );
        let titles: ConceptDescriptor = serde_json::from_value(serde_json::json!({
            "with": { "title": { "the": "squash.bug/title", "as": "text:" } }
        }))?;
        let rows = branch.select(query_all(&titles)).perform(operator).try_vec().await?;
        eprintln!("squash.bug/title: {} rows before", rows.len());
        // One command, as tonk's issue page sends it.
        let create = sources
            .get("v2/issue-create")
            .ok_or_else(|| anyhow::anyhow!("no v2/issue-create concept"))?;
        let issue = Entity::from_str("did:key:z6MkhaXgBZDvotDkL5257faiztiGiC2QtKLGpbnnEGta2doK")?;
        let event = Entity::from_str("urn:uuid:5e1f0000-0000-4000-8000-000000000001")?;
        let fields: BTreeMap<&str, Value> = BTreeMap::from([
            ("created", Value::String("2026-11-06T12:00:00.000Z".into())),
            ("creator", Value::Entity(issue.clone())),
            ("id", Value::Entity(issue.clone())),
            ("ident", Value::String("BUG-9001".into())),
            ("ordering", Value::Float(9001.0)),
            ("priority", Value::String("medium".into())),
            ("status", Value::String("open".into())),
            ("time", Value::UnsignedInt(1)),
            ("title", Value::String("A bench issue".into())),
        ]);
        let mut command = Vec::new();
        for (field, value) in fields {
            let the = create["with"][field]["the"]
                .as_str()
                .ok_or_else(|| anyhow::anyhow!("v2/issue-create has no {field}"))?;
            command.push((Relation::from_str(the)?, event.clone(), value));
        }
        let started = Instant::now();
        branch
            .transaction()
            .dispatch(Command(command))
            .commit()
            .publish()
            .perform(operator)
            .await?;
        eprintln!(
            "v2/issue-create committed in {:.1} ms",
            started.elapsed().as_secs_f64() * 1e3
        );
        let rows = branch.select(query_all(bug)).perform(operator).try_vec().await?;
        eprintln!("v2/issue: {} rows after", rows.len());
        let rows = branch.select(query_all(&titles)).perform(operator).try_vec().await?;
        eprintln!("squash.bug/title: {} rows after", rows.len());
        let events: ConceptDescriptor = serde_json::from_value(serde_json::json!({
            "with": { "title": { "the": "dom.event.detail/create-issue-title", "as": "text:" } }
        }))?;
        let rows = branch.select(query_all(&events)).perform(operator).try_vec().await?;
        eprintln!("create-issue events stored: {}", rows.len());
        anyhow::Ok(())
        }};
    }

    pub async fn main() -> anyhow::Result<()> {
        let args: Vec<String> = std::env::args().skip(1).collect();
        let [path, store, scratch] = args.as_slice() else {
            anyhow::bail!("usage: tonk_bench <dataset.jsonl> <store> <scratch dir>");
        };
        let path = Path::new(path);
        let hash = dataset::hash(path)?;
        let lines = dataset::read(path)?;
        let at = At::At(
            PathBuf::from(scratch)
                .join(store)
                .to_string_lossy()
                .into_owned(),
        );
        eprintln!("dataset {hash}, store {store}");
        match store.as_str() {
            "fs" => {
                type Fs = Space<NativeSpace, FileSystem, FileSystem, FileSystem, FileSystem>;
                let (operator, branch) = open!(Fs, at, true);
                run!(&lines, operator, branch)
            }
            other => anyhow::bail!("no store {other}"),
        }
    }

    /// The changes `changes` make, as a transaction integrates them.
    fn changes_of(changes: &[Change]) -> anyhow::Result<Changes> {
        let mut out = Changes::new();
        for change in changes {
            let the = Relation::from_str(&change.the)?;
            let of = Entity::from_str(&change.of)?;
            match change.op {
                Op::Add => out.associate(the, of, change.is.clone(), Pick::All),
                Op::Replace => out.associate(the, of, change.is.clone(), Pick::Last),
                Op::Retract => out.dissociate(the, of, change.is.clone()),
            }
        }
        Ok(out)
    }

    /// A command's facts, dispatched as a transient: visible to the
    /// commit's rules, never committed, as tonk sends a command.
    struct Command(Vec<(Relation, Entity, Value)>);

    impl Statement for Command {
        fn assert(self, update: &mut impl Update) {
            for (the, of, is) in self.0 {
                update.associate(the, of, is, Pick::Last);
            }
        }

        fn retract(self, update: &mut impl Update) {
            for (the, of, is) in self.0 {
                update.dissociate(the, of, is);
            }
        }
    }

    /// A query of `descriptor` with every field a variable of its name.
    fn query_all(descriptor: &ConceptDescriptor) -> ConceptQuery {
        let mut terms = Parameters::new();
        terms.insert("this".into(), Term::var("this"));
        for field in descriptor.with().keys() {
            terms.insert(field.to_string(), Term::var(field.to_string()));
        }
        ConceptQuery {
            predicate: descriptor.clone(),
            terms,
        }
    }
}

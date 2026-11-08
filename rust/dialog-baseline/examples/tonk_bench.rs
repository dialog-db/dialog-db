//! Runs a tonk space's workload from dialog alone, against a store.
//!
//! ```sh
//! cargo run --release -p dialog-baseline --example tonk_bench -- \
//!   dataset.jsonl fs path/to/scratch
//! ```
//!
//! The space's commands are inductive rules it stores, its views' queries
//! are concept queries over descriptors it stores, so neither needs tonk:
//! a command is its facts dispatched (the space's rules do what it does),
//! a view is its query.
//!
//! A run is a team: the dataset's members, each a profile and a replica of
//! their own on the store under test, sharing a remote (a directory, the
//! same for every store). Member 0 owns the space: it builds it from the
//! dataset's setup and pushes it; every other member joins by pulling.
//! Then the week: in each round every member commits its commands on its
//! own replica, then each in turn pulls what the others pushed and pushes
//! its own, so pulls merge concurrent histories, and a member's first
//! commit after a pull runs the space's rules over what it pulled in.
//! Member 1 keeps the views subscribed and polls them after each of its
//! pulls, as an open page would.
//!
//! It uses only what main and this branch share, so the same file builds
//! in a worktree of `origin/main` for the baseline (`--cfg dialog_main`
//! leaves out the stores main does not have).

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
    use std::time::{Duration, Instant};

    use anyhow::Context as _;
    use dialog_artifacts::{Changes, Entity, Pick, Relation, Statement, Update, Value};
    use dialog_capability::Subject;
    use dialog_credentials::{Credential, SignerCredential};
    use dialog_effects::credential::prelude::*;
    use dialog_effects::storage::{Directory, Location};
    use dialog_peer::helpers::{Seed, open_peer_as, test_owned};
    use dialog_peer::{Peer, PeerSpace, Session};
    use dialog_query::query::Output as _;
    use dialog_query::{
        ConceptConclusion, ConceptDescriptor, ConceptQuery, DeductiveRule, InductiveRule,
        Parameters, Term,
    };
    use dialog_remote_fs::FsAddress;
    use dialog_repository::{
        Branch, Repository, RepositoryExt as _, SiteAddress, Subscription, Transient, contact,
    };
    use dialog_storage::provider::storage::NativeSpace;
    use dialog_storage::provider::{FileSystem, Space, Storage};
    use dialog_storage::resource::Resource as _;
    use dialog_varsig::{Did, Principal};

    use crate::dataset::{self, Change, Line, Op, Phase};

    /// How many setup commands share a commit: one at a time, a 100×
    /// space would take hours to build per store. The measured week sends
    /// one command per commit, as tonk does.
    const SETUP_BATCH: usize = 32;

    /// The views a member keeps open: the issues page's models and the
    /// memos page's.
    const VIEWS: [&str; 8] = [
        "v2/issue",
        "v2/issue-labels",
        "v2/label",
        "v2/project",
        "v2/person",
        "v2/activity",
        "memo/full",
        "memo/byline",
    ];

    type Fs = NativeSpace;
    #[cfg(not(dialog_main))]
    type File =
        Space<dialog_archive_file::FileArchive, FileSystem, FileSystem, FileSystem, FileSystem>;

    pub async fn main() -> anyhow::Result<()> {
        let args: Vec<String> = std::env::args().skip(1).collect();
        let [path, store, scratch] = args.as_slice() else {
            anyhow::bail!("usage: tonk_bench <dataset.jsonl> <store> <scratch dir>");
        };
        let path = Path::new(path);
        let hash = dataset::hash(path)?;
        let lines = dataset::read(path)?;
        let root = PathBuf::from(scratch).join(store);
        if root.exists() {
            std::fs::remove_dir_all(&root)?;
        }
        std::fs::create_dir_all(&root)?;
        eprintln!("dataset {hash}, store {store}");
        let report = match store.as_str() {
            "fs" => run::<Fs>(&lines, &root, false).await?,
            #[cfg(not(dialog_main))]
            "file" => run::<File>(&lines, &root, true).await?,
            #[cfg(not(dialog_main))]
            "file-blocks" => run::<File>(&lines, &root, false).await?,
            other => anyhow::bail!("no store {other}"),
        };
        let out = root.join("report.json");
        std::fs::write(
            &out,
            serde_json::to_vec_pretty(&serde_json::json!({
                "dataset": hash,
                "store": store,
                "build": if cfg!(dialog_main) { "main" } else { "branch" },
                "measures": report,
            }))?,
        )?;
        eprintln!("wrote {}", out.display());
        Ok(())
    }

    /// Durations by measure, in milliseconds, and counts.
    #[derive(Default, serde::Serialize)]
    struct Report {
        timings: BTreeMap<String, Vec<f64>>,
        counts: BTreeMap<String, u64>,
    }

    impl Report {
        fn time(&mut self, name: &str, took: Duration) {
            self.timings
                .entry(name.to_string())
                .or_default()
                .push(took.as_secs_f64() * 1e3);
        }

        fn count(&mut self, name: &str, value: u64) {
            self.counts.insert(name.to_string(), value);
        }

        fn summary(&self) {
            for (name, samples) in &self.timings {
                let mut sorted = samples.clone();
                sorted.sort_by(f64::total_cmp);
                let median = sorted[sorted.len() / 2];
                let p95 = sorted[(sorted.len() * 95 / 100).min(sorted.len() - 1)];
                let total: f64 = sorted.iter().sum();
                eprintln!(
                    "{name:40} n={:<5} median {median:9.2} ms  p95 {p95:9.2} ms  total {total:10.1} ms",
                    sorted.len()
                );
            }
            for (name, value) in &self.counts {
                eprintln!("{name:40} {value}");
            }
        }
    }

    /// A member of the team: a profile, a session of it, and the replica
    /// of the space it writes to.
    struct Member<S: PeerSpace> {
        profile: Peer<S>,
        operator: Peer<S, Session>,
        branch: Branch,
        /// Where its replica keeps its storage.
        home: PathBuf,
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

    /// Dataset entities that stand for members' profiles, made real.
    struct Profiles(Vec<String>);

    impl Profiles {
        fn entity(&self, text: &str) -> anyhow::Result<Entity> {
            match dataset::member(text) {
                Some(n) => Ok(Entity::from_str(&self.0[n as usize])?),
                None => Ok(Entity::from_str(text)?),
            }
        }

        fn value(&self, value: &Value) -> anyhow::Result<Value> {
            Ok(match value {
                Value::Entity(entity) => Value::Entity(self.entity(&entity.to_string())?),
                Value::String(text) if dataset::member(text).is_some() => {
                    Value::String(self.entity(text)?.to_string())
                }
                other => other.clone(),
            })
        }

        fn changes(&self, changes: &[Change], into: &mut Changes) -> anyhow::Result<()> {
            for change in changes {
                let the = Relation::from_str(&change.the)?;
                let of = self.entity(&change.of)?;
                let is = self.value(&change.is)?;
                match change.op {
                    Op::Add => into.associate(the, of, is, Pick::All),
                    Op::Replace => into.associate(the, of, is, Pick::Last),
                    Op::Retract => into.dissociate(the, of, is),
                }
            }
            Ok(())
        }

        fn command(&self, changes: &[Change]) -> anyhow::Result<Command> {
            changes
                .iter()
                .map(|change| {
                    Ok((
                        Relation::from_str(&change.the)?,
                        self.entity(&change.of)?,
                        self.value(&change.is)?,
                    ))
                })
                .collect::<anyhow::Result<_>>()
                .map(Command)
        }
    }

    /// A query of `descriptor` with every field a variable of its name,
    /// and `bound` fields bound to constants.
    fn query(descriptor: &ConceptDescriptor, bound: &[(&str, Entity)]) -> ConceptQuery {
        let mut terms = Parameters::new();
        terms.insert("this".into(), Term::var("this"));
        for field in descriptor.with().keys() {
            terms.insert(field.to_string(), Term::var(field.to_string()));
        }
        for (field, value) in bound {
            terms.insert((*field).into(), Term::constant(value.clone()));
        }
        ConceptQuery {
            predicate: descriptor.clone(),
            terms,
        }
    }

    fn bytes_under(dir: &Path) -> u64 {
        let mut bytes = 0;
        let mut pending = vec![dir.to_path_buf()];
        while let Some(next) = pending.pop() {
            let Ok(entries) = std::fs::read_dir(&next) else {
                continue;
            };
            for entry in entries.flatten() {
                let path = entry.path();
                if path.is_dir() {
                    pending.push(path);
                } else if let Ok(meta) = entry.metadata() {
                    bytes += meta.len();
                }
            }
        }
        bytes
    }

    async fn open_member<S: PeerSpace>(
        seed: &Seed,
        root: &Path,
        n: usize,
    ) -> anyhow::Result<(Peer<S>, Peer<S, Session>, PathBuf)> {
        let home = root.join(format!("member-{n}"));
        std::fs::create_dir_all(&home)?;
        let storage = test_owned(Storage::<S>::new()).await;
        let profile = open_peer_as(
            storage,
            Location::new(
                Directory::At(home.to_string_lossy().into_owned()),
                "peer".to_string(),
            ),
            seed.credential(&format!("member-{n}")).await?,
        )
        .await?;
        let operator = profile
            .session(b"tonk-bench")
            .space(profile.state())
            .allow(Subject::any())
            .await?;
        Ok((profile, operator, home))
    }

    /// Makes the archive at `file` the node source of `branch`'s subject in
    /// `operator`'s environment, so its trees read nodes from the file's
    /// rows in place rather than from blocks built back from them. A second
    /// handle on the same file, as the archive's own tests open one: nothing
    /// in a peer registers it yet.
    #[cfg(not(dialog_main))]
    fn serve_in_place(
        operator: &impl dialog_common::Holds,
        branch: &Branch,
        file: &Path,
    ) -> anyhow::Result<()> {
        let archive: std::sync::Arc<dyn dialog_search_tree::NodeSource> =
            std::sync::Arc::new(dialog_archive_file::FileArchive::open(file)?);
        <dyn dialog_search_tree::NodeSource>::serve(
            operator,
            branch.subject().did(),
            archive.clone(),
        );
        // Queries, subscriptions and the rules a commit runs read through
        // the query session, under the wildcard subject: register the file
        // there too. Right for this bench, where each member's peer holds
        // one space; a peer holding several could not (dialog-db/dialog-db#753
        // plans the reads that make this unnecessary).
        <dyn dialog_search_tree::NodeSource>::serve(
            operator,
            dialog_capability::Subject::any().did(),
            archive,
        );
        Ok(())
    }

    #[cfg(dialog_main)]
    fn serve_in_place(_: &impl dialog_common::Holds, _: &Branch, _: &Path) -> anyhow::Result<()> {
        anyhow::bail!("main has no node source")
    }

    /// The workload over `lines`, every member's replica on a space of `S`,
    /// its tree reading nodes in place when `serve` says so.
    async fn run<S: PeerSpace>(lines: &[Line], root: &Path, serve: bool) -> anyhow::Result<Report> {
        let mut report = Report::default();
        let seed = Seed::new(733);
        let count = lines
            .iter()
            .find_map(|line| match line {
                Line::Members { count } => Some(*count as usize),
                _ => None,
            })
            .unwrap_or(1)
            .max(2);
        let mut concepts: BTreeMap<String, ConceptDescriptor> = BTreeMap::new();
        for line in lines {
            if let Line::Concepts { concepts: found } = line {
                for (name, json) in found {
                    concepts.insert(name.clone(), serde_json::from_value(json.clone())?);
                }
            }
        }

        // The owner, its space, and the remote every member shares.
        let (owner_profile, owner_operator, owner_home) = open_member::<S>(&seed, root, 0).await?;
        let server: Repository<SignerCredential> = owner_profile
            .space("space".to_string())
            .create()
            .with_credential(seed.signer("space").await?)
            .perform(&owner_operator)
            .await?;
        let remote = root.join("remote");
        std::fs::create_dir_all(&remote)?;
        let location = Location::new(
            Directory::At(remote.to_string_lossy().into_owned()),
            "space",
        );
        let filesystem = FileSystem::open(&location).await?;
        server
            .did()
            .credential()
            .key("self")
            .save(Credential::Signer(server.credential().clone()))
            .perform(&filesystem)
            .await?;
        let address = FsAddress::new(location);
        let vault: Did = Principal::did(&seed.signer("vault").await?);

        let mut profiles = vec![owner_profile.did().to_string()];
        let mut members: Vec<Member<S>> = Vec::new();
        let mut pending = Vec::new();
        for n in 1..count {
            let (profile, operator, home) = open_member::<S>(&seed, root, n).await?;
            profiles.push(profile.did().to_string());
            pending.push((profile, operator, home));
        }
        let profiles = Profiles(profiles);

        let owner_branch = {
            name_origin(&owner_operator, &vault, &address).await?;
            let origin = contact("origin")
                .connect()
                .repository(server.did())
                .open()
                .perform(&owner_operator)
                .await?;
            let branch = server
                .branch("main")
                .open()
                .perform(&owner_operator)
                .await?;
            if serve {
                serve_in_place(
                    &owner_operator,
                    &branch,
                    &owner_home.join("space").join("archive.dialog"),
                )?;
            }
            let upstream = origin
                .branch("main")
                .open()
                .perform(&owner_operator)
                .await?;
            branch
                .set_upstream(upstream)
                .perform(&owner_operator)
                .await?;
            branch
        };

        // Setup: the owner builds the space from the dataset.
        let started = Instant::now();
        let mut commits = 0usize;
        let mut batch: Vec<&Line> = Vec::new();
        for line in lines {
            let is_setup_command = matches!(
                line,
                Line::Command {
                    phase: Phase::Setup,
                    ..
                }
            );
            if !is_setup_command && !batch.is_empty() {
                commit_commands(&owner_branch, &owner_operator, &profiles, &batch).await?;
                commits += 1;
                batch.clear();
            }
            match line {
                Line::Command {
                    phase: Phase::Setup,
                    ..
                } => {
                    batch.push(line);
                    if batch.len() == SETUP_BATCH {
                        commit_commands(&owner_branch, &owner_operator, &profiles, &batch).await?;
                        commits += 1;
                        batch.clear();
                    }
                }
                Line::Commit {
                    phase: Phase::Setup,
                    changes,
                } => {
                    let mut batch = Changes::new();
                    profiles.changes(changes, &mut batch)?;
                    owner_branch
                        .transaction()
                        .integrate(batch)
                        .commit()
                        .publish()
                        .perform(&owner_operator)
                        .await?;
                    commits += 1;
                }
                Line::Schema {
                    inductive,
                    deductive,
                    transient,
                } => {
                    let mut transaction = owner_branch.transaction();
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
                    transaction
                        .commit()
                        .publish()
                        .perform(&owner_operator)
                        .await?;
                    commits += 1;
                }
                _ => {}
            }
        }
        if !batch.is_empty() {
            commit_commands(&owner_branch, &owner_operator, &profiles, &batch).await?;
            commits += 1;
        }
        report.time("setup/build", started.elapsed());
        report.count("setup/commits", commits as u64);
        report.count("setup/owner-bytes", bytes_under(&owner_home));
        let started = Instant::now();
        owner_branch
            .push()
            .perform(&owner_operator)
            .await?
            .context("the setup push publishes")?;
        report.time("setup/push", started.elapsed());
        report.count("setup/remote-bytes", bytes_under(&remote));
        eprintln!(
            "built the space in {commits} commits, {:.1}s",
            report.timings["setup/build"][0] / 1e3
        );

        // Every other member joins: a delegation from the space, a replica
        // of its own tracking the remote, and a pull.
        members.push(Member {
            profile: owner_profile,
            operator: owner_operator,
            branch: owner_branch,
            home: owner_home,
        });
        for (n, (profile, operator, home)) in pending.into_iter().enumerate() {
            let owner = &members[0];
            let chain = server
                .access()
                .claim(&server)
                .delegate(profile.did())
                .perform(&owner.operator)
                .await?;
            profile.access().save(chain).perform(&operator).await?;
            let local = profile
                .space(format!("space-{}", n + 1))
                .create()
                .with_credential(seed.signer(&format!("space-{}", n + 1)).await?)
                .perform(&operator)
                .await?;
            name_origin(&operator, &vault, &address).await?;
            let origin = contact("origin")
                .connect()
                .repository(server.did())
                .open()
                .perform(&operator)
                .await?;
            let branch = local.branch("main").open().perform(&operator).await?;
            if serve {
                serve_in_place(
                    &operator,
                    &branch,
                    &home.join(format!("space-{}", n + 1)).join("archive.dialog"),
                )?;
            }
            let upstream = origin.branch("main").open().perform(&operator).await?;
            branch.set_upstream(upstream).perform(&operator).await?;
            let started = Instant::now();
            branch.pull().perform(&operator).await?;
            report.time("join/pull", started.elapsed());
            members.push(Member {
                profile,
                operator,
                branch,
                home,
            });
        }

        // The views, read by a member that just joined: cold, then warm.
        let reader = &members[1];
        let mut sizes = BTreeMap::new();
        for view in VIEWS {
            let descriptor = &concepts[view];
            reader.profile.caches().release(&server.did());
            let started = Instant::now();
            let rows = reader
                .branch
                .select(query(descriptor, &[]))
                .perform(&reader.operator)
                .try_vec()
                .await?;
            report.time(&format!("query/{view}/cold"), started.elapsed());
            for _ in 0..5 {
                let started = Instant::now();
                reader
                    .branch
                    .select(query(descriptor, &[]))
                    .perform(&reader.operator)
                    .try_vec()
                    .await?;
                report.time(&format!("query/{view}/warm"), started.elapsed());
            }
            sizes.insert(view, rows.len());
            report.count(&format!("rows/{view}"), rows.len() as u64);
        }
        // An issue's detail: its activity, for the issue with the most.
        let activity = &concepts["v2/activity"];
        let rows = reader
            .branch
            .select(query(activity, &[]))
            .perform(&reader.operator)
            .try_vec()
            .await?;
        let mut by_issue: BTreeMap<String, usize> = BTreeMap::new();
        for row in &rows {
            if let Some(issue) = field(row, "issue") {
                *by_issue.entry(issue).or_default() += 1;
            }
        }
        if let Some((issue, _)) = by_issue.iter().max_by_key(|(_, count)| **count) {
            let issue = Entity::from_str(issue)?;
            for temperature in ["cold", "warm"] {
                if temperature == "cold" {
                    reader.profile.caches().release(&server.did());
                }
                let started = Instant::now();
                let rows = reader
                    .branch
                    .select(query(activity, &[("issue", issue.clone())]))
                    .perform(&reader.operator)
                    .try_vec()
                    .await?;
                report.time(
                    &format!("query/issue-detail/{temperature}"),
                    started.elapsed(),
                );
                report.count("rows/issue-detail", rows.len() as u64);
            }
        }

        // The reader keeps the issues page and the memos open.
        let mut subscriptions: Vec<(&str, Subscription<ConceptQuery>)> = Vec::new();
        for view in ["v2/issue", "v2/activity", "memo/full"] {
            let mut subscription = reader.branch.subscribe(query(&concepts[view], &[]));
            let started = Instant::now();
            subscription.poll(&reader.operator).await?;
            report.time(&format!("subscription/{view}/first"), started.elapsed());
            subscriptions.push((view, subscription));
        }

        // The week.
        let mut caught_up = vec![true; members.len()];
        let mut round = 0usize;
        for line in lines {
            match line {
                Line::Command {
                    phase: Phase::Writes,
                    name,
                    author,
                    changes,
                    durable,
                } => {
                    let member = &members[*author as usize];
                    let mut transaction = member.branch.transaction();
                    transaction = transaction.dispatch(profiles.command(changes)?);
                    if !durable.is_empty() {
                        let mut batch = Changes::new();
                        profiles.changes(durable, &mut batch)?;
                        transaction = transaction.integrate(batch);
                    }
                    let started = Instant::now();
                    transaction
                        .commit()
                        .publish()
                        .perform(&member.operator)
                        .await?;
                    let took = started.elapsed();
                    report.time(&format!("command/{name}"), took);
                    if caught_up[*author as usize] {
                        report.time("command/all", took);
                    } else {
                        // The first commit after a pull runs the rules
                        // over what the pull brought in.
                        report.time("command/first-after-pull", took);
                        caught_up[*author as usize] = true;
                    }
                }
                Line::Sync => {
                    round += 1;
                    for (n, member) in members.iter().enumerate() {
                        let started = Instant::now();
                        member.branch.pull().perform(&member.operator).await?;
                        report.time("sync/pull", started.elapsed());
                        caught_up[n] = false;
                        let started = Instant::now();
                        member.branch.push().perform(&member.operator).await?;
                        report.time("sync/push", started.elapsed());
                        if n == 1 {
                            for (view, subscription) in subscriptions.iter_mut() {
                                let started = Instant::now();
                                subscription.poll(&member.operator).await?;
                                report
                                    .time(&format!("subscription/{view}/poll"), started.elapsed());
                            }
                        }
                    }
                }
                _ => {}
            }
        }
        report.count("week/rounds", round as u64);

        // Every replica has what every other pushed: one more pull each,
        // then the views agree everywhere.
        for member in &members {
            member.branch.pull().perform(&member.operator).await?;
        }
        let expected = members[0]
            .branch
            .select(query(&concepts["v2/issue"], &[]))
            .perform(&members[0].operator)
            .try_vec()
            .await?
            .len();
        for (n, member) in members.iter().enumerate() {
            let seen = member
                .branch
                .select(query(&concepts["v2/issue"], &[]))
                .perform(&member.operator)
                .try_vec()
                .await?
                .len();
            anyhow::ensure!(
                seen == expected,
                "member {n} sees {seen} issues where the owner sees {expected}"
            );
            report.count(&format!("bytes/member-{n}"), bytes_under(&member.home));
        }
        report.count("rows/v2/issue/end", expected as u64);

        // Steady: one member's commands in a row, everyone caught up and no
        // pull between, so its rules and caches are warm.
        for line in lines {
            if let Line::Command {
                phase: Phase::Steady,
                name,
                author,
                changes,
                durable,
            } = line
            {
                let member = &members[*author as usize];
                let mut transaction = member.branch.transaction();
                transaction = transaction.dispatch(profiles.command(changes)?);
                if !durable.is_empty() {
                    let mut batch = Changes::new();
                    profiles.changes(durable, &mut batch)?;
                    transaction = transaction.integrate(batch);
                }
                let started = Instant::now();
                transaction
                    .commit()
                    .publish()
                    .perform(&member.operator)
                    .await?;
                let took = started.elapsed();
                report.time(&format!("steady/{name}"), took);
                report.time("steady/all", took);
            }
        }
        report.count("bytes/remote", bytes_under(&remote));
        report.summary();
        Ok(report)
    }

    /// Commits `commands`, each dispatched, in one transaction.
    async fn commit_commands<S: PeerSpace>(
        branch: &Branch,
        operator: &Peer<S, Session>,
        profiles: &Profiles,
        commands: &[&Line],
    ) -> anyhow::Result<()> {
        let mut transaction = branch.transaction();
        let mut durable_changes = Changes::new();
        for line in commands {
            if let Line::Command {
                changes, durable, ..
            } = line
            {
                transaction = transaction.dispatch(profiles.command(changes)?);
                profiles.changes(durable, &mut durable_changes)?;
            }
        }
        transaction
            .integrate(durable_changes)
            .commit()
            .publish()
            .perform(operator)
            .await?;
        Ok(())
    }

    /// Names the remote `origin` for `operator`.
    async fn name_origin<S: PeerSpace>(
        operator: &Peer<S, Session>,
        vault: &Did,
        address: &FsAddress,
    ) -> anyhow::Result<()> {
        contact(vault)
            .add_address(SiteAddress::Fs(address.clone()))
            .name("origin")
            .perform(operator)
            .await?;
        Ok(())
    }

    /// The value of field `name` of `row`, as text.
    fn field(row: &ConceptConclusion, name: &str) -> Option<String> {
        row.get::<Entity>(name)
            .ok()
            .map(|entity| entity.to_string())
    }
}

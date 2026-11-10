//! What every scenario shares: the configuration a run takes, the seeded
//! site a client replicates (a server replica, its vault, the `origin`
//! contact every client of the run mounts through), the measured phase,
//! and the report a run ends in.
//!
//! A scenario is a function from a seeded [`Site`] to a list of
//! [`PhaseReport`]s; [`run`] seeds the site unshaped, switches the link
//! model on, runs the scenario's phases, and labels the report.

use std::path::{Path, PathBuf};
use std::str::FromStr;

use anyhow::{Context as _, Result};
use dialog_common::helpers::entropy;
use dialog_credentials::SignerCredential;
use dialog_effects::storage::{Directory, Location};
use dialog_peer::Peer;
use dialog_peer::helpers::{Seed, unique_name};
use dialog_remote_fs::FsAddress;
use dialog_remote_fs::simulation::{self, NetworkShape, Traffic};
use dialog_repository::{Branch, Repository, RepositoryExt as _, SiteAddress, contact};
use dialog_storage::provider::storage::VolatileSpace;
use dialog_varsig::Did;

use crate::join::{
    FACTS_PER_ENTITY, entity_facts, meta_facts, mount_client, open_card_rule, seed_vault,
    vault_peer,
};
use crate::report::{PhaseReport, Report};

/// The scenarios the harness runs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Kind {
    /// A fresh client joins the seeded space ([`crate::join`]).
    Join,
    /// A subscribed client catches up on commits the upstream made
    /// ([`crate::catchup`]).
    CatchUp,
    /// A joined client commits and pushes ([`crate::writer`]).
    Writer,
}

impl Kind {
    /// Every scenario, in sweep order.
    pub const ALL: [Kind; 3] = [Kind::Join, Kind::CatchUp, Kind::Writer];

    /// The scenario's name in reports and file names.
    pub fn label(&self) -> &'static str {
        match self {
            Kind::Join => "join",
            Kind::CatchUp => "catchup",
            Kind::Writer => "writer",
        }
    }
}

impl FromStr for Kind {
    type Err = anyhow::Error;

    fn from_str(name: &str) -> Result<Self> {
        Kind::ALL
            .into_iter()
            .find(|kind| kind.label() == name)
            .ok_or_else(|| {
                anyhow::anyhow!("unknown scenario {name:?} (use join, catchup or writer)")
            })
    }
}

impl std::fmt::Display for Kind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.label())
    }
}

/// Configuration for one scenario run.
#[derive(Debug, Clone)]
pub struct Scenario {
    /// Which scenario to run.
    pub kind: Kind,
    /// Entities to seed (each carries [`FACTS_PER_ENTITY`] facts).
    pub entities: usize,
    /// Commits the seed is split into (history depth of the space).
    pub commits: usize,
    /// Members seeded into the roster.
    pub members: usize,
    /// The link model the client runs over; `None` measures counts only.
    pub network: Option<NetworkShape>,
    /// Label for the report (`mobile`, `broadband`, ...).
    pub network_label: String,
    /// Directory to place the remote vault in. Created if absent; the
    /// vault is written under a unique subdirectory per run.
    pub vault_dir: PathBuf,
    /// The seed every identity of the run derives from: the same seed
    /// writes the same tree, so the run's counts repeat exactly.
    pub seed: u64,
}

/// The seed a run takes when `DIALOG_SOAK_SEED` names none. The per-PR
/// gate runs it, so its counts are exact and its baselines are this
/// seed's.
pub const DEFAULT_SEED: u64 = 0x5eed_d1a1_0600_0001;

/// The environment variable a run reads its seed from: a number, or unset
/// for [`DEFAULT_SEED`]. The sweep also takes `random`, a fresh seed per
/// run, which each report records so the run can be replayed.
pub const SEED_VARIABLE: &str = "DIALOG_SOAK_SEED";

/// Which seed the runs take.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Seeding {
    /// Every run takes this seed, and so writes the same tree.
    Fixed(u64),
    /// Every run draws a fresh seed from the system, so repeats exercise
    /// the spread identities cause; each report records its seed.
    Random,
}

impl Seeding {
    /// The seeding [`SEED_VARIABLE`] names: unset is [`DEFAULT_SEED`],
    /// `random` is [`Seeding::Random`], and a number (decimal, or hex
    /// after `0x`) is that seed.
    pub fn from_env() -> Result<Self> {
        match std::env::var(SEED_VARIABLE) {
            Err(std::env::VarError::NotPresent) => Ok(Seeding::Fixed(DEFAULT_SEED)),
            Err(error) => Err(error).with_context(|| format!("reading {SEED_VARIABLE}")),
            Ok(value) => value.parse(),
        }
    }

    /// The seed for the next run.
    pub fn draw(&self) -> Result<u64> {
        match self {
            Seeding::Fixed(seed) => Ok(*seed),
            Seeding::Random => {
                let mut bytes = [0u8; 8];
                dialog_common::random::fill(&mut bytes)
                    .map_err(|error| anyhow::anyhow!("drawing a seed: {error}"))?;
                Ok(u64::from_le_bytes(bytes))
            }
        }
    }
}

impl FromStr for Seeding {
    type Err = anyhow::Error;

    fn from_str(value: &str) -> Result<Self> {
        let value = value.trim();
        if value == "random" {
            return Ok(Seeding::Random);
        }
        let seed = match value.strip_prefix("0x") {
            Some(hex) => u64::from_str_radix(hex, 16),
            None => value.parse(),
        };
        seed.map(Seeding::Fixed).map_err(|_| {
            anyhow::anyhow!("{SEED_VARIABLE}={value:?} is neither a number nor `random`")
        })
    }
}

/// The operator a replica of a run performs through: the server's, or a
/// client's own (see [`Site::mount`]).
pub(crate) type Operator = Peer<VolatileSpace, dialog_peer::Session>;

/// A seeded site: the server replica, the vault it pushed the space to,
/// and what a client needs to mount it.
pub(crate) struct Site {
    /// The server's session.
    pub operator: Operator,
    /// The server replica whose space the clients replicate.
    pub server: Repository<SignerCredential>,
    /// The server's `main`, tracking `origin/main`: the upstream writer.
    pub branch: Branch,
    /// The DID the vault directory is known by, named `origin`.
    pub vault: Did,
    /// The vault's address.
    pub address: FsAddress,
    /// What the run's identities derive from.
    pub seed: Seed,
    /// The seed's commit count, as the report labels it.
    pub commits: usize,
    /// The tree root of the server's head once seeded, hex.
    pub root: String,
    /// Files in the vault after seeding.
    pub vault_files: u64,
    /// Bytes in the vault after seeding.
    pub vault_bytes: u64,
    /// The run's own vault directory, removed when the run ends.
    pub run_dir: PathBuf,
    /// The server's delegation to the profile, which each client's
    /// operator saves so it may reach the server's repository.
    pub chain: dialog_ucan::UcanDelegation,
    /// Whether the run is unshaped, so every operator speculates nothing.
    pub unshaped: bool,
}

impl Site {
    /// Seed a site for `scenario`, unshaped: the network model applies to
    /// the clients, not to the server writing its own vault.
    pub async fn seed(scenario: &Scenario) -> Result<Self> {
        simulation::configure(None);
        simulation::reset_tally();

        let seed = Seed::new(scenario.seed);
        let (operator, profile) = seed.session_with_peer().await;
        // Speculative preloading is ambient (the operator's queue, hints
        // default-on); the unshaped profile turns it off on every operator.
        let unshaped = scenario.network.is_none();
        if unshaped {
            speculate_nothing(&operator).await;
        }
        let server = profile
            .space(seed.name("soak-server"))
            .create()
            .with_credential(seed.signer("soak-server").await?)
            .perform(&operator)
            .await?;
        let chain = server
            .access()
            .claim(&server)
            .delegate(profile.did())
            .perform(&operator)
            .await?;
        profile
            .access()
            .save(chain.clone())
            .perform(&operator)
            .await?;

        // The vault's directory is the one name not derived from the seed:
        // runs of one seed share a temp directory, and the path reaches
        // only the contact, not the tree.
        let run_dir = scenario.vault_dir.join(unique_name("soak-vault"));
        std::fs::create_dir_all(&run_dir)?;
        let location = Location::new(
            Directory::At(run_dir.to_string_lossy().into_owned()),
            "space",
        );
        let address = seed_vault(&server, &location).await?;
        let vault = vault_peer(&seed).await?;

        let origin = {
            let site = SiteAddress::Fs(address.clone());
            contact(&vault)
                .add_address(site)
                .name("origin")
                .perform(&operator)
                .await?;
            contact("origin")
                .connect()
                .repository(server.did())
                .open()
                .perform(&operator)
                .await?
        };
        let branch = server.branch("main").open().perform(&operator).await?;
        let remote_branch = origin.branch("main").open().perform(&operator).await?;
        branch
            .set_upstream(remote_branch)
            .perform(&operator)
            .await?;

        // Seed: metadata first, then the entities spread over the
        // requested number of commits (history depth shapes the head the
        // client adopts).
        branch
            .transaction()
            .integrate(meta_facts(scenario.members)?.into_iter().collect())
            .commit()
            .publish()
            .perform(&operator)
            .await?;
        // The derived-concept rule ships with the space: its facts live in
        // the tree like any others, so a cold client discovers and
        // hydrates the rule on first use (the join's `rule` phase).
        branch
            .transaction()
            .assert(open_card_rule()?)
            .commit()
            .publish()
            .perform(&operator)
            .await?;
        let commits = scenario.commits.max(1);
        let per_commit = scenario.entities.div_ceil(commits);
        let mut seeded = 0;
        while seeded < scenario.entities {
            let batch_end = (seeded + per_commit).min(scenario.entities);
            let mut batch = Vec::new();
            for index in seeded..batch_end {
                batch.extend(entity_facts(index)?);
            }
            branch
                .transaction()
                .integrate(batch.into_iter().collect())
                .commit()
                .publish()
                .perform(&operator)
                .await?;
            seeded = batch_end;
        }
        branch
            .push()
            .perform(&operator)
            .await?
            .context("seed push should ship the space")?;

        let (vault_files, vault_bytes) = vault_stats(&run_dir);
        let root = branch
            .revision()
            .context("the seeded branch has a head")?
            .tree
            .hash()
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect();

        Ok(Site {
            operator,
            server,
            branch,
            vault,
            address,
            seed,
            commits: commits + 1,
            root,
            vault_files,
            vault_bytes,
            run_dir,
            chain,
            unshaped,
        })
    }

    /// Mount a fresh client replica of the site under `name`: its `main`
    /// tracks `origin/main`, with nothing pulled yet.
    ///
    /// The client gets an operator of its own, opened over fresh storage
    /// from the same profile key, so it shares no environment, cache or
    /// speculation queue with the server or with another client: what it
    /// knows of the space it fetched, as a replica on another device would.
    pub async fn mount(&self, name: &str) -> Result<(Branch, Operator)> {
        let (operator, profile) = self.seed.session_with_peer().await;
        profile
            .access()
            .save(self.chain.clone())
            .perform(&operator)
            .await?;
        if self.unshaped {
            speculate_nothing(&operator).await;
        }
        let branch = mount_client(
            &operator,
            &profile,
            &self.server,
            &self.vault,
            &self.address,
            &self.seed,
            name,
        )
        .await?;
        Ok((branch, operator))
    }
}

/// Turn speculative preloading off for `operator`. The unshaped profile's
/// job is to pin the engine's deterministic demand shape, and replication
/// overlap is a latency behavior only the shaped profiles measure.
async fn speculate_nothing(operator: &Operator) {
    use dialog_capability::Provider;
    let queue = Provider::<dialog_artifacts::Speculation>::execute(operator, ()).await;
    queue.set_budget(dialog_artifacts::FetchBudget::ZERO);
}

/// Files and bytes under `dir`, recursively.
fn vault_stats(dir: &Path) -> (u64, u64) {
    let mut files = 0;
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
                files += 1;
                bytes += meta.len();
            }
        }
    }
    (files, bytes)
}

/// Measure one phase: tally deltas, the get ledger's accounting, the
/// dependent-request chain the phase added, and virtual elapsed time.
pub(crate) async fn measured<F, T>(name: &str, phases: &mut Vec<PhaseReport>, work: F) -> Result<T>
where
    F: Future<Output = Result<T>>,
{
    let before_tally = simulation::tally();
    let before_gets = simulation::get_ledger();
    let before_chain = simulation::longest_chain();
    if std::env::var("DIALOG_SOAK_DUMP_GETS").is_ok() {
        eprintln!("PHASE-START {name}");
    }
    let before = tokio::time::Instant::now();
    let outcome = work.await.with_context(|| format!("phase {name} failed"))?;
    let elapsed = before.elapsed();
    let traffic = simulation::tally().since(&before_tally);
    let gets = simulation::get_ledger().since(&before_gets);
    let round_trips = simulation::longest_chain() - before_chain;
    let blob_requests = traffic.count(Traffic::BlobRead) + traffic.count(Traffic::BlobImport);
    let blob_bytes = traffic.bytes(Traffic::BlobRead) + traffic.bytes(Traffic::BlobImport);
    // With DIALOG_SOAK_DUMP_GETS set, print the phase's most re-requested
    // keys so a duplication investigation can see which blocks they are.
    if std::env::var("DIALOG_SOAK_DUMP_GETS").is_ok() {
        for (key, record) in gets.offenders().into_iter().take(24) {
            eprintln!(
                "DUP {name} {key} requests={} empty={} bytes={}",
                record.requests, record.empty, record.bytes
            );
        }
    }
    phases.push(PhaseReport {
        name: name.to_string(),
        virtual_ms: elapsed.as_millis() as u64,
        rounds: 0.0,
        round_trips: Some(round_trips),
        blob_requests,
        blob_bytes,
        unique_blocks: gets.unique_blocks(),
        duplicate_requests: gets.duplicate_requests(),
        duplicate_bytes: gets.duplicate_bytes(),
        empty_requests: gets.empty_requests(),
        traffic: traffic.into(),
    });
    Ok(outcome)
}

/// Run `scenario` end to end: seed the site, switch the link model on,
/// run the scenario's phases over it, and report what they measured.
pub async fn run(scenario: Scenario) -> Result<Report> {
    // A line's identifier, which the run cannot derive by label, draws
    // from the seed too, on this thread, for the whole run.
    let _entropy = entropy::seed(scenario.seed);
    let site = Site::seed(&scenario).await?;

    // The clients run over the modeled link. Unshaped, they run in
    // lockstep, so the chain reads the rounds of their schedule and not
    // the order the filesystem returns their requests in.
    simulation::configure(scenario.network);
    simulation::lockstep(scenario.network.is_none());
    simulation::reset_tally();
    let mut phases = Vec::new();
    let outcome = match scenario.kind {
        Kind::Join => crate::join::phases(&site, &scenario, &mut phases).await,
        Kind::CatchUp => crate::catchup::phases(&site, &scenario, &mut phases).await,
        Kind::Writer => crate::writer::phases(&site, &scenario, &mut phases).await,
    };
    simulation::configure(None);
    simulation::lockstep(false);
    // Every run writes a full vault (tens of MiB at the default size); a
    // sweep runs dozens. Remove it whether the phases passed or failed, so
    // repeated runs do not fill the disk they write to.
    let _ = std::fs::remove_dir_all(&site.run_dir);
    outcome?;

    // Every tree in this run was created here, so they all carry the
    // format a new tree takes in this process (which the sweep sets
    // through the environment); the report labels the run with it.
    let manifest = dialog_search_tree::Manifest::default();
    let (latency_ms, auth_ms, bandwidth_mbps) = match &scenario.network {
        Some(shape) => (
            shape.latency.as_secs_f64() * 1000.0,
            shape.auth_latency.as_secs_f64() * 1000.0,
            shape
                .bandwidth
                .map_or(0.0, |b| b as f64 * 8.0 / 1_000_000.0),
        ),
        None => (0.0, 0.0, 0.0),
    };

    // Derive each phase's sequential-fetch-chain estimate: under the
    // paused clock a chain of dependent requests costs its length times
    // the per-request serial cost (auth redeem, then the round trip), so
    // the quotient bounds the chain depth from above. Bandwidth
    // serialization also advances the clock, which is why this is a bound
    // and not a count; `round_trips` is the count.
    let per_request_ms = latency_ms + auth_ms;
    if per_request_ms > 0.0 {
        for phase in &mut phases {
            phase.rounds = phase.virtual_ms as f64 / per_request_ms;
        }
    }

    Ok(Report {
        scenario: scenario.kind.label().into(),
        network: scenario.network_label,
        latency_ms,
        auth_ms,
        bandwidth_mbps,
        connections: scenario.network.and_then(|shape| shape.connections),
        fanout_n: manifest.fanout_n,
        max_segment: manifest.max_segment,
        entities: scenario.entities,
        facts: scenario.entities * FACTS_PER_ENTITY + 1 + scenario.members * 2,
        commits: site.commits,
        vault_files: site.vault_files,
        vault_bytes: site.vault_bytes,
        seed: Some(scenario.seed),
        seeded_root: Some(site.root),
        phases,
    })
}

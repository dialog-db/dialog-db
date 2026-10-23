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
use dialog_credentials::SignerCredential;
use dialog_effects::storage::{Directory, Location};
use dialog_peer::Peer;
use dialog_peer::helpers::{test_session_with_peer, unique_name};
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
}

/// The operator every replica of a run performs through.
pub(crate) type Operator = Peer<VolatileSpace, dialog_peer::Session>;

/// A seeded site: the server replica, the vault it pushed the space to,
/// and what a client needs to mount it.
pub(crate) struct Site {
    /// The session every replica performs through.
    pub operator: Operator,
    /// The profile the replicas are spaces of.
    pub profile: Peer<VolatileSpace>,
    /// The server replica whose space the clients replicate.
    pub server: Repository<SignerCredential>,
    /// The server's `main`, tracking `origin/main`: the upstream writer.
    pub branch: Branch,
    /// The DID the vault directory is known by, named `origin`.
    pub vault: Did,
    /// The vault's address.
    pub address: FsAddress,
    /// The seed's commit count, as the report labels it.
    pub commits: usize,
    /// Files in the vault after seeding.
    pub vault_files: u64,
    /// Bytes in the vault after seeding.
    pub vault_bytes: u64,
}

impl Site {
    /// Seed a site for `scenario`, unshaped: the network model applies to
    /// the clients, not to the server writing its own vault.
    pub async fn seed(scenario: &Scenario) -> Result<Self> {
        simulation::configure(None);
        simulation::reset_tally();

        let (operator, profile) = test_session_with_peer().await;
        // Speculative preloading is ambient (the operator's queue, hints
        // default-on). The unshaped profile turns it off: its job is to
        // pin the engine's deterministic demand shape, and replication
        // overlap is a latency behavior only the shaped profiles measure.
        if scenario.network.is_none() {
            use dialog_capability::Provider;
            let queue = Provider::<dialog_artifacts::Speculation>::execute(&operator, ()).await;
            queue.set_budget(dialog_artifacts::FetchBudget::ZERO);
        }
        let server = profile
            .space(unique_name("soak-server"))
            .create()
            .perform(&operator)
            .await?;
        let chain = server
            .access()
            .claim(&server)
            .delegate(profile.did())
            .perform(&operator)
            .await?;
        profile.access().save(chain).perform(&operator).await?;

        let run_dir = scenario.vault_dir.join(unique_name("soak-vault"));
        std::fs::create_dir_all(&run_dir)?;
        let location = Location::new(
            Directory::At(run_dir.to_string_lossy().into_owned()),
            "space",
        );
        let address = seed_vault(&server, &location).await?;
        let vault = vault_peer().await?;

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

        Ok(Site {
            operator,
            profile,
            server,
            branch,
            vault,
            address,
            commits: commits + 1,
            vault_files,
            vault_bytes,
        })
    }

    /// Mount a fresh client replica of the site under `name`: its `main`
    /// tracks `origin/main`, with nothing pulled yet.
    pub async fn mount(&self, name: &str) -> Result<Branch> {
        mount_client(
            &self.operator,
            &self.profile,
            &self.server,
            &self.vault,
            &self.address,
            name,
        )
        .await
    }
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
    let site = Site::seed(&scenario).await?;

    // The clients run over the modeled link.
    simulation::configure(scenario.network);
    simulation::reset_tally();
    let mut phases = Vec::new();
    match scenario.kind {
        Kind::Join => crate::join::phases(&site, &scenario, &mut phases).await?,
        Kind::CatchUp => crate::catchup::phases(&site, &scenario, &mut phases).await?,
        Kind::Writer => crate::writer::phases(&site, &scenario, &mut phases).await?,
    }
    simulation::configure(None);

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
        fanout_n: manifest.fanout_n,
        max_segment: manifest.max_segment,
        entities: scenario.entities,
        facts: scenario.entities * FACTS_PER_ENTITY + 1 + scenario.members * 2,
        commits: site.commits,
        vault_files: site.vault_files,
        vault_bytes: site.vault_bytes,
        phases,
    })
}

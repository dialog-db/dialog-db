// The harness is native-only. On wasm the crate below is empty, so it has
// no `main` to require.
#![cfg_attr(target_arch = "wasm32", no_main)]
#![cfg(not(target_arch = "wasm32"))]

//! # Text write cost (research/text-write-cost.md, experiment E1)
//!
//! Replays a concurrent editing trace (the `concurrent_traces` format:
//! transactions with `parents`, `agent` and `patches`) into Dialog under
//! one of several layouts and flush policies, and prints one JSON line of
//! what it cost: bytes written to the replica's storage, the live size of
//! the final state, bytes moved by sync, and wall time per commit.
//!
//! ```sh
//! text_write_cost --trace friendsforever.json.gz --variant facts --flush 100
//! text_write_cost --trace clownschool.json.gz --variant facts --flush second --sync
//! ```
//!
//! Variants:
//!
//! - `raw`: the trace's own operations in a compact encoding, summed. No
//!   Dialog. The floor every other variant is a ratio to.
//! - `facts`: each transaction is one fact `(doc, text/change, bytes)`
//!   asserted under `Pick::All`, through the repository's commit path.
//! - `doc`: one fact `(doc, text/content, text)` under `Pick::Last`, the
//!   whole current text, replaced on each commit.
//! - `region-canonical`: each change inserted straight into a
//!   `PersistentTree` under `tag ‖ doc ‖ origin ‖ seq`, persisted per
//!   flush. No claim records, no revision record, no signature.
//! - `region-buffered`: the same keys through a `HitchhikerTree` with the
//!   repository's default buffers, persisted (buffers intact) per flush.
//!
//! Flush policies: `1`, `10`, `100`, `1000` transactions per commit (in
//! trace order), `all` (one commit at the end), `second` (one commit per
//! distinct timestamp second), `pause:<s>` (a commit wherever the next
//! transaction is at least `s` seconds later).
//!
//! With `--sync` (facts and doc only), each agent of the trace is a
//! replica of its own, mounted on a shared remote (the soak's local
//! directory remote, whose transfer tally counts every request and byte).
//! At the end of each flush window every replica that typed in it commits
//! its own transactions as one commit, then each replica in turn pulls
//! and pushes. Sync bytes are the remote's tally over the replay.

use std::collections::HashSet;
use std::ops::Range;
use std::path::PathBuf;
use std::process::Command;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::{Context as _, Result, bail};
use dialog_artifacts::{Artifact, ArtifactSelector, Instruction, Pick, Value};
use dialog_capability::{Provider, Subject};
use dialog_common::Blake3Hash;
use dialog_common::helpers::entropy;
use dialog_credentials::{Credential, SignerCredential};
use dialog_effects::archive::prelude::ArchiveScope;
use dialog_effects::credential::prelude::*;
use dialog_effects::storage::{Directory, Location};
use dialog_peer::Peer;
use dialog_peer::helpers::{Meter, Metered, Seed, unique_name};
use dialog_remote_fs::FsAddress;
use dialog_remote_fs::simulation::{self, Traffic, TransferTally};
use dialog_repository::{Branch, Repository, RepositoryExt as _, SiteAddress, contact};
use dialog_search_tree::{Delta, HitchhikerTree, PersistentTree};
use dialog_storage::provider::storage::VolatileSpace;
use dialog_storage::provider::{FileSystem, Volatile};
use dialog_storage::resource::Resource as _;
use dialog_varsig::{Did, Principal};
use futures_util::StreamExt as _;
use serde_json::json;

/// The fixed seed every identity of a run derives from.
const SEED: u64 = 0x7e47_e100_0000_0001;

/// The document every change belongs to.
const DOC: &str = "doc:text-e1";

/// The region key's tag byte: outside the artifact tags.
const REGION_TAG: u8 = 0x54;

/// `tag ‖ doc (32) ‖ origin (32) ‖ seq (8)`.
const REGION_KEY: usize = 1 + 32 + 32 + 8;

type Operator = Peer<VolatileSpace, dialog_peer::Session>;
type Client = Metered<Operator, Store>;

/// One transaction of the trace.
struct Txn {
    parents: Vec<usize>,
    agent: usize,
    /// The agent's own sequence number for this transaction.
    seq: u64,
    /// Seconds since the epoch, to the second.
    time: i64,
    patches: Vec<(u64, u64, String)>,
}

struct Trace {
    agents: usize,
    end_content: String,
    txns: Vec<Txn>,
}

fn read_trace(path: &PathBuf) -> Result<Trace> {
    let output = Command::new("gzip")
        .arg("-dc")
        .arg(path)
        .output()
        .context("running gzip -dc on the trace")?;
    if !output.status.success() {
        bail!("gzip -dc {} failed", path.display());
    }
    let document: serde_json::Value = serde_json::from_slice(&output.stdout)?;
    let agents = document["numAgents"].as_u64().context("numAgents")? as usize;
    let end_content = document["endContent"]
        .as_str()
        .context("endContent")?
        .to_string();
    let mut next_seq = vec![0u64; agents];
    let mut txns = Vec::new();
    for txn in document["txns"].as_array().context("txns")? {
        let agent = txn["agent"].as_u64().context("agent")? as usize;
        let parents = txn["parents"]
            .as_array()
            .context("parents")?
            .iter()
            .map(|parent| parent.as_u64().map(|p| p as usize))
            .collect::<Option<Vec<_>>>()
            .context("a parent index")?;
        let mut patches = Vec::new();
        for patch in txn["patches"].as_array().context("patches")? {
            patches.push((
                patch[0].as_u64().context("pos")?,
                patch[1].as_u64().context("del")?,
                patch[2].as_str().context("ins")?.to_string(),
            ));
        }
        let time = parse_time(txn["time"].as_str().unwrap_or("1970-01-01T00:00:00"))?;
        txns.push(Txn {
            parents,
            agent,
            seq: next_seq[agent],
            time,
            patches,
        });
        next_seq[agent] += 1;
    }
    Ok(Trace {
        agents,
        end_content,
        txns,
    })
}

/// Seconds since the epoch of `YYYY-MM-DDTHH:MM:SS...`, read as UTC (every
/// timestamp in the traces carries `+00:00`).
fn parse_time(stamp: &str) -> Result<i64> {
    let field = |range: Range<usize>| -> Result<i64> {
        Ok(stamp
            .get(range)
            .context("a short timestamp")?
            .parse::<i64>()?)
    };
    let (year, month, day) = (field(0..4)?, field(5..7)?, field(8..10)?);
    let (hour, minute, second) = (field(11..13)?, field(14..16)?, field(17..19)?);
    // Days from the civil date (Howard Hinnant's algorithm).
    let y = if month <= 2 { year - 1 } else { year };
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let mp = (month + 9) % 12;
    let doy = (153 * mp + 2) / 5 + day - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    let days = era * 146_097 + doe - 719_468;
    Ok(days * 86_400 + hour * 3_600 + minute * 60 + second)
}

fn varint(mut value: u64, out: &mut Vec<u8>) {
    while value >= 0x80 {
        out.push((value as u8) | 0x80);
        value >>= 7;
    }
    out.push(value as u8);
}

/// A transaction's body: its parents and patches, without its own id.
///
/// Parents are `(agent, seq)` ids. The common case, one parent that is
/// the agent's own previous transaction, is the single byte `0`;
/// otherwise a count and the ids follow.
fn encode_body(trace: &Trace, index: usize) -> Vec<u8> {
    let txn = &trace.txns[index];
    let mut out = Vec::new();
    let own_previous = txn.parents.len() == 1 && {
        let parent = &trace.txns[txn.parents[0]];
        parent.agent == txn.agent && parent.seq + 1 == txn.seq
    };
    if own_previous {
        out.push(0);
    } else {
        varint(txn.parents.len() as u64 + 1, &mut out);
        for parent in &txn.parents {
            let parent = &trace.txns[*parent];
            varint(parent.agent as u64, &mut out);
            varint(parent.seq, &mut out);
        }
    }
    varint(txn.patches.len() as u64, &mut out);
    for (pos, del, ins) in &txn.patches {
        varint(*pos, &mut out);
        varint(*del, &mut out);
        varint(ins.len() as u64, &mut out);
        out.extend_from_slice(ins.as_bytes());
    }
    out
}

/// A transaction as a self-identifying change: `agent ‖ seq ‖ body`.
fn encode_change(trace: &Trace, index: usize) -> Vec<u8> {
    let txn = &trace.txns[index];
    let mut out = Vec::new();
    varint(txn.agent as u64, &mut out);
    varint(txn.seq, &mut out);
    out.extend(encode_body(trace, index));
    out
}

#[derive(Debug, Clone, Copy)]
enum Flush {
    Every(usize),
    All,
    Second,
    Pause(i64),
}

impl std::str::FromStr for Flush {
    type Err = anyhow::Error;

    fn from_str(value: &str) -> Result<Self> {
        Ok(match value {
            "all" => Flush::All,
            "second" => Flush::Second,
            _ => match value.strip_prefix("pause:") {
                Some(seconds) => Flush::Pause(seconds.parse()?),
                None => Flush::Every(value.parse()?),
            },
        })
    }
}

/// The trace cut into flush windows, in trace order.
fn windows(trace: &Trace, flush: Flush) -> Vec<Range<usize>> {
    let total = trace.txns.len();
    let mut cuts = vec![0];
    for index in 1..total {
        let (before, here) = (&trace.txns[index - 1], &trace.txns[index]);
        let cut = match flush {
            Flush::Every(n) => index % n == 0,
            Flush::All => false,
            Flush::Second => here.time != before.time,
            Flush::Pause(seconds) => here.time - before.time >= seconds,
        };
        if cut {
            cuts.push(index);
        }
    }
    cuts.push(total);
    cuts.windows(2).map(|pair| pair[0]..pair[1]).collect()
}

/// Every block a replica's storage was handed, and the distinct ones: the
/// distinct bytes are what a content-addressed store holds, since nothing
/// is ever reclaimed.
#[derive(Clone, Default)]
struct Store {
    inner: Arc<Mutex<StoreTally>>,
}

#[derive(Default, Clone)]
struct StoreTally {
    seen: HashSet<Blake3Hash>,
    unique_blocks: u64,
    unique_bytes: u64,
    puts: u64,
    put_bytes: u64,
    largest: u64,
    blobs: u64,
    blob_bytes: u64,
    /// With `TEXT_E1_BREAKDOWN` set: distinct node bytes by what they hold
    /// (`index`, or a segment's bytes split evenly over its entries by the
    /// key tag each entry carries), with entry counts.
    breakdown: std::collections::BTreeMap<String, (u64, f64, u64)>,
}

impl Store {
    fn snapshot(&self) -> (u64, u64, u64, u64) {
        let tally = self.inner.lock().expect("the store tally");
        (
            tally.unique_blocks,
            tally.unique_bytes,
            tally.puts,
            tally.put_bytes,
        )
    }

    fn put(&self, hash: Blake3Hash, length: u64) {
        let mut tally = self.inner.lock().expect("the store tally");
        tally.puts += 1;
        tally.put_bytes += length;
        tally.largest = tally.largest.max(length);
        if tally.seen.insert(hash) {
            tally.unique_blocks += 1;
            tally.unique_bytes += length;
        }
    }

    fn largest(&self) -> u64 {
        self.inner.lock().expect("the store tally").largest
    }

    fn classify(&self, block: &[u8]) {
        use dialog_artifacts::inspect::{inspect_keys, inspect_node};
        let Ok(node) = inspect_node(block.to_vec()) else {
            return;
        };
        let mut tally = self.inner.lock().expect("the store tally");
        if node.kind == "index" {
            let entry = tally.breakdown.entry("index".into()).or_default();
            entry.0 += node.novelty;
            entry.1 += block.len() as f64;
            return;
        }
        let Ok(keys) = inspect_keys(block.to_vec()) else {
            return;
        };
        let share = block.len() as f64 / keys.len().max(1) as f64;
        for key in keys {
            let tag = match key.key.first() {
                Some(0) => "entity",
                Some(1) => "attribute",
                Some(2) => "value",
                Some(3) => "history",
                Some(4) => "blob",
                Some(5) => "coverage",
                Some(6) => "context",
                _ => "other",
            };
            let entry = tally.breakdown.entry(tag.into()).or_default();
            entry.0 += 1;
            entry.1 += share;
            entry.2 += key.key.len() as u64;
        }
    }

    fn breakdown(&self) -> serde_json::Value {
        let tally = self.inner.lock().expect("the store tally");
        tally
            .breakdown
            .iter()
            .map(|(name, (entries, bytes, key_bytes))| {
                (
                    name.clone(),
                    json!({ "entries": entries, "bytes": *bytes as u64, "key_bytes": key_bytes }),
                )
            })
            .collect::<serde_json::Map<_, _>>()
            .into()
    }

    /// Distinct blobs (spilled values, revision records) and their bytes.
    fn blobs(&self) -> (u64, u64) {
        let tally = self.inner.lock().expect("the store tally");
        (tally.blobs, tally.blob_bytes)
    }
}

impl Meter for Store {
    fn wrote(&self, block: &[u8]) {
        let hash = Blake3Hash::hash(block);
        let fresh = !self
            .inner
            .lock()
            .expect("the store tally")
            .seen
            .contains(&hash);
        self.put(hash, block.len() as u64);
        if fresh && std::env::var_os("TEXT_E1_BREAKDOWN").is_some() {
            self.classify(block);
        }
    }

    fn read(&self, _block: &[u8]) {}

    fn imported_blob(&self, digest: &Blake3Hash, size: u64) {
        let fresh = !self
            .inner
            .lock()
            .expect("the store tally")
            .seen
            .contains(digest);
        self.put(digest.clone(), size);
        if fresh {
            let mut tally = self.inner.lock().expect("the store tally");
            tally.blobs += 1;
            tally.blob_bytes += size;
        }
    }
}

/// The server space every replica syncs through, on a local directory
/// remote.
struct Site {
    seed: Seed,
    server: Repository<SignerCredential>,
    chain: dialog_ucan::UcanDelegation,
    vault: Did,
    address: FsAddress,
    run_dir: PathBuf,
}

impl Site {
    async fn open(vault_dir: &std::path::Path) -> Result<Self> {
        simulation::configure(None);
        simulation::reset_tally();
        let seed = Seed::new(SEED);
        let (operator, profile) = seed.session_with_peer().await;
        speculate_nothing(&operator).await;
        let server = profile
            .space(seed.name("text-server"))
            .create()
            .with_credential(seed.signer("text-server").await?)
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
        let run_dir = vault_dir.join(unique_name("text-vault"));
        std::fs::create_dir_all(&run_dir)?;
        let location = Location::new(
            Directory::At(run_dir.to_string_lossy().into_owned()),
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
        let vault = Principal::did(&seed.signer("text-vault").await?);
        Ok(Self {
            seed,
            server,
            chain,
            vault,
            address,
            run_dir,
        })
    }

    /// A replica of its own (its own storage, its own repository and
    /// writer key) whose `main` tracks the server's `main`.
    async fn mount(&self, name: &str) -> Result<(Branch, Client)> {
        let (operator, profile) = self.seed.session_with_peer().await;
        profile
            .access()
            .save(self.chain.clone())
            .perform(&operator)
            .await?;
        speculate_nothing(&operator).await;
        let name = self.seed.name(name);
        let repo = profile
            .space(name.clone())
            .create()
            .with_credential(self.seed.signer(&name).await?)
            .perform(&operator)
            .await?;
        let origin = {
            contact(&self.vault)
                .add_address(SiteAddress::Fs(self.address.clone()))
                .name("origin")
                .perform(&operator)
                .await?;
            contact("origin")
                .connect()
                .repository(self.server.did())
                .open()
                .perform(&operator)
                .await?
        };
        let branch = repo.branch("main").open().perform(&operator).await?;
        let remote = origin.branch("main").open().perform(&operator).await?;
        branch.set_upstream(remote).perform(&operator).await?;
        Ok((branch, Metered::new(operator, Store::default())))
    }
}

impl Drop for Site {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.run_dir);
    }
}

/// No speculative prefetch: count what the operations need, not what a
/// prefetcher guesses.
async fn speculate_nothing(operator: &Operator) {
    let queue = Provider::<dialog_artifacts::Speculation>::execute(operator, ()).await;
    queue.set_budget(dialog_artifacts::FetchBudget::ZERO);
}

fn doc() -> Result<dialog_artifacts::Entity> {
    Ok(DOC.parse()?)
}

fn change_fact(trace: &Trace, index: usize) -> Result<Instruction> {
    Ok(Instruction::Assert(
        Artifact {
            the: "text/change".parse()?,
            of: doc()?,
            is: Value::Bytes(encode_change(trace, index)),
            cause: None,
        },
        Pick::All,
    ))
}

fn content_fact(text: &[char]) -> Result<Instruction> {
    Ok(Instruction::Assert(
        Artifact {
            the: "text/content".parse()?,
            of: doc()?,
            is: Value::String(text.iter().collect()),
            cause: None,
        },
        Pick::Last,
    ))
}

/// Applies a transaction's patches to `text` in trace order. Positions
/// are relative to the transaction's parents, not to this linearization,
/// so concurrent transactions land a little off; positions are clamped.
/// The length is exact, which is what the doc-as-value variant's cost
/// depends on.
fn apply(text: &mut Vec<char>, txn: &Txn) {
    for (pos, del, ins) in &txn.patches {
        let pos = (*pos as usize).min(text.len());
        let end = (pos + *del as usize).min(text.len());
        text.splice(pos..end, ins.chars());
    }
}

async fn commit(branch: &Branch, client: &Client, changes: Vec<Instruction>) -> Result<Duration> {
    let started = Instant::now();
    branch
        .transaction()
        .integrate(changes.into_iter().collect())
        .commit()
        .publish()
        .perform(client)
        .await?;
    Ok(started.elapsed())
}

fn wire(tally: &TransferTally) -> serde_json::Value {
    let up = tally.bytes(Traffic::ArchivePut)
        + tally.bytes(Traffic::ArchiveImport)
        + tally.bytes(Traffic::BlobImport);
    let down = tally.bytes(Traffic::ArchiveGet) + tally.bytes(Traffic::BlobRead);
    json!({
        "bytes": up + down,
        "up_bytes": up,
        "down_bytes": down,
        "puts": tally.count(Traffic::ArchivePut) + tally.count(Traffic::ArchiveImport),
        "gets": tally.count(Traffic::ArchiveGet),
        "blob_up_bytes": tally.bytes(Traffic::BlobImport),
        "blob_down_bytes": tally.bytes(Traffic::BlobRead),
        "resolves": tally.count(Traffic::MemoryResolve),
        "publishes": tally.count(Traffic::MemoryPublish),
    })
}

/// The bytes a fresh replica downloads to hold the whole final state:
/// the live size of the tree the head names.
async fn live_size(site: &Site) -> Result<serde_json::Value> {
    let (fresh, client) = site.mount("text-fresh").await?;
    let before = simulation::tally();
    fresh.pull().download().perform(&client).await?;
    let moved = simulation::tally().since(&before);
    Ok(json!({
        "bytes": moved.bytes(Traffic::ArchiveGet) + moved.bytes(Traffic::BlobRead),
        "blocks": moved.count(Traffic::ArchiveGet),
        "blob_bytes": moved.bytes(Traffic::BlobRead),
    }))
}

/// How many claims of `attribute` the branch holds, and their value bytes.
async fn read_back(branch: &Branch, client: &Client, attribute: &str) -> Result<(usize, usize)> {
    let rows = branch
        .claims()
        .select(ArtifactSelector::new().the(attribute.parse()?))
        .to_owned()
        .perform(client)
        .await?
        .collect::<Vec<_>>()
        .await;
    let mut bytes = 0;
    for row in &rows {
        bytes += row
            .as_ref()
            .map_err(|e| anyhow::anyhow!("{e}"))?
            .is
            .to_bytes()
            .len();
    }
    Ok((rows.len(), bytes))
}

fn root_of(branch: &Branch) -> Option<String> {
    branch.revision().map(|revision| {
        revision
            .tree
            .hash()
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect()
    })
}

fn timing(durations: &[Duration]) -> serde_json::Value {
    let mut ms: Vec<f64> = durations.iter().map(|d| d.as_secs_f64() * 1e3).collect();
    ms.sort_by(|a, b| a.total_cmp(b));
    let total: f64 = ms.iter().sum();
    let at = |q: f64| {
        ms.get(((ms.len() as f64 - 1.0) * q).round() as usize)
            .copied()
    };
    json!({
        "commits": ms.len(),
        "total_ms": total,
        "mean_ms": if ms.is_empty() { 0.0 } else { total / ms.len() as f64 },
        "p50_ms": at(0.5),
        "p99_ms": at(0.99),
        "max_ms": ms.last(),
    })
}

/// One replica replays the whole trace.
async fn single(
    trace: &Trace,
    variant: &str,
    flush: Flush,
    site: &Site,
) -> Result<serde_json::Value> {
    let (branch, client) = site.mount("text-writer").await?;
    let mut text = Vec::new();
    let mut durations = Vec::new();
    let mut per_commit_bytes = Vec::new();
    // Cumulative bytes written after each tenth of the trace.
    let mut series = Vec::new();
    let tenth = trace.txns.len().div_ceil(10);
    for window in windows(trace, flush) {
        let end = window.end;
        let mut changes = Vec::new();
        for index in window.clone() {
            apply(&mut text, &trace.txns[index]);
            if variant == "facts" {
                changes.push(change_fact(trace, index)?);
            }
        }
        if variant == "doc" {
            changes.push(content_fact(&text)?);
        }
        let (_, before, _, _) = client.meter().snapshot();
        durations.push(commit(&branch, &client, changes).await?);
        let (_, after, _, _) = client.meter().snapshot();
        per_commit_bytes.push(after - before);
        while series.len() < 10 && end >= ((series.len() + 1) * tenth).min(trace.txns.len()) {
            series.push(json!([end, after]));
        }
    }
    let (unique_blocks, unique_bytes, puts, put_bytes) = client.meter().snapshot();
    // Publish the final state once, then measure what a fresh replica
    // downloads to hold it.
    let before = simulation::tally();
    branch.push().perform(&client).await?;
    let push = simulation::tally().since(&before);
    let live = live_size(site).await?;
    let attribute = if variant == "facts" {
        "text/change"
    } else {
        "text/content"
    };
    let (claims, value_bytes) = read_back(&branch, &client, attribute).await?;
    per_commit_bytes.sort_unstable();
    Ok(json!({
        "written": {
            "bytes": unique_bytes,
            "blocks": unique_blocks,
            "puts": puts,
            "put_bytes": put_bytes,
            "largest_block": client.meter().largest(),
            "blob_bytes": client.meter().blobs().1,
            "blobs": client.meter().blobs().0,
            "breakdown": client.meter().breakdown(),
            "per_commit_p50": per_commit_bytes.get(per_commit_bytes.len() / 2),
            "per_commit_max": per_commit_bytes.last(),
        },
        "series": series,
        "final_push": wire(&push),
        "live": live,
        "timing": timing(&durations),
        "text_len": text.len(),
        "read_back": { "claims": claims, "value_bytes": value_bytes },
    }))
}

/// One replica per agent; each commits its own transactions per window,
/// then each in turn pulls and pushes.
async fn replicated(
    trace: &Trace,
    variant: &str,
    flush: Flush,
    site: &Site,
) -> Result<serde_json::Value> {
    if variant != "facts" {
        bail!("--sync replays the facts variant only");
    }
    let mut replicas = Vec::new();
    for agent in 0..trace.agents {
        replicas.push(site.mount(&format!("text-agent-{agent}")).await?);
    }
    let sync_before = simulation::tally();
    let mut durations = Vec::new();
    let mut sync_time = Duration::ZERO;
    let mut rounds = 0u64;
    for window in windows(trace, flush) {
        let mut pending: Vec<Vec<Instruction>> = (0..trace.agents).map(|_| Vec::new()).collect();
        for index in window {
            pending[trace.txns[index].agent].push(change_fact(trace, index)?);
        }
        for (agent, changes) in pending.into_iter().enumerate() {
            if !changes.is_empty() {
                let (branch, client) = &replicas[agent];
                durations.push(commit(branch, client, changes).await?);
            }
        }
        let started = Instant::now();
        for (branch, client) in &replicas {
            branch.pull().perform(client).await?;
            branch.push().perform(client).await?;
        }
        sync_time += started.elapsed();
        rounds += 1;
    }
    // A last round of pulls so every replica holds everything.
    for (branch, client) in &replicas {
        branch.pull().perform(client).await?;
    }
    let sync = simulation::tally().since(&sync_before);
    let roots: Vec<Option<String>> = replicas.iter().map(|(branch, _)| root_of(branch)).collect();
    let mut written = Vec::new();
    for (_, client) in &replicas {
        let (blocks, bytes, puts, put_bytes) = client.meter().snapshot();
        let (blobs, blob_bytes) = client.meter().blobs();
        written.push(json!({
            "bytes": bytes,
            "blocks": blocks,
            "puts": puts,
            "put_bytes": put_bytes,
            "blobs": blobs,
            "blob_bytes": blob_bytes,
        }));
    }
    let live = live_size(site).await?;
    let mut changes = Vec::new();
    for (branch, client) in &replicas {
        changes.push(read_back(branch, client, "text/change").await?.0);
    }
    Ok(json!({
        "rounds": rounds,
        "sync": wire(&sync),
        "sync_ms": sync_time.as_secs_f64() * 1e3,
        "written_per_replica": written,
        "live": live,
        "timing": timing(&durations),
        "roots": roots,
        "converged": roots.windows(2).all(|pair| pair[0] == pair[1]),
        "change_facts": changes,
    }))
}

fn region_key(txn: &Txn) -> [u8; REGION_KEY] {
    let mut key = [0u8; REGION_KEY];
    key[0] = REGION_TAG;
    key[1..33].copy_from_slice(Blake3Hash::hash(DOC.as_bytes()).as_bytes());
    let origin = Blake3Hash::hash(format!("origin-{}", txn.agent).as_bytes());
    key[33..65].copy_from_slice(origin.as_bytes());
    key[65..].copy_from_slice(&txn.seq.to_be_bytes());
    key
}

type RegionTree = PersistentTree<[u8; REGION_KEY], Vec<u8>>;

/// Flushes `delta` into `storage`, counting each block once.
async fn land(
    delta: &mut Delta<Blake3Hash, dialog_common::Buffer>,
    catalog: &dialog_effects::archive::prelude::CatalogScope,
    storage: &Volatile,
    store: &Store,
) -> Result<()> {
    let blocks: Vec<_> = delta
        .flush()
        .map(|(hash, block)| {
            store.put(hash, block.as_ref().len() as u64);
            block
        })
        .collect();
    catalog.import(blocks).perform(storage).await?;
    Ok(())
}

/// Changes straight into a tree region: what the tree costs without
/// claim records, revision records or signatures.
async fn region(trace: &Trace, buffered: bool, flush: Flush) -> Result<serde_json::Value> {
    let storage = Volatile::new();
    let catalog =
        ArchiveScope::new(Subject::from(dialog_capability::did!("key:z6MkTextE1"))).index();
    let store = Store::default();
    let mut tree = RegionTree::empty_in(catalog.clone());
    let mut hitchhiker = HitchhikerTree::open(&tree);
    let mut durations = Vec::new();
    for window in windows(trace, flush) {
        let started = Instant::now();
        let mut delta = Delta::zero();
        if buffered {
            let mut next = hitchhiker;
            for index in window {
                let txn = &trace.txns[index];
                next = next
                    .insert(region_key(txn), encode_body(trace, index), &storage)
                    .await?;
            }
            next.persist_mut(&mut delta, &storage)?;
            hitchhiker = next;
        } else {
            let mut edit = tree.edit();
            for index in window {
                let txn = &trace.txns[index];
                edit = edit
                    .insert(region_key(txn), encode_body(trace, index), &storage)
                    .await?;
            }
            tree = edit.persist(&mut delta, &storage)?;
        }
        land(&mut delta, &catalog, &storage, &store).await?;
        durations.push(started.elapsed());
    }
    let (unique_blocks, unique_bytes, puts, put_bytes) = store.snapshot();

    // The live size: the tree holding every change, built in one batch in
    // a store of its own. The canonical tree is history independent, so
    // this is the canonical variant's final tree exactly; the buffered
    // variant's final tree differs by what its buffers still hold.
    let live_storage = Volatile::new();
    let live_store = Store::default();
    let mut edit = RegionTree::empty_in(catalog.clone()).edit();
    for (index, txn) in trace.txns.iter().enumerate() {
        edit = edit
            .insert(region_key(txn), encode_body(trace, index), &live_storage)
            .await?;
    }
    let mut delta = Delta::zero();
    edit.persist(&mut delta, &live_storage)?;
    land(&mut delta, &catalog, &live_storage, &live_store).await?;
    let (live_blocks, live_bytes, _, _) = live_store.snapshot();
    Ok(json!({
        "written": {
            "bytes": unique_bytes,
            "blocks": unique_blocks,
            "puts": puts,
            "put_bytes": put_bytes,
            "largest_block": store.largest(),
        },
        "live": { "bytes": live_bytes, "blocks": live_blocks },
        "timing": timing(&durations),
    }))
}

struct Args {
    trace: PathBuf,
    variant: String,
    flush: Flush,
    sync: bool,
    vault_dir: PathBuf,
    dump: Option<PathBuf>,
    limit: Option<usize>,
}

fn args() -> Result<Args> {
    let mut trace = None;
    let mut variant = None;
    let mut flush = Flush::All;
    let mut sync = false;
    let mut vault_dir = std::env::temp_dir();
    let mut dump = None;
    let mut limit = None;
    let mut rest = std::env::args().skip(1);
    while let Some(arg) = rest.next() {
        match arg.as_str() {
            "--trace" => trace = rest.next().map(PathBuf::from),
            "--variant" => variant = rest.next(),
            "--flush" => flush = rest.next().context("--flush needs a value")?.parse()?,
            "--sync" => sync = true,
            "--vault-dir" => vault_dir = rest.next().context("--vault-dir")?.into(),
            "--dump" => dump = rest.next().map(PathBuf::from),
            "--limit" => limit = Some(rest.next().context("--limit")?.parse()?),
            other => bail!("unknown argument {other}"),
        }
    }
    Ok(Args {
        trace: trace.context("--trace <file.json.gz>")?,
        variant: variant.context("--variant raw|facts|doc|region-canonical|region-buffered")?,
        flush,
        sync,
        vault_dir,
        dump,
        limit,
    })
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let _entropy = entropy::seed(SEED);
    let args = args()?;
    let mut trace = read_trace(&args.trace)?;
    if let Some(limit) = args.limit {
        // A prefix of the trace: every parent of a kept transaction is
        // earlier, so the prefix is a closed history of its own.
        trace.txns.truncate(limit);
    }
    let mut raw = Vec::new();
    for index in 0..trace.txns.len() {
        raw.extend(encode_change(&trace, index));
    }
    if let Some(path) = &args.dump {
        std::fs::write(path, &raw)?;
    }
    let windows = windows(&trace, args.flush).len();
    let started = Instant::now();
    let result = match args.variant.as_str() {
        "raw" => json!({}),
        "facts" | "doc" => {
            let site = Site::open(&args.vault_dir).await?;
            if args.sync {
                replicated(&trace, &args.variant, args.flush, &site).await?
            } else {
                single(&trace, &args.variant, args.flush, &site).await?
            }
        }
        "region-canonical" => region(&trace, false, args.flush).await?,
        "region-buffered" => region(&trace, true, args.flush).await?,
        other => bail!("unknown variant {other}"),
    };
    let line = json!({
        "trace": args.trace.file_name().map(|name| name.to_string_lossy().into_owned()),
        "variant": args.variant,
        "flush": format!("{:?}", args.flush),
        "sync": args.sync,
        "txns": trace.txns.len(),
        "agents": trace.agents,
        "windows": windows,
        "limit": args.limit,
        "end_content_len": trace.end_content.chars().count(),
        "raw_bytes": raw.len(),
        "fanout_n": dialog_search_tree::Manifest::default().fanout_n,
        "inline_n": dialog_search_tree::Manifest::default().inline_n,
        "result": result,
        "wall_s": started.elapsed().as_secs_f64(),
    });
    println!("{line}");
    Ok(())
}

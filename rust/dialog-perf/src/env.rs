//! The environment a scenario runs in: a session on a volatile peer whose
//! archive traffic is tallied, with one open branch, and the
//! deterministic facts, rules and queries the scenarios are built from.
//!
//! Everything a scenario writes is a function of its size: entities are
//! named by index, the profile and space by fixed names (the storage is
//! volatile and the process runs one scenario, so nothing collides), and
//! the signer is the fixed-seed test system. Two runs of one scenario
//! move the same blocks.

use anyhow::Result;
use dialog_artifacts::{Artifact, Changes, Entity, Instruction, Pick, Relation, Value};
use dialog_capability::Subject;
use dialog_credentials::{Ed25519Signer, SignerCredential};
use dialog_effects::storage::Location;
use dialog_peer::helpers::{Meter, Metered, Tally, open_peer_as, test_storage};
use dialog_peer::{Peer, Session};
use dialog_query::{
    ConceptDescriptor, ConceptQuery, DeductiveRule, DeductiveRuleDescriptor, Parameters, Term,
};
use dialog_repository::{Branch, Repository, RepositoryExt as _};
use dialog_storage::provider::storage::VolatileSpace;

/// The operator every scenario commits and reads through: a session on
/// a volatile peer, its archive traffic tallied beneath every cache.
pub type Operator = Metered<Peer<VolatileSpace, Session>, Probe>;

/// The meter on the operator: a tally, and, with `DIALOG_PERF_BLOCKS`
/// naming a file, a line per block moved (`put`/`get`, its length and
/// its hash) for telling two runs apart block by block.
#[derive(Clone, Default)]
pub struct Probe {
    tally: Tally,
    log: Option<std::sync::Arc<std::sync::Mutex<std::fs::File>>>,
}

impl Probe {
    fn open() -> Self {
        let log = std::env::var("DIALOG_PERF_BLOCKS").ok().map(|path| {
            std::sync::Arc::new(std::sync::Mutex::new(
                std::fs::File::create(path).expect("the block log opens"),
            ))
        });
        Self {
            tally: Tally::default(),
            log,
        }
    }

    fn record(&self, kind: &str, block: &[u8]) {
        use std::io::Write as _;
        if let Some(log) = &self.log {
            let _ = writeln!(
                log.lock().expect("the block log"),
                "{kind} {} {}",
                block.len(),
                blake3::hash(block).to_hex()
            );
        }
    }
}

impl Env {
    /// Note `label` in the block log, so the blocks before and after a
    /// phase can be told apart.
    pub fn mark(&self, label: &str) {
        self.operator.meter().record("mark", label.as_bytes());
    }
}

impl Meter for Probe {
    fn wrote(&self, block: &[u8]) {
        self.tally.wrote(block);
        self.record("put", block);
    }

    fn read(&self, block: &[u8]) {
        self.tally.read(block);
        self.record("get", block);
    }
}

/// The branch every scenario works on. Not `main`: opening the space
/// records its access there (a fresh account key, its encrypted secret
/// and a delegation, random by design), and a scenario's commits must
/// land in the same tree on every run for its block counts to repeat.
const SCENARIO_BRANCH: &str = "scenario";

/// A repository with its scenario branch open, and the operator it is
/// driven through.
pub struct Env {
    /// The operator commits and reads run as.
    pub operator: Operator,
    /// The branch every scenario works on.
    pub branch: Branch,
    repository: Repository,
}

impl Env {
    /// Open a fresh volatile repository and its `main` branch.
    pub async fn open() -> Result<Self> {
        let storage = test_storage().await;
        let credential = SignerCredential::from(Ed25519Signer::import(&[0x5f; 32]).await?);
        let profile = open_peer_as(storage, Location::profile("perf"), credential).await?;
        let session = profile
            .session(b"perf")
            .space(profile.state())
            .allow(Subject::any())
            .await?;
        let operator = Metered::new(session, Probe::open());
        let repository = profile.space("perf").open().perform(&operator).await?;
        let branch = repository
            .branch(SCENARIO_BRANCH)
            .open()
            .perform(&operator)
            .await?;
        Ok(Self {
            operator,
            branch,
            repository,
        })
    }

    /// The branch's head, for telling whether two runs minted the same
    /// revision.
    pub fn head(&self) -> String {
        format!("{:?}", self.branch.revision())
    }

    /// The tally of archive blocks moved through the operator so far.
    pub fn tally(&self) -> &Tally {
        &self.operator.meter().tally
    }

    /// Reopen the branch at its current head, as an application that
    /// commits and then reads through a fresh handle does.
    pub async fn reopen(&mut self) -> Result<()> {
        self.branch = self
            .repository
            .branch(SCENARIO_BRANCH)
            .open()
            .perform(&self.operator)
            .await?;
        Ok(())
    }

    /// Commit `changes` to the branch and publish the head.
    pub async fn commit(&self, changes: Changes) -> Result<()> {
        self.branch
            .transaction()
            .integrate(changes)
            .commit()
            .publish()
            .perform(&self.operator)
            .await?;
        Ok(())
    }

    /// Commit `rules` to the branch in one transaction.
    pub async fn install(&self, rules: &[DeductiveRule]) -> Result<()> {
        let mut transaction = self.branch.transaction();
        for rule in rules {
            transaction = transaction.assert(rule);
        }
        transaction
            .commit()
            .publish()
            .perform(&self.operator)
            .await?;
        Ok(())
    }
}

/// The `index`th entity of the `tag` population: `id:<tag>-<index>`.
pub fn entity(tag: &str, index: usize) -> Entity {
    format!("id:{tag}-{index}")
        .parse()
        .expect("a tag and an index spell an entity")
}

/// The attribute `name` spells.
pub fn attribute(name: &str) -> Relation {
    name.parse().expect("a scenario's attribute names parse")
}

/// A text value.
pub fn text(value: impl Into<String>) -> Value {
    Value::String(value.into())
}

/// The fact `of` `the` `is`.
pub fn fact(of: &Entity, the: &str, is: Value) -> Artifact {
    Artifact {
        the: attribute(the),
        of: of.clone(),
        is,
        cause: None,
    }
}

/// `facts` asserted under `policy`, as one batch.
pub fn assert_all(facts: impl IntoIterator<Item = Artifact>, policy: Pick) -> Changes {
    facts
        .into_iter()
        .map(|artifact| Instruction::Assert(artifact, policy.clone()))
        .collect()
}

/// The concept `json` describes.
pub fn concept(json: serde_json::Value) -> ConceptDescriptor {
    serde_json::from_value(json).expect("a scenario's concept descriptor parses")
}

/// The rule `json` describes, compiled.
pub fn rule(json: serde_json::Value) -> DeductiveRule {
    let descriptor: DeductiveRuleDescriptor =
        serde_json::from_value(json).expect("a scenario's rule descriptor parses");
    descriptor.compile().expect("a scenario's rule compiles")
}

/// A query over `descriptor` with every field in `free` a variable of
/// its own name, and `this` bound to `of` when given.
pub fn query(descriptor: &ConceptDescriptor, free: &[&str], of: Option<&Entity>) -> ConceptQuery {
    let mut terms = Parameters::new();
    match of {
        Some(of) => terms.insert("this".into(), Term::constant(of.clone())),
        None => terms.insert("this".into(), Term::var("this")),
    }
    for field in free {
        terms.insert((*field).into(), Term::var(*field));
    }
    ConceptQuery {
        predicate: descriptor.clone(),
        terms,
    }
}

/// A variable reference in a descriptor's `where`.
pub fn var(name: &str) -> serde_json::Value {
    serde_json::json!({ "?": { "name": name } })
}

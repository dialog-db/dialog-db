//! The dataset file the tonk benchmarks replay, the same file into every
//! store and every build.
//!
//! A dataset is JSON lines, each a [`Line`]. It is made once, by
//! `tonk_dataset` (from a replica of a tonk space, or generated from a
//! seed in that space's shape), and never committed: it holds a space's
//! data. A run names it by the hash of its bytes, so two runs that read
//! different files show it.

use std::collections::BTreeMap;
use std::io::{BufRead as _, BufReader, BufWriter, Write as _};
use std::path::Path;

use dialog_artifacts::Value;
use serde::{Deserialize, Serialize};

/// One line of a dataset.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Line {
    /// What the dataset is: where it came from, its seed and its scale.
    Header {
        /// Where it came from: `export` or `generate`.
        source: String,
        /// The seed it was generated from, 0 for an export.
        seed: u64,
        /// Its scale against the space it is shaped like, 1 for an export.
        scale: u32,
    },
    /// The space's concepts, by name: each a concept descriptor as JSON.
    Concepts {
        /// Descriptor JSON by concept name.
        concepts: BTreeMap<String, serde_json::Value>,
    },
    /// The space's rules and command markers, installed in one commit.
    Schema {
        /// Each inductive rule, as the canonical bytes a branch stores.
        #[serde(with = "hex_list")]
        inductive: Vec<Vec<u8>>,
        /// Each deductive rule, as the canonical bytes a branch stores.
        #[serde(with = "hex_list")]
        deductive: Vec<Vec<u8>>,
        /// The concepts whose facts are commands, by entity.
        transient: Vec<String>,
    },
    /// Facts committed as they are, as a history replays.
    Commit {
        /// What the commit is for.
        phase: Phase,
        /// What it changes, in order.
        changes: Vec<Change>,
    },
    /// How many members write to the space, each a replica and a profile
    /// of their own. An entity `id:member-<n>` anywhere in the dataset
    /// stands for member `n`'s profile, which only a run knows.
    Members {
        /// How many.
        count: u32,
    },
    /// A command: facts of a transient concept, which the space's rules
    /// turn into what the command does when it is committed.
    Command {
        /// What the command is for.
        phase: Phase,
        /// The command concept's name, as the space names it.
        name: String,
        /// Which member sends it.
        author: u32,
        /// The command's facts, dispatched.
        changes: Vec<Change>,
        /// Facts committed with it, as a page asserts beside a command.
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        durable: Vec<Change>,
    },
    /// The end of a round: every member, in turn, pulls what the others
    /// pushed and pushes what it committed.
    Sync,
    /// The shape of the space a schema came from: how many records of each
    /// kind it held and how they linked, with none of the records.
    Shape(Shape),
}

/// What a commit or command in a dataset is for.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Phase {
    /// Building the space a run starts from.
    Setup,
    /// The writes a run measures.
    Writes,
    /// What another replica writes, for a run to pull.
    Remote,
}

/// One change a commit makes.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Change {
    /// What it does.
    pub op: Op,
    /// The attribute.
    pub the: String,
    /// The entity.
    pub of: String,
    /// The value.
    pub is: Value,
}

/// What a [`Change`] does.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Op {
    /// Adds a claim beside those of its cell.
    Add,
    /// Replaces the newest claim of its cell.
    Replace,
    /// Retracts a claim.
    Retract,
}

/// Writes `lines` to `path`, and returns the hash of what it wrote.
pub fn write(path: &Path, lines: impl IntoIterator<Item = Line>) -> anyhow::Result<String> {
    let mut out = BufWriter::new(std::fs::File::create(path)?);
    for line in lines {
        serde_json::to_writer(&mut out, &line)?;
        out.write_all(b"\n")?;
    }
    out.flush()?;
    drop(out);
    hash(path)
}

/// Every line of the dataset at `path`.
pub fn read(path: &Path) -> anyhow::Result<Vec<Line>> {
    let file = BufReader::new(std::fs::File::open(path)?);
    let mut lines = Vec::new();
    for line in file.lines() {
        lines.push(serde_json::from_str(&line?)?);
    }
    Ok(lines)
}

/// The shape a space's records have: how many of each, and how they link.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Shape {
    /// Issues.
    pub issues: usize,
    /// Labels.
    pub labels: usize,
    /// Projects.
    pub projects: usize,
    /// Label links per issue.
    pub labels_per_issue: f64,
    /// The share of issues in a project.
    pub in_project: f64,
    /// The share of issues with an assignee.
    pub assigned: f64,
    /// Comments per issue, changes left out.
    pub comments_per_issue: f64,
    /// Memos.
    pub memos: usize,
    /// Mentions per memo.
    pub mentions_per_memo: f64,
}

/// Byte strings as hex, half the size of JSON's arrays of numbers.
mod hex_list {
    use serde::{Deserialize as _, Deserializer, Serializer, de::Error as _};

    pub fn serialize<S: Serializer>(list: &[Vec<u8>], serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_seq(list.iter().map(|bytes| {
            bytes
                .iter()
                .map(|byte| format!("{byte:02x}"))
                .collect::<String>()
        }))
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Vec<Vec<u8>>, D::Error> {
        Vec::<String>::deserialize(deserializer)?
            .into_iter()
            .map(|text| {
                (0..text.len())
                    .step_by(2)
                    .map(|at| {
                        text.get(at..at + 2)
                            .and_then(|pair| u8::from_str_radix(pair, 16).ok())
                            .ok_or_else(|| D::Error::custom("rule bytes are not hex"))
                    })
                    .collect()
            })
            .collect()
    }
}

/// The profile a dataset's entity stands for, if it is `id:member-<n>`.
pub fn member(entity: &str) -> Option<u32> {
    entity.strip_prefix("id:member-")?.parse().ok()
}

/// The entity a dataset writes for member `n`'s profile.
pub fn member_entity(n: u32) -> String {
    format!("id:member-{n}")
}

/// The hash a run names the dataset at `path` by: the first 16 hex digits
/// of its bytes' blake3 hash.
pub fn hash(path: &Path) -> anyhow::Result<String> {
    let bytes = std::fs::read(path)?;
    let digest = dialog_common::Blake3Hash::hash(&bytes);
    Ok(digest
        .as_bytes()
        .iter()
        .take(8)
        .map(|byte| format!("{byte:02x}"))
        .collect())
}

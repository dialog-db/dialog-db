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
        inductive: Vec<Vec<u8>>,
        /// Each deductive rule, as the canonical bytes a branch stores.
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
    /// A command: facts of a transient concept, which the space's rules
    /// turn into what the command does when it is committed.
    Command {
        /// What the command is for.
        phase: Phase,
        /// The command concept's name, as the space names it.
        name: String,
        /// Which of the dataset's people asks.
        author: u32,
        /// The command's facts.
        changes: Vec<Change>,
    },
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

//! Stack Exchange fact-log replay: the realistic workload.
//!
//! `scripts/se-transform.py` turns a Stack Exchange site dump into a flat,
//! transaction-ordered fact log (`txn,at,the,of,as,is` — see
//! `notes/benchmark-dataset.md`). Rows sharing a `txn` are one commit, and
//! commit boundaries are the site's real ones. This module replays that log
//! against both stores, one commit per transaction, which exercises exactly
//! the shape the synthetic `stuff` workload cannot: small skewed commits
//! (p50 of 1 fact), genuine cardinality-one supersession (44% of pairs are
//! written more than once), and a long-tailed value-size distribution that
//! crosses the spill boundary.
//!
//! Two data sources:
//!
//! - **Real data** when `DIALOG_SE_CSV` points at a transformed dump. This
//!   is the reporting configuration.
//! - **A deterministic synthetic approximation** otherwise, so the bench
//!   runs out of the box. The generator reproduces the *measured* statistics
//!   of the retrocomputing dump documented in `notes/benchmark-dataset.md`
//!   (revision skew p50 3 / p90 7 / p99 12 / max 122; body sizes with ~9%
//!   past the 4096-byte inline threshold; ~36% questions carrying titles and
//!   ~2.5 tags each). It approximates — the real log is the reference.
//!
//! Supersession mapping, kept honest on both sides: a repeated write to a
//! cardinality-one `(entity, attribute)` is [`Instruction::Replace`] in
//! dialog (remove all priors from the three indexes, insert the new value)
//! and `DELETE (of, the)` + `INSERT` inside the same SQLite transaction.
//! Multi-valued attributes (`se.post/tag`) are plain asserts / inserts.

use std::str::FromStr;

use anyhow::{Context, Result};
use dialog_artifacts::{
    Artifact, ArtifactSelector, ArtifactStoreMut, Attribute, Entity, Instruction, Value,
};
use futures_util::stream;
use rand::{Rng, SeedableRng};
use rand_chacha::ChaCha8Rng;

use crate::{DialogFacts, SqliteFacts};

/// A single fact in a Stack Exchange transaction.
#[derive(Clone, Debug)]
pub struct SeFact {
    /// The attribute, e.g. `se.post/body`.
    pub the: String,
    /// The entity, e.g. `post:1`.
    pub of: String,
    /// The value, already reduced to its textual form plus a type tag.
    pub value: SeValue,
}

/// The value types the transformed dump contains (text / entity / boolean).
#[derive(Clone, Debug)]
pub enum SeValue {
    /// A `text` value.
    Text(String),
    /// An `entity` reference value.
    Entity(String),
    /// A `boolean` value.
    Boolean(bool),
}

impl SeValue {
    /// The value rendered as SQLite TEXT.
    pub fn as_sql_text(&self) -> &str {
        match self {
            SeValue::Text(text) => text,
            SeValue::Entity(entity) => entity,
            SeValue::Boolean(true) => "true",
            SeValue::Boolean(false) => "false",
        }
    }

    /// The value as a dialog [`Value`].
    pub fn to_dialog(&self) -> Result<Value> {
        Ok(match self {
            SeValue::Text(text) => Value::String(text.clone()),
            SeValue::Entity(entity) => Value::Entity(Entity::from_str(entity)?),
            SeValue::Boolean(flag) => Value::Boolean(*flag),
        })
    }
}

/// Whether an attribute is multi-valued (asserted, never superseded).
/// In the SE schema only `se.post/tag` accumulates values.
pub fn is_multi_valued(attribute: &str) -> bool {
    attribute == "se.post/tag"
}

/// A transaction-ordered fact log ready to replay.
#[derive(Clone, Debug, Default)]
pub struct SeLog {
    /// Commits in order; each inner vector is one transaction.
    pub transactions: Vec<Vec<SeFact>>,
}

impl SeLog {
    /// Total fact count across all transactions.
    pub fn fact_count(&self) -> usize {
        self.transactions.iter().map(Vec::len).sum()
    }

    /// Load the log used by the benchmarks: the real transformed dump when
    /// `DIALOG_SE_CSV` is set (truncated to `limit` transactions), the
    /// synthetic approximation otherwise.
    pub fn load(limit: usize) -> Result<Self> {
        match std::env::var("DIALOG_SE_CSV") {
            Ok(path) => Self::from_csv_path(&path, limit)
                .with_context(|| format!("loading DIALOG_SE_CSV={path}")),
            Err(_) => Ok(Self::synthetic(limit)),
        }
    }

    /// Parse a transformed dump (`txn,at,the,of,as,is`), keeping the first
    /// `limit` transactions (`usize::MAX` for all).
    pub fn from_csv_path(path: &str, limit: usize) -> Result<Self> {
        let text = std::fs::read_to_string(path)?;
        let mut transactions: Vec<Vec<SeFact>> = Vec::new();
        let mut current_txn: Option<String> = None;
        for record in parse_csv(&text).into_iter().skip(1) {
            if record.len() < 6 {
                continue;
            }
            let [txn, _at, the, of, value_type, is] = &record[..6] else {
                continue;
            };
            if current_txn.as_deref() != Some(txn.as_str()) {
                if transactions.len() == limit {
                    break;
                }
                current_txn = Some(txn.clone());
                transactions.push(Vec::new());
            }
            let value = match value_type.as_str() {
                "text" => SeValue::Text(is.clone()),
                "entity" => SeValue::Entity(is.clone()),
                "boolean" => match bool::from_str(is) {
                    Ok(flag) => SeValue::Boolean(flag),
                    Err(_) => continue,
                },
                _ => continue,
            };
            transactions
                .last_mut()
                .expect("transaction pushed above")
                .push(SeFact {
                    the: the.clone(),
                    of: of.clone(),
                    value,
                });
        }
        Ok(Self { transactions })
    }

    /// Deterministic synthetic approximation of the retrocomputing dump's
    /// measured statistics (see the module docs). Produces exactly
    /// `transaction_count` commits, seeded, so every run replays the same
    /// log.
    pub fn synthetic(transaction_count: usize) -> Self {
        let mut rng = ChaCha8Rng::from_seed([11u8; 32]);
        let mut transactions = Vec::with_capacity(transaction_count);
        let user_pool = (transaction_count / 8).max(4);

        // Interleave post creations with edit revisions of already-created
        // posts, matching the scattered-writes property (commits land across
        // the keyspace, not in one region).
        let mut posts: Vec<(usize, bool)> = Vec::new(); // (post id, is question)
        let mut next_post = 0usize;

        while transactions.len() < transaction_count {
            // Roughly 40% of commits create a post (21,771 entities across
            // 50,553 commits); the rest edit an existing one.
            let create = posts.is_empty() || rng.gen_bool(0.4);
            let mut txn = Vec::new();
            if create {
                next_post += 1;
                let is_question = rng.gen_bool(0.36);
                posts.push((next_post, is_question));
                let post = format!("post:{next_post}");
                txn.push(SeFact {
                    the: "se.post/kind".into(),
                    of: post.clone(),
                    value: SeValue::Text(if is_question { "question" } else { "answer" }.into()),
                });
                txn.push(SeFact {
                    the: "se.post/author".into(),
                    of: post.clone(),
                    value: SeValue::Entity(format!("user:{}", rng.gen_range(1..=user_pool))),
                });
                txn.push(SeFact {
                    the: "se.post/body".into(),
                    of: post.clone(),
                    value: SeValue::Text(body_text(&mut rng)),
                });
                if is_question {
                    txn.push(SeFact {
                        the: "se.post/title".into(),
                        of: post.clone(),
                        value: SeValue::Text(format!("Question number {next_post}?")),
                    });
                    for _ in 0..rng.gen_range(1..=4) {
                        txn.push(SeFact {
                            the: "se.post/tag".into(),
                            of: post.clone(),
                            value: SeValue::Text(format!("tag{}", rng.gen_range(0..64))),
                        });
                    }
                }
            } else {
                // Edit-frequency skew: bias picks toward recent posts a bit,
                // but let any post be edited (the long tail comes from
                // repeatedly re-picking the same busy posts by chance).
                let (post_id, is_question) = posts[rng.gen_range(0..posts.len())];
                let post = format!("post:{post_id}");
                let roll: f64 = rng.r#gen();
                if roll < 0.85 {
                    txn.push(SeFact {
                        the: "se.post/body".into(),
                        of: post,
                        value: SeValue::Text(body_text(&mut rng)),
                    });
                } else if roll < 0.90 && is_question {
                    txn.push(SeFact {
                        the: "se.post/title".into(),
                        of: post,
                        value: SeValue::Text(format!("Question number {post_id} (edited)?")),
                    });
                } else {
                    txn.push(SeFact {
                        the: "se.post/tag".into(),
                        of: post,
                        value: SeValue::Text(format!("tag{}", rng.gen_range(0..64))),
                    });
                }
            }
            transactions.push(txn);
        }
        Self { transactions }
    }
}

/// A body value with the measured long-tailed size distribution: log-normal
/// around a ~700-byte median with roughly 9% of bodies past the 4096-byte
/// inline threshold, clamped at the observed 29,597-byte maximum.
fn body_text(rng: &mut ChaCha8Rng) -> String {
    let normal: f64 = {
        // Box-Muller from two uniforms; avoids a distribution dependency.
        let u1: f64 = rng.gen_range(f64::EPSILON..1.0);
        let u2: f64 = rng.r#gen();
        (-2.0 * u1.ln()).sqrt() * (std::f64::consts::TAU * u2).cos()
    };
    let length = (700.0 * (1.32 * normal).exp()).clamp(8.0, 29_597.0) as usize;
    // Compressible, position-dependent filler; content does not matter, size
    // does.
    let mut body = String::with_capacity(length);
    while body.len() < length {
        body.push_str("lorem ipsum dolor sit amet ");
    }
    body.truncate(length);
    body
}

/// Minimal RFC-4180 parser (quoted fields, doubled quotes, embedded commas
/// and newlines) — post bodies contain both, so naive splitting silently
/// corrupts the log.
fn parse_csv(text: &str) -> Vec<Vec<String>> {
    let mut records = Vec::new();
    let mut record = Vec::new();
    let mut field = String::new();
    let mut in_quotes = false;
    let mut chars = text.chars().peekable();
    while let Some(ch) = chars.next() {
        match ch {
            '"' if in_quotes && chars.peek() == Some(&'"') => {
                field.push('"');
                chars.next();
            }
            '"' => in_quotes = !in_quotes,
            ',' if !in_quotes => record.push(std::mem::take(&mut field)),
            '\n' if !in_quotes => {
                record.push(std::mem::take(&mut field));
                records.push(std::mem::take(&mut record));
            }
            '\r' if !in_quotes => {}
            _ => field.push(ch),
        }
    }
    if !field.is_empty() || !record.is_empty() {
        record.push(field);
        records.push(record);
    }
    records
}

impl SqliteFacts {
    /// Replay the log, one SQLite transaction per commit. Cardinality-one
    /// writes delete the prior `(of, the)` rows before inserting; tag writes
    /// insert alongside.
    pub fn replay_se(&mut self, log: &SeLog) -> Result<()> {
        let connection = self.connection_mut();
        for commit in &log.transactions {
            let tx = connection.transaction()?;
            {
                let mut delete =
                    tx.prepare_cached("DELETE FROM facts WHERE of = ?1 AND the = ?2")?;
                let mut insert = tx.prepare_cached(
                    "INSERT OR REPLACE INTO facts (the, of, val) VALUES (?1, ?2, ?3)",
                )?;
                for fact in commit {
                    if !is_multi_valued(&fact.the) {
                        delete.execute((&fact.of, &fact.the))?;
                    }
                    insert.execute((&fact.the, &fact.of, fact.value.as_sql_text()))?;
                }
            }
            tx.commit()?;
        }
        Ok(())
    }

    /// The current title of a post (point read of a superseded pair).
    pub fn se_title(&self, post: &str) -> Result<Option<String>> {
        let mut statement = self
            .connection()
            .prepare_cached("SELECT val FROM facts WHERE of = ?1 AND the = 'se.post/title'")?;
        let mut result = statement.query((post,))?;
        Ok(match result.next()? {
            Some(found) => Some(found.get(0)?),
            None => None,
        })
    }

    /// All entities whose `se.post/kind` is `kind` (a value-indexed lookup).
    pub fn se_by_kind(&self, kind: &str) -> Result<usize> {
        let mut statement = self
            .connection()
            .prepare_cached("SELECT of FROM facts WHERE the = 'se.post/kind' AND val = ?1")?;
        let mut count = 0;
        let mut result = statement.query((kind,))?;
        while let Some(found) = result.next()? {
            let _entity: String = found.get(0)?;
            count += 1;
        }
        Ok(count)
    }
}

/// One transaction's worth of dialog instructions: cardinality-one writes
/// are [`Instruction::Replace`]; tag writes are asserts. Shared by the
/// fact-store replay below and the repository-layer replay
/// ([`crate::repo::DialogRepo::replay_se`]), so both drive identical
/// instruction streams.
pub fn se_instructions(commit: &[SeFact]) -> Result<Vec<Instruction>> {
    let mut instructions = Vec::with_capacity(commit.len());
    for fact in commit {
        let artifact = Artifact {
            the: Attribute::from_str(&fact.the)?,
            of: Entity::from_str(&fact.of)?,
            is: fact.value.to_dialog()?,
            cause: None,
        };
        instructions.push(if is_multi_valued(&fact.the) {
            Instruction::Assert(artifact)
        } else {
            Instruction::Replace(artifact)
        });
    }
    Ok(instructions)
}

impl DialogFacts {
    /// Replay the log, one dialog commit per transaction. Cardinality-one
    /// writes are [`Instruction::Replace`]; tag writes are asserts.
    pub async fn replay_se(&mut self, log: &SeLog) -> Result<()> {
        for commit in &log.transactions {
            let instructions = se_instructions(commit)?;
            match self {
                Self::Memory(artifacts) => {
                    artifacts.commit(stream::iter(instructions)).await?;
                }
                Self::Disk(artifacts, _) => {
                    artifacts.commit(stream::iter(instructions)).await?;
                }
            }
        }
        Ok(())
    }

    /// The current title of a post (point read of a superseded pair).
    pub async fn se_title(&self, post: &str) -> Result<Option<Value>> {
        let selector = ArtifactSelector::new()
            .the(Attribute::from_str("se.post/title")?)
            .of(Entity::from_str(post)?);
        Ok(self.collect(selector).await?.pop().map(|found| found.is))
    }

    /// All entities whose `se.post/kind` is `kind` (a VAE-indexed lookup).
    pub async fn se_by_kind(&self, kind: &str) -> Result<usize> {
        let selector = ArtifactSelector::new()
            .the(Attribute::from_str("se.post/kind")?)
            .is(Value::String(kind.to_owned()));
        Ok(self.collect(selector).await?.len())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{DialogMode, SqliteMode};

    #[test]
    fn synthetic_log_shape() {
        let log = SeLog::synthetic(200);
        assert_eq!(log.transactions.len(), 200);
        // Some commits are single-fact edits, some are multi-fact creations.
        let sizes: Vec<usize> = log.transactions.iter().map(Vec::len).collect();
        assert!(sizes.contains(&1));
        assert!(sizes.iter().any(|&len| len >= 3));
        // Supersession exists: at least one (of, the) pair written twice.
        let mut seen = std::collections::HashSet::new();
        let mut superseded = false;
        for fact in log.transactions.iter().flatten() {
            if !is_multi_valued(&fact.the) && !seen.insert((fact.of.clone(), fact.the.clone())) {
                superseded = true;
            }
        }
        assert!(superseded);
    }

    #[tokio::test]
    async fn stores_agree_after_replay() -> Result<()> {
        let log = SeLog::synthetic(120);
        let mut sqlite = SqliteFacts::open(SqliteMode::Memory)?;
        sqlite.replay_se(&log)?;
        let mut dialog = DialogFacts::open(DialogMode::Memory).await?;
        dialog.replay_se(&log).await?;

        assert_eq!(
            sqlite.se_by_kind("question")?,
            dialog.se_by_kind("question").await?
        );
        assert_eq!(
            sqlite.se_by_kind("answer")?,
            dialog.se_by_kind("answer").await?
        );
        // A superseded title reads back the same current value.
        let post = log
            .transactions
            .iter()
            .flatten()
            .find(|fact| fact.the == "se.post/title")
            .map(|fact| fact.of.clone())
            .expect("synthetic log contains titles");
        let sqlite_title = sqlite.se_title(&post)?;
        let dialog_title = dialog.se_title(&post).await?;
        match (sqlite_title, dialog_title) {
            (Some(expected), Some(Value::String(actual))) => assert_eq!(expected, actual),
            (expected, actual) => panic!("title mismatch: {expected:?} vs {actual:?}"),
        }
        Ok(())
    }
}

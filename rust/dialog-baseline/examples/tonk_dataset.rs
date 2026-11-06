//! Makes the dataset file the tonk benchmarks replay.
//!
//! ```sh
//! # a replica of a tonk space, as its history wrote it
//! cargo run --release -p dialog-baseline --example tonk_dataset -- \
//!   export path/to/replica/data/main out.jsonl
//! ```
//!
//! An export replays the replica's history region as commits, one per
//! revision, in the order the revisions were made. Dialog's own facts (the
//! `dialog.` namespace) cannot be written by an instruction, so they are
//! split off: the space's rules and command markers become the dataset's
//! [`Line::Schema`], installed through dialog's rule API once the history
//! is in (installed earlier, they would derive the replayed facts again);
//! every other one replays under a namespace of the same length, so its
//! keys keep their size and place. The concepts a space's views query are
//! taken from its concept records.
//!
//! The dataset holds the space's data: write it outside the repository.

#[cfg(not(target_arch = "wasm32"))]
#[path = "tonk/dataset.rs"]
// Shared with `tonk_bench`, which reads what this writes.
#[allow(dead_code)]
mod dataset;

#[cfg(not(target_arch = "wasm32"))]
fn main() -> anyhow::Result<()> {
    native::main()
}

#[cfg(target_arch = "wasm32")]
fn main() {}

#[cfg(not(target_arch = "wasm32"))]
mod native {
    use std::collections::{BTreeMap, HashMap, HashSet};
    use std::path::{Path, PathBuf};
    use std::time::SystemTime;

    use dialog_artifacts::history::{Edition, Record};
    use dialog_artifacts::tree::{ArtifactTree, fetch_spilled};
    use dialog_artifacts::{State, Value, history_region_range};
    use dialog_baseline::nodes::TreeNode;
    use dialog_baseline::replica::Replica;
    use dialog_capability::{Did, Subject};
    use dialog_common::{Blake3Hash, Buffer};
    use dialog_effects::archive::prelude::ArchiveScope;
    use dialog_query::{DeductiveRule, InductiveRule};
    use dialog_search_tree::NodeBody;
    use futures_util::TryStreamExt as _;

    use crate::dataset::{self, Change, Line, Op, Phase};

    pub fn main() -> anyhow::Result<()> {
        let args: Vec<String> = std::env::args().skip(1).collect();
        match args.as_slice() {
            [mode, replica, out] if mode == "export" => {
                let lines = export(Path::new(replica))?;
                let hash = dataset::write(Path::new(out), lines)?;
                eprintln!("wrote {out}, dataset {hash}");
                Ok(())
            }
            _ => anyhow::bail!("usage: tonk_dataset export <replica data dir> <out.jsonl>"),
        }
    }

    /// Every file under `directory`, with when it was last written.
    fn files(directory: &Path) -> anyhow::Result<Vec<(PathBuf, SystemTime)>> {
        let mut out = Vec::new();
        let mut pending = vec![directory.to_path_buf()];
        while let Some(directory) = pending.pop() {
            for entry in std::fs::read_dir(&directory)? {
                let entry = entry?;
                if entry.file_type()?.is_dir() {
                    pending.push(entry.path());
                } else {
                    out.push((entry.path(), entry.metadata()?.modified()?));
                }
            }
        }
        Ok(out)
    }

    /// The replica's blocks, and its head: the tree node no other node
    /// links to that was written last.
    fn blocks(archive: &Path) -> anyhow::Result<(Vec<Buffer>, Blake3Hash)> {
        let mut blocks = Vec::new();
        let mut written = HashMap::new();
        let mut nodes = HashMap::new();
        for (path, modified) in files(archive)? {
            let bytes = Buffer::from(std::fs::read(&path)?);
            let hash = bytes.blake3_hash().clone();
            if let Ok(node) = TreeNode::try_from(bytes.clone()) {
                nodes.insert(hash.clone(), node);
            }
            written.insert(hash, modified);
            blocks.push(bytes);
        }
        let mut linked = HashSet::new();
        for node in nodes.values() {
            if let NodeBody::Index(index) = node.body() {
                for at in 0..index.len() {
                    linked.insert(index.hash_at(at)?.clone());
                }
            }
        }
        let head = nodes
            .keys()
            .filter(|hash| !linked.contains(*hash))
            .max_by_key(|hash| (written[*hash], hash.as_bytes().to_vec()))
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("the archive holds no tree root"))?;
        Ok((blocks, head))
    }

    /// The claims of a replayed history, by revision, with dialog's own
    /// facts split off.
    struct History {
        revisions: BTreeMap<(Edition, [u8; 32]), Vec<Change>>,
        /// Rule sources and command markers, by revision, as they changed.
        reserved: BTreeMap<(Edition, [u8; 32]), Vec<Change>>,
    }

    fn is_schema(the: &str) -> bool {
        the == "dialog.rule/source" || the == "dialog.concept/transient"
    }

    fn export(replica: &Path) -> anyhow::Result<Vec<Line>> {
        let (blocks, head) = blocks(&replica.join("archive"))?;
        let replica = Replica::in_memory(blocks, Some(replica.join("blob")));
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        let mut history = History {
            revisions: BTreeMap::new(),
            reserved: BTreeMap::new(),
        };
        let mut renamed = 0usize;
        let mut stand_ins = 0usize;
        runtime.block_on(async {
            // The replica answers every catalog's reads, so any will do.
            let did: Did = "did:key:zTonkDataset".parse()?;
            let catalog = ArchiveScope::new(Subject::from(did)).index();
            let tree = ArtifactTree::at(catalog.clone(), head.clone());
            let (from, to) = history_region_range();
            let stream = tree.stream_range(from..=to, &replica);
            futures_util::pin_mut!(stream);
            while let Some(entry) = stream.try_next().await? {
                let State::Added(datum) = entry.value else {
                    continue;
                };
                let Some(version) = datum.version else {
                    continue;
                };
                let spilled = match fetch_spilled(&replica, &catalog.archive(), &entry.key).await {
                    Ok(spilled) => spilled,
                    Err(_) => {
                        stand_ins += 1;
                        Some(vec![b'x'; 8192])
                    }
                };
                let record = Record::try_from_key_datum_with_value(&entry.key, datum, spilled)?;
                let claim = record.claim().clone();
                let op = match (record.is_assertion(), claim.cause.versions().is_empty()) {
                    (true, false) => Op::Replace,
                    (true, true) => Op::Add,
                    (false, _) => Op::Retract,
                };
                let the = claim.the.to_string();
                let revision = (version.edition, version.origin.0);
                let mut change = Change {
                    op,
                    the,
                    of: claim.of.to_string(),
                    is: claim.is,
                };
                if is_schema(&change.the) {
                    history.reserved.entry(revision).or_default().push(change);
                    continue;
                }
                if let Some(rest) = change.the.strip_prefix("dialog.") {
                    change.the = format!("dialox.{rest}");
                    renamed += 1;
                }
                history.revisions.entry(revision).or_default().push(change);
            }
            anyhow::Ok(())
        })?;
        eprintln!(
            "history at {head}: {} revisions, {} claims ({renamed} renamed out of dialog's namespace), {stand_ins} spilled values stood in for",
            history.revisions.len(),
            history.revisions.values().map(Vec::len).sum::<usize>(),
        );

        let schema = schema(&history.reserved)?;
        let concepts = concepts(&history.revisions);
        let mut lines = vec![
            Line::Header {
                source: "export".into(),
                seed: 0,
                scale: 1,
            },
            Line::Concepts { concepts },
        ];
        lines.extend(history.revisions.into_values().map(|changes| Line::Commit {
            phase: Phase::Setup,
            changes,
        }));
        lines.push(schema);
        Ok(lines)
    }

    /// The rules and command markers live at the head, applying every
    /// revision's changes in order.
    fn schema(reserved: &BTreeMap<(Edition, [u8; 32]), Vec<Change>>) -> anyhow::Result<Line> {
        let mut live: BTreeMap<(String, String), Value> = BTreeMap::new();
        for change in reserved.values().flatten() {
            let cell = (change.the.clone(), change.of.clone());
            match change.op {
                Op::Add | Op::Replace => {
                    live.insert(cell, change.is.clone());
                }
                Op::Retract => {
                    if live.get(&cell) == Some(&change.is) {
                        live.remove(&cell);
                    }
                }
            }
        }
        let (mut inductive, mut deductive, mut transient) = (Vec::new(), Vec::new(), Vec::new());
        let mut unreadable = 0usize;
        for ((the, of), is) in live {
            match (the.as_str(), is) {
                ("dialog.concept/transient", Value::Boolean(true)) => transient.push(of),
                ("dialog.rule/source", Value::Bytes(bytes)) => {
                    if InductiveRule::decode(&bytes).is_ok() {
                        inductive.push(bytes);
                    } else if DeductiveRule::decode(&bytes).is_ok() {
                        deductive.push(bytes);
                    } else {
                        unreadable += 1;
                    }
                }
                _ => {}
            }
        }
        eprintln!(
            "schema: {} inductive rules, {} deductive, {} command concepts, {unreadable} rules this build cannot read",
            inductive.len(),
            deductive.len(),
            transient.len()
        );
        Ok(Line::Schema {
            inductive,
            deductive,
            transient,
        })
    }

    /// Every cell's claims at the head, applying `changes` in order.
    fn live<'a>(
        changes: impl IntoIterator<Item = &'a Change>,
    ) -> HashMap<(&'a str, &'a str), Vec<&'a Value>> {
        let mut cells: HashMap<(&str, &str), Vec<&Value>> = HashMap::new();
        for change in changes {
            let cell = cells
                .entry((change.the.as_str(), change.of.as_str()))
                .or_default();
            match change.op {
                Op::Add => {
                    if !cell.contains(&&change.is) {
                        cell.push(&change.is);
                    }
                }
                Op::Replace => {
                    cell.clear();
                    cell.push(&change.is);
                }
                Op::Retract => cell.retain(|value| **value != change.is),
            }
        }
        cells
    }

    /// The space's concepts, by name, as its concept records hold them at
    /// the head: a name (`id:<name>`) refers to a concept, whose fields
    /// each name an attribute record.
    fn concepts(
        revisions: &BTreeMap<(Edition, [u8; 32]), Vec<Change>>,
    ) -> BTreeMap<String, serde_json::Value> {
        let cells = live(revisions.values().flatten());
        let one = |the: &str, of: &str| -> Option<String> {
            cells
                .get(&(the, of))
                .and_then(|values| values.last())
                .and_then(|value| match value {
                    Value::String(text) => Some(text.clone()),
                    Value::Entity(entity) => Some(entity.to_string()),
                    Value::Symbol(symbol) => Some(symbol.to_string()),
                    _ => None,
                })
        };
        let mut fields: HashMap<&str, Vec<(&str, &str, bool)>> = HashMap::new();
        for ((the, of), values) in &cells {
            let (field, optional) = if let Some(field) = the.strip_prefix("db.concept.with/") {
                (field, false)
            } else if let Some(field) = the.strip_prefix("db.concept.optional/") {
                (field, true)
            } else {
                continue;
            };
            if let Some(Value::Entity(attribute)) = values.last() {
                let attribute: &str = Box::leak(attribute.to_string().into_boxed_str());
                fields
                    .entry(of)
                    .or_default()
                    .push((field, attribute, optional));
            }
        }
        let mut concepts = BTreeMap::new();
        let mut unreadable = Vec::new();
        for ((the, of), values) in &cells {
            if *the != "db.name/referent" {
                continue;
            }
            let (Some(name), Some(Value::Entity(concept))) =
                (of.strip_prefix("id:"), values.last())
            else {
                continue;
            };
            let concept = concept.to_string();
            let Some(fields) = fields.get(concept.as_str()) else {
                continue;
            };
            let mut with = serde_json::Map::new();
            for (field, attribute, optional) in fields {
                let mut descriptor = serde_json::Map::new();
                if let Some(id) = one("db.attribute/id", attribute) {
                    descriptor.insert("the".into(), id.into());
                }
                if let Some(kind) = one("db.attribute/as", attribute) {
                    descriptor.insert("as".into(), kind.into());
                }
                if let Some(pick) = one("db.attribute/pick", attribute) {
                    descriptor.insert("pick".into(), pick.into());
                }
                if *optional {
                    descriptor.insert("optional".into(), true.into());
                }
                with.insert((*field).into(), descriptor.into());
            }
            let descriptor = serde_json::json!({ "with": with });
            match serde_json::from_value::<dialog_query::ConceptDescriptor>(descriptor.clone()) {
                Ok(_) => {
                    concepts.insert(name.to_string(), descriptor);
                }
                Err(error) => unreadable.push(format!("{name}: {error}")),
            }
        }
        eprintln!(
            "concepts: {} by name, {} this build cannot read {unreadable:?}",
            concepts.len(),
            unreadable.len()
        );
        concepts
    }
}

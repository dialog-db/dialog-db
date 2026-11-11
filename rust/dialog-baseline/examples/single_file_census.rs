//! Single-file census: what a replica's archive would weigh in one
//! append-only file, under several encodings, against file-per-block, and
//! what each costs to read a block back.
//!
//! The spike for `research/single-file-format/idea.md`. It replays the Stack
//! Exchange log through an in-memory branch, keeps every distinct block the
//! archive is asked to store (in write order, tagged with the commit that
//! wrote it), and then models the stores side by side on the same bytes:
//!
//! - **fs**: file-per-block as the `fs` provider lays it out, by content
//!   length and rounded up to a 4 KiB filesystem block.
//! - **log**: every block appended to one file, with a get-by-hash index
//!   entry (32-byte hash, 8-byte offset) each. What DCAA built.
//! - **log+zstd**: the same with each block compressed on its own, so any
//!   block still reads without touching another.
//! - **log+delta/K**: each tree node compressed against the node it
//!   supersedes (same depth below the root, same left separator, previous
//!   version), with a whole (zstd) keyframe every `K` versions so a read
//!   decodes at most `K` blocks. `K = inf` never resets the chain.
//! - **tokens+frames**: the idea. Every key component and every value is a
//!   token written once, at its first appearance; a node is a frame of
//!   4-byte offsets into the tokens, and an index frame points at its
//!   children's frames by offset instead of carrying their hashes. A frame
//!   starts with its own 32-byte hash, so the get-by-hash index needs only
//!   a fingerprint and an offset (16 bytes) per frame, and the token
//!   dictionary a fingerprint and an offset (12 bytes) per distinct token.
//!   These are real bytes: every frame is decoded back and re-encoded with
//!   the tree's own encoder, and the census counts how many come out as
//!   the original block, byte for byte. A block an older encoder wrote may
//!   not: the frame rebuilds it with today's.
//! - **tokens+zstd frames**, **tokens+delta frames/K**: the idea with each
//!   frame compressed alone, or against the frame it supersedes.
//! - **stream**: the whole log through one zstd stream with long-distance
//!   matching. No random access; a floor for what any encoding of these
//!   bytes can reach.
//! - **columnar/K**: the idea with our own encoding instead of zstd. A
//!   frame is rows of dense token ids (and child frame positions) written
//!   column by column as zigzag-delta varints. Against the frame it
//!   supersedes, a section is written as runs copied from the
//!   predecessor's rows and literal rows, with a whole frame every `K`
//!   versions. Writes are timed (encode and append, next to a file per
//!   block and a plain append), and reads are timed both as decoding the
//!   rows and as realizing the block from them.
//!
//! zstd models stay as references for how much redundancy there is; they
//! say nothing about what our own encoding costs to write or read.
//!
//! Every model is reported cumulatively at checkpoints, next to the live
//! tree (the nodes reachable from that commit's root), so the growth with
//! commits is visible as well as the size. After the last checkpoint the
//! census times reading every live node back under each model.
//!
//! It reads its blocks from one of two places:
//!
//! - **a replay** (`<commits> [checkpoints]`): the first `commits`
//!   Stack Exchange commits, through an in-memory branch, with the commit
//!   that wrote each block known exactly.
//! - **an archive directory** (`archive <directory> [checkpoints]`): a
//!   real replica's file-per-block store, every file under the directory.
//!   The commit order is reconstructed from the files (see [`scan`]), so
//!   point it at a replica that wrote its own history, or set
//!   `DIALOG_CENSUS_ORDER=history` to order roots by the history records
//!   under them (for blocks fetched from a remote). `disk <commits>`
//!   writes a replay to disk for checking this mode against the replay.
//!
//! Checkpoints count commits; they default to every quarter.
//!
//! ```sh
//! DIALOG_SE_CSV=path/to/retro-facts.csv cargo run --release -p dialog-baseline \
//!   --example single_file_census -- 4096 512,1024,2048,4096
//! cargo run --release -p dialog-baseline --example single_file_census -- \
//!   archive path/to/replica/archive
//! ```

use std::collections::{BTreeMap, HashMap, HashSet};
use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::str::FromStr as _;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime};

use dialog_baseline::metered::Meter;
use dialog_baseline::nodes::TreeNode;
use dialog_baseline::replica::Replica;
use dialog_baseline::repo::DialogRepo;
use dialog_baseline::se::{SeLog, se_instructions};
use dialog_capability::{Did, Subject};
use dialog_common::{Blake3Hash, Buffer};
use dialog_corpus::history::{Edition, Record};
use dialog_corpus::tree::{Corpus, fetch_spilled};
use dialog_corpus::{
    ATTRIBUTE_KEY_TAG, BLOB_KEY_TAG, Datum, ENTITY_KEY_TAG, Fact, Instruction, Key, Pick, Relation,
    State, VALUE_KEY_TAG, history_region_range,
};
use dialog_effects::archive::prelude::ArchiveScope;
use dialog_search_tree::{
    Entry, Key as TreeKey, Link, Manifest, NodeBody, NoveltyBuffer, NoveltyEntry, NoveltyOp,
    PersistentNodeBody, PersistentSegment, Scale, Separator, into_owned,
};
use futures_util::TryStreamExt as _;

/// Get-by-hash index entry per block in the log models: the hash and an
/// 8-byte offset.
const HASH_INDEX: usize = 32 + 8;
/// Get-by-hash index entry per frame in the tokenized model: the frame
/// carries its hash, so the index holds a fingerprint and an offset.
const FRAME_INDEX: usize = 8 + 8;
/// Token dictionary entry per distinct token: a fingerprint and an offset,
/// for finding whether a token is already in the file.
const TOKEN_INDEX: usize = 8 + 4;
/// Filesystem allocation unit the `fs` provider's files round up to.
const FS_BLOCK: usize = 4096;
/// zstd level for every compressed model.
const LEVEL: i32 = 3;
/// Keyframe intervals for the delta models; `usize::MAX` never resets.
const INTERVALS: [usize; 3] = [8, 32, usize::MAX];
/// The child offset marking a link to a node the file does not hold; the
/// child's 32-byte hash follows it.
const ABSENT: u32 = u32::MAX;
/// The entry form of a key stored as one opaque token.
const FORM_OPAQUE: u8 = 0xFE;

/// One distinct block the archive was asked to store.
struct Written {
    hash: Blake3Hash,
    bytes: Buffer,
    commit: usize,
}

/// Every distinct block, in write order, tagged with the commit that wrote
/// it.
#[derive(Clone, Default)]
struct Recorder {
    commit: Arc<AtomicUsize>,
    blocks: Arc<Mutex<(HashSet<Blake3Hash>, Vec<Written>)>>,
}

impl Meter for Recorder {
    fn wrote(&self, block: &[u8]) {
        let bytes = Buffer::from(block.to_vec());
        let hash = bytes.blake3_hash().clone();
        let mut blocks = self.blocks.lock().expect("recorder lock");
        if blocks.0.insert(hash.clone()) {
            blocks.1.push(Written {
                hash,
                bytes,
                commit: self.commit.load(Ordering::Relaxed),
            });
        }
    }

    fn read(&self, _: &[u8]) {}
}

fn put_varint(out: &mut Vec<u8>, mut value: usize) {
    while value >= 0x80 {
        out.push((value as u8) | 0x80);
        value >>= 7;
    }
    out.push(value as u8);
}

fn get_varint(bytes: &[u8], at: &mut usize) -> usize {
    let (mut value, mut shift) = (0usize, 0);
    loop {
        let byte = bytes[*at];
        *at += 1;
        value |= ((byte & 0x7F) as usize) << shift;
        if byte < 0x80 {
            return value;
        }
        shift += 7;
    }
}

/// The token region: each distinct token once, length-prefixed, addressed
/// by its offset.
#[derive(Default)]
struct Tokens {
    /// Each distinct token's offset in the region and its dense id.
    offsets: HashMap<Vec<u8>, (u32, u32)>,
    /// Region offset by dense id.
    starts: Vec<u32>,
    region: Vec<u8>,
    /// Distinct tokens and their bytes, per kind.
    by_kind: HashMap<&'static str, (usize, usize)>,
    /// References made to each kind, distinct or not.
    references: HashMap<&'static str, usize>,
}

impl Tokens {
    fn intern(&mut self, kind: &'static str, token: &[u8]) -> u32 {
        *self.references.entry(kind).or_default() += 1;
        self.add(kind, token).0
    }

    /// The token's offset and dense id, adding it at the end of the region
    /// when it is new. Unlike [`Tokens::intern`] it does not count a
    /// reference, so a second model over the same tokens leaves the counts
    /// alone.
    fn add(&mut self, kind: &'static str, token: &[u8]) -> (u32, u32) {
        if let Some(found) = self.offsets.get(token) {
            return *found;
        }
        let offset = self.region.len() as u32;
        let id = self.starts.len() as u32;
        put_varint(&mut self.region, token.len());
        self.region.extend_from_slice(token);
        let share = self.by_kind.entry(kind).or_default();
        share.0 += 1;
        share.1 += self.region.len() - offset as usize;
        self.offsets.insert(token.to_vec(), (offset, id));
        self.starts.push(offset);
        (offset, id)
    }

    fn id(&mut self, kind: &'static str, token: &[u8]) -> u32 {
        self.add(kind, token).1
    }

    fn by_id(&self, id: u32) -> &[u8] {
        self.get(self.starts[id as usize])
    }

    fn get(&self, offset: u32) -> &[u8] {
        let mut at = offset as usize;
        let len = get_varint(&self.region, &mut at);
        &self.region[at..at + len]
    }

    fn count(&self) -> usize {
        self.offsets.len()
    }
}

/// Splits a key into its form and the tokens a frame slot points at: the
/// entity, the attribute, and the value (its type byte and payload
/// together, so the same value is one token in every ordering). Keys of
/// any other layout, or that do not split, are one opaque token.
fn key_tokens(key: &[u8]) -> (u8, Vec<(&'static str, Vec<u8>)>) {
    let layout = <Key as TreeKey>::layout_of(key).unwrap_or(u8::MAX);
    let mut parts = Vec::new();
    if <Key as TreeKey>::components_of(key, layout, &mut parts).is_err() {
        return (FORM_OPAQUE, vec![("opaque", key.to_vec())]);
    }
    let joined = |a: &[u8], b: &[u8]| [a, b].concat();
    match (layout, parts.as_slice()) {
        (ENTITY_KEY_TAG, [_, entity, attribute, kind, payload])
        | (ATTRIBUTE_KEY_TAG, [_, attribute, entity, kind, payload])
        | (VALUE_KEY_TAG, [_, kind, payload, attribute, entity]) => (
            layout,
            vec![
                ("entity", entity.to_vec()),
                ("attribute", attribute.to_vec()),
                ("value", joined(kind, payload)),
            ],
        ),
        (BLOB_KEY_TAG, [_, hash]) => (BLOB_KEY_TAG, vec![("blob", hash.to_vec())]),
        _ => (FORM_OPAQUE, vec![("opaque", key.to_vec())]),
    }
}

/// Rebuilds a key from its form and tokens: the inverse of [`key_tokens`].
fn key_from_tokens(form: u8, tokens: &[&[u8]]) -> Vec<u8> {
    match (form, tokens) {
        (ENTITY_KEY_TAG, [entity, attribute, value]) => {
            [&[form][..], entity, attribute, value].concat()
        }
        (ATTRIBUTE_KEY_TAG, [entity, attribute, value]) => {
            [&[form][..], attribute, entity, value].concat()
        }
        (VALUE_KEY_TAG, [entity, attribute, value]) => {
            [&[form][..], value, attribute, entity].concat()
        }
        (BLOB_KEY_TAG, [hash]) => [&[form][..], hash].concat(),
        (_, [key]) => key.to_vec(),
        _ => unreachable!("a form always has its slots"),
    }
}

fn slots_of(form: u8) -> usize {
    match form {
        ENTITY_KEY_TAG | ATTRIBUTE_KEY_TAG | VALUE_KEY_TAG => 3,
        _ => 1,
    }
}

fn datum_bytes(value: &State<Datum>) -> anyhow::Result<Vec<u8>> {
    Ok(rkyv::to_bytes::<rkyv::rancor::Error>(value)?.to_vec())
}

fn datum_from(bytes: &[u8]) -> anyhow::Result<State<Datum>> {
    let mut aligned = rkyv::util::AlignedVec::<16>::with_capacity(bytes.len());
    aligned.extend_from_slice(bytes);
    Ok(rkyv::from_bytes::<State<Datum>, rkyv::rancor::Error>(
        &aligned,
    )?)
}

/// What the frames written hold, next to the blocks they stand for.
#[derive(Default, Clone, Copy)]
struct FrameShare {
    index_frames: usize,
    index_bytes: usize,
    index_blocks: usize,
    segment_frames: usize,
    segment_bytes: usize,
    segment_blocks: usize,
    novelty_entries: usize,
    segment_entries: usize,
}

/// Appends one entry: its form, an offset per key token, then the value's.
fn put_entry(
    out: &mut Vec<u8>,
    tokens: &mut Tokens,
    key: &[u8],
    datum: Option<&State<Datum>>,
) -> anyhow::Result<()> {
    let (form, parts) = key_tokens(key);
    out.push(form);
    for (kind, token) in parts {
        out.extend_from_slice(&tokens.intern(kind, &token).to_le_bytes());
    }
    if let Some(datum) = datum {
        let bytes = datum_bytes(datum)?;
        out.extend_from_slice(&tokens.intern("datum", &bytes).to_le_bytes());
    }
    Ok(())
}

fn get_u32(bytes: &[u8], at: &mut usize) -> u32 {
    let value = u32::from_le_bytes(bytes[*at..*at + 4].try_into().expect("four bytes"));
    *at += 4;
    value
}

/// Reads one entry back: its key and, when `with_datum`, its value.
fn get_entry(
    frame: &[u8],
    at: &mut usize,
    tokens: &Tokens,
    with_datum: bool,
) -> anyhow::Result<(Vec<u8>, Option<State<Datum>>)> {
    let form = frame[*at];
    *at += 1;
    let parts: Vec<&[u8]> = (0..slots_of(form))
        .map(|_| tokens.get(get_u32(frame, at)))
        .collect();
    let key = key_from_tokens(form, &parts);
    let datum = if with_datum {
        Some(datum_from(tokens.get(get_u32(frame, at)))?)
    } else {
        None
    };
    Ok((key, datum))
}

/// Encodes `node` as a frame of offsets: what the idea stores in place of
/// the block. Child links point at the children's frames.
fn frame(
    hash: &Blake3Hash,
    node: &TreeNode,
    tokens: &mut Tokens,
    frames: &HashMap<Blake3Hash, u32>,
    share: &mut FrameShare,
) -> anyhow::Result<Vec<u8>> {
    let mut out = Vec::new();
    out.extend_from_slice(hash.as_bytes());
    match node.body() {
        NodeBody::Index(index) => {
            out.push(1);
            out.extend_from_slice(&(index.len() as u32).to_le_bytes());
            for at in 0..index.len() {
                let separator = index.separator(at)?;
                out.extend_from_slice(&tokens.intern("separator", &separator).to_le_bytes());
                // A child the replica does not hold (a lazily fetched
                // replica holds only what it read) has no frame to point
                // at, so the link carries the child's hash instead.
                let child = index.hash_at(at)?;
                match frames.get(child) {
                    Some(offset) => out.extend_from_slice(&offset.to_le_bytes()),
                    None => {
                        out.extend_from_slice(&ABSENT.to_le_bytes());
                        out.extend_from_slice(child.as_bytes());
                    }
                }
                out.push(index.scale_at(at)?.as_u8());
            }
            out.extend_from_slice(&(index.buffers().count() as u32).to_le_bytes());
            for buffer in index.buffers() {
                let entries = buffer.entries::<Key>()?;
                out.extend_from_slice(&buffer.child().to_le_bytes());
                out.extend_from_slice(&(entries.len() as u32).to_le_bytes());
                for item in &entries {
                    share.novelty_entries += 1;
                    match &item.op {
                        NoveltyOp::Assert(value) => {
                            out.push(1);
                            put_entry(&mut out, tokens, &item.key, Some(value))?;
                        }
                        NoveltyOp::Retract => {
                            out.push(0);
                            put_entry(&mut out, tokens, &item.key, None)?;
                        }
                    }
                }
            }
            share.index_frames += 1;
            share.index_bytes += out.len();
            share.index_blocks += node
                .buffer()
                .expect("a node read from its block")
                .as_ref()
                .len();
        }
        NodeBody::Segment(segment) => {
            out.push(0);
            out.extend_from_slice(&(segment.len() as u32).to_le_bytes());
            let mut keys = segment.keys::<Key>()?;
            while let Some((at, key)) = keys.next_key()? {
                let value: State<Datum> = into_owned(segment.value_at(at)?)?;
                share.segment_entries += 1;
                put_entry(&mut out, tokens, key, Some(&value))?;
            }
            share.segment_frames += 1;
            share.segment_bytes += out.len();
            share.segment_blocks += node
                .buffer()
                .expect("a node read from its block")
                .as_ref()
                .len();
        }
    }
    Ok(out)
}

/// The frames written so far, addressed by their offset in the file.
#[derive(Default)]
struct Frames {
    frames: Vec<Vec<u8>>,
    by_offset: HashMap<u32, usize>,
    by_hash: HashMap<Blake3Hash, u32>,
    len: u32,
}

impl Frames {
    fn push(&mut self, hash: &Blake3Hash, frame: Vec<u8>) {
        self.by_hash.insert(hash.clone(), self.len);
        self.by_offset.insert(self.len, self.frames.len());
        self.len += frame.len() as u32;
        self.frames.push(frame);
    }

    fn at(&self, offset: u32) -> &[u8] {
        &self.frames[self.by_offset[&offset]]
    }

    fn of(&self, hash: &Blake3Hash) -> Option<&[u8]> {
        self.by_hash.get(hash).map(|offset| self.at(*offset))
    }
}

/// Realizes the wire block a frame stands for: decodes its entries from
/// the tokens and encodes them with the tree's own encoder. A child link's
/// hash is read from the head of the child's frame.
fn realize(
    frame: &[u8],
    tokens: &Tokens,
    frames: &Frames,
    manifest: &Manifest,
) -> anyhow::Result<Vec<u8>> {
    let mut at = 32;
    let kind = frame[at];
    at += 1;
    let count = get_u32(frame, &mut at) as usize;
    let body = if kind == 1 {
        let mut links = Vec::with_capacity(count);
        for _ in 0..count {
            let separator = tokens.get(get_u32(frame, &mut at)).to_vec();
            let offset = get_u32(frame, &mut at);
            let child = if offset == ABSENT {
                at += 32;
                &frame[at - 32..at]
            } else {
                &frames.at(offset)[..32]
            };
            let node = Blake3Hash::from(<[u8; 32]>::try_from(child)?);
            let scale = Scale::from_u8(frame[at]);
            at += 1;
            links.push(Link {
                separator: Separator::from(separator),
                node,
                scale,
            });
        }
        let buffers = get_u32(frame, &mut at) as usize;
        let mut novelty = Vec::with_capacity(buffers);
        for _ in 0..buffers {
            let child = get_u32(frame, &mut at);
            let entries = get_u32(frame, &mut at) as usize;
            let mut ops = Vec::with_capacity(entries);
            for _ in 0..entries {
                let assert = frame[at] == 1;
                at += 1;
                let (key, datum) = get_entry(frame, &mut at, tokens, assert)?;
                let op = match datum {
                    Some(value) => NoveltyOp::Assert(value),
                    None => NoveltyOp::Retract,
                };
                ops.push(NoveltyEntry { key, op });
            }
            novelty.push(NoveltyBuffer::from_entries::<Key>(child, ops)?);
        }
        PersistentNodeBody::index_from_buffers(links, novelty, manifest.clone())?
    } else {
        let mut entries = Vec::with_capacity(count);
        for _ in 0..count {
            let (key, datum) = get_entry(frame, &mut at, tokens, true)?;
            entries.push(Entry {
                key: <Key as TreeKey>::try_from_bytes(&key)?,
                value: datum.expect("a segment entry has a value"),
            });
        }
        PersistentNodeBody::from_segment(
            PersistentSegment::from_entries::<Key>(entries)?,
            manifest.clone(),
        )
    };
    Ok(body.as_bytes()?.to_vec())
}

fn zstd_alone(bytes: &[u8]) -> anyhow::Result<Vec<u8>> {
    Ok(zstd::bulk::compress(bytes, LEVEL)?)
}

fn zstd_against(bytes: &[u8], base: &[u8]) -> anyhow::Result<Vec<u8>> {
    let mut compressor = zstd::bulk::Compressor::with_dictionary(LEVEL, base)?;
    Ok(compressor.compress(bytes)?)
}

fn unzstd(bytes: &[u8], base: Option<&[u8]>, capacity: usize) -> anyhow::Result<Vec<u8>> {
    let mut decompressor = match base {
        Some(base) => zstd::bulk::Decompressor::with_dictionary(base)?,
        None => zstd::bulk::Decompressor::new()?,
    };
    Ok(decompressor.decompress(bytes, capacity)?)
}

/// A delta-coded store with a keyframe every `interval` versions: what it
/// holds and how to read a block back.
struct DeltaStore {
    interval: usize,
    /// Per block: its encoded bytes, its depth in the chain, and the block
    /// it is a delta against (none for a keyframe).
    entries: HashMap<Blake3Hash, (Vec<u8>, usize, Option<Blake3Hash>)>,
    bytes: usize,
}

impl DeltaStore {
    fn new(interval: usize) -> Self {
        Self {
            interval,
            entries: HashMap::new(),
            bytes: 0,
        }
    }

    /// Stores `bytes`, as a delta against `base` when that keeps the chain
    /// within the interval and is smaller than the keyframe `alone`.
    fn put(
        &mut self,
        hash: &Blake3Hash,
        bytes: &[u8],
        alone: &[u8],
        base: Option<(&Blake3Hash, &[u8])>,
    ) -> anyhow::Result<()> {
        let chained = base.and_then(|(previous, base_bytes)| {
            let depth = self.entries.get(previous)?.1 + 1;
            (depth < self.interval).then_some((previous, base_bytes, depth))
        });
        let entry = match chained {
            Some((previous, base_bytes, depth)) => {
                let delta = zstd_against(bytes, base_bytes)?;
                if delta.len() < alone.len() {
                    (delta, depth, Some(previous.clone()))
                } else {
                    (alone.to_vec(), 0, None)
                }
            }
            None => (alone.to_vec(), 0, None),
        };
        self.bytes += entry.0.len();
        self.entries.insert(hash.clone(), entry);
        Ok(())
    }

    /// Reads `hash` back by decoding its chain from the keyframe.
    fn get(&self, hash: &Blake3Hash, capacity: usize) -> anyhow::Result<Vec<u8>> {
        let mut chain = vec![hash];
        while let Some((_, _, Some(base))) = self.entries.get(chain[chain.len() - 1]) {
            chain.push(base);
        }
        let mut bytes: Option<Vec<u8>> = None;
        for link in chain.into_iter().rev() {
            let (encoded, _, _) = &self.entries[link];
            bytes = Some(unzstd(encoded, bytes.as_deref(), capacity)?);
        }
        Ok(bytes.expect("a chain has at least the block itself"))
    }
}

/// Running totals of the models kept outside a [`DeltaStore`], in bytes.
#[derive(Default, Clone, Copy)]
struct Totals {
    blocks: usize,
    content: usize,
    fs_allocated: usize,
    log: usize,
    log_zstd: usize,
    tokens: usize,
    token_index: usize,
    frames: usize,
    frames_zstd: usize,
    /// Blocks that are not tree nodes, which every model stores as they
    /// are.
    other_blocks: usize,
    other_bytes: usize,
}

impl Totals {
    /// The idea's file with frames that weigh `frames`: the tokens and
    /// their dictionary, the frames, and the get-by-hash index.
    fn tokenized(&self, frames: usize) -> usize {
        let nodes = self.blocks - self.other_blocks;
        self.tokens
            + self.token_index
            + frames
            + nodes * FRAME_INDEX
            + self.other_bytes
            + self.other_blocks * HASH_INDEX
    }
}

fn mib(bytes: usize) -> String {
    format!("{:.1}", bytes as f64 / (1024.0 * 1024.0))
}

fn interval(k: usize) -> String {
    if k == usize::MAX {
        "inf".into()
    } else {
        k.to_string()
    }
}

/// The nodes reachable from `root`, and their bytes and leaf entries.
fn live(
    root: &Blake3Hash,
    nodes: &HashMap<Blake3Hash, TreeNode>,
) -> anyhow::Result<(Vec<Blake3Hash>, usize, usize)> {
    let (mut reached, mut bytes, mut entries) = (Vec::new(), 0, 0);
    let mut stack = vec![root.clone()];
    while let Some(hash) = stack.pop() {
        // A partial replica holds only the nodes it read; the live tree
        // is what of it the replica holds.
        let Some(node) = nodes.get(&hash) else {
            continue;
        };
        bytes += node
            .buffer()
            .expect("a node read from its block")
            .as_ref()
            .len();
        match node.body() {
            NodeBody::Index(index) => {
                for at in 0..index.len() {
                    stack.push(index.hash_at(at)?.clone());
                }
            }
            NodeBody::Segment(segment) => entries += segment.len(),
        }
        reached.push(hash);
    }
    Ok((reached, bytes, entries))
}

/// Every model's size at one checkpoint.
struct Checkpoint<'a> {
    commit: usize,
    totals: &'a Totals,
    log_deltas: &'a [DeltaStore],
    frame_deltas: &'a [DeltaStore],
    columns: &'a [ColumnStore],
    stream: usize,
}

fn header() -> String {
    let deltas = |prefix: &str| {
        INTERVALS
            .iter()
            .map(|k| format!("{prefix}{}_mib", interval(*k)))
            .collect::<Vec<_>>()
            .join(",")
    };
    format!(
        "commits,live_nodes,live_mib,live_entries,blocks,fs_mib,fs_alloc_mib,log_mib,log_zstd_mib,{},tokens_mib,frames_mib,tokenized_mib,tokenized_zstd_mib,{},stream_mib,{}",
        deltas("log_delta"),
        deltas("tokenized_delta"),
        COLUMN_INTERVALS
            .iter()
            .map(|k| format!("columnar{k}_mib"))
            .collect::<Vec<_>>()
            .join(","),
    )
}

fn row(
    at: &Checkpoint<'_>,
    roots: &[Blake3Hash],
    nodes: &HashMap<Blake3Hash, TreeNode>,
) -> anyhow::Result<String> {
    let (reached, live_bytes, live_entries) = live(&roots[at.commit - 1], nodes)?;
    let totals = at.totals;
    let index = totals.blocks * HASH_INDEX;
    let deltas = |stores: &[DeltaStore], size: &dyn Fn(usize) -> usize| {
        stores
            .iter()
            .map(|store| mib(size(store.bytes)))
            .collect::<Vec<_>>()
            .join(",")
    };
    Ok(format!(
        "{},{},{},{live_entries},{},{},{},{},{},{},{},{},{},{},{},{},{}",
        at.commit,
        reached.len(),
        mib(live_bytes),
        totals.blocks,
        mib(totals.content),
        mib(totals.fs_allocated),
        mib(totals.log),
        mib(totals.log_zstd),
        deltas(at.log_deltas, &|bytes| bytes + index),
        mib(totals.tokens + totals.token_index),
        mib(totals.frames),
        mib(totals.tokenized(totals.frames)),
        mib(totals.tokenized(totals.frames_zstd)),
        deltas(at.frame_deltas, &|bytes| totals.tokenized(bytes)),
        mib(at.stream),
        at.columns
            .iter()
            .map(|store| mib(totals.tokenized(store.bytes)))
            .collect::<Vec<_>>()
            .join(","),
    ))
}

/// The blocks a census measures, in the order a single-file writer would
/// append them, with the root of every commit and every block that parses
/// as a tree node.
struct Source {
    blocks: Vec<Written>,
    roots: Vec<Blake3Hash>,
    nodes: HashMap<Blake3Hash, TreeNode>,
}

fn parse_nodes<'a>(blocks: impl IntoIterator<Item = &'a Written>) -> HashMap<Blake3Hash, TreeNode> {
    blocks
        .into_iter()
        .filter_map(|block| {
            TreeNode::try_from(block.bytes.clone())
                .ok()
                .map(|node| (block.hash.clone(), node))
        })
        .collect()
}

/// Replays the first `count` Stack Exchange commits through an in-memory
/// branch, recording every block its archive is asked to store.
fn replay(count: usize) -> anyhow::Result<Source> {
    let log = SeLog::load(count)?;
    let commits = log
        .transactions
        .iter()
        .map(|commit| se_instructions(commit))
        .collect::<anyhow::Result<Vec<_>>>()?;
    replay_commits(commits)
}

/// Commits each batch of `commits` in turn through an in-memory branch,
/// recording every block its archive is asked to store.
fn replay_commits(commits: Vec<Vec<Instruction>>) -> anyhow::Result<Source> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    let recorder = Recorder::default();
    let mut roots: Vec<Blake3Hash> = Vec::with_capacity(commits.len());
    let facts: usize = commits.iter().map(Vec::len).sum();
    let count = commits.len();
    let started = Instant::now();
    runtime.block_on(async {
        let repo = DialogRepo::metered(recorder.clone()).await?;
        for (at, commit) in commits.into_iter().enumerate() {
            recorder.commit.store(at + 1, Ordering::Relaxed);
            repo.branch()
                .transaction()
                .integrate(commit.into_iter().collect())
                .commit()
                .allow_empty()
                .publish()
                .perform(repo.operator())
                .await?;
            roots.push(repo.root().expect("a committed branch has a tree"));
        }
        anyhow::Ok(())
    })?;
    eprintln!(
        "replayed {count} commits ({facts} instructions) in {:.1}s",
        started.elapsed().as_secs_f64()
    );
    let written = std::mem::take(&mut recorder.blocks.lock().expect("recorder lock").1);
    let nodes = parse_nodes(&written);
    if let Some(block) = written
        .iter()
        .find(|block| !nodes.contains_key(&block.hash))
    {
        anyhow::bail!("block {} is not a tree node", block.hash);
    }
    let blocks = children_first(written, &nodes)?;
    Ok(Source {
        blocks,
        roots,
        nodes,
    })
}

/// Replays the first `count` Stack Exchange commits into a file-per-block
/// repository on disk and prints where it is, so the `archive` mode can be
/// checked against the replay it came from.
fn write_disk(count: usize) -> anyhow::Result<()> {
    let log = SeLog::load(count)?;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    runtime.block_on(async {
        let repo = DialogRepo::temp().await?;
        repo.replay_se(&log).await?;
        anyhow::Ok(())
    })?;
    println!("{}", dialog_storage::temp_storage_base().display());
    Ok(())
}

/// Every regular file under `directory`, with its modification time.
fn files(directory: &Path) -> anyhow::Result<Vec<(PathBuf, SystemTime)>> {
    let mut out = Vec::new();
    let mut pending = vec![directory.to_path_buf()];
    while let Some(directory) = pending.pop() {
        for entry in std::fs::read_dir(&directory)? {
            let entry = entry?;
            let kind = entry.file_type()?;
            if kind.is_dir() {
                pending.push(entry.path());
            } else if kind.is_file() {
                out.push((entry.path(), entry.metadata()?.modified()?));
            }
        }
    }
    Ok(out)
}

/// Reads a file-per-block archive directory as it stands: every block the
/// replica ever stored. Nothing records which commit wrote a block, so the
/// order is reconstructed. A commit's root is a tree node no other node
/// links to, and roots are taken in the order their files were written;
/// each root's nodes are appended children first, the first time any root
/// reaches them. Blocks that are not tree nodes (and anything else under
/// the directory) are kept as they are, after the commit their file
/// follows. A replica that pulled its history in from a peer wrote those
/// files when it pulled, not when they were committed, so its order is
/// only as good as its file times.
fn scan(directory: &Path) -> anyhow::Result<Source> {
    let started = Instant::now();
    let mut seen: HashSet<Blake3Hash> = HashSet::new();
    let mut read: Vec<(Written, SystemTime)> = Vec::new();
    for (path, modified) in files(directory)? {
        let bytes = Buffer::from(std::fs::read(&path)?);
        let hash = bytes.blake3_hash().clone();
        if seen.insert(hash.clone()) {
            read.push((
                Written {
                    hash,
                    bytes,
                    commit: 0,
                },
                modified,
            ));
        }
    }
    let nodes = parse_nodes(read.iter().map(|(block, _)| block));

    let mut linked: HashSet<Blake3Hash> = HashSet::new();
    for node in nodes.values() {
        if let NodeBody::Index(index) = node.body() {
            for at in 0..index.len() {
                linked.insert(index.hash_at(at)?.clone());
            }
        }
    }
    let mut modified: HashMap<Blake3Hash, SystemTime> = read
        .iter()
        .map(|(block, time)| (block.hash.clone(), *time))
        .collect();
    let mut roots: Vec<Blake3Hash> = nodes
        .keys()
        .filter(|hash| !linked.contains(*hash))
        .cloned()
        .collect();
    roots.sort_by(|a, b| {
        modified[a]
            .cmp(&modified[b])
            .then_with(|| a.as_bytes().cmp(b.as_bytes()))
    });
    // Blocks fetched from a remote carry the time they were fetched, not
    // written. The history region only grows, so the number of history
    // records under a root orders the roots instead.
    if std::env::var("DIALOG_CENSUS_ORDER").as_deref() == Ok("history") {
        let counts = history_counts(&roots, &nodes)?;
        roots.sort_by(|a, b| {
            counts[a]
                .cmp(&counts[b])
                .then_with(|| a.as_bytes().cmp(b.as_bytes()))
        });
        let mut ordered = HashMap::new();
        let start = SystemTime::UNIX_EPOCH;
        for (at, root) in roots.iter().enumerate() {
            ordered.insert(root.clone(), start + Duration::from_secs(at as u64 + 1));
        }
        // Blocks that are not tree nodes go after the last root.
        let end = start + Duration::from_secs(roots.len() as u64 + 1);
        for (block, time) in read.iter_mut() {
            if !nodes.contains_key(&block.hash) {
                *time = end;
            }
        }
        modified = ordered;
        eprintln!("ordered {} roots by their history records", roots.len());
    }
    let root_times: Vec<SystemTime> = roots.iter().map(|root| modified[root]).collect();

    // The commit each block belongs to: for a node, the first root that
    // reaches it; for anything else, the last root written before it.
    let mut commit_of: HashMap<Blake3Hash, (usize, usize)> = HashMap::new();
    let mut sequence = 0usize;
    for (at, root) in roots.iter().enumerate() {
        let mut stack = vec![(root.clone(), false)];
        while let Some((hash, expanded)) = stack.pop() {
            if commit_of.contains_key(&hash) {
                continue;
            }
            if expanded {
                commit_of.insert(hash, (at + 1, sequence));
                sequence += 1;
                continue;
            }
            stack.push((hash.clone(), true));
            if let Some(NodeBody::Index(index)) = nodes.get(&hash).map(TreeNode::body) {
                for child in 0..index.len() {
                    stack.push((index.hash_at(child)?.clone(), false));
                }
            }
        }
    }
    let mut blocks: Vec<(usize, usize, Written)> = read
        .into_iter()
        .map(|(mut block, time)| {
            let (commit, order) = commit_of.get(&block.hash).copied().unwrap_or_else(|| {
                let commit = root_times.partition_point(|root| *root <= time).max(1);
                (commit, usize::MAX)
            });
            block.commit = commit;
            (commit, order, block)
        })
        .collect();
    blocks.sort_by_key(|(commit, order, _)| (*commit, *order));
    let blocks: Vec<Written> = blocks.into_iter().map(|(_, _, block)| block).collect();
    eprintln!(
        "scanned {}: {} blocks, {} tree nodes, {} roots, in {:.1}s",
        directory.display(),
        blocks.len(),
        nodes.len(),
        roots.len(),
        started.elapsed().as_secs_f64()
    );
    Ok(Source {
        blocks,
        roots,
        nodes,
    })
}

/// The first key a node holds: a segment's first entry, an index's first
/// separator. `None` for a node with nothing in it.
fn first_key(node: &TreeNode) -> anyhow::Result<Option<Vec<u8>>> {
    Ok(match node.body() {
        NodeBody::Segment(segment) => segment
            .keys::<Key>()?
            .next_key()?
            .map(|(_, key)| key.to_vec()),
        NodeBody::Index(index) => (!index.is_empty())
            .then(|| index.separator(0))
            .transpose()?,
    })
}

/// The history records under each root: the leaf entries and buffered
/// assertions whose key is in the history region, counted once per node.
fn history_counts(
    roots: &[Blake3Hash],
    nodes: &HashMap<Blake3Hash, TreeNode>,
) -> anyhow::Result<HashMap<Blake3Hash, usize>> {
    use dialog_corpus::HISTORY_KEY_TAG;
    let mut counts: HashMap<Blake3Hash, usize> = HashMap::new();
    for root in roots {
        let mut stack = vec![(root.clone(), false)];
        while let Some((hash, expanded)) = stack.pop() {
            if counts.contains_key(&hash) {
                continue;
            }
            let Some(node) = nodes.get(&hash) else {
                counts.insert(hash, 0);
                continue;
            };
            match node.body() {
                NodeBody::Segment(segment) => {
                    let mut count = 0;
                    let mut keys = segment.keys::<Key>()?;
                    while let Some((_, key)) = keys.next_key()? {
                        count += (key.first() == Some(&HISTORY_KEY_TAG)) as usize;
                    }
                    counts.insert(hash, count);
                }
                NodeBody::Index(index) if expanded => {
                    let mut count = 0;
                    for at in 0..index.len() {
                        count += counts[index.hash_at(at)?];
                    }
                    for buffer in index.buffers() {
                        for item in buffer.entries::<Key>()? {
                            count += (matches!(item.op, NoveltyOp::Assert(_))
                                && item.key.first() == Some(&HISTORY_KEY_TAG))
                                as usize;
                        }
                    }
                    counts.insert(hash, count);
                }
                NodeBody::Index(index) => {
                    stack.push((hash.clone(), true));
                    for at in 0..index.len() {
                        stack.push((index.hash_at(at)?.clone(), false));
                    }
                }
            }
        }
    }
    Ok(counts)
}

/// The commits of a replica, rebuilt from its head tree: the history
/// region keeps one record per claim every revision made, under that
/// revision's version, with the claim, its polarity and the versions it
/// superseded. Records are grouped by version and the groups ordered by
/// edition, then origin, which is a causal order of the revisions but
/// replays concurrent writers' revisions as one line rather than merging
/// them. A record that superseded something becomes an assert that
/// replaces the newest claim of its cell; one that did not, an additive
/// assert; a retraction, a retraction. A spilled value whose bytes the
/// replica does not hold is replayed as a stand-in of a spilling size, so
/// the tree's keys keep their shape.
fn history(directory: &Path) -> anyhow::Result<Vec<Vec<Instruction>>> {
    let archive = scan(&directory.join("archive"))?;
    let head = archive
        .blocks
        .iter()
        .rev()
        .find(|block| archive.roots.contains(&block.hash))
        .map(|block| block.hash.clone())
        .ok_or_else(|| anyhow::anyhow!("the archive holds no tree root"))?;
    let replica = Replica::in_memory(
        archive.blocks.iter().map(|block| block.bytes.clone()),
        Some(directory.join("blob")),
    );
    drop(archive);
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    let mut revisions: BTreeMap<(Edition, [u8; 32]), Vec<Instruction>> = BTreeMap::new();
    let mut stand_ins = 0usize;
    let mut reserved = 0usize;
    runtime.block_on(async {
        // The replica answers every catalog's reads, so any catalog will do.
        let did: Did = "did:key:zSingleFileCensus".parse()?;
        let catalog = ArchiveScope::new(Subject::from(did)).index();
        let tree = Corpus::at(catalog.clone(), head.clone());
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
            let assertion = record.is_assertion();
            let claim = record.claim().clone();
            let superseding = !claim.cause.versions().is_empty();
            // Facts dialog writes for itself (delegations, revision
            // records) sit in the reserved `dialog.` namespace, which an
            // instruction may not write: they replay under a namespace of
            // the same length, so their keys keep their size and place.
            let the = match claim.the.as_str().strip_prefix("dialog.") {
                Some(rest) => {
                    reserved += 1;
                    Relation::from_str(&format!("dialox.{rest}"))?
                }
                None => claim.the,
            };
            let artifact = Fact {
                the,
                of: claim.of,
                is: claim.is,
                cause: None,
            };
            let instruction = match (assertion, superseding) {
                (true, true) => Instruction::Assert(artifact, Pick::Last),
                (true, false) => Instruction::Assert(artifact, Pick::All),
                (false, _) => Instruction::Retract(artifact),
            };
            revisions
                .entry((version.edition, version.origin.0))
                .or_default()
                .push(instruction);
        }
        anyhow::Ok(())
    })?;
    let origins: HashSet<[u8; 32]> = revisions.keys().map(|(_, origin)| *origin).collect();
    eprintln!(
        "history of {}: head {head}, {} revisions from {} origins, {} claims ({reserved} in the reserved namespace), {stand_ins} spilled values stood in for",
        directory.display(),
        revisions.len(),
        origins.len(),
        revisions.values().map(Vec::len).sum::<usize>(),
    );
    Ok(revisions.into_values().collect())
}

/// Parses `raw` as a comma-separated list of commit counts, kept within
/// `count` and ending at it.
fn checkpoints(raw: Option<String>, count: usize) -> Vec<usize> {
    let mut points: Vec<usize> = match raw {
        Some(raw) => raw.split(',').filter_map(|n| n.parse().ok()).collect(),
        None => (1..=4).map(|quarter| count * quarter / 4).collect(),
    };
    points.retain(|&point| point > 0 && point <= count);
    points.dedup();
    if points.last() != Some(&count) {
        points.push(count);
    }
    points
}

fn main() -> anyhow::Result<()> {
    let mut args = std::env::args().skip(1);
    let first = args.next().unwrap_or_else(|| "2048".into());
    let (source, points) = match first.as_str() {
        "archive" => {
            let directory = args
                .next()
                .ok_or_else(|| anyhow::anyhow!("usage: archive <directory> [checkpoints]"))?;
            let source = scan(Path::new(&directory))?;
            let count = source.roots.len();
            (source, checkpoints(args.next(), count))
        }
        "history" => {
            let directory = args.next().ok_or_else(|| {
                anyhow::anyhow!("usage: history <replica directory> [checkpoints]")
            })?;
            let source = replay_commits(history(Path::new(&directory))?)?;
            let count = source.roots.len();
            (source, checkpoints(args.next(), count))
        }
        "disk" => {
            let count = args.next().and_then(|raw| raw.parse().ok()).unwrap_or(2048);
            return write_disk(count);
        }
        _ => {
            let count: usize = first.parse()?;
            let source = replay(count)?;
            let count = source.roots.len();
            (source, checkpoints(args.next(), count))
        }
    };
    if let Ok(directory) = std::env::var("DIALOG_CENSUS_FILE") {
        return file_archive(&source, Path::new(&directory));
    }
    if let Ok(record) = std::env::var("DIALOG_CENSUS_INDEX") {
        let blocks: Vec<&Written> = source.blocks.iter().collect();
        index_layouts(
            "replayed history",
            &appends_of(&blocks),
            record.parse().unwrap_or(1024),
        );
        return Ok(());
    }
    measure(source, &points)
}

fn kib(bytes: u64) -> String {
    format!("{:.1} MiB", bytes as f64 / (1024.0 * 1024.0))
}

/// Writes `source` into the single-file archive as it is built, one append
/// per commit, and reports what the file holds against the blocks it came
/// from: once with every block, to learn which nodes rebuild exactly, then
/// again with only those (and the blocks that are not tree nodes), so the
/// tokens of nodes kept whole do not count. Then estimates an index kept in
/// the file as a search tree of its own.
fn file_archive(source: &Source, directory: &Path) -> anyhow::Result<()> {
    use dialog_archive_file::FileArchive;
    const CATALOG: &str = "index";
    std::fs::create_dir_all(directory)?;
    let commits = |blocks: &mut dyn Iterator<Item = &Written>| {
        let mut grouped: Vec<Vec<Buffer>> = Vec::new();
        let mut last = None;
        for block in blocks {
            if last != Some(block.commit) {
                grouped.push(Vec::new());
                last = Some(block.commit);
            }
            grouped
                .last_mut()
                .expect("a group")
                .push(block.bytes.clone());
        }
        grouped
    };

    let everything = directory.join("everything.dialog");
    std::fs::remove_file(&everything).ok();
    let first = FileArchive::open(&everything)?;
    for group in commits(&mut source.blocks.iter()) {
        first.append(CATALOG, &group)?;
    }
    let modern: HashSet<Blake3Hash> = source
        .blocks
        .iter()
        .filter(|block| first.is_frame(CATALOG, &block.hash))
        .map(|block| block.hash.clone())
        .collect();

    let (mut modern_bytes, mut legacy_bytes, mut other_bytes) = (0u64, 0u64, 0u64);
    let (mut modern_count, mut legacy_count, mut other_count) = (0, 0, 0);
    for block in &source.blocks {
        let len = block.bytes.as_ref().len() as u64;
        if modern.contains(&block.hash) {
            modern_bytes += len;
            modern_count += 1;
        } else if source.nodes.contains_key(&block.hash) {
            legacy_bytes += len;
            legacy_count += 1;
        } else {
            other_bytes += len;
            other_count += 1;
        }
    }
    println!("source, a file per block:");
    println!(
        "  {modern_count} tree nodes that rebuild exactly: {}",
        kib(modern_bytes)
    );
    println!(
        "  {legacy_count} tree nodes in the old layout (left out below): {}",
        kib(legacy_bytes)
    );
    println!(
        "  {other_count} blocks that are not tree nodes: {}",
        kib(other_bytes)
    );

    let clean = directory.join("modern.dialog");
    std::fs::remove_file(&clean).ok();
    let archive = FileArchive::open(&clean)?;
    let kept: Vec<&Written> = source
        .blocks
        .iter()
        .filter(|block| modern.contains(&block.hash) || !source.nodes.contains_key(&block.hash))
        .collect();
    for group in commits(&mut kept.iter().copied()) {
        archive.append(CATALOG, &group)?;
    }
    let stats = archive.stats();
    let records = stats.file_bytes - stats.frame_bytes - stats.token_bytes - stats.block_bytes;
    println!("the same blocks in one file: {}", kib(stats.file_bytes));
    println!(
        "  {} frames: {} ({} against another)",
        stats.frames,
        kib(stats.frame_bytes),
        stats.frames_against
    );
    println!("  {} tokens: {}", stats.tokens, kib(stats.token_bytes));
    println!(
        "  {} blocks kept whole: {}",
        stats.blocks,
        kib(stats.block_bytes)
    );
    println!(
        "  record headers, padding, footers and checkpoints: {}",
        kib(records)
    );
    println!(
        "  index held in memory: {} (estimate from the index's tables)",
        kib(stats.index_memory as u64)
    );

    index_tree_estimate(&kept, source)?;
    writer_costs(&kept, directory)?;
    if std::env::var("DIALOG_CENSUS_INDEX").is_ok() {
        let record = archive.stats().file_bytes / kept.len().max(1) as u64;
        index_layouts(
            "tonk-team, the blocks the file holds",
            &appends_of(&kept),
            record,
        );
    }
    Ok(())
}

/// What a writer that has just opened the file pays against one that has
/// been writing: the same file, written up to its last commits, then
/// reopened by a fresh process, which appends the rest one commit at a time.
/// The first append builds the writer's token dictionary from the file.
fn writer_costs(kept: &[&Written], directory: &Path) -> anyhow::Result<()> {
    use dialog_archive_file::FileArchive;
    const CATALOG: &str = "index";
    const TAIL: usize = 20;
    let mut commits: Vec<Vec<Buffer>> = Vec::new();
    let mut last = None;
    for block in kept {
        if last != Some(block.commit) {
            commits.push(Vec::new());
            last = Some(block.commit);
        }
        commits
            .last_mut()
            .expect("a commit")
            .push(block.bytes.clone());
    }
    let split = commits.len().saturating_sub(TAIL);
    let path = directory.join("writer.dialog");
    std::fs::remove_file(&path).ok();
    {
        let archive = FileArchive::open(&path)?;
        for commit in &commits[..split] {
            archive.append(CATALOG, commit)?;
        }
    }
    // A fresh open: the registry let the handle go when it was dropped.
    let started = Instant::now();
    let archive = FileArchive::open(&path)?;
    let opened = started.elapsed();
    let dictionary = archive.prepare_writer();
    let mut times = Vec::new();
    for commit in &commits[split..] {
        let started = Instant::now();
        archive.append(CATALOG, commit)?;
        times.push(started.elapsed());
    }
    let mean = |times: &[Duration]| {
        times.iter().sum::<Duration>().as_secs_f64() * 1e3 / times.len().max(1) as f64
    };
    println!(
        "a writer that just opened the file: open {:.1} ms, token dictionary {:.1} ms \
         ({} tokens); then appends of {TAIL} commits, {:.2} ms each on average",
        opened.as_secs_f64() * 1e3,
        dictionary.as_secs_f64() * 1e3,
        archive.stats().tokens,
        mean(&times),
    );
    Ok(())
}

/// Estimates an index kept in the file as a search tree keyed by block hash:
/// each leaf holds rows of (the block's hash, as a token; where the block
/// is), cut by the same content-defined rule the tree uses (an entry ends a
/// leaf when its hash says so, here 1 in `fanout`), and each commit
/// rewrites the leaves its new blocks land in, and the root, as frames
/// against their previous versions, whole every 32.
fn index_tree_estimate(blocks: &[&Written], source: &Source) -> anyhow::Result<()> {
    const CHAIN: usize = 32;
    // A frame's header with the hash it is known by as a token reference:
    // the record header (9), catalog (6), hash token (3), kind, base (3),
    // manifest, lineage (about 8).
    const HEADER: u64 = 9 + 6 + 3 + 1 + 3 + 1 + 8;
    // A row: the hash token's id and the block's place, each a zigzag delta
    // against the row before; in hash order both are effectively random.
    let width = |value: u64| (64 - value.leading_zeros() as u64).max(1).div_ceil(7);
    let n = blocks.len() as u64;
    let row = width(n << 1)
        + width(
            source
                .blocks
                .iter()
                .map(|b| b.bytes.as_ref().len() as u64)
                .sum::<u64>()
                / 8,
        );
    // A copy op: its length and where it starts.
    const COPY: u64 = 3;
    for fanout in [64u64, 128, 256] {
        let ends_leaf = |hash: &Blake3Hash| {
            (u16::from_le_bytes([hash.as_bytes()[30], hash.as_bytes()[31]]) as u64)
                .is_multiple_of(fanout)
        };
        let mut index: std::collections::BTreeSet<[u8; 32]> = std::collections::BTreeSet::new();
        // Each leaf's versions since its last whole frame, by its first key.
        let mut depth: HashMap<[u8; 32], usize> = HashMap::new();
        let (mut leaf_bytes, mut root_bytes, mut leaf_frames, mut root_frames) =
            (0u64, 0u64, 0u64, 0u64);
        let mut root_depth = 0usize;
        let mut commit = None;
        let mut fresh: Vec<[u8; 32]> = Vec::new();
        let mut flush = |index: &mut std::collections::BTreeSet<[u8; 32]>,
                         fresh: &mut Vec<[u8; 32]>| {
            if fresh.is_empty() {
                return;
            }
            for hash in fresh.iter() {
                index.insert(*hash);
            }
            // The leaves, as runs of the sorted hashes.
            let mut leaves: Vec<(Vec<[u8; 32]>, bool)> = vec![(Vec::new(), false)];
            let new: HashSet<[u8; 32]> = fresh.iter().copied().collect();
            for hash in index.iter() {
                let leaf = leaves.last_mut().expect("a leaf");
                leaf.0.push(*hash);
                leaf.1 |= new.contains(hash);
                if ends_leaf(&Blake3Hash::from(*hash)) {
                    leaves.push((Vec::new(), false));
                }
            }
            leaves.retain(|leaf| !leaf.0.is_empty());
            let mut changed_links = 0u64;
            for (rows, changed) in &leaves {
                if !changed {
                    continue;
                }
                changed_links += 1;
                leaf_frames += 1;
                let first = rows[0];
                let versions = depth.entry(first).or_insert(CHAIN);
                let added = rows.iter().filter(|hash| new.contains(*hash)).count() as u64;
                if *versions + 1 >= CHAIN {
                    leaf_bytes += HEADER + rows.len() as u64 * row;
                    *versions = 0;
                } else {
                    // Copy runs around each new row, the new rows themselves.
                    leaf_bytes += HEADER + (added + 1) * COPY + added * row;
                    *versions += 1;
                }
            }
            root_frames += 1;
            let links = leaves.len() as u64;
            if root_depth + 1 >= CHAIN || root_frames == 1 {
                root_bytes += HEADER + links * (row + 1);
                root_depth = 0;
            } else {
                root_bytes += HEADER + (changed_links + 1) * COPY + changed_links * (row + 1);
                root_depth += 1;
            }
            fresh.clear();
        };
        for block in blocks {
            if commit != Some(block.commit) {
                flush(&mut index, &mut fresh);
                commit = Some(block.commit);
            }
            fresh.push(*block.hash.as_bytes());
        }
        flush(&mut index, &mut fresh);
        let leaves = index
            .iter()
            .filter(|hash| ends_leaf(&Blake3Hash::from(**hash)))
            .count()
            + 1;
        println!(
            "index as a search tree, a leaf every {fanout} hashes ({leaves} leaves, {n} entries, \
             rows of {row} bytes): {} in {leaf_frames} leaf frames, {} in {root_frames} root frames",
            kib(leaf_bytes),
            kib(root_bytes)
        );
    }
    // The hashes as tokens: 32 bytes, a length and a padding byte, and
    // padding to 16 if hash tokens keep the alignment datums need.
    println!(
        "hashes as tokens: {} blocks × 34 bytes = {} unaligned, × 48 = {} aligned to 16",
        n,
        kib(n * 34),
        kib(n * 48)
    );
    Ok(())
}

/// Runs every model over `source`, reporting at each of `checkpoints`,
/// then checks every frame realizes to its block and times reads.
fn measure(source: Source, checkpoints: &[usize]) -> anyhow::Result<()> {
    let Source {
        blocks,
        roots,
        nodes,
    } = source;
    anyhow::ensure!(!roots.is_empty(), "no tree roots to measure");
    let position: HashMap<Blake3Hash, usize> = blocks
        .iter()
        .enumerate()
        .map(|(at, block)| (block.hash.clone(), at))
        .collect();

    // Lineage: the node each new node supersedes, the previous holder of
    // its slot (depth below the root, left separator).
    let mut predecessor: HashMap<Blake3Hash, Blake3Hash> = HashMap::new();
    let mut slots: HashMap<(usize, Vec<u8>), Blake3Hash> = HashMap::new();
    let mut seen: HashSet<Blake3Hash> = HashSet::new();
    for root in &roots {
        let mut stack = vec![(root.clone(), 0usize, Vec::new())];
        while let Some((hash, depth, separator)) = stack.pop() {
            if !seen.insert(hash.clone()) {
                continue;
            }
            let Some(node) = nodes.get(&hash) else {
                continue;
            };
            if let NodeBody::Index(index) = node.body() {
                for at in 0..index.len() {
                    stack.push((index.hash_at(at)?.clone(), depth + 1, index.separator(at)?));
                }
            }
            if let Some(previous) = slots.insert((depth, separator), hash.clone())
                && previous != hash
            {
                predecessor.insert(hash, previous);
            }
        }
    }

    let mut stream = zstd::stream::Encoder::new(Vec::new(), LEVEL)?;
    stream.long_distance_matching(true)?;
    stream.window_log(27)?;
    let mut tokens = Tokens::default();
    let mut frames = Frames::default();
    let mut share = FrameShare::default();
    let mut totals = Totals::default();
    let mut log_deltas: Vec<DeltaStore> = INTERVALS.iter().map(|&k| DeltaStore::new(k)).collect();
    let mut frame_deltas: Vec<DeltaStore> = INTERVALS.iter().map(|&k| DeltaStore::new(k)).collect();
    let mut log_zstd: HashMap<Blake3Hash, Vec<u8>> = HashMap::new();
    // What writing costs: a file per block, every block appended to one
    // file, and each columnar store's encode and append. None of them
    // syncs, so these are the costs up to the page cache.
    let written = tempfile::tempdir()?;
    let per_block = written.path().join("fs");
    std::fs::create_dir(&per_block)?;
    let mut log_file = std::fs::File::create(written.path().join("log"))?;
    let (mut fs_write, mut log_append) = (Duration::ZERO, Duration::ZERO);
    let mut columns: Vec<ColumnStore> = COLUMN_INTERVALS
        .iter()
        .map(|&k| ColumnStore::new(k, written.path()))
        .collect::<anyhow::Result<_>>()?;
    let mut positions: HashMap<Blake3Hash, u32> = HashMap::new();
    let mut all_rows: Vec<Rows> = Vec::new();
    // What a provider can know without the tree's lineage: the latest
    // frame of the same kind that starts at the same key.
    let mut by_first: HashMap<(u8, Vec<u8>), u32> = HashMap::new();
    let mut by_first_store = ColumnStore::new(32, written.path())?;
    let mut by_first_store_named = 0usize;
    let (mut lineage_bases, mut agree) = (0usize, 0usize);
    // Nodes a later node of the same kind and first key supersedes: under
    // a "newest stays whole" policy, everything else stays a raw block.
    let mut superseded: HashSet<u32> = HashSet::new();
    let mut node_bytes: Vec<usize> = Vec::new();
    let mut rows_time = Duration::ZERO;

    println!("{}", header());
    let mut next = 0usize;
    for block in blocks.iter().map(Some).chain([None]) {
        let commit = block.map_or(usize::MAX, |block| block.commit);
        while next < checkpoints.len() && commit > checkpoints[next] {
            stream.flush()?;
            let at = Checkpoint {
                commit: checkpoints[next],
                totals: &totals,
                log_deltas: &log_deltas,
                frame_deltas: &frame_deltas,
                columns: &columns,
                stream: stream.get_ref().len(),
            };
            println!("{}", row(&at, &roots, &nodes)?);
            next += 1;
        }
        let Some(block) = block else {
            break;
        };
        let bytes = block.bytes.as_ref();
        let len = bytes.len();
        totals.blocks += 1;
        totals.content += len;
        totals.fs_allocated += len.div_ceil(FS_BLOCK).max(1) * FS_BLOCK;
        totals.log += len + HASH_INDEX;
        let started = Instant::now();
        std::fs::write(per_block.join(block.hash.to_string()), bytes)?;
        fs_write += started.elapsed();
        let started = Instant::now();
        log_file.write_all(bytes)?;
        log_append += started.elapsed();
        stream.write_all(bytes)?;
        let alone = zstd_alone(bytes)?;
        totals.log_zstd += alone.len() + HASH_INDEX;
        let base = predecessor.get(&block.hash);
        let base_block = base.map(|previous| (previous, blocks[position[previous]].bytes.as_ref()));
        for store in &mut log_deltas {
            store.put(&block.hash, bytes, &alone, base_block)?;
        }
        log_zstd.insert(block.hash.clone(), alone);

        let Some(node) = nodes.get(&block.hash) else {
            totals.other_blocks += 1;
            totals.other_bytes += len;
            continue;
        };
        let before_count = tokens.count();
        let before_bytes = tokens.region.len();
        let encoded = frame(&block.hash, node, &mut tokens, &frames.by_hash, &mut share)?;
        let started = Instant::now();
        let rows = rows_of(node, &mut tokens, &positions)?;
        rows_time += started.elapsed();
        let base_rows = base
            .and_then(|previous| positions.get(previous))
            .map(|&position| (position, &all_rows[position as usize]));
        for store in &mut columns {
            store.put(&block.hash, &rows, base_rows)?;
        }
        let first = first_key(node)?;
        let guess = first
            .clone()
            .and_then(|first| by_first.get(&(rows.kind, first)).copied());
        lineage_bases += base_rows.is_some() as usize;
        agree += (base_rows.is_some() && base_rows.map(|(p, _)| p) == guess) as usize;
        by_first_store_named += guess.is_some() as usize;
        if let Some(position) = guess {
            superseded.insert(position);
        }
        node_bytes.push(len);
        by_first_store.put(
            &block.hash,
            &rows,
            guess.map(|position| (position, &all_rows[position as usize])),
        )?;
        if let Some(first) = first {
            by_first.insert((rows.kind, first), all_rows.len() as u32);
        }
        positions.insert(block.hash.clone(), all_rows.len() as u32);
        all_rows.push(rows);
        totals.tokens += tokens.region.len() - before_bytes;
        totals.token_index += (tokens.count() - before_count) * TOKEN_INDEX;
        totals.frames += encoded.len();
        let encoded_alone = zstd_alone(&encoded)?;
        totals.frames_zstd += encoded_alone.len().min(encoded.len());
        let base_frame =
            base.and_then(|previous| frames.of(previous).map(|frame| (previous, frame)));
        for store in &mut frame_deltas {
            store.put(&block.hash, &encoded, &encoded_alone, base_frame)?;
        }
        frames.push(&block.hash, encoded);
    }

    eprintln!(
        "\nblocks: {} index ({} MiB) -> frames {} MiB; {} segment ({} MiB) -> frames {} MiB; {} other ({} MiB) kept as they are",
        share.index_frames,
        mib(share.index_blocks),
        mib(share.index_bytes),
        share.segment_frames,
        mib(share.segment_blocks),
        mib(share.segment_bytes),
        totals.other_blocks,
        mib(totals.other_bytes),
    );
    eprintln!(
        "entries: {} buffered ops in index frames, {} leaf entries in segment frames",
        share.novelty_entries, share.segment_entries,
    );
    let mut kinds: Vec<_> = tokens.by_kind.iter().collect();
    kinds.sort_by_key(|(_, (_, bytes))| std::cmp::Reverse(*bytes));
    for (kind, (distinct, bytes)) in kinds {
        let references = tokens.references.get(kind).copied().unwrap_or(0);
        eprintln!(
            "tokens {kind:>9}: {distinct:>8} distinct, {:>7} MiB, {references:>9} references ({:.1} per token)",
            mib(*bytes),
            references as f64 / *distinct as f64
        );
    }

    let nodes_written = all_rows.len().max(1) as f64;
    let per = |elapsed: Duration, count: f64| elapsed.as_secs_f64() * 1e6 / count;
    let blocks_written = blocks.len().max(1) as f64;
    eprintln!("\nwrites, mean us per block (no fsync):");
    eprintln!(
        "  {:<36} {:>8.1}",
        "fs: file per block",
        per(fs_write, blocks_written)
    );
    eprintln!(
        "  {:<36} {:>8.1}",
        "log: append the block",
        per(log_append, blocks_written)
    );
    eprintln!(
        "  {:<36} {:>8.1}",
        "columnar: split into rows (shared)",
        per(rows_time, nodes_written)
    );
    for store in &columns {
        eprintln!(
            "  {:<36} {:>8.1}   ({} of {} frames against a predecessor, {} MiB of frames)",
            format!("columnar/{}: encode + append", store.interval),
            per(store.encode + store.append, nodes_written),
            store.against,
            store.frames.len(),
            mib(store.bytes),
        );
    }
    let whole: usize = node_bytes
        .iter()
        .enumerate()
        .filter(|(position, _)| !superseded.contains(&(*position as u32)))
        .map(|(_, bytes)| bytes)
        .sum();
    eprintln!(
        "  newest stays whole: {} of {} nodes, {} MiB, are never superseded by first key",
        node_bytes.len() - superseded.len(),
        node_bytes.len(),
        mib(whole),
    );
    eprintln!(
        "  columnar/32 by first key: {} MiB of frames ({} named a predecessor; lineage named {}, and {} of those agree)",
        mib(by_first_store.bytes),
        by_first_store_named,
        lineage_bases,
        agree,
    );
    drop(all_rows);

    // Every columnar frame realizes to its block too. Blocks an older
    // encoder wrote may not, since a frame rebuilds with today's.
    let mut differ: Vec<Blake3Hash> = Vec::new();
    for store in &columns {
        let started = Instant::now();
        for block in &blocks {
            let Some(node) = nodes.get(&block.hash) else {
                continue;
            };
            let rows = store.rows(positions[&block.hash]);
            let bytes = realize_rows(&rows, &tokens, store, &node.manifest()?)?;
            if bytes.as_slice() != block.bytes.as_ref() {
                differ.push(block.hash.clone());
            }
        }
        eprintln!(
            "realize: {} of {} columnar/{} frames rebuild their block byte for byte ({:.1}s)",
            positions.len() - differ.len(),
            positions.len(),
            store.interval,
            started.elapsed().as_secs_f64()
        );
        differ.clear();
    }

    // Every frame realizes to the block it stands for, byte for byte,
    // under the manifest of the tree the node belongs to.
    let realized = Instant::now();
    for block in &blocks {
        let Some(node) = nodes.get(&block.hash) else {
            continue;
        };
        let frame = frames.of(&block.hash).expect("every node has a frame");
        let bytes = realize(frame, &tokens, &frames, &node.manifest()?)?;
        if bytes.as_slice() != block.bytes.as_ref() {
            differ.push(block.hash.clone());
        }
    }
    eprintln!(
        "\nrealize: {} of {} frames rebuild their block byte for byte ({:.1}s)",
        frames.frames.len() - differ.len(),
        frames.frames.len(),
        realized.elapsed().as_secs_f64()
    );
    if !differ.is_empty() {
        let bytes: usize = differ
            .iter()
            .map(|hash| {
                nodes[hash]
                    .buffer()
                    .expect("a node read from its block")
                    .as_ref()
                    .len()
            })
            .sum();
        let commits: Vec<usize> = differ
            .iter()
            .map(|hash| blocks[position[hash]].commit)
            .collect();
        let legacy = differ
            .iter()
            .filter(|hash| {
                nodes[*hash].layout_version() != nodes[&roots[roots.len() - 1]].layout_version()
            })
            .count();
        let legacy_all = nodes
            .values()
            .filter(|node| node.layout_version() != nodes[&roots[roots.len() - 1]].layout_version())
            .count();
        eprintln!(
            "  {legacy} of them, and {legacy_all} nodes in all, are in a layout other than the head's"
        );
        eprintln!(
            "  the {} that differ hold {} MiB as blocks, written in commits {}..={}",
            differ.len(),
            mib(bytes),
            commits.iter().min().unwrap_or(&0),
            commits.iter().max().unwrap_or(&0),
        );
    }
    let mut sizes: Vec<(usize, usize, &str)> = differ
        .iter()
        .take(8)
        .map(|hash| {
            let node = &nodes[hash];
            let kind = match node.body() {
                NodeBody::Index(_) => "index",
                NodeBody::Segment(_) => "segment",
            };
            let realized = realize(
                frames.of(hash).expect("a frame"),
                &tokens,
                &frames,
                &node.manifest().expect("a manifest"),
            )
            .map(|bytes| bytes.len())
            .unwrap_or(0);
            (
                node.buffer()
                    .expect("a node read from its block")
                    .as_ref()
                    .len(),
                realized,
                kind,
            )
        })
        .collect();
    sizes.sort();
    if !sizes.is_empty() {
        eprintln!("  differing (block bytes, realized bytes, kind): {sizes:?}");
    }

    read_costs(
        &roots[roots.len() - 1],
        &nodes,
        &blocks,
        &position,
        &log_zstd,
        &log_deltas,
        &tokens,
        &frames,
        &columns,
        &positions,
    )
}

/// Orders each commit's blocks children first, the order a single-file
/// writer appends them in so that a parent's frame can point at its
/// children's. The archive is handed a commit's blocks in no such order.
fn children_first(
    written: Vec<Written>,
    nodes: &HashMap<Blake3Hash, TreeNode>,
) -> anyhow::Result<Vec<Written>> {
    let mut ordered = Vec::with_capacity(written.len());
    let mut pending = written.into_iter().peekable();
    while let Some(first) = pending.next() {
        let commit = first.commit;
        let mut group: HashMap<Blake3Hash, Written> = HashMap::new();
        let mut order = vec![first.hash.clone()];
        group.insert(first.hash.clone(), first);
        while let Some(block) = pending.next_if(|block| block.commit == commit) {
            order.push(block.hash.clone());
            group.insert(block.hash.clone(), block);
        }
        let mut emitted: HashSet<Blake3Hash> = HashSet::new();
        for start in order {
            let mut stack = vec![(start, false)];
            while let Some((hash, expanded)) = stack.pop() {
                if emitted.contains(&hash) || !group.contains_key(&hash) {
                    continue;
                }
                if expanded {
                    emitted.insert(hash.clone());
                    ordered.push(group.remove(&hash).expect("in the group"));
                    continue;
                }
                stack.push((hash.clone(), true));
                if let Some(NodeBody::Index(index)) = nodes.get(&hash).map(TreeNode::body) {
                    for at in 0..index.len() {
                        stack.push((index.hash_at(at)?.clone(), false));
                    }
                }
            }
        }
    }
    Ok(ordered)
}

/// Reads every live node back under each model, as a get by hash would,
/// and reports the mean time per node.
#[allow(clippy::too_many_arguments)]
fn read_costs(
    root: &Blake3Hash,
    nodes: &HashMap<Blake3Hash, TreeNode>,
    blocks: &[Written],
    position: &HashMap<Blake3Hash, usize>,
    log_zstd: &HashMap<Blake3Hash, Vec<u8>>,
    log_deltas: &[DeltaStore],
    tokens: &Tokens,
    frames: &Frames,
    columns: &[ColumnStore],
    positions: &HashMap<Blake3Hash, u32>,
) -> anyhow::Result<()> {
    let (reached, live_bytes, _) = live(root, nodes)?;
    let directory = tempfile::tempdir()?;
    for hash in &reached {
        std::fs::write(
            directory.path().join(hash.to_string()),
            blocks[position[hash]].bytes.as_ref(),
        )?;
    }
    let capacity = 1 << 24;
    let reads = reached.len();
    eprintln!(
        "\nreads of the {reads} live nodes ({} MiB), mean us per node:",
        mib(live_bytes)
    );
    let timed = |label: &str, read: &dyn Fn(&Blake3Hash) -> anyhow::Result<Vec<u8>>| {
        // Warm up, then time passes over every live node.
        for hash in &reached {
            read(hash)?;
        }
        let passes = 5;
        let started = Instant::now();
        let mut bytes = 0usize;
        for _ in 0..passes {
            for hash in &reached {
                bytes += read(hash)?.len();
            }
        }
        let elapsed: Duration = started.elapsed();
        anyhow::ensure!(
            bytes == passes * live_bytes,
            "{label} read back {bytes} bytes"
        );
        eprintln!(
            "  {label:<28} {:>8.1}",
            elapsed.as_secs_f64() * 1e6 / (passes * reads) as f64
        );
        anyhow::Ok(())
    };
    let verify = |hash: &Blake3Hash, bytes: Vec<u8>| {
        anyhow::ensure!(blake3::hash(&bytes).as_bytes() == hash.as_bytes());
        Ok(bytes)
    };
    let path = |hash: &Blake3Hash| directory.path().join(hash.to_string());
    timed("fs file read", &|hash| Ok(std::fs::read(path(hash))?))?;
    timed("fs file read + blake3", &|hash| {
        verify(hash, std::fs::read(path(hash))?)
    })?;
    timed("log+zstd", &|hash| unzstd(&log_zstd[hash], None, capacity))?;
    for store in log_deltas {
        let label = format!("log+delta/{}", interval(store.interval));
        timed(&label, &|hash| store.get(hash, capacity))?;
    }
    let frame = |hash: &Blake3Hash| frames.of(hash).expect("every live node has a frame");
    let manifests: HashMap<&Blake3Hash, Manifest> = reached
        .iter()
        .map(|hash| Ok((hash, nodes[hash].manifest()?)))
        .collect::<anyhow::Result<_>>()?;
    timed("tokens: realize", &|hash| {
        realize(frame(hash), tokens, frames, &manifests[hash])
    })?;
    timed("tokens: realize + blake3", &|hash| {
        verify(
            hash,
            realize(frame(hash), tokens, frames, &manifests[hash])?,
        )
    })?;
    // Where a realize spends its time, over the live segments: building
    // the entries from rows (keys from tokens, datums deserialized), then
    // the tree's encoder, then the validation every read pays when it
    // turns bytes into a node, from a file or not.
    if let Some(store) = columns.first() {
        let segments: Vec<&Blake3Hash> = reached
            .iter()
            .filter(|hash| matches!(nodes[*hash].body(), NodeBody::Segment(_)))
            .collect();
        let (mut build, mut datums, mut encode, mut validate) = (
            Duration::ZERO,
            Duration::ZERO,
            Duration::ZERO,
            Duration::ZERO,
        );
        let passes = 5;
        for _ in 0..passes {
            for hash in &segments {
                let rows = store.rows(positions[*hash]);
                let started = Instant::now();
                let mut entries = Vec::with_capacity(rows.sections[0].len());
                for row in &rows.sections[0] {
                    let form = row[0] as u8;
                    let parts: Vec<&[u8]> = (0..slots_of(form))
                        .map(|slot| tokens.by_id(row[1 + slot]))
                        .collect();
                    let key = key_from_tokens(form, &parts);
                    let datum_started = Instant::now();
                    let value = datum_from(tokens.by_id(row[4]))?;
                    datums += datum_started.elapsed();
                    entries.push(Entry {
                        key: <Key as TreeKey>::try_from_bytes(&key)?,
                        value,
                    });
                }
                build += started.elapsed();
                let started = Instant::now();
                let bytes = PersistentNodeBody::from_segment(
                    PersistentSegment::from_entries::<Key>(entries)?,
                    manifests[*hash].clone(),
                )
                .as_bytes()?
                .to_vec();
                encode += started.elapsed();
                let started = Instant::now();
                let node = TreeNode::try_from(Buffer::from(bytes))?;
                validate += started.elapsed();
                anyhow::ensure!(
                    node.buffer()
                        .expect("a node read from its block")
                        .as_ref()
                        .len()
                        == nodes[*hash]
                            .buffer()
                            .expect("a node read from its block")
                            .as_ref()
                            .len()
                );
            }
        }
        let count = (passes * segments.len()).max(1) as f64;
        let per = |elapsed: Duration| elapsed.as_secs_f64() * 1e6 / count;
        eprintln!(
            "  realize of a live segment ({} of them), us: build entries {:.1} (of which datums {:.1}), encode {:.1}; validating a node from bytes {:.1}",
            segments.len(),
            per(build),
            per(datums),
            per(encode),
            per(validate),
        );
    }
    for store in columns {
        let label = |what: &str| format!("columnar/{}: {what}", store.interval);
        // Decoding the rows is what a reader that worked on rows would pay;
        // the count is checked, not the bytes, so pass the node's size.
        timed(&label("decode rows"), &|hash| {
            let rows = store.rows(positions[hash]);
            anyhow::ensure!(!rows.sections.is_empty());
            Ok(vec![
                0;
                nodes[hash]
                    .buffer()
                    .expect("a node read from its block")
                    .as_ref()
                    .len()
            ])
        })?;
        timed(&label("realize"), &|hash| {
            realize_rows(
                &store.rows(positions[hash]),
                tokens,
                store,
                &manifests[hash],
            )
        })?;
        timed(&label("realize + blake3"), &|hash| {
            verify(
                hash,
                realize_rows(
                    &store.rows(positions[hash]),
                    tokens,
                    store,
                    &manifests[hash],
                )?,
            )
        })?;
    }
    Ok(())
}

/// One row of a columnar frame section: the ids an entry, a link or a
/// buffer is made of. Which fields mean what depends on the [`Section`].
type Row = [u32; 5];

/// A field with no value: a retraction's datum, a link to a child the file
/// does not hold.
const NONE: u32 = u32::MAX;

/// The sections a columnar frame is made of. A segment frame has one
/// (`Entries`); an index frame has three (`Links`, `Buffers`, `Ops`).
#[derive(Clone, Copy)]
enum Section {
    /// `[form, slot0, slot1, slot2, datum]`: a leaf entry.
    Entries,
    /// `[separator, child frame or NONE, scale, child hash token or NONE, 0]`.
    Links,
    /// `[child index, ops in the buffer, 0, 0, 0]`.
    Buffers,
    /// `[form, slot0, slot1, slot2, datum or NONE for a retraction]`.
    Ops,
}

const SEGMENT_SECTIONS: [Section; 1] = [Section::Entries];
const INDEX_SECTIONS: [Section; 3] = [Section::Links, Section::Buffers, Section::Ops];

fn sections_of(kind: u8) -> &'static [Section] {
    if kind == 1 {
        &INDEX_SECTIONS
    } else {
        &SEGMENT_SECTIONS
    }
}

/// A node as columnar rows: its kind and its sections' rows.
#[derive(Clone)]
struct Rows {
    kind: u8,
    sections: Vec<Vec<Row>>,
}

fn put_zigzag(out: &mut Vec<u8>, value: i64) {
    put_varint(out, ((value << 1) ^ (value >> 63)) as usize);
}

fn get_zigzag(bytes: &[u8], at: &mut usize) -> i64 {
    let raw = get_varint(bytes, at) as u64;
    ((raw >> 1) as i64) ^ -((raw & 1) as i64)
}

/// The row of one entry's key and datum, by token id.
fn entry_row(tokens: &mut Tokens, key: &[u8], datum: Option<&State<Datum>>) -> anyhow::Result<Row> {
    let (form, parts) = key_tokens(key);
    let mut row = [form as u32, 0, 0, 0, NONE];
    for (slot, (kind, token)) in parts.iter().enumerate() {
        row[1 + slot] = tokens.id(kind, token);
    }
    if let Some(datum) = datum {
        row[4] = tokens.id("datum", &datum_bytes(datum)?);
    }
    Ok(row)
}

/// Splits `node` into columnar rows. Child links point at the children's
/// frames by their position in the file.
fn rows_of(
    node: &TreeNode,
    tokens: &mut Tokens,
    positions: &HashMap<Blake3Hash, u32>,
) -> anyhow::Result<Rows> {
    match node.body() {
        NodeBody::Segment(segment) => {
            let mut entries = Vec::with_capacity(segment.len());
            let mut keys = segment.keys::<Key>()?;
            while let Some((at, key)) = keys.next_key()? {
                let value: State<Datum> = into_owned(segment.value_at(at)?)?;
                entries.push(entry_row(tokens, key, Some(&value))?);
            }
            Ok(Rows {
                kind: 0,
                sections: vec![entries],
            })
        }
        NodeBody::Index(index) => {
            let mut links = Vec::with_capacity(index.len());
            for at in 0..index.len() {
                let separator = tokens.id("separator", &index.separator(at)?);
                let child = index.hash_at(at)?;
                let (frame, absent) = match positions.get(child) {
                    Some(position) => (*position, NONE),
                    None => (NONE, tokens.id("absent", child.as_bytes())),
                };
                links.push([
                    separator,
                    frame,
                    index.scale_at(at)?.as_u8() as u32,
                    absent,
                    0,
                ]);
            }
            let (mut buffers, mut ops) = (Vec::new(), Vec::new());
            for buffer in index.buffers() {
                let entries = buffer.entries::<Key>()?;
                buffers.push([buffer.child(), entries.len() as u32, 0, 0, 0]);
                for item in &entries {
                    let datum = match &item.op {
                        NoveltyOp::Assert(value) => Some(value),
                        NoveltyOp::Retract => None,
                    };
                    ops.push(entry_row(tokens, &item.key, datum)?);
                }
            }
            Ok(Rows {
                kind: 1,
                sections: vec![links, buffers, ops],
            })
        }
    }
}

/// Writes rows column by column. Ids are zigzag deltas against the same
/// column's previous row, so a sorted or repeating column (the entity of
/// an EAV segment, the attribute of an AEV one) costs a byte a row.
fn put_columns(out: &mut Vec<u8>, section: Section, rows: &[&Row]) {
    let delta = |out: &mut Vec<u8>, field: usize, when: &dyn Fn(&Row) -> bool| {
        let mut previous = 0i64;
        for row in rows.iter().filter(|row| when(row)) {
            put_zigzag(out, row[field] as i64 - previous);
            previous = row[field] as i64;
        }
    };
    match section {
        Section::Entries | Section::Ops => {
            for row in rows {
                out.push(row[0] as u8);
            }
            for slot in 0..3 {
                delta(out, 1 + slot, &|row| slots_of(row[0] as u8) > slot);
            }
            if matches!(section, Section::Ops) {
                for row in rows {
                    out.push((row[4] != NONE) as u8);
                }
            }
            delta(out, 4, &|row| row[4] != NONE);
        }
        Section::Links => {
            delta(out, 0, &|_| true);
            // The scale, with the top bit marking a child the file does
            // not hold, ahead of the columns that depend on it.
            for row in rows {
                out.push(row[2] as u8 | (((row[1] == NONE) as u8) << 7));
            }
            delta(out, 1, &|row| row[1] != NONE);
            delta(out, 3, &|row| row[1] == NONE);
        }
        Section::Buffers => {
            for row in rows {
                put_varint(out, row[0] as usize);
                put_varint(out, row[1] as usize);
            }
        }
    }
}

/// Reads `count` rows written by [`put_columns`].
fn get_columns(bytes: &[u8], at: &mut usize, section: Section, count: usize) -> Vec<Row> {
    let mut rows = vec![[0u32; 5]; count];
    let delta = |rows: &mut [Row], at: &mut usize, field: usize, when: &dyn Fn(&Row) -> bool| {
        let mut previous = 0i64;
        for row in rows.iter_mut() {
            if when(row) {
                previous += get_zigzag(bytes, at);
                row[field] = previous as u32;
            }
        }
    };
    match section {
        Section::Entries | Section::Ops => {
            for row in rows.iter_mut() {
                row[0] = bytes[*at] as u32;
                *at += 1;
            }
            for slot in 0..3 {
                delta(&mut rows, at, 1 + slot, &|row| {
                    slots_of(row[0] as u8) > slot
                });
            }
            if matches!(section, Section::Ops) {
                for row in rows.iter_mut() {
                    // Mark retractions; asserted datums are read next.
                    row[4] = if bytes[*at] == 1 { 0 } else { NONE };
                    *at += 1;
                }
            }
            delta(&mut rows, at, 4, &|row| row[4] != NONE);
        }
        Section::Links => {
            delta(&mut rows, at, 0, &|_| true);
            for row in rows.iter_mut() {
                let flag = bytes[*at];
                *at += 1;
                row[2] = (flag & 0x7F) as u32;
                row[1] = if flag >> 7 == 1 { NONE } else { 0 };
            }
            delta(&mut rows, at, 1, &|row| row[1] != NONE);
            delta(&mut rows, at, 3, &|row| row[1] == NONE);
        }
        Section::Buffers => {
            for row in rows.iter_mut() {
                row[0] = get_varint(bytes, at) as u32;
                row[1] = get_varint(bytes, at) as u32;
            }
        }
    }
    rows
}

/// One step of a section written against the rows of the version it
/// supersedes: copy a run of the predecessor's rows, or take the next rows
/// from the frame's own columns.
enum Op {
    Copy { from: usize, len: usize },
    Literal(usize),
}

/// The ops that rebuild `rows` from `base`: runs of rows the predecessor
/// already has are copied, the rest are literals. Rows are sorted by key
/// in both, so a rewrite that changes one entry is two copies around one
/// literal.
fn diff(rows: &[Row], base: &[Row]) -> Vec<Op> {
    let at: HashMap<&Row, usize> = base.iter().enumerate().map(|(i, row)| (row, i)).collect();
    let mut ops = Vec::new();
    let mut i = 0;
    let mut expected = 0;
    while i < rows.len() {
        let start = if base.get(expected) == Some(&rows[i]) {
            Some(expected)
        } else {
            at.get(&rows[i]).copied()
        };
        match start {
            Some(from) => {
                let mut len = 1;
                while i + len < rows.len()
                    && from + len < base.len()
                    && rows[i + len] == base[from + len]
                {
                    len += 1;
                }
                ops.push(Op::Copy { from, len });
                expected = from + len;
                i += len;
            }
            None => {
                match ops.last_mut() {
                    Some(Op::Literal(n)) => *n += 1,
                    _ => ops.push(Op::Literal(1)),
                }
                i += 1;
            }
        }
    }
    ops
}

/// Writes one section: its row count, then, against a predecessor, the
/// ops, then the literal rows as columns.
fn put_section(out: &mut Vec<u8>, section: Section, rows: &[Row], base: Option<&[Row]>) {
    put_varint(out, rows.len());
    let Some(base) = base else {
        put_columns(out, section, &rows.iter().collect::<Vec<_>>());
        return;
    };
    let ops = diff(rows, base);
    put_varint(out, ops.len());
    let mut literals = Vec::new();
    let (mut i, mut expected) = (0usize, 0i64);
    for op in &ops {
        match op {
            Op::Copy { from, len } => {
                put_varint(out, len << 1 | 1);
                put_zigzag(out, *from as i64 - expected);
                expected = (*from + *len) as i64;
                i += len;
            }
            Op::Literal(n) => {
                put_varint(out, n << 1);
                literals.extend(&rows[i..i + n]);
                i += n;
            }
        }
    }
    put_columns(out, section, &literals);
}

fn get_section(bytes: &[u8], at: &mut usize, section: Section, base: Option<&[Row]>) -> Vec<Row> {
    let count = get_varint(bytes, at);
    let Some(base) = base else {
        return get_columns(bytes, at, section, count);
    };
    let steps = get_varint(bytes, at);
    let mut ops = Vec::with_capacity(steps);
    let (mut literal, mut expected) = (0usize, 0i64);
    for _ in 0..steps {
        let head = get_varint(bytes, at);
        if head & 1 == 1 {
            let from = (expected + get_zigzag(bytes, at)) as usize;
            let len = head >> 1;
            expected = (from + len) as i64;
            ops.push(Op::Copy { from, len });
        } else {
            literal += head >> 1;
            ops.push(Op::Literal(head >> 1));
        }
    }
    let literals = get_columns(bytes, at, section, literal);
    let mut rows = Vec::with_capacity(count);
    let mut next = 0;
    for op in ops {
        match op {
            Op::Copy { from, len } => rows.extend_from_slice(&base[from..from + len]),
            Op::Literal(n) => {
                rows.extend_from_slice(&literals[next..next + n]);
                next += n;
            }
        }
    }
    rows
}

/// A frame: its hash, its kind, the frame it is written against (0 for
/// none, else its position plus one), then its sections.
fn encode_rows(hash: &Blake3Hash, rows: &Rows, base: Option<(u32, &Rows)>) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(hash.as_bytes());
    out.push(rows.kind);
    put_varint(
        &mut out,
        base.map_or(0, |(position, _)| position as usize + 1),
    );
    for (i, section) in sections_of(rows.kind).iter().enumerate() {
        let against = base.map(|(_, base)| base.sections[i].as_slice());
        put_section(&mut out, *section, &rows.sections[i], against);
    }
    out
}

fn decode_rows(frame: &[u8], base: Option<&Rows>) -> Rows {
    let mut at = 32;
    let kind = frame[at];
    at += 1;
    get_varint(frame, &mut at);
    let sections = sections_of(kind)
        .iter()
        .enumerate()
        .map(|(i, section)| {
            let against = base.map(|base| base.sections[i].as_slice());
            get_section(frame, &mut at, *section, against)
        })
        .collect();
    Rows { kind, sections }
}

/// Keyframe intervals for the columnar model: 1 writes every frame whole.
const COLUMN_INTERVALS: [usize; 3] = [1, 8, 32];

/// The columnar model at one keyframe interval: its frames in file order,
/// what each is written against, and what writing them cost.
struct ColumnStore {
    interval: usize,
    frames: Vec<Vec<u8>>,
    depth: Vec<usize>,
    base: Vec<Option<u32>>,
    bytes: usize,
    against: usize,
    encode: Duration,
    append: Duration,
    file: std::fs::File,
}

impl ColumnStore {
    fn new(interval: usize, directory: &Path) -> anyhow::Result<Self> {
        Ok(Self {
            interval,
            frames: Vec::new(),
            depth: Vec::new(),
            base: Vec::new(),
            bytes: 0,
            against: 0,
            encode: Duration::ZERO,
            append: Duration::ZERO,
            file: std::fs::File::create(directory.join(format!("columnar-{interval}")))?,
        })
    }

    /// Encodes `rows` against `base` when that keeps the chain within the
    /// interval, and appends the frame to the file.
    fn put(
        &mut self,
        hash: &Blake3Hash,
        rows: &Rows,
        base: Option<(u32, &Rows)>,
    ) -> anyhow::Result<()> {
        let started = Instant::now();
        let base = base.filter(|(position, base)| {
            base.kind == rows.kind && self.depth[*position as usize] + 1 < self.interval
        });
        let frame = encode_rows(hash, rows, base);
        self.encode += started.elapsed();
        let started = Instant::now();
        self.file.write_all(&frame)?;
        self.append += started.elapsed();
        self.depth
            .push(base.map_or(0, |(position, _)| self.depth[position as usize] + 1));
        self.base.push(base.map(|(position, _)| position));
        self.against += base.is_some() as usize;
        self.bytes += frame.len();
        self.frames.push(frame);
        Ok(())
    }

    /// The rows of the frame at `position`, decoded through its chain.
    fn rows(&self, position: u32) -> Rows {
        let mut chain = vec![position];
        while let Some(base) = self.base[chain[chain.len() - 1] as usize] {
            chain.push(base);
        }
        let mut rows: Option<Rows> = None;
        for link in chain.into_iter().rev() {
            rows = Some(decode_rows(&self.frames[link as usize], rows.as_ref()));
        }
        rows.expect("a chain has at least the frame itself")
    }
}

fn entry_of(row: &Row, tokens: &Tokens) -> anyhow::Result<(Vec<u8>, Option<State<Datum>>)> {
    let form = row[0] as u8;
    let parts: Vec<&[u8]> = (0..slots_of(form))
        .map(|slot| tokens.by_id(row[1 + slot]))
        .collect();
    let datum = match row[4] {
        NONE => None,
        id => Some(datum_from(tokens.by_id(id))?),
    };
    Ok((key_from_tokens(form, &parts), datum))
}

/// Realizes the block a node's rows stand for, with the tree's own encoder.
fn realize_rows(
    rows: &Rows,
    tokens: &Tokens,
    store: &ColumnStore,
    manifest: &Manifest,
) -> anyhow::Result<Vec<u8>> {
    let body = if rows.kind == 1 {
        let [links, buffers, ops] = rows.sections.as_slice() else {
            anyhow::bail!("an index frame has three sections");
        };
        let links = links
            .iter()
            .map(|row| {
                let child = match row[1] {
                    NONE => tokens.by_id(row[3]),
                    frame => &store.frames[frame as usize][..32],
                };
                Ok(Link {
                    separator: Separator::from(tokens.by_id(row[0]).to_vec()),
                    node: Blake3Hash::from(<[u8; 32]>::try_from(child)?),
                    scale: Scale::from_u8(row[2] as u8),
                })
            })
            .collect::<anyhow::Result<Vec<_>>>()?;
        let mut novelty = Vec::with_capacity(buffers.len());
        let mut next = 0;
        for buffer in buffers {
            let len = buffer[1] as usize;
            let entries = ops[next..next + len]
                .iter()
                .map(|row| {
                    let (key, datum) = entry_of(row, tokens)?;
                    let op = match datum {
                        Some(value) => NoveltyOp::Assert(value),
                        None => NoveltyOp::Retract,
                    };
                    Ok(NoveltyEntry { key, op })
                })
                .collect::<anyhow::Result<Vec<_>>>()?;
            next += len;
            novelty.push(NoveltyBuffer::from_entries::<Key>(buffer[0], entries)?);
        }
        PersistentNodeBody::index_from_buffers(links, novelty, manifest.clone())?
    } else {
        let entries = rows.sections[0]
            .iter()
            .map(|row| {
                let (key, datum) = entry_of(row, tokens)?;
                Ok(Entry {
                    key: <Key as TreeKey>::try_from_bytes(&key)?,
                    value: datum.expect("a segment entry has a value"),
                })
            })
            .collect::<anyhow::Result<Vec<_>>>()?;
        PersistentNodeBody::from_segment(
            PersistentSegment::from_entries::<Key>(entries)?,
            manifest.clone(),
        )
    };
    Ok(body.as_bytes()?.to_vec())
}

/// Each commit's blocks, in the order the history wrote them.
fn appends_of(blocks: &[&Written]) -> Vec<Vec<Blake3Hash>> {
    let mut appends: Vec<Vec<Blake3Hash>> = Vec::new();
    let mut last = None;
    for block in blocks {
        if last != Some(block.commit) {
            appends.push(Vec::new());
            last = Some(block.commit);
        }
        appends
            .last_mut()
            .expect("an append")
            .push(block.hash.clone());
    }
    appends
}

/// The 4 KiB page a byte at `position` in the file is on.
fn page_of(position: u64) -> u64 {
    position / 4096
}

/// The pages `len` bytes from `position` cover.
fn pages_of(position: u64, len: u64) -> std::ops::RangeInclusive<u64> {
    page_of(position)..=page_of(position + len.max(1) - 1)
}

fn varint_len(value: u64) -> u64 {
    (64 - value.leading_zeros() as u64).max(1).div_ceil(7)
}

/// A block-index layout, replayed over a history one append at a time.
trait Layout {
    fn name(&self) -> String;
    /// Indexes `entries` (each block's hash and where its record starts),
    /// appending the layout's records at `*end` and moving it past them.
    /// Returns the bytes written.
    fn append(&mut self, entries: &[([u8; 32], u64)], end: &mut u64) -> u64;
    /// The pages a lookup of `hash`, with nothing cached, touches in the
    /// index before it reaches the block's own record.
    fn lookup(&self, hash: &[u8; 32]) -> HashSet<u64>;
    /// The same with the top of the layout cached, as it is for a reader
    /// that has looked anything up: a tree's root, every run's header and
    /// fences.
    fn lookup_warm(&self, hash: &[u8; 32]) -> HashSet<u64>;
}

/// A record header: kind, length, checksum.
const RECORD: u64 = 9;

/// A B+ tree keyed by block hash, its leaves holding each block's place,
/// every node a record. `keys_by_pointer` keeps no part of the hash in the
/// tree (Hyperbee's way: a key is where it is stored, and comparing reads
/// it there); otherwise each entry keeps 8 bytes of it. With `chain`, a
/// rewritten node is written against its previous version, whole every
/// `chain` versions; without, every rewrite is whole (copied paths).
struct Tree {
    fanout: usize,
    keys_by_pointer: bool,
    chain: Option<usize>,
    nodes: Vec<TreeNodeSim>,
    root: usize,
}

struct TreeNodeSim {
    leaf: bool,
    /// A leaf's hashes and places, or an internal node's separators (each
    /// child's first hash and the place of the record holding it).
    keys: Vec<([u8; 32], u64)>,
    children: Vec<usize>,
    /// Every version written, newest last: where, how long, whether whole.
    versions: Vec<(u64, u64, bool)>,
    /// The keys and children as last written, for writing against.
    written: Option<LastWritten>,
}

/// An index entry: a block's hash and where its record is.
type IndexEntry = ([u8; 32], u64);

/// A node's keys and children's places, as last written.
type LastWritten = (Vec<IndexEntry>, Vec<u64>);

impl Tree {
    fn new(fanout: usize, keys_by_pointer: bool, chain: Option<usize>) -> Self {
        Self {
            fanout,
            keys_by_pointer,
            chain,
            nodes: vec![TreeNodeSim {
                leaf: true,
                keys: Vec::new(),
                children: Vec::new(),
                versions: Vec::new(),
                written: None,
            }],
            root: 0,
        }
    }

    fn entry_len(&self, place: u64, child: Option<u64>) -> u64 {
        let key = if self.keys_by_pointer { 0 } else { 8 };
        key + varint_len(place) + child.map_or(0, varint_len)
    }

    /// Where a node's latest version is.
    fn place(&self, node: usize) -> u64 {
        self.nodes[node]
            .versions
            .last()
            .map_or(0, |version| version.0)
    }

    /// Inserts `entry`, marking every node it rewrites.
    fn insert(&mut self, entry: ([u8; 32], u64), touched: &mut HashSet<usize>) {
        let mut path = vec![self.root];
        while !self.nodes[*path.last().expect("a node")].leaf {
            let node = &self.nodes[*path.last().expect("a node")];
            let at = node
                .keys
                .partition_point(|(key, _)| key <= &entry.0)
                .saturating_sub(1);
            path.push(node.children[at]);
        }
        let leaf = *path.last().expect("a leaf");
        let at = self.nodes[leaf]
            .keys
            .partition_point(|(key, _)| key < &entry.0);
        self.nodes[leaf].keys.insert(at, entry);
        touched.extend(path.iter().copied());
        // Split overfull nodes bottom up.
        let mut level = path.len() - 1;
        loop {
            let node = path[level];
            if self.nodes[node].keys.len() <= self.fanout {
                break;
            }
            let half = self.nodes[node].keys.len() / 2;
            let keys = self.nodes[node].keys.split_off(half);
            let children = if self.nodes[node].leaf {
                Vec::new()
            } else {
                self.nodes[node].children.split_off(half)
            };
            let separator = keys[0];
            let sibling = self.nodes.len();
            self.nodes.push(TreeNodeSim {
                leaf: self.nodes[node].leaf,
                keys,
                children,
                versions: Vec::new(),
                written: None,
            });
            touched.insert(sibling);
            if level == 0 {
                let first = self.nodes[node].keys[0];
                let root = self.nodes.len();
                self.nodes.push(TreeNodeSim {
                    leaf: false,
                    keys: vec![first, separator],
                    children: vec![node, sibling],
                    versions: Vec::new(),
                    written: None,
                });
                touched.insert(root);
                self.root = root;
                break;
            }
            let parent = path[level - 1];
            let at = self.nodes[parent]
                .children
                .iter()
                .position(|child| *child == node)
                .expect("a child");
            self.nodes[parent].keys.insert(at + 1, separator);
            self.nodes[parent].children.insert(at + 1, sibling);
            level -= 1;
        }
    }

    /// Bytes of a node written whole.
    fn whole_len(&self, keys: &[([u8; 32], u64)], children: &[u64]) -> u64 {
        let mut len = RECORD + 2;
        for (at, (_, place)) in keys.iter().enumerate() {
            len += self.entry_len(*place, children.get(at).copied());
        }
        len
    }

    /// Bytes of a node written against `before`: copied runs, and the
    /// entries that are new or moved.
    fn delta_len(&self, before: &LastWritten, keys: &[([u8; 32], u64)], children: &[u64]) -> u64 {
        let old: HashMap<([u8; 32], u64, Option<u64>), usize> = before
            .0
            .iter()
            .enumerate()
            .map(|(at, (key, place))| ((*key, *place, before.1.get(at).copied()), at))
            .collect();
        let (mut len, mut runs, mut previous) = (RECORD + 2 + 3, 0u64, None::<usize>);
        for (at, (key, place)) in keys.iter().enumerate() {
            let child = children.get(at).copied();
            match old.get(&(*key, *place, child)) {
                Some(from) if previous.is_some_and(|previous| previous + 1 == *from) => {
                    previous = Some(*from)
                }
                Some(from) => {
                    runs += 1;
                    previous = Some(*from);
                }
                None => {
                    len += self.entry_len(*place, child);
                    previous = None;
                    runs += 1;
                }
            }
        }
        len + runs * 3
    }
}

impl Layout for Tree {
    fn name(&self) -> String {
        format!(
            "B-tree, {} keys a node, {}, {}",
            self.fanout,
            if self.keys_by_pointer {
                "keys as pointers"
            } else {
                "8-byte hash prefix"
            },
            match self.chain {
                None => "whole paths copied".to_string(),
                Some(chain) => format!("nodes against their last version, whole every {chain}"),
            }
        )
    }

    fn append(&mut self, entries: &[([u8; 32], u64)], end: &mut u64) -> u64 {
        let mut touched = HashSet::new();
        let mut sorted = entries.to_vec();
        sorted.sort();
        for entry in sorted {
            self.insert(entry, &mut touched);
        }
        // Children first, so a parent names where its children now are.
        let mut order: Vec<usize> = touched.into_iter().collect();
        let depth = |tree: &Tree, node: usize| {
            let mut depth = 0;
            let mut at = node;
            while !tree.nodes[at].leaf {
                at = tree.nodes[at].children[0];
                depth += 1;
            }
            depth
        };
        order.sort_by_key(|node| depth(self, *node));
        let mut written = 0;
        for node in order {
            let children: Vec<u64> = self.nodes[node]
                .children
                .iter()
                .map(|child| self.place(*child))
                .collect();
            let keys = self.nodes[node].keys.clone();
            let whole = self.whole_len(&keys, &children);
            let since_whole = self.nodes[node]
                .versions
                .iter()
                .rev()
                .take_while(|version| !version.2)
                .count();
            let len = match (&self.chain, &self.nodes[node].written) {
                (Some(chain), Some(before)) if since_whole + 1 < *chain => {
                    self.delta_len(before, &keys, &children).min(whole)
                }
                _ => whole,
            };
            let is_whole = len == whole;
            self.nodes[node].versions.push((*end, len, is_whole));
            self.nodes[node].written = Some((keys, children));
            *end += len;
            written += len;
        }
        written
    }

    fn lookup_warm(&self, hash: &[u8; 32]) -> HashSet<u64> {
        let mut pages = self.lookup(hash);
        for version in &self.nodes[self.root].versions {
            for page in pages_of(version.0, version.1) {
                pages.remove(&page);
            }
        }
        pages
    }

    fn lookup(&self, hash: &[u8; 32]) -> HashSet<u64> {
        let mut pages = HashSet::new();
        let mut at = self.root;
        loop {
            let node = &self.nodes[at];
            // The node's latest version, and every version back to its
            // last whole one: a delta is read with what it is written
            // against.
            for version in node.versions.iter().rev() {
                pages.extend(pages_of(version.0, version.1));
                if version.2 {
                    break;
                }
            }
            // Keys by pointer: each comparison of the binary search reads
            // the key where it is stored.
            if self.keys_by_pointer {
                let (mut low, mut high) = (0, node.keys.len());
                while low < high {
                    let middle = (low + high) / 2;
                    pages.insert(page_of(node.keys[middle].1));
                    if &node.keys[middle].0 <= hash {
                        low = middle + 1;
                    } else {
                        high = middle;
                    }
                }
            }
            if node.leaf {
                return pages;
            }
            let child = node
                .keys
                .partition_point(|(key, _)| key <= hash)
                .saturating_sub(1);
            at = node.children[child];
        }
    }
}

/// Sorted runs of fixed-width entries (8 bytes of hash, 8 of place), one
/// per append, merged on a carry schedule: whenever `base` runs share a
/// level, they merge into one run of the next level, as Hypercore merges
/// its roots. Each run starts with fence keys (the first hash prefix of
/// each page of entries), so a lookup reads a run's fences, then one page.
struct Runs {
    base: usize,
    /// The live runs, oldest first: level, where, entries.
    runs: Vec<(u32, u64, Vec<IndexEntry>)>,
}

const RUN_ENTRY: u64 = 16;

impl Runs {
    fn run_len(entries: usize) -> u64 {
        let pages = (entries as u64 * RUN_ENTRY).div_ceil(4096).max(1);
        RECORD + 8 + pages * 8 + entries as u64 * RUN_ENTRY
    }
}

impl Layout for Runs {
    fn name(&self) -> String {
        format!("sorted runs, merged {} at a time", self.base)
    }

    fn append(&mut self, entries: &[([u8; 32], u64)], end: &mut u64) -> u64 {
        let mut run = entries.to_vec();
        run.sort();
        let mut written = Self::run_len(run.len());
        self.runs.push((0, *end, run));
        *end += written;
        loop {
            let level = self.runs.last().expect("a run").0;
            let same = self
                .runs
                .iter()
                .rev()
                .take_while(|run| run.0 == level)
                .count();
            if same < self.base {
                break;
            }
            let merged: Vec<([u8; 32], u64)> = {
                let mut all: Vec<_> = self
                    .runs
                    .drain(self.runs.len() - same..)
                    .flat_map(|run| run.2)
                    .collect();
                all.sort();
                all
            };
            let len = Self::run_len(merged.len());
            self.runs.push((level + 1, *end, merged));
            *end += len;
            written += len;
        }
        written
    }

    fn lookup_warm(&self, hash: &[u8; 32]) -> HashSet<u64> {
        let mut pages = HashSet::new();
        for (_, place, entries) in self.runs.iter().rev() {
            let fences = (entries.len() as u64 * RUN_ENTRY).div_ceil(4096).max(1) * 8;
            let at = entries.partition_point(|(key, _)| key < hash);
            let entry_at = place
                + RECORD
                + 8
                + fences
                + at.min(entries.len().saturating_sub(1)) as u64 * RUN_ENTRY;
            // Fences in memory say whether the run can hold the hash, and on
            // which page: a run whose page range misses it costs nothing.
            if entries.get(at).is_some_and(|(key, _)| key == hash) {
                pages.insert(page_of(entry_at));
                break;
            }
        }
        pages
    }

    fn lookup(&self, hash: &[u8; 32]) -> HashSet<u64> {
        let mut pages = HashSet::new();
        for (_, place, entries) in self.runs.iter().rev() {
            // The header and fences, then the page the fences point at.
            let fences = (entries.len() as u64 * RUN_ENTRY).div_ceil(4096).max(1) * 8;
            pages.extend(pages_of(*place, RECORD + 8 + fences));
            let at = entries.partition_point(|(key, _)| key < hash);
            let entry_at = place
                + RECORD
                + 8
                + fences
                + at.min(entries.len().saturating_sub(1)) as u64 * RUN_ENTRY;
            pages.insert(page_of(entry_at));
            if entries.get(at).is_some_and(|(key, _)| key == hash) {
                break;
            }
        }
        pages
    }
}

/// Replays `appends` (each append's blocks, in the order the history wrote
/// them) into every layout, then looks up a sample of the blocks with
/// nothing cached, and reports what each layout wrote and read.
fn index_layouts(name: &str, appends: &[Vec<Blake3Hash>], record: u64) {
    let mut layouts: Vec<Box<dyn Layout>> = vec![
        Box::new(Tree::new(8, true, None)),
        Box::new(Tree::new(8, false, None)),
        Box::new(Tree::new(32, false, None)),
        Box::new(Tree::new(128, false, None)),
        Box::new(Tree::new(32, false, Some(8))),
        Box::new(Tree::new(32, false, Some(32))),
        Box::new(Tree::new(128, false, Some(8))),
        Box::new(Tree::new(128, false, Some(32))),
        Box::new(Hitchhiker::new(32, 64, None)),
        Box::new(Hitchhiker::new(32, 256, None)),
        Box::new(Hitchhiker::new(64, 256, None)),
        Box::new(Hitchhiker::new(64, 512, None)),
        Box::new(Hitchhiker::new(32, 256, Some(8))),
        Box::new(Hitchhiker::new(64, 512, Some(8))),
        Box::new(Hitchhiker::new(32, 128, Some(32))),
        Box::new(Hitchhiker::new(32, 256, Some(32))),
        Box::new(Hitchhiker::new(64, 512, Some(64))),
        Box::new(Runs {
            base: 2,
            runs: Vec::new(),
        }),
        Box::new(Runs {
            base: 4,
            runs: Vec::new(),
        }),
        Box::new(Runs {
            base: 8,
            runs: Vec::new(),
        }),
    ];
    let blocks: usize = appends.iter().map(Vec::len).sum();
    let _ = &blocks;
    println!(
        "{name}: {} appends, {blocks} blocks, a record every {record} bytes on average",
        appends.len()
    );
    println!(
        "{:<84} {:>10} {:>11} {:>11} {:>8} {:>8} {:>8} {:>8} {:>8}",
        "layout", "index MiB", "B/append", "max/append", "pages", "p99", "max", "warm", "warm p99"
    );
    // Lookups at sixteen points of the history, of every 7th block written
    // by then, so a layout is not judged at one lucky moment (a run count
    // that just carried into one run, say).
    let points: Vec<usize> = (1..=16).map(|at| appends.len() * at / 16).collect();
    for layout in &mut layouts {
        let mut end = 0u64;
        let (mut total, mut most) = (0u64, 0u64);
        let mut written: Vec<[u8; 32]> = Vec::new();
        let mut pages: Vec<usize> = Vec::new();
        let mut warm: Vec<usize> = Vec::new();
        for (at, append) in appends.iter().enumerate() {
            let entries: Vec<([u8; 32], u64)> = append
                .iter()
                .map(|hash| {
                    let place = end;
                    end += record;
                    (*hash.as_bytes(), place)
                })
                .collect();
            written.extend(entries.iter().map(|(hash, _)| *hash));
            let bytes = layout.append(&entries, &mut end);
            total += bytes;
            most = most.max(bytes);
            if points.contains(&(at + 1)) {
                pages.extend(
                    written
                        .iter()
                        .step_by(7)
                        .map(|hash| layout.lookup(hash).len()),
                );
                warm.extend(
                    written
                        .iter()
                        .step_by(7)
                        .map(|hash| layout.lookup_warm(hash).len()),
                );
            }
        }
        pages.sort();
        warm.sort();
        let mean = pages.iter().sum::<usize>() as f64 / pages.len().max(1) as f64;
        let warm_mean = warm.iter().sum::<usize>() as f64 / warm.len().max(1) as f64;
        println!(
            "{:<84} {:>10.2} {:>11.0} {:>11} {:>8.1} {:>8} {:>8} {:>8.1} {:>8}",
            layout.name(),
            total as f64 / (1024.0 * 1024.0),
            total as f64 / appends.len() as f64,
            most,
            mean,
            pages[pages.len() * 99 / 100],
            pages[pages.len() - 1],
            warm_mean,
            warm[warm.len() * 99 / 100]
        );
    }
}

/// A hitchhiker tree keyed by block hash, children named by position:
/// an append's entries land in the root's buffer, and a buffer over
/// `buffer` entries flushes its largest share down to the child it belongs
/// to, so an append rewrites the root and only now and then the nodes below.
/// Leaves hold up to `fanout` entries, internal nodes up to `fanout`
/// children. Entries keep 8 bytes of the hash and the place. With
/// `root_chain`, the root (rewritten every append) is written against its
/// last version, whole every `root_chain` versions.
struct Hitchhiker {
    fanout: usize,
    buffer: usize,
    root_chain: Option<usize>,
    nodes: Vec<HitchNode>,
    root: usize,
}

struct HitchNode {
    leaf: bool,
    /// A leaf's entries, or an internal node's separators.
    keys: Vec<IndexEntry>,
    children: Vec<usize>,
    /// An internal node's buffered entries, sorted.
    buffered: Vec<IndexEntry>,
    versions: Vec<(u64, u64, bool)>,
    last: Option<(Vec<IndexEntry>, Vec<IndexEntry>)>,
}

impl HitchNode {
    fn new(leaf: bool) -> Self {
        Self {
            leaf,
            keys: Vec::new(),
            children: Vec::new(),
            buffered: Vec::new(),
            versions: Vec::new(),
            last: None,
        }
    }
}

impl Hitchhiker {
    fn new(fanout: usize, buffer: usize, root_chain: Option<usize>) -> Self {
        Self {
            fanout,
            buffer,
            root_chain,
            nodes: vec![HitchNode::new(true)],
            root: 0,
        }
    }

    fn route(&self, node: usize, hash: &[u8; 32]) -> usize {
        self.nodes[node]
            .keys
            .partition_point(|(key, _)| key <= hash)
            .saturating_sub(1)
    }

    /// Puts `entries` into `node`: a leaf takes them, an internal node
    /// buffers them. Returns the new siblings split off `node`, each with
    /// its first key, for the caller to link.
    fn put(
        &mut self,
        node: usize,
        entries: Vec<IndexEntry>,
        touched: &mut HashSet<usize>,
    ) -> Vec<(IndexEntry, usize)> {
        touched.insert(node);
        if self.nodes[node].leaf {
            self.nodes[node].keys.extend(entries);
            self.nodes[node].keys.sort();
        } else {
            self.nodes[node].buffered.extend(entries);
            self.nodes[node].buffered.sort();
            // Flush the largest share down while the buffer is over.
            while self.nodes[node].buffered.len() > self.buffer {
                let mut shares: HashMap<usize, Vec<IndexEntry>> = HashMap::new();
                for entry in &self.nodes[node].buffered {
                    shares
                        .entry(self.route(node, &entry.0))
                        .or_default()
                        .push(*entry);
                }
                let (child, share) = shares
                    .into_iter()
                    .max_by_key(|(at, share)| (share.len(), *at))
                    .expect("a share");
                let moving: HashSet<IndexEntry> = share.iter().copied().collect();
                self.nodes[node]
                    .buffered
                    .retain(|entry| !moving.contains(entry));
                let target = self.nodes[node].children[child];
                let split = self.put(target, share, touched);
                // Link what the child split into.
                let mut at = child;
                for (separator, sibling) in split {
                    at += 1;
                    self.nodes[node].keys.insert(at, separator);
                    self.nodes[node].children.insert(at, sibling);
                }
            }
        }
        self.split(node, touched)
    }

    /// Splits `node` while it is over size, returning the siblings.
    fn split(&mut self, node: usize, touched: &mut HashSet<usize>) -> Vec<(IndexEntry, usize)> {
        let mut out = Vec::new();
        while self.nodes[node].keys.len() > self.fanout {
            let half = self.nodes[node].keys.len() / 2;
            let keys = self.nodes[node].keys.split_off(half);
            let separator = keys[0];
            let mut sibling = HitchNode::new(self.nodes[node].leaf);
            if !self.nodes[node].leaf {
                sibling.children = self.nodes[node].children.split_off(half);
                let (stay, go): (Vec<IndexEntry>, Vec<IndexEntry>) = self.nodes[node]
                    .buffered
                    .iter()
                    .partition(|entry| entry.0 < separator.0);
                self.nodes[node].buffered = stay;
                sibling.buffered = go;
            }
            sibling.keys = keys;
            let id = self.nodes.len();
            self.nodes.push(sibling);
            touched.insert(id);
            out.push((separator, id));
        }
        // Siblings in key order; each later split came off the left part.
        out.sort_by_key(|split| split.0);
        out
    }

    fn entry_len(place: u64) -> u64 {
        8 + varint_len(place)
    }

    fn node_len(&self, keys: &[IndexEntry], children: &[u64], buffered: &[IndexEntry]) -> u64 {
        let mut len = RECORD + 3;
        for (at, (_, place)) in keys.iter().enumerate() {
            len += match children.get(at) {
                Some(child) => 8 + varint_len(*child),
                None => Self::entry_len(*place),
            };
        }
        len + buffered
            .iter()
            .map(|(_, place)| Self::entry_len(*place))
            .sum::<u64>()
    }
}

impl Layout for Hitchhiker {
    fn name(&self) -> String {
        format!(
            "hitchhiker tree, {} a node, buffers of {}, root {}",
            self.fanout,
            self.buffer,
            match self.root_chain {
                None => "whole".to_string(),
                Some(chain) => format!("against its last version, whole every {chain}"),
            }
        )
    }

    fn append(&mut self, entries: &[IndexEntry], end: &mut u64) -> u64 {
        let mut touched = HashSet::new();
        let root = self.root;
        let split = self.put(root, entries.to_vec(), &mut touched);
        if !split.is_empty() {
            let mut grown = HitchNode::new(false);
            let first = self.nodes[root].keys[0];
            grown.keys.push(first);
            grown.children.push(root);
            for (separator, sibling) in split {
                grown.keys.push(separator);
                grown.children.push(sibling);
            }
            let id = self.nodes.len();
            self.nodes.push(grown);
            touched.insert(id);
            self.root = id;
            // A root over size splits again on the next append.
        }
        let depth = |tree: &Hitchhiker, node: usize| {
            let (mut depth, mut at) = (0, node);
            while !tree.nodes[at].leaf {
                at = tree.nodes[at].children[0];
                depth += 1;
            }
            depth
        };
        let mut order: Vec<usize> = touched.into_iter().collect();
        order.sort_by_key(|node| depth(self, *node));
        let mut written = 0;
        for node in order {
            let children: Vec<u64> = self.nodes[node]
                .children
                .iter()
                .map(|child| {
                    self.nodes[*child]
                        .versions
                        .last()
                        .map_or(0, |version| version.0)
                })
                .collect();
            let (keys, buffered) = (
                self.nodes[node].keys.clone(),
                self.nodes[node].buffered.clone(),
            );
            let whole = self.node_len(&keys, &children, &buffered);
            let since_whole = self.nodes[node]
                .versions
                .iter()
                .rev()
                .take_while(|version| !version.2)
                .count();
            let len = match (self.root_chain, &self.nodes[node].last) {
                (Some(chain), Some((_, before)))
                    if node == self.root && since_whole + 1 < chain =>
                {
                    // Against the last version: what the buffer gained, the
                    // links that moved, and a few copy runs.
                    let had: HashSet<&IndexEntry> = before.iter().collect();
                    let gained: u64 = buffered
                        .iter()
                        .filter(|entry| !had.contains(entry))
                        .map(|(_, place)| Self::entry_len(*place))
                        .sum();
                    (RECORD + 6 + gained + 12 * 3).min(whole)
                }
                _ => whole,
            };
            self.nodes[node].versions.push((*end, len, len == whole));
            self.nodes[node].last = Some((keys, buffered));
            *end += len;
            written += len;
        }
        written
    }

    fn lookup_warm(&self, hash: &[u8; 32]) -> HashSet<u64> {
        let mut pages = self.lookup(hash);
        for version in &self.nodes[self.root].versions {
            for page in pages_of(version.0, version.1) {
                pages.remove(&page);
            }
        }
        pages
    }

    fn lookup(&self, hash: &[u8; 32]) -> HashSet<u64> {
        let mut pages = HashSet::new();
        let mut at = self.root;
        loop {
            let node = &self.nodes[at];
            for version in node.versions.iter().rev() {
                pages.extend(pages_of(version.0, version.1));
                if version.2 {
                    break;
                }
            }
            if node.leaf
                || node
                    .buffered
                    .binary_search_by(|entry| entry.0.cmp(hash))
                    .is_ok()
            {
                return pages;
            }
            at = node.children[self.route(at, hash)];
        }
    }
}

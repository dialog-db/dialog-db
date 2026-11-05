//! Single-file census: what a replica's archive would weigh in one
//! append-only file, under several encodings, against file-per-block, and
//! what each costs to read a block back.
//!
//! The spike for `ideas/single-file-format.md`. It replays the Stack
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
//!   the tree's own encoder, and the census fails unless the result is the
//!   original block, byte for byte.
//! - **tokens+zstd frames**, **tokens+delta frames/K**: the idea with each
//!   frame compressed alone, or against the frame it supersedes.
//! - **stream**: the whole log through one zstd stream with long-distance
//!   matching. No random access; a floor for what any encoding of these
//!   bytes can reach.
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
//!   point it at a replica that wrote its own history. `disk <commits>`
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

use base58::ToBase58 as _;
use dialog_artifacts::history::{Edition, Record};
use dialog_artifacts::tree::{ArtifactTree, fetch_spilled};
use dialog_artifacts::{
    ATTRIBUTE_KEY_TAG, Artifact, BLOB_KEY_TAG, Datum, DialogArtifactsError, ENTITY_KEY_TAG,
    Instruction, Key, LoadBlob, Pick, Relation, State, VALUE_KEY_TAG, history_region_range,
};
use dialog_baseline::metered::Meter;
use dialog_baseline::nodes::TreeNode;
use dialog_baseline::repo::DialogRepo;
use dialog_baseline::se::{SeLog, se_instructions};
use dialog_capability::Provider;
use dialog_common::{Blake3Hash, Buffer};
use dialog_search_tree::{
    DialogSearchTreeError, Entry, Key as TreeKey, Link, LoadBlock, Manifest, MemoryBlocks,
    NodeBody, Nodes, NoveltyBuffer, NoveltyEntry, NoveltyOp, PersistentNodeBody, PersistentSegment,
    Scale, Separator, into_owned,
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
    offsets: HashMap<Vec<u8>, u32>,
    region: Vec<u8>,
    /// Distinct tokens and their bytes, per kind.
    by_kind: HashMap<&'static str, (usize, usize)>,
    /// References made to each kind, distinct or not.
    references: HashMap<&'static str, usize>,
}

impl Tokens {
    fn intern(&mut self, kind: &'static str, token: &[u8]) -> u32 {
        *self.references.entry(kind).or_default() += 1;
        if let Some(offset) = self.offsets.get(token) {
            return *offset;
        }
        let offset = self.region.len() as u32;
        put_varint(&mut self.region, token.len());
        self.region.extend_from_slice(token);
        let share = self.by_kind.entry(kind).or_default();
        share.0 += 1;
        share.1 += self.region.len() - offset as usize;
        self.offsets.insert(token.to_vec(), offset);
        offset
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
            out.extend_from_slice(&(index.novelty.len() as u32).to_le_bytes());
            for buffer in index.novelty.iter() {
                let entries = buffer.entries::<Key>()?;
                out.extend_from_slice(&buffer.child.to_native().to_le_bytes());
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
            share.index_blocks += node.buffer().as_ref().len();
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
            share.segment_blocks += node.buffer().as_ref().len();
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
        bytes += node.buffer().as_ref().len();
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
        "commits,live_nodes,live_mib,live_entries,blocks,fs_mib,fs_alloc_mib,log_mib,log_zstd_mib,{},tokens_mib,frames_mib,tokenized_mib,tokenized_zstd_mib,{},stream_mib",
        deltas("log_delta"),
        deltas("tokenized_delta"),
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
        "{},{},{},{live_entries},{},{},{},{},{},{},{},{},{},{},{},{}",
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
    let modified: HashMap<Blake3Hash, SystemTime> = read
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

/// A replica's file-per-block store, read as an archive the artifact tree
/// can load nodes and spilled values from: nodes from memory, spilled
/// values from the replica's blob directory.
#[derive(Clone)]
struct Replica {
    nodes: MemoryBlocks,
    blobs: PathBuf,
}

#[async_trait::async_trait]
impl Provider<LoadBlock> for Replica {
    async fn execute(
        &self,
        LoadBlock { hash }: LoadBlock,
    ) -> Result<Option<Buffer>, DialogSearchTreeError> {
        Ok(self.nodes.get(&hash))
    }
}

impl Nodes<Key, State<Datum>> for Replica {}

#[async_trait::async_trait]
impl Provider<LoadBlob> for Replica {
    async fn execute(
        &self,
        LoadBlob { hash }: LoadBlob,
    ) -> Result<Option<Buffer>, DialogArtifactsError> {
        Ok(std::fs::read(self.blobs.join(hash.as_bytes().to_base58()))
            .ok()
            .map(Buffer::from))
    }
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
    let replica = Replica {
        nodes: MemoryBlocks::new(),
        blobs: directory.join("blob"),
    };
    for block in &archive.blocks {
        replica.nodes.store(block.bytes.clone());
    }
    drop(archive);
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    let mut revisions: BTreeMap<(Edition, [u8; 32]), Vec<Instruction>> = BTreeMap::new();
    let mut stand_ins = 0usize;
    let mut reserved = 0usize;
    runtime.block_on(async {
        let tree = ArtifactTree::from_hash(head.clone());
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
            let spilled = match fetch_spilled(&replica, &entry.key).await {
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
            let artifact = Artifact {
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
    measure(source, &points)
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

    // Every frame realizes to the block it stands for, byte for byte,
    // under the manifest of the tree the node belongs to.
    let realized = Instant::now();
    for block in &blocks {
        let Some(node) = nodes.get(&block.hash) else {
            continue;
        };
        let frame = frames.of(&block.hash).expect("every node has a frame");
        let bytes = realize(frame, &tokens, &frames, &node.manifest()?)?;
        anyhow::ensure!(
            bytes.as_slice() == block.bytes.as_ref(),
            "the frame of {} realized to {} bytes that differ from its {}-byte block",
            block.hash,
            bytes.len(),
            block.bytes.as_ref().len()
        );
    }
    eprintln!(
        "\nrealize: all {} frames rebuild their block byte for byte ({:.1}s)",
        frames.frames.len(),
        realized.elapsed().as_secs_f64()
    );

    read_costs(
        &roots[roots.len() - 1],
        &nodes,
        &blocks,
        &position,
        &log_zstd,
        &log_deltas,
        &tokens,
        &frames,
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
    Ok(())
}

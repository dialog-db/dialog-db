//! Single-file content-addressed archive engine (DCAA v1).
//!
//! Implements `notes/dcaa.md` including both review-note amendments:
//!
//! 1. **Per-commit index deltas.** A commit appends only the index records
//!    for the entries it introduces; the footer carries the location of the
//!    newest merged (base) index plus the length of the delta chain since.
//!    Lookup searches deltas newest-first, then the base. When the chain
//!    would exceed the fold threshold the commit writes a merged index
//!    instead, resetting the chain.
//! 2. **Outboard policy for small blobs.** Blobs at or below
//!    [`OUTBOARD_THRESHOLD`] store `outboard_len = 0` and are verified by a
//!    whole-blob BLAKE3 hash on read; larger blobs store a BAO outboard
//!    tree (8-byte LE length header + pre-order parent nodes, per the spec)
//!    and are verified chunk-by-chunk against it.
//!
//! Deviations from the spec text, both forced by the amendments and the
//! single-fsync commit protocol (documented in
//! `notes/sqlite-baseline-results.md`):
//!
//! - The footer is 72 bytes, not 40: the delta-chain amendment needs the
//!   base index location, the previous footer offset, and the chain length.
//! - The footer checksum covers this commit's entire appended payload
//!   (records + index) as well as the footer prefix, not just the footer
//!   bytes. With a single fsync per commit there is no write barrier
//!   between payload and footer, so the kernel may persist the footer page
//!   before the payload pages; a footer-only checksum would then validate a
//!   torn commit. Hashing the payload (already in memory at commit time)
//!   closes that hole at no extra write cost, and recovery pays O(tail
//!   commit) to verify it.

use std::collections::BTreeMap;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use bao_tree::io::outboard::PreOrderMemOutboard;
use bao_tree::io::sync::encode_ranges_validated;
use bao_tree::{BaoTree, BlockSize, ChunkRanges};

/// A content address: the BLAKE3 hash of the blob.
pub type Address = [u8; 32];

const MAGIC: &[u8; 4] = b"DCAA";
const VERSION: u16 = 1;
const HEADER_LEN: u64 = 8;
const FOOTER_LEN: u64 = 72;
const INDEX_RECORD_LEN: u64 = 40;
/// The number of footer bytes covered by the checksum together with the
/// commit payload (everything before the checksum field itself).
const FOOTER_CHECKSUM_PREFIX: usize = 64;

/// Blobs at or below this size store no outboard tree and are verified by
/// a whole-blob BLAKE3 hash; larger blobs carry a BAO outboard for
/// chunk-granular verification (review amendment 2).
pub const OUTBOARD_THRESHOLD: usize = 64 * 1024;

/// Default delta-chain length above which a commit folds the chain into a
/// fresh merged index (review amendment 1). A threshold of 0 folds on
/// every commit, i.e. the original spec behavior of one complete merged
/// index per commit.
pub const DEFAULT_FOLD_THRESHOLD: usize = 32;

fn hex(address: &Address) -> String {
    address.iter().map(|b| format!("{b:02x}")).collect()
}

/// Errors from the archive engine, mirroring the spec's error set.
#[derive(Debug, thiserror::Error)]
pub enum CasError {
    /// Underlying I/O failure.
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    /// The file is not a DCAA v1 archive.
    #[error("format error: {0}")]
    Format(String),
    /// A stored record failed verification against its address.
    #[error("corrupt record for {}", hex(.0))]
    Corrupt(Address),
    /// The address has never been committed.
    #[error("not found: {}", hex(.0))]
    NotFound(Address),
    /// The address is redacted: readers treat it as absent, writers must
    /// not re-insert it.
    #[error("redacted: {}", hex(.0))]
    Redacted(Address),
}

/// A committed index entry: `offset == 0` is the redaction sentinel (safe
/// because the 8-byte header occupies offset 0).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct IndexRecord {
    hash: Address,
    offset: u64,
}

/// A resolved live entry. Produced by [`CasFile::get`]; carries the record
/// geometry so a follow-up read seeks straight to the bytes.
#[derive(Debug, Clone, Copy)]
pub struct CasEntry {
    /// The content address.
    pub address: Address,
    /// Offset of the blob record's first byte.
    pub offset: u64,
    /// Raw blob length.
    pub blob_len: u64,
    /// BAO outboard tree length (0 = whole-blob verification).
    pub outboard_len: u64,
}

/// A single-file content-addressed archive (`.dialog`), append-only with
/// one fsync per commit.
#[derive(Debug)]
pub struct CasFile {
    file: File,
    path: PathBuf,
    /// Committed file length; bytes past this never survive reopen.
    committed_len: u64,
    /// Total committed entries, including redacted ones.
    entry_count: u64,
    /// The newest merged index, sorted by hash.
    base: Vec<IndexRecord>,
    base_offset: u64,
    base_count: u64,
    /// Per-commit delta indexes since the base, oldest first, each sorted.
    deltas: Vec<Vec<IndexRecord>>,
    /// Offset of the last committed footer (0 = none).
    last_footer: u64,
    fold_threshold: usize,
    /// When false, commits skip the per-commit fdatasync (relaxed mode).
    durable: bool,
}

impl CasFile {
    /// Open or create the archive at `path`, recovering from any torn tail
    /// per the spec's crash model. `fold_threshold` is the maximum delta
    /// chain length before a commit folds the chain (0 = merged index
    /// every commit). `durable` controls the per-commit fdatasync: when
    /// false, commits leave persistence to the OS writeback cache — the
    /// same (absence of a) durability guarantee the file-per-block
    /// archive provides. Crash RECOVERY is unaffected either way: the
    /// footer scan still finds the last commit whose bytes actually
    /// reached the disk and truncates the rest; relaxed mode only widens
    /// how many recent commits that can be.
    pub fn open(
        path: impl AsRef<Path>,
        fold_threshold: usize,
        durable: bool,
    ) -> Result<Self, CasError> {
        let path = path.as_ref().to_path_buf();
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&path)?;
        let len = file.metadata()?.len();

        let mut store = Self {
            file,
            path,
            committed_len: HEADER_LEN,
            entry_count: 0,
            base: Vec::new(),
            base_offset: 0,
            base_count: 0,
            deltas: Vec::new(),
            last_footer: 0,
            fold_threshold,
            durable,
        };

        if len == 0 {
            store.file.write_all(&header_bytes())?;
            if durable {
                store.file.sync_data()?;
            }
            return Ok(store);
        }

        // Recovery reads the whole file once: footers can sit arbitrarily
        // far from the tail when a large commit was torn, and the payload
        // checksum needs the commit bytes anyway.
        let mut bytes = Vec::with_capacity(len as usize);
        store.file.seek(SeekFrom::Start(0))?;
        store.file.read_to_end(&mut bytes)?;
        if bytes.len() < HEADER_LEN as usize || &bytes[0..4] != MAGIC {
            return Err(CasError::Format("missing DCAA header".into()));
        }
        let version = u16::from_le_bytes([bytes[4], bytes[5]]);
        if version != VERSION {
            return Err(CasError::Format(format!("unsupported version {version}")));
        }

        let committed = find_last_valid_footer(&bytes);
        let committed_len = match committed {
            Some(footer_offset) => footer_offset + FOOTER_LEN,
            // No valid footer anywhere: policy is "empty store" (a crash
            // during the very first commit legitimately leaves this state).
            None => HEADER_LEN,
        };
        if committed_len < len {
            store.file.set_len(committed_len)?;
            store.file.sync_data()?;
        }
        store.committed_len = committed_len;

        if let Some(footer_offset) = committed {
            store.load_state(&bytes, footer_offset)?;
        }
        Ok(store)
    }

    /// Rebuild in-memory index state from the tail footer at
    /// `footer_offset`, walking the delta chain through previous footers.
    fn load_state(&mut self, bytes: &[u8], footer_offset: u64) -> Result<(), CasError> {
        let footer =
            Footer::parse(&bytes[footer_offset as usize..(footer_offset + FOOTER_LEN) as usize]);
        self.entry_count = footer.entry_count;
        self.base_offset = footer.base_offset;
        self.base_count = footer.base_count;
        self.last_footer = footer_offset;
        self.base = read_index_region(bytes, footer.base_offset, footer.base_count)?;

        let mut deltas = Vec::with_capacity(footer.chain_len as usize);
        let mut cursor = footer;
        for step in 0..footer.chain_len {
            deltas.push(read_index_region(
                bytes,
                cursor.index_offset,
                cursor.index_count,
            )?);
            if step + 1 < footer.chain_len {
                let start = cursor.prev_footer as usize;
                let end = start + FOOTER_LEN as usize;
                if cursor.prev_footer < HEADER_LEN || end > bytes.len() {
                    return Err(CasError::Format("delta chain footer out of bounds".into()));
                }
                let prev = Footer::parse(&bytes[start..end]);
                if &prev.magic != MAGIC || prev.version != VERSION {
                    return Err(CasError::Format("delta chain footer invalid".into()));
                }
                cursor = prev;
            }
        }
        deltas.reverse();
        self.deltas = deltas;
        Ok(())
    }

    /// The archive's on-disk path.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Committed file length in bytes. The file is strictly append-only,
    /// so this equals the total bytes ever written (including folds).
    pub fn file_len(&self) -> u64 {
        self.committed_len
    }

    /// Number of delta indexes currently chained after the base index.
    pub fn chain_len(&self) -> usize {
        self.deltas.len()
    }

    /// Total committed entries, including redacted ones.
    pub fn len(&self) -> u64 {
        self.entry_count
    }

    /// True when nothing has ever been committed.
    pub fn is_empty(&self) -> bool {
        self.entry_count == 0
    }

    /// Look up an address: `None` = never committed, `Some(0)` = redacted,
    /// `Some(offset)` = live record. Searches delta indexes newest-first,
    /// then the base (review amendment 1).
    fn lookup(&self, address: &Address) -> Option<u64> {
        for delta in self.deltas.iter().rev() {
            if let Ok(i) = delta.binary_search_by(|r| r.hash.cmp(address)) {
                return Some(delta[i].offset);
            }
        }
        self.base
            .binary_search_by(|r| r.hash.cmp(address))
            .ok()
            .map(|i| self.base[i].offset)
    }

    /// True when the address resolves to a live (non-redacted) entry.
    pub fn contains(&self, address: &Address) -> bool {
        matches!(self.lookup(address), Some(offset) if offset != 0)
    }

    /// Resolve an address to its entry without loading blob bytes (only
    /// the 16-byte record header is read).
    pub fn get(&mut self, address: &Address) -> Result<CasEntry, CasError> {
        match self.lookup(address) {
            None => Err(CasError::NotFound(*address)),
            Some(0) => Err(CasError::Redacted(*address)),
            Some(offset) => {
                let mut header = [0u8; 16];
                self.file.seek(SeekFrom::Start(offset))?;
                self.file.read_exact(&mut header)?;
                let blob_len = u64::from_le_bytes(header[0..8].try_into().expect("8 bytes"));
                let outboard_len = u64::from_le_bytes(header[8..16].try_into().expect("8 bytes"));
                let end = offset
                    .checked_add(16)
                    .and_then(|v| v.checked_add(outboard_len))
                    .and_then(|v| v.checked_add(blob_len));
                match end {
                    Some(end) if end <= self.committed_len => Ok(CasEntry {
                        address: *address,
                        offset,
                        blob_len,
                        outboard_len,
                    }),
                    _ => Err(CasError::Corrupt(*address)),
                }
            }
        }
    }

    /// Read and verify a blob. Verification is BAO chunk-verified when the
    /// record carries an outboard tree, whole-blob BLAKE3 otherwise.
    pub fn read(&mut self, address: &Address) -> Result<Vec<u8>, CasError> {
        let entry = self.get(address)?;
        let mut outboard = vec![0u8; entry.outboard_len as usize];
        let mut blob = vec![0u8; entry.blob_len as usize];
        self.file.seek(SeekFrom::Start(entry.offset + 16))?;
        self.file.read_exact(&mut outboard)?;
        self.file.read_exact(&mut blob)?;

        if entry.outboard_len == 0 {
            if blake3::hash(&blob).as_bytes() != address {
                return Err(CasError::Corrupt(*address));
            }
            return Ok(blob);
        }

        // The stored outboard is the spec's BAO shape: an 8-byte LE length
        // header followed by the pre-order parent nodes.
        if outboard.len() < 8
            || u64::from_le_bytes(outboard[0..8].try_into().expect("8 bytes")) != entry.blob_len
        {
            return Err(CasError::Corrupt(*address));
        }
        let rebuilt = PreOrderMemOutboard {
            root: blake3::Hash::from(*address),
            tree: BaoTree::new(entry.blob_len, BlockSize::ZERO),
            data: &outboard[8..],
        };
        let mut sink = std::io::sink();
        encode_ranges_validated(&blob[..], &rebuilt, &ChunkRanges::all(), &mut sink)
            .map_err(|_| CasError::Corrupt(*address))?;
        Ok(blob)
    }

    /// Begin a transaction. A dropped transaction is discarded.
    pub fn begin(&mut self) -> CasTransaction<'_> {
        CasTransaction {
            store: self,
            ops: Vec::new(),
        }
    }

    /// Append this commit's records, index, and footer, then fsync once.
    fn apply(&mut self, ops: Vec<PendingOp>) -> Result<(), CasError> {
        if ops.is_empty() {
            return Ok(());
        }
        let commit_start = self.committed_len;
        let mut payload: Vec<u8> = Vec::new();
        let mut delta_map: BTreeMap<Address, u64> = BTreeMap::new();

        for op in ops {
            match op {
                PendingOp::Insert {
                    address,
                    bytes,
                    outboard,
                } => {
                    let offset = commit_start + payload.len() as u64;
                    payload.extend_from_slice(&(bytes.len() as u64).to_le_bytes());
                    payload.extend_from_slice(&(outboard.len() as u64).to_le_bytes());
                    payload.extend_from_slice(&outboard);
                    payload.extend_from_slice(&bytes);
                    delta_map.insert(address, offset);
                }
                PendingOp::Redact(address) => {
                    delta_map.insert(address, 0);
                }
            }
        }

        let delta: Vec<IndexRecord> = delta_map
            .into_iter()
            .map(|(hash, offset)| IndexRecord { hash, offset })
            .collect();
        let new_entries = delta
            .iter()
            .filter(|r| self.lookup(&r.hash).is_none())
            .count() as u64;
        let entry_count = self.entry_count + new_entries;

        let fold = self.deltas.len() + 1 > self.fold_threshold;
        let (index, chain_len): (Vec<IndexRecord>, u32) = if fold {
            let mut merged: BTreeMap<Address, u64> =
                self.base.iter().map(|r| (r.hash, r.offset)).collect();
            for chained in &self.deltas {
                merged.extend(chained.iter().map(|r| (r.hash, r.offset)));
            }
            merged.extend(delta.iter().map(|r| (r.hash, r.offset)));
            (
                merged
                    .into_iter()
                    .map(|(hash, offset)| IndexRecord { hash, offset })
                    .collect(),
                0,
            )
        } else {
            (delta.clone(), self.deltas.len() as u32 + 1)
        };

        let index_offset = commit_start + payload.len() as u64;
        for record in &index {
            payload.extend_from_slice(&record.hash);
            payload.extend_from_slice(&record.offset.to_le_bytes());
        }
        let footer_offset = commit_start + payload.len() as u64;

        let (base_offset, base_count) = if fold {
            (index_offset, index.len() as u64)
        } else {
            (self.base_offset, self.base_count)
        };
        let footer = Footer {
            magic: *MAGIC,
            version: VERSION,
            entry_count,
            index_offset,
            index_count: index.len() as u64,
            base_offset,
            base_count,
            prev_footer: self.last_footer,
            chain_len,
        };
        let footer_bytes = footer.to_bytes(&payload);
        payload.extend_from_slice(&footer_bytes);

        self.file.seek(SeekFrom::Start(commit_start))?;
        self.file.write_all(&payload)?;
        // The single durability point. Ordering justification: everything
        // in this commit rides one write; the footer checksum covers the
        // whole appended payload, so a crash that persists the footer page
        // but not every payload page yields a footer that fails
        // verification and recovery truncates to the previous commit. A
        // crash before fdatasync returns loses at most this commit; after
        // it returns the commit is durable, including the file size
        // (fdatasync flushes the metadata needed to read appended bytes).
        // Relaxed mode skips the fdatasync: recovery still works (the
        // footer scan finds whatever prefix the OS actually persisted),
        // but recent commits can be lost — the file-per-block archive's
        // durability level.
        if self.durable {
            self.file.sync_data()?;
        }

        self.committed_len = footer_offset + FOOTER_LEN;
        self.entry_count = entry_count;
        self.last_footer = footer_offset;
        if fold {
            self.base = index;
            self.base_offset = base_offset;
            self.base_count = base_count;
            self.deltas.clear();
        } else {
            self.deltas.push(delta);
        }
        Ok(())
    }
}

/// A pending mutation staged by a transaction.
enum PendingOp {
    Insert {
        address: Address,
        bytes: Vec<u8>,
        /// Spec-shaped outboard field (8-byte LE length header + pre-order
        /// parents), empty for blobs at or below [`OUTBOARD_THRESHOLD`].
        outboard: Vec<u8>,
    },
    Redact(Address),
}

/// A write transaction: staged inserts and redactions, committed as one
/// atomic durable append. Dropping the transaction discards it.
pub struct CasTransaction<'a> {
    store: &'a mut CasFile,
    ops: Vec<PendingOp>,
}

impl CasTransaction<'_> {
    /// The transaction-local state of an address, if any op staged it.
    fn staged(&self, address: &Address) -> Option<bool> {
        self.ops.iter().rev().find_map(|op| match op {
            PendingOp::Insert { address: a, .. } if a == address => Some(true),
            PendingOp::Redact(a) if a == address => Some(false),
            _ => None,
        })
    }

    /// Stage a blob insert. Duplicate inserts (already committed or staged
    /// in this transaction) are no-ops; inserting a redacted address fails
    /// with [`CasError::Redacted`].
    pub fn insert(&mut self, bytes: &[u8]) -> Result<Address, CasError> {
        let (address, outboard) = if bytes.len() > OUTBOARD_THRESHOLD {
            let ob = PreOrderMemOutboard::create(bytes, BlockSize::ZERO);
            let mut outboard = Vec::with_capacity(8 + ob.data.len());
            outboard.extend_from_slice(&(bytes.len() as u64).to_le_bytes());
            outboard.extend_from_slice(&ob.data);
            (*ob.root.as_bytes(), outboard)
        } else {
            (*blake3::hash(bytes).as_bytes(), Vec::new())
        };

        match self.staged(&address) {
            Some(true) => return Ok(address),
            Some(false) => return Err(CasError::Redacted(address)),
            None => {}
        }
        match self.store.lookup(&address) {
            Some(0) => Err(CasError::Redacted(address)),
            Some(_) => Ok(address),
            None => {
                self.ops.push(PendingOp::Insert {
                    address,
                    bytes: bytes.to_vec(),
                    outboard,
                });
                Ok(address)
            }
        }
    }

    /// Stage a redaction. Redacting an already-redacted address is a
    /// no-op; the content bytes are never removed, only marked absent.
    pub fn redact(&mut self, address: &Address) -> Result<(), CasError> {
        if self.staged(address).is_none() && self.store.lookup(address) == Some(0) {
            return Ok(());
        }
        self.ops.push(PendingOp::Redact(*address));
        Ok(())
    }

    /// Commit all staged operations as one atomic durable append (a
    /// single fsync). A transaction that staged nothing writes nothing.
    pub fn commit(self) -> Result<(), CasError> {
        let Self { store, ops } = self;
        store.apply(ops)
    }
}

fn header_bytes() -> [u8; HEADER_LEN as usize] {
    let mut header = [0u8; HEADER_LEN as usize];
    header[0..4].copy_from_slice(MAGIC);
    header[4..6].copy_from_slice(&VERSION.to_le_bytes());
    header
}

/// The commit footer. 72 bytes on disk; see the module docs for why it is
/// wider than the spec's 40 bytes.
#[derive(Debug, Clone, Copy)]
struct Footer {
    magic: [u8; 4],
    version: u16,
    entry_count: u64,
    index_offset: u64,
    index_count: u64,
    base_offset: u64,
    base_count: u64,
    prev_footer: u64,
    chain_len: u32,
}

impl Footer {
    fn parse(bytes: &[u8]) -> Self {
        let u64at = |i: usize| u64::from_le_bytes(bytes[i..i + 8].try_into().expect("8 bytes"));
        Self {
            magic: bytes[0..4].try_into().expect("4 bytes"),
            version: u16::from_le_bytes([bytes[4], bytes[5]]),
            entry_count: u64at(8),
            index_offset: u64at(16),
            index_count: u64at(24),
            base_offset: u64at(32),
            base_count: u64at(40),
            prev_footer: u64at(48),
            chain_len: u32::from_le_bytes(bytes[56..60].try_into().expect("4 bytes")),
        }
    }

    /// Serialize, computing the checksum over `commit_payload` (this
    /// commit's records + index bytes) followed by the footer prefix.
    fn to_bytes(self, commit_payload: &[u8]) -> [u8; FOOTER_LEN as usize] {
        let mut bytes = [0u8; FOOTER_LEN as usize];
        bytes[0..4].copy_from_slice(&self.magic);
        bytes[4..6].copy_from_slice(&self.version.to_le_bytes());
        bytes[8..16].copy_from_slice(&self.entry_count.to_le_bytes());
        bytes[16..24].copy_from_slice(&self.index_offset.to_le_bytes());
        bytes[24..32].copy_from_slice(&self.index_count.to_le_bytes());
        bytes[32..40].copy_from_slice(&self.base_offset.to_le_bytes());
        bytes[40..48].copy_from_slice(&self.base_count.to_le_bytes());
        bytes[48..56].copy_from_slice(&self.prev_footer.to_le_bytes());
        bytes[56..60].copy_from_slice(&self.chain_len.to_le_bytes());
        let mut hasher = blake3::Hasher::new();
        hasher.update(commit_payload);
        hasher.update(&bytes[0..FOOTER_CHECKSUM_PREFIX]);
        let digest = hasher.finalize();
        bytes[64..72].copy_from_slice(&digest.as_bytes()[0..8]);
        bytes
    }
}

/// Find the last valid footer offset in `bytes`, or `None`. Tries the
/// exact tail first (the common case), then scans backwards for `"DCAA"`
/// candidates; each candidate must pass structural checks and the payload
/// checksum, which eliminates false magic matches and torn commits.
fn find_last_valid_footer(bytes: &[u8]) -> Option<u64> {
    let len = bytes.len() as u64;
    if len < HEADER_LEN + FOOTER_LEN {
        return None;
    }
    let tail = len - FOOTER_LEN;
    if validate_footer(bytes, tail) {
        return Some(tail);
    }
    let mut candidate = tail;
    while candidate > HEADER_LEN {
        candidate -= 1;
        if &bytes[candidate as usize..candidate as usize + 4] == MAGIC
            && validate_footer(bytes, candidate)
        {
            return Some(candidate);
        }
    }
    None
}

/// Validate a candidate footer at `offset`: structural invariants first
/// (cheap), then the checksum over the commit payload and footer prefix.
fn validate_footer(bytes: &[u8], offset: u64) -> bool {
    let start = offset as usize;
    let end = start + FOOTER_LEN as usize;
    if offset < HEADER_LEN || end > bytes.len() {
        return false;
    }
    let footer = Footer::parse(&bytes[start..end]);
    if &footer.magic != MAGIC || footer.version != VERSION {
        return false;
    }
    // The commit's own index is appended immediately before its footer.
    let index_len = match footer.index_count.checked_mul(INDEX_RECORD_LEN) {
        Some(v) => v,
        None => return false,
    };
    if footer.index_offset.checked_add(index_len) != Some(offset) {
        return false;
    }
    let commit_start = if footer.prev_footer == 0 {
        HEADER_LEN
    } else {
        match footer.prev_footer.checked_add(FOOTER_LEN) {
            Some(v) => v,
            None => return false,
        }
    };
    if commit_start > footer.index_offset {
        return false;
    }
    let base_len = match footer.base_count.checked_mul(INDEX_RECORD_LEN) {
        Some(v) => v,
        None => return false,
    };
    // `base_offset == 0` with `base_count == 0` is the empty base of the
    // commits before the first fold.
    let base_empty = footer.base_offset == 0 && footer.base_count == 0;
    match footer.base_offset.checked_add(base_len) {
        _ if base_empty => {}
        Some(base_end) if footer.base_offset >= HEADER_LEN && base_end <= offset => {}
        _ => return false,
    }
    let mut hasher = blake3::Hasher::new();
    hasher.update(&bytes[commit_start as usize..start]);
    hasher.update(&bytes[start..start + FOOTER_CHECKSUM_PREFIX]);
    let digest = hasher.finalize();
    digest.as_bytes()[0..8] == bytes[start + FOOTER_CHECKSUM_PREFIX..end]
}

/// Parse an index region out of the recovery byte buffer.
fn read_index_region(bytes: &[u8], offset: u64, count: u64) -> Result<Vec<IndexRecord>, CasError> {
    let start = offset as usize;
    let len = (count * INDEX_RECORD_LEN) as usize;
    if start + len > bytes.len() {
        return Err(CasError::Format("index region out of bounds".into()));
    }
    let mut records = Vec::with_capacity(count as usize);
    for i in 0..count as usize {
        let record = &bytes[start + i * INDEX_RECORD_LEN as usize..][..INDEX_RECORD_LEN as usize];
        records.push(IndexRecord {
            hash: record[0..32].try_into().expect("32 bytes"),
            offset: u64::from_le_bytes(record[32..40].try_into().expect("8 bytes")),
        });
    }
    Ok(records)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_path(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "dcaa-cas-{name}-{}-{}.dialog",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("clock")
                .as_nanos()
        ))
    }

    fn commit_one(store: &mut CasFile, bytes: &[u8]) -> Address {
        let mut tx = store.begin();
        let address = tx.insert(bytes).expect("insert");
        tx.commit().expect("commit");
        address
    }

    #[test]
    fn it_round_trips_small_and_large_blobs() {
        let path = temp_path("roundtrip");
        let mut store = CasFile::open(&path, DEFAULT_FOLD_THRESHOLD, true).expect("open");

        let small = b"hello dcaa".to_vec();
        let large: Vec<u8> = (0..200_000u32).map(|i| (i % 251) as u8).collect();
        let empty: Vec<u8> = Vec::new();

        let a = commit_one(&mut store, &small);
        let b = commit_one(&mut store, &large);
        let c = commit_one(&mut store, &empty);

        assert_eq!(store.read(&a).expect("read small"), small);
        assert_eq!(store.read(&b).expect("read large"), large);
        assert_eq!(store.read(&c).expect("read empty"), empty);

        // Amendment 2: only the large blob carries an outboard tree.
        assert_eq!(store.get(&a).expect("get").outboard_len, 0);
        assert!(store.get(&b).expect("get").outboard_len > 0);

        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn it_reopens_and_reads_back() {
        let path = temp_path("reopen");
        let payloads: Vec<Vec<u8>> = (0..10u8).map(|i| vec![i; 100 + i as usize]).collect();
        let mut addresses = Vec::new();
        {
            let mut store = CasFile::open(&path, DEFAULT_FOLD_THRESHOLD, true).expect("open");
            for payload in &payloads {
                addresses.push(commit_one(&mut store, payload));
            }
        }
        let mut store = CasFile::open(&path, DEFAULT_FOLD_THRESHOLD, true).expect("reopen");
        assert_eq!(store.len(), payloads.len() as u64);
        for (address, payload) in addresses.iter().zip(&payloads) {
            assert_eq!(&store.read(address).expect("read"), payload);
        }
        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn it_dedups_duplicate_inserts_without_writing() {
        let path = temp_path("dedup");
        let mut store = CasFile::open(&path, DEFAULT_FOLD_THRESHOLD, true).expect("open");
        let address = commit_one(&mut store, b"same bytes");
        let len_after_first = store.file_len();

        // A duplicate insert is a no-op: the transaction stages nothing
        // and the commit appends nothing (not even a footer).
        let mut tx = store.begin();
        assert_eq!(tx.insert(b"same bytes").expect("insert"), address);
        tx.commit().expect("commit");
        assert_eq!(store.file_len(), len_after_first);
        assert_eq!(store.len(), 1);

        // Duplicate within one transaction: one record, one entry.
        let mut tx = store.begin();
        tx.insert(b"fresh").expect("insert");
        tx.insert(b"fresh").expect("insert");
        tx.commit().expect("commit");
        assert_eq!(store.len(), 2);

        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn it_redacts_and_rejects_reinsertion() {
        let path = temp_path("redact");
        let mut store = CasFile::open(&path, DEFAULT_FOLD_THRESHOLD, true).expect("open");
        let address = commit_one(&mut store, b"secret");

        let mut tx = store.begin();
        tx.redact(&address).expect("redact");
        tx.commit().expect("commit");

        assert!(matches!(store.read(&address), Err(CasError::Redacted(_))));
        assert!(!store.contains(&address));
        // Entry count includes redacted entries.
        assert_eq!(store.len(), 1);

        // Insert of a redacted address fails at insert time.
        let mut tx = store.begin();
        assert!(matches!(tx.insert(b"secret"), Err(CasError::Redacted(_))));
        drop(tx);

        // Redacting again is a no-op commit (nothing staged, nothing written).
        let len_before = store.file_len();
        let mut tx = store.begin();
        tx.redact(&address).expect("redact");
        tx.commit().expect("commit");
        assert_eq!(store.file_len(), len_before);

        // Redaction survives reopen.
        drop(store);
        let mut store = CasFile::open(&path, DEFAULT_FOLD_THRESHOLD, true).expect("reopen");
        assert!(matches!(store.read(&address), Err(CasError::Redacted(_))));

        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn it_detects_corrupt_blob_bytes() {
        let path = temp_path("corrupt");
        let large: Vec<u8> = (0..200_000u32).map(|i| (i % 251) as u8).collect();
        let (small_address, large_address, small_entry, large_entry) = {
            let mut store = CasFile::open(&path, DEFAULT_FOLD_THRESHOLD, true).expect("open");
            let small_address = commit_one(&mut store, b"small blob");
            let large_address = commit_one(&mut store, &large);
            // A trailing intact commit: recovery verifies the TAIL
            // commit's payload, so corrupting the tail would read as a
            // torn commit and be truncated. Media corruption of older,
            // fully-durable commits is what read-time verification is for.
            commit_one(&mut store, b"intact tail commit");
            let small_entry = store.get(&small_address).expect("get");
            let large_entry = store.get(&large_address).expect("get");
            (small_address, large_address, small_entry, large_entry)
        };

        // Flip one blob byte in each record, bypassing the API. (This
        // simulates media corruption, not a crash: commits were durable.)
        let mut bytes = std::fs::read(&path).expect("read file");
        bytes[(small_entry.offset + 16) as usize] ^= 1;
        bytes[(large_entry.offset + 16 + large_entry.outboard_len + 12345) as usize] ^= 1;
        std::fs::write(&path, &bytes).expect("write file");

        let mut store = CasFile::open(&path, DEFAULT_FOLD_THRESHOLD, true).expect("reopen");
        assert!(matches!(
            store.read(&small_address),
            Err(CasError::Corrupt(_))
        ));
        assert!(matches!(
            store.read(&large_address),
            Err(CasError::Corrupt(_))
        ));

        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn it_recovers_from_a_truncated_tail() {
        let path = temp_path("truncated");
        let a;
        let b;
        let full_len;
        {
            let mut store = CasFile::open(&path, DEFAULT_FOLD_THRESHOLD, true).expect("open");
            a = commit_one(&mut store, b"first commit");
            let before_second = store.file_len();
            b = commit_one(&mut store, b"second commit");
            full_len = store.file_len();
            assert!(before_second < full_len);
        }

        // Truncate into the middle of the second commit: a crash before
        // its fsync completed.
        let bytes = std::fs::read(&path).expect("read file");
        std::fs::write(&path, &bytes[..(full_len as usize - 17)]).expect("truncate");

        let mut store = CasFile::open(&path, DEFAULT_FOLD_THRESHOLD, true).expect("recover");
        assert_eq!(store.read(&a).expect("first survives"), b"first commit");
        assert!(matches!(store.read(&b), Err(CasError::NotFound(_))));
        assert_eq!(store.len(), 1);

        // The torn tail was truncated away; committing works again.
        let c = commit_one(&mut store, b"third commit");
        assert_eq!(store.read(&c).expect("read"), b"third commit");

        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn it_rejects_a_footer_whose_payload_was_torn() {
        let path = temp_path("torn-payload");
        let a;
        let record_offset;
        {
            let mut store = CasFile::open(&path, DEFAULT_FOLD_THRESHOLD, true).expect("open");
            a = commit_one(&mut store, b"durable commit");
            record_offset = store.file_len();
            commit_one(&mut store, b"torn commit");
        }

        // Simulate out-of-order page persistence with a single fsync in
        // flight: the second commit's footer bytes reached the disk but a
        // payload byte did not. The footer's own 64-byte prefix is intact,
        // so a footer-only checksum would accept this state.
        let mut bytes = std::fs::read(&path).expect("read file");
        bytes[record_offset as usize + 16] ^= 1;
        std::fs::write(&path, &bytes).expect("write file");

        let mut store = CasFile::open(&path, DEFAULT_FOLD_THRESHOLD, true).expect("recover");
        assert_eq!(store.len(), 1, "torn commit must be discarded");
        assert_eq!(store.read(&a).expect("read"), b"durable commit");

        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn it_recovers_an_empty_store_from_a_torn_first_commit() {
        let path = temp_path("torn-first");
        {
            let mut store = CasFile::open(&path, DEFAULT_FOLD_THRESHOLD, true).expect("open");
            commit_one(&mut store, b"only commit");
        }
        let bytes = std::fs::read(&path).expect("read file");
        std::fs::write(&path, &bytes[..bytes.len() - 5]).expect("truncate");

        let store = CasFile::open(&path, DEFAULT_FOLD_THRESHOLD, true).expect("recover");
        assert!(store.is_empty());
        assert_eq!(store.file_len(), 8);

        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn it_answers_lookups_across_an_unfolded_delta_chain() {
        let path = temp_path("chain");
        let fold_threshold = 4;
        let mut addresses = Vec::new();
        {
            let mut store = CasFile::open(&path, fold_threshold, true).expect("open");
            // 11 single-entry commits with threshold 4: folds at commits
            // 5 and 10, leaving one unfolded delta after commit 11.
            for i in 0..11u32 {
                addresses.push(commit_one(&mut store, format!("payload {i}").as_bytes()));
                assert!(store.chain_len() <= fold_threshold);
            }
            assert_eq!(store.chain_len(), 1, "chain must be mid-cycle, not folded");
            // Every entry resolves regardless of whether it lives in the
            // base index or a chained delta.
            for (i, address) in addresses.iter().enumerate() {
                assert_eq!(
                    store.read(address).expect("read"),
                    format!("payload {i}").as_bytes()
                );
            }
        }

        // Reopen: state is rebuilt by walking the on-disk footer chain.
        let mut store = CasFile::open(&path, fold_threshold, true).expect("reopen");
        assert_eq!(store.chain_len(), 1);
        assert_eq!(store.len(), 11);
        for (i, address) in addresses.iter().enumerate() {
            assert_eq!(
                store.read(address).expect("read"),
                format!("payload {i}").as_bytes()
            );
        }
        assert!(!store.contains(&[0u8; 32]));

        std::fs::remove_file(&path).ok();
    }

    /// Relaxed mode (`durable = false`) writes the identical byte format
    /// and recovers through the same footer scan; only the fdatasync is
    /// skipped, so a crash may lose recent commits. On a live OS the page
    /// cache makes unsynced commits visible to a reopen.
    #[test]
    fn it_round_trips_in_relaxed_fsync_mode() {
        let path = temp_path("relaxed");
        let mut addresses = Vec::new();
        {
            let mut store = CasFile::open(&path, DEFAULT_FOLD_THRESHOLD, false).expect("open");
            for i in 0..5u32 {
                addresses.push(commit_one(&mut store, format!("relaxed {i}").as_bytes()));
            }
        }
        // Reopen durable: the modes share the format, so recovery and
        // reads behave identically.
        let mut store = CasFile::open(&path, DEFAULT_FOLD_THRESHOLD, true).expect("reopen");
        assert_eq!(store.len(), 5);
        for (i, address) in addresses.iter().enumerate() {
            assert_eq!(
                store.read(address).expect("read"),
                format!("relaxed {i}").as_bytes()
            );
        }

        // A truncated (lost) tail still recovers to the longest valid
        // prefix, exactly as in durable mode.
        drop(store);
        let bytes = std::fs::read(&path).expect("read file");
        std::fs::write(&path, &bytes[..bytes.len() - 9]).expect("truncate");
        let mut store = CasFile::open(&path, DEFAULT_FOLD_THRESHOLD, false).expect("recover");
        assert_eq!(store.len(), 4, "the torn tail commit is discarded");
        assert_eq!(store.read(&addresses[3]).expect("read"), b"relaxed 3");

        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn it_folds_every_commit_at_threshold_zero() {
        let path = temp_path("fold-zero");
        let mut store = CasFile::open(&path, 0, true).expect("open");
        for i in 0..5u32 {
            commit_one(&mut store, format!("payload {i}").as_bytes());
            assert_eq!(store.chain_len(), 0, "threshold 0 folds every commit");
        }
        drop(store);

        let mut store = CasFile::open(&path, 0, true).expect("reopen");
        assert_eq!(store.len(), 5);
        assert_eq!(store.chain_len(), 0);
        let address = *blake3::hash(b"payload 3").as_bytes();
        assert_eq!(store.read(&address).expect("read"), b"payload 3");

        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn it_supersedes_within_the_chain_after_redaction() {
        // A redaction recorded in a NEWER delta must win over the live
        // entry in an older delta or the base.
        let path = temp_path("chain-redact");
        let mut store = CasFile::open(&path, 8, true).expect("open");
        let address = commit_one(&mut store, b"live then redacted");
        commit_one(&mut store, b"unrelated");
        let mut tx = store.begin();
        tx.redact(&address).expect("redact");
        tx.commit().expect("commit");
        assert!(matches!(store.read(&address), Err(CasError::Redacted(_))));

        drop(store);
        let mut store = CasFile::open(&path, 8, true).expect("reopen");
        assert!(matches!(store.read(&address), Err(CasError::Redacted(_))));

        std::fs::remove_file(&path).ok();
    }
}

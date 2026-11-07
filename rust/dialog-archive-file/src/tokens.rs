//! The file's tokens: every distinct entity, attribute, value, datum,
//! separator and manifest a frame refers to, once, by a dense id.
//!
//! A token's bytes are read where they sit in the file, through a mapping
//! of it: what the index keeps in memory is where each token is, not what
//! it holds. The operating system keeps the pages a read touches and lets
//! them go under memory pressure, as it does for any file.
//!
//! Every token starts at a file offset aligned to [`ALIGN`], so a datum (a
//! standalone archive of a `State<Datum>`) is read as its archived type
//! where it sits, with no copy and no deserialization.
//!
//! The tokens the file holds are published in the index
//! ([`Index`](crate::archive::Index)); the tokens an append adds are its
//! [`Draft`] until they are written.

use std::io;

use hashbrown::HashTable;

use crate::archive::{Index, View};
use crate::rows::{Reader, invalid, put_bijou};

/// The alignment of every token's bytes in the file.
pub(crate) const ALIGN: u64 = 16;

/// Tokens by id.
pub(crate) trait TokenRead {
    /// The bytes of token `id`.
    fn get(&self, id: u32) -> Option<&[u8]>;
}

/// The tokens an append adds, on top of those the file holds: a new token
/// takes the next id after every token published when the draft began.
pub(crate) struct Draft<'a> {
    index: &'a Index,
    view: View<'a>,
    /// The id of the first new token.
    first: u32,
    /// The new tokens' bytes, back to back, where each ends, and the hash
    /// of each.
    bytes: Vec<u8>,
    ends: Vec<u32>,
    hashes: Vec<u64>,
    /// Each new token's id, found by its bytes' hash.
    ids: HashTable<u32>,
}

impl<'a> Draft<'a> {
    /// A draft on top of every token `index` holds now. The writer brings
    /// the index's id table up to date first ([`Index::build_ids`]), or a
    /// token the file holds is drafted again.
    pub fn new(index: &'a Index) -> Self {
        let view = index.view();
        let first = view.len();
        Self {
            index,
            view,
            first,
            bytes: Vec::new(),
            ends: Vec::new(),
            hashes: Vec::new(),
            ids: HashTable::new(),
        }
    }

    /// The id of `token`, adding it when it is new.
    pub fn intern(&mut self, token: &[u8]) -> u32 {
        let hash = self.index.hash_of(token);
        if let Some(id) = self.index.find(&self.view, hash, token)
            && id < self.first
        {
            return id;
        }
        if let Some(id) = self.ids.find(hash, |id| self.get(*id) == Some(token)) {
            return *id;
        }
        let id = self.first + self.ends.len() as u32;
        self.bytes.extend_from_slice(token);
        self.ends.push(self.bytes.len() as u32);
        self.hashes.push(hash);
        let hashes = &self.hashes;
        let first = self.first;
        self.ids
            .insert_unique(hash, id, |id| hashes[(*id - first) as usize]);
        id
    }

    /// The id of the first new token.
    pub fn first(&self) -> u32 {
        self.first
    }

    /// How many new tokens there are.
    pub fn pending(&self) -> usize {
        self.ends.len()
    }

    /// The hash of new token `id`'s bytes.
    pub fn hash(&self, id: u32) -> Option<u64> {
        self.hashes
            .get(id.checked_sub(self.first)? as usize)
            .copied()
    }

    /// Drops the new tokens past the first `keep`: those interned for a
    /// node that is kept whole after all.
    pub fn roll_back(&mut self, keep: usize) {
        while self.ends.len() > keep {
            let id = self.first + self.ends.len() as u32 - 1;
            let hash = self.hashes.pop().expect("a hash per token");
            if let Ok(entry) = self.ids.find_entry(hash, |candidate| *candidate == id) {
                entry.remove();
            }
            self.ends.pop();
            let end = self.ends.last().copied().unwrap_or(0) as usize;
            self.bytes.truncate(end);
        }
    }

    /// The body of a token record holding the new tokens `ids` names, in
    /// that order, for a record whose body starts at `body_offset` in the
    /// file, and where each token starts in it. Each token is its length,
    /// then padding to [`ALIGN`] (a byte saying how much, then that many
    /// zeros), then its bytes.
    pub fn record(&self, body_offset: u64, ids: &[u32]) -> (Vec<u8>, Vec<(usize, u32)>) {
        let mut body = Vec::with_capacity(self.bytes.len() + 20 * ids.len() + 8);
        let mut places = Vec::with_capacity(ids.len());
        put_bijou(&mut body, ids.len() as u64);
        for id in ids {
            let token = self.get(*id).expect("a new token");
            put_bijou(&mut body, token.len() as u64);
            let after = body_offset + body.len() as u64 + 1;
            let pad = (ALIGN - after % ALIGN) % ALIGN;
            body.push(pad as u8);
            body.resize(body.len() + pad as usize, 0);
            places.push((body.len(), token.len() as u32));
            body.extend_from_slice(token);
        }
        (body, places)
    }
}

impl TokenRead for Draft<'_> {
    fn get(&self, id: u32) -> Option<&[u8]> {
        if id < self.first {
            return self.view.get(id);
        }
        let at = (id - self.first) as usize;
        let end = *self.ends.get(at)? as usize;
        let start = if at == 0 {
            0
        } else {
            self.ends[at - 1] as usize
        };
        self.bytes.get(start..end)
    }
}

/// Where each token a token record whose body starts at `body_offset`
/// holds is, in order.
pub(crate) fn read_token_record(body: &[u8], body_offset: u64) -> io::Result<Vec<(u64, u32)>> {
    let mut reader = Reader::new(body);
    let count = reader.bijou()?;
    let mut tokens = Vec::with_capacity(count.min(1 << 20) as usize);
    for _ in 0..count {
        let len = reader.bijou()? as usize;
        let pad = reader.byte()? as usize;
        reader.take(pad)?;
        let at = reader.position();
        reader.take(len)?;
        tokens.push((
            body_offset + at as u64,
            u32::try_from(len).map_err(|_| invalid("a token over 4 GiB"))?,
        ));
    }
    if !reader.is_empty() {
        return Err(invalid("a token record runs past its tokens"));
    }
    Ok(tokens)
}

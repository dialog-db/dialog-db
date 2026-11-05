//! The file's tokens: every distinct entity, attribute, value, datum,
//! separator and manifest a frame refers to, once, by a dense id.
//!
//! A token's bytes are read where they sit in the file, through a mapping
//! of it: what the index keeps in memory is where each token is, not what
//! it holds. The operating system keeps the pages a read touches and lets
//! them go under memory pressure, as it does for any file.

use std::fs::File;
use std::hash::BuildHasher as _;
use std::io;
use std::sync::Arc;

use hashbrown::{DefaultHashBuilder, HashTable};
use memmap2::Mmap;

use crate::rows::{Reader, invalid, put_varint};

/// Every token the file holds, by id, and the tokens an append has added
/// but not written yet.
#[derive(Default)]
pub(crate) struct Tokens {
    /// The file, mapped at least as far as the last token in it.
    map: Option<Arc<Mmap>>,
    /// Where each token in the file starts, and its length, by id.
    extents: Vec<(u64, u32)>,
    /// The bytes of tokens added since the last write, back to back. Their
    /// ids follow the file's.
    pending: Vec<u8>,
    /// Where each of those ends in `pending`.
    pending_ends: Vec<u32>,
    /// Each token's id, found by its bytes' hash and compared against the
    /// token, so the table holds ids, not bytes. Built on the first
    /// [`intern`](Self::intern): a reader never needs it, and never reads a
    /// token it does not use.
    ids: Option<HashTable<u32>>,
    /// How many of the file's tokens the id table holds. Tokens another
    /// process wrote join it on the next intern.
    indexed: usize,
    hasher: DefaultHashBuilder,
}

impl Tokens {
    /// The token `id` names.
    pub fn get(&self, id: u32) -> Option<&[u8]> {
        let id = id as usize;
        match self.extents.get(id) {
            Some(&(offset, len)) => {
                let start = usize::try_from(offset).ok()?;
                self.map.as_ref()?.get(start..start + len as usize)
            }
            None => {
                let at = id - self.extents.len();
                let end = *self.pending_ends.get(at)? as usize;
                let start = if at == 0 {
                    0
                } else {
                    self.pending_ends[at - 1] as usize
                };
                self.pending.get(start..end)
            }
        }
    }

    /// The id of `token`, adding it when it is new. A new token is pending
    /// until [`take_pending`](Self::take_pending) writes it.
    pub fn intern(&mut self, token: &[u8]) -> u32 {
        let hash = self.hasher.hash_one(token);
        self.build_ids();
        if let Some(id) = self.find(hash, token) {
            return id;
        }
        let id = self.len() as u32;
        self.pending.extend_from_slice(token);
        self.pending_ends.push(self.pending.len() as u32);
        self.index(id, hash);
        id
    }

    /// The id of a token equal to `token`, whose bytes hash to `hash`.
    fn find(&self, hash: u64, token: &[u8]) -> Option<u32> {
        self.ids
            .as_ref()?
            .find(hash, |id| self.get(*id) == Some(token))
            .copied()
    }

    /// Makes token `id`, whose bytes hash to `hash`, findable by its bytes.
    fn index(&mut self, id: u32, hash: u64) {
        let mut ids = self.ids.take().expect("the id table is built");
        ids.insert_unique(hash, id, |id| {
            self.hasher
                .hash_one(self.get(*id).expect("an indexed token"))
        });
        self.ids = Some(ids);
    }

    /// Brings the id table up to every token in the file, building it on
    /// first use. A token written twice keeps its first id.
    fn build_ids(&mut self) {
        if self.ids.is_none() {
            self.ids = Some(HashTable::with_capacity(self.extents.len()));
            self.indexed = 0;
        }
        for id in self.indexed..self.extents.len() {
            let token = self.get(id as u32).expect("a mapped token");
            let hash = self.hasher.hash_one(token);
            if self.find(hash, token).is_none() {
                self.index(id as u32, hash);
            }
        }
        self.indexed = self.extents.len();
    }

    /// How many tokens there are, pending ones included.
    pub fn len(&self) -> usize {
        self.extents.len() + self.pending_ends.len()
    }

    /// How many tokens are pending.
    pub fn pending(&self) -> usize {
        self.pending_ends.len()
    }

    /// The memory the tokens take: where each is, the pending ones, and
    /// the id table. Mapped pages are the file's, not counted here.
    pub fn footprint(&self) -> usize {
        self.extents.capacity() * size_of::<(u64, u32)>()
            + self.pending.capacity()
            + self.pending_ends.capacity() * 4
            + self.ids.as_ref().map_or(0, |ids| ids.capacity() * (4 + 1))
    }

    /// The body of a token record holding the pending tokens, and where
    /// each token starts in it.
    pub fn take_pending(&self) -> (Vec<u8>, Vec<(usize, u32)>) {
        let mut body = Vec::with_capacity(self.pending.len() + 4 * self.pending() + 8);
        let mut places = Vec::with_capacity(self.pending());
        put_varint(&mut body, self.pending() as u64);
        for id in self.extents.len()..self.len() {
            let token = self.get(id as u32).expect("a pending token");
            put_varint(&mut body, token.len() as u64);
            places.push((body.len(), token.len() as u32));
            body.extend_from_slice(token);
        }
        (body, places)
    }

    /// Records that the pending tokens are in the file, in a record whose
    /// body starts at `body_offset`, at `places` in it.
    pub fn settle(&mut self, body_offset: u64, places: &[(usize, u32)]) {
        self.extents.extend(
            places
                .iter()
                .map(|(at, len)| (body_offset + *at as u64, *len)),
        );
        self.pending.clear();
        self.pending_ends.clear();
        // Pending tokens joined the id table as they were interned.
        if self.ids.is_some() {
            self.indexed = self.extents.len();
        }
    }

    /// Adds a token at `offset` in the file, `len` bytes long.
    pub fn push(&mut self, offset: u64, len: u32) {
        self.extents.push((offset, len));
    }

    /// Adds the tokens a token record whose body starts at `body_offset`
    /// holds, in order.
    pub fn read_record(&mut self, body: &[u8], body_offset: u64) -> io::Result<()> {
        let mut reader = Reader::new(body);
        let count = reader.varint()?;
        for _ in 0..count {
            let len = reader.varint()? as usize;
            let at = reader.position();
            reader.take(len)?;
            self.push(
                body_offset + at as u64,
                u32::try_from(len).map_err(|_| invalid("a token over 4 GiB"))?,
            );
        }
        if !reader.is_empty() {
            return Err(invalid("a token record runs past its tokens"));
        }
        Ok(())
    }

    /// Where each token in the file is, by id.
    pub fn extents(&self) -> &[(u64, u32)] {
        &self.extents
    }

    /// Maps `file` again if the mapping does not reach every token in it.
    pub fn map(&mut self, file: &File) -> io::Result<()> {
        let needed = self
            .extents
            .iter()
            .map(|(offset, len)| offset + *len as u64)
            .max()
            .unwrap_or(0);
        let mapped = self.map.as_ref().map_or(0, |map| map.len() as u64);
        if needed > mapped {
            // SAFETY: the file is append-only. A process changes bytes only
            // past the last footer, by an append under the file's lock, or by
            // dropping an append that did not finish; no token any process
            // has indexed lies there. Every token this maps was indexed
            // from a record before a footer, or written by this process.
            // Like any mapped file, changing it from outside dialog is not
            // supported.
            self.map = Some(Arc::new(unsafe { Mmap::map(file)? }));
        }
        Ok(())
    }
}

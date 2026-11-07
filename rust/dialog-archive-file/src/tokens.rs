//! The file's tokens: every distinct entity, attribute, value, datum,
//! separator and manifest a frame refers to, once, named by where it is.
//!
//! A token's bytes start at a file offset aligned to [`ALIGN`], with their
//! length in the four bytes before, so a token is read from its offset
//! alone, through a mapping of the file: rows name a token by its offset
//! divided by [`ALIGN`], and nothing in memory says where tokens are. A
//! datum (a standalone archive of a `State<Datum>`) is read as its archived
//! type where it sits, with no copy and no deserialization.
//!
//! The tokens an append adds are its [`Draft`] until its turn, when they
//! take their places in the file.

use std::sync::Arc;

use hashbrown::HashTable;

use crate::rows::put_varint;

/// The alignment of every token's bytes in the file.
pub(crate) const ALIGN: u64 = 16;

/// Tokens by the value rows name them by.
pub(crate) trait TokenRead {
    /// The bytes of the token `id` names.
    fn get(&self, id: u32) -> Option<&[u8]>;
}

/// The token whose bytes start at `offset` (a multiple of [`ALIGN`]) in
/// `file`, the file's bytes up to the end of what was written.
pub(crate) fn token_at(file: &[u8], offset: u64) -> Option<&[u8]> {
    let start = usize::try_from(offset).ok()?;
    let len = u32::from_le_bytes(file.get(start.checked_sub(4)?..start)?.try_into().ok()?);
    file.get(start..start + len as usize)
}

/// Finds a token the file holds: the value rows name it by, given its
/// bytes and their [`hash`](crate::hash::hash).
pub(crate) type Find<'a> = Box<dyn Fn(u64, &[u8]) -> Option<u32> + 'a>;

/// The tokens a file holds, read by the value rows name them by.
pub(crate) type Held = Arc<dyn TokenRead + Send + Sync>;

/// The tokens an append adds, on top of those the file holds. A token the
/// file holds is named as the file names it; a new one by a provisional id
/// from [`first`](Self::first) up, above every value a token of the file
/// has, until the append's turn gives it its place.
pub(crate) struct Draft<'a> {
    /// Tokens the file holds, by the value rows name them by.
    held: Held,
    /// Finds a token the file holds.
    find: Option<Find<'a>>,
    first: u32,
    /// The new tokens' bytes, back to back, where each ends, and the hash
    /// of each.
    bytes: Vec<u8>,
    ends: Vec<u32>,
    hashes: Vec<u64>,
    /// Each new token's provisional id, found by its bytes' hash.
    ids: HashTable<u32>,
}

impl<'a> Draft<'a> {
    /// A draft over the tokens `held` reads, finding them with `find`, its
    /// new tokens named from `first` up.
    pub fn new(held: Held, find: Find<'a>, first: u32) -> Self {
        Self {
            held,
            find: Some(find),
            first,
            bytes: Vec::new(),
            ends: Vec::new(),
            hashes: Vec::new(),
            ids: HashTable::new(),
        }
    }

    /// A draft over no tokens at all.
    #[cfg(test)]
    pub fn detached() -> Draft<'static> {
        struct Nothing;
        impl TokenRead for Nothing {
            fn get(&self, _: u32) -> Option<&[u8]> {
                None
            }
        }
        Draft {
            held: Arc::new(Nothing),
            find: None,
            first: 0,
            bytes: Vec::new(),
            ends: Vec::new(),
            hashes: Vec::new(),
            ids: HashTable::new(),
        }
    }

    /// The value rows name `token` by, adding it when it is new.
    pub fn intern(&mut self, token: &[u8]) -> u32 {
        let hash = crate::hash::hash(token);
        if let Some(find) = &self.find
            && let Some(id) = find(hash, token)
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
        let (hashes, first) = (&self.hashes, self.first);
        self.ids
            .insert_unique(hash, id, |id| hashes[(*id - first) as usize]);
        id
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
    /// file, and where each token's bytes start in the file. Each token is
    /// a byte saying how much padding follows, the padding, its length (a
    /// u32, little-endian), then its bytes, starting on an [`ALIGN`]
    /// boundary.
    pub fn record(&self, body_offset: u64, ids: &[u32]) -> (Vec<u8>, Vec<u64>) {
        let mut body = Vec::with_capacity(self.bytes.len() + 24 * ids.len() + 8);
        let mut places = Vec::with_capacity(ids.len());
        put_varint(&mut body, ids.len() as u64);
        for id in ids {
            let token = self.get(*id).expect("a new token");
            let after = body_offset + body.len() as u64 + 1 + 4;
            let pad = (ALIGN - after % ALIGN) % ALIGN;
            body.push(pad as u8);
            body.resize(body.len() + pad as usize, 0);
            body.extend_from_slice(&(token.len() as u32).to_le_bytes());
            places.push(body_offset + body.len() as u64);
            body.extend_from_slice(token);
        }
        (body, places)
    }
}

impl TokenRead for Draft<'_> {
    fn get(&self, id: u32) -> Option<&[u8]> {
        if id < self.first {
            return self.held.get(id);
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
/// holds starts in the file, in order: for tools that read records.
#[cfg(test)]
pub(crate) fn read_token_record(body: &[u8], body_offset: u64) -> std::io::Result<Vec<u64>> {
    let mut reader = crate::rows::Reader::new(body);
    let count = reader.varint()?;
    let mut tokens = Vec::with_capacity(count.min(1 << 20) as usize);
    for _ in 0..count {
        let pad = reader.byte()? as usize;
        reader.take(pad)?;
        let len = u32::from_le_bytes(reader.take(4)?.try_into().expect("four bytes")) as usize;
        let at = reader.position();
        reader.take(len)?;
        tokens.push(body_offset + at as u64);
    }
    if !reader.is_empty() {
        return Err(crate::rows::invalid("a token record runs past its tokens"));
    }
    Ok(tokens)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io;

    #[test]
    fn it_reads_back_each_token_from_its_place_alone() -> io::Result<()> {
        let mut draft = Draft::detached();
        let tokens: Vec<Vec<u8>> = (0..40).map(|n| vec![n as u8; n * 3 % 37]).collect();
        let ids: Vec<u32> = tokens.iter().map(|token| draft.intern(token)).collect();
        // A record at an offset that is not aligned, as any is.
        let body_offset = 1_000_003;
        let (body, places) = draft.record(body_offset, &ids);
        let mut file = vec![0u8; body_offset as usize];
        file.extend_from_slice(&body);
        assert_eq!(read_token_record(&body, body_offset)?, places);
        for (token, place) in tokens.iter().zip(&places) {
            assert_eq!(place % ALIGN, 0, "a token starts on the alignment");
            assert_eq!(token_at(&file, *place), Some(token.as_slice()));
        }
        Ok(())
    }

    #[test]
    fn it_interns_a_token_once() {
        let mut draft = Draft::detached();
        let a = draft.intern(b"alpha");
        assert_eq!(draft.intern(b"beta"), a + 1);
        assert_eq!(draft.intern(b"alpha"), a);
        assert_eq!(draft.pending(), 2);
        draft.roll_back(1);
        assert_eq!(draft.pending(), 1);
        assert_eq!(draft.intern(b"beta"), a + 1);
    }
}

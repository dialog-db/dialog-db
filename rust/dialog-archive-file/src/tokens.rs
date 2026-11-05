//! The file's tokens: every distinct entity, attribute, value, datum,
//! separator and manifest a frame refers to, once, by a dense id.

use std::hash::BuildHasher as _;

use hashbrown::{DefaultHashBuilder, HashTable};
use std::io;

use crate::rows::{Reader, invalid, put_varint};

/// Every token the file holds, in id order, and (for writing) their ids.
#[derive(Default)]
pub(crate) struct Tokens {
    /// Every token's bytes, back to back.
    region: Vec<u8>,
    /// Where each token ends in `region`, by id.
    ends: Vec<u32>,
    /// Each token's id, found by its bytes' hash and compared against the
    /// region, so the map holds ids, not a second copy of every token.
    ids: HashTable<u32>,
    hasher: DefaultHashBuilder,
}

/// How many tokens and bytes [`Tokens`] held at a point.
#[derive(Clone, Copy)]
pub(crate) struct Mark {
    tokens: usize,
    bytes: usize,
}

impl Tokens {
    /// The token `id` names.
    pub fn get(&self, id: u32) -> Option<&[u8]> {
        token(&self.region, &self.ends, id)
    }

    /// The id of `token`, adding it when it is new.
    pub fn intern(&mut self, token: &[u8]) -> u32 {
        let hash = self.hasher.hash_one(token);
        let (region, ends) = (&self.region, &self.ends);
        if let Some(id) = self
            .ids
            .find(hash, |id| self::token(region, ends, *id) == Some(token))
        {
            return *id;
        }
        let id = self.ends.len() as u32;
        self.region.extend_from_slice(token);
        self.ends.push(self.region.len() as u32);
        self.index(id, hash);
        id
    }

    /// Makes token `id`, whose bytes hash to `hash`, findable by its bytes.
    fn index(&mut self, id: u32, hash: u64) {
        let (region, ends, hasher) = (&self.region, &self.ends, &self.hasher);
        self.ids.insert_unique(hash, id, |id| {
            hasher.hash_one(self::token(region, ends, *id).expect("an indexed token"))
        });
    }

    /// How many tokens there are.
    pub fn len(&self) -> usize {
        self.ends.len()
    }

    /// The bytes the tokens take in memory: the region, the ends and the
    /// id map.
    pub fn footprint(&self) -> usize {
        self.region.capacity() + self.ends.capacity() * 4 + self.ids.capacity() * (4 + 1)
    }

    /// How many tokens were added since `mark`.
    pub fn added_since(&self, mark: Mark) -> usize {
        self.ends.len() - mark.tokens
    }

    pub fn mark(&self) -> Mark {
        Mark {
            tokens: self.ends.len(),
            bytes: self.region.len(),
        }
    }

    /// The body of a token record holding every token added since `mark`.
    pub fn record_since(&self, mark: Mark) -> Vec<u8> {
        let mut body = Vec::with_capacity(self.region.len() - mark.bytes + 8);
        put_varint(&mut body, (self.ends.len() - mark.tokens) as u64);
        for id in mark.tokens..self.ends.len() {
            let token = self.get(id as u32).expect("a token past the mark");
            put_varint(&mut body, token.len() as u64);
            body.extend_from_slice(token);
        }
        body
    }

    /// Adds the tokens a token record holds, in order.
    pub fn read_record(&mut self, body: &[u8]) -> io::Result<()> {
        let mut reader = Reader::new(body);
        let count = reader.varint()?;
        for _ in 0..count {
            let len = reader.varint()? as usize;
            let token = reader.take(len)?;
            let id = self.ends.len() as u32;
            let hash = self.hasher.hash_one(token);
            // A token written twice keeps its first id.
            let (region, ends) = (&self.region, &self.ends);
            let known = self
                .ids
                .find(hash, |id| self::token(region, ends, *id) == Some(token))
                .is_some();
            self.region.extend_from_slice(token);
            self.ends
                .push(u32::try_from(self.region.len()).map_err(|_| invalid("tokens over 4 GiB"))?);
            if !known {
                self.index(id, hash);
            }
        }
        if !reader.is_empty() {
            return Err(invalid("a token record runs past its tokens"));
        }
        Ok(())
    }
}

/// The token `id` names in `region`, given where each token ends.
fn token<'a>(region: &'a [u8], ends: &[u32], id: u32) -> Option<&'a [u8]> {
    let id = id as usize;
    let end = *ends.get(id)? as usize;
    let start = if id == 0 { 0 } else { ends[id - 1] as usize };
    region.get(start..end)
}

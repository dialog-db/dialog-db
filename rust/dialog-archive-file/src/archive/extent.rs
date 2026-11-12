//! Extents: space in the file a writer appends to without the writer's
//! turn.
//!
//! A writer that finds the turn taken does not wait for it. It writes its
//! blocks whole into an extent of its own, a record of the [extent](Kind::Extent)
//! kind it claimed earlier in a turn, and its blocks are readable at once;
//! integration later turns them into frames and tokens, and frees the
//! extent once that is durable. Only claiming an extent takes the turn,
//! once per extent filled.
//!
//! An extent's body is a byte of padding and the padding, so what follows
//! starts on an eight-byte boundary; then how many bytes of frames it
//! holds, twice, the second inverted, so a reader that catches the count
//! being written tells it from a whole one; then its epoch, the sequence of
//! the slot that claimed it, so a reader tells an extent claimed again
//! from the one it read before; then its frames, each a block
//! kept whole as a [frame](Kind::Frame) record of the [whole](WHOLE) kind,
//! exactly as the file keeps whole blocks elsewhere, so a block in an
//! extent is read where it is, as any block kept whole.
//!
//! One process writes an extent: the one holding the byte its record's
//! offset names among the owners' (see the `locks` module). Its frames
//! only ever grow, and its count is written after them, so a reader that
//! reads the count reads whole frames below it.

use std::fs::File;
use std::io;

use dialog_common::{Blake3Hash, Buffer};

use super::index::{FrameHeader, WHOLE};
use super::put_frame;
use crate::file::{HEADER, Kind, put_record, record_in, write_at};
use crate::rows::{Reader, invalid, put_varint};
use crate::tokens::ALIGN;

/// The bytes of the count, its inverse and the epoch.
const COUNT: u64 = 24;

/// Where an extent's count is, from where its record starts: past the
/// record's header and the padding that puts it on an eight-byte boundary.
fn count_at(record: u64) -> u64 {
    let body = record + HEADER;
    body + 1 + (8 - (body + 1) % 8) % 8
}

/// The start of an extent's record, claiming `size` bytes of frames after
/// `record`, in `epoch`: its header, padding, count and epoch, and how many
/// bytes the whole record takes. Its frames are not written: nothing reads
/// past the count, so the file is grown to the record's end without them
/// (sparse, where the file system allows).
pub(crate) fn record(record: u64, size: u64, epoch: u64) -> io::Result<(Vec<u8>, u64)> {
    let count = count_at(record);
    let pad = count - (record + HEADER) - 1;
    let mut start = Vec::with_capacity((1 + pad + COUNT) as usize);
    start.push(pad as u8);
    start.resize(1 + pad as usize, 0);
    start.extend_from_slice(&0u64.to_le_bytes());
    start.extend_from_slice(&(!0u64).to_le_bytes());
    start.extend_from_slice(&epoch.to_le_bytes());
    let body = start.len() as u64 + size;
    let mut bytes = Vec::with_capacity(HEADER as usize + start.len());
    put_record(&mut bytes, Kind::Extent, &start)?;
    // The header names the whole body; an extent's is never checked
    // against its checksum (see `Kind::checked`).
    let length = u32::try_from(body).map_err(|_| invalid("an extent over 4 GiB"))?;
    bytes[1..5].copy_from_slice(&length.to_le_bytes());
    Ok((bytes, HEADER + body))
}

/// An extent this process writes: its record's offset, where its frames
/// start and end, how many bytes of them it holds, and its epoch.
#[derive(Debug)]
pub(crate) struct Owned {
    pub record: u64,
    start: u64,
    end: u64,
    pub used: u64,
    pub epoch: u64,
}

impl Owned {
    /// The extent whose record starts at `record` in `bytes` (the file,
    /// mapped), with what it holds so far.
    pub fn of(bytes: &[u8], record: u64) -> io::Result<Self> {
        let (kind, _, body_end) = record_in(bytes, record as usize)
            .ok_or_else(|| invalid("an extent that is not whole"))?;
        if kind != Kind::Extent {
            return Err(invalid("not an extent"));
        }
        let count = count_at(record);
        Ok(Self {
            record,
            start: count + COUNT,
            end: body_end as u64,
            used: used(bytes, record).unwrap_or(0),
            epoch: epoch(bytes, record).unwrap_or(0),
        })
    }

    /// Where the extent's record ends.
    pub fn end(&self) -> u64 {
        self.end
    }

    /// Whether `bytes` more bytes of frames fit.
    pub fn fits(&self, bytes: u64) -> bool {
        self.start + self.used + bytes <= self.end
    }

    /// The frames that keep `blocks` of the catalog numbered `catalog`
    /// whole, laid out to follow what the extent holds; with each block's
    /// place. `None` when they do not fit.
    pub fn lay_out(
        &self,
        catalog: u16,
        blocks: &[&Buffer],
    ) -> io::Result<Option<(Vec<u8>, Vec<u32>)>> {
        let start = self.start + self.used;
        let mut bytes = Vec::new();
        let mut places = Vec::with_capacity(blocks.len());
        for block in blocks {
            let mut data = Vec::with_capacity(block.as_ref().len() + 48);
            FrameHeader {
                hash: block.blake3_hash().clone(),
                kind: WHOLE,
                depth: 0,
                catalog,
                base: None,
                manifest: 0,
            }
            .put(&mut data);
            put_varint(&mut data, block.as_ref().len() as u64);
            data.extend_from_slice(block.as_ref());
            places.push(put_frame(&mut bytes, start, &data)?);
        }
        Ok(self.fits(bytes.len() as u64).then_some((bytes, places)))
    }

    /// Writes frames laid out by [`lay_out`](Self::lay_out), then the
    /// count that makes them readable.
    pub fn write(&mut self, file: &File, bytes: &[u8]) -> io::Result<()> {
        write_at(file, self.start + self.used, bytes)?;
        self.used += bytes.len() as u64;
        write_count(file, self.record, self.used)
    }

    /// Empties the extent, claimed again in `epoch`.
    pub fn clear(&mut self, file: &File, epoch: u64) -> io::Result<()> {
        self.used = 0;
        self.epoch = epoch;
        let mut count = [0u8; COUNT as usize];
        count[..8].copy_from_slice(&0u64.to_le_bytes());
        count[8..16].copy_from_slice(&(!0u64).to_le_bytes());
        count[16..].copy_from_slice(&epoch.to_le_bytes());
        write_at(file, count_at(self.record), &count)
    }
}

/// Writes that the extent at `record` holds `used` bytes of frames: for
/// recovery, when the frames reached the disk and the count did not.
pub(crate) fn set_used(file: &File, record: u64, used: u64) -> io::Result<()> {
    write_count(file, record, used)
}

fn write_count(file: &File, record: u64, used: u64) -> io::Result<()> {
    let mut count = [0u8; 16];
    count[..8].copy_from_slice(&used.to_le_bytes());
    count[8..].copy_from_slice(&(!used).to_le_bytes());
    write_at(file, count_at(record), &count)
}

/// How many bytes of frames the extent at `record` holds, as `bytes` (the
/// file, mapped) say; `None` while its count is being written.
pub(crate) fn used(bytes: &[u8], record: u64) -> Option<u64> {
    let at = count_at(record) as usize;
    let count = bytes.get(at..at + 16)?;
    let used = u64::from_le_bytes(count[..8].try_into().ok()?);
    let check = u64::from_le_bytes(count[8..].try_into().ok()?);
    (check == !used).then_some(used)
}

/// The epoch of the extent at `record`: the sequence of the slot that last
/// claimed it.
pub(crate) fn epoch(bytes: &[u8], record: u64) -> Option<u64> {
    let at = count_at(record) as usize + 16;
    Some(u64::from_le_bytes(bytes.get(at..at + 8)?.try_into().ok()?))
}

/// A block an extent holds: its catalog's number, its hash, and where its
/// frame's data starts, divided by [`ALIGN`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Held {
    pub catalog: u16,
    pub hash: Blake3Hash,
    pub at: u32,
}

/// The blocks the extent at `record` holds in its frames from byte `from`
/// of them to byte `to`, as `bytes` (the file, mapped) say, and the byte of
/// its frames the walk reached. A frame that is not whole ends the walk:
/// cut short by a crash, or read before its bytes all arrived, it is read
/// again from there next time.
pub(crate) fn held(bytes: &[u8], record: u64, from: u64, to: u64) -> io::Result<(Vec<Held>, u64)> {
    let start = count_at(record) + COUNT;
    let mut at = (start + from) as usize;
    let end = (start + to) as usize;
    let mut found = Vec::new();
    while at < end {
        let Some((Kind::Frame, body, next)) = record_in(bytes, at) else {
            break;
        };
        let pad = bytes[body] as usize;
        let data = body + 1 + pad;
        let header = FrameHeader::get(&mut Reader::new(&bytes[data..next]))?;
        found.push(Held {
            catalog: header.catalog,
            hash: header.hash,
            at: (data as u64 / ALIGN) as u32,
        });
        at = next;
    }
    Ok((found, at as u64 - start))
}

#[cfg(test)]
mod tests {
    use super::super::slot::DATA as DATA_FOR_TESTS;
    use super::*;
    use crate::file::read_at;

    #[dialog_common::test]
    fn it_reads_back_the_blocks_an_extent_holds() -> anyhow::Result<()> {
        let directory = tempfile::tempdir()?;
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(directory.path().join("extent"))?;
        let at = DATA_FOR_TESTS;
        let (bytes, whole) = record(at, 4096, 1)?;
        write_at(&file, at, &bytes)?;
        file.set_len(at + whole)?;
        let read = |len: u64| -> anyhow::Result<Vec<u8>> {
            let mut out = vec![0u8; len as usize];
            read_at(&file, 0, &mut out)?;
            Ok(out)
        };
        let len = at + whole;

        let mut owned = Owned::of(&read(len)?, at)?;
        assert_eq!(owned.used, 0);
        assert!(held(&read(len)?, at, 0, owned.used)?.0.is_empty());
        assert_eq!(epoch(&read(len)?, at), Some(1));

        let blocks: Vec<Buffer> = (0..3u8).map(|n| Buffer::from(vec![n; 100])).collect();
        let refs: Vec<&Buffer> = blocks.iter().collect();
        let (frames, places) = owned.lay_out(7, &refs)?.expect("they fit");
        owned.write(&file, &frames)?;
        let (found, reached) = held(&read(len)?, at, 0, owned.used)?;
        assert_eq!(found.len(), 3);
        assert_eq!(reached, owned.used);
        for ((found, block), place) in found.iter().zip(&blocks).zip(&places) {
            assert_eq!(&found.hash, block.blake3_hash());
            assert_eq!(found.catalog, 7);
            assert_eq!(found.at, *place);
        }
        assert_eq!(used(&read(len)?, at), Some(owned.used));

        // A frame not whole ends the walk where it starts, for the next
        // walk to start there.
        let last = places[2] as u64 * ALIGN;
        write_at(&file, last + 8, &[0xEE; 4])?;
        let (found, reached) = held(&read(len)?, at, 0, owned.used)?;
        assert_eq!(found.len(), 2);
        assert!(reached < owned.used);
        let (found, _) = held(&read(len)?, at, reached, owned.used)?;
        assert!(found.is_empty(), "read again from where it stopped");

        // A count caught half written is not read as one.
        write_at(&file, count_at(at), &5u64.to_le_bytes())?;
        assert_eq!(used(&read(len)?, at), None);

        // Claimed again: empty, in its new epoch.
        owned.clear(&file, 2)?;
        assert_eq!(used(&read(len)?, at), Some(0));
        assert_eq!(epoch(&read(len)?, at), Some(2));

        // Too much for the extent does not lay out.
        let big = Buffer::from(vec![9u8; 8192]);
        assert!(owned.lay_out(7, &[&big])?.is_none());
        Ok(())
    }
}

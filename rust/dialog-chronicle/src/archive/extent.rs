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

use std::collections::HashSet;
use std::fs::File;
use std::io;

use dialog_common::{Blake3Hash, Buffer};

use super::index::{FrameHeader, WHOLE};
use super::put_frame;
use crate::file::{HEADER, Kind, put_record, record_in, write_at};
use crate::locks;
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

/// The extents this process writes: those idle, free for a writer to take,
/// and the records of those a thread has taken.
///
/// An extent's owner byte belongs to the whole process (see the `locks`
/// module): a thread's lock on a byte another thread of this process
/// holds succeeds, and either thread's unlock lets go of it for both. So
/// this process holds an extent's byte only while exactly one of its
/// threads has the extent, and only that thread lets go of the byte. The
/// fields are private to this module so every path keeps to that: a
/// thread takes an extent only through [`own`](Self::own), which refuses
/// one any thread has already, or [`take_idle`](Self::take_idle); and it
/// lets go only of what it holds, a [`Claim`] or an [`Owned`], which
/// cannot be copied.
#[derive(Default)]
pub(crate) struct Extents {
    idle: Vec<Owned>,
    busy: HashSet<u64>,
}

/// An extent a thread of this process took with [`Extents::own`]: its
/// owner byte held, and marked busy. Not copied, so only the thread
/// holding it lets go of it ([`Extents::let_go`]) or makes it the
/// [`Owned`] it writes ([`Extents::owned`]).
#[must_use]
#[derive(Debug)]
pub(crate) struct Claim {
    record: u64,
}

impl Claim {
    /// The extent's record.
    pub fn record(&self) -> u64 {
        self.record
    }
}

impl Extents {
    /// Whether a thread of this process has the extent at `record`, idle
    /// or busy.
    pub fn has(&self, record: u64) -> bool {
        self.busy.contains(&record) || self.idle.iter().any(|owned| owned.record == record)
    }

    /// Whether this process has no extent at all.
    pub fn is_empty(&self) -> bool {
        self.idle.is_empty() && self.busy.is_empty()
    }

    /// The records of every extent this process has, idle or busy.
    pub fn records(&self) -> impl Iterator<Item = u64> + '_ {
        self.idle
            .iter()
            .map(|owned| owned.record)
            .chain(self.busy.iter().copied())
    }

    /// Takes the extent at `record` of `file` for the caller: its owner
    /// byte tried, never waited for, and the extent marked busy. `None`
    /// when a thread of this process has it already, whose lock would not
    /// keep this one out and whose unlock this one's would undo, or when
    /// another process holds its byte.
    pub fn own(&mut self, file: &File, record: u64) -> io::Result<Option<Claim>> {
        if self.has(record) || !locks::lock(file, locks::owner(record), true, false)? {
            return Ok(None);
        }
        self.busy.insert(record);
        Ok(Some(Claim { record }))
    }

    /// Lets go of what `claim` took: the owner byte, then the mark, so no
    /// thread takes the extent while this one still holds its byte.
    pub fn let_go(&mut self, file: &File, claim: Claim) -> io::Result<()> {
        let unlocked = locks::unlock(file, locks::owner(claim.record));
        self.busy.remove(&claim.record);
        unlocked
    }

    /// The extent `claim` took, as `bytes` (the file, mapped) hold it, to
    /// write; on an error, let go of.
    pub fn owned(&mut self, file: &File, bytes: &[u8], claim: Claim) -> io::Result<Owned> {
        match Owned::of(bytes, claim.record) {
            Ok(owned) => Ok(owned),
            Err(error) => {
                let _ = self.let_go(file, claim);
                Err(error)
            }
        }
    }

    /// An idle extent, taken for a writer: marked busy.
    pub fn take_idle(&mut self) -> Option<Owned> {
        let owned = self.idle.pop()?;
        self.busy.insert(owned.record);
        Some(owned)
    }

    /// The idle extent at `record`, taken if `pick` chooses it: marked
    /// busy.
    pub fn take_idle_at(
        &mut self,
        record: u64,
        pick: impl FnOnce(&Owned) -> bool,
    ) -> Option<Owned> {
        let at = self.idle.iter().position(|owned| owned.record == record)?;
        if !pick(&self.idle[at]) {
            return None;
        }
        let owned = self.idle.swap_remove(at);
        self.busy.insert(owned.record);
        Some(owned)
    }

    /// Gives back an extent the caller took, idle for the next writer.
    pub fn give_back(&mut self, owned: Owned) {
        self.busy.remove(&owned.record);
        self.idle.push(owned);
    }

    /// Lets go of an extent the caller took, to write no more: its owner
    /// byte, then its mark.
    pub fn release(&mut self, file: &File, owned: Owned) -> io::Result<()> {
        self.let_go(
            file,
            Claim {
                record: owned.record,
            },
        )
    }

    /// The records of the idle extents, for tests.
    #[cfg(test)]
    pub fn idle_records(&self) -> Vec<u64> {
        self.idle.iter().map(|owned| owned.record).collect()
    }

    /// Whether the extent at `record` is marked busy, for tests.
    #[cfg(test)]
    pub fn is_busy(&self, record: u64) -> bool {
        self.busy.contains(&record)
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

/// The checksum of the first `used` bytes of frames of the extent at
/// `record`, as `bytes` (the file, mapped) hold them: what a cell's record
/// names the extent's frames by, so recovery tells them from frames an
/// earlier claim of the extent left in the same place (see the `cells`
/// module). `None` past the file's end.
pub(crate) fn checksum(bytes: &[u8], record: u64, used: u64) -> Option<u32> {
    let start = (count_at(record) + COUNT) as usize;
    Some(crc32fast::hash(bytes.get(start..start + used as usize)?))
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

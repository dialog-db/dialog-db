//! The file: a magic number, three slots (see the archive's `slot`
//! module), then a sequence of records, appended.
//!
//! Every record is its kind (1 byte), its body's length (4 bytes, little
//! endian), the crc32 of its kind and body (4 bytes, little endian), then
//! its body, so a reader can walk the file record by record and tell a whole record
//! from a torn one: a record whose length runs past the end of the file, or
//! whose checksum does not match, ends the file there.
//!
//! A [pages](Kind::Pages) record is an exception: its pages are the
//! index's, written over once no root reaches them, so its checksum holds
//! only until the first is. Each page carries a checksum of its own. An
//! [extent](Kind::Extent) record is the other: its body is space a writer
//! fills later, each frame in it a record with a checksum of its own; and a
//! [cell](Kind::Cell) record's halves are written over, each with a
//! checksum of its own.

use std::fs::File;
use std::io;

/// The first bytes of every archive file. The last byte is the format
/// version.
pub(crate) const MAGIC: &[u8; 8] = b"DIALOGA\x0c";

/// The bytes of a record before its body.
pub(crate) const HEADER: u64 = 1 + 4 + 4;

/// What a record holds.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Kind {
    /// Tokens, each its length and its bytes, which start on an alignment
    /// boundary: a token is named by where it starts.
    Tokens,
    /// A tree node as rows naming tokens and children by where they are,
    /// possibly written against the rows of the node it supersedes; or a
    /// block kept whole, after the same header.
    Frame,
    /// Pages of the file's index (see the archive's `tree` module): the
    /// padding's length (u16) and the padding, then pages, each on a page
    /// boundary of the file.
    Pages,
    /// Space claimed by one writer, which appends frames to it without the
    /// writer's turn (see the archive's `extent` module).
    Extent,
    /// A memory cell, written over in place (see the archive's `cells`
    /// module).
    Cell,
}

impl Kind {
    /// Whether the record's checksum covers its body as it is now: not for
    /// records whose body is written to after the record is.
    fn checked(self) -> bool {
        !matches!(self, Kind::Pages | Kind::Extent | Kind::Cell)
    }
}

impl Kind {
    fn tag(self) -> u8 {
        match self {
            Kind::Tokens => 4,
            Kind::Frame => 5,
            Kind::Pages => 7,
            Kind::Extent => 8,
            Kind::Cell => 9,
        }
    }

    pub(crate) fn from_tag(tag: u8) -> Option<Self> {
        match tag {
            4 => Some(Kind::Tokens),
            5 => Some(Kind::Frame),
            7 => Some(Kind::Pages),
            8 => Some(Kind::Extent),
            9 => Some(Kind::Cell),
            _ => None,
        }
    }
}

pub(crate) fn checksum(kind: u8, body: &[u8]) -> u32 {
    let mut hasher = crc32fast::Hasher::new();
    hasher.update(&[kind]);
    hasher.update(body);
    hasher.finalize()
}

/// Appends a record of `kind` holding `body` to `out`.
pub(crate) fn put_record(out: &mut Vec<u8>, kind: Kind, body: &[u8]) -> io::Result<()> {
    let length = u32::try_from(body.len())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "a record over 4 GiB"))?;
    let tag = kind.tag();
    out.push(tag);
    out.extend_from_slice(&length.to_le_bytes());
    out.extend_from_slice(&checksum(tag, body).to_le_bytes());
    out.extend_from_slice(body);
    Ok(())
}

/// A whole record read back from the file, for tools that walk the file.
#[cfg(test)]
pub(crate) struct Record {
    /// Where the record starts.
    pub offset: u64,
    pub kind: Kind,
    pub body: Vec<u8>,
}

#[cfg(test)]
impl Record {
    /// Where the record's body starts.
    pub fn body_offset(&self) -> u64 {
        self.offset + HEADER
    }

    /// Where the next record starts.
    pub fn end(&self) -> u64 {
        self.body_offset() + self.body.len() as u64
    }
}

/// The record at `offset` in a file of `len` bytes, or `None` when the bytes
/// there are not a whole, intact record of a known kind.
#[cfg(test)]
pub(crate) fn read_record(file: &File, offset: u64, len: u64) -> io::Result<Option<Record>> {
    if offset + HEADER > len {
        return Ok(None);
    }
    let mut header = [0u8; HEADER as usize];
    read_at(file, offset, &mut header)?;
    let Some(kind) = Kind::from_tag(header[0]) else {
        return Ok(None);
    };
    let length = u32::from_le_bytes(header[1..5].try_into().expect("four bytes")) as u64;
    let expected = u32::from_le_bytes(header[5..9].try_into().expect("four bytes"));
    if offset + HEADER + length > len {
        return Ok(None);
    }
    let mut body = vec![0u8; length as usize];
    read_at(file, offset + HEADER, &mut body)?;
    if kind.checked() && checksum(header[0], &body) != expected {
        return Ok(None);
    }
    Ok(Some(Record { offset, kind, body }))
}

/// The record at `at` in `bytes` (the file, mapped): its kind, where its
/// body starts and where it ends. `None` when the bytes there are not a
/// whole, intact record of a known kind; a [pages](Kind::Pages),
/// [extent](Kind::Extent) or [cell](Kind::Cell) record's checksum is not
/// checked, as their bodies are written to after them.
pub(crate) fn record_in(bytes: &[u8], at: usize) -> Option<(Kind, usize, usize)> {
    let header = bytes.get(at..at + HEADER as usize)?;
    let kind = Kind::from_tag(header[0])?;
    let len = u32::from_le_bytes(header[1..5].try_into().ok()?) as usize;
    let start = at + HEADER as usize;
    let body = bytes.get(start..start + len)?;
    if kind.checked()
        && checksum(header[0], body) != u32::from_le_bytes(header[5..9].try_into().ok()?)
    {
        return None;
    }
    Some((kind, start, start + len))
}

/// Fills `buffer` from `offset`, without moving any shared cursor.
pub(crate) fn read_at(file: &File, offset: u64, buffer: &mut [u8]) -> io::Result<()> {
    #[cfg(unix)]
    {
        std::os::unix::fs::FileExt::read_exact_at(file, buffer, offset)
    }
    #[cfg(windows)]
    {
        let mut done = 0;
        while done < buffer.len() {
            let read = std::os::windows::fs::FileExt::seek_read(
                file,
                &mut buffer[done..],
                offset + done as u64,
            )?;
            if read == 0 {
                return Err(io::ErrorKind::UnexpectedEof.into());
            }
            done += read;
        }
        Ok(())
    }
}

/// Writes all of `bytes` at `offset`, without moving any shared cursor.
pub(crate) fn write_at(file: &File, offset: u64, bytes: &[u8]) -> io::Result<()> {
    #[cfg(unix)]
    {
        std::os::unix::fs::FileExt::write_all_at(file, bytes, offset)
    }
    #[cfg(windows)]
    {
        let mut done = 0;
        while done < bytes.len() {
            done += std::os::windows::fs::FileExt::seek_write(
                file,
                &bytes[done..],
                offset + done as u64,
            )?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn file_with(bytes: &[u8]) -> anyhow::Result<(tempfile::NamedTempFile, u64)> {
        let file = tempfile::NamedTempFile::new()?;
        write_at(file.as_file(), 0, bytes)?;
        Ok((file, bytes.len() as u64))
    }

    #[test]
    fn it_reads_back_a_record_it_wrote() -> anyhow::Result<()> {
        let mut bytes = Vec::new();
        put_record(&mut bytes, Kind::Tokens, b"a body")?;
        put_record(&mut bytes, Kind::Frame, &0u64.to_le_bytes())?;
        let (file, len) = file_with(&bytes)?;

        let first = read_record(file.as_file(), 0, len)?.expect("a whole record");
        assert_eq!(first.kind, Kind::Tokens);
        assert_eq!(first.body, b"a body");
        let second = read_record(file.as_file(), first.end(), len)?.expect("a whole record");
        assert_eq!(second.kind, Kind::Frame);
        assert_eq!(second.end(), len);
        assert_eq!(second.end() - second.offset, HEADER + 8);
        Ok(())
    }

    #[test]
    fn it_refuses_a_record_cut_short() -> anyhow::Result<()> {
        let mut bytes = Vec::new();
        put_record(&mut bytes, Kind::Tokens, b"a body")?;
        bytes.pop();
        let (file, len) = file_with(&bytes)?;
        assert!(read_record(file.as_file(), 0, len)?.is_none());
        Ok(())
    }

    #[test]
    fn it_refuses_a_record_whose_body_changed() -> anyhow::Result<()> {
        let mut bytes = Vec::new();
        put_record(&mut bytes, Kind::Tokens, b"a body")?;
        let last = bytes.len() - 1;
        bytes[last] ^= 1;
        let (file, len) = file_with(&bytes)?;
        assert!(read_record(file.as_file(), 0, len)?.is_none());
        Ok(())
    }

    #[test]
    fn it_refuses_an_unknown_kind() -> anyhow::Result<()> {
        let mut bytes = Vec::new();
        put_record(&mut bytes, Kind::Tokens, b"a body")?;
        bytes[0] = 0xEE;
        let (file, len) = file_with(&bytes)?;
        assert!(read_record(file.as_file(), 0, len)?.is_none());
        Ok(())
    }
}

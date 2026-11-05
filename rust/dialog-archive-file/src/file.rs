//! The file: a magic number, then an append-only sequence of records.
//!
//! Every record is its kind (1 byte), its body's length (4 bytes, little
//! endian), the crc32 of its kind and body (4 bytes, little endian), then
//! its body, so a reader can walk the file record by record and tell a whole record
//! from a torn one: a record whose length runs past the end of the file, or
//! whose checksum does not match, ends the file there.

use std::fs::File;
use std::io;

/// The first bytes of every archive file. The last byte is the format
/// version.
pub(crate) const MAGIC: &[u8; 8] = b"DIALOGA\x01";

/// The bytes of a record before its body.
pub(crate) const HEADER: u64 = 1 + 4 + 4;

/// The bytes of a whole footer record: a header and an 8-byte offset.
pub(crate) const FOOTER: u64 = HEADER + 8;

/// What a record holds.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Kind {
    /// A block: its catalog (a length byte and the name), then its bytes.
    Block,
    /// The index as of this record: every block's catalog, hash and extent.
    Checkpoint,
    /// The offset of the latest checkpoint record, or 0 for none. Written
    /// last by every append, so a file that ends in a footer ends whole.
    Footer,
    /// Tokens, each a length and its bytes. A token's id is its place among
    /// all the tokens the file holds, in file order.
    Tokens,
    /// A tree node as rows of token ids, possibly written against the rows
    /// of the node it supersedes.
    Frame,
}

impl Kind {
    fn tag(self) -> u8 {
        match self {
            Kind::Block => 1,
            Kind::Checkpoint => 2,
            Kind::Footer => 3,
            Kind::Tokens => 4,
            Kind::Frame => 5,
        }
    }

    fn from_tag(tag: u8) -> Option<Self> {
        match tag {
            1 => Some(Kind::Block),
            2 => Some(Kind::Checkpoint),
            3 => Some(Kind::Footer),
            4 => Some(Kind::Tokens),
            5 => Some(Kind::Frame),
            _ => None,
        }
    }
}

fn checksum(kind: u8, body: &[u8]) -> u32 {
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

/// A whole record read back from the file.
pub(crate) struct Record {
    /// Where the record starts.
    pub offset: u64,
    pub kind: Kind,
    pub body: Vec<u8>,
}

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
    if checksum(header[0], &body) != expected {
        return Ok(None);
    }
    Ok(Some(Record { offset, kind, body }))
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
        put_record(&mut bytes, Kind::Block, b"a body")?;
        put_record(&mut bytes, Kind::Footer, &0u64.to_le_bytes())?;
        let (file, len) = file_with(&bytes)?;

        let first = read_record(file.as_file(), 0, len)?.expect("a whole record");
        assert_eq!(first.kind, Kind::Block);
        assert_eq!(first.body, b"a body");
        let second = read_record(file.as_file(), first.end(), len)?.expect("a whole record");
        assert_eq!(second.kind, Kind::Footer);
        assert_eq!(second.end(), len);
        assert_eq!(second.end() - second.offset, FOOTER);
        Ok(())
    }

    #[test]
    fn it_refuses_a_record_cut_short() -> anyhow::Result<()> {
        let mut bytes = Vec::new();
        put_record(&mut bytes, Kind::Block, b"a body")?;
        bytes.pop();
        let (file, len) = file_with(&bytes)?;
        assert!(read_record(file.as_file(), 0, len)?.is_none());
        Ok(())
    }

    #[test]
    fn it_refuses_a_record_whose_body_changed() -> anyhow::Result<()> {
        let mut bytes = Vec::new();
        put_record(&mut bytes, Kind::Block, b"a body")?;
        let last = bytes.len() - 1;
        bytes[last] ^= 1;
        let (file, len) = file_with(&bytes)?;
        assert!(read_record(file.as_file(), 0, len)?.is_none());
        Ok(())
    }

    #[test]
    fn it_refuses_an_unknown_kind() -> anyhow::Result<()> {
        let mut bytes = Vec::new();
        put_record(&mut bytes, Kind::Block, b"a body")?;
        bytes[0] = 0xEE;
        let (file, len) = file_with(&bytes)?;
        assert!(read_record(file.as_file(), 0, len)?.is_none());
        Ok(())
    }
}

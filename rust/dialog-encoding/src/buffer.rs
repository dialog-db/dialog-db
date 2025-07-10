//! Buffer encoding and decoding for columnar data with zero-copy reading.
//!
//! This module provides efficient encoding and decoding of columnar data structures
//! that implement the [`Cellular`] trait. The encoding scheme is designed to minimize
//! memory usage through deduplication and enable zero-copy reading of the encoded data.
//!
//! # Binary Layout
//!
//! The encoded buffer has the following layout:
//!
//! ```text
//! ┌─────────────────┐
//! │   Data Length   │
//! │    (LEB128)     │
//! ├─────────────────┤   ┌─────────────────┬─────────────────┬───────┐
//! │      Data       ├──→│     Cell 0      │     Cell 1      │  ...  │
//! │   (Raw Bytes)   │   │   (Raw Bytes)   │   (Raw Bytes)   │       │
//! │                 │   └─────────────────┴─────────────────┴───────┘
//! ├─────────────────┤
//! │  Ranges Length  │
//! │    (LEB128)     │
//! ├─────────────────┤   ┌─────────────────┬─────────────────┬───────┐
//! │     Ranges      ├──→│  Cell 0 Offset  │ Cell 0 Length   │  ...  │
//! │   (LEB128[])    │   │    (LEB128)     │    (LEB128)     │       │
//! │                 │   └─────────────────┴─────────────────┴───────┘
//! ├─────────────────┤   ┌─────────────────┬─────────────────┬───────┐
//! │      Cells      ├──→│  Cell 0 Index   │  Cell 1 Index   │  ...  │
//! │   (LEB128[])    │   │    (LEB128)     │    (LEB128)     │       │
//! │                 │   └─────────────────┴─────────────────┴───────┘
//! └─────────────────┘
//! ```
//!
//! In the above layout:
//!
//! - **Data** is a sequence of all byte-wise unique cells (byte slices) in the input
//! - **Ranges** are the offset and length for each cell within the data section
//! - **Cells** are pointers into the range section, and their order is derived from
//!   the input data.
//!
//! # Deduplication
//!
//! The encoding automatically deduplicates identical byte sequences. When the same
//! data is encountered multiple times, only the first occurrence is stored in the
//! data section, and subsequent references use the same index.
//!
//! # Zero-Copy Reading
//!
//! During decoding, cell data is not copied. Instead, the [`CellDecoder`] iterator
//! returns slices directly into the original buffer, enabling efficient access to
//! the encoded data without additional memory allocations.

use std::{
    collections::BTreeMap,
    io::{Cursor, Seek, SeekFrom, Write},
};

use crate::{Cellular, DialogEncodingError};

/// Encodes a columnar data structure into a binary buffer with deduplication.
///
/// This function takes a data structure that implements [`Cellular`] and encodes it
/// into a compact binary format that enables zero-copy reading. Identical byte
/// sequences are automatically deduplicated to minimize storage space.
///
/// # Arguments
///
/// * `layout` - The data structure to encode, must implement [`Cellular`]
/// * `buffer` - The output buffer to write the encoded data to
///
/// # Returns
///
/// Returns `Ok(())` on success, or an `std::io::Error` if writing to the buffer fails.
///
/// # Example
///
/// ```rust
/// use dialog_encoding::{encode, Cellular, Width};
///
/// struct MyData<'a> {
///     cells: Vec<&'a [u8]>,
/// }
///
/// impl<'a> Cellular<'a> for MyData<'a> {
///     fn cell_width() -> Width {
///         Width::Unbounded
///     }
///
///     fn cells(&'a self) -> impl Iterator<Item = &'a [u8]> {
///         self.cells.iter().copied()
///     }
///     
///     fn try_from_cells<I>(cells: &mut I) -> Result<Self, dialog_encoding::DialogEncodingError>
///     where I: Iterator<Item = &'a [u8]> {
///         Ok(MyData { cells: cells.collect() })
///     }
/// }
///
/// let data = MyData { cells: vec![b"hello", b"world", b"hello"] };
/// let mut buffer = Vec::new();
/// encode(&data, &mut buffer).unwrap();
/// // The encoded buffer will deduplicate the repeated "hello"
/// ```
pub fn encode<'a, Layout, Buffer>(
    layout: &Layout,
    mut buffer: Buffer,
) -> Result<(), DialogEncodingError>
where
    Layout: Cellular<'a>,
    Buffer: Write,
{
    let mut data = Cursor::new(Vec::new());
    let mut ranges = Cursor::new(Vec::new());
    let mut cells = Cursor::new(Vec::new());
    let mut bytes_to_index = BTreeMap::<&'a [u8], u64>::new();
    let mut next_index = 0u64;
    let mut data_length = 0usize;

    for cell in layout.cells() {
        if let Some(index) = bytes_to_index.get(cell) {
            leb128::write::unsigned(&mut cells, *index)?;
        } else {
            leb128::write::unsigned(&mut ranges, data_length as u64)?;
            leb128::write::unsigned(&mut ranges, cell.len() as u64)?;

            data_length += cell.len();
            data.write_all(cell)?;
            bytes_to_index.insert(cell, next_index);

            leb128::write::unsigned(&mut cells, next_index)?;

            next_index += 1;
        }
    }

    let data = data.into_inner();
    let ranges = ranges.into_inner();
    let cells = cells.into_inner();

    // [ data length ][ data ]
    leb128::write::unsigned(&mut buffer, data.len() as u64)?;
    buffer.write_all(&data)?;

    // [ ranges length ][ ranges ]
    leb128::write::unsigned(&mut buffer, ranges.len() as u64)?;
    buffer.write_all(&ranges)?;

    // [ cells ]
    buffer.write_all(&cells)?;

    Ok(())
}

/// Decodes a binary buffer into a columnar data structure with zero-copy access.
///
/// This function takes a buffer previously encoded with [`encode`] and reconstructs
/// the original data structure. The decoding process is zero-copy, meaning that
/// cell data references slices directly into the input buffer rather than copying data.
///
/// # Arguments
///
/// * `buffer` - The encoded buffer to decode, must contain data in the format produced by [`encode`]
///
/// # Returns
///
/// Returns the decoded data structure on success, or a [`DialogEncodingError`] if the
/// buffer is malformed or decoding fails.
///
/// # Example
///
/// ```rust
/// use dialog_encoding::{encode, decode, Cellular, Width, DialogEncodingError};
///
/// struct MyData<'a> {
///     cells: Vec<&'a [u8]>,
/// }
///
/// impl<'a> Cellular<'a> for MyData<'a> {
///     fn cell_width() -> Width {
///         Width::Unbounded
///     }
///
///     fn cells(&'a self) -> impl Iterator<Item = &'a [u8]> {
///         self.cells.iter().copied()
///     }
///     
///     fn try_from_cells<I>(cells: &mut I) -> Result<Self, DialogEncodingError>
///     where I: Iterator<Item = &'a [u8]> {
///         Ok(MyData { cells: cells.collect() })
///     }
/// }
///
/// let original_data = MyData { cells: vec![b"hello", b"world"] };
/// let mut buffer = Vec::new();
/// encode(&original_data, &mut buffer).unwrap();
///
/// // Zero-copy decode - decoded_data references the buffer directly
/// let decoded_data: MyData = decode(&buffer).unwrap();
/// ```
///
/// # Zero-Copy Behavior
///
/// The returned data structure contains references to slices within the input buffer.
/// This means the buffer must remain valid for as long as the decoded data is used.
/// The lifetime parameter `'a` ensures this relationship is enforced by the compiler.
pub fn decode<'a, Layout, Buffer>(buffer: &'a Buffer) -> Result<Layout, DialogEncodingError>
where
    Layout: Cellular<'a>,
    Buffer: AsRef<[u8]> + ?Sized,
{
    let mut cursor: Cursor<&[u8]> = Cursor::new(buffer.as_ref());

    let data_length = leb128::read::unsigned(&mut cursor)?;
    let data_range = cursor.position() as usize..(cursor.position() + data_length) as usize;
    let data = &buffer.as_ref()[data_range];

    cursor.seek(SeekFrom::Start(cursor.position() + data_length))?;

    let ranges_length = leb128::read::unsigned(&mut cursor)?;
    let ranges_range = cursor.position() as usize..(cursor.position() + ranges_length) as usize;
    let mut range_data = Cursor::new(&buffer.as_ref()[ranges_range]);
    let mut ranges: Vec<(usize, usize)> = Vec::new();

    while (range_data.position() as usize) < range_data.get_ref().as_ref().len() {
        ranges.push((
            leb128::read::unsigned(&mut range_data)? as usize,
            leb128::read::unsigned(&mut range_data)? as usize,
        ))
    }

    cursor.seek(SeekFrom::Start(cursor.position() + ranges_length))?;

    Layout::try_from_cells(&mut CellDecoder {
        data,
        ranges,
        cells: cursor,
    })
}

/// Iterator that provides zero-copy access to individual cells in an encoded buffer.
///
/// This iterator is created by the [`decode`] function and yields byte slices that
/// reference data directly within the original encoded buffer. No copying is performed,
/// making this extremely efficient for accessing large amounts of columnar data.
///
/// # Zero-Copy Guarantee
///
/// Each call to `next()` returns a slice directly into the `data` section of the
/// original buffer. The lifetime `'a` ensures the buffer remains valid for the
/// duration of iteration.
pub struct CellDecoder<'a> {
    /// The deduplicated cell data section
    pub data: &'a [u8],
    /// Offset and length pairs for each unique cell in the data section  
    pub ranges: Vec<(usize, usize)>,
    /// Cursor over the cell indices that reference into the ranges
    pub cells: Cursor<&'a [u8]>,
}

impl<'a> Iterator for CellDecoder<'a> {
    type Item = &'a [u8];

    /// Returns the next cell as a zero-copy slice into the original buffer.
    ///
    /// This method reads the next cell index from the encoded buffer, looks up
    /// the corresponding range in the ranges table, and returns a slice directly
    /// into the data section. No copying is performed.
    ///
    /// # Returns
    ///
    /// - `Some(&[u8])` - A slice referencing the next cell's data in the buffer
    /// - `None` - When all cells have been consumed
    ///
    /// # Panics
    ///
    /// This method may panic if the encoded buffer is malformed and contains
    /// invalid indices that exceed the bounds of the ranges table.
    fn next(&mut self) -> Option<Self::Item> {
        let index = leb128::read::unsigned(&mut self.cells).ok()?;

        let (data_index, data_length) = self.ranges[index as usize];
        Some(&self.data[data_index..(data_index + data_length)])
    }
}

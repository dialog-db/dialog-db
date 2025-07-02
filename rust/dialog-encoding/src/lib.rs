//! Dialog Encoding - Zero-Copy Columnar Data Encoding
//!
//! This crate provides a bespoke binary encoding scheme designed for "columnar"
//! data with repetitive patterns. The encoding enables zero-copy reading of the
//! encoded representation, making it extremely efficient for accessing large
//! datasets without memory allocation overhead.
//!
//! # Architecture
//!
//! The encoding is built around three core concepts:
//!
//! 1. **Cells**: Individual byte sequences that make up the data structure
//! 2. **Deduplication**: Identical cells are stored once and referenced by
//!    index
//! 3. **Zero-Copy Access**: Decoding returns slices into the original buffer
//!
//! # Basic Usage
//!
//! ```rust
//! use dialog_encoding::{encode, decode, Cellular, DialogEncodingError};
//!
//! // Define a data structure that can be broken into cells
//! struct Record<'a> {
//!     name: &'a [u8],
//!     value: &'a [u8],
//! }
//!
//! impl<'a> Cellular<'a> for Record<'a> {
//!     fn cells(&'a self) -> impl Iterator<Item = &'a [u8]> {
//!         [self.name, self.value].into_iter()
//!     }
//!
//!     fn try_from_cells<I>(mut cells: I) -> Result<Self, DialogEncodingError>
//!     where I: Iterator<Item = &'a [u8]> {
//!         let name = cells.next().unwrap();
//!         let value = cells.next().unwrap();
//!         Ok(Record { name, value })
//!     }
//! }
//!
//! // Encode data
//! let record = Record { name: b"hello", value: b"world" };
//! let mut buffer = Vec::new();
//! encode(&record, &mut buffer).unwrap();
//!
//! // Zero-copy decode
//! let decoded: Record = decode(&buffer).unwrap();
//! assert_eq!(decoded.name, b"hello");
//! assert_eq!(decoded.value, b"world");
//! ```
//!
//! # Performance Characteristics
//!
//! - **Encoding**: O(n) where n is the number of cells, with additional
//!   overhead for deduplication
//! - **Decoding**: O(1) setup cost, then O(1) per cell access with zero memory
//!   allocation
//! - **Memory Usage**: Encoded size depends on deduplication effectiveness,
//!   typically much smaller than raw data
//!
//! # Binary Format
//!
//! See the [`buffer`] module documentation for detailed information about the
//! binary layout, including ASCII art diagrams of the encoding structure.

mod error;
pub use error::*;

mod cellular;
pub use cellular::*;

mod buffer;
pub use buffer::*;

#[cfg(test)]
mod tests {
    use crate::{decode, encode};

    use super::Cellular;
    use anyhow::Result;
    use itertools::Itertools;
    use rand::random;

    #[derive(PartialEq, Debug)]
    struct Byte(u8);

    #[derive(PartialEq, Debug)]
    struct Collection {
        entries: Vec<Entry>,
    }

    #[derive(PartialEq, Debug)]
    struct Entry {
        string: String,
        bytes: Vec<u8>,
        inner: Inner,
    }

    #[derive(PartialEq, Debug)]
    struct Inner {
        byte: Byte,
        array: [u8; 32],
    }

    struct CollectionCells<'a> {
        entries: Vec<EntryCells<'a>>,
    }

    struct EntryCells<'a> {
        string: &'a [u8],
        bytes: &'a [u8],
        inner: InnerCells<'a>,
    }

    struct InnerCells<'a> {
        byte: &'a [u8],
        array: &'a [u8],
    }

    impl<'a> TryFrom<CollectionCells<'a>> for Collection {
        type Error = anyhow::Error;

        fn try_from(value: CollectionCells<'a>) -> std::result::Result<Self, Self::Error> {
            let mut entries = Vec::new();

            for entry_cells in value.entries {
                entries.push(Entry::try_from(entry_cells)?);
            }

            Ok(Collection { entries })
        }
    }

    impl<'a> TryFrom<EntryCells<'a>> for Entry {
        type Error = anyhow::Error;

        fn try_from(value: EntryCells<'a>) -> std::result::Result<Self, Self::Error> {
            Ok(Entry {
                string: String::from_utf8(value.string.to_vec())?,
                bytes: value.bytes.into(),
                inner: Inner::try_from(value.inner)?,
            })
        }
    }

    impl<'a> TryFrom<InnerCells<'a>> for Inner {
        type Error = anyhow::Error;

        fn try_from(value: InnerCells<'a>) -> std::result::Result<Self, Self::Error> {
            Ok(Inner {
                byte: Byte(value.byte.first().copied().unwrap_or_default()),
                array: value.array.try_into()?,
            })
        }
    }

    impl<'a> Cellular<'a> for CollectionCells<'a> {
        fn cells(&'a self) -> impl Iterator<Item = &'a [u8]> {
            self.entries
                .iter()
                .flat_map(|entry_cells| entry_cells.cells())
        }

        fn try_from_cells<I>(cells: I) -> std::result::Result<Self, crate::DialogEncodingError>
        where
            I: Iterator<Item = &'a [u8]>,
        {
            let mut entries = Vec::new();

            for chunk in &cells.chunks(4) {
                entries.push(EntryCells::try_from_cells(chunk)?);
            }

            Ok(Self { entries })
        }
    }

    impl<'a> Cellular<'a> for EntryCells<'a> {
        fn cells(&'a self) -> impl Iterator<Item = &'a [u8]> {
            [self.string, self.bytes]
                .into_iter()
                .chain(self.inner.cells())
        }

        fn try_from_cells<I>(mut cells: I) -> std::result::Result<Self, crate::DialogEncodingError>
        where
            I: Iterator<Item = &'a [u8]>,
        {
            let Some(string) = cells.next() else { panic!() };
            let Some(bytes) = cells.next() else { panic!() };

            Ok(Self {
                string,
                bytes,
                inner: InnerCells::try_from_cells(cells)?,
            })
        }
    }

    impl<'a> Cellular<'a> for InnerCells<'a> {
        fn cells(&'a self) -> impl Iterator<Item = &'a [u8]> {
            [self.byte, self.array].into_iter()
        }

        fn try_from_cells<I>(mut cells: I) -> std::result::Result<Self, crate::DialogEncodingError>
        where
            I: Iterator<Item = &'a [u8]>,
        {
            let Some(byte) = cells.next() else { panic!() };
            let Some(array) = cells.next() else { panic!() };
            Ok(Self { byte, array })
        }
    }

    impl<'a> From<&'a Collection> for CollectionCells<'a> {
        fn from(value: &'a Collection) -> Self {
            Self {
                entries: value.entries.iter().map(EntryCells::from).collect(),
            }
        }
    }

    impl<'a> From<&'a Entry> for EntryCells<'a> {
        fn from(value: &'a Entry) -> Self {
            EntryCells {
                string: value.string.as_bytes(),
                bytes: value.bytes.as_ref(),
                inner: InnerCells::from(&value.inner),
            }
        }
    }

    impl<'a> From<&'a Inner> for InnerCells<'a> {
        fn from(value: &'a Inner) -> Self {
            InnerCells {
                byte: std::slice::from_ref(&value.byte.0),
                array: value.array.as_ref(),
            }
        }
    }

    #[test]
    fn it_can_convert_a_struct_to_cells_and_back() -> Result<()> {
        let entry = Entry {
            string: "Hello".into(),
            bytes: vec![1, 2, 3],
            inner: Inner {
                byte: Byte(123),
                array: random(),
            },
        };

        let entry_cells = EntryCells::from(&entry);
        assert_eq!(entry_cells.cells().count(), 4);

        let mut buffer = Vec::new();
        encode(&entry_cells, &mut buffer)?;

        let entry_cells: EntryCells<'_> = decode(&buffer)?;
        let final_entry = Entry::try_from(entry_cells)?;

        assert_eq!(entry, final_entry);

        Ok(())
    }

    #[test]
    fn it_can_convert_a_collection_to_cells_and_back() -> Result<()> {
        let collection = Collection {
            entries: vec![
                Entry {
                    string: "Hello".into(),
                    bytes: vec![1, 2, 3],
                    inner: Inner {
                        byte: Byte(123),
                        array: random(),
                    },
                },
                Entry {
                    string: "World".into(),
                    bytes: vec![2, 2, 2],
                    inner: Inner {
                        byte: Byte(222),
                        array: random(),
                    },
                },
            ],
        };

        let collection_cells = CollectionCells::from(&collection);
        assert_eq!(collection_cells.cells().count(), 8);

        let mut buffer = Vec::new();
        encode(&collection_cells, &mut buffer)?;

        let collection_cells: CollectionCells<'_> = decode(&buffer)?;
        let final_collection = Collection::try_from(collection_cells)?;

        assert_eq!(collection, final_collection);

        Ok(())
    }
}

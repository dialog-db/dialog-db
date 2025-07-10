use dialog_encoding::{Cellular, DialogEncodingError, Width};
use itertools::Itertools;
use nonempty::NonEmpty;

use crate::{Entry, KeyBuffer, ValueBuffer};

pub struct Segment<'a, Key, Value>
where
    Key: KeyBuffer<'a>,
    Value: ValueBuffer<'a>,
{
    pub entries: NonEmpty<Entry<'a, Key, Value>>,
}

impl<'a, Key, Value> Cellular<'a> for Segment<'a, Key, Value>
where
    Key: KeyBuffer<'a>,
    Value: ValueBuffer<'a>,
{
    fn cell_width() -> Width {
        Width::Unbounded
    }

    fn cells(&self) -> impl Iterator<Item = &[u8]> {
        self.entries.iter().flat_map(|entry| entry.cells())
    }

    fn try_from_cells<I>(cells: &mut I) -> Result<Self, dialog_encoding::DialogEncodingError>
    where
        I: Iterator<Item = &'a [u8]>,
    {
        let mut entries = Vec::new();
        let chunk_size = match Entry::<Key, Value>::cell_width() {
            Width::Bounded(size) => size,
            _ => {
                return Err(DialogEncodingError::InvalidLayout(format!(
                    "Entries must have bounded cell width"
                )));
            }
        };

        for mut chunk in &cells.chunks(chunk_size) {
            entries.push(Entry::try_from_cells(&mut chunk)?);
        }

        Ok(Self {
            entries: NonEmpty::from_vec(entries).ok_or_else(|| {
                DialogEncodingError::InvalidLayout("Segment must have at least one entry".into())
            })?,
        })
    }
}

// use crate::{Entry, Key, Value};
// use dialog_encoding::{Cellular, DialogEncodingError, Width};
// use itertools::Itertools;
// use nonempty::NonEmpty;

// pub struct Segment<'a, K, V>
// where
//     Self: 'a,
//     K: Key<'a>,
//     V: Value<'a>,
// {
//     pub entries: NonEmpty<Entry<'a, K, V>>,
// }

// impl<'a, K, V> Cellular<'a> for Segment<'a, K, V>
// where
//     K: Key<'a>,
//     V: Value<'a>,
// {
//     fn cell_width() -> Width {
//         Width::Unbounded
//     }

//     fn cells(&'a self) -> impl Iterator<Item = &'a [u8]> {
//         self.entries.iter().flat_map(|entry| entry.cells())
//     }

//     fn try_from_cells<I>(cells: &mut I) -> Result<Self, dialog_encoding::DialogEncodingError>
//     where
//         I: Iterator<Item = &'a [u8]>,
//     {
//         let mut entries = Vec::new();
//         let chunk_size = match Entry::<K, V>::cell_width() {
//             Width::Bounded(size) => size,
//             _ => {
//                 return Err(DialogEncodingError::InvalidLayout(format!(
//                     "Entries must have bounded cell width"
//                 )));
//             }
//         };

//         for mut chunk in &cells.chunks(chunk_size) {
//             entries.push(Entry::try_from_cells(&mut chunk)?);
//         }

//         Ok(Self {
//             entries: NonEmpty::from_vec(entries).ok_or_else(|| {
//                 DialogEncodingError::InvalidLayout("Segment must have at least one entry".into())
//             })?,
//         })
//     }
// }

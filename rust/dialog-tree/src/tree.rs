use std::marker::PhantomData;

use dialog_common::Blake3Hash;

use crate::{DialogTreeError, KeyBuffer, Node, TreeStorage, ValueBuffer};

pub struct Tree<'a, Key, Value, Distribution>
where
    Key: KeyBuffer<'a>,
    Value: ValueBuffer<'a>,
{
    key: PhantomData<Key>,
    value: PhantomData<Value>,
    distribution: PhantomData<Distribution>,
    lifetime: PhantomData<&'a ()>,

    root: Option<Node<'a, Key, Value>>,
}

impl<'a, Key, Value, Distribution> Tree<'a, Key, Value, Distribution>
where
    Self: 'a,
    Key: KeyBuffer<'a>,
    Value: ValueBuffer<'a>,
    Distribution: crate::Distribution<'a, Key = Key, Value = Value>,
{
    pub fn new() -> Self {
        Self {
            key: PhantomData,
            value: PhantomData,
            distribution: PhantomData,
            lifetime: PhantomData,

            root: None,
        }
    }

    pub async fn insert<Storage>(
        &mut self,
        key: Key,
        value: Value,
        storage: &Storage,
    ) -> Result<(), DialogTreeError>
    where
        Storage: TreeStorage,
    {
        todo!();
    }

    pub async fn get<Storage>(
        &self,
        key: &Key,
        storage: &Storage,
    ) -> Result<Option<&'a Value::Ref>, DialogTreeError>
    where
        Storage: TreeStorage,
    {
        todo!();
    }

    pub async fn remove<Storage>(
        &mut self,
        key: &Key,
        storage: &Storage,
    ) -> Result<Option<Value>, DialogTreeError>
    where
        Storage: TreeStorage,
    {
        todo!();
    }

    pub async fn flush<Storage>(
        &mut self,
        storage: &mut Storage,
    ) -> Result<Blake3Hash, DialogTreeError>
    where
        Storage: TreeStorage,
    {
        todo!();
    }
}

// struct Entries<'a, K, V>(Vec<(K, V)>, PhantomData<&'a ()>);

// impl<'a, K, V> Cellular<'a> for Entries<'a, K, V>
// where
//     K: KeyBuffer<'a>,
//     V: Value<'a>,
// {
//     fn cell_width() -> Width {
//         Width::Unbounded
//     }

//     fn cells(&'a self) -> impl Iterator<Item = &'a [u8]> {
//         self.0.iter().flat_map(|(k, v)| k.cells().chain(v.cells()))
//     }

//     fn try_from_cells<I>(cells: &mut I) -> Result<Self, dialog_encoding::DialogEncodingError>
//     where
//         I: Iterator<Item = &'a [u8]>,
//     {
//         let mut entries = Vec::new();

//         let chunk_size = match K::cell_width() + V::cell_width() {
//             Width::Bounded(chunk_size) => chunk_size,
//             _ => todo!(),
//         };

//         for mut chunk in &cells.chunks(chunk_size) {
//             let key = K::try_from_cells(&mut chunk)?;
//             let value = V::try_from_cells(&mut chunk)?;

//             entries.push((key, value));
//         }

//         Ok(Self(entries, PhantomData))
//     }
// }

// struct Node<'a, K, V> {
//     buffer: Bytes,
//     entries: OnceCell<Entries<'a, K, V>>,
//     dirty: bool,
//     lifetime: PhantomData<&'a ()>,
// }

// impl<'a, K, V> Node<'a, K, V>
// where
//     K: KeyBuffer<'a>,
//     V: Value<'a>,
// {
//     pub fn decode(buffer: Bytes) -> Self {
//         Self {
//             buffer,
//             entries: OnceCell::new(),
//             dirty: false,
//             lifetime: PhantomData,
//         }
//     }

//     fn entries(&'a self) -> &'a Entries<'a, K, V> {
//         self.entries
//             .get_or_try_init(|| decode(&self.buffer))
//             .unwrap()
//     }

//     // fn entries_mut(&'a mut self) -> &'a mut Entries<'a, K, V> {
//     //     let _ = self
//     //         .entries
//     //         .get_or_try_init(|| decode(&self.buffer))
//     //         .unwrap();
//     //     self.entries.get_mut().unwrap()
//     // }

//     pub fn push(&'a mut self, key: K, value: V) -> Self {
//         self.dirty = true;
//         let entries = self.entries_mut();
//         entries.0.push((key, value));
//         let mut buffer = Vec::new();
//         encode(entries, &mut buffer);
//     }
//     // let entries = decode(&buffer).unwrap();
// }

// // impl<'a, K, V> Cellular<'a> for Node<'a, K, V>
// // where
// //     K: Key<'a>,
// //     V: Value<'a>,
// // {
// //     fn cell_width() -> Width {
// //         Width::Unbounded
// //     }

// //     fn cells(&'a self) -> impl Iterator<Item = &'a [u8]> {
// //         self.values
// //             .iter()
// //             .flat_map(|(k, v)| k.cells().chain(v.cells()))
// //     }

// //     fn try_from_cells<I>(cells: &mut I) -> Result<Self, dialog_encoding::DialogEncodingError>
// //     where
// //         I: Iterator<Item = &'a [u8]>,
// //     {
// //         let key = K::try_from_cells(cells)?;
// //         let value = V::try_from_cells(cells)?;

// //     }
// // }

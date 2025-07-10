use std::marker::PhantomData;

use dialog_encoding::{Cellular, Width};

use crate::{KeyBuffer, ValueBuffer};

// pub trait Entry<'a> {
//     type Key: KeyBuffer<'a>;
//     type Value: ValueBuffer<'a>;
// }

#[derive(Clone)]
pub struct Entry<'a, Key, Value>
where
    Self: 'a,
    Key: KeyBuffer<'a>,
    Value: ValueBuffer<'a>,
{
    pub key: Key::Ref,
    pub value: Value::Ref,
    lifetime: PhantomData<&'a ()>,
}



impl<'a, Key, Value> Cellular<'a> for Entry<'a, Key, Value>
where
    Key: KeyBuffer<'a>,
    Value: ValueBuffer<'a>,
{
    fn cell_width() -> Width {
        Key::Ref::cell_width() + Value::Ref::cell_width()
    }

    fn cells(&self) -> impl Iterator<Item = &[u8]> {
        self.key.cells().chain(self.value.cells())
    }

    fn try_from_cells<I>(cells: &mut I) -> Result<Self, dialog_encoding::DialogEncodingError>
    where
        I: Iterator<Item = &'a [u8]>,
    {
        let key = Key::Ref::try_from_cells(cells)?;
        let value = Value::Ref::try_from_cells(cells)?;

        Ok(Self {
            key,
            value,
            lifetime: PhantomData,
        })
    }
}

// use std::marker::PhantomData;

// use crate::{Key, KeyRef, Value, ValueRef};
// use dialog_encoding::{Cellular, Width};

// pub struct Entry<'a, K, V>
// where
//     Self: 'a,
//     K: Key<'a>,
//     V: Value<'a>,
// {
//     key: K,
//     value: V,
//     lifetime: PhantomData<&'a ()>,
// }

// impl<'a, K, V> Cellular<'a> for Entry<'a, K, V>
// where
//     K: Key<'a>,
//     V: Value<'a>,
// {
//     fn cell_width() -> Width {
//         K::cell_width() + V::Ref::cell_width()
//     }

//     fn cells(&'a self) -> impl Iterator<Item = &'a [u8]> {
//         self.key.cells().chain(self.value.cells())
//     }

//     fn try_from_cells<I>(cells: &mut I) -> Result<Self, dialog_encoding::DialogEncodingError>
//     where
//         I: Iterator<Item = &'a [u8]>,
//     {
//         let key = K::try_from_cells(cells)?;
//         let value = V::try_from_cells(cells)?;

//         Ok(Self {
//             key,
//             value,
//             lifetime: PhantomData,
//         })
//     }
// }

use std::{
    collections::{BTreeMap, HashMap},
    hash::BuildHasher,
    rc::Rc,
    sync::Arc,
};

use crate::{Integer, Reference, Type};

/// A value with a reference.
///
/// The crate implements it for the core types: [`Null`] and `()` (null),
/// `bool`, the integer primitives other than `u8` and [`Integer`], `f64`,
/// `str` and `String` (text), [`Bytes`], slices, arrays and `Vec`s (lists),
/// and `BTreeMap`s and `HashMap`s (maps). `u8` is left out so that a `Vec<u8>`
/// is not taken for a list of integers by mistake: wrap bytes in [`Bytes`].
///
/// [`Reference`] implements it as itself, which is how a part stored apart is
/// put in its parent: by its reference, with the same result as the part.
pub trait Refer {
    /// The value's reference.
    fn refer(&self) -> Reference;
}

/// The null value.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub struct Null;

/// Plain bytes, referenced by their BLAKE3 hash.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub struct Bytes<T: AsRef<[u8]>>(pub T);

impl Refer for Reference {
    fn refer(&self) -> Reference {
        *self
    }
}

impl Refer for Null {
    fn refer(&self) -> Reference {
        Type::NULL.refer(&[])
    }
}

impl Refer for () {
    fn refer(&self) -> Reference {
        Null.refer()
    }
}

impl Refer for bool {
    fn refer(&self) -> Reference {
        Type::BOOLEAN.refer(&[u8::from(*self)])
    }
}

impl Refer for Integer {
    fn refer(&self) -> Reference {
        Type::INTEGER.refer(&self.content())
    }
}

macro_rules! refer_integer {
    ($($ty:ty),*) => {$(
        impl Refer for $ty {
            fn refer(&self) -> Reference {
                Integer::from(*self).refer()
            }
        }
    )*};
}

refer_integer!(u16, u32, u64, u128, usize, i8, i16, i32, i64, i128, isize);

impl Refer for f64 {
    fn refer(&self) -> Reference {
        Type::FLOAT.refer(&self.to_le_bytes())
    }
}

impl Refer for str {
    fn refer(&self) -> Reference {
        Type::TEXT.refer(self.as_bytes())
    }
}

impl Refer for String {
    fn refer(&self) -> Reference {
        self.as_str().refer()
    }
}

impl<T: AsRef<[u8]>> Refer for Bytes<T> {
    fn refer(&self) -> Reference {
        Reference::of_bytes(self.0.as_ref())
    }
}

impl<T: Refer> Refer for [T] {
    fn refer(&self) -> Reference {
        Type::LIST.list(self.iter().map(Refer::refer))
    }
}

impl<T: Refer, const N: usize> Refer for [T; N] {
    fn refer(&self) -> Reference {
        self.as_slice().refer()
    }
}

impl<T: Refer> Refer for Vec<T> {
    fn refer(&self) -> Reference {
        self.as_slice().refer()
    }
}

impl<K: Refer, V: Refer> Refer for BTreeMap<K, V> {
    fn refer(&self) -> Reference {
        Type::MAP.map(self.iter().map(|(key, value)| (key.refer(), value.refer())))
    }
}

impl<K: Refer, V: Refer, S: BuildHasher> Refer for HashMap<K, V, S> {
    fn refer(&self) -> Reference {
        Type::MAP.map(self.iter().map(|(key, value)| (key.refer(), value.refer())))
    }
}

impl<T: Refer + ?Sized> Refer for &T {
    fn refer(&self) -> Reference {
        (**self).refer()
    }
}

impl<T: Refer + ?Sized> Refer for Box<T> {
    fn refer(&self) -> Reference {
        (**self).refer()
    }
}

impl<T: Refer + ?Sized> Refer for Rc<T> {
    fn refer(&self) -> Reference {
        (**self).refer()
    }
}

impl<T: Refer + ?Sized> Refer for Arc<T> {
    fn refer(&self) -> Reference {
        (**self).refer()
    }
}

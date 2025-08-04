use std::marker::PhantomData;

use dialog_common::Blake3Hash;
use dialog_encoding::{Buf, Cellular, Ref, Width};

use crate::{Key, KeyRef};

/// A serializable reference to a [`Node`].
#[derive(Debug, Clone, PartialEq)]
pub struct Link<'a, Key>
where
    Key: self::Key<'a>,
    Key::Ref: self::KeyRef<'a, Key>,
{
    node: Blake3Hash,
    upper_bound: Key,
    lifetime: PhantomData<&'a ()>,
}

impl<'a, Key> Link<'a, Key>
where
    Key: self::Key<'a>,
    Key::Ref: self::KeyRef<'a, Key>,
{
    pub fn new(node: Blake3Hash, upper_bound: Key) -> Self {
        Self {
            node,
            upper_bound,
            lifetime: PhantomData,
        }
    }

    pub fn node(&self) -> &Blake3Hash {
        &self.node
    }

    pub fn upper_bound(&self) -> &Key {
        &self.upper_bound
    }
}

impl<'a, Key> Buf<'a> for Link<'a, Key>
where
    Key: self::Key<'a>,
    Key::Ref: self::KeyRef<'a, Key>,
{
    type Ref = LinkRef<'a, Key>;

    fn to_ref(&'a self) -> Self::Ref {
        LinkRef {
            node: &self.node,
            upper_bound: self.upper_bound.to_ref(),
        }
    }
}

impl<'a, Key> Cellular<'a> for Link<'a, Key>
where
    Key: self::Key<'a>,
    Key::Ref: self::KeyRef<'a, Key>,
{
    fn cell_width() -> Width {
        Blake3Hash::cell_width() + Key::cell_width()
    }

    fn cells(&self) -> impl Iterator<Item = &[u8]> {
        self.node.cells().chain(self.upper_bound.cells())
    }

    fn try_from_cells<I>(cells: &mut I) -> Result<Self, dialog_encoding::DialogEncodingError>
    where
        I: Iterator<Item = &'a [u8]>,
    {
        let node = Blake3Hash::try_from_cells(cells)?;
        let upper_bound = Key::try_from_cells(cells)?;

        Ok(Link {
            node,
            upper_bound,
            lifetime: PhantomData,
        })
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct LinkRef<'a, Key>
where
    Key: self::Key<'a>,
    Key::Ref: self::KeyRef<'a, Key>,
{
    node: &'a Blake3Hash,
    upper_bound: Key::Ref,
}

impl<'a, Key> LinkRef<'a, Key>
where
    Key: self::Key<'a>,
    Key::Ref: self::KeyRef<'a, Key>,
{
    pub fn node(&self) -> &Blake3Hash {
        self.node
    }

    pub fn upper_bound(&self) -> &Key::Ref {
        &self.upper_bound
    }
}

impl<'a, Key> Ref<'a, Link<'a, Key>> for LinkRef<'a, Key>
where
    Key: self::Key<'a>,
    Key::Ref: self::KeyRef<'a, Key>,
{
    fn to_buf(&self) -> Link<'a, Key> {
        Link {
            node: self.node.to_owned(),
            upper_bound: self.upper_bound.to_buf(),
            lifetime: PhantomData,
        }
    }
}

impl<'a, Key> Cellular<'a> for LinkRef<'a, Key>
where
    Key: self::Key<'a>,
    Key::Ref: self::KeyRef<'a, Key>,
{
    fn cell_width() -> Width {
        Blake3Hash::cell_width() + Key::cell_width()
    }

    fn cells(&self) -> impl Iterator<Item = &[u8]> {
        self.node.cells().chain(self.upper_bound.cells())
    }

    fn try_from_cells<I>(cells: &mut I) -> Result<Self, dialog_encoding::DialogEncodingError>
    where
        I: Iterator<Item = &'a [u8]>,
    {
        let node = <&Blake3Hash>::try_from_cells(cells)?;
        let upper_bound = Key::Ref::try_from_cells(cells)?;

        Ok(LinkRef { node, upper_bound })
    }
}

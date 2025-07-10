use std::marker::PhantomData;

use dialog_common::Blake3Hash;
use dialog_encoding::{Cellular, DialogEncodingError, Width};
use zerocopy::TryFromBytes;

use crate::KeyBuffer;

/// A serializable reference to a [`Node`].
#[derive(Debug, Clone, PartialEq)]
pub struct NodeLink<'a, Key>
where
    Key: KeyBuffer<'a>,
{
    pub hash: Blake3Hash,
    pub upper_bound: Key,
    lifetime: PhantomData<&'a ()>,
}

impl<'a, Key> NodeLink<'a, Key>
where
    Key: KeyBuffer<'a>,
{
    pub fn link_ref(&'a self) -> NodeLinkRef<'a, Key> {
        NodeLinkRef {
            hash: &self.hash,
            upper_bound: self.upper_bound.key_ref(),
        }
    }
}

pub struct NodeLinkRef<'a, Key>
where
    Key: KeyBuffer<'a>,
{
    pub hash: &'a Blake3Hash,
    pub upper_bound: Key::Ref,
}

impl<'a, Key> Cellular<'a> for NodeLinkRef<'a, Key>
where
    Key: KeyBuffer<'a>,
{
    fn cell_width() -> Width {
        Width::Bounded(1) + Key::Ref::cell_width()
    }

    fn cells(&self) -> impl Iterator<Item = &[u8]> {
        std::iter::once(self.hash.bytes().as_ref()).chain(self.upper_bound.cells())
    }

    fn try_from_cells<I>(cells: &mut I) -> Result<Self, DialogEncodingError>
    where
        I: Iterator<Item = &'a [u8]>,
    {
        let Some(hash) = cells
            .next()
            .and_then(|bytes| Blake3Hash::try_ref_from_bytes(bytes).ok())
        else {
            return Err(DialogEncodingError::InvalidLayout(
                "Could not decode node hash reference".into(),
            ));
        };

        let upper_bound = Key::Ref::try_from_cells(cells)?;

        Ok(Self { hash, upper_bound })
    }
}

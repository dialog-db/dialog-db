use dialog_encoding::{Cellular, DialogEncodingError, Width};
use itertools::Itertools;
use nonempty::NonEmpty;

use crate::{KeyBuffer, NodeLinkRef};

pub struct Branch<'a, Key>
where
    Key: KeyBuffer<'a>,
{
    pub links: NonEmpty<NodeLinkRef<'a, Key>>,
}

impl<'a, Key> Cellular<'a> for Branch<'a, Key>
where
    Key: KeyBuffer<'a>,
{
    fn cell_width() -> Width {
        Width::Unbounded
    }

    fn cells(&self) -> impl Iterator<Item = &[u8]> {
        self.links.iter().flat_map(|link| link.cells())
    }

    fn try_from_cells<I>(cells: &mut I) -> Result<Self, dialog_encoding::DialogEncodingError>
    where
        I: Iterator<Item = &'a [u8]>,
    {
        let mut links = Vec::new();
        let chunk_size = match NodeLinkRef::<'a, Key>::cell_width() {
            Width::Bounded(size) => size,
            _ => {
                return Err(DialogEncodingError::InvalidLayout(format!(
                    "Node references must have bounded cell width"
                )));
            }
        };

        for mut chunk in &cells.chunks(chunk_size) {
            links.push(NodeLinkRef::try_from_cells(&mut chunk)?);
        }

        Ok(Self {
            links: NonEmpty::from_vec(links).ok_or_else(|| {
                DialogEncodingError::InvalidLayout("Branch must have at least one reference".into())
            })?,
        })
    }
}

// use crate::{Key, KeyRef, Link};
// use dialog_encoding::{Cellular, DialogEncodingError, Width};
// use itertools::Itertools;
// use nonempty::NonEmpty;
// use zerocopy::{IntoBytes, TryFromBytes};

// use super::NodeType;

// pub struct Branch<'a, K>
// where
//     K: Key<'a>,
// {
//     pub node_type: &'a NodeType,
//     pub references: NonEmpty<Link<'a, K>>,
// }

// impl<'a, K> Cellular<'a> for Branch<'a, K>
// where
//     K: Key<'a>,
// {
//     fn cell_width() -> Width {
//         Width::Unbounded
//     }

//     fn cells(&'a self) -> impl Iterator<Item = &'a [u8]> {
//         std::iter::once(self.node_type.as_bytes()).chain(
//             self.references
//                 .iter()
//                 .flat_map(|reference| reference.cells()),
//         )
//     }

//     fn try_from_cells<I>(cells: &mut I) -> Result<Self, dialog_encoding::DialogEncodingError>
//     where
//         I: Iterator<Item = &'a [u8]>,
//     {
//         let node_type = cells
//             .next()
//             .ok_or_else(|| DialogEncodingError::InvalidLayout("Missing node type tag".into()))
//             .and_then(|bytes| {
//                 NodeType::try_ref_from_bytes(bytes).map_err(|error| {
//                     DialogEncodingError::InvalidLayout(format!(
//                         "Could not interpret as node type: {error}"
//                     ))
//                 })
//             })?;

//         let mut references = Vec::new();
//         let chunk_size = match Link::<'a, K>::cell_width() {
//             Width::Bounded(size) => size,
//             _ => {
//                 return Err(DialogEncodingError::InvalidLayout(format!(
//                     "Node references must have bounded cell width"
//                 )));
//             }
//         };

//         for mut chunk in &cells.chunks(chunk_size) {
//             references.push(Link::try_from_cells(&mut chunk)?);
//         }

//         Ok(Self {
//             node_type,
//             references: NonEmpty::from_vec(references).ok_or_else(|| {
//                 DialogEncodingError::InvalidLayout("Branch must have at least one reference".into())
//             })?,
//         })
//     }
// }

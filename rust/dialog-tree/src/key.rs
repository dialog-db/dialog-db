use dialog_common::{Blake3Hashed, ConditionalSync};
use dialog_encoding::CellularToOwned;
use std::fmt::Debug;

pub trait KeyBuffer<'a>: ConditionalSync + Clone + PartialEq + Ord + Debug {
    type Ref: KeyRef<'a, Self>;

    fn key_ref(&'a self) -> Self::Ref;
}

/// A key used to reference values in a [Tree] or [Node].
pub trait KeyRef<'a, K>:
    CellularToOwned<'a, Owned = K> + Blake3Hashed + ConditionalSync + Clone + PartialEq + Ord + Debug
where
    K: KeyBuffer<'a>,
{
}

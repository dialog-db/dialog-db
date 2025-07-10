use dialog_common::ConditionalSync;
use dialog_encoding::CellularToOwned;
use std::fmt::Debug;

pub trait ValueBuffer<'a>: ConditionalSync + Clone + Debug
where
    Self: 'a,
{
    type Ref: ValueRef<'a, Self> + 'a;

    fn value_ref(&'a self) -> &'a Self::Ref;
}

/// A key used to reference values in a [Tree] or [Node].
pub trait ValueRef<'a, V>:
    CellularToOwned<'a, Owned = V> + From<V> + ConditionalSync + Clone + Debug
where
    V: ValueBuffer<'a>,
{
}

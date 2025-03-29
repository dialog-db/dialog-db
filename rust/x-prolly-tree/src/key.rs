use x_common::ConditionalSync;

/// A key used to reference values in a [Tree] or [Node].
pub trait KeyType:
    std::fmt::Debug + AsRef<[u8]> + ConditionalSync + Clone + PartialEq + Ord
{
}

impl KeyType for Vec<u8> {}

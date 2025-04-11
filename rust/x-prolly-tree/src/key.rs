use x_common::ConditionalSync;

/// A key used to reference values in a [Tree] or [Node].
pub trait KeyType:
    std::fmt::Debug + AsRef<[u8]> + TryFrom<Vec<u8>> + ConditionalSync + Clone + PartialEq + Ord
{
}

impl KeyType for Vec<u8> {}

/// A value that may be stored within a [Tree]
pub trait ValueType: std::fmt::Debug + TryFrom<Vec<u8>> + ConditionalSync + Clone {
    /// Serialize this value into its byte representation
    fn to_vec(&self) -> Vec<u8>;
}

impl ValueType for Vec<u8> {
    fn to_vec(&self) -> Vec<u8> {
        self.clone()
    }
}

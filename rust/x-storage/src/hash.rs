use x_common::ConditionalSend;

/// A trait that can be implemented for types that represent a hash. A blanket
/// "unchecked" implementation is provided for any type that matches
/// `AsRef<[u8]>` (this might be an antipattern; more investigation required).
pub trait HashType<const SIZE: usize>: Clone + ConditionalSend {
    /// Get the raw bytes of the hash
    fn bytes(&self) -> [u8; SIZE];
}

impl<const SIZE: usize, T> HashType<SIZE> for T
where
    T: Clone + AsRef<[u8]> + ConditionalSend,
{
    fn bytes(&self) -> [u8; SIZE] {
        let mut bytes = [0u8; SIZE];
        bytes.copy_from_slice(self.as_ref());
        bytes
    }
}

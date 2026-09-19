//! What an effect ships beside its invocation.
//!
//! An invocation commits to its payload and does not contain it: the
//! attenuation projects a block to a digest and a checksum, so the bytes
//! travel as their own block in the same container. This trait is the
//! sending half of that — the counterpart of [`crate::resolve`], which
//! is how a peer finds them again.
//!
//! There is no blanket implementation, on purpose. An effect added later
//! will not compile until it says whether it carries anything, which is
//! the question that would otherwise be answered by silently shipping
//! nothing and failing at the far end.

use dialog_effects::{archive, memory};

/// The blocks an effect's arguments name.
///
/// Order is not significant: a bundle keys blocks by the hash of their
/// bytes, so the receiver addresses them rather than indexing them.
pub trait Carries {
    /// The bytes this effect commits to and must ship.
    fn blocks(&self) -> Vec<Vec<u8>>;
}

/// Reads name nothing, so they carry nothing.
macro_rules! carries_nothing {
    ($($effect:ty),+ $(,)?) => {
        $(impl Carries for $effect {
            fn blocks(&self) -> Vec<Vec<u8>> {
                Vec::new()
            }
        })+
    };
}

carries_nothing!(archive::Get, memory::Resolve, memory::Retract);

impl Carries for archive::Put {
    fn blocks(&self) -> Vec<Vec<u8>> {
        vec![self.block.as_ref().to_vec()]
    }
}

impl Carries for archive::Import {
    fn blocks(&self) -> Vec<Vec<u8>> {
        self.blocks
            .iter()
            .map(|block| block.as_ref().to_vec())
            .collect()
    }
}

impl Carries for memory::Publish {
    fn blocks(&self) -> Vec<Vec<u8>> {
        vec![self.content.clone()]
    }
}
